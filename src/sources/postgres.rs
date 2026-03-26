use crate::config::{
    ColumnSelection, PostgresConfig, PostgresTableConfig, PostgresTableDefaults, SchemaChangePolicy,
};
use crate::destinations::etl_bigquery::{CdcTableInfo, EtlBigQueryDestination};
use crate::destinations::{Destination, WriteMode};
use crate::runner::ShutdownSignal;
use crate::state::{ConnectionState, StateHandle};
use crate::stats::StatsHandle;
use crate::types::{
    ColumnSchema, DataType, META_DELETED_AT, META_SYNCED_AT, RowBatch, SchemaFieldSnapshot,
    TableCheckpoint, TableSchema,
};
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use etl::config::{PgConnectionConfig, TlsConfig};
use etl::destination::Destination as EtlDestinationTrait;
use etl::replication::client::PgReplicationClient;
use etl::replication::stream::{EventsStream, StatusUpdateType, TableCopyStream};
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, DeleteEvent, Event, InsertEvent, TableId, TableRow, TableSchema as EtlTableSchema,
    TruncateEvent, UpdateEvent,
};
use etl_postgres::replication::slots::EtlReplicationSlot;
use futures::StreamExt;
use globset::{Glob, GlobSet};
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use secrecy::SecretString;
use serde::Serialize;
use serde_json::Value;
use sqlx::Row as SqlxRow;
use sqlx::ValueRef;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

pub struct PostgresSource {
    config: PostgresConfig,
    pool: PgPool,
}

const DEFAULT_PG_POOL_MAX: u32 = 5;

#[derive(Debug, Clone)]
pub struct ResolvedPostgresTable {
    pub name: String,
    pub primary_key: String,
    pub updated_at_column: Option<String>,
    pub soft_delete: bool,
    pub soft_delete_column: Option<String>,
    pub where_clause: Option<String>,
    pub columns: ColumnSelection,
}

pub struct TableSyncRequest<'a> {
    pub table: &'a ResolvedPostgresTable,
    pub dest: &'a dyn Destination,
    pub checkpoint: TableCheckpoint,
    pub state_handle: Option<StateHandle>,
    pub mode: crate::types::SyncMode,
    pub dry_run: bool,
    pub default_batch_size: usize,
    pub schema_diff_enabled: bool,
    pub stats: Option<StatsHandle>,
}

pub struct CdcSyncRequest<'a> {
    pub dest: &'a crate::destinations::bigquery::BigQueryDestination,
    pub state: &'a mut ConnectionState,
    pub state_handle: Option<StateHandle>,
    pub mode: crate::types::SyncMode,
    pub dry_run: bool,
    pub follow: bool,
    pub default_batch_size: usize,
    pub tables: &'a [ResolvedPostgresTable],
    pub schema_diff_enabled: bool,
    pub stats: Option<StatsHandle>,
    pub shutdown: Option<ShutdownSignal>,
}

struct TableRunOptions<'a> {
    dry_run: bool,
    batch_size: usize,
    stats: Option<&'a StatsHandle>,
    state_handle: Option<StateHandle>,
}

struct IncrementalSqlParts<'a> {
    schema: &'a str,
    table: &'a str,
    columns: &'a str,
    updated_at: &'a str,
    primary_key: &'a str,
    pk_cast: &'a str,
    where_clause: Option<&'a str>,
    has_last_pk: bool,
}

struct CdcStreamConfig<'a> {
    publication: &'a str,
    slot_name: &'a str,
    start_lsn: etl::types::PgLsn,
    pipeline_id: u64,
    idle_timeout: Duration,
    max_pending_events: usize,
    follow: bool,
    shutdown: Option<ShutdownSignal>,
}

struct CdcStreamRuntime<'a> {
    include_tables: &'a HashSet<TableId>,
    table_configs: &'a HashMap<TableId, ResolvedPostgresTable>,
    store: &'a MemoryStore,
    dest: &'a EtlBigQueryDestination,
    table_info_map: &'a mut HashMap<TableId, CdcTableInfo>,
    etl_schemas: &'a mut HashMap<TableId, EtlTableSchema>,
    table_hashes: &'a mut HashMap<TableId, String>,
    table_snapshots: &'a mut HashMap<TableId, Vec<SchemaFieldSnapshot>>,
    state: &'a mut ConnectionState,
    schema_policy: SchemaChangePolicy,
    schema_diff_enabled: bool,
    stats: Option<StatsHandle>,
    state_handle: Option<StateHandle>,
}

struct CdcRelationRuntime<'a> {
    table_configs: &'a HashMap<TableId, ResolvedPostgresTable>,
    store: &'a MemoryStore,
    dest: &'a EtlBigQueryDestination,
    table_info_map: &'a mut HashMap<TableId, CdcTableInfo>,
    etl_schemas: &'a mut HashMap<TableId, EtlTableSchema>,
    table_hashes: &'a mut HashMap<TableId, String>,
    table_snapshots: &'a mut HashMap<TableId, Vec<SchemaFieldSnapshot>>,
    state: &'a mut ConnectionState,
    schema_policy: SchemaChangePolicy,
    schema_diff_enabled: bool,
    state_handle: Option<StateHandle>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PostgresTableSummary {
    pub row_count: i64,
    pub max_updated_at: Option<DateTime<Utc>>,
}

impl PostgresSource {
    pub async fn new(config: PostgresConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_PG_POOL_MAX)
            .connect(&config.url)
            .await
            .context("connecting to postgres")?;
        Ok(Self { config, pool })
    }

    pub fn pool_max_connections(&self) -> u32 {
        DEFAULT_PG_POOL_MAX
    }

    pub async fn summarize_table(
        &self,
        table: &ResolvedPostgresTable,
    ) -> Result<PostgresTableSummary> {
        let (schema_name, table_name) = split_table_name(&table.name);
        let max_expr = if let Some(updated_at_column) = &table.updated_at_column {
            format!("max({updated_at_column}) as max_updated_at")
        } else {
            "null as max_updated_at".to_string()
        };
        let sql = if let Some(where_clause) = &table.where_clause {
            format!(
                "select count(*)::bigint as row_count, {max_expr} \
                 from {schema}.{table} where ({where_clause})",
                max_expr = max_expr,
                schema = schema_name,
                table = table_name,
                where_clause = where_clause
            )
        } else {
            format!(
                "select count(*)::bigint as row_count, {max_expr} from {schema}.{table}",
                max_expr = max_expr,
                schema = schema_name,
                table = table_name
            )
        };
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        Ok(PostgresTableSummary {
            row_count: row.try_get("row_count")?,
            max_updated_at: read_updated_at(&row, "max_updated_at"),
        })
    }

    pub async fn resolve_tables(&self) -> Result<Vec<ResolvedPostgresTable>> {
        let mut table_map: HashMap<String, PostgresTableConfig> = HashMap::new();

        if let Some(tables) = &self.config.tables {
            for table in tables {
                table_map.insert(table.name.clone(), table.clone());
            }
        }

        if let Some(selection) = &self.config.table_selection {
            if !selection.include.is_empty() || !selection.exclude.is_empty() {
                let all_tables = self.discover_table_names().await?;
                let include_set = build_globset(&selection.include)?;
                let exclude_set = build_globset(&selection.exclude)?;

                for name in all_tables {
                    if !selection.include.is_empty() && !include_set.is_match(&name) {
                        continue;
                    }
                    if !selection.exclude.is_empty() && exclude_set.is_match(&name) {
                        continue;
                    }
                    table_map
                        .entry(name.clone())
                        .or_insert(PostgresTableConfig {
                            name,
                            primary_key: None,
                            updated_at_column: None,
                            soft_delete: None,
                            soft_delete_column: None,
                            where_clause: None,
                            columns: None,
                        });
                }
            }

            if !selection.exclude.is_empty() {
                let exclude_set = build_globset(&selection.exclude)?;
                table_map.retain(|name, _| !exclude_set.is_match(name));
            }
        }

        if table_map.is_empty() {
            anyhow::bail!("no postgres tables resolved from config");
        }

        let defaults = self
            .config
            .table_selection
            .as_ref()
            .and_then(|sel| sel.defaults.clone());

        let mut resolved = Vec::with_capacity(table_map.len());
        for table_cfg in table_map.values() {
            resolved.push(
                self.apply_table_defaults(table_cfg, defaults.as_ref())
                    .await?,
            );
        }

        resolved.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(resolved)
    }

    async fn discover_table_names(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            select table_schema, table_name
            from information_schema.tables
            where table_type = 'BASE TABLE'
              and table_schema not in ('pg_catalog', 'information_schema')
            order by table_schema, table_name
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut names = Vec::with_capacity(rows.len());
        for row in rows {
            let schema: String = row.try_get("table_schema")?;
            let table: String = row.try_get("table_name")?;
            names.push(format!("{}.{}", schema, table));
        }
        Ok(names)
    }

    async fn apply_table_defaults(
        &self,
        table: &PostgresTableConfig,
        defaults: Option<&PostgresTableDefaults>,
    ) -> Result<ResolvedPostgresTable> {
        let mut primary_key = table.primary_key.clone();
        if primary_key.is_none() {
            primary_key = defaults.and_then(|d| d.primary_key.clone());
        }
        if primary_key.is_none() {
            primary_key = self.discover_primary_key(&table.name).await?;
        }
        let primary_key = primary_key
            .ok_or_else(|| anyhow::anyhow!("primary key required for table {}", table.name))?;

        let updated_at_column = table
            .updated_at_column
            .clone()
            .or_else(|| defaults.and_then(|d| d.updated_at_column.clone()));
        let soft_delete = table
            .soft_delete
            .or_else(|| defaults.and_then(|d| d.soft_delete))
            .unwrap_or(false);
        let soft_delete_column = table
            .soft_delete_column
            .clone()
            .or_else(|| defaults.and_then(|d| d.soft_delete_column.clone()));
        let where_clause = table
            .where_clause
            .clone()
            .or_else(|| defaults.and_then(|d| d.where_clause.clone()));
        let columns = table
            .columns
            .clone()
            .or_else(|| defaults.and_then(|d| d.columns.clone()))
            .unwrap_or(ColumnSelection {
                include: Vec::new(),
                exclude: Vec::new(),
            });

        Ok(ResolvedPostgresTable {
            name: table.name.clone(),
            primary_key,
            updated_at_column,
            soft_delete,
            soft_delete_column,
            where_clause,
            columns,
        })
    }

    async fn discover_primary_key(&self, table: &str) -> Result<Option<String>> {
        let (schema_name, table_name) = split_table_name(table);
        let rows = sqlx::query(
            r#"
            select a.attname as column_name
            from pg_index i
            join pg_class c on c.oid = i.indrelid
            join pg_namespace n on n.oid = c.relnamespace
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
            where i.indisprimary
              and n.nspname = $1
              and c.relname = $2
            order by a.attnum
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }
        if rows.len() > 1 {
            anyhow::bail!(
                "table {} has composite primary key; only single-column primary keys are supported",
                table
            );
        }
        let name: String = rows[0].try_get("column_name")?;
        Ok(Some(name))
    }

    pub async fn discover_schema(&self, table: &ResolvedPostgresTable) -> Result<TableSchema> {
        let (schema_name, table_name) = split_table_name(&table.name);
        let rows = sqlx::query(
            r#"SELECT column_name, data_type, is_nullable
               FROM information_schema.columns
               WHERE table_schema = $1 AND table_name = $2
               ORDER BY ordinal_position"#,
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(&self.pool)
        .await?;

        let mut all_columns = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.try_get("column_name")?;
            let data_type: String = row.try_get("data_type")?;
            let nullable: String = row.try_get("is_nullable")?;
            all_columns.push(ColumnSchema {
                name,
                data_type: pg_type_to_data_type(&data_type),
                nullable: nullable == "YES",
            });
        }

        let required = required_columns(table);
        ensure_required_columns(&all_columns, &required)?;
        let columns = filter_columns(&all_columns, &table.columns, &required);

        Ok(TableSchema {
            name: crate::types::destination_table_name(&table.name),
            columns,
            primary_key: Some(table.primary_key.clone()),
        })
    }

    async fn resolve_primary_key_cast(
        &self,
        schema_name: &str,
        table_name: &str,
        column: &str,
    ) -> Result<String> {
        let row = sqlx::query(
            r#"
            select udt_name, udt_schema
            from information_schema.columns
            where table_schema = $1 and table_name = $2 and column_name = $3
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .bind(column)
        .fetch_one(&self.pool)
        .await?;
        let udt_name: String = row.try_get("udt_name")?;
        let udt_schema: String = row.try_get("udt_schema")?;
        Ok(format_cast_type(&udt_name, &udt_schema))
    }

    pub async fn sync_table(&self, request: TableSyncRequest<'_>) -> Result<TableCheckpoint> {
        let TableSyncRequest {
            table,
            dest,
            checkpoint,
            state_handle,
            mode,
            dry_run,
            default_batch_size,
            schema_diff_enabled,
            stats,
        } = request;
        let schema = self.discover_schema(table).await?;
        dest.ensure_table(&schema).await?;

        let schema_hash = schema_fingerprint(&schema);
        let policy = self.config.schema_policy();
        let mut entry = checkpoint;
        let run_options = TableRunOptions {
            dry_run,
            batch_size: self.config.batch_size.unwrap_or(default_batch_size),
            stats: stats.as_ref(),
            state_handle,
        };
        if let Some(diff) = schema_diff(entry.schema_snapshot.as_deref(), &schema)
            && !diff.is_empty()
        {
            if schema_diff_enabled {
                log_schema_diff(&table.name, &diff);
            }
            match policy {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    info!(table = %table.name, "schema change detected; resyncing table");
                    if !dry_run {
                        dest.truncate_table(&schema.name).await?;
                    }
                    self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                    entry.schema_hash = Some(schema_hash);
                    entry.schema_snapshot = Some(schema_snapshot_from_schema(&schema));
                    return Ok(entry);
                }
                SchemaChangePolicy::Auto => {
                    if diff.has_incompatible() {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; set schema_changes=resync or fail",
                            table.name
                        );
                    }
                    info!(table = %table.name, "schema change detected; auto-altering destination");
                }
            }
        }

        match mode {
            crate::types::SyncMode::Full => {
                if !dry_run {
                    dest.truncate_table(&schema.name).await?;
                }
                self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                    .await?;
            }
            crate::types::SyncMode::Incremental => {
                if table.updated_at_column.is_none() {
                    warn!(
                        "table {} missing updated_at_column; falling back to full refresh",
                        table.name
                    );
                    if !dry_run {
                        dest.truncate_table(&schema.name).await?;
                    }
                    self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                } else {
                    self.run_incremental(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                }
            }
        }

        entry.schema_hash = Some(schema_hash);
        entry.schema_snapshot = Some(schema_snapshot_from_schema(&schema));
        Ok(entry)
    }

    pub fn cdc_enabled(&self) -> bool {
        self.config.cdc.unwrap_or(true)
    }

    pub async fn sync_cdc(&self, request: CdcSyncRequest<'_>) -> Result<()> {
        let CdcSyncRequest {
            dest,
            state,
            state_handle,
            mode,
            dry_run,
            follow,
            default_batch_size,
            tables,
            schema_diff_enabled,
            stats,
            shutdown,
        } = request;
        if dry_run {
            info!("dry-run: skipping CDC sync");
            return Ok(());
        }

        if !self.cdc_enabled() {
            anyhow::bail!("CDC is disabled for Postgres source");
        }
        if tables.is_empty() {
            anyhow::bail!("no postgres tables configured for CDC");
        }
        let policy = self.config.schema_policy();

        let publication = self
            .config
            .publication
            .as_deref()
            .context("postgres.publication is required when CDC is enabled")?;
        let pipeline_id = self.config.cdc_pipeline_id.unwrap_or(1);
        let batch_size = self
            .config
            .cdc_batch_size
            .or(self.config.batch_size)
            .unwrap_or(default_batch_size);
        let max_pending_events = self.config.cdc_max_pending_events.unwrap_or(100_000);
        let idle_timeout = Duration::from_secs(self.config.cdc_idle_timeout_seconds.unwrap_or(10));

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;

        self.validate_wal_level().await?;

        if !replication_client.publication_exists(publication).await? {
            anyhow::bail!("publication '{}' does not exist", publication);
        }

        self.validate_publication_tables(&replication_client, publication, tables)
            .await?;
        self.validate_publication_filters(publication, tables, false)
            .await?;

        let table_ids = self.resolve_table_ids(tables).await?;

        let store = MemoryStore::new();
        let mut table_info_map: HashMap<TableId, CdcTableInfo> = HashMap::new();
        let mut etl_schemas: HashMap<TableId, EtlTableSchema> = HashMap::new();
        let mut include_tables: HashSet<TableId> = HashSet::new();
        let mut table_hashes: HashMap<TableId, String> = HashMap::new();
        let mut table_snapshots: HashMap<TableId, Vec<SchemaFieldSnapshot>> = HashMap::new();
        let mut resync_tables: HashSet<TableId> = HashSet::new();

        for (table_id, table_cfg) in table_ids.iter() {
            let etl_schema = self.load_etl_table_schema(*table_id).await?;
            store.store_table_schema(etl_schema.clone()).await?;
            let info = cdc_table_info_from_schema(table_cfg, &etl_schema)?;
            dest.ensure_table(&info.schema).await?;
            let schema_hash = schema_fingerprint(&info.schema);
            let prev_snapshot = state
                .postgres
                .get(&table_cfg.name)
                .and_then(|checkpoint| checkpoint.schema_snapshot.clone());
            if let Some(diff) = schema_diff(prev_snapshot.as_deref(), &info.schema)
                && !diff.is_empty()
            {
                if schema_diff_enabled {
                    log_schema_diff(&table_cfg.name, &diff);
                }
                match policy {
                    SchemaChangePolicy::Fail => {
                        anyhow::bail!(
                            "schema change detected for {}; set schema_changes=auto or resync",
                            table_cfg.name
                        );
                    }
                    SchemaChangePolicy::Resync => {
                        resync_tables.insert(*table_id);
                    }
                    SchemaChangePolicy::Auto => {
                        if diff.has_incompatible() {
                            anyhow::bail!(
                                "incompatible schema change detected for {}; set schema_changes=resync or fail",
                                table_cfg.name
                            );
                        }
                    }
                }
            }
            table_snapshots.insert(*table_id, schema_snapshot_from_schema(&info.schema));
            table_hashes.insert(*table_id, schema_hash);
            include_tables.insert(*table_id);
            table_info_map.insert(*table_id, info);
            etl_schemas.insert(*table_id, etl_schema);
        }

        let cdc_dest =
            EtlBigQueryDestination::new(dest.clone(), table_info_map.clone(), stats.clone());

        let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
            .try_into()
            .context("building CDC replication slot name")?;

        {
            let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
            cdc_state.slot_name = Some(slot_name.clone());
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }
        let mut last_lsn = state.postgres_cdc.as_ref().and_then(|s| s.last_lsn.clone());

        let mut start_lsn = None;
        let mut needs_snapshot = mode == crate::types::SyncMode::Full || last_lsn.is_none();
        if !resync_tables.is_empty() {
            needs_snapshot = true;
        }

        if needs_snapshot {
            if let Err(err) = replication_client.delete_slot(&slot_name).await
                && err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
            {
                return Err(err.into());
            }

            let (transaction, slot) = replication_client
                .create_slot_with_transaction(&slot_name)
                .await?;

            let snapshot_table_ids: HashSet<TableId> =
                if mode == crate::types::SyncMode::Full || last_lsn.is_none() {
                    include_tables.clone()
                } else {
                    resync_tables.clone()
                };

            if mode == crate::types::SyncMode::Full {
                for table_id in &snapshot_table_ids {
                    if let Some(info) = table_info_map.get(table_id) {
                        dest.truncate_table(&info.dest_name).await?;
                    }
                }
            } else if !resync_tables.is_empty() {
                for table_id in &resync_tables {
                    if let Some(info) = table_info_map.get(table_id) {
                        dest.truncate_table(&info.dest_name).await?;
                    }
                }
            }

            for (table_id, info) in table_info_map.iter() {
                if !snapshot_table_ids.contains(table_id) {
                    continue;
                }
                let etl_schema = etl_schemas
                    .get(table_id)
                    .context("missing table schema for CDC copy")?;
                info!(table = %info.source_name, "starting CDC snapshot copy");
                let copy_stream = transaction
                    .get_table_copy_stream(*table_id, &etl_schema.column_schemas, Some(publication))
                    .await?;
                let stream =
                    TableCopyStream::wrap(copy_stream, &etl_schema.column_schemas, pipeline_id);
                tokio::pin!(stream);
                let mut buffer: Vec<TableRow> = Vec::with_capacity(batch_size);
                while let Some(result) = stream.next().await {
                    let row = result?;
                    buffer.push(row);
                    if buffer.len() >= batch_size {
                        if let Some(stats) = &stats {
                            stats
                                .record_extract(&info.source_name, buffer.len(), 0)
                                .await;
                        }
                        cdc_dest
                            .write_table_rows(*table_id, std::mem::take(&mut buffer))
                            .await?;
                    }
                }
                if !buffer.is_empty() {
                    if let Some(stats) = &stats {
                        stats
                            .record_extract(&info.source_name, buffer.len(), 0)
                            .await;
                    }
                    cdc_dest.write_table_rows(*table_id, buffer).await?;
                }
                info!(table = %info.source_name, "completed CDC snapshot copy");
            }

            transaction.commit().await?;
            start_lsn = Some(slot.consistent_point);
            last_lsn = Some(slot.consistent_point.to_string());
        }

        let start_lsn = match (start_lsn, last_lsn.as_deref()) {
            (Some(lsn), _) => lsn,
            (None, Some(lsn_str)) => lsn_str
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid LSN '{}'", lsn_str))?,
            (None, None) => {
                let slot = replication_client.get_or_create_slot(&slot_name).await?;
                slot.get_start_lsn()
            }
        };

        let last_flushed = self
            .stream_cdc_changes(
                replication_client.clone(),
                CdcStreamConfig {
                    publication,
                    slot_name: &slot_name,
                    start_lsn,
                    pipeline_id,
                    idle_timeout,
                    max_pending_events,
                    follow,
                    shutdown: shutdown.clone(),
                },
                CdcStreamRuntime {
                    include_tables: &include_tables,
                    table_configs: &table_ids,
                    store: &store,
                    dest: &cdc_dest,
                    table_info_map: &mut table_info_map,
                    etl_schemas: &mut etl_schemas,
                    table_hashes: &mut table_hashes,
                    table_snapshots: &mut table_snapshots,
                    state,
                    schema_policy: policy,
                    schema_diff_enabled,
                    stats: stats.clone(),
                    state_handle: state_handle.clone(),
                },
            )
            .await?;

        if let Ok(slot) = replication_client.get_slot(&slot_name).await {
            last_lsn = Some(slot.confirmed_flush_lsn.to_string());
        } else {
            last_lsn = Some(last_flushed.to_string());
        }

        if let Some(cdc_state) = state.postgres_cdc.as_mut() {
            cdc_state.last_lsn = last_lsn;
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }

        for (table_id, hash) in table_hashes {
            if let Some(info) = table_info_map.get(&table_id) {
                let entry = state.postgres.entry(info.source_name.clone()).or_default();
                entry.schema_hash = Some(hash);
                entry.schema_snapshot = table_snapshots.get(&table_id).cloned();
                if let Some(state_handle) = &state_handle {
                    state_handle
                        .save_postgres_checkpoint(&info.source_name, entry)
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn validate_cdc_publication(
        &self,
        tables: &[ResolvedPostgresTable],
        verbose: bool,
    ) -> Result<()> {
        if !self.cdc_enabled() {
            anyhow::bail!("CDC is disabled for Postgres source");
        }
        if tables.is_empty() {
            anyhow::bail!("no postgres tables configured for CDC");
        }

        let publication = self
            .config
            .publication
            .as_deref()
            .context("postgres.publication is required when CDC is enabled")?;

        self.validate_wal_level().await?;

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;
        if !replication_client.publication_exists(publication).await? {
            anyhow::bail!("publication '{}' does not exist", publication);
        }

        self.validate_publication_tables(&replication_client, publication, tables)
            .await?;
        self.validate_publication_filters(publication, tables, verbose)
            .await?;
        Ok(())
    }

    async fn run_full_refresh(
        &self,
        table: &ResolvedPostgresTable,
        schema: &TableSchema,
        dest: &dyn Destination,
        checkpoint: &mut TableCheckpoint,
        options: &TableRunOptions<'_>,
    ) -> Result<()> {
        let (schema_name, table_name) = split_table_name(&table.name);

        if !options.dry_run {
            // Ensure table exists after truncate; emulator deletes tables on truncate.
            dest.ensure_table(schema).await?;
        }

        let pk_alias = "__cdsync_pk";
        let pk_cast = self
            .resolve_primary_key_cast(&schema_name, &table_name, &table.primary_key)
            .await?;
        let select_columns = schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let select_expr = format!(
            "{}, {}::text as {}",
            select_columns, table.primary_key, pk_alias
        );
        let mut last_pk: Option<String> = None;
        let has_where = table.where_clause.is_some();
        let base_sql = if let Some(where_clause) = &table.where_clause {
            format!(
                "SELECT {select} FROM {schema}.{table} WHERE ({where_clause})",
                select = select_expr,
                schema = schema_name,
                table = table_name,
                where_clause = where_clause
            )
        } else {
            format!(
                "SELECT {select} FROM {schema}.{table}",
                select = select_expr,
                schema = schema_name,
                table = table_name
            )
        };
        let sql_without_pk = format!("{} ORDER BY {} LIMIT $1", base_sql, table.primary_key);
        let sql_with_pk = if has_where {
            format!(
                "{} AND {} > $1::{} ORDER BY {} LIMIT $2",
                base_sql, table.primary_key, pk_cast, table.primary_key
            )
        } else {
            format!(
                "{} WHERE {} > $1::{} ORDER BY {} LIMIT $2",
                base_sql, table.primary_key, pk_cast, table.primary_key
            )
        };
        loop {
            let extract_start = Instant::now();
            let rows = if let Some(last_pk_value) = last_pk.as_ref() {
                sqlx::query(&sql_with_pk)
                    .bind(last_pk_value)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                sqlx::query(&sql_without_pk)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            };
            let extract_ms = extract_start.elapsed().as_millis() as u64;

            if rows.is_empty() {
                break;
            }

            let batch = rows_to_batch(schema, &rows, table, Utc::now())?;
            if let Some(stats) = options.stats {
                stats
                    .record_extract(&table.name, rows.len(), extract_ms)
                    .await;
            }
            if !options.dry_run {
                let load_start = Instant::now();
                dest.write_batch(
                    &schema.name,
                    schema,
                    &batch.frame,
                    WriteMode::Append,
                    Some(&table.primary_key),
                )
                .await?;
                if let Some(stats) = options.stats {
                    let load_ms = load_start.elapsed().as_millis() as u64;
                    stats
                        .record_load(&table.name, rows.len(), 0, 0, load_ms)
                        .await;
                }
            }

            let last_value: String = rows
                .last()
                .and_then(|row| row.try_get(pk_alias).ok())
                .context("missing primary key value for keyset pagination")?;
            last_pk = Some(last_value);
            checkpoint.last_synced_at = Some(Utc::now());
            if let Some(state_handle) = &options.state_handle {
                state_handle
                    .save_postgres_checkpoint(&table.name, checkpoint)
                    .await?;
            }
        }
        Ok(())
    }

    async fn run_incremental(
        &self,
        table: &ResolvedPostgresTable,
        schema: &TableSchema,
        dest: &dyn Destination,
        checkpoint: &mut TableCheckpoint,
        options: &TableRunOptions<'_>,
    ) -> Result<()> {
        let updated_at = table
            .updated_at_column
            .as_ref()
            .context("updated_at_column required for incremental sync")?;
        let (schema_name, table_name) = split_table_name(&table.name);
        let pk_cast = self
            .resolve_primary_key_cast(&schema_name, &table_name, &table.primary_key)
            .await?;

        if checkpoint.last_synced_at.is_some() && checkpoint.last_primary_key.is_none() {
            warn!(
                table = %table.name,
                "missing last_primary_key in checkpoint; falling back to updated_at-only paging"
            );
        }

        let mut last_seen = checkpoint
            .last_synced_at
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now));
        let mut last_pk = checkpoint.last_primary_key.clone();

        let select_columns = schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        loop {
            let sql = build_incremental_sql(&IncrementalSqlParts {
                schema: &schema_name,
                table: &table_name,
                columns: &select_columns,
                updated_at,
                primary_key: &table.primary_key,
                pk_cast: &pk_cast,
                where_clause: table.where_clause.as_deref(),
                has_last_pk: last_pk.is_some(),
            });

            let extract_start = Instant::now();
            let rows = if let Some(last_pk_value) = last_pk.as_ref() {
                sqlx::query(&sql)
                    .bind(last_seen)
                    .bind(last_pk_value)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                sqlx::query(&sql)
                    .bind(last_seen)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            };
            let extract_ms = extract_start.elapsed().as_millis() as u64;

            if rows.is_empty() {
                break;
            }

            let batch_synced_at = Utc::now();
            let batch = rows_to_batch(schema, &rows, table, batch_synced_at)?;
            if let Some(stats) = options.stats {
                stats
                    .record_extract(&table.name, rows.len(), extract_ms)
                    .await;
            }
            if !options.dry_run {
                let load_start = Instant::now();
                dest.write_batch(
                    &schema.name,
                    schema,
                    &batch.frame,
                    WriteMode::Upsert,
                    Some(&table.primary_key),
                )
                .await?;
                if let Some(stats) = options.stats {
                    let load_ms = load_start.elapsed().as_millis() as u64;
                    stats
                        .record_load(&table.name, rows.len(), rows.len(), 0, load_ms)
                        .await;
                }
            }

            let last_row = rows
                .last()
                .context("incremental batch empty after non-empty check")?;
            let next_seen = read_updated_at(last_row, updated_at)
                .context("missing updated_at value for incremental paging")?;
            let next_pk = read_primary_key(last_row, &table.primary_key)?;
            last_seen = next_seen;
            last_pk = Some(next_pk.clone());
            checkpoint.last_synced_at = Some(last_seen);
            checkpoint.last_primary_key = Some(next_pk);
            if let Some(state_handle) = &options.state_handle {
                state_handle
                    .save_postgres_checkpoint(&table.name, checkpoint)
                    .await?;
            }
        }
        Ok(())
    }

    async fn build_pg_connection_config(&self) -> Result<PgConnectionConfig> {
        let url = Url::parse(&self.config.url).context("invalid postgres url")?;
        let host = url.host_str().context("postgres url missing host")?;
        let username = url.username();
        if username.is_empty() {
            anyhow::bail!("postgres url missing username");
        }
        let db_name = url.path().trim_start_matches('/');
        if db_name.is_empty() {
            anyhow::bail!("postgres url missing database name");
        }
        let port = url.port().unwrap_or(5432);
        let password = url
            .password()
            .map(|pw| SecretString::new(pw.to_string().into()));

        let tls = if self.config.cdc_tls.unwrap_or(false) {
            let pem = if let Some(raw) = &self.config.cdc_tls_ca {
                raw.clone()
            } else if let Some(path) = &self.config.cdc_tls_ca_path {
                tokio::fs::read_to_string(path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?
            } else {
                String::new()
            };
            TlsConfig {
                trusted_root_certs: pem,
                enabled: true,
            }
        } else {
            TlsConfig::disabled()
        };

        Ok(PgConnectionConfig {
            host: host.to_string(),
            port,
            name: db_name.to_string(),
            username: username.to_string(),
            password,
            tls,
            keepalive: None,
        })
    }

    async fn resolve_table_ids(
        &self,
        tables: &[ResolvedPostgresTable],
    ) -> Result<HashMap<TableId, ResolvedPostgresTable>> {
        let mut table_ids = HashMap::new();
        for table in tables {
            let (schema_name, table_name) = split_table_name(&table.name);
            let oid: Option<i32> = sqlx::query_scalar(
                r#"
                select c.oid::int
                from pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                where n.nspname = $1 and c.relname = $2
                "#,
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await?;
            let oid = oid.context(format!("table {} not found", table.name))?;
            table_ids.insert(TableId::new(oid as u32), table.clone());
        }
        Ok(table_ids)
    }

    async fn validate_wal_level(&self) -> Result<()> {
        let wal_level: String = sqlx::query_scalar("show wal_level")
            .fetch_one(&self.pool)
            .await
            .context("checking wal_level")?;
        if wal_level != "logical" {
            anyhow::bail!("wal_level must be logical for CDC (found '{}')", wal_level);
        }
        Ok(())
    }

    async fn validate_publication_tables(
        &self,
        replication_client: &PgReplicationClient,
        publication: &str,
        tables: &[ResolvedPostgresTable],
    ) -> Result<()> {
        let publication_tables = replication_client
            .get_publication_table_names(publication)
            .await?;
        let publication_set: HashSet<String> = publication_tables
            .into_iter()
            .map(|t| format!("{}.{}", t.schema, t.name))
            .collect();

        for table in tables {
            if !publication_set.contains(&table.name) {
                anyhow::bail!(
                    "table {} not found in publication {}",
                    table.name,
                    publication
                );
            }
        }
        Ok(())
    }

    async fn validate_publication_filters(
        &self,
        publication: &str,
        tables: &[ResolvedPostgresTable],
        verbose: bool,
    ) -> Result<()> {
        let filters = self.load_publication_filters(publication).await?;
        for table in tables {
            if let Some(where_clause) = &table.where_clause {
                let actual = filters.get(&table.name).and_then(|v| v.as_ref());
                let actual = actual.context(format!(
                    "publication {} missing row filter for table {}",
                    publication, table.name
                ))?;
                let expected_norm = normalize_filter(where_clause);
                let actual_norm = normalize_filter(actual);
                if expected_norm != actual_norm {
                    if verbose {
                        warn!(
                            table = %table.name,
                            expected = %where_clause,
                            actual = %actual,
                            "publication row filter mismatch"
                        );
                    }
                    anyhow::bail!(
                        "publication {} row filter mismatch for {} (expected `{}`, got `{}`)",
                        publication,
                        table.name,
                        where_clause,
                        actual
                    );
                }
            }
        }
        Ok(())
    }

    async fn load_publication_filters(
        &self,
        publication: &str,
    ) -> Result<HashMap<String, Option<String>>> {
        let rows = sqlx::query(
            r#"
            select n.nspname as schema_name,
                   c.relname as table_name,
                   pg_get_expr(pr.prqual, pr.prrelid) as row_filter
            from pg_publication_rel pr
            join pg_publication p on pr.prpubid = p.oid
            join pg_class c on pr.prrelid = c.oid
            join pg_namespace n on c.relnamespace = n.oid
            where p.pubname = $1
            "#,
        )
        .bind(publication)
        .fetch_all(&self.pool)
        .await?;

        let mut filters = HashMap::new();
        for row in rows {
            let schema: String = row.try_get("schema_name")?;
            let table: String = row.try_get("table_name")?;
            let filter: Option<String> = row.try_get("row_filter")?;
            filters.insert(format!("{}.{}", schema, table), filter);
        }
        Ok(filters)
    }

    async fn load_etl_table_schema(&self, table_id: TableId) -> Result<EtlTableSchema> {
        let row = sqlx::query(
            r#"
            select n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = $1
            "#,
        )
        .bind(table_id.into_inner() as i32)
        .fetch_one(&self.pool)
        .await?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_name: String = row.try_get("table_name")?;

        let columns = sqlx::query(
            r#"
            select a.attname as column_name,
                   a.atttypid::int as type_oid,
                   a.atttypmod as type_modifier,
                   a.attnotnull as not_null,
                   coalesce(i.indisprimary, false) as is_primary
            from pg_attribute a
            left join pg_index i
              on i.indrelid = a.attrelid
             and a.attnum = any(i.indkey)
             and i.indisprimary
            where a.attrelid = $1
              and a.attnum > 0
              and not a.attisdropped
            order by a.attnum
            "#,
        )
        .bind(table_id.into_inner() as i32)
        .fetch_all(&self.pool)
        .await?;

        let mut column_schemas = Vec::with_capacity(columns.len());
        for row in columns {
            let name: String = row.try_get("column_name")?;
            let type_oid: i32 = row.try_get("type_oid")?;
            let type_modifier: i32 = row.try_get("type_modifier")?;
            let not_null: bool = row.try_get("not_null")?;
            let is_primary: bool = row.try_get("is_primary")?;
            let typ = etl_postgres::types::convert_type_oid_to_type(type_oid as u32);
            column_schemas.push(etl_postgres::types::ColumnSchema::new(
                name,
                typ,
                type_modifier,
                !not_null,
                is_primary,
            ));
        }

        let table_name = etl_postgres::types::TableName::new(schema_name, table_name);
        Ok(EtlTableSchema::new(table_id, table_name, column_schemas))
    }

    async fn stream_cdc_changes(
        &self,
        replication_client: PgReplicationClient,
        config: CdcStreamConfig<'_>,
        runtime: CdcStreamRuntime<'_>,
    ) -> Result<etl::types::PgLsn> {
        let CdcStreamConfig {
            publication,
            slot_name,
            start_lsn,
            pipeline_id,
            idle_timeout,
            max_pending_events,
            follow,
            shutdown,
        } = config;
        let include_tables = runtime.include_tables;
        let table_configs = runtime.table_configs;
        let store = runtime.store;
        let dest = runtime.dest;
        let table_info_map = runtime.table_info_map;
        let etl_schemas = runtime.etl_schemas;
        let table_hashes = runtime.table_hashes;
        let table_snapshots = runtime.table_snapshots;
        let state = runtime.state;
        let schema_policy = runtime.schema_policy;
        let schema_diff_enabled = runtime.schema_diff_enabled;
        let stats = runtime.stats;
        let state_handle = runtime.state_handle;
        let logical_stream = replication_client
            .start_logical_replication(publication, slot_name, start_lsn)
            .await?;
        let stream = EventsStream::wrap(logical_stream, pipeline_id);
        tokio::pin!(stream);

        let mut pending_events: Vec<Event> = Vec::with_capacity(max_pending_events.min(1024));
        let mut pending_stats: HashMap<TableId, usize> = HashMap::new();
        let mut last_received_lsn = start_lsn;
        let mut last_flushed_lsn = start_lsn;
        let mut in_tx = false;
        let mut expected_commit_lsn: Option<etl::types::PgLsn> = None;
        let mut shutdown = shutdown;
        let mut shutdown_requested = false;

        loop {
            if shutdown_requested && !in_tx {
                break;
            }

            let message = if let Some(shutdown) = shutdown.as_mut() {
                tokio::select! {
                    changed = shutdown.changed(), if !shutdown_requested => {
                        if changed {
                            shutdown_requested = true;
                            if in_tx {
                                continue;
                            }
                            break;
                        }
                        continue;
                    }
                    result = timeout(idle_timeout, stream.next()) => {
                        match result {
                            Ok(Some(msg)) => msg?,
                            Ok(None) => break,
                            Err(_) => {
                                if in_tx {
                                    continue;
                                }
                                if follow {
                                    continue;
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                match timeout(idle_timeout, stream.next()).await {
                    Ok(Some(msg)) => msg?,
                    Ok(None) => break,
                    Err(_) => {
                        if in_tx {
                            continue;
                        }
                        if follow {
                            continue;
                        }
                        break;
                    }
                }
            };

            match message {
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    let wal_end = etl::types::PgLsn::from(keepalive.wal_end());
                    if wal_end > last_received_lsn {
                        last_received_lsn = wal_end;
                    }
                    if keepalive.reply() == 1 {
                        stream
                            .as_mut()
                            .send_status_update(
                                last_received_lsn,
                                last_flushed_lsn,
                                true,
                                StatusUpdateType::KeepAlive,
                            )
                            .await?;
                    }
                }
                ReplicationMessage::XLogData(xlog) => {
                    let start = etl::types::PgLsn::from(xlog.wal_start());
                    let end = etl::types::PgLsn::from(xlog.wal_end());
                    if end > last_received_lsn {
                        last_received_lsn = end;
                    }

                    match xlog.data() {
                        LogicalReplicationMessage::Begin(begin) => {
                            in_tx = true;
                            expected_commit_lsn = Some(etl::types::PgLsn::from(begin.final_lsn()));
                            pending_events.clear();
                            pending_stats.clear();
                        }
                        LogicalReplicationMessage::Commit(commit) => {
                            let commit_lsn = etl::types::PgLsn::from(commit.commit_lsn());
                            if let Some(expected) = expected_commit_lsn.take()
                                && expected != commit_lsn
                            {
                                warn!(
                                    expected = %expected,
                                    actual = %commit_lsn,
                                    "commit lsn mismatch"
                                );
                            }

                            if !pending_events.is_empty() {
                                dest.write_events(std::mem::take(&mut pending_events))
                                    .await?;
                            }
                            if let Some(stats) = &stats {
                                for (table_id, count) in pending_stats.drain() {
                                    if let Some(cfg) = table_configs.get(&table_id) {
                                        stats.record_extract(&cfg.name, count, 0).await;
                                    }
                                }
                            } else {
                                pending_stats.clear();
                            }

                            last_flushed_lsn = commit_lsn;
                            stream
                                .as_mut()
                                .send_status_update(
                                    last_received_lsn,
                                    last_flushed_lsn,
                                    true,
                                    StatusUpdateType::KeepAlive,
                                )
                                .await?;
                            let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
                            cdc_state.last_lsn = Some(last_flushed_lsn.to_string());
                            if let Some(state_handle) = &state_handle {
                                state_handle.save_postgres_cdc_state(cdc_state).await?;
                            }
                            in_tx = false;
                        }
                        LogicalReplicationMessage::Relation(relation) => {
                            let table_id = TableId::new(relation.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            let mut relation_runtime = CdcRelationRuntime {
                                table_configs,
                                store,
                                dest,
                                table_info_map: &mut *table_info_map,
                                etl_schemas: &mut *etl_schemas,
                                table_hashes: &mut *table_hashes,
                                table_snapshots: &mut *table_snapshots,
                                state: &mut *state,
                                schema_policy: schema_policy.clone(),
                                schema_diff_enabled,
                                state_handle: state_handle.clone(),
                            };
                            self.handle_relation_change(table_id, &mut relation_runtime)
                                .await?;
                        }
                        LogicalReplicationMessage::Insert(insert) => {
                            let table_id = TableId::new(insert.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let schema = store
                                .get_table_schema(&table_id)
                                .await?
                                .context("missing schema for insert")?;
                            let table_row = tuple_to_row(
                                &schema.column_schemas,
                                insert.tuple().tuple_data(),
                                None,
                            )?;
                            let event = InsertEvent {
                                start_lsn: start,
                                commit_lsn,
                                table_id,
                                table_row,
                            };
                            pending_events.push(Event::Insert(event));
                        }
                        LogicalReplicationMessage::Update(update) => {
                            let table_id = TableId::new(update.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let schema = store
                                .get_table_schema(&table_id)
                                .await?
                                .context("missing schema for update")?;
                            let is_key = update.old_tuple().is_none();
                            let old_tuple = update.old_tuple().or(update.key_tuple());
                            let old_table_row = old_tuple
                                .map(|identity| {
                                    tuple_to_row(
                                        &schema.column_schemas,
                                        identity.tuple_data(),
                                        None,
                                    )
                                    .map(|row| (is_key, row))
                                })
                                .transpose()?;
                            let table_row = tuple_to_row(
                                &schema.column_schemas,
                                update.new_tuple().tuple_data(),
                                old_table_row.as_ref().map(|(_, row)| row),
                            )?;
                            let event = UpdateEvent {
                                start_lsn: start,
                                commit_lsn,
                                table_id,
                                table_row,
                                old_table_row,
                            };
                            pending_events.push(Event::Update(event));
                        }
                        LogicalReplicationMessage::Delete(delete) => {
                            let table_id = TableId::new(delete.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let schema = store
                                .get_table_schema(&table_id)
                                .await?
                                .context("missing schema for delete")?;
                            let is_key = delete.old_tuple().is_none();
                            let old_tuple = delete.old_tuple().or(delete.key_tuple());
                            let old_table_row = old_tuple
                                .map(|identity| {
                                    tuple_to_row(
                                        &schema.column_schemas,
                                        identity.tuple_data(),
                                        None,
                                    )
                                    .map(|row| (is_key, row))
                                })
                                .transpose()?;
                            let event = DeleteEvent {
                                start_lsn: start,
                                commit_lsn,
                                table_id,
                                old_table_row,
                            };
                            pending_events.push(Event::Delete(event));
                        }
                        LogicalReplicationMessage::Truncate(truncate) => {
                            let rel_ids = truncate.rel_ids().to_vec();
                            if !rel_ids
                                .iter()
                                .any(|id| include_tables.contains(&TableId::new(*id)))
                            {
                                continue;
                            }
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let event = TruncateEvent {
                                start_lsn: start,
                                commit_lsn,
                                options: truncate.options(),
                                rel_ids,
                            };
                            pending_events.push(Event::Truncate(event));
                        }
                        LogicalReplicationMessage::Origin(_)
                        | LogicalReplicationMessage::Type(_) => {}
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        Ok(last_flushed_lsn)
    }

    async fn handle_relation_change(
        &self,
        table_id: TableId,
        runtime: &mut CdcRelationRuntime<'_>,
    ) -> Result<()> {
        let table_cfg = runtime
            .table_configs
            .get(&table_id)
            .context("relation for unknown table")?;

        let etl_schema = self.load_etl_table_schema(table_id).await?;
        runtime.store.store_table_schema(etl_schema.clone()).await?;
        let info = cdc_table_info_from_schema(table_cfg, &etl_schema)?;
        let new_hash = schema_fingerprint(&info.schema);
        let prev_snapshot = runtime.table_snapshots.get(&table_id).cloned();
        if let Some(diff) = schema_diff(prev_snapshot.as_deref(), &info.schema)
            && !diff.is_empty()
        {
            if runtime.schema_diff_enabled {
                log_schema_diff(&table_cfg.name, &diff);
            }
            match runtime.schema_policy.clone() {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table_cfg.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    warn!(
                        table = %table_cfg.name,
                        "schema change detected; marking CDC for resync"
                    );
                    if let Some(cdc_state) = runtime.state.postgres_cdc.as_mut() {
                        cdc_state.last_lsn = None;
                    }
                    return Err(anyhow::anyhow!(
                        "schema change detected for {}; resync required",
                        table_cfg.name
                    ));
                }
                SchemaChangePolicy::Auto => {
                    if diff.has_incompatible() {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; set schema_changes=resync or fail",
                            table_cfg.name
                        );
                    }
                    info!(
                        table = %table_cfg.name,
                        "schema change detected; updating destination schema"
                    );
                }
            }
        }

        let snapshot = schema_snapshot_from_schema(&info.schema);
        runtime.dest.ensure_table_schema(&info.schema).await?;
        runtime.dest.update_table_info(info.clone()).await?;
        runtime.table_info_map.insert(table_id, info);
        runtime.etl_schemas.insert(table_id, etl_schema);
        runtime.table_hashes.insert(table_id, new_hash.clone());
        runtime.table_snapshots.insert(table_id, snapshot.clone());

        let entry = runtime
            .state
            .postgres
            .entry(table_cfg.name.clone())
            .or_default();
        entry.schema_hash = Some(new_hash);
        entry.schema_snapshot = Some(snapshot);
        if let Some(state_handle) = &runtime.state_handle {
            state_handle
                .save_postgres_checkpoint(&table_cfg.name, entry)
                .await?;
        }

        Ok(())
    }
}

fn split_table_name(table: &str) -> (String, String) {
    let mut parts = table.split('.');
    let first = parts.next().unwrap_or("public");
    let second = parts.next();
    match second {
        Some(table_name) => (first.to_string(), table_name.to_string()),
        None => ("public".to_string(), first.to_string()),
    }
}

fn normalize_filter(value: &str) -> String {
    let mut trimmed = value.trim();
    while let Some(stripped) = trimmed.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        trimmed = stripped.trim();
    }
    trimmed
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
}

fn schema_fingerprint(schema: &TableSchema) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for column in &schema.columns {
        column.name.hash(&mut hasher);
        column.data_type.hash(&mut hasher);
        column.nullable.hash(&mut hasher);
    }
    schema.primary_key.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

#[derive(Debug, Default)]
struct SchemaDiff {
    added: Vec<String>,
    removed: Vec<String>,
    type_changed: Vec<(String, DataType, DataType)>,
    nullable_changed: Vec<(String, bool, bool)>,
}

impl SchemaDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.type_changed.is_empty()
            && self.nullable_changed.is_empty()
    }

    fn has_incompatible(&self) -> bool {
        !self.removed.is_empty()
            || !self.type_changed.is_empty()
            || !self.nullable_changed.is_empty()
    }
}

fn schema_snapshot_from_schema(schema: &TableSchema) -> Vec<SchemaFieldSnapshot> {
    schema
        .columns
        .iter()
        .map(|col| SchemaFieldSnapshot {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
        })
        .collect()
}

fn schema_diff(
    previous: Option<&[SchemaFieldSnapshot]>,
    current: &TableSchema,
) -> Option<SchemaDiff> {
    let previous = previous?;
    let mut diff = SchemaDiff::default();
    let mut prev_map: HashMap<&str, &SchemaFieldSnapshot> = HashMap::new();
    for col in previous {
        prev_map.insert(col.name.as_str(), col);
    }

    let mut current_names = HashSet::new();
    for col in &current.columns {
        current_names.insert(col.name.as_str());
        if let Some(prev) = prev_map.get(col.name.as_str()) {
            if prev.data_type != col.data_type {
                diff.type_changed.push((
                    col.name.clone(),
                    prev.data_type.clone(),
                    col.data_type.clone(),
                ));
            } else if prev.nullable != col.nullable {
                diff.nullable_changed
                    .push((col.name.clone(), prev.nullable, col.nullable));
            }
        } else {
            diff.added.push(col.name.clone());
        }
    }

    for prev in previous {
        if !current_names.contains(prev.name.as_str()) {
            diff.removed.push(prev.name.clone());
        }
    }

    Some(diff)
}

fn log_schema_diff(table: &str, diff: &SchemaDiff) {
    if !diff.added.is_empty() {
        info!(table = %table, added = ?diff.added, "schema change: added columns");
    }
    if !diff.removed.is_empty() {
        warn!(table = %table, removed = ?diff.removed, "schema change: removed columns");
    }
    if !diff.type_changed.is_empty() {
        warn!(table = %table, type_changed = ?diff.type_changed, "schema change: column type changes");
    }
    if !diff.nullable_changed.is_empty() {
        warn!(
            table = %table,
            nullable_changed = ?diff.nullable_changed,
            "schema change: nullable changes"
        );
    }
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

fn required_columns(table: &ResolvedPostgresTable) -> HashSet<String> {
    let mut required = HashSet::new();
    required.insert(table.primary_key.clone());
    if let Some(updated_at) = &table.updated_at_column {
        required.insert(updated_at.clone());
    }
    if table.soft_delete
        && let Some(column) = &table.soft_delete_column
    {
        required.insert(column.clone());
    }
    required
}

fn ensure_required_columns(all_columns: &[ColumnSchema], required: &HashSet<String>) -> Result<()> {
    for name in required {
        if !all_columns.iter().any(|c| &c.name == name) {
            anyhow::bail!("required column {} not found in source table", name);
        }
    }
    Ok(())
}

fn filter_columns(
    all_columns: &[ColumnSchema],
    selection: &ColumnSelection,
    required: &HashSet<String>,
) -> Vec<ColumnSchema> {
    let include_set: HashSet<&str> = selection.include.iter().map(|s| s.as_str()).collect();
    let exclude_set: HashSet<&str> = selection.exclude.iter().map(|s| s.as_str()).collect();
    let include_all = include_set.is_empty();

    let mut filtered = Vec::new();
    for column in all_columns {
        let mut include = include_all || include_set.contains(column.name.as_str());
        if exclude_set.contains(column.name.as_str()) {
            include = false;
        }
        if required.contains(&column.name) {
            include = true;
        }
        if include {
            filtered.push(column.clone());
        }
    }
    filtered
}

fn pg_type_to_data_type(data_type: &str) -> DataType {
    match data_type {
        "smallint" | "integer" | "bigint" => DataType::Int64,
        "real" | "double precision" => DataType::Float64,
        "numeric" | "decimal" => DataType::Numeric,
        "boolean" => DataType::Bool,
        "timestamp without time zone" | "timestamp with time zone" => DataType::Timestamp,
        "date" => DataType::Date,
        "json" | "jsonb" => DataType::Json,
        "bytea" => DataType::Bytes,
        _ => DataType::String,
    }
}

fn rows_to_batch(
    schema: &TableSchema,
    rows: &[PgRow],
    table: &ResolvedPostgresTable,
    synced_at: DateTime<Utc>,
) -> Result<RowBatch> {
    let polars_schema = polars_schema_with_metadata(schema)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for column in &schema.columns {
            let value = pg_value_to_anyvalue(row, column)?;
            values.push(value);
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));
        let deleted_at = derive_deleted_at(row, table).unwrap_or(Value::Null);
        match deleted_at {
            Value::String(value) => values.push(AnyValue::StringOwned(PlSmallStr::from(value))),
            _ => values.push(AnyValue::Null),
        }
        output.push(PolarsRow::new(values));
    }

    let frame = DataFrame::from_rows_and_schema(&output, &polars_schema)?;
    Ok(RowBatch {
        table: schema.name.clone(),
        schema: schema.clone(),
        frame,
    })
}

fn derive_deleted_at(row: &PgRow, table: &ResolvedPostgresTable) -> Option<Value> {
    if !table.soft_delete {
        return Some(Value::Null);
    }
    let column = table.soft_delete_column.as_ref()?;
    let raw = row.try_get_raw(column.as_str()).ok()?;
    if raw.is_null() {
        return Some(Value::Null);
    }

    if let Ok(flag) = row.try_get::<bool, _>(column.as_str())
        && flag
    {
        return Some(Value::String(Utc::now().to_rfc3339()));
    }

    if let Ok(ts) = row.try_get::<NaiveDateTime, _>(column.as_str()) {
        let dt = Utc.from_utc_datetime(&ts);
        return Some(Value::String(dt.to_rfc3339()));
    }
    if let Ok(ts) = row.try_get::<DateTime<Utc>, _>(column.as_str()) {
        return Some(Value::String(ts.to_rfc3339()));
    }

    if let Ok(date) = row.try_get::<NaiveDate, _>(column.as_str()) {
        return Some(Value::String(date.format("%Y-%m-%d").to_string()));
    }

    Some(Value::Null)
}

fn pg_value_to_anyvalue(row: &PgRow, column: &ColumnSchema) -> Result<AnyValue<'static>> {
    let name = column.name.as_str();
    let value = match column.data_type {
        DataType::String => row
            .try_get::<Option<String>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Int64 => row.try_get::<Option<i64>, _>(name)?.map(AnyValue::Int64),
        DataType::Float64 => row.try_get::<Option<f64>, _>(name)?.map(AnyValue::Float64),
        DataType::Bool => row.try_get::<Option<bool>, _>(name)?.map(AnyValue::Boolean),
        DataType::Timestamp => {
            if let Ok(value) = row.try_get::<Option<NaiveDateTime>, _>(name) {
                value
                    .map(|v| Utc.from_utc_datetime(&v).to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else if let Ok(value) = row.try_get::<Option<DateTime<Utc>>, _>(name) {
                value
                    .map(|v| v.to_rfc3339())
                    .map(|v| AnyValue::StringOwned(PlSmallStr::from(v)))
            } else {
                None
            }
        }
        DataType::Date => row
            .try_get::<Option<NaiveDate>, _>(name)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v))),
        DataType::Bytes => row
            .try_get::<Option<Vec<u8>>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(encode_base64(&v)))),
        DataType::Numeric => row
            .try_get::<Option<bigdecimal::BigDecimal>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v.to_string()))),
        DataType::Json => row
            .try_get::<Option<serde_json::Value>, _>(name)?
            .map(|v| AnyValue::StringOwned(PlSmallStr::from(v.to_string()))),
    };

    Ok(value.unwrap_or(AnyValue::Null))
}

fn read_updated_at(row: &PgRow, updated_at: &str) -> Option<DateTime<Utc>> {
    if let Ok(ts) = row.try_get::<NaiveDateTime, _>(updated_at) {
        return Some(Utc.from_utc_datetime(&ts));
    }
    if let Ok(ts) = row.try_get::<DateTime<Utc>, _>(updated_at) {
        return Some(ts);
    }
    None
}

fn read_primary_key(row: &PgRow, column: &str) -> Result<String> {
    if let Ok(value) = row.try_get::<String, _>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<i32, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<BigDecimal, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<Uuid, _>(column) {
        return Ok(value.to_string());
    }
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Ok(value.to_string());
    }
    let raw = row
        .try_get::<serde_json::Value, _>(column)
        .with_context(|| format!("unable to decode primary key column {}", column))?;
    read_primary_key_from_value(raw)
}

fn build_incremental_sql(parts: &IncrementalSqlParts<'_>) -> String {
    let mut where_clauses = Vec::new();
    if let Some(where_clause) = parts.where_clause {
        where_clauses.push(format!("({})", where_clause));
    }
    let pagination_clause = if parts.has_last_pk {
        format!(
            "({updated_at} > $1 OR ({updated_at} = $1 AND {pk} > $2::{pk_cast}))",
            updated_at = parts.updated_at,
            pk = parts.primary_key,
            pk_cast = parts.pk_cast
        )
    } else {
        format!("{updated_at} > $1", updated_at = parts.updated_at)
    };
    where_clauses.push(pagination_clause);
    let where_sql = format!(" WHERE {}", where_clauses.join(" AND "));
    let limit_placeholder = if parts.has_last_pk { "$3" } else { "$2" };
    format!(
        "SELECT {columns} FROM {schema}.{table}{where_sql} ORDER BY {updated_at} ASC, {pk} ASC LIMIT {limit}",
        columns = parts.columns,
        schema = parts.schema,
        table = parts.table,
        where_sql = where_sql,
        updated_at = parts.updated_at,
        pk = parts.primary_key,
        limit = limit_placeholder
    )
}

fn read_primary_key_from_value(value: Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => anyhow::bail!("primary key value is null"),
        other => Ok(other.to_string()),
    }
}

fn format_cast_type(udt_name: &str, udt_schema: &str) -> String {
    if udt_schema == "pg_catalog" {
        udt_name.to_string()
    } else {
        format!("{}.{}", udt_schema, udt_name)
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn polars_schema_with_metadata(schema: &TableSchema) -> Result<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len() + 2);
    for column in &schema.columns {
        let dtype = match column.data_type {
            DataType::Int64 => PolarsDataType::Int64,
            DataType::Float64 => PolarsDataType::Float64,
            DataType::Bool => PolarsDataType::Boolean,
            _ => PolarsDataType::String,
        };
        fields.push(Field::new(column.name.as_str().into(), dtype));
    }
    fields.push(Field::new(META_SYNCED_AT.into(), PolarsDataType::String));
    fields.push(Field::new(META_DELETED_AT.into(), PolarsDataType::String));
    Ok(Schema::from_iter(fields))
}

fn pg_type_to_data_type_from_type(typ: &etl::types::Type) -> DataType {
    use etl::types::Type;
    match *typ {
        Type::INT2 | Type::INT4 | Type::INT8 => DataType::Int64,
        Type::FLOAT4 | Type::FLOAT8 => DataType::Float64,
        Type::BOOL => DataType::Bool,
        Type::TIMESTAMP | Type::TIMESTAMPTZ => DataType::Timestamp,
        Type::DATE => DataType::Date,
        Type::JSON | Type::JSONB => DataType::Json,
        Type::BYTEA => DataType::Bytes,
        Type::NUMERIC => DataType::Numeric,
        _ => DataType::String,
    }
}

fn cdc_table_info_from_schema(
    table_cfg: &ResolvedPostgresTable,
    etl_schema: &EtlTableSchema,
) -> Result<CdcTableInfo> {
    let source_columns: Vec<ColumnSchema> = etl_schema
        .column_schemas
        .iter()
        .map(|col| ColumnSchema {
            name: col.name.clone(),
            data_type: pg_type_to_data_type_from_type(&col.typ),
            nullable: col.nullable,
        })
        .collect();

    let required = required_columns(table_cfg);
    ensure_required_columns(&source_columns, &required)?;
    let dest_columns = filter_columns(&source_columns, &table_cfg.columns, &required);

    let schema = TableSchema {
        name: crate::types::destination_table_name(&etl_schema.name.to_string()),
        columns: dest_columns,
        primary_key: Some(table_cfg.primary_key.clone()),
    };

    if !schema
        .columns
        .iter()
        .any(|c| c.name == table_cfg.primary_key)
    {
        anyhow::bail!(
            "primary key {} not found in table {}",
            table_cfg.primary_key,
            etl_schema.name
        );
    }

    CdcTableInfo::new(
        crate::destinations::etl_bigquery::CdcTableSpec {
            table_id: etl_schema.id,
            source_name: etl_schema.name.to_string(),
            dest_name: schema.name.clone(),
            schema,
            primary_key: table_cfg.primary_key.clone(),
            soft_delete: table_cfg.soft_delete,
            soft_delete_column: table_cfg.soft_delete_column.clone(),
        },
        etl_schema,
    )
}

fn tuple_to_row(
    column_schemas: &[etl_postgres::types::ColumnSchema],
    tuple_data: &[TupleData],
    old_row: Option<&TableRow>,
) -> Result<TableRow> {
    let mut values = Vec::with_capacity(column_schemas.len());

    for (idx, column_schema) in column_schemas.iter().enumerate() {
        let value = match tuple_data.get(idx) {
            Some(TupleData::Null) => Cell::Null,
            Some(TupleData::UnchangedToast) => old_row
                .and_then(|row| row.values.get(idx).cloned())
                .unwrap_or(Cell::Null),
            Some(TupleData::Text(bytes)) => {
                let text = String::from_utf8_lossy(bytes);
                parse_text_cell(&column_schema.typ, &text)
            }
            Some(TupleData::Binary(bytes)) => Cell::Bytes(bytes.to_vec()),
            None => Cell::Null,
        };
        values.push(value);
    }

    Ok(TableRow::new(values))
}

fn parse_text_cell(typ: &etl::types::Type, text: &str) -> Cell {
    use etl::types::Type;
    match *typ {
        Type::BOOL => match text {
            "t" | "true" | "TRUE" => Cell::Bool(true),
            "f" | "false" | "FALSE" => Cell::Bool(false),
            _ => Cell::String(text.to_string()),
        },
        Type::INT2 | Type::INT4 | Type::INT8 => text
            .parse::<i64>()
            .map(Cell::I64)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        Type::FLOAT4 | Type::FLOAT8 => text
            .parse::<f64>()
            .map(Cell::F64)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        Type::BYTEA => parse_bytea(text)
            .map(Cell::Bytes)
            .unwrap_or_else(|_| Cell::String(text.to_string())),
        _ => Cell::String(text.to_string()),
    }
}

fn parse_bytea(text: &str) -> Result<Vec<u8>> {
    if let Some(hex) = text.strip_prefix("\\x") {
        return hex::decode(hex).context("invalid bytea hex");
    }
    Ok(text.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_pg_types_to_internal_types() {
        assert_eq!(
            pg_type_to_data_type_from_type(&etl::types::Type::INT4),
            DataType::Int64
        );
        assert_eq!(
            pg_type_to_data_type_from_type(&etl::types::Type::FLOAT8),
            DataType::Float64
        );
        assert_eq!(
            pg_type_to_data_type_from_type(&etl::types::Type::BOOL),
            DataType::Bool
        );
        assert_eq!(
            pg_type_to_data_type_from_type(&etl::types::Type::TIMESTAMPTZ),
            DataType::Timestamp
        );
    }

    #[test]
    fn incremental_sql_uses_strict_paging_without_last_pk() {
        let sql = build_incremental_sql(&IncrementalSqlParts {
            schema: "public",
            table: "accounts",
            columns: "id, updated_at",
            updated_at: "updated_at",
            primary_key: "id",
            pk_cast: "bigint",
            where_clause: None,
            has_last_pk: false,
        });
        assert!(sql.contains("updated_at > $1"));
        assert!(!sql.contains(">="));
        assert!(sql.contains("ORDER BY updated_at ASC, id ASC LIMIT $2"));
    }

    #[test]
    fn incremental_sql_uses_tie_breaker_with_last_pk_and_filters() {
        let sql = build_incremental_sql(&IncrementalSqlParts {
            schema: "public",
            table: "accounts",
            columns: "id, updated_at",
            updated_at: "updated_at",
            primary_key: "id",
            pk_cast: "bigint",
            where_clause: Some("tenant_id = 42"),
            has_last_pk: true,
        });
        assert!(sql.contains("(tenant_id = 42)"));
        assert!(sql.contains("(updated_at > $1 OR (updated_at = $1 AND id > $2::bigint))"));
        assert!(sql.contains("ORDER BY updated_at ASC, id ASC LIMIT $3"));
    }

    #[test]
    fn read_primary_key_prefers_string_then_numeric() {
        let value =
            read_primary_key_from_value(Value::String("abc123".to_string())).expect("string pk");
        assert_eq!(value, "abc123");

        let value = read_primary_key_from_value(Value::Number(serde_json::Number::from(42)))
            .expect("numeric pk");
        assert_eq!(value, "42");
    }

    #[test]
    fn read_primary_key_supports_bool_and_null() {
        let value = read_primary_key_from_value(Value::Bool(true)).expect("bool pk");
        assert_eq!(value, "true");

        assert!(read_primary_key_from_value(Value::Null).is_err());
    }

    #[test]
    fn read_primary_key_supports_uuid_and_decimal() {
        let uuid = Uuid::parse_str("2e4b7f22-5a7f-4f94-9a9f-6b1f1c2e0a5b").expect("valid uuid");
        let value = read_primary_key_from_value(Value::String(uuid.to_string())).expect("uuid pk");
        assert_eq!(value, uuid.to_string());

        let decimal = BigDecimal::from(12345i64);
        let value =
            read_primary_key_from_value(Value::String(decimal.to_string())).expect("decimal pk");
        assert_eq!(value, "12345");
    }

    #[test]
    fn primary_key_cast_qualifies_non_pg_schema() {
        assert_eq!(format_cast_type("citext", "public"), "public.citext");
        assert_eq!(format_cast_type("uuid", "pg_catalog"), "uuid");
    }

    #[test]
    fn filter_columns_keeps_required_fields() {
        let all_columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
            ColumnSchema {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
            },
        ];
        let selection = ColumnSelection {
            include: vec!["name".to_string()],
            exclude: vec!["updated_at".to_string()],
        };
        let required: HashSet<String> = ["id".to_string(), "updated_at".to_string()]
            .into_iter()
            .collect();

        let filtered = filter_columns(&all_columns, &selection, &required);
        let names: Vec<&str> = filtered.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["id", "name", "updated_at"]);
    }

    #[test]
    fn schema_diff_detects_incompatible_changes() {
        let previous = vec![
            SchemaFieldSnapshot {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            SchemaFieldSnapshot {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: false,
            },
        ];
        let current = TableSchema {
            name: "public__accounts".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "extra".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };

        let diff = schema_diff(Some(&previous), &current).expect("diff");
        assert!(!diff.is_empty());
        assert!(diff.has_incompatible());
        assert!(diff.added.contains(&"extra".to_string()));
        assert!(diff.removed.contains(&"id".to_string()));
        assert!(diff.type_changed.iter().any(|(name, _, _)| name == "name"));
    }
}
