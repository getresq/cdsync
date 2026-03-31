use crate::config::{
    ColumnSelection, PostgresConfig, PostgresTableConfig, PostgresTableDefaults,
    SchemaChangePolicy, TableSelectionConfig,
};
use crate::destinations::etl_bigquery::{CdcTableInfo, EtlBigQueryDestination};
use crate::destinations::{Destination, WriteMode};
use crate::runner::ShutdownSignal;
use crate::state::{ConnectionState, StateHandle};
use crate::stats::StatsHandle;
use crate::tls::MakeNativeTlsConnect;
use crate::types::{
    ColumnSchema, DataType, MetadataColumns, SchemaFieldSnapshot, SnapshotChunkCheckpoint,
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
use etl::replication::stream::{EventsStream, StatusUpdateType};
use etl::store::both::memory::MemoryStore;
use etl::store::schema::SchemaStore;
use etl::types::{
    Cell, DeleteEvent, Event, InsertEvent, TableId, TableRow, TableSchema as EtlTableSchema,
    TruncateEvent, UpdateEvent,
};
use etl_postgres::replication::slots::EtlReplicationSlot;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use globset::{Glob, GlobSet};
use native_tls::Certificate;
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde_json::Value;
use sqlx::Row as SqlxRow;
use sqlx::ValueRef;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client as TokioPgClient, NoTls, Row as TokioPgRow, SimpleQueryMessage};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

mod cdc_pipeline;
mod cdc_runtime;
mod cdc_sync;
mod convert;
mod schema;
mod snapshot_sync;
mod table_selection;
#[cfg(test)]
mod tests;

use self::cdc_pipeline::*;
use self::convert::*;
use self::schema::*;
use self::table_selection::*;

type CdcApplyFuture = Pin<Box<dyn Future<Output = Result<CdcApplyFragmentAck>> + Send>>;

pub struct PostgresSource {
    config: PostgresConfig,
    metadata: MetadataColumns,
    pool: PgPool,
}

struct ExportedSnapshotSlot {
    client: Option<TokioPgClient>,
}

struct ExportedSnapshotSlotInfo {
    consistent_point: String,
    snapshot_name: String,
}

struct CdcSnapshotTask {
    table: ResolvedPostgresTable,
    info: CdcTableInfo,
    checkpoint_state: Arc<Mutex<TableCheckpoint>>,
    resume_from_primary_key: Option<String>,
    chunk: Option<SnapshotChunkRange>,
    write_mode: WriteMode,
    state_handle: Option<StateHandle>,
}

struct SnapshotCopyContext<'a> {
    dest: &'a dyn Destination,
    snapshot_name: Option<&'a str>,
    batch_size: usize,
    stats: Option<StatsHandle>,
    checkpoint_state: Arc<Mutex<TableCheckpoint>>,
    resume_from_primary_key: Option<String>,
    write_mode: WriteMode,
    state_handle: Option<StateHandle>,
}

const DEFAULT_PG_POOL_MAX: u32 = 5;
const SNAPSHOT_CHUNK_MIN_BATCHES_PER_WORKER: usize = 8;

struct PrimaryKeyTypeInfo {
    cast_type: String,
    udt_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotChunkRange {
    start_pk: i64,
    end_pk: i64,
}

struct SnapshotChunkPlan {
    row_count: i64,
    chunk_ranges: Vec<SnapshotChunkRange>,
}

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
    pub snapshot_concurrency: usize,
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
    apply_concurrency: usize,
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
    pub async fn new(config: PostgresConfig, metadata: MetadataColumns) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_PG_POOL_MAX)
            .connect(&config.url)
            .await
            .context("connecting to postgres")?;
        Ok(Self {
            config,
            metadata,
            pool,
        })
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
        let discovered_names = if self
            .config
            .table_selection
            .as_ref()
            .is_some_and(|selection| !selection.include.is_empty() || !selection.exclude.is_empty())
        {
            self.discover_table_names().await?
        } else {
            Vec::new()
        };
        let table_configs = collect_table_configs(
            self.config.tables.as_deref(),
            self.config.table_selection.as_ref(),
            &discovered_names,
        )?;

        let defaults = self
            .config
            .table_selection
            .as_ref()
            .and_then(|sel| sel.defaults.clone());

        let mut resolved = Vec::with_capacity(table_configs.len());
        for table_cfg in &table_configs {
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

    pub fn cdc_enabled(&self) -> bool {
        self.config.cdc.unwrap_or(true)
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

fn quote_pg_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_pg_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn is_chunkable_snapshot_primary_key(udt_name: &str) -> bool {
    matches!(udt_name, "int2" | "int4" | "int8")
}

fn table_has_snapshot_progress(checkpoint: &TableCheckpoint) -> bool {
    !checkpoint.snapshot_chunks.is_empty()
}

fn snapshot_chunk_filter_sql(
    primary_key_ident: &str,
    pk_cast: &str,
    range: SnapshotChunkRange,
) -> String {
    format!(
        "{pk} >= {start}::{pk_cast} AND {pk} <= {end}::{pk_cast}",
        pk = primary_key_ident,
        start = range.start_pk,
        end = range.end_pk,
        pk_cast = pk_cast
    )
}

fn plan_snapshot_chunk_ranges_from_bounds(
    row_count: i64,
    min_pk: i64,
    max_pk: i64,
    batch_size: usize,
    max_chunks: usize,
) -> Vec<SnapshotChunkRange> {
    if row_count <= 0 || batch_size == 0 || max_chunks <= 1 || min_pk >= max_pk {
        return Vec::new();
    }

    let min_rows_per_worker = batch_size.saturating_mul(SNAPSHOT_CHUNK_MIN_BATCHES_PER_WORKER);
    let total_rows_threshold = min_rows_per_worker.saturating_mul(max_chunks);
    let total_rows_threshold = i64::try_from(total_rows_threshold).unwrap_or(i64::MAX);
    if row_count < total_rows_threshold {
        return Vec::new();
    }

    let span = u128::try_from(i128::from(max_pk) - i128::from(min_pk) + 1).unwrap_or(0);
    if span <= 1 {
        return Vec::new();
    }

    let chunk_count = max_chunks.min(usize::try_from(span).unwrap_or(max_chunks));
    if chunk_count <= 1 {
        return Vec::new();
    }

    let step = span.div_ceil(chunk_count as u128);
    let mut ranges = Vec::with_capacity(chunk_count);
    let mut start = i128::from(min_pk);
    let max_pk_i128 = i128::from(max_pk);

    for chunk_idx in 0..chunk_count {
        let end = if chunk_idx + 1 == chunk_count {
            max_pk_i128
        } else {
            (start + i128::try_from(step).unwrap_or(i128::MAX) - 1).min(max_pk_i128)
        };
        ranges.push(SnapshotChunkRange {
            start_pk: i64::try_from(start).unwrap_or(min_pk),
            end_pk: i64::try_from(end).unwrap_or(max_pk),
        });
        start = end + 1;
    }

    ranges
}

fn snapshot_chunk_checkpoints_from_ranges(
    ranges: &[SnapshotChunkRange],
) -> Vec<SnapshotChunkCheckpoint> {
    if ranges.is_empty() {
        return vec![SnapshotChunkCheckpoint {
            start_primary_key: None,
            end_primary_key: None,
            last_primary_key: None,
            complete: false,
        }];
    }

    ranges
        .iter()
        .map(|range| SnapshotChunkCheckpoint {
            start_primary_key: Some(range.start_pk.to_string()),
            end_primary_key: Some(range.end_pk.to_string()),
            last_primary_key: None,
            complete: false,
        })
        .collect()
}

fn snapshot_chunk_checkpoint_to_range(
    checkpoint: &SnapshotChunkCheckpoint,
) -> Result<Option<SnapshotChunkRange>> {
    match (
        checkpoint.start_primary_key.as_deref(),
        checkpoint.end_primary_key.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(start), Some(end)) => Ok(Some(SnapshotChunkRange {
            start_pk: start
                .parse()
                .with_context(|| format!("parsing snapshot chunk start {}", start))?,
            end_pk: end
                .parse()
                .with_context(|| format!("parsing snapshot chunk end {}", end))?,
        })),
        _ => anyhow::bail!("snapshot chunk checkpoint has mismatched bounds"),
    }
}

fn snapshot_chunk_checkpoint_matches_range(
    checkpoint: &SnapshotChunkCheckpoint,
    range: Option<SnapshotChunkRange>,
) -> bool {
    match range {
        Some(range) => {
            let start = range.start_pk.to_string();
            let end = range.end_pk.to_string();
            checkpoint.start_primary_key.as_deref() == Some(start.as_str())
                && checkpoint.end_primary_key.as_deref() == Some(end.as_str())
        }
        None => checkpoint.start_primary_key.is_none() && checkpoint.end_primary_key.is_none(),
    }
}

fn snapshot_resume_tasks_from_checkpoint(
    checkpoint: &TableCheckpoint,
) -> Result<Vec<(Option<SnapshotChunkRange>, Option<String>)>> {
    checkpoint
        .snapshot_chunks
        .iter()
        .filter(|chunk| !chunk.complete)
        .map(|chunk| {
            Ok((
                snapshot_chunk_checkpoint_to_range(chunk)?,
                chunk.last_primary_key.clone(),
            ))
        })
        .collect()
}

fn update_snapshot_chunk_checkpoint(
    checkpoint: &mut TableCheckpoint,
    range: Option<SnapshotChunkRange>,
    last_primary_key: Option<String>,
    complete: bool,
) -> Result<()> {
    let chunk = checkpoint
        .snapshot_chunks
        .iter_mut()
        .find(|chunk| snapshot_chunk_checkpoint_matches_range(chunk, range))
        .context("missing snapshot chunk checkpoint for progress update")?;
    chunk.last_primary_key = last_primary_key;
    chunk.complete = complete;
    Ok(())
}

fn build_select_columns(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|column| {
            let ident = quote_pg_identifier(&column.name);
            match column.data_type {
                DataType::String | DataType::Numeric | DataType::Json => {
                    format!("{ident}::text as {ident}")
                }
                _ => ident,
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}
