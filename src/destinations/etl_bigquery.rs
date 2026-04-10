use crate::destinations::bigquery::{
    BigQueryDestination, CdcBatchLoadJobPayload, CdcBatchLoadJobStep,
};
use crate::destinations::{Destination as CdsDestination, WriteMode};
use crate::state::{
    CdcBatchLoadJobRecord, CdcBatchLoadJobStatus, CdcCommitFragmentRecord, CdcCommitFragmentStatus,
    StateHandle,
};
use crate::stats::StatsHandle;
use crate::types::{DataType, MetadataColumns, TableSchema};
use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use etl::destination::Destination as EtlDestination;
use etl::destination::async_result::{
    TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::types::{Cell, Event, TableId, TableRow};
use futures::stream::{FuturesUnordered, StreamExt};
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, oneshot};
use tokio::task;
use tracing::{info, info_span, warn};

mod frame;
mod queue;

use self::frame::*;
use self::queue::CdcApplyReadiness;

#[derive(Clone)]
pub struct EtlBigQueryDestination {
    inner: BigQueryDestination,
    tables: Arc<RwLock<HashMap<TableId, CdcTableInfo>>>,
    stats: Option<StatsHandle>,
    apply_concurrency: usize,
    cdc_batch_load_manager: Option<Arc<CdcBatchLoadManager>>,
}

#[derive(Clone)]
pub struct CdcTableInfo {
    pub table_id: TableId,
    pub source_name: String,
    pub dest_name: String,
    pub schema: TableSchema,
    pub metadata: MetadataColumns,
    pub primary_key: String,
    pub soft_delete: bool,
    dest_source_indices: Vec<usize>,
    source_primary_key_index: usize,
    source_soft_delete_index: Option<usize>,
}

#[derive(Clone)]
pub struct CdcTableSpec {
    pub table_id: TableId,
    pub source_name: String,
    pub dest_name: String,
    pub schema: TableSchema,
    pub metadata: MetadataColumns,
    pub primary_key: String,
    pub soft_delete: bool,
    pub soft_delete_column: Option<String>,
}

struct CdcCommitTableWork {
    table_id: TableId,
    rows: Vec<TableRow>,
    delete_rows: Vec<TableRow>,
    truncate: bool,
}

struct CdcQueuedBatchLoadJob {
    record: CdcBatchLoadJobRecord,
    payload: CdcBatchLoadJobPayload,
}

#[derive(Clone)]
pub(crate) struct CdcCommitFragmentMeta {
    pub(crate) sequence: u64,
    pub(crate) commit_lsn: String,
}

type CdcBatchLoadJobWaiters = HashMap<String, Vec<oneshot::Sender<EtlResult<()>>>>;

#[derive(Clone)]
struct CdcBatchLoadManager {
    inner: BigQueryDestination,
    stats: Option<StatsHandle>,
    state_handle: StateHandle,
    mixed_mode_gate_enabled: Arc<AtomicBool>,
    waiters: Arc<Mutex<CdcBatchLoadJobWaiters>>,
    notify: Arc<Notify>,
    local_retry_retryable_failures: bool,
}

enum PendingTableRowAction {
    Upsert(TableRow),
    Delete(TableRow),
}

const CDC_FRAME_BUILD_BLOCKING_ROWS: usize = 512;
const CDC_BATCH_LOAD_CLAIM_POLL_INTERVAL: Duration = Duration::from_secs(5);
const CDC_BATCH_LOAD_JOB_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const CDC_BATCH_LOAD_JOB_STALE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Copy)]
enum CdcBatchLoadStepKind {
    Upsert,
    Delete,
}

impl CdcBatchLoadStepKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
}

impl CdcTableInfo {
    pub fn new(spec: CdcTableSpec, etl_schema: &etl::types::TableSchema) -> Result<Self> {
        let source_index_by_name: HashMap<String, usize> = etl_schema
            .column_schemas
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.clone(), idx))
            .collect();

        let mut dest_source_indices = Vec::with_capacity(spec.schema.columns.len());
        for column in &spec.schema.columns {
            let idx = source_index_by_name
                .get(&column.name)
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!("column {} not found in source schema", column.name)
                })?;
            dest_source_indices.push(idx);
        }

        let source_primary_key_index = source_index_by_name
            .get(&spec.primary_key)
            .copied()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "primary key {} not found in source schema",
                    spec.primary_key
                )
            })?;

        let source_soft_delete_index = spec
            .soft_delete_column
            .as_ref()
            .and_then(|name| source_index_by_name.get(name).copied());

        Ok(Self {
            table_id: spec.table_id,
            source_name: spec.source_name,
            dest_name: spec.dest_name,
            schema: spec.schema,
            metadata: spec.metadata,
            primary_key: spec.primary_key,
            soft_delete: spec.soft_delete,
            dest_source_indices,
            source_primary_key_index,
            source_soft_delete_index,
        })
    }
}

impl EtlBigQueryDestination {
    pub(crate) fn supports_mixed_mode(&self) -> bool {
        self.cdc_batch_load_manager.is_some()
    }

    pub(crate) fn set_mixed_mode_gate_enabled(&self, enabled: bool) {
        if let Some(manager) = &self.cdc_batch_load_manager {
            manager.set_mixed_mode_gate_enabled(enabled);
        }
    }

    pub async fn new(
        inner: BigQueryDestination,
        tables: HashMap<TableId, CdcTableInfo>,
        stats: Option<StatsHandle>,
        apply_concurrency: usize,
        cdc_batch_load_worker_count: usize,
        state_handle: Option<StateHandle>,
        local_retry_retryable_failures: bool,
    ) -> Result<Self> {
        let cdc_batch_load_manager = if inner.cdc_batch_load_queue_enabled() {
            if let Some(state_handle) = state_handle {
                Some(Arc::new(
                    CdcBatchLoadManager::new(
                        inner.clone(),
                        stats.clone(),
                        state_handle,
                        cdc_batch_load_worker_count.max(1),
                        local_retry_retryable_failures,
                    )
                    .await?,
                ))
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self {
            inner,
            tables: Arc::new(RwLock::new(tables)),
            stats,
            apply_concurrency: apply_concurrency.max(1),
            cdc_batch_load_manager,
        })
    }

    async fn get_table(&self, table_id: TableId) -> EtlResult<CdcTableInfo> {
        let guard = self.tables.read().await;
        guard
            .get(&table_id)
            .cloned()
            .ok_or_else(|| etl::etl_error!(ErrorKind::MissingTableSchema, "table not configured"))
    }

    pub async fn update_table_info(&self, info: CdcTableInfo) -> EtlResult<()> {
        let mut guard = self.tables.write().await;
        guard.insert(info.table_id, info);
        Ok(())
    }

    async fn ensure_table(&self, info: &CdcTableInfo) -> EtlResult<()> {
        self.ensure_table_schema(&info.schema).await
    }

    pub async fn ensure_table_schema(&self, schema: &TableSchema) -> EtlResult<()> {
        self.inner.ensure_table(schema).await.map_err(|err| {
            etl::etl_error!(
                ErrorKind::DestinationError,
                "failed to ensure BigQuery table",
                err.to_string()
            )
        })
    }

    async fn write_rows(
        &self,
        info: &CdcTableInfo,
        rows: Vec<TableRow>,
        mode: WriteMode,
        synced_at: DateTime<Utc>,
        deleted_at_override: Option<DateTime<Utc>>,
    ) -> EtlResult<()> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(());
        }
        let ensure_started_at = Instant::now();
        info!(
            table = %info.source_name,
            destination_table = %info.dest_name,
            rows = row_count,
            mode = ?mode,
            "ensuring destination table before CDC write"
        );
        self.ensure_table(info).await?;
        info!(
            table = %info.source_name,
            destination_table = %info.dest_name,
            rows = row_count,
            ensure_ms = ensure_started_at.elapsed().as_millis() as u64,
            "destination table ensured before CDC write"
        );
        let frame = build_cdc_frame(info.clone(), rows, synced_at, deleted_at_override).await?;
        let load_start = Instant::now();
        self.inner
            .write_batch(
                &info.dest_name,
                &info.schema,
                &frame,
                mode,
                Some(info.primary_key.as_str()),
            )
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to write CDC batch",
                    err.to_string()
                )
            })?;
        if let Some(stats) = &self.stats {
            let deleted = if deleted_at_override.is_some() {
                row_count
            } else {
                0
            };
            let upserted = if matches!(mode, WriteMode::Upsert) {
                row_count
            } else {
                0
            };
            stats
                .record_load(
                    &info.source_name,
                    row_count,
                    upserted,
                    deleted,
                    load_start.elapsed().as_millis() as u64,
                )
                .await;
        }
        Ok(())
    }

    async fn flush_table_work(
        &self,
        work: CdcCommitTableWork,
        synced_at: DateTime<Utc>,
        delete_synced_at: DateTime<Utc>,
    ) -> EtlResult<()> {
        let info = self.get_table(work.table_id).await?;
        self.apply_table_work(&info, work, synced_at, delete_synced_at)
            .await
    }

    async fn apply_table_work(
        &self,
        info: &CdcTableInfo,
        work: CdcCommitTableWork,
        synced_at: DateTime<Utc>,
        delete_synced_at: DateTime<Utc>,
    ) -> EtlResult<()> {
        if work.truncate
            && let Err(err) = self.inner.truncate_table(&info.dest_name).await
        {
            return Err(etl::etl_error!(
                ErrorKind::DestinationError,
                "failed to truncate table from CDC event",
                err.to_string()
            ));
        }

        if !work.rows.is_empty() {
            self.write_rows(info, work.rows, WriteMode::Upsert, synced_at, None)
                .await?;
        }

        if !work.delete_rows.is_empty() {
            self.write_rows(
                info,
                work.delete_rows,
                WriteMode::Upsert,
                delete_synced_at,
                Some(delete_synced_at),
            )
            .await?;
        }

        Ok(())
    }

    async fn flush_pending(
        &self,
        pending: &mut HashMap<TableId, Vec<TableRow>>,
        pending_deletes: &mut HashMap<TableId, Vec<TableRow>>,
        truncate_tables: &mut Vec<TableId>,
    ) -> EtlResult<()> {
        let synced_at = Utc::now();
        let delete_synced_at = synced_at;
        let mut work_by_table: HashMap<TableId, CdcCommitTableWork> = HashMap::new();

        for table_id in truncate_tables.drain(..) {
            work_by_table
                .entry(table_id)
                .or_insert_with(|| CdcCommitTableWork {
                    table_id,
                    rows: Vec::new(),
                    delete_rows: Vec::new(),
                    truncate: false,
                })
                .truncate = true;
        }

        for (table_id, rows) in pending.drain() {
            work_by_table
                .entry(table_id)
                .or_insert_with(|| CdcCommitTableWork {
                    table_id,
                    rows: Vec::new(),
                    delete_rows: Vec::new(),
                    truncate: false,
                })
                .rows = rows;
        }

        for (table_id, rows) in pending_deletes.drain() {
            work_by_table
                .entry(table_id)
                .or_insert_with(|| CdcCommitTableWork {
                    table_id,
                    rows: Vec::new(),
                    delete_rows: Vec::new(),
                    truncate: false,
                })
                .delete_rows = rows;
        }

        let semaphore = Arc::new(Semaphore::new(self.apply_concurrency));
        let mut tasks = FuturesUnordered::new();
        for work in work_by_table.into_values() {
            let permit_pool = Arc::clone(&semaphore);
            let dest = self.clone();
            tasks.push(async move {
                let permit = permit_pool.acquire_owned().await.map_err(|_| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to acquire CDC apply concurrency permit"
                    )
                })?;
                let _permit = permit;
                dest.flush_table_work(work, synced_at, delete_synced_at)
                    .await
            });
        }

        while let Some(result) = tasks.next().await {
            result?;
        }

        Ok(())
    }

    pub async fn write_table_events(&self, table_id: TableId, events: Vec<Event>) -> EtlResult<()> {
        let info = self.get_table(table_id).await?;
        let work = compact_table_events(&info, table_id, events)?;

        let synced_at = Utc::now();
        self.apply_table_work(&info, work, synced_at, synced_at)
            .await
    }

    pub(crate) async fn dispatch_table_events(
        &self,
        table_id: TableId,
        events: Vec<Event>,
        fragments: Vec<CdcCommitFragmentMeta>,
    ) -> EtlResult<crate::sources::postgres::CdcTableApplyExecution> {
        let info = self.get_table(table_id).await?;
        let work = compact_table_events(&info, table_id, events)?;
        let synced_at = Utc::now();
        let delete_synced_at = synced_at;

        if let Some(manager) = &self.cdc_batch_load_manager {
            let payload = self
                .prepare_cdc_batch_load_job(
                    &info,
                    work,
                    synced_at,
                    delete_synced_at,
                    manager.state_handle.connection_id(),
                    &fragments,
                )
                .await?;
            let first_sequence = fragments.first().map_or(0, |fragment| fragment.sequence);
            let rx = manager.enqueue(first_sequence, payload, fragments).await?;
            if queue::should_check_table_apply_readiness(manager.mixed_mode_gate_enabled()) {
                let readiness = manager
                    .table_apply_readiness(&info.source_name)
                    .await
                    .map_err(|err| {
                        etl::etl_error!(
                            ErrorKind::DestinationError,
                            "failed to inspect CDC table readiness",
                            err.to_string()
                        )
                    })?;
                if !matches!(readiness, CdcApplyReadiness::Ready) {
                    return Ok(crate::sources::postgres::CdcTableApplyExecution::Immediate);
                }
            }
            return Ok(crate::sources::postgres::CdcTableApplyExecution::Deferred(
                rx,
            ));
        }

        self.apply_table_work(&info, work, synced_at, delete_synced_at)
            .await?;
        Ok(crate::sources::postgres::CdcTableApplyExecution::Immediate)
    }
}

impl EtlDestination for EtlBigQueryDestination {
    fn name() -> &'static str {
        "cdsync_bigquery"
    }

    async fn truncate_table(
        &self,
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        let result = async {
            let info = self.get_table(table_id).await?;
            self.inner
                .truncate_table(&info.dest_name)
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to truncate table",
                        err.to_string()
                    )
                })
        }
        .await;

        async_result.send(result);
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let result = async {
            let info = self.get_table(table_id).await?;
            let synced_at = Utc::now();
            self.write_rows(&info, table_rows, WriteMode::Append, synced_at, None)
                .await
        }
        .await;

        async_result.send(result);
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let result = async {
            let mut pending: HashMap<TableId, Vec<TableRow>> = HashMap::new();
            let mut pending_deletes: HashMap<TableId, Vec<TableRow>> = HashMap::new();
            let mut truncate_tables: Vec<TableId> = Vec::new();

            for event in events {
                match event {
                    Event::Insert(insert_event) => {
                        if let Ok(info) = self.get_table(insert_event.table_id).await {
                            pending
                                .entry(info.table_id)
                                .or_default()
                                .push(insert_event.table_row);
                        }
                    }
                    Event::Update(update_event) => {
                        if let Ok(info) = self.get_table(update_event.table_id).await {
                            pending
                                .entry(info.table_id)
                                .or_default()
                                .push(update_event.table_row);
                        }
                    }
                    Event::Delete(delete_event) => {
                        if let Ok(info) = self.get_table(delete_event.table_id).await {
                            if !info.soft_delete {
                                continue;
                            }
                            if let Some((_, old_row)) = delete_event.old_table_row {
                                pending_deletes
                                    .entry(info.table_id)
                                    .or_default()
                                    .push(old_row);
                            } else {
                                warn!(
                                    table = %info.source_name,
                                    "skipping delete event without primary key"
                                );
                            }
                        }
                    }
                    Event::Truncate(truncate_event) => {
                        for rel_id in truncate_event.rel_ids {
                            truncate_tables.push(TableId::new(rel_id));
                        }
                    }
                    Event::Commit(_) => {
                        self.flush_pending(
                            &mut pending,
                            &mut pending_deletes,
                            &mut truncate_tables,
                        )
                        .await?;
                    }
                    Event::Begin(_) | Event::Relation(_) | Event::Unsupported => {}
                }
            }

            if !pending.is_empty() || !pending_deletes.is_empty() || !truncate_tables.is_empty() {
                self.flush_pending(&mut pending, &mut pending_deletes, &mut truncate_tables)
                    .await?;
            }

            Ok(())
        }
        .await;

        async_result.send(result);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType, TableSchema};
    use chrono::{DateTime, TimeZone, Utc};
    use etl::types::{
        Cell, ColumnSchema as EtlColumnSchema, DeleteEvent, Event, InsertEvent, TableId, TableName,
        TableRow, TruncateEvent, Type, UpdateEvent,
    };

    fn build_info() -> CdcTableInfo {
        let table_id = TableId::new(1);
        let table_name = TableName::new("public".to_string(), "items".to_string());
        let etl_schema = etl::types::TableSchema::new(
            table_id,
            table_name,
            vec![
                EtlColumnSchema::new("id".to_string(), Type::INT8, -1, false, true),
                EtlColumnSchema::new("deleted_at".to_string(), Type::TIMESTAMPTZ, -1, true, false),
            ],
        );

        let schema = TableSchema {
            name: "public__items".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "deleted_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };

        CdcTableInfo::new(
            CdcTableSpec {
                table_id,
                source_name: "public.items".to_string(),
                dest_name: "public__items".to_string(),
                schema,
                metadata: MetadataColumns::default(),
                primary_key: "id".to_string(),
                soft_delete: true,
                soft_delete_column: Some("deleted_at".to_string()),
            },
            &etl_schema,
        )
        .expect("cdc table info")
    }

    #[test]
    fn derive_deleted_at_handles_timestamptz() {
        let info = build_info();
        let ts = Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap();
        let row = TableRow::new(vec![Cell::I64(1), Cell::TimestampTz(ts)]);
        let value = derive_deleted_at_from_row(&info, &row).expect("deleted_at");
        assert_eq!(value, ts.to_rfc3339());
    }

    #[test]
    fn derive_deleted_at_handles_bool_and_null() {
        let info = build_info();
        let row = TableRow::new(vec![Cell::I64(1), Cell::Bool(true)]);
        let value = derive_deleted_at_from_row(&info, &row).expect("deleted_at");
        assert!(DateTime::parse_from_rfc3339(&value).is_ok());

        let row = TableRow::new(vec![Cell::I64(2), Cell::Bool(false)]);
        assert!(derive_deleted_at_from_row(&info, &row).is_none());

        let row = TableRow::new(vec![Cell::I64(3), Cell::Null]);
        assert!(derive_deleted_at_from_row(&info, &row).is_none());
    }

    #[test]
    fn table_rows_to_frame_keeps_interval_values_numeric() {
        let table_id = TableId::new(2);
        let table_name = TableName::new("public".to_string(), "metrics".to_string());
        let etl_schema = etl::types::TableSchema::new(
            table_id,
            table_name,
            vec![EtlColumnSchema::new(
                "elapsed".to_string(),
                Type::INTERVAL,
                -1,
                true,
                false,
            )],
        );

        let schema = TableSchema {
            name: "public__metrics".to_string(),
            columns: vec![ColumnSchema {
                name: "elapsed".to_string(),
                data_type: DataType::Interval,
                nullable: true,
            }],
            primary_key: Some("elapsed".to_string()),
        };

        let info = CdcTableInfo::new(
            CdcTableSpec {
                table_id,
                source_name: "public.metrics".to_string(),
                dest_name: "public__metrics".to_string(),
                schema,
                metadata: MetadataColumns::default(),
                primary_key: "elapsed".to_string(),
                soft_delete: false,
                soft_delete_column: None,
            },
            &etl_schema,
        )
        .expect("cdc table info");

        let frame = table_rows_to_frame(
            &info,
            &[TableRow::new(vec![Cell::F64(42.5)])],
            Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap(),
            None,
        )
        .expect("frame");

        assert_eq!(
            frame.column("elapsed").expect("elapsed").dtype(),
            &PolarsDataType::Float64
        );
    }

    #[test]
    fn compact_table_events_keeps_latest_row_per_primary_key() {
        let info = build_info();
        let table_id = info.table_id;
        let events = vec![
            Event::Insert(InsertEvent {
                start_lsn: etl::types::PgLsn::from(0),
                commit_lsn: etl::types::PgLsn::from(1),
                tx_ordinal: 0,
                table_id,
                table_row: TableRow::new(vec![Cell::I64(1), Cell::Null]),
            }),
            Event::Update(UpdateEvent {
                start_lsn: etl::types::PgLsn::from(2),
                commit_lsn: etl::types::PgLsn::from(3),
                tx_ordinal: 1,
                table_id,
                table_row: TableRow::new(vec![Cell::I64(1), Cell::Bool(true)]),
                old_table_row: None,
            }),
        ];

        let work = compact_table_events(&info, table_id, events).expect("work");
        assert!(!work.truncate);
        assert_eq!(work.rows.len(), 1);
        assert!(work.delete_rows.is_empty());
        assert_eq!(
            work.rows[0],
            TableRow::new(vec![Cell::I64(1), Cell::Bool(true)])
        );
    }

    #[test]
    fn compact_table_events_delete_then_upsert_keeps_latest_live_row() {
        let info = build_info();
        let table_id = info.table_id;
        let events = vec![
            Event::Delete(DeleteEvent {
                start_lsn: etl::types::PgLsn::from(0),
                commit_lsn: etl::types::PgLsn::from(1),
                tx_ordinal: 0,
                table_id,
                old_table_row: Some((false, TableRow::new(vec![Cell::I64(1), Cell::Null]))),
            }),
            Event::Update(UpdateEvent {
                start_lsn: etl::types::PgLsn::from(2),
                commit_lsn: etl::types::PgLsn::from(3),
                tx_ordinal: 1,
                table_id,
                table_row: TableRow::new(vec![Cell::I64(1), Cell::Null]),
                old_table_row: None,
            }),
        ];

        let work = compact_table_events(&info, table_id, events).expect("work");
        assert_eq!(work.rows.len(), 1);
        assert!(work.delete_rows.is_empty());
    }

    #[test]
    fn compact_table_events_truncate_discards_prior_buffered_rows() {
        let info = build_info();
        let table_id = info.table_id;
        let events = vec![
            Event::Insert(InsertEvent {
                start_lsn: etl::types::PgLsn::from(0),
                commit_lsn: etl::types::PgLsn::from(1),
                tx_ordinal: 0,
                table_id,
                table_row: TableRow::new(vec![Cell::I64(1), Cell::Null]),
            }),
            Event::Truncate(TruncateEvent {
                start_lsn: etl::types::PgLsn::from(2),
                commit_lsn: etl::types::PgLsn::from(3),
                tx_ordinal: 1,
                options: 0,
                rel_ids: vec![table_id.into_inner()],
            }),
            Event::Insert(InsertEvent {
                start_lsn: etl::types::PgLsn::from(4),
                commit_lsn: etl::types::PgLsn::from(5),
                tx_ordinal: 2,
                table_id,
                table_row: TableRow::new(vec![Cell::I64(2), Cell::Null]),
            }),
        ];

        let work = compact_table_events(&info, table_id, events).expect("work");
        assert!(work.truncate);
        assert_eq!(
            work.rows,
            vec![TableRow::new(vec![Cell::I64(2), Cell::Null])]
        );
        assert!(work.delete_rows.is_empty());
    }
}
