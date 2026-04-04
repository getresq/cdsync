use crate::destinations::bigquery::{
    BigQueryDestination, CdcBatchLoadJobPayload, CdcBatchLoadJobStep,
};
use crate::destinations::{Destination as CdsDestination, WriteMode};
use crate::state::{CdcBatchLoadJobRecord, CdcBatchLoadJobStatus, StateHandle};
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
use std::collections::HashMap;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock, Semaphore, oneshot};
use tokio::task;
use tracing::{info, info_span, warn};
use uuid::Uuid;

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

#[derive(Default)]
struct CdcBatchLoadSchedulerState {
    active_tables: HashSet<String>,
    queued_by_table: HashMap<String, VecDeque<CdcQueuedBatchLoadJob>>,
}

#[derive(Clone)]
struct CdcBatchLoadManager {
    inner: BigQueryDestination,
    stats: Option<StatsHandle>,
    state_handle: StateHandle,
    scheduler: Arc<Mutex<CdcBatchLoadSchedulerState>>,
    waiters: Arc<Mutex<HashMap<String, oneshot::Sender<EtlResult<()>>>>>,
}

enum PendingTableRowAction {
    Upsert(TableRow),
    Delete(TableRow),
}

const CDC_FRAME_BUILD_BLOCKING_ROWS: usize = 512;

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
    pub async fn new(
        inner: BigQueryDestination,
        tables: HashMap<TableId, CdcTableInfo>,
        stats: Option<StatsHandle>,
        apply_concurrency: usize,
        state_handle: Option<StateHandle>,
    ) -> Result<Self> {
        let cdc_batch_load_manager = if inner.cdc_batch_load_queue_enabled() {
            if let Some(state_handle) = state_handle {
                Some(Arc::new(
                    CdcBatchLoadManager::new(inner.clone(), stats.clone(), state_handle).await?,
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
        first_sequence: u64,
    ) -> EtlResult<crate::sources::postgres::CdcTableApplyExecution> {
        let info = self.get_table(table_id).await?;
        let work = compact_table_events(&info, table_id, events)?;
        let synced_at = Utc::now();
        let delete_synced_at = synced_at;

        if let Some(manager) = &self.cdc_batch_load_manager {
            let payload = self
                .prepare_cdc_batch_load_job(&info, work, synced_at, delete_synced_at)
                .await?;
            let rx = manager.enqueue(first_sequence, payload).await?;
            return Ok(crate::sources::postgres::CdcTableApplyExecution::Deferred(
                rx,
            ));
        }

        self.apply_table_work(&info, work, synced_at, delete_synced_at)
            .await?;
        Ok(crate::sources::postgres::CdcTableApplyExecution::Immediate)
    }
}

impl EtlBigQueryDestination {
    async fn prepare_cdc_batch_load_job(
        &self,
        info: &CdcTableInfo,
        work: CdcCommitTableWork,
        synced_at: DateTime<Utc>,
        delete_synced_at: DateTime<Utc>,
    ) -> EtlResult<CdcBatchLoadJobPayload> {
        let mut steps = Vec::new();

        if !work.rows.is_empty() {
            let row_count = work.rows.len();
            let frame = build_cdc_frame(info.clone(), work.rows, synced_at, None).await?;
            let staging_table = super::bigquery::upsert_staging_table_id(&info.dest_name);
            let object_uri = self
                .inner
                .upload_cdc_batch_load_artifact(&staging_table, &info.schema, &frame)
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to upload CDC batch-load artifact",
                        err.to_string()
                    )
                })?;
            steps.push(CdcBatchLoadJobStep {
                staging_table,
                object_uri,
                row_count,
                upserted_count: row_count,
                deleted_count: 0,
            });
        }

        if !work.delete_rows.is_empty() {
            let row_count = work.delete_rows.len();
            let frame = build_cdc_frame(
                info.clone(),
                work.delete_rows,
                delete_synced_at,
                Some(delete_synced_at),
            )
            .await?;
            let staging_table = super::bigquery::upsert_staging_table_id(&info.dest_name);
            let object_uri = self
                .inner
                .upload_cdc_batch_load_artifact(&staging_table, &info.schema, &frame)
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to upload CDC delete batch-load artifact",
                        err.to_string()
                    )
                })?;
            steps.push(CdcBatchLoadJobStep {
                staging_table,
                object_uri,
                row_count,
                upserted_count: row_count,
                deleted_count: row_count,
            });
        }

        Ok(CdcBatchLoadJobPayload {
            job_id: Uuid::new_v4().to_string(),
            source_table: info.source_name.clone(),
            target_table: info.dest_name.clone(),
            schema: info.schema.clone(),
            primary_key: info.primary_key.clone(),
            truncate: work.truncate,
            steps,
        })
    }
}

impl CdcBatchLoadManager {
    async fn new(
        inner: BigQueryDestination,
        stats: Option<StatsHandle>,
        state_handle: StateHandle,
    ) -> Result<Self> {
        let manager = Self {
            inner,
            stats,
            state_handle,
            scheduler: Arc::new(Mutex::new(CdcBatchLoadSchedulerState::default())),
            waiters: Arc::new(Mutex::new(HashMap::new())),
        };
        manager.restore_pending_jobs().await?;
        Ok(manager)
    }

    async fn restore_pending_jobs(&self) -> Result<()> {
        for record in self
            .state_handle
            .load_cdc_batch_load_jobs(&[
                CdcBatchLoadJobStatus::Pending,
                CdcBatchLoadJobStatus::Running,
            ])
            .await?
        {
            let payload: CdcBatchLoadJobPayload = serde_json::from_str(&record.payload_json)?;
            self.enqueue_loaded_job(CdcQueuedBatchLoadJob { record, payload })
                .await;
        }
        Ok(())
    }

    async fn enqueue(
        &self,
        first_sequence: u64,
        payload: CdcBatchLoadJobPayload,
    ) -> EtlResult<oneshot::Receiver<EtlResult<()>>> {
        let now = Utc::now().timestamp_millis();
        let record = CdcBatchLoadJobRecord {
            job_id: payload.job_id.clone(),
            table_key: payload.target_table.clone(),
            first_sequence,
            status: CdcBatchLoadJobStatus::Pending,
            payload_json: serde_json::to_string(&payload).map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to serialize CDC batch-load job payload",
                    err.to_string()
                )
            })?,
            attempt_count: 0,
            last_error: None,
            created_at: now,
            updated_at: now,
        };
        self.state_handle
            .enqueue_cdc_batch_load_job(&record)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to persist CDC batch-load job",
                    err.to_string()
                )
            })?;

        let (tx, rx) = oneshot::channel();
        self.waiters.lock().await.insert(record.job_id.clone(), tx);
        self.enqueue_loaded_job(CdcQueuedBatchLoadJob { record, payload })
            .await;
        Ok(rx)
    }

    async fn enqueue_loaded_job(&self, job: CdcQueuedBatchLoadJob) {
        let table_key = job.record.table_key.clone();
        let maybe_start = {
            let mut scheduler = self.scheduler.lock().await;
            let should_start = !scheduler.active_tables.contains(&table_key);
            scheduler
                .queued_by_table
                .entry(table_key.clone())
                .or_insert_with(VecDeque::new)
                .push_back(job);
            if should_start {
                scheduler.active_tables.insert(table_key.clone());
                scheduler
                    .queued_by_table
                    .get_mut(&table_key)
                    .and_then(|queue| queue.pop_front())
            } else {
                None
            }
        };
        if let Some(job) = maybe_start {
            self.spawn_job(job);
        }
    }

    fn spawn_job(&self, job: CdcQueuedBatchLoadJob) {
        let manager = self.clone();
        tokio::spawn(async move {
            manager.run_job(job).await;
        });
    }

    async fn run_job(&self, job: CdcQueuedBatchLoadJob) {
        let job_id = job.record.job_id.clone();
        let table_key = job.record.table_key.clone();
        let started_at = Instant::now();
        let span = info_span!(
            "cdc_batch_load_job",
            job_id = %job_id,
            table = %table_key,
            first_sequence = job.record.first_sequence,
            step_count = job.payload.steps.len()
        );
        let _span = span.enter();
        let running_mark = self
            .state_handle
            .mark_cdc_batch_load_job_running(&job_id)
            .await;
        let result = if let Err(err) = running_mark {
            Err(etl::etl_error!(
                ErrorKind::DestinationError,
                "failed to mark CDC batch-load job running",
                err.to_string()
            ))
        } else {
            self.inner
                .process_cdc_batch_load_job(&job.payload)
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to process CDC batch-load job",
                        err.to_string()
                    )
                })
        };

        match &result {
            Ok(()) => {
                info!(
                    job_id = %job_id,
                    table = %table_key,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "queued CDC batch-load job succeeded"
                );
                if let Some(stats) = &self.stats {
                    let row_count = job.payload.steps.iter().map(|step| step.row_count).sum();
                    let upserted = job
                        .payload
                        .steps
                        .iter()
                        .map(|step| step.upserted_count)
                        .sum();
                    let deleted = job
                        .payload
                        .steps
                        .iter()
                        .map(|step| step.deleted_count)
                        .sum();
                    stats
                        .record_load(
                            &job.payload.source_table,
                            row_count,
                            upserted,
                            deleted,
                            started_at.elapsed().as_millis() as u64,
                        )
                        .await;
                }
                let _ = self
                    .state_handle
                    .mark_cdc_batch_load_job_succeeded(&job_id)
                    .await;
            }
            Err(err) => {
                warn!(
                    job_id = %job_id,
                    table = %table_key,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    error = %err,
                    "queued CDC batch-load job failed"
                );
                let _ = self
                    .state_handle
                    .mark_cdc_batch_load_job_failed(&job_id, &err.to_string())
                    .await;
            }
        }

        if let Some(waiter) = self.waiters.lock().await.remove(&job_id) {
            let _ = waiter.send(result);
        }

        let next_job = {
            let mut scheduler = self.scheduler.lock().await;
            let next = scheduler
                .queued_by_table
                .get_mut(&table_key)
                .and_then(|queue| queue.pop_front());
            if next.is_none() {
                scheduler.active_tables.remove(&table_key);
                scheduler.queued_by_table.remove(&table_key);
            }
            next
        };
        if let Some(next_job) = next_job {
            self.spawn_job(next_job);
        }
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

async fn build_cdc_frame(
    info: CdcTableInfo,
    rows: Vec<TableRow>,
    synced_at: DateTime<Utc>,
    deleted_at_override: Option<DateTime<Utc>>,
) -> EtlResult<DataFrame> {
    if rows.len() < CDC_FRAME_BUILD_BLOCKING_ROWS {
        return table_rows_to_frame(&info, &rows, synced_at, deleted_at_override);
    }

    task::spawn_blocking(move || table_rows_to_frame(&info, &rows, synced_at, deleted_at_override))
        .await
        .map_err(|err| {
            etl::etl_error!(
                ErrorKind::DestinationError,
                "failed to join CDC frame build task",
                err.to_string()
            )
        })?
}

fn compact_table_events(
    info: &CdcTableInfo,
    table_id: TableId,
    events: Vec<Event>,
) -> EtlResult<CdcCommitTableWork> {
    let mut row_actions: HashMap<String, PendingTableRowAction> = HashMap::new();
    let mut truncate = false;

    for event in events {
        match event {
            Event::Insert(insert_event) => {
                if insert_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC insert batch"
                    ));
                }
                let key = primary_key_identity(info, &insert_event.table_row)?;
                row_actions.insert(key, PendingTableRowAction::Upsert(insert_event.table_row));
            }
            Event::Update(update_event) => {
                if update_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC update batch"
                    ));
                }
                let key = primary_key_identity(info, &update_event.table_row)?;
                row_actions.insert(key, PendingTableRowAction::Upsert(update_event.table_row));
            }
            Event::Delete(delete_event) => {
                if delete_event.table_id != table_id {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC delete batch"
                    ));
                }
                if !info.soft_delete {
                    continue;
                }
                if let Some((_, old_row)) = delete_event.old_table_row {
                    let key = primary_key_identity(info, &old_row)?;
                    row_actions.insert(key, PendingTableRowAction::Delete(old_row));
                } else {
                    warn!(
                        table = %info.source_name,
                        "skipping delete event without primary key"
                    );
                }
            }
            Event::Truncate(truncate_event) => {
                if !truncate_event
                    .rel_ids
                    .iter()
                    .any(|rel_id| TableId::new(*rel_id) == table_id)
                {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "received mixed-table CDC truncate batch"
                    ));
                }
                truncate = true;
                row_actions.clear();
            }
            Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported => {}
        }
    }

    let mut rows = Vec::new();
    let mut delete_rows = Vec::new();
    for action in row_actions.into_values() {
        match action {
            PendingTableRowAction::Upsert(row) => rows.push(row),
            PendingTableRowAction::Delete(row) => delete_rows.push(row),
        }
    }

    Ok(CdcCommitTableWork {
        table_id,
        rows,
        delete_rows,
        truncate,
    })
}

fn primary_key_identity(info: &CdcTableInfo, row: &TableRow) -> EtlResult<String> {
    let cell = row
        .values()
        .get(info.source_primary_key_index)
        .ok_or_else(|| {
            etl::etl_error!(
                ErrorKind::ConversionError,
                "missing primary key value in CDC row",
                info.primary_key.clone()
            )
        })?;
    cell_identity_key(cell)
}

fn cell_identity_key(cell: &Cell) -> EtlResult<String> {
    let key = match cell {
        Cell::Null => {
            return Err(etl::etl_error!(
                ErrorKind::ConversionError,
                "null primary key value in CDC row"
            ));
        }
        Cell::Bool(value) => value.to_string(),
        Cell::String(value) => value.clone(),
        Cell::I16(value) => value.to_string(),
        Cell::I32(value) => value.to_string(),
        Cell::U32(value) => value.to_string(),
        Cell::I64(value) => value.to_string(),
        Cell::F32(value) => value.to_string(),
        Cell::F64(value) => value.to_string(),
        Cell::Numeric(value) => value.to_string(),
        Cell::Date(value) => value.format("%Y-%m-%d").to_string(),
        Cell::Time(value) => format_time(value),
        Cell::Timestamp(value) => Utc.from_utc_datetime(value).to_rfc3339(),
        Cell::TimestampTz(value) => value.to_rfc3339(),
        Cell::Uuid(value) => value.to_string(),
        Cell::Json(value) => value.to_string(),
        Cell::Bytes(value) => encode_base64(value),
        Cell::Array(array) => serde_json::to_string(&array_to_json(array)).map_err(|err| {
            etl::etl_error!(
                ErrorKind::ConversionError,
                "failed to encode array primary key",
                err.to_string()
            )
        })?,
    };
    Ok(key)
}

fn table_rows_to_frame(
    info: &CdcTableInfo,
    rows: &[TableRow],
    synced_at: DateTime<Utc>,
    deleted_at_override: Option<DateTime<Utc>>,
) -> Result<DataFrame, EtlError> {
    let polars_schema = polars_schema_with_metadata(&info.schema, &info.metadata)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for (column, source_idx) in info
            .schema
            .columns
            .iter()
            .zip(info.dest_source_indices.iter())
        {
            let cell = row.values().get(*source_idx).unwrap_or(&Cell::Null);
            values.push(cell_to_anyvalue(cell, &column.data_type));
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));

        let deleted_at = if info.soft_delete {
            if let Some(ts) = deleted_at_override {
                Some(ts.to_rfc3339())
            } else {
                derive_deleted_at_from_row(info, row)
            }
        } else {
            None
        };

        match deleted_at {
            Some(ts) => values.push(AnyValue::StringOwned(PlSmallStr::from(ts))),
            None => values.push(AnyValue::Null),
        }

        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(|err| {
        etl::etl_error!(
            ErrorKind::ConversionError,
            "failed to build CDC dataframe",
            err.to_string()
        )
    })
}

fn derive_deleted_at_from_row(info: &CdcTableInfo, row: &TableRow) -> Option<String> {
    let idx = info.source_soft_delete_index?;
    let cell = row.values().get(idx)?;
    match cell {
        Cell::Null => None,
        Cell::Bool(value) => {
            if *value {
                Some(Utc::now().to_rfc3339())
            } else {
                None
            }
        }
        Cell::Timestamp(value) => Some(Utc.from_utc_datetime(value).to_rfc3339()),
        Cell::TimestampTz(value) => Some(value.to_rfc3339()),
        Cell::Date(value) => Some(value.format("%Y-%m-%d").to_string()),
        Cell::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn cell_to_anyvalue(cell: &Cell, data_type: &DataType) -> AnyValue<'static> {
    if matches!(data_type, DataType::Interval) {
        return match cell {
            Cell::Null => AnyValue::Null,
            Cell::F32(value) => AnyValue::Float64(*value as f64),
            Cell::F64(value) => AnyValue::Float64(*value),
            _ => AnyValue::Null,
        };
    }

    match cell {
        Cell::Null => AnyValue::Null,
        Cell::Bool(value) => AnyValue::Boolean(*value),
        Cell::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
        Cell::I16(value) => AnyValue::Int64(*value as i64),
        Cell::I32(value) => AnyValue::Int64(*value as i64),
        Cell::U32(value) => AnyValue::Int64(*value as i64),
        Cell::I64(value) => AnyValue::Int64(*value),
        Cell::F32(value) => AnyValue::Float64(*value as f64),
        Cell::F64(value) => AnyValue::Float64(*value),
        Cell::Numeric(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Date(value) => {
            AnyValue::StringOwned(PlSmallStr::from(value.format("%Y-%m-%d").to_string()))
        }
        Cell::Time(value) => AnyValue::StringOwned(PlSmallStr::from(format_time(value))),
        Cell::Timestamp(value) => {
            AnyValue::StringOwned(PlSmallStr::from(Utc.from_utc_datetime(value).to_rfc3339()))
        }
        Cell::TimestampTz(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_rfc3339())),
        Cell::Uuid(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Json(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Bytes(value) => AnyValue::StringOwned(PlSmallStr::from(encode_base64(value))),
        Cell::Array(array) => {
            let json = serde_json::to_string(&array_to_json(array)).unwrap_or_default();
            AnyValue::StringOwned(PlSmallStr::from(json))
        }
    }
}

fn array_to_json(array: &etl::types::ArrayCell) -> serde_json::Value {
    match array {
        etl::types::ArrayCell::Bool(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(serde_json::Value::Bool)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::String(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::I16(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as i64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::I32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as i64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::U32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number((n as u64).into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::I64(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.map(|n| serde_json::Value::Number(n.into()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::F32(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.and_then(|n| {
                        serde_json::Number::from_f64(n as f64).map(serde_json::Value::Number)
                    })
                    .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::F64(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.and_then(|n| serde_json::Number::from_f64(n).map(serde_json::Value::Number))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Numeric(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|n| serde_json::Value::String(n.to_string()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Date(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Time(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|t| serde_json::Value::String(format_time(t)))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Timestamp(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|t| serde_json::Value::String(Utc.from_utc_datetime(t).to_rfc3339()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::TimestampTz(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|t| serde_json::Value::String(t.to_rfc3339()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Uuid(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|u| serde_json::Value::String(u.to_string()))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        etl::types::ArrayCell::Json(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| v.as_ref().cloned().unwrap_or(serde_json::Value::Null))
                .collect(),
        ),
        etl::types::ArrayCell::Bytes(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|b| serde_json::Value::String(encode_base64(b)))
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
    }
}

fn polars_schema_with_metadata(
    schema: &TableSchema,
    metadata: &MetadataColumns,
) -> EtlResult<Schema> {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len() + 2);
    for column in &schema.columns {
        let dtype = match column.data_type {
            DataType::Int64 => PolarsDataType::Int64,
            DataType::Float64 | DataType::Interval => PolarsDataType::Float64,
            DataType::Bool => PolarsDataType::Boolean,
            _ => PolarsDataType::String,
        };
        fields.push(Field::new(column.name.as_str().into(), dtype));
    }
    fields.push(Field::new(
        metadata.synced_at.as_str().into(),
        PolarsDataType::String,
    ));
    fields.push(Field::new(
        metadata.deleted_at.as_str().into(),
        PolarsDataType::String,
    ));
    Ok(Schema::from_iter(fields))
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn format_time(value: &NaiveTime) -> String {
    value.format("%H:%M:%S%.f").to_string()
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
