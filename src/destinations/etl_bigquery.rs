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
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, RwLock, Semaphore, oneshot};
use tokio::task;
use tracing::{info, info_span, warn};

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
    waiters: Arc<Mutex<CdcBatchLoadJobWaiters>>,
    notify: Arc<Notify>,
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
                    CdcBatchLoadManager::new(
                        inner.clone(),
                        stats.clone(),
                        state_handle,
                        apply_concurrency.max(1),
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
            let first_sequence = fragments
                .first()
                .map(|fragment| fragment.sequence)
                .unwrap_or(0);
            let rx = manager.enqueue(first_sequence, payload, fragments).await?;
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
        connection_id: &str,
        fragments: &[CdcCommitFragmentMeta],
    ) -> EtlResult<CdcBatchLoadJobPayload> {
        let job_id =
            stable_cdc_batch_load_job_id(connection_id, &info.dest_name, work.truncate, fragments);
        let mut steps = Vec::new();

        if !work.rows.is_empty() {
            let row_count = work.rows.len();
            let build_frame_span = info_span!(
                "cdc_producer.build_frame",
                table = %info.source_name,
                destination_table = %info.dest_name,
                rows = row_count,
                mode = "upsert"
            );
            let frame = {
                let _build_frame_span = build_frame_span.enter();
                build_cdc_frame(info.clone(), work.rows, synced_at, None).await?
            };
            let staging_table = super::bigquery::stable_cdc_staging_table_id(
                &info.dest_name,
                &job_id,
                CdcBatchLoadStepKind::Upsert.as_str(),
            );
            let object_name = stable_cdc_batch_load_object_name(
                self.inner.batch_load_prefix(),
                &info.dest_name,
                &job_id,
                CdcBatchLoadStepKind::Upsert,
            );
            let upload_span = info_span!(
                "cdc_producer.upload_artifact",
                table = %info.source_name,
                destination_table = %info.dest_name,
                staging_table = %staging_table,
                rows = row_count,
                mode = "upsert"
            );
            let object_uri = {
                let _upload_span = upload_span.enter();
                self.inner
                    .upload_cdc_batch_load_artifact_with_object_name(
                        &info.schema,
                        &frame,
                        &object_name,
                    )
                    .await
                    .map_err(|err| {
                        etl::etl_error!(
                            ErrorKind::DestinationError,
                            "failed to upload CDC batch-load artifact",
                            err.to_string()
                        )
                    })?
            };
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
            let build_frame_span = info_span!(
                "cdc_producer.build_frame",
                table = %info.source_name,
                destination_table = %info.dest_name,
                rows = row_count,
                mode = "delete"
            );
            let frame = {
                let _build_frame_span = build_frame_span.enter();
                build_cdc_frame(
                    info.clone(),
                    work.delete_rows,
                    delete_synced_at,
                    Some(delete_synced_at),
                )
                .await?
            };
            let staging_table = super::bigquery::stable_cdc_staging_table_id(
                &info.dest_name,
                &job_id,
                CdcBatchLoadStepKind::Delete.as_str(),
            );
            let object_name = stable_cdc_batch_load_object_name(
                self.inner.batch_load_prefix(),
                &info.dest_name,
                &job_id,
                CdcBatchLoadStepKind::Delete,
            );
            let upload_span = info_span!(
                "cdc_producer.upload_artifact",
                table = %info.source_name,
                destination_table = %info.dest_name,
                staging_table = %staging_table,
                rows = row_count,
                mode = "delete"
            );
            let object_uri = {
                let _upload_span = upload_span.enter();
                self.inner
                    .upload_cdc_batch_load_artifact_with_object_name(
                        &info.schema,
                        &frame,
                        &object_name,
                    )
                    .await
                    .map_err(|err| {
                        etl::etl_error!(
                            ErrorKind::DestinationError,
                            "failed to upload CDC delete batch-load artifact",
                            err.to_string()
                        )
                    })?
            };
            steps.push(CdcBatchLoadJobStep {
                staging_table,
                object_uri,
                row_count,
                upserted_count: row_count,
                deleted_count: row_count,
            });
        }

        Ok(CdcBatchLoadJobPayload {
            job_id,
            source_table: info.source_name.clone(),
            target_table: info.dest_name.clone(),
            schema: info.schema.clone(),
            primary_key: info.primary_key.clone(),
            truncate: work.truncate,
            steps,
        })
    }
}

fn stable_cdc_batch_load_job_id(
    connection_id: &str,
    target_table: &str,
    truncate: bool,
    fragments: &[CdcCommitFragmentMeta],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(connection_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(target_table.as_bytes());
    hasher.update(b"\0");
    if truncate {
        hasher.update(b"truncate");
    } else {
        hasher.update(b"upsert");
    }
    for fragment in fragments {
        hasher.update(b"\0");
        hasher.update(fragment.sequence.to_string().as_bytes());
        hasher.update(b"@");
        hasher.update(fragment.commit_lsn.as_bytes());
    }
    let digest = hex::encode(hasher.finalize());
    format!("cdc_job_{}", &digest[..24])
}

fn stable_cdc_batch_load_fragment_id(job_id: &str, sequence: u64) -> String {
    format!("{job_id}:{sequence}")
}

fn stable_cdc_batch_load_object_name(
    prefix: Option<&str>,
    target_table: &str,
    job_id: &str,
    step_kind: CdcBatchLoadStepKind,
) -> String {
    let suffix = format!("{}_{}", step_kind.as_str(), job_id);
    super::bigquery::stable_cdc_batch_load_object_name(prefix, target_table, &suffix)
}

impl CdcBatchLoadManager {
    async fn new(
        inner: BigQueryDestination,
        stats: Option<StatsHandle>,
        state_handle: StateHandle,
        worker_count: usize,
    ) -> Result<Self> {
        let manager = Self {
            inner,
            stats,
            state_handle,
            waiters: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        };
        manager.restore_pending_jobs().await?;
        manager.start_workers(worker_count.max(1));
        Ok(manager)
    }

    fn start_workers(&self, worker_count: usize) {
        for worker_index in 0..worker_count {
            let manager = self.clone();
            tokio::spawn(async move {
                manager.worker_loop(worker_index).await;
            });
        }
    }

    async fn worker_loop(&self, worker_index: usize) {
        loop {
            let job = match self.claim_next_job().await {
                Ok(Some(job)) => job,
                Ok(None) => {
                    tokio::select! {
                        _ = self.notify.notified() => {}
                        _ = tokio::time::sleep(CDC_BATCH_LOAD_CLAIM_POLL_INTERVAL) => {}
                    }
                    continue;
                }
                Err(err) => {
                    warn!(
                        component = "consumer",
                        event = "cdc_consumer_claim_failed",
                        connection_id = self.state_handle.connection_id(),
                        worker = worker_index,
                        error = %err,
                        "consumer worker failed to claim queued CDC batch-load job"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            info!(
                component = "consumer",
                event = "cdc_consumer_claimed_job",
                connection_id = self.state_handle.connection_id(),
                worker = worker_index,
                job_id = %job.record.job_id,
                table = %job.record.table_key,
                first_sequence = job.record.first_sequence,
                "consumer worker claimed queued CDC batch-load job"
            );
            self.run_job(job).await;
        }
    }

    async fn restore_pending_jobs(&self) -> Result<()> {
        let requeued_running = self
            .state_handle
            .requeue_cdc_batch_load_running_jobs()
            .await?;
        let pending_jobs = self
            .state_handle
            .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
            .await?;
        if requeued_running > 0 || !pending_jobs.is_empty() {
            info!(
                component = "consumer",
                event = "cdc_consumer_restored_jobs",
                connection_id = self.state_handle.connection_id(),
                requeued_running_jobs = requeued_running,
                pending_jobs = pending_jobs.len(),
                "restored queued CDC batch-load jobs from durable state"
            );
            self.notify.notify_waiters();
        }
        Ok(())
    }

    async fn enqueue(
        &self,
        first_sequence: u64,
        payload: CdcBatchLoadJobPayload,
        fragments: Vec<CdcCommitFragmentMeta>,
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
        let total_rows = payload
            .steps
            .iter()
            .map(|step| step.row_count)
            .sum::<usize>() as i64;
        let total_upserted = payload
            .steps
            .iter()
            .map(|step| step.upserted_count)
            .sum::<usize>() as i64;
        let total_deleted = payload
            .steps
            .iter()
            .map(|step| step.deleted_count)
            .sum::<usize>() as i64;
        let fragment_count = fragments.len().max(1);
        let fragment_records: Vec<CdcCommitFragmentRecord> = fragments
            .into_iter()
            .enumerate()
            .map(|(index, fragment)| CdcCommitFragmentRecord {
                fragment_id: stable_cdc_batch_load_fragment_id(&record.job_id, fragment.sequence),
                job_id: record.job_id.clone(),
                sequence: fragment.sequence,
                commit_lsn: fragment.commit_lsn,
                table_key: record.table_key.clone(),
                status: CdcCommitFragmentStatus::Pending,
                row_count: if fragment_count == 1 || index == 0 {
                    total_rows
                } else {
                    0
                },
                upserted_count: if fragment_count == 1 || index == 0 {
                    total_upserted
                } else {
                    0
                },
                deleted_count: if fragment_count == 1 || index == 0 {
                    total_deleted
                } else {
                    0
                },
                last_error: None,
                created_at: now,
                updated_at: now,
            })
            .collect();
        let persisted_record = self
            .state_handle
            .enqueue_cdc_batch_load_job(&record)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to persist CDC batch-load job",
                    err.to_string()
                )
            })?;
        self.state_handle
            .upsert_cdc_commit_fragments(&fragment_records)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to persist CDC commit fragments",
                    err.to_string()
                )
            })?;
        if let Some(last_fragment) = fragment_records.last() {
            let mut watermark = self
                .state_handle
                .load_cdc_watermark_state()
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to load CDC watermark state",
                        err.to_string()
                    )
                })?
                .unwrap_or_default();
            watermark.last_enqueued_sequence = Some(
                watermark
                    .last_enqueued_sequence
                    .unwrap_or_default()
                    .max(last_fragment.sequence),
            );
            watermark.last_received_lsn = Some(last_fragment.commit_lsn.clone());
            watermark.last_relevant_change_seen_at = Some(Utc::now());
            watermark.updated_at = Some(Utc::now());
            self.state_handle
                .save_cdc_watermark_state(&watermark)
                .await
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to persist CDC watermark enqueue state",
                        err.to_string()
                    )
                })?;
        }

        let (tx, rx) = oneshot::channel();
        self.waiters
            .lock()
            .await
            .entry(persisted_record.job_id.clone())
            .or_default()
            .push(tx);
        match persisted_record.status {
            CdcBatchLoadJobStatus::Succeeded => {
                if let Some(waiters) = self.waiters.lock().await.remove(&persisted_record.job_id) {
                    for waiter in waiters {
                        let _ = waiter.send(Ok(()));
                    }
                }
            }
            CdcBatchLoadJobStatus::Pending | CdcBatchLoadJobStatus::Running => {
                info!(
                    component = "producer",
                    event = "cdc_producer_enqueued_job",
                    connection_id = self.state_handle.connection_id(),
                    table = %persisted_record.table_key,
                    first_sequence = persisted_record.first_sequence,
                    status = ?persisted_record.status,
                    job_id = %persisted_record.job_id,
                    "queued CDC batch-load job scheduled for consumer workers"
                );
                self.notify.notify_one();
            }
            CdcBatchLoadJobStatus::Failed => {
                unreachable!("failed jobs should be revived or preserved before returning")
            }
        }
        Ok(rx)
    }

    async fn claim_next_job(&self) -> EtlResult<Option<CdcQueuedBatchLoadJob>> {
        let stale_before_ms =
            Utc::now().timestamp_millis() - CDC_BATCH_LOAD_JOB_STALE_TIMEOUT.as_millis() as i64;
        let Some(record) = self
            .state_handle
            .claim_next_cdc_batch_load_job(stale_before_ms)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to claim queued CDC batch-load job",
                    err.to_string()
                )
            })?
        else {
            return Ok(None);
        };
        let payload: CdcBatchLoadJobPayload =
            serde_json::from_str(&record.payload_json).map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to deserialize claimed CDC batch-load job payload",
                    err.to_string()
                )
            })?;
        Ok(Some(CdcQueuedBatchLoadJob { record, payload }))
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
        let (heartbeat_stop_tx, mut heartbeat_stop_rx) = oneshot::channel();
        let heartbeat_handle = {
            let state_handle = self.state_handle.clone();
            let heartbeat_job_id = job_id.clone();
            let heartbeat_table = table_key.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(CDC_BATCH_LOAD_JOB_HEARTBEAT_INTERVAL) => {
                            if let Err(err) = state_handle.heartbeat_cdc_batch_load_job(&heartbeat_job_id).await {
                                warn!(
                                    component = "consumer",
                                    event = "cdc_consumer_job_heartbeat_failed",
                                    connection_id = state_handle.connection_id(),
                                    job_id = %heartbeat_job_id,
                                    table = %heartbeat_table,
                                    error = %err,
                                    "failed to heartbeat queued CDC batch-load job"
                                );
                            }
                        }
                        _ = &mut heartbeat_stop_rx => break,
                    }
                }
            })
        };
        let result = self
            .inner
            .process_cdc_batch_load_job(self.state_handle.connection_id(), &job.payload)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to process CDC batch-load job",
                    err.to_string()
                )
            });
        let _ = heartbeat_stop_tx.send(());
        let _ = heartbeat_handle.await;

        match &result {
            Ok(()) => {
                crate::telemetry::record_cdc_batch_load_job(
                    self.state_handle.connection_id(),
                    &table_key,
                    "succeeded",
                    started_at.elapsed().as_secs_f64() * 1000.0,
                );
                info!(
                    component = "consumer",
                    event = "cdc_consumer_job_succeeded",
                    connection_id = self.state_handle.connection_id(),
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
                let _ = self
                    .state_handle
                    .mark_cdc_commit_fragments_succeeded_for_job(&job_id)
                    .await;
            }
            Err(err) => {
                crate::telemetry::record_cdc_batch_load_job(
                    self.state_handle.connection_id(),
                    &table_key,
                    "failed",
                    started_at.elapsed().as_secs_f64() * 1000.0,
                );
                warn!(
                    component = "consumer",
                    event = "cdc_consumer_job_failed",
                    connection_id = self.state_handle.connection_id(),
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
                let _ = self
                    .state_handle
                    .mark_cdc_commit_fragments_failed_for_job(&job_id, &err.to_string())
                    .await;
            }
        }

        if let Some(waiters) = self.waiters.lock().await.remove(&job_id) {
            for waiter in waiters {
                let waiter_result = match &result {
                    Ok(()) => Ok(()),
                    Err(err) => Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "queued CDC batch-load job failed",
                        err.to_string()
                    )),
                };
                let _ = waiter.send(waiter_result);
            }
        }
        self.notify.notify_one();
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
