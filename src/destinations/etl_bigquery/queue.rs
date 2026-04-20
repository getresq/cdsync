use super::*;
use crate::destinations::bigquery;
use crate::retry::{SyncRetryClass, classify_sync_retry, compute_sync_retry_backoff};
use crate::types::TableCheckpoint;
use crate::types::TableRuntimeStatus;
use tokio::time::timeout;

async fn cdc_batch_load_job_with_timeout<F>(
    duration: Duration,
    job_id: &str,
    table_key: &str,
    future: F,
) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    match timeout(duration, future).await {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!(
            "CDC batch-load job {} for {} exceeded hard timeout of {}s",
            job_id,
            table_key,
            duration.as_secs()
        )),
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

fn stable_cdc_batch_load_job_step_load_job_id(
    job_id: &str,
    step_kind: CdcBatchLoadStepKind,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(job_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(step_kind.as_str().as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("cdsync_load_{}", &digest[..24])
}

fn stable_cdc_batch_load_object_name(
    prefix: Option<&str>,
    target_table: &str,
    job_id: &str,
    step_kind: CdcBatchLoadStepKind,
) -> String {
    let suffix = format!("{}_{}", step_kind.as_str(), job_id);
    bigquery::stable_cdc_batch_load_object_name(prefix, target_table, &suffix)
}

#[derive(Clone, Copy)]
enum CdcBatchLoadStagingStrategy {
    PerJob,
}

impl CdcBatchLoadStagingStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::PerJob => "per_job",
        }
    }
}

const CDC_BATCH_LOAD_STAGING_STRATEGY: CdcBatchLoadStagingStrategy =
    CdcBatchLoadStagingStrategy::PerJob;

fn cdc_batch_load_ledger_metadata_json(payload: &CdcBatchLoadJobPayload) -> Option<String> {
    serde_json::to_string(&serde_json::json!({
        "staging_strategy": CDC_BATCH_LOAD_STAGING_STRATEGY.as_str(),
        "scalar_metadata": "first_step_hints",
        "steps": payload.steps.iter().map(|step| serde_json::json!({
            "staging_table": &step.staging_table,
            "object_uri": &step.object_uri,
            "load_job_id": &step.load_job_id,
            "row_count": step.row_count,
            "upserted_count": step.upserted_count,
            "deleted_count": step.deleted_count,
        })).collect::<Vec<_>>(),
    }))
    .ok()
}

fn sequence_by_commit_lsn(fragments: &[CdcCommitFragmentMeta]) -> HashMap<String, u64> {
    fragments
        .iter()
        .map(|fragment| (fragment.commit_lsn.clone(), fragment.sequence))
        .collect()
}

fn should_resolve_cdc_batch_load_waiters(
    retry_class: SyncRetryClass,
    local_retry_retryable_failures: bool,
) -> bool {
    !local_retry_retryable_failures || !retry_class.is_retryable()
}

fn should_mark_cdc_batch_load_fragments_failed(
    retry_class: SyncRetryClass,
    local_retry_retryable_failures: bool,
) -> bool {
    !local_retry_retryable_failures || !retry_class.is_retryable()
}

const CDC_SNAPSHOT_HANDOFF_WAIT_PREFIX: &str = "CDC batch-load waiting for snapshot handoff";
fn cdc_snapshot_handoff_wait_reason(source_table: &str) -> String {
    format!("{CDC_SNAPSHOT_HANDOFF_WAIT_PREFIX} for {source_table}")
}

fn is_cdc_snapshot_handoff_wait_error(err: &anyhow::Error) -> bool {
    err.to_string()
        .starts_with(CDC_SNAPSHOT_HANDOFF_WAIT_PREFIX)
}

fn cdc_batch_load_retry_delay(attempt_count: i32, table_key: &str) -> Duration {
    compute_sync_retry_backoff(
        &format!("cdc_batch_load:{table_key}"),
        attempt_count.max(1) as u32,
        1_000,
    )
}

fn cdc_batch_load_retry_due_at_ms(record: &CdcBatchLoadJobRecord) -> i64 {
    let delay_ms = i64::try_from(
        cdc_batch_load_retry_delay(record.attempt_count, &record.table_key).as_millis(),
    )
    .unwrap_or(i64::MAX);
    record.updated_at.saturating_add(delay_ms)
}

fn cdc_batch_load_retry_is_due(record: &CdcBatchLoadJobRecord, now_ms: i64) -> bool {
    cdc_batch_load_retry_due_at_ms(record) <= now_ms
}

fn cdc_batch_load_apply_attempt_key<'a>(
    records: impl IntoIterator<Item = &'a CdcBatchLoadJobRecord>,
) -> String {
    records
        .into_iter()
        .map(|record| format!("{}:{}", record.job_id, record.attempt_count.max(0)))
        .collect::<Vec<_>>()
        .join(",")
}

fn due_retryable_failed_cdc_batch_load_jobs(
    records: Vec<CdcBatchLoadJobRecord>,
    now_ms: i64,
) -> Vec<CdcBatchLoadJobRecord> {
    records
        .into_iter()
        .filter(|record| cdc_batch_load_retry_is_due(record, now_ms))
        .collect()
}

fn checkpoint_has_incomplete_snapshot(checkpoint: &TableCheckpoint) -> bool {
    !checkpoint.snapshot_chunks.is_empty()
        && checkpoint
            .snapshot_chunks
            .iter()
            .any(|chunk| !chunk.complete)
}

pub(super) enum CdcApplyReadiness {
    Ready,
    Snapshotting,
    Blocked,
}

pub(super) fn should_check_table_apply_readiness(mixed_mode_gate_enabled: bool) -> bool {
    mixed_mode_gate_enabled
}

impl CdcQueuedBatchLoadJob {
    fn apply_attempt_key(&self) -> String {
        cdc_batch_load_apply_attempt_key(std::slice::from_ref(&self.record))
    }
}

impl CdcQueuedBatchLoadWindow {
    fn first(&self) -> Option<&CdcQueuedBatchLoadJob> {
        self.jobs.first()
    }

    fn table_key(&self) -> &str {
        self.first().map_or("", |job| job.record.table_key.as_str())
    }

    fn first_sequence(&self) -> u64 {
        self.first()
            .map(|job| job.record.first_sequence)
            .unwrap_or_default()
    }

    fn job_ids(&self) -> Vec<String> {
        self.jobs
            .iter()
            .map(|job| job.record.job_id.clone())
            .collect()
    }

    fn apply_attempt_key(&self) -> String {
        cdc_batch_load_apply_attempt_key(self.jobs.iter().map(|job| &job.record))
    }

    fn merged_payload(&self) -> EtlResult<CdcBatchLoadJobPayload> {
        let first = self.first().ok_or_else(|| {
            etl::etl_error!(
                ErrorKind::DestinationError,
                "CDC reducer window contained no jobs"
            )
        })?;
        let mut payload = first.payload.clone();
        payload.job_id = first.record.job_id.clone();
        payload.truncate = false;
        payload.steps.clear();

        for job in &self.jobs {
            if job.payload.source_table != first.payload.source_table
                || job.payload.target_table != first.payload.target_table
                || job.payload.primary_key != first.payload.primary_key
            {
                return Err(etl::etl_error!(
                    ErrorKind::DestinationError,
                    "CDC reducer window contained mixed table payloads"
                ));
            }
            if job.payload.truncate && self.jobs.len() > 1 {
                return Err(etl::etl_error!(
                    ErrorKind::DestinationError,
                    "CDC reducer window crossed a truncate barrier"
                ));
            }
            payload.truncate |= job.payload.truncate;
            if payload.staging_schema.is_none() || job.payload.staging_schema.is_none() {
                payload.staging_schema = None;
            }
            payload.steps.extend(job.payload.steps.iter().cloned());
        }

        Ok(payload)
    }
}

impl EtlBigQueryDestination {
    pub(super) async fn prepare_cdc_batch_load_job(
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
        let staging_schema = cdc_staging_table_schema(&info.schema);
        let sequence_by_lsn = sequence_by_commit_lsn(fragments);
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
            let frame = build_cdc_staging_frame(
                info.clone(),
                work.rows,
                sequence_by_lsn.clone(),
                synced_at,
                None,
            )
            .instrument(build_frame_span)
            .await?;
            let staging_table = bigquery::stable_cdc_staging_table_id(
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
            let object_uri = self
                .inner
                .upload_cdc_batch_load_artifact_with_object_name(
                    &staging_schema,
                    &frame,
                    &object_name,
                )
                .instrument(upload_span)
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
                load_job_id: Some(stable_cdc_batch_load_job_step_load_job_id(
                    &job_id,
                    CdcBatchLoadStepKind::Upsert,
                )),
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
            let frame = build_cdc_staging_frame(
                info.clone(),
                work.delete_rows,
                sequence_by_lsn.clone(),
                delete_synced_at,
                Some(delete_synced_at),
            )
            .instrument(build_frame_span)
            .await?;
            let staging_table = bigquery::stable_cdc_staging_table_id(
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
            let object_uri = self
                .inner
                .upload_cdc_batch_load_artifact_with_object_name(
                    &staging_schema,
                    &frame,
                    &object_name,
                )
                .instrument(upload_span)
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
                load_job_id: Some(stable_cdc_batch_load_job_step_load_job_id(
                    &job_id,
                    CdcBatchLoadStepKind::Delete,
                )),
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
            staging_schema: Some(staging_schema),
            primary_key: info.primary_key.clone(),
            truncate: work.truncate,
            steps,
        })
    }
}

impl CdcBatchLoadManager {
    pub(super) async fn new(
        inner: BigQueryDestination,
        stats: Option<StatsHandle>,
        state_handle: StateHandle,
        staging_worker_count: usize,
        reducer_worker_count: usize,
        reducer_max_jobs: usize,
        local_retry_retryable_failures: bool,
    ) -> Result<Self> {
        let manager = Self {
            inner,
            stats,
            state_handle,
            mixed_mode_gate_enabled: Arc::new(AtomicBool::new(false)),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            reducer_max_jobs: reducer_max_jobs.max(1),
            local_retry_retryable_failures,
        };
        manager.discard_inflight_jobs_for_replay().await?;
        manager.start_workers(staging_worker_count.max(1), reducer_worker_count.max(1));
        Ok(manager)
    }

    fn start_workers(&self, staging_worker_count: usize, reducer_worker_count: usize) {
        for worker_index in 0..staging_worker_count {
            let manager = self.clone();
            tokio::spawn(async move {
                Box::pin(manager.staging_worker_loop(worker_index)).await;
            });
        }
        for worker_index in 0..reducer_worker_count {
            let manager = self.clone();
            tokio::spawn(async move {
                Box::pin(manager.apply_worker_loop(worker_index)).await;
            });
        }
    }

    pub(super) fn mixed_mode_gate_enabled(&self) -> bool {
        self.mixed_mode_gate_enabled.load(Ordering::Relaxed)
    }

    pub(super) fn set_mixed_mode_gate_enabled(&self, enabled: bool) {
        self.mixed_mode_gate_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub(super) async fn table_apply_readiness(
        &self,
        source_table: &str,
    ) -> anyhow::Result<CdcApplyReadiness> {
        let checkpoint = self
            .state_handle
            .load_postgres_checkpoint(source_table)
            .await?;
        Ok(match checkpoint.as_ref() {
            Some(checkpoint)
                if checkpoint.runtime.as_ref().is_some_and(|runtime| {
                    matches!(&runtime.status, TableRuntimeStatus::Blocked)
                }) =>
            {
                CdcApplyReadiness::Blocked
            }
            Some(checkpoint) if checkpoint_has_incomplete_snapshot(checkpoint) => {
                CdcApplyReadiness::Snapshotting
            }
            _ => CdcApplyReadiness::Ready,
        })
    }

    async fn staging_worker_loop(&self, worker_index: usize) {
        loop {
            let job = match self.claim_next_staging_job().await {
                Ok(Some(job)) => job,
                Ok(None) => {
                    tokio::select! {
                        () = self.notify.notified() => {}
                        () = tokio::time::sleep(CDC_BATCH_LOAD_CLAIM_POLL_INTERVAL) => {}
                    }
                    continue;
                }
                Err(err) => {
                    warn!(
                        component = "consumer",
                        event = "cdc_staging_worker_claim_failed",
                        connection_id = self.state_handle.connection_id(),
                        worker = worker_index,
                        error = %err,
                        "staging worker failed to claim queued CDC batch-load job"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            info!(
                component = "consumer",
                event = "cdc_staging_worker_claimed_job",
                connection_id = self.state_handle.connection_id(),
                worker = worker_index,
                job_id = %job.record.job_id,
                table = %job.record.table_key,
                first_sequence = job.record.first_sequence,
                "staging worker claimed queued CDC batch-load job"
            );
            Box::pin(self.run_staging_job(job)).await;
        }
    }

    async fn apply_worker_loop(&self, worker_index: usize) {
        loop {
            let window = match self.claim_next_apply_window().await {
                Ok(Some(window)) => window,
                Ok(None) => {
                    tokio::select! {
                        () = self.notify.notified() => {}
                        () = tokio::time::sleep(CDC_BATCH_LOAD_CLAIM_POLL_INTERVAL) => {}
                    }
                    continue;
                }
                Err(err) => {
                    warn!(
                        component = "consumer",
                        event = "cdc_apply_worker_claim_failed",
                        connection_id = self.state_handle.connection_id(),
                        worker = worker_index,
                        error = %err,
                        "apply worker failed to claim loaded CDC batch-load job"
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            info!(
                component = "consumer",
                event = "cdc_apply_worker_claimed_job",
                connection_id = self.state_handle.connection_id(),
                worker = worker_index,
                table = %window.table_key(),
                first_sequence = window.first_sequence(),
                job_count = window.jobs.len(),
                "apply worker claimed loaded CDC batch-load reducer window"
            );
            Box::pin(self.run_apply_window(window)).await;
        }
    }

    async fn discard_inflight_jobs_for_replay(&self) -> Result<()> {
        let cleanup = self
            .state_handle
            .discard_inflight_cdc_batch_load_state_for_replay()
            .await?;
        if cleanup.discarded_jobs > 0 || cleanup.discarded_fragments > 0 {
            info!(
                component = "consumer",
                event = "cdc_consumer_discarded_inflight_jobs_for_replay",
                connection_id = self.state_handle.connection_id(),
                discarded_jobs = cleanup.discarded_jobs,
                discarded_fragments = cleanup.discarded_fragments,
                "discarded in-flight queued CDC batch-load state so CDC can replay from durable feedback"
            );
        }
        Ok(())
    }

    pub(super) async fn enqueue(
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
            stage: CdcLedgerStage::Received,
            payload_json: serde_json::to_string(&payload).map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to serialize CDC batch-load job payload",
                    err.to_string()
                )
            })?,
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            staging_table: payload.steps.first().map(|step| step.staging_table.clone()),
            artifact_uri: payload.steps.first().map(|step| step.object_uri.clone()),
            load_job_id: None,
            merge_job_id: None,
            primary_key_lane: None,
            barrier_kind: work_barrier_kind(payload.truncate),
            ledger_metadata_json: cdc_batch_load_ledger_metadata_json(&payload),
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
                stage: CdcLedgerStage::Received,
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
                artifact_uri: record.artifact_uri.clone(),
                staging_table: record.staging_table.clone(),
                load_job_id: None,
                merge_job_id: None,
                primary_key_lane: None,
                barrier_kind: record.barrier_kind.clone(),
                ledger_metadata_json: record.ledger_metadata_json.clone(),
                created_at: now,
                updated_at: now,
            })
            .collect();
        let (tx, rx) = oneshot::channel();
        self.waiters
            .lock()
            .await
            .entry(record.job_id.clone())
            .or_default()
            .push(tx);
        let persisted_record = match self
            .state_handle
            .enqueue_cdc_batch_load_bundle(&record, &fragment_records)
            .await
        {
            Ok(record) => record,
            Err(err) => {
                let _ = self.waiters.lock().await.remove(&record.job_id);
                return Err(etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to persist CDC batch-load job",
                    err.to_string()
                ));
            }
        };
        match persisted_record.status {
            CdcBatchLoadJobStatus::Succeeded => {
                if let Some(waiters) = self.waiters.lock().await.remove(&persisted_record.job_id) {
                    for waiter in waiters {
                        let _ignored = waiter.send(Ok(()));
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

    async fn claim_next_staging_job(&self) -> EtlResult<Option<CdcQueuedBatchLoadJob>> {
        self.requeue_due_retryable_failed_jobs().await?;
        let stale_before_ms =
            Utc::now().timestamp_millis() - CDC_BATCH_LOAD_JOB_STALE_TIMEOUT.as_millis() as i64;
        let Some(record) = self
            .state_handle
            .claim_next_cdc_batch_load_staging_job(stale_before_ms)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to claim CDC batch-load staging job",
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
                    "failed to deserialize claimed CDC batch-load staging job payload",
                    err.to_string()
                )
            })?;
        Ok(Some(CdcQueuedBatchLoadJob { record, payload }))
    }

    async fn claim_next_apply_window(&self) -> EtlResult<Option<CdcQueuedBatchLoadWindow>> {
        let stale_before_ms =
            Utc::now().timestamp_millis() - CDC_BATCH_LOAD_JOB_STALE_TIMEOUT.as_millis() as i64;
        let records = self
            .state_handle
            .claim_next_loaded_cdc_batch_load_job_window_for_apply(
                stale_before_ms,
                self.reducer_max_jobs,
            )
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to claim loaded CDC batch-load apply job",
                    err.to_string()
                )
            })?;
        if records.is_empty() {
            return Ok(None);
        }

        let mut jobs = Vec::with_capacity(records.len());
        for record in records {
            let payload: CdcBatchLoadJobPayload = serde_json::from_str(&record.payload_json)
                .map_err(|err| {
                    etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to deserialize loaded CDC batch-load apply job payload",
                        err.to_string()
                    )
                })?;
            jobs.push(CdcQueuedBatchLoadJob { record, payload });
        }
        Ok(Some(CdcQueuedBatchLoadWindow { jobs }))
    }

    async fn requeue_due_retryable_failed_jobs(&self) -> EtlResult<u64> {
        let records = self
            .state_handle
            .load_retryable_failed_cdc_batch_load_jobs()
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to load retryable failed CDC batch-load jobs",
                    err.to_string()
                )
            })?;
        if records.is_empty() {
            return Ok(0);
        }

        let now_ms = Utc::now().timestamp_millis();
        let mut requeued = 0_u64;
        for record in due_retryable_failed_cdc_batch_load_jobs(records, now_ms) {
            let retry_due_at_ms = cdc_batch_load_retry_due_at_ms(&record);
            let retry_class = record.retry_class.map_or("unknown", SyncRetryClass::as_str);
            match self
                .state_handle
                .requeue_cdc_batch_load_job(&record.job_id)
                .await
            {
                Ok(true) => {
                    requeued = requeued.saturating_add(1);
                    info!(
                        component = "consumer",
                        event = "cdc_consumer_job_retry_requeued_from_durable_state",
                        connection_id = self.state_handle.connection_id(),
                        job_id = %record.job_id,
                        table = %record.table_key,
                        retry_class,
                        attempt_count = record.attempt_count,
                        retry_due_at_ms,
                        "requeued retryable failed CDC batch-load job from durable state"
                    );
                }
                Ok(false) => {}
                Err(err) => warn!(
                    component = "consumer",
                    event = "cdc_consumer_job_retry_requeue_from_durable_state_failed",
                    connection_id = self.state_handle.connection_id(),
                    job_id = %record.job_id,
                    table = %record.table_key,
                    retry_class,
                    error = %err,
                    "failed to requeue retryable failed CDC batch-load job from durable state"
                ),
            }
        }

        if requeued > 0 {
            self.notify.notify_one();
        }
        Ok(requeued)
    }

    async fn run_staging_job(&self, job: CdcQueuedBatchLoadJob) {
        let job_id = job.record.job_id.clone();
        let table_key = job.record.table_key.clone();
        let started_at = Instant::now();
        let span = info_span!(
            "cdc_batch_load_staging_job",
            job_id = %job_id,
            table = %table_key,
            first_sequence = job.record.first_sequence,
            step_count = job.payload.steps.len()
        );
        let result = self
            .inner
            .stage_cdc_batch_load_job(
                self.state_handle.connection_id(),
                &job.payload,
                job.record.attempt_count,
            )
            .instrument(span)
            .await;
        match result {
            Ok(()) => {
                info!(
                    component = "consumer",
                    event = "cdc_staging_worker_job_loaded",
                    connection_id = self.state_handle.connection_id(),
                    job_id = %job_id,
                    table = %table_key,
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "queued CDC batch-load job staged and loaded"
                );
                let _ = self
                    .state_handle
                    .mark_cdc_batch_load_job_loaded(&job_id)
                    .await;
                self.notify.notify_one();
            }
            Err(err) => {
                self.fail_job(job.record, &table_key, started_at, &err)
                    .await;
            }
        }
    }

    async fn run_apply_window(&self, mut window: CdcQueuedBatchLoadWindow) {
        if window.jobs.len() <= 1 {
            if let Some(job) = window.jobs.pop() {
                self.run_apply_job(job).await;
            }
            return;
        }

        let job_ids = window.job_ids();
        let records: Vec<CdcBatchLoadJobRecord> =
            window.jobs.iter().map(|job| job.record.clone()).collect();
        let apply_attempt_key = window.apply_attempt_key();
        let table_key = window.table_key().to_string();
        let started_at = Instant::now();
        let payload = match window.merged_payload() {
            Ok(payload) => payload,
            Err(err) => {
                let message = err.to_string();
                let err = anyhow::anyhow!(message.clone());
                if self
                    .fail_reducer_window(records, &table_key, started_at, &err)
                    .await
                {
                    let result = Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "queued CDC batch-load reducer window failed",
                        message
                    ));
                    self.resolve_window_waiters(&job_ids, result).await;
                }
                self.notify.notify_one();
                return;
            }
        };
        let span = info_span!(
            "cdc_batch_load_reducer_window",
            head_job_id = job_ids.first().map_or("", String::as_str),
            table = %table_key,
            first_sequence = window.first_sequence(),
            job_count = job_ids.len(),
            step_count = payload.steps.len()
        );
        let (heartbeat_stop_tx, mut heartbeat_stop_rx) = oneshot::channel();
        let heartbeat_handle = {
            let state_handle = self.state_handle.clone();
            let heartbeat_job_ids = job_ids.clone();
            let heartbeat_table = table_key.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = tokio::time::sleep(CDC_BATCH_LOAD_JOB_HEARTBEAT_INTERVAL) => {
                            if let Err(err) = state_handle.heartbeat_cdc_batch_load_jobs(&heartbeat_job_ids).await {
                                warn!(
                                    component = "consumer",
                                    event = "cdc_consumer_window_heartbeat_failed",
                                    connection_id = state_handle.connection_id(),
                                    job_count = heartbeat_job_ids.len(),
                                    table = %heartbeat_table,
                                    error = %err,
                                    "failed to heartbeat queued CDC batch-load reducer window"
                                );
                            }
                        }
                        _ = &mut heartbeat_stop_rx => break,
                    }
                }
            })
        };

        let result: anyhow::Result<()> = Box::pin(cdc_batch_load_job_with_timeout(
            CDC_BATCH_LOAD_JOB_HARD_TIMEOUT,
            job_ids.first().map_or("cdc-window", String::as_str),
            &table_key,
            async {
                if should_check_table_apply_readiness(self.mixed_mode_gate_enabled()) {
                    match self.table_apply_readiness(&payload.source_table).await {
                        Ok(CdcApplyReadiness::Ready) => {
                            self.inner
                                .apply_cdc_batch_load_job(
                                    self.state_handle.connection_id(),
                                    &payload,
                                    &apply_attempt_key,
                                )
                                .await
                        }
                        Ok(CdcApplyReadiness::Snapshotting) => Err(anyhow::anyhow!(
                            "{}",
                            cdc_snapshot_handoff_wait_reason(&payload.source_table)
                        )),
                        Ok(CdcApplyReadiness::Blocked) => {
                            info!(
                                component = "consumer",
                                event = "cdc_consumer_window_skipped_blocked_table",
                                connection_id = self.state_handle.connection_id(),
                                table = %table_key,
                                job_count = job_ids.len(),
                                "skipping queued CDC batch-load reducer window because table is blocked until manual resync"
                            );
                            Ok(())
                        }
                        Err(err) => Err(anyhow::anyhow!(
                            "failed to inspect CDC apply readiness for {}: {}",
                            payload.source_table,
                            err
                        )),
                    }
                } else {
                    self.inner
                        .apply_cdc_batch_load_job(
                            self.state_handle.connection_id(),
                            &payload,
                            &apply_attempt_key,
                        )
                        .await
                }
            }
            .instrument(span),
        ))
        .await;
        let _ = heartbeat_stop_tx.send(());
        let _ = heartbeat_handle.await;

        let terminal_state_committed = match &result {
            Ok(()) => {
                match self
                    .state_handle
                    .mark_cdc_batch_load_window_succeeded(&job_ids)
                    .await
                {
                    Err(err) => {
                        warn!(
                            component = "consumer",
                            event = "cdc_consumer_window_success_state_update_failed",
                            connection_id = self.state_handle.connection_id(),
                            table = %table_key,
                            job_count = job_ids.len(),
                            error = %err,
                            "failed to durably mark CDC batch-load reducer window succeeded"
                        );
                        false
                    }
                    Ok(()) => {
                        crate::telemetry::record_cdc_batch_load_job(
                            self.state_handle.connection_id(),
                            &table_key,
                            "succeeded",
                            started_at.elapsed().as_secs_f64() * 1000.0,
                        );
                        info!(
                            component = "consumer",
                            event = "cdc_consumer_window_succeeded",
                            connection_id = self.state_handle.connection_id(),
                            table = %table_key,
                            job_count = job_ids.len(),
                            duration_ms = started_at.elapsed().as_millis() as u64,
                            "queued CDC batch-load reducer window succeeded"
                        );
                        if let Some(stats) = &self.stats {
                            let row_count = payload.steps.iter().map(|step| step.row_count).sum();
                            let upserted =
                                payload.steps.iter().map(|step| step.upserted_count).sum();
                            let deleted = payload.steps.iter().map(|step| step.deleted_count).sum();
                            stats
                                .record_load(
                                    &payload.source_table,
                                    row_count,
                                    upserted,
                                    deleted,
                                    started_at.elapsed().as_millis() as u64,
                                )
                                .await;
                        }
                        true
                    }
                }
            }
            Err(err) => {
                if is_cdc_snapshot_handoff_wait_error(err) {
                    self.block_window_for_snapshot_handoff(
                        &job_ids,
                        &table_key,
                        &payload.source_table,
                        started_at,
                    )
                    .await;
                    false
                } else {
                    self.fail_reducer_window(records, &table_key, started_at, err)
                        .await
                }
            }
        };

        let resolve_waiters = match &result {
            Ok(()) => terminal_state_committed,
            Err(err) => {
                should_resolve_cdc_batch_load_waiters(
                    classify_sync_retry(err),
                    self.local_retry_retryable_failures,
                ) && terminal_state_committed
            }
        };

        if resolve_waiters {
            let waiter_result = match &result {
                Ok(()) => Ok(()),
                Err(err) => Err(etl::etl_error!(
                    ErrorKind::DestinationError,
                    "queued CDC batch-load job failed",
                    err.to_string()
                )),
            };
            self.resolve_window_waiters(&job_ids, waiter_result).await;
        }
        self.notify.notify_one();
    }

    async fn block_window_for_snapshot_handoff(
        &self,
        job_ids: &[String],
        table_key: &str,
        source_table: &str,
        started_at: Instant,
    ) {
        let reason = cdc_snapshot_handoff_wait_reason(source_table);
        match self
            .state_handle
            .mark_cdc_batch_load_window_blocked_for_snapshot_handoff(job_ids, &reason)
            .await
        {
            Ok(()) => {
                info!(
                    component = "consumer",
                    event = "cdc_consumer_window_waiting_snapshot_handoff",
                    connection_id = self.state_handle.connection_id(),
                    table = %table_key,
                    source_table,
                    job_count = job_ids.len(),
                    duration_ms = started_at.elapsed().as_millis() as u64,
                    "queued CDC batch-load reducer window is waiting for snapshot handoff"
                );
            }
            Err(err) => {
                warn!(
                    component = "consumer",
                    event = "cdc_consumer_window_snapshot_handoff_state_update_failed",
                    connection_id = self.state_handle.connection_id(),
                    table = %table_key,
                    source_table,
                    job_count = job_ids.len(),
                    error = %err,
                    "failed to mark queued CDC batch-load reducer window waiting for snapshot handoff"
                );
            }
        }
        self.notify.notify_one();
    }

    async fn resolve_window_waiters(&self, job_ids: &[String], result: EtlResult<()>) {
        let mut waiters_by_job = self.waiters.lock().await;
        let mut result = Some(result);
        for job_id in job_ids {
            if let Some(waiters) = waiters_by_job.remove(job_id) {
                for waiter in waiters {
                    let waiter_result = match result.take() {
                        Some(Ok(())) | None => Ok(()),
                        Some(Err(err)) => {
                            let message = err.to_string();
                            result = Some(Err(err));
                            Err(etl::etl_error!(
                                ErrorKind::DestinationError,
                                "queued CDC batch-load job failed",
                                message
                            ))
                        }
                    };
                    let _ignored = waiter.send(waiter_result);
                }
            }
        }
    }

    async fn run_apply_job(&self, job: CdcQueuedBatchLoadJob) {
        let job_id = job.record.job_id.clone();
        let table_key = job.record.table_key.clone();
        let record = job.record.clone();
        let apply_attempt_key = job.apply_attempt_key();
        let started_at = Instant::now();
        let span = info_span!(
            "cdc_batch_load_job",
            job_id = %job_id,
            table = %table_key,
            first_sequence = job.record.first_sequence,
            step_count = job.payload.steps.len()
        );
        let (heartbeat_stop_tx, mut heartbeat_stop_rx) = oneshot::channel();
        let heartbeat_handle = {
            let state_handle = self.state_handle.clone();
            let heartbeat_job_id = job_id.clone();
            let heartbeat_table = table_key.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        () = tokio::time::sleep(CDC_BATCH_LOAD_JOB_HEARTBEAT_INTERVAL) => {
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
        let result: anyhow::Result<()> = Box::pin(cdc_batch_load_job_with_timeout(
            CDC_BATCH_LOAD_JOB_HARD_TIMEOUT,
            &job_id,
            &table_key,
            async {
                if should_check_table_apply_readiness(self.mixed_mode_gate_enabled()) {
                    match self.table_apply_readiness(&job.payload.source_table).await {
                        Ok(CdcApplyReadiness::Ready) => {
                            self.inner
                                .apply_cdc_batch_load_job(
                                    self.state_handle.connection_id(),
                                    &job.payload,
                                    &apply_attempt_key,
                                )
                                .await
                        }
                        Ok(CdcApplyReadiness::Snapshotting) => Err(anyhow::anyhow!(
                            "{}",
                            cdc_snapshot_handoff_wait_reason(&job.payload.source_table)
                        )),
                        Ok(CdcApplyReadiness::Blocked) => {
                            info!(
                                component = "consumer",
                                event = "cdc_consumer_job_skipped_blocked_table",
                                connection_id = self.state_handle.connection_id(),
                                job_id = %job_id,
                                table = %table_key,
                                "skipping queued CDC batch-load job because table is blocked until manual resync"
                            );
                            Ok(())
                        }
                        Err(err) => Err(anyhow::anyhow!(
                            "failed to inspect CDC apply readiness for {}: {}",
                            job.payload.source_table,
                            err
                        )),
                    }
                } else {
                    self.inner
                        .apply_cdc_batch_load_job(
                            self.state_handle.connection_id(),
                            &job.payload,
                            &apply_attempt_key,
                        )
                        .await
                }
            }
            .instrument(span),
        ))
        .await;
        let _ = heartbeat_stop_tx.send(());
        let _ = heartbeat_handle.await;

        let terminal_state_committed = match &result {
            Ok(()) => {
                match self
                    .state_handle
                    .mark_cdc_batch_load_bundle_succeeded(&job_id)
                    .await
                {
                    Err(err) => {
                        warn!(
                            component = "consumer",
                            event = "cdc_consumer_job_success_state_update_failed",
                            connection_id = self.state_handle.connection_id(),
                            job_id = %job_id,
                            table = %table_key,
                            error = %err,
                            "failed to durably mark CDC batch-load job succeeded"
                        );
                        false
                    }
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
                            let row_count =
                                job.payload.steps.iter().map(|step| step.row_count).sum();
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
                        true
                    }
                }
            }
            Err(err) => {
                if is_cdc_snapshot_handoff_wait_error(err) {
                    self.block_window_for_snapshot_handoff(
                        std::slice::from_ref(&job_id),
                        &table_key,
                        &job.payload.source_table,
                        started_at,
                    )
                    .await;
                    false
                } else {
                    self.fail_job(record, &table_key, started_at, err).await
                }
            }
        };

        let resolve_waiters = match &result {
            Ok(()) => terminal_state_committed,
            Err(err) => {
                should_resolve_cdc_batch_load_waiters(
                    classify_sync_retry(err),
                    self.local_retry_retryable_failures,
                ) && terminal_state_committed
            }
        };

        if resolve_waiters && let Some(waiters) = self.waiters.lock().await.remove(&job_id) {
            for waiter in waiters {
                let waiter_result = match &result {
                    Ok(()) => Ok(()),
                    Err(err) => Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "queued CDC batch-load job failed",
                        err.to_string()
                    )),
                };
                let _ignored = waiter.send(waiter_result);
            }
        }
        self.notify.notify_one();
    }

    async fn fail_reducer_window(
        &self,
        records: Vec<CdcBatchLoadJobRecord>,
        table_key: &str,
        started_at: Instant,
        err: &anyhow::Error,
    ) -> bool {
        let job_ids: Vec<String> = records.iter().map(|record| record.job_id.clone()).collect();
        let retry_class = classify_sync_retry(err);
        crate::telemetry::record_cdc_batch_load_job(
            self.state_handle.connection_id(),
            table_key,
            "failed",
            started_at.elapsed().as_secs_f64() * 1000.0,
        );
        warn!(
            component = "consumer",
            event = "cdc_consumer_window_failed",
            connection_id = self.state_handle.connection_id(),
            job_count = job_ids.len(),
            table = %table_key,
            duration_ms = started_at.elapsed().as_millis() as u64,
            error = %err,
            "queued CDC batch-load reducer window failed"
        );
        let mark_fragments_failed = should_mark_cdc_batch_load_fragments_failed(
            retry_class,
            self.local_retry_retryable_failures,
        );
        if let Err(state_err) = self
            .state_handle
            .mark_cdc_batch_load_window_failed(
                &job_ids,
                &format!("{err:#}"),
                retry_class,
                mark_fragments_failed,
            )
            .await
        {
            warn!(
                component = "consumer",
                event = "cdc_consumer_window_failure_state_update_failed",
                connection_id = self.state_handle.connection_id(),
                table = %table_key,
                job_count = job_ids.len(),
                error = %state_err,
                "failed to durably mark CDC batch-load reducer window failed"
            );
            return false;
        }
        if !mark_fragments_failed {
            for record in records {
                let state_handle = self.state_handle.clone();
                let notify = Arc::clone(&self.notify);
                let retry_job_id = record.job_id.clone();
                let retry_table = table_key.to_string();
                let retry_delay = cdc_batch_load_retry_delay(record.attempt_count, &retry_table);
                info!(
                    component = "consumer",
                    event = "cdc_consumer_job_retry_scheduled",
                    connection_id = self.state_handle.connection_id(),
                    job_id = %retry_job_id,
                    table = %retry_table,
                    retry_class = retry_class.as_str(),
                    retry_delay_ms = retry_delay.as_millis() as u64,
                    "scheduled queued CDC batch-load job for local retry"
                );
                tokio::spawn(async move {
                    tokio::time::sleep(retry_delay).await;
                    match state_handle.requeue_cdc_batch_load_job(&retry_job_id).await {
                        Ok(true) => notify.notify_one(),
                        Ok(false) => {}
                        Err(err) => warn!(
                            component = "consumer",
                            event = "cdc_consumer_job_retry_requeue_failed",
                            connection_id = state_handle.connection_id(),
                            job_id = %retry_job_id,
                            table = %retry_table,
                            error = %err,
                            "failed to requeue queued CDC batch-load job for retry"
                        ),
                    }
                });
            }
        }
        true
    }

    async fn fail_job(
        &self,
        record: CdcBatchLoadJobRecord,
        table_key: &str,
        started_at: Instant,
        err: &anyhow::Error,
    ) -> bool {
        let job_id = record.job_id.clone();
        let retry_class = classify_sync_retry(err);
        crate::telemetry::record_cdc_batch_load_job(
            self.state_handle.connection_id(),
            table_key,
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
        let mark_fragments_failed = should_mark_cdc_batch_load_fragments_failed(
            retry_class,
            self.local_retry_retryable_failures,
        );
        if let Err(state_err) = self
            .state_handle
            .mark_cdc_batch_load_bundle_failed(
                &job_id,
                &format!("{err:#}"),
                retry_class,
                mark_fragments_failed,
            )
            .await
        {
            warn!(
                component = "consumer",
                event = "cdc_consumer_job_failure_state_update_failed",
                connection_id = self.state_handle.connection_id(),
                job_id = %job_id,
                table = %table_key,
                error = %state_err,
                "failed to durably mark CDC batch-load job failed"
            );
            return false;
        }
        if !mark_fragments_failed {
            let state_handle = self.state_handle.clone();
            let notify = Arc::clone(&self.notify);
            let retry_job_id = job_id.clone();
            let retry_table = table_key.to_string();
            let retry_delay = cdc_batch_load_retry_delay(record.attempt_count, &retry_table);
            info!(
                component = "consumer",
                event = "cdc_consumer_job_retry_scheduled",
                connection_id = self.state_handle.connection_id(),
                job_id = %retry_job_id,
                table = %retry_table,
                retry_class = retry_class.as_str(),
                retry_delay_ms = retry_delay.as_millis() as u64,
                "scheduled queued CDC batch-load job for local retry"
            );
            tokio::spawn(async move {
                tokio::time::sleep(retry_delay).await;
                match state_handle.requeue_cdc_batch_load_job(&retry_job_id).await {
                    Ok(true) => notify.notify_one(),
                    Ok(false) => {}
                    Err(err) => warn!(
                        component = "consumer",
                        event = "cdc_consumer_job_retry_requeue_failed",
                        connection_id = state_handle.connection_id(),
                        job_id = %retry_job_id,
                        table = %retry_table,
                        error = %err,
                        "failed to requeue queued CDC batch-load job for retry"
                    ),
                }
            });
        }
        true
    }
}

fn work_barrier_kind(truncate: bool) -> Option<String> {
    truncate.then(|| "truncate".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_batch_load_failures_keep_waiters_and_fragments_open() {
        assert!(!should_resolve_cdc_batch_load_waiters(
            SyncRetryClass::Backpressure,
            true
        ));
        assert!(!should_mark_cdc_batch_load_fragments_failed(
            SyncRetryClass::Transient,
            true
        ));
    }

    #[test]
    fn permanent_batch_load_failures_resolve_waiters_and_fail_fragments() {
        assert!(should_resolve_cdc_batch_load_waiters(
            SyncRetryClass::Permanent,
            true
        ));
        assert!(should_mark_cdc_batch_load_fragments_failed(
            SyncRetryClass::Permanent,
            true
        ));
    }

    #[test]
    fn one_shot_batch_load_failures_surface_retryable_errors() {
        assert!(should_resolve_cdc_batch_load_waiters(
            SyncRetryClass::Backpressure,
            false
        ));
        assert!(should_mark_cdc_batch_load_fragments_failed(
            SyncRetryClass::Transient,
            false
        ));
    }

    #[test]
    fn batch_load_retry_delay_uses_bounded_retry_backoff() {
        let delay = cdc_batch_load_retry_delay(1, "public.accounts");
        assert!(delay >= Duration::from_secs(16));
        assert!(delay <= Duration::from_secs(20));
    }

    #[test]
    fn batch_load_retry_due_uses_durable_failure_timestamp() {
        let record = CdcBatchLoadJobRecord {
            table_key: "public.accounts".to_string(),
            attempt_count: 1,
            updated_at: 1_000,
            ..Default::default()
        };
        let due_at = cdc_batch_load_retry_due_at_ms(&record);
        assert!(!cdc_batch_load_retry_is_due(&record, due_at - 1));
        assert!(cdc_batch_load_retry_is_due(&record, due_at));
    }

    #[test]
    fn retry_due_filter_does_not_depend_on_failed_job_order() {
        let now = 2_000_000;
        let not_due = CdcBatchLoadJobRecord {
            job_id: "not-due".to_string(),
            table_key: "public.accounts".to_string(),
            attempt_count: 30,
            updated_at: now - 100_000,
            retry_class: Some(SyncRetryClass::Transient),
            ..Default::default()
        };
        let due = CdcBatchLoadJobRecord {
            job_id: "due".to_string(),
            table_key: "public.accounts".to_string(),
            attempt_count: 1,
            updated_at: now - 100_000,
            retry_class: Some(SyncRetryClass::Transient),
            ..Default::default()
        };

        assert!(!cdc_batch_load_retry_is_due(&not_due, now));
        assert!(cdc_batch_load_retry_is_due(&due, now));
        let due_jobs = due_retryable_failed_cdc_batch_load_jobs(vec![not_due, due], now);

        assert_eq!(due_jobs.len(), 1);
        assert_eq!(due_jobs[0].job_id, "due");
    }

    fn test_payload(job_id: &str) -> CdcBatchLoadJobPayload {
        CdcBatchLoadJobPayload {
            job_id: job_id.to_string(),
            source_table: "public.accounts".to_string(),
            target_table: "public__accounts".to_string(),
            schema: TableSchema {
                name: "public__accounts".to_string(),
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
            },
            staging_schema: None,
            primary_key: "id".to_string(),
            truncate: false,
            steps: Vec::new(),
        }
    }

    #[test]
    fn queued_job_apply_attempt_key_changes_after_durable_retry_attempt_changes() {
        let first_attempt = CdcBatchLoadJobRecord {
            job_id: "cdc_job_abc".to_string(),
            attempt_count: 1,
            ..Default::default()
        };
        let retry_attempt = CdcBatchLoadJobRecord {
            attempt_count: 2,
            ..first_attempt.clone()
        };
        let first_job = super::super::CdcQueuedBatchLoadJob {
            record: first_attempt,
            payload: test_payload("cdc_job_abc"),
        };
        let retry_job = super::super::CdcQueuedBatchLoadJob {
            record: retry_attempt,
            payload: test_payload("cdc_job_abc"),
        };

        assert_eq!(first_job.apply_attempt_key(), "cdc_job_abc:1");
        assert_ne!(first_job.apply_attempt_key(), retry_job.apply_attempt_key());
    }

    #[test]
    fn queued_window_apply_attempt_key_keeps_job_attempts_ordered() {
        let window = super::super::CdcQueuedBatchLoadWindow {
            jobs: vec![
                super::super::CdcQueuedBatchLoadJob {
                    record: CdcBatchLoadJobRecord {
                        job_id: "job-1".to_string(),
                        attempt_count: 3,
                        ..Default::default()
                    },
                    payload: test_payload("job-1"),
                },
                super::super::CdcQueuedBatchLoadJob {
                    record: CdcBatchLoadJobRecord {
                        job_id: "job-2".to_string(),
                        attempt_count: 1,
                        ..Default::default()
                    },
                    payload: test_payload("job-2"),
                },
            ],
        };

        assert_eq!(window.apply_attempt_key(), "job-1:3,job-2:1");
    }

    #[tokio::test]
    async fn manager_requeues_due_failed_jobs_from_durable_state() -> anyhow::Result<()> {
        let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL").ok() else {
            return Ok(());
        };
        let state_config = crate::config::StateConfig {
            url: pg_url,
            schema: Some(format!(
                "cdsync_state_queue_{}",
                uuid::Uuid::new_v4().simple()
            )),
        };
        crate::state::SyncStateStore::migrate_with_config(&state_config, 16).await?;
        let store = crate::state::SyncStateStore::open_with_config(&state_config, 16).await?;
        let handle = store.handle("app");
        let now = Utc::now().timestamp_millis();

        for (job_id, updated_at) in [
            ("job-retry-due", now - 2_000_000),
            ("job-retry-not-due", now),
        ] {
            handle
                .enqueue_cdc_batch_load_bundle(
                    &CdcBatchLoadJobRecord {
                        job_id: job_id.to_string(),
                        table_key: "public__accounts".to_string(),
                        first_sequence: 10,
                        status: CdcBatchLoadJobStatus::Failed,
                        stage: CdcLedgerStage::Failed,
                        payload_json: "{}".to_string(),
                        attempt_count: 1,
                        retry_class: Some(SyncRetryClass::Transient),
                        last_error: Some("transient failure".to_string()),
                        created_at: updated_at,
                        updated_at,
                        ..Default::default()
                    },
                    &[],
                )
                .await?;
        }

        let inner = BigQueryDestination::new(
            crate::config::BigQueryConfig {
                project_id: "project".to_string(),
                dataset: "dataset".to_string(),
                location: None,
                service_account_key_path: None,
                service_account_key: None,
                partition_by_synced_at: Some(false),
                batch_load_bucket: None,
                batch_load_prefix: None,
                emulator_http: Some("http://localhost:9050".to_string()),
                emulator_grpc: Some("localhost:9051".to_string()),
            },
            true,
            MetadataColumns::default(),
        )
        .await?;
        let manager = CdcBatchLoadManager {
            inner,
            stats: None,
            state_handle: handle.clone(),
            mixed_mode_gate_enabled: Arc::new(AtomicBool::new(false)),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            reducer_max_jobs: 1,
            local_retry_retryable_failures: true,
        };

        assert_eq!(manager.requeue_due_retryable_failed_jobs().await?, 1);

        let pending = store
            .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
            .await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].job_id, "job-retry-due");
        assert_eq!(pending[0].stage, CdcLedgerStage::Received);
        assert_eq!(pending[0].last_error, None);

        let failed = store
            .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Failed])
            .await?;
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].job_id, "job-retry-not-due");

        Ok(())
    }

    #[test]
    fn checkpoint_readiness_checks_only_run_in_mixed_mode() {
        assert!(!should_check_table_apply_readiness(false));
        assert!(should_check_table_apply_readiness(true));
    }

    #[test]
    fn snapshot_handoff_errors_are_detected_for_wait_state() {
        let err = anyhow::anyhow!("{}", cdc_snapshot_handoff_wait_reason("public.accounts"));
        assert!(is_cdc_snapshot_handoff_wait_error(&err));

        let other = anyhow::anyhow!("failed to inspect CDC apply readiness for public.accounts");
        assert!(!is_cdc_snapshot_handoff_wait_error(&other));
    }

    #[tokio::test]
    async fn apply_job_snapshot_handoff_path_parks_job_without_failure() -> anyhow::Result<()> {
        let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL").ok() else {
            return Ok(());
        };
        let state_config = crate::config::StateConfig {
            url: pg_url,
            schema: Some(format!(
                "cdsync_state_queue_{}",
                uuid::Uuid::new_v4().simple()
            )),
        };
        crate::state::SyncStateStore::migrate_with_config(&state_config, 16).await?;
        let store = crate::state::SyncStateStore::open_with_config(&state_config, 16).await?;
        let handle = store.handle("app");
        handle
            .save_postgres_checkpoint(
                "public.accounts",
                &TableCheckpoint {
                    snapshot_chunks: vec![crate::types::SnapshotChunkCheckpoint {
                        start_primary_key: Some("1".to_string()),
                        end_primary_key: Some("10".to_string()),
                        last_primary_key: Some("5".to_string()),
                        complete: false,
                    }],
                    ..Default::default()
                },
            )
            .await?;

        let inner = BigQueryDestination::new(
            crate::config::BigQueryConfig {
                project_id: "project".to_string(),
                dataset: "dataset".to_string(),
                location: None,
                service_account_key_path: None,
                service_account_key: None,
                partition_by_synced_at: Some(false),
                batch_load_bucket: None,
                batch_load_prefix: None,
                emulator_http: Some("http://localhost:9050".to_string()),
                emulator_grpc: Some("localhost:9051".to_string()),
            },
            true,
            MetadataColumns::default(),
        )
        .await?;
        let manager = CdcBatchLoadManager {
            inner,
            stats: None,
            state_handle: handle.clone(),
            mixed_mode_gate_enabled: Arc::new(AtomicBool::new(true)),
            waiters: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            reducer_max_jobs: 1,
            local_retry_retryable_failures: true,
        };
        let payload = CdcBatchLoadJobPayload {
            job_id: "job-snapshot-handoff".to_string(),
            source_table: "public.accounts".to_string(),
            target_table: "public__accounts".to_string(),
            schema: TableSchema {
                name: "public__accounts".to_string(),
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
            },
            staging_schema: Some(TableSchema {
                name: "public__accounts".to_string(),
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
            }),
            primary_key: "id".to_string(),
            truncate: false,
            steps: Vec::new(),
        };
        let now = Utc::now().timestamp_millis();
        let record = CdcBatchLoadJobRecord {
            job_id: payload.job_id.clone(),
            table_key: payload.target_table.clone(),
            first_sequence: 10,
            status: CdcBatchLoadJobStatus::Running,
            stage: CdcLedgerStage::Applying,
            payload_json: serde_json::to_string(&payload)?,
            attempt_count: 1,
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        handle
            .enqueue_cdc_batch_load_bundle(
                &record,
                &[CdcCommitFragmentRecord {
                    fragment_id: "job-snapshot-handoff:10".to_string(),
                    job_id: record.job_id.clone(),
                    sequence: 10,
                    commit_lsn: "0/AAA".to_string(),
                    table_key: record.table_key.clone(),
                    status: CdcCommitFragmentStatus::Pending,
                    row_count: 1,
                    upserted_count: 1,
                    created_at: now,
                    updated_at: now,
                    ..Default::default()
                }],
            )
            .await?;

        manager
            .run_apply_job(super::super::CdcQueuedBatchLoadJob { record, payload })
            .await;

        let summary = store.load_cdc_batch_load_queue_summary("app").await?;
        assert_eq!(summary.failed_jobs, 0);
        assert_eq!(summary.snapshot_handoff_waiting_jobs, 1);
        let jobs = store
            .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
            .await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].stage, CdcLedgerStage::Blocked);
        assert_eq!(jobs[0].retry_class, None);

        let fragments = store
            .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
            .await?;
        assert_eq!(fragments.len(), 1);
        assert_eq!(fragments[0].stage, CdcLedgerStage::Blocked);
        assert!(fragments[0].last_error.is_some());

        Ok(())
    }

    #[test]
    fn batch_load_ledger_metadata_preserves_all_steps() {
        let metadata = cdc_batch_load_ledger_metadata_json(&CdcBatchLoadJobPayload {
            job_id: "job-1".to_string(),
            source_table: "public.accounts".to_string(),
            target_table: "public__accounts".to_string(),
            schema: TableSchema {
                name: "public__accounts".to_string(),
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
            },
            staging_schema: Some(cdc_staging_table_schema(&TableSchema {
                name: "public__accounts".to_string(),
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
            })),
            primary_key: "id".to_string(),
            truncate: false,
            steps: vec![
                CdcBatchLoadJobStep {
                    staging_table: "stage_upsert".to_string(),
                    object_uri: "gs://bucket/upsert.parquet".to_string(),
                    load_job_id: Some("load-upsert".to_string()),
                    row_count: 3,
                    upserted_count: 3,
                    deleted_count: 0,
                },
                CdcBatchLoadJobStep {
                    staging_table: "stage_delete".to_string(),
                    object_uri: "gs://bucket/delete.parquet".to_string(),
                    load_job_id: Some("load-delete".to_string()),
                    row_count: 2,
                    upserted_count: 2,
                    deleted_count: 2,
                },
            ],
        })
        .expect("ledger metadata json");
        let value = serde_json::from_str::<serde_json::Value>(&metadata)
            .expect("valid ledger metadata json");

        assert_eq!(value["staging_strategy"], "per_job");
        assert_eq!(value["scalar_metadata"], "first_step_hints");
        assert_eq!(value["steps"].as_array().expect("steps").len(), 2);
        assert_eq!(value["steps"][0]["staging_table"], "stage_upsert");
        assert_eq!(
            value["steps"][1]["object_uri"],
            "gs://bucket/delete.parquet"
        );
        assert_eq!(value["steps"][1]["load_job_id"], "load-delete");
    }

    #[test]
    fn batch_load_payload_can_read_legacy_payload_without_staging_schema() {
        let payload = serde_json::from_str::<CdcBatchLoadJobPayload>(
            r#"{
                "job_id":"job-1",
                "source_table":"public.accounts",
                "target_table":"public__accounts",
                "schema":{"name":"public__accounts","columns":[],"primary_key":"id"},
                "primary_key":"id",
                "truncate":false,
                "steps":[]
            }"#,
        )
        .expect("legacy payload should deserialize");

        assert!(payload.staging_schema.is_none());
    }

    #[tokio::test]
    async fn batch_load_job_timeout_surfaces_stuck_work() {
        let result = cdc_batch_load_job_with_timeout(
            Duration::from_millis(1),
            "cdc_job_stuck",
            "public__accounts",
            std::future::pending::<anyhow::Result<()>>(),
        )
        .await;

        let err = result.expect_err("pending job should time out");
        assert!(err.to_string().contains(
            "CDC batch-load job cdc_job_stuck for public__accounts exceeded hard timeout"
        ));
    }
}
