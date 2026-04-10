use super::*;
use crate::destinations::bigquery;
use crate::retry::{SyncRetryClass, classify_sync_retry, compute_sync_retry_backoff};
use crate::types::TableCheckpoint;
use crate::types::TableRuntimeStatus;

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
    bigquery::stable_cdc_batch_load_object_name(prefix, target_table, &suffix)
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

fn cdc_batch_load_retry_delay(attempt_count: i32, table_key: &str) -> Duration {
    compute_sync_retry_backoff(
        &format!("cdc_batch_load:{table_key}"),
        attempt_count.max(1) as u32,
        1_000,
    )
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

impl CdcBatchLoadManager {
    pub(super) async fn new(
        inner: BigQueryDestination,
        stats: Option<StatsHandle>,
        state_handle: StateHandle,
        worker_count: usize,
        local_retry_retryable_failures: bool,
    ) -> Result<Self> {
        let manager = Self {
            inner,
            stats,
            state_handle,
            waiters: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
            local_retry_retryable_failures,
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

    async fn worker_loop(&self, worker_index: usize) {
        loop {
            let job = match self.claim_next_job().await {
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
        let requeued_failed = self
            .state_handle
            .requeue_retryable_failed_cdc_batch_load_jobs()
            .await?;
        let pending_jobs = self
            .state_handle
            .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
            .await?;
        if requeued_running > 0 || requeued_failed > 0 || !pending_jobs.is_empty() {
            info!(
                component = "consumer",
                event = "cdc_consumer_restored_jobs",
                connection_id = self.state_handle.connection_id(),
                requeued_running_jobs = requeued_running,
                requeued_failed_jobs = requeued_failed,
                pending_jobs = pending_jobs.len(),
                "restored queued CDC batch-load jobs from durable state"
            );
            self.notify.notify_waiters();
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
        let result: anyhow::Result<()> = match self
            .table_apply_readiness(&job.payload.source_table)
            .await
        {
            Ok(CdcApplyReadiness::Ready) => self
                .inner
                .process_cdc_batch_load_job(self.state_handle.connection_id(), &job.payload)
                .await,
            Ok(CdcApplyReadiness::Snapshotting) => Err(anyhow::anyhow!(
                "CDC batch-load waiting for snapshot handoff for {}",
                job.payload.source_table
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
        };
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
                let retry_class = classify_sync_retry(err);
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
                    .mark_cdc_batch_load_job_failed(&job_id, &err.to_string(), retry_class)
                    .await;
                if should_mark_cdc_batch_load_fragments_failed(
                    retry_class,
                    self.local_retry_retryable_failures,
                ) {
                    let _ = self
                        .state_handle
                        .mark_cdc_commit_fragments_failed_for_job(&job_id, &err.to_string())
                        .await;
                } else {
                    let state_handle = self.state_handle.clone();
                    let notify = Arc::clone(&self.notify);
                    let retry_job_id = job_id.clone();
                    let retry_table = table_key.clone();
                    let retry_delay =
                        cdc_batch_load_retry_delay(job.record.attempt_count, &retry_table);
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
        }

        let resolve_waiters = match &result {
            Ok(()) => true,
            Err(err) => should_resolve_cdc_batch_load_waiters(
                classify_sync_retry(err),
                self.local_retry_retryable_failures,
            ),
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
}
