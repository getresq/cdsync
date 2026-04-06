use super::*;
use crate::destinations::bigquery;

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
                let _ignored = waiter.send(waiter_result);
            }
        }
        self.notify.notify_one();
    }
}
