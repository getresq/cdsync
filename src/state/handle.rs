use super::*;
use crate::retry::SyncRetryClass;

impl StateHandle {
    pub async fn save_connection_state(
        &self,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.store
            .save_connection_state(&self.connection_id, connection_state)
            .await
    }

    pub async fn save_connection_meta(
        &self,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.store
            .save_connection_meta(&self.connection_id, connection_state)
            .await
    }

    pub async fn save_postgres_checkpoint(
        &self,
        table_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        self.store
            .save_table_checkpoint(&self.connection_id, "postgres", table_name, checkpoint)
            .await
    }

    pub async fn save_postgres_cdc_state(
        &self,
        cdc_state: &PostgresCdcState,
    ) -> anyhow::Result<()> {
        self.store
            .save_postgres_cdc_state(&self.connection_id, cdc_state)
            .await
    }

    pub async fn enqueue_cdc_batch_load_bundle(
        &self,
        job: &CdcBatchLoadJobRecord,
        fragments: &[CdcCommitFragmentRecord],
    ) -> anyhow::Result<CdcBatchLoadJobRecord> {
        self.store
            .enqueue_cdc_batch_load_bundle(&self.connection_id, job, fragments)
            .await
    }

    pub async fn claim_next_cdc_batch_load_staging_job(
        &self,
        stale_staged_before_ms: i64,
    ) -> anyhow::Result<Option<CdcBatchLoadJobRecord>> {
        self.store
            .claim_next_cdc_batch_load_staging_job(&self.connection_id, stale_staged_before_ms)
            .await
    }

    pub async fn mark_cdc_batch_load_job_loaded(&self, job_id: &str) -> anyhow::Result<bool> {
        self.store
            .mark_cdc_batch_load_job_loaded(&self.connection_id, job_id)
            .await
    }

    pub async fn claim_next_loaded_cdc_batch_load_job_window_for_apply(
        &self,
        stale_running_before_ms: i64,
        max_jobs: usize,
    ) -> anyhow::Result<Vec<CdcBatchLoadJobRecord>> {
        self.store
            .claim_next_loaded_cdc_batch_load_job_window_for_apply(
                &self.connection_id,
                stale_running_before_ms,
                max_jobs,
            )
            .await
    }

    pub async fn heartbeat_cdc_batch_load_job(&self, job_id: &str) -> anyhow::Result<()> {
        self.store
            .heartbeat_cdc_batch_load_job(&self.connection_id, job_id)
            .await
    }

    pub async fn heartbeat_cdc_batch_load_jobs(&self, job_ids: &[String]) -> anyhow::Result<()> {
        self.store
            .heartbeat_cdc_batch_load_jobs(&self.connection_id, job_ids)
            .await
    }

    pub async fn discard_inflight_cdc_batch_load_state_for_replay(
        &self,
    ) -> anyhow::Result<CdcReplayCleanupSummary> {
        self.store
            .discard_inflight_cdc_batch_load_state_for_replay(&self.connection_id)
            .await
    }

    pub async fn requeue_cdc_batch_load_job(&self, job_id: &str) -> anyhow::Result<bool> {
        self.store
            .requeue_cdc_batch_load_job(&self.connection_id, job_id)
            .await
    }

    pub async fn mark_cdc_batch_load_window_blocked_for_snapshot_handoff(
        &self,
        job_ids: &[String],
        error: &str,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_window_blocked_for_snapshot_handoff(
                &self.connection_id,
                job_ids,
                error,
            )
            .await
    }

    pub async fn mark_cdc_batch_load_bundle_succeeded(&self, job_id: &str) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_bundle_succeeded(&self.connection_id, job_id)
            .await
    }

    pub async fn mark_cdc_batch_load_window_succeeded(
        &self,
        job_ids: &[String],
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_window_succeeded(&self.connection_id, job_ids)
            .await
    }

    pub async fn mark_cdc_batch_load_bundle_failed(
        &self,
        job_id: &str,
        error: &str,
        retry_class: SyncRetryClass,
        mark_fragments_failed: bool,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_bundle_failed(
                &self.connection_id,
                job_id,
                error,
                retry_class,
                mark_fragments_failed,
            )
            .await
    }

    pub async fn mark_cdc_batch_load_window_failed(
        &self,
        job_ids: &[String],
        error: &str,
        retry_class: SyncRetryClass,
        mark_fragments_failed: bool,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_window_failed(
                &self.connection_id,
                job_ids,
                error,
                retry_class,
                mark_fragments_failed,
            )
            .await
    }

    pub async fn save_cdc_feedback_state(&self, state: &CdcWatermarkState) -> anyhow::Result<()> {
        self.store
            .save_cdc_feedback_state(&self.connection_id, state)
            .await
    }

    pub async fn load_cdc_durable_apply_frontier(
        &self,
        from_sequence: u64,
        max_sequences: usize,
    ) -> anyhow::Result<Option<CdcDurableApplyFrontier>> {
        self.store
            .load_cdc_durable_apply_frontier(&self.connection_id, from_sequence, max_sequences)
            .await
    }

    pub async fn load_postgres_table_resync_requests(
        &self,
    ) -> anyhow::Result<Vec<PostgresTableResyncRequest>> {
        self.store
            .load_postgres_table_resync_requests(&self.connection_id)
            .await
    }

    pub async fn clear_postgres_table_resync_request(
        &self,
        source_table: &str,
    ) -> anyhow::Result<()> {
        self.store
            .clear_postgres_table_resync_request(&self.connection_id, source_table)
            .await
    }

    pub async fn load_cdc_feedback_state(&self) -> anyhow::Result<Option<CdcWatermarkState>> {
        self.store
            .load_cdc_feedback_state(&self.connection_id)
            .await
    }

    pub async fn load_cdc_watermark_state(&self) -> anyhow::Result<Option<CdcWatermarkState>> {
        self.store
            .load_cdc_watermark_state(&self.connection_id)
            .await
    }

    pub async fn load_cdc_coordinator_summary(
        &self,
        wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary> {
        self.store
            .load_cdc_coordinator_summary(&self.connection_id, wal_bytes_behind_confirmed)
            .await
    }

    pub async fn load_postgres_checkpoint(
        &self,
        table_name: &str,
    ) -> anyhow::Result<Option<TableCheckpoint>> {
        self.store
            .load_table_checkpoint(&self.connection_id, "postgres", table_name)
            .await
    }

    pub async fn load_all_postgres_checkpoints(
        &self,
    ) -> anyhow::Result<HashMap<String, TableCheckpoint>> {
        self.store
            .load_all_table_checkpoints(&self.connection_id, "postgres")
            .await
    }
}

impl ConnectionLease {
    pub async fn release(mut self) -> anyhow::Result<()> {
        let cleanup = self
            .cleanup
            .take()
            .context("connection lease already released")?;
        Self::cleanup_parts(cleanup, self.stop_tx.take(), self.heartbeat_task.take()).await
    }

    async fn cleanup_parts(
        cleanup: Arc<LeaseCleanup>,
        stop_tx: Option<oneshot::Sender<()>>,
        heartbeat_task: Option<JoinHandle<()>>,
    ) -> anyhow::Result<()> {
        if let Some(stop_tx) = stop_tx {
            let _ = stop_tx.send(());
        }
        if let Some(task) = heartbeat_task {
            let _ = task.await;
        }
        cleanup
            .store
            .release_lock(&cleanup.connection_id, &cleanup.owner_id)
            .await
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let stop_tx = self.stop_tx.take();
            let heartbeat_task = self.heartbeat_task.take();
            handle.spawn(async move {
                if let Err(err) =
                    ConnectionLease::cleanup_parts(cleanup, stop_tx, heartbeat_task).await
                {
                    warn!(error = %err, "failed to release connection lease during drop");
                }
            });
        }
    }
}
