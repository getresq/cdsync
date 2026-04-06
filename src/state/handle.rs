use super::*;

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

    pub async fn enqueue_cdc_batch_load_job(
        &self,
        job: &CdcBatchLoadJobRecord,
    ) -> anyhow::Result<CdcBatchLoadJobRecord> {
        self.store
            .enqueue_cdc_batch_load_job(&self.connection_id, job)
            .await
    }

    pub async fn load_cdc_batch_load_jobs(
        &self,
        statuses: &[CdcBatchLoadJobStatus],
    ) -> anyhow::Result<Vec<CdcBatchLoadJobRecord>> {
        self.store
            .load_cdc_batch_load_jobs(&self.connection_id, statuses)
            .await
    }

    pub async fn claim_next_cdc_batch_load_job(
        &self,
        stale_running_before_ms: i64,
    ) -> anyhow::Result<Option<CdcBatchLoadJobRecord>> {
        self.store
            .claim_next_cdc_batch_load_job(&self.connection_id, stale_running_before_ms)
            .await
    }

    pub async fn heartbeat_cdc_batch_load_job(&self, job_id: &str) -> anyhow::Result<()> {
        self.store
            .heartbeat_cdc_batch_load_job(&self.connection_id, job_id)
            .await
    }

    pub async fn requeue_cdc_batch_load_running_jobs(&self) -> anyhow::Result<u64> {
        self.store
            .requeue_cdc_batch_load_running_jobs(&self.connection_id)
            .await
    }

    pub async fn requeue_retryable_failed_cdc_batch_load_jobs(&self) -> anyhow::Result<u64> {
        self.store
            .requeue_retryable_failed_cdc_batch_load_jobs(&self.connection_id)
            .await
    }

    pub async fn mark_cdc_batch_load_job_succeeded(&self, job_id: &str) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_job_succeeded(&self.connection_id, job_id)
            .await
    }

    pub async fn mark_cdc_batch_load_job_failed(
        &self,
        job_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_batch_load_job_failed(&self.connection_id, job_id, error)
            .await
    }

    pub async fn upsert_cdc_commit_fragments(
        &self,
        fragments: &[CdcCommitFragmentRecord],
    ) -> anyhow::Result<()> {
        self.store
            .upsert_cdc_commit_fragments(&self.connection_id, fragments)
            .await
    }

    pub async fn mark_cdc_commit_fragments_succeeded_for_job(
        &self,
        job_id: &str,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_commit_fragments_succeeded_for_job(&self.connection_id, job_id)
            .await
    }

    pub async fn mark_cdc_commit_fragments_failed_for_job(
        &self,
        job_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        self.store
            .mark_cdc_commit_fragments_failed_for_job(&self.connection_id, job_id, error)
            .await
    }

    pub async fn save_cdc_watermark_state(&self, state: &CdcWatermarkState) -> anyhow::Result<()> {
        self.store
            .save_cdc_watermark_state(&self.connection_id, state)
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

    pub async fn load_cdc_watermark_state(&self) -> anyhow::Result<Option<CdcWatermarkState>> {
        self.store
            .load_cdc_watermark_state(&self.connection_id)
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
