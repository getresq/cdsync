use super::*;
use crate::retry::SyncRetryClass;

const CDC_BATCH_LOAD_CLAIM_HEAD_SCAN_LIMIT: i64 = 128;

fn cdc_batch_load_reducer_eligible(job: &CdcBatchLoadJobRecord) -> bool {
    if job.barrier_kind.is_some() {
        return false;
    }
    serde_json::from_str::<serde_json::Value>(&job.payload_json)
        .ok()
        .and_then(|value| value.get("staging_schema").cloned())
        .is_some_and(|staging_schema| !staging_schema.is_null())
}

fn distinct_table_keys(rows: &[PgRow]) -> anyhow::Result<Vec<String>> {
    let mut table_keys = std::collections::BTreeSet::new();
    for row in rows {
        table_keys.insert(row.try_get::<String, _>("table_key")?);
    }
    Ok(table_keys.into_iter().collect())
}

impl SyncStateStore {
    async fn refresh_cdc_batch_load_ready_heads_for_tables(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        connection_id: &str,
        table_keys: &[String],
    ) -> anyhow::Result<()> {
        if table_keys.is_empty() {
            return Ok(());
        }
        sqlx::query(&format!(
            r#"
            delete from {}
            where connection_id = $1
              and table_key = any($2)
            "#,
            self.table("cdc_batch_load_ready_heads")
        ))
        .bind(connection_id)
        .bind(table_keys)
        .execute(&mut **tx)
        .await?;

        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                table_key,
                head_job_id,
                first_sequence,
                created_at,
                updated_at
            )
            select connection_id,
                   table_key,
                   job_id as head_job_id,
                   first_sequence,
                   created_at,
                   $5 as updated_at
            from (
                select distinct on (connection_id, table_key)
                       connection_id,
                       table_key,
                       job_id,
                       first_sequence,
                       created_at
                from {}
                where connection_id = $1
                  and table_key = any($2)
                  and status in ($3, $4, $6)
                order by connection_id, table_key, first_sequence, created_at
            ) heads
            on conflict (connection_id, table_key) do update set
                head_job_id = excluded.head_job_id,
                first_sequence = excluded.first_sequence,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
            self.table("cdc_batch_load_ready_heads"),
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(table_keys)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub async fn enqueue_cdc_batch_load_bundle(
        &self,
        connection_id: &str,
        job: &CdcBatchLoadJobRecord,
        fragments: &[CdcCommitFragmentRecord],
    ) -> anyhow::Result<CdcBatchLoadJobRecord> {
        let mut tx = self.pool.begin().await?;
        let table = self.table("cdc_batch_load_jobs");
        let row = sqlx::query(&format!(
            r#"
            insert into {table} (
                connection_id,
                job_id,
                table_key,
                first_sequence,
                status,
                stage,
                payload_json,
                attempt_count,
                retry_class,
                last_error,
                staging_table,
                artifact_uri,
                load_job_id,
                merge_job_id,
                primary_key_lane,
                barrier_kind,
                ledger_metadata_json,
                reducer_eligible,
                created_at,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
            on conflict(job_id) do update set
                table_key = case
                    when {table}.status = 'failed' then excluded.table_key
                    else {table}.table_key
                end,
                first_sequence = case
                    when {table}.status = 'failed' then excluded.first_sequence
                    else {table}.first_sequence
                end,
                status = case
                    when {table}.status = 'failed' then excluded.status
                    else {table}.status
                end,
                stage = case
                    when {table}.status = 'failed' then excluded.stage
                    else {table}.stage
                end,
                payload_json = case
                    when {table}.status = 'failed' then excluded.payload_json
                    else {table}.payload_json
                end,
                attempt_count = {table}.attempt_count,
                retry_class = case
                    when {table}.status = 'failed' then excluded.retry_class
                    else {table}.retry_class
                end,
                last_error = case
                    when {table}.status = 'failed' then null
                    else {table}.last_error
                end,
                staging_table = case
                    when {table}.status = 'failed' then excluded.staging_table
                    else {table}.staging_table
                end,
                artifact_uri = case
                    when {table}.status = 'failed' then excluded.artifact_uri
                    else {table}.artifact_uri
                end,
                load_job_id = case
                    when {table}.status = 'failed' then excluded.load_job_id
                    else {table}.load_job_id
                end,
                merge_job_id = case
                    when {table}.status = 'failed' then excluded.merge_job_id
                    else {table}.merge_job_id
                end,
                primary_key_lane = case
                    when {table}.status = 'failed' then excluded.primary_key_lane
                    else {table}.primary_key_lane
                end,
                barrier_kind = case
                    when {table}.status = 'failed' then excluded.barrier_kind
                    else {table}.barrier_kind
                end,
                ledger_metadata_json = case
                    when {table}.status = 'failed' then excluded.ledger_metadata_json
                    else {table}.ledger_metadata_json
                end,
                reducer_eligible = case
                    when {table}.status = 'failed' then excluded.reducer_eligible
                    else {table}.reducer_eligible
                end,
                updated_at = case
                    when {table}.status = 'failed' then excluded.updated_at
                    else {table}.updated_at
                end
            returning job_id, table_key, first_sequence, status, payload_json, attempt_count,
                      retry_class, last_error, stage, staging_table, artifact_uri, load_job_id,
                      merge_job_id, primary_key_lane, barrier_kind, ledger_metadata_json,
                      created_at, updated_at
            "#,
            table = table,
        ))
        .bind(connection_id)
        .bind(&job.job_id)
        .bind(&job.table_key)
        .bind(saturating_u64_to_i64(job.first_sequence))
        .bind(job.status.as_str())
        .bind(
            CdcLedgerStage::normalize_for_job_status(job.stage, job.status)
                .as_str(),
        )
        .bind(&job.payload_json)
        .bind(job.attempt_count)
        .bind(job.retry_class.map(SyncRetryClass::as_str))
        .bind(job.last_error.clone())
        .bind(job.staging_table.clone())
        .bind(job.artifact_uri.clone())
        .bind(job.load_job_id.clone())
        .bind(job.merge_job_id.clone())
        .bind(job.primary_key_lane.clone())
        .bind(job.barrier_kind.clone())
        .bind(job.ledger_metadata_json.clone())
        .bind(cdc_batch_load_reducer_eligible(job))
        .bind(job.created_at)
        .bind(job.updated_at)
        .fetch_one(&mut *tx)
        .await?;

        let fragment_table = self.table("cdc_commit_fragments");
        let sequence_table = self.table("cdc_commit_sequences");
        for fragment in fragments {
            sqlx::query(&format!(
                r#"
                insert into {sequence_table} (
                    connection_id,
                    sequence,
                    commit_lsn,
                    expected_fragments,
                    created_at,
                    updated_at
                ) values ($1, $2, $3, $4, $5, $6)
                on conflict(connection_id, sequence) do update set
                    commit_lsn = excluded.commit_lsn,
                    expected_fragments = greatest(
                        {sequence_table}.expected_fragments,
                        excluded.expected_fragments
                    ),
                    updated_at = greatest({sequence_table}.updated_at, excluded.updated_at)
                "#,
                sequence_table = sequence_table,
            ))
            .bind(connection_id)
            .bind(saturating_u64_to_i64(fragment.sequence))
            .bind(&fragment.commit_lsn)
            .bind(fragment.expected_fragments.max(1))
            .bind(fragment.created_at)
            .bind(fragment.updated_at)
            .execute(&mut *tx)
            .await?;

            sqlx::query(&format!(
                r#"
                insert into {fragment_table} (
                    connection_id,
                    fragment_id,
                    job_id,
                    sequence,
                    commit_lsn,
                    table_key,
                    status,
                    stage,
                    expected_fragments,
                    row_count,
                    upserted_count,
                    deleted_count,
                    last_error,
                    artifact_uri,
                    staging_table,
                    load_job_id,
                    merge_job_id,
                    primary_key_lane,
                    barrier_kind,
                    ledger_metadata_json,
                    created_at,
                    updated_at
                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
                on conflict(fragment_id) do update set
                    job_id = excluded.job_id,
                    sequence = excluded.sequence,
                    commit_lsn = excluded.commit_lsn,
                    table_key = excluded.table_key,
                    expected_fragments = greatest(
                        coalesce({fragment_table}.expected_fragments, 1),
                        excluded.expected_fragments
                    ),
                    status = case
                        when {fragment_table}.status = 'failed' then excluded.status
                        else {fragment_table}.status
                    end,
                    stage = case
                        when {fragment_table}.status = 'failed' then excluded.stage
                        else {fragment_table}.stage
                    end,
                    row_count = excluded.row_count,
                    upserted_count = excluded.upserted_count,
                    deleted_count = excluded.deleted_count,
                    last_error = case
                        when {fragment_table}.status = 'failed' then excluded.last_error
                        else {fragment_table}.last_error
                    end,
                    artifact_uri = excluded.artifact_uri,
                    staging_table = excluded.staging_table,
                    load_job_id = excluded.load_job_id,
                    merge_job_id = excluded.merge_job_id,
                    primary_key_lane = excluded.primary_key_lane,
                    barrier_kind = excluded.barrier_kind,
                    ledger_metadata_json = excluded.ledger_metadata_json,
                    updated_at = case
                        when {fragment_table}.status = 'failed' then excluded.updated_at
                        else {fragment_table}.updated_at
                    end
                "#,
                fragment_table = fragment_table,
            ))
            .bind(connection_id)
            .bind(&fragment.fragment_id)
            .bind(&fragment.job_id)
            .bind(saturating_u64_to_i64(fragment.sequence))
            .bind(&fragment.commit_lsn)
            .bind(&fragment.table_key)
            .bind(fragment.status.as_str())
            .bind(
                CdcLedgerStage::normalize_for_fragment_status(fragment.stage, fragment.status)
                    .as_str(),
            )
            .bind(fragment.expected_fragments.max(1))
            .bind(fragment.row_count)
            .bind(fragment.upserted_count)
            .bind(fragment.deleted_count)
            .bind(fragment.last_error.clone())
            .bind(fragment.artifact_uri.clone())
            .bind(fragment.staging_table.clone())
            .bind(fragment.load_job_id.clone())
            .bind(fragment.merge_job_id.clone())
            .bind(fragment.primary_key_lane.clone())
            .bind(fragment.barrier_kind.clone())
            .bind(fragment.ledger_metadata_json.clone())
            .bind(fragment.created_at)
            .bind(fragment.updated_at)
            .execute(&mut *tx)
            .await?;
        }

        self.refresh_cdc_batch_load_ready_heads_for_tables(
            &mut tx,
            connection_id,
            std::slice::from_ref(&job.table_key),
        )
        .await?;
        tx.commit().await?;
        cdc_batch_load_job_record_from_row(&row)
    }

    pub async fn load_cdc_batch_load_jobs(
        &self,
        connection_id: &str,
        statuses: &[CdcBatchLoadJobStatus],
    ) -> anyhow::Result<Vec<CdcBatchLoadJobRecord>> {
        let status_values: Vec<&str> = statuses.iter().map(|status| status.as_str()).collect();
        let rows = sqlx::query(&format!(
            r#"
            select job_id, table_key, first_sequence, status, payload_json, attempt_count,
                   retry_class, last_error, stage, staging_table, artifact_uri, load_job_id,
                   merge_job_id, primary_key_lane, barrier_kind, ledger_metadata_json,
                   created_at, updated_at
            from {}
            where connection_id = $1
              and status = any($2)
            order by first_sequence asc, created_at asc
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(&status_values)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| cdc_batch_load_job_record_from_row(&row))
            .collect()
    }

    pub async fn load_retryable_failed_cdc_batch_load_jobs(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Vec<CdcBatchLoadJobRecord>> {
        let rows = sqlx::query(&format!(
            r#"
            select job_id, table_key, first_sequence, status, payload_json, attempt_count,
                   retry_class, last_error, stage, staging_table, artifact_uri, load_job_id,
                   merge_job_id, primary_key_lane, barrier_kind, ledger_metadata_json,
                   created_at, updated_at
            from {}
            where connection_id = $1
              and status = $2
              and retry_class = any($3)
            order by updated_at asc, first_sequence asc, created_at asc
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind([
            SyncRetryClass::Backpressure.as_str(),
            SyncRetryClass::Transient.as_str(),
        ])
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| cdc_batch_load_job_record_from_row(&row))
            .collect()
    }

    pub async fn claim_next_cdc_batch_load_staging_job(
        &self,
        connection_id: &str,
        stale_staged_before_ms: i64,
    ) -> anyhow::Result<Option<CdcBatchLoadJobRecord>> {
        let now = now_millis();
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(&format!(
            r#"
            with candidate as (
                select j.job_id
                from {} j
                where j.connection_id = $1
                  and j.status = $2
                  and (
                    j.stage = $3
                    or (j.stage = $4 and j.updated_at < $5)
                  )
                order by j.first_sequence asc, j.created_at asc
                for update skip locked
                limit 1
            )
            update {} jobs
            set stage = $4,
                attempt_count = jobs.attempt_count + 1,
                last_error = null,
                updated_at = $6
            from candidate
            where jobs.connection_id = $1
              and jobs.job_id = candidate.job_id
            returning jobs.job_id, jobs.table_key, jobs.first_sequence, jobs.status,
                      jobs.payload_json, jobs.attempt_count, jobs.retry_class, jobs.last_error,
                      jobs.stage, jobs.staging_table, jobs.artifact_uri, jobs.load_job_id,
                      jobs.merge_job_id, jobs.primary_key_lane, jobs.barrier_kind,
                      jobs.ledger_metadata_json,
                      jobs.created_at, jobs.updated_at
            "#,
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcLedgerStage::Received.as_str())
        .bind(CdcLedgerStage::Staged.as_str())
        .bind(stale_staged_before_ms)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(row) = &row {
            self.refresh_cdc_batch_load_ready_heads_for_tables(
                &mut tx,
                connection_id,
                &[row.try_get("table_key")?],
            )
            .await?;
        }
        tx.commit().await?;

        row.as_ref()
            .map(cdc_batch_load_job_record_from_row)
            .transpose()
    }

    pub async fn mark_cdc_batch_load_job_loaded(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<bool> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(&format!(
            r#"
            update {}
            set stage = $3,
                updated_at = $4
            where connection_id = $1
              and job_id = $2
              and status = $5
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcLedgerStage::Loaded.as_str())
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&rows)?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(!table_keys.is_empty())
    }

    pub async fn mark_cdc_batch_load_fragments_durable_landed(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<u64> {
        let now = now_millis();
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $4,
                last_error = null,
                updated_at = $5
            where connection_id = $1
              and job_id = $2
              and status = $6
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(CdcLedgerStage::ApplyPending.as_str())
        .bind(now)
        .bind(CdcCommitFragmentStatus::Pending.as_str())
        .execute(&mut *tx)
        .await?;
        let summary = sqlx::query(&format!(
            r#"
            select count(*)::bigint as total_fragments,
                   count(*) filter (where status = $3)::bigint as durable_fragments,
                   count(*) filter (where status = $4)::bigint as failed_fragments
            from {}
            where connection_id = $1
              and job_id = $2
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(CdcCommitFragmentStatus::Failed.as_str())
        .fetch_one(&mut *tx)
        .await?;
        let total_fragments = summary.try_get::<i64, _>("total_fragments")?;
        let durable_fragments = summary.try_get::<i64, _>("durable_fragments")?;
        let failed_fragments = summary.try_get::<i64, _>("failed_fragments")?;
        if total_fragments <= 0 {
            anyhow::bail!("CDC batch-load job {job_id} has no fragments to mark durable landed");
        }
        if failed_fragments > 0 || durable_fragments != total_fragments {
            anyhow::bail!(
                "CDC batch-load job {job_id} is not fully durable landed: total={total_fragments} durable={durable_fragments} failed={failed_fragments}"
            );
        }
        let job_rows = sqlx::query(&format!(
            r#"
            update {}
            set stage = $3,
                updated_at = $4
            where connection_id = $1
              and job_id = $2
              and status = $5
              and stage = $6
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcLedgerStage::Received.as_str())
        .bind(now)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcLedgerStage::ApplyPending.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&job_rows)?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected())
    }

    pub async fn mark_cdc_batch_load_window_blocked_for_snapshot_handoff(
        &self,
        connection_id: &str,
        job_ids: &[String],
        error: &str,
    ) -> anyhow::Result<()> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let now = now_millis();
        let mut tx = self.pool.begin().await?;
        let job_rows = sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $5,
                retry_class = null,
                last_error = $4,
                updated_at = $6
            where connection_id = $1
              and job_id = any($2)
              and status = $7
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(error)
        .bind(CdcLedgerStage::Blocked.as_str())
        .bind(now)
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&job_rows)?;
        sqlx::query(&format!(
            r#"
            update {}
            set stage = $3,
                last_error = $4,
                updated_at = $5
            where connection_id = $1
              and job_id = any($2)
              and status = $6
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(CdcLedgerStage::Blocked.as_str())
        .bind(error)
        .bind(now)
        .bind(CdcCommitFragmentStatus::Pending.as_str())
        .execute(&mut *tx)
        .await?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn release_snapshot_handoff_blocked_cdc_batch_load_jobs(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<u64> {
        let now = now_millis();
        let jobs_table = self.table("cdc_batch_load_jobs");
        let fragment_table = self.table("cdc_commit_fragments");
        let checkpoints_table = self.table("table_checkpoints");
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(&format!(
            r#"
            with ready_jobs as (
                select j.job_id
                from {jobs_table} j
                left join {checkpoints_table} checkpoint
                  on checkpoint.connection_id = j.connection_id
                 and checkpoint.source_kind = 'postgres'
                 and checkpoint.entity_name = j.payload_json::jsonb ->> 'source_table'
                where j.connection_id = $1
                  and j.status = $2
                  and j.stage = $3
                  and j.last_error like $4
                  and not exists (
                    select 1
                    from jsonb_array_elements(
                        coalesce(checkpoint.checkpoint_json::jsonb -> 'snapshot_chunks', '[]'::jsonb)
                    ) chunk
                    where not coalesce((chunk ->> 'complete')::boolean, false)
                  )
                order by j.first_sequence asc, j.created_at asc
                for update of j skip locked
            )
            update {jobs_table} jobs
            set stage = $5,
                last_error = null,
                updated_at = $6
            from ready_jobs
            where jobs.connection_id = $1
              and jobs.job_id = ready_jobs.job_id
            returning jobs.job_id, jobs.table_key
            "#,
            jobs_table = jobs_table,
            checkpoints_table = checkpoints_table,
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcLedgerStage::Blocked.as_str())
        .bind("CDC batch-load waiting for snapshot handoff%")
        .bind(CdcLedgerStage::Loaded.as_str())
        .bind(now)
        .fetch_all(&mut *tx)
        .await?;
        let job_ids = rows
            .iter()
            .map(|row| row.try_get::<String, _>("job_id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let table_keys = distinct_table_keys(&rows)?;
        if !job_ids.is_empty() {
            sqlx::query(&format!(
                r#"
                update {fragment_table}
                set stage = $3,
                    last_error = null,
                    updated_at = $4
                where connection_id = $1
                  and job_id = any($2)
                  and status = $5
                "#,
                fragment_table = fragment_table
            ))
            .bind(connection_id)
            .bind(&job_ids)
            .bind(CdcLedgerStage::Received.as_str())
            .bind(now)
            .bind(CdcCommitFragmentStatus::Pending.as_str())
            .execute(&mut *tx)
            .await?;
        }
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(job_ids.len() as u64)
    }

    pub async fn claim_next_loaded_cdc_batch_load_job_window_for_apply(
        &self,
        connection_id: &str,
        stale_running_before_ms: i64,
        max_jobs: usize,
        max_fill_ms: i64,
    ) -> anyhow::Result<Vec<CdcBatchLoadJobRecord>> {
        let now = now_millis();
        let claimable_loaded_before_ms = if max_fill_ms <= 0 {
            i64::MAX
        } else {
            now.saturating_sub(max_fill_ms)
        };
        self.release_snapshot_handoff_blocked_cdc_batch_load_jobs(connection_id)
            .await?;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(&format!(
            r#"
            with candidate_heads as (
                select j.job_id,
                       j.table_key,
                       j.first_sequence,
                       j.status,
                       j.updated_at,
                       j.barrier_kind,
                       j.reducer_eligible,
                       j.created_at
                from {} heads
                join {} j
                  on j.connection_id = heads.connection_id
                 and j.job_id = heads.head_job_id
                where j.connection_id = $1
                  and (
                    (j.status = $2 and j.stage = $3)
                    or (j.status = $4 and j.updated_at < $5)
                  )
                order by j.first_sequence asc, j.created_at asc
                limit $11
            ),
            window_candidates as (
                select first.job_id as head_job_id, j.job_id, j.first_sequence, j.created_at
                from {} j
                join candidate_heads first
                  on first.table_key = j.table_key
                where j.connection_id = $1
                  and (
                    j.job_id = first.job_id
                    or (
                      first.barrier_kind is null
                      and first.reducer_eligible
                      and j.status = $2
                      and j.stage = $3
                      and j.barrier_kind is null
                      and j.first_sequence > first.first_sequence
                      and j.reducer_eligible
                      and not exists (
                        select 1
                        from {} blockers
                        where blockers.connection_id = j.connection_id
                          and blockers.table_key = j.table_key
                          and blockers.job_id <> first.job_id
                          and blockers.first_sequence >= first.first_sequence
                          and blockers.first_sequence < j.first_sequence
                          and not (
                            blockers.status = $2
                            and blockers.stage = $3
                            and blockers.barrier_kind is null
                            and blockers.reducer_eligible
                          )
                      )
                    )
                  )
            ),
            window_counts as (
                select head_job_id, count(*) as job_count
                from window_candidates
                group by head_job_id
            ),
            claimable_heads as (
                select first.job_id, first.first_sequence, first.created_at
                from candidate_heads first
                join window_counts counts
                  on counts.head_job_id = first.job_id
                where first.status = $4
                   or first.updated_at <= $10
                   or counts.job_count >= $9
                   or first.barrier_kind is not null
                   or not first.reducer_eligible
            ),
            locked_head as (
                select j.job_id
                from {} j
                join claimable_heads first
                  on first.job_id = j.job_id
                where j.connection_id = $1
                order by first.first_sequence asc, first.created_at asc
                for update of j skip locked
                limit 1
            ),
            chosen_window as (
                select j.job_id
                from {} j
                join window_candidates candidates
                  on candidates.job_id = j.job_id
                join locked_head first
                  on first.job_id = candidates.head_job_id
                where j.connection_id = $1
                order by candidates.first_sequence asc, candidates.created_at asc
                for update of j skip locked
                limit $9
            )
            update {} jobs
            set status = $4,
                stage = $7,
                last_error = null,
                updated_at = $8
            where jobs.connection_id = $1
              and jobs.job_id in (select job_id from chosen_window)
              and (
                (jobs.status = $2 and jobs.stage = $3)
                or (jobs.status = $4 and jobs.updated_at < $5)
              )
            returning jobs.job_id, jobs.table_key, jobs.first_sequence, jobs.status,
                      jobs.payload_json, jobs.attempt_count, jobs.retry_class, jobs.last_error,
                      jobs.stage, jobs.staging_table, jobs.artifact_uri, jobs.load_job_id,
                      jobs.merge_job_id, jobs.primary_key_lane, jobs.barrier_kind,
                      jobs.ledger_metadata_json,
                      jobs.created_at, jobs.updated_at
            "#,
            self.table("cdc_batch_load_ready_heads"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcLedgerStage::Loaded.as_str())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .bind(stale_running_before_ms)
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind(CdcLedgerStage::Applying.as_str())
        .bind(now)
        .bind(saturating_usize_to_i64(max_jobs.max(1)))
        .bind(claimable_loaded_before_ms)
        .bind(CDC_BATCH_LOAD_CLAIM_HEAD_SCAN_LIMIT)
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&rows)?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;

        let mut records = rows
            .iter()
            .map(cdc_batch_load_job_record_from_row)
            .collect::<anyhow::Result<Vec<_>>>()?;
        records.sort_by_key(|record| (record.first_sequence, record.created_at));
        Ok(records)
    }

    pub async fn heartbeat_cdc_batch_load_job(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        self.heartbeat_cdc_batch_load_jobs(connection_id, &[job_id.to_string()])
            .await
    }

    pub async fn heartbeat_cdc_batch_load_jobs(
        &self,
        connection_id: &str,
        job_ids: &[String],
    ) -> anyhow::Result<()> {
        if job_ids.is_empty() {
            return Ok(());
        }
        sqlx::query(&format!(
            r#"
            update {}
            set updated_at = $3
            where connection_id = $1
              and job_id = any($2)
              and (
                status = $4
                or (status = $5 and stage = $6)
              )
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcLedgerStage::Staged.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn discard_inflight_cdc_batch_load_state_for_replay(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<CdcReplayCleanupSummary> {
        let mut tx = self.pool.begin().await?;
        let jobs_table = self.table("cdc_batch_load_jobs");
        let fragment_table = self.table("cdc_commit_fragments");
        let now = now_millis();

        let repair_result = sqlx::query(&format!(
            r#"
            update {fragment_table} fragments
            set status = $3,
                stage = $4,
                last_error = null,
                updated_at = $5
            from {jobs_table} jobs
            where fragments.connection_id = $1
              and jobs.connection_id = fragments.connection_id
              and jobs.job_id = fragments.job_id
              and jobs.status = $2
              and fragments.status <> $3
            "#,
            fragment_table = fragment_table,
            jobs_table = jobs_table,
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Succeeded.as_str())
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(CdcLedgerStage::Applied.as_str())
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let inflight_jobs = self
            .load_cdc_batch_load_jobs(
                connection_id,
                &[
                    CdcBatchLoadJobStatus::Pending,
                    CdcBatchLoadJobStatus::Running,
                ],
            )
            .await?;
        let inflight_job_ids = inflight_jobs
            .iter()
            .map(|job| job.job_id.clone())
            .collect::<Vec<_>>();
        let inflight_table_by_job = inflight_jobs
            .iter()
            .map(|job| (job.job_id.clone(), job.table_key.clone()))
            .collect::<std::collections::HashMap<_, _>>();
        if inflight_job_ids.is_empty() {
            tx.commit().await?;
            return Ok(CdcReplayCleanupSummary {
                repaired_terminal_fragments: repair_result.rows_affected(),
                ..Default::default()
            });
        }

        let durable_job_rows = sqlx::query(&format!(
            r#"
            select job_id
            from {fragment_table}
            where connection_id = $1
              and job_id = any($2)
            group by job_id
            having count(*) > 0
               and count(*) filter (where status = $3) = count(*)
            "#,
            fragment_table = fragment_table,
        ))
        .bind(connection_id)
        .bind(&inflight_job_ids)
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let durable_job_ids = durable_job_rows
            .iter()
            .map(|row| row.try_get::<String, _>("job_id"))
            .collect::<std::result::Result<std::collections::HashSet<_>, _>>()?;
        let doomed_job_ids = inflight_jobs
            .into_iter()
            .filter(|job| !durable_job_ids.contains(&job.job_id))
            .map(|job| job.job_id)
            .collect::<Vec<_>>();
        let doomed_table_keys = doomed_job_ids
            .iter()
            .filter_map(|job_id| inflight_table_by_job.get(job_id).cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if doomed_job_ids.is_empty() {
            tx.commit().await?;
            return Ok(CdcReplayCleanupSummary {
                repaired_terminal_fragments: repair_result.rows_affected(),
                ..Default::default()
            });
        }

        let fragment_result = sqlx::query(&format!(
            r#"
            delete from {}
            where connection_id = $1
              and job_id = any($2)
            "#,
            fragment_table
        ))
        .bind(connection_id)
        .bind(&doomed_job_ids)
        .execute(&mut *tx)
        .await?;

        let job_result = sqlx::query(&format!(
            r#"
            delete from {}
            where connection_id = $1
              and job_id = any($2)
            "#,
            jobs_table
        ))
        .bind(connection_id)
        .bind(&doomed_job_ids)
        .execute(&mut *tx)
        .await?;

        self.refresh_cdc_batch_load_ready_heads_for_tables(
            &mut tx,
            connection_id,
            &doomed_table_keys,
        )
        .await?;
        tx.commit().await?;
        Ok(CdcReplayCleanupSummary {
            discarded_jobs: job_result.rows_affected(),
            discarded_fragments: fragment_result.rows_affected(),
            repaired_terminal_fragments: repair_result.rows_affected(),
        })
    }

    pub async fn requeue_cdc_batch_load_job(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<bool> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $7,
                retry_class = retry_class,
                last_error = null,
                updated_at = $4
            where connection_id = $1
              and job_id = $2
              and status = $5
              and retry_class = any($6)
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind([
            SyncRetryClass::Backpressure.as_str(),
            SyncRetryClass::Transient.as_str(),
        ])
        .bind(CdcLedgerStage::Received.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&rows)?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(!table_keys.is_empty())
    }

    pub async fn mark_cdc_batch_load_bundle_succeeded(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        self.mark_cdc_batch_load_window_succeeded(connection_id, &[job_id.to_string()])
            .await
    }

    pub async fn mark_cdc_batch_load_window_succeeded(
        &self,
        connection_id: &str,
        job_ids: &[String],
    ) -> anyhow::Result<()> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let now = now_millis();
        let mut tx = self.pool.begin().await?;
        let job_rows = sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $5,
                retry_class = null,
                last_error = null,
                updated_at = $4
            where connection_id = $1 and job_id = any($2)
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(CdcBatchLoadJobStatus::Succeeded.as_str())
        .bind(now)
        .bind(CdcLedgerStage::Applied.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&job_rows)?;
        sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $5,
                last_error = null,
                updated_at = $4
            where connection_id = $1 and job_id = any($2)
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(now)
        .bind(CdcLedgerStage::Applied.as_str())
        .execute(&mut *tx)
        .await?;
        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_cdc_batch_load_bundle_failed(
        &self,
        connection_id: &str,
        job_id: &str,
        error: &str,
        retry_class: SyncRetryClass,
        mark_fragments_failed: bool,
    ) -> anyhow::Result<()> {
        self.mark_cdc_batch_load_window_failed(
            connection_id,
            &[job_id.to_string()],
            error,
            retry_class,
            mark_fragments_failed,
        )
        .await
    }

    pub async fn mark_cdc_batch_load_window_failed(
        &self,
        connection_id: &str,
        job_ids: &[String],
        error: &str,
        retry_class: SyncRetryClass,
        mark_fragments_failed: bool,
    ) -> anyhow::Result<()> {
        if job_ids.is_empty() {
            return Ok(());
        }
        let now = now_millis();
        let mut tx = self.pool.begin().await?;
        let job_rows = sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                stage = $7,
                retry_class = $4,
                last_error = $5,
                updated_at = $6
            where connection_id = $1 and job_id = any($2)
            returning table_key
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_ids)
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind(retry_class.as_str())
        .bind(error)
        .bind(now)
        .bind(CdcLedgerStage::Failed.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let table_keys = distinct_table_keys(&job_rows)?;

        if mark_fragments_failed {
            sqlx::query(&format!(
                r#"
                update {}
                set status = $3,
                    stage = $6,
                    last_error = $4,
                    updated_at = $5
                where connection_id = $1
                  and job_id = any($2)
                  and status <> $7
                "#,
                self.table("cdc_commit_fragments")
            ))
            .bind(connection_id)
            .bind(job_ids)
            .bind(CdcCommitFragmentStatus::Failed.as_str())
            .bind(error)
            .bind(now)
            .bind(CdcLedgerStage::Failed.as_str())
            .bind(CdcCommitFragmentStatus::Succeeded.as_str())
            .execute(&mut *tx)
            .await?;
        }

        self.refresh_cdc_batch_load_ready_heads_for_tables(&mut tx, connection_id, &table_keys)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn load_cdc_batch_load_backpressure_summary(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadBackpressureSummary> {
        let now = now_millis();
        let row = sqlx::query(&format!(
            r#"
            select count(*)::bigint as pending_jobs,
                   min(created_at) as oldest_pending_ms
            from {}
            where connection_id = $1
              and status = $2
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .fetch_one(&self.pool)
        .await?;

        let oldest_pending_ms: Option<i64> = row.try_get("oldest_pending_ms")?;
        Ok(CdcBatchLoadBackpressureSummary {
            pending_jobs: row.try_get::<i64, _>("pending_jobs")?,
            oldest_pending_age_seconds: oldest_pending_ms.map(|ts| ((now - ts).max(0)) / 1000),
        })
    }

    pub async fn load_cdc_batch_load_queue_summary(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadQueueSummary> {
        let now = now_millis();
        let one_minute_ago = now - 60_000;
        let fifteen_minutes_ago = now - (15 * 60_000);
        let aggregate = sqlx::query(&format!(
            r#"
            select
                count(*)::bigint as total_jobs,
                count(*) filter (where status = 'pending')::bigint as pending_jobs,
                count(*) filter (where status = 'running')::bigint as running_jobs,
                count(*) filter (where status = 'succeeded')::bigint as succeeded_jobs,
                count(*) filter (where status = 'failed')::bigint as failed_jobs,
                count(*) filter (
                    where status = 'failed'
                      and retry_class in ('backpressure', 'transient')
                )::bigint as failed_retryable_jobs,
                count(*) filter (
                    where status = 'failed'
                      and retry_class = 'permanent'
                )::bigint as failed_permanent_jobs,
                count(*) filter (
                    where status = 'failed'
                      and retry_class in ('backpressure', 'transient')
                      and last_error like 'CDC batch-load waiting for snapshot handoff%'
                )::bigint as failed_snapshot_handoff_jobs,
                count(*) filter (
                    where status = 'failed'
                      and (
                          retry_class is null
                          or retry_class not in ('backpressure', 'transient', 'permanent')
                      )
                )::bigint as failed_unclassified_jobs,
                count(*) filter (where stage = 'received')::bigint as received_jobs,
                count(*) filter (where stage = 'staged')::bigint as staged_jobs,
                count(*) filter (where stage = 'loaded')::bigint as loaded_jobs,
                count(*) filter (where stage = 'blocked')::bigint as blocked_jobs,
                count(*) filter (
                    where stage = 'blocked'
                      and last_error like 'CDC batch-load waiting for snapshot handoff%'
                )::bigint as snapshot_handoff_waiting_jobs,
                count(*) filter (where stage = 'applying')::bigint as applying_jobs,
                count(*) filter (where stage = 'applied')::bigint as applied_jobs,
                count(*) filter (where stage = 'failed')::bigint as failed_stage_jobs,
                min(created_at) filter (where status = 'pending') as oldest_pending_ms,
                min(updated_at) filter (where status = 'running') as oldest_running_ms,
                min(updated_at) filter (where stage = 'received') as oldest_received_ms,
                min(updated_at) filter (where stage = 'loaded') as oldest_loaded_ms,
                min(updated_at) filter (where stage = 'blocked') as oldest_blocked_ms,
                min(updated_at) filter (where stage = 'applying') as oldest_applying_ms,
                count(*) filter (where status = 'succeeded' and updated_at >= $2)::bigint as jobs_per_minute,
                avg((updated_at - created_at)::double precision) filter (where status = 'succeeded' and updated_at >= $3) as avg_job_duration_ms
            from {}
            where connection_id = $1
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(one_minute_ago)
        .bind(fifteen_minutes_ago)
        .fetch_one(&self.pool)
        .await?;

        let queued_tables = sqlx::query(&format!(
            r#"
            with job_rows as (
                select
                    job_id,
                    table_key,
                    coalesce(
                        (
                            select sum((step->>'row_count')::bigint)
                            from jsonb_array_elements((payload_json::jsonb)->'steps') step
                        ),
                        0
                    ) as row_count
                from {}
                where connection_id = $1
                  and status in ('pending', 'running')
            )
            select table_key,
                   count(*)::bigint as queued_jobs,
                   coalesce(sum(row_count), 0)::bigint as queued_rows
            from job_rows
            group by table_key
            order by queued_rows desc, queued_jobs desc, table_key asc
            limit 5
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await?;

        let loaded_tables = sqlx::query(&format!(
            r#"
            with succeeded_jobs as (
                select
                    job_id,
                    table_key,
                    created_at,
                    updated_at,
                    coalesce(
                        (
                            select sum((step->>'row_count')::bigint)
                            from jsonb_array_elements((payload_json::jsonb)->'steps') step
                        ),
                        0
                    ) as row_count
                from {}
                where connection_id = $1
                  and status = 'succeeded'
                  and updated_at >= $2
            )
            select table_key,
                   count(*)::bigint as succeeded_jobs,
                   coalesce(sum(row_count), 0)::bigint as loaded_rows,
                   coalesce(sum((updated_at - created_at)::double precision), 0) as total_duration_ms
            from succeeded_jobs
            group by table_key
            order by total_duration_ms desc, succeeded_jobs desc, table_key asc
            limit 5
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(fifteen_minutes_ago)
        .fetch_all(&self.pool)
        .await?;

        let rows_per_minute = sqlx::query_scalar::<_, i64>(&format!(
            r#"
            with recent_jobs as (
                select
                    coalesce(
                        (
                            select sum((step->>'row_count')::bigint)
                            from jsonb_array_elements((payload_json::jsonb)->'steps') step
                        ),
                        0
                    ) as row_count
                from {}
                where connection_id = $1
                  and status = 'succeeded'
                  and updated_at >= $2
            )
            select coalesce(sum(row_count), 0)::bigint from recent_jobs
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(one_minute_ago)
        .fetch_one(&self.pool)
        .await?;

        let failed = sqlx::query(&format!(
            r#"
            select last_error, retry_class, updated_at
            from {}
            where connection_id = $1
              and status = 'failed'
            order by updated_at desc
            limit 1
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?;

        let oldest_pending_ms: Option<i64> = aggregate.try_get("oldest_pending_ms")?;
        let oldest_running_ms: Option<i64> = aggregate.try_get("oldest_running_ms")?;
        let oldest_received_ms: Option<i64> = aggregate.try_get("oldest_received_ms")?;
        let oldest_loaded_ms: Option<i64> = aggregate.try_get("oldest_loaded_ms")?;
        let oldest_blocked_ms: Option<i64> = aggregate.try_get("oldest_blocked_ms")?;
        let oldest_applying_ms: Option<i64> = aggregate.try_get("oldest_applying_ms")?;
        let avg_job_duration_ms: Option<f64> = aggregate.try_get("avg_job_duration_ms")?;

        Ok(CdcBatchLoadQueueSummary {
            total_jobs: aggregate.try_get::<i64, _>("total_jobs")?,
            pending_jobs: aggregate.try_get::<i64, _>("pending_jobs")?,
            running_jobs: aggregate.try_get::<i64, _>("running_jobs")?,
            succeeded_jobs: aggregate.try_get::<i64, _>("succeeded_jobs")?,
            failed_jobs: aggregate.try_get::<i64, _>("failed_jobs")?,
            failed_retryable_jobs: aggregate.try_get::<i64, _>("failed_retryable_jobs")?,
            failed_permanent_jobs: aggregate.try_get::<i64, _>("failed_permanent_jobs")?,
            failed_snapshot_handoff_jobs: aggregate
                .try_get::<i64, _>("failed_snapshot_handoff_jobs")?,
            failed_unclassified_jobs: aggregate.try_get::<i64, _>("failed_unclassified_jobs")?,
            received_jobs: aggregate.try_get::<i64, _>("received_jobs")?,
            staged_jobs: aggregate.try_get::<i64, _>("staged_jobs")?,
            loaded_jobs: aggregate.try_get::<i64, _>("loaded_jobs")?,
            blocked_jobs: aggregate.try_get::<i64, _>("blocked_jobs")?,
            snapshot_handoff_waiting_jobs: aggregate
                .try_get::<i64, _>("snapshot_handoff_waiting_jobs")?,
            applying_jobs: aggregate.try_get::<i64, _>("applying_jobs")?,
            applied_jobs: aggregate.try_get::<i64, _>("applied_jobs")?,
            failed_stage_jobs: aggregate.try_get::<i64, _>("failed_stage_jobs")?,
            oldest_pending_age_seconds: oldest_pending_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_running_age_seconds: oldest_running_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_received_age_seconds: oldest_received_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_loaded_age_seconds: oldest_loaded_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_blocked_age_seconds: oldest_blocked_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_applying_age_seconds: oldest_applying_ms.map(|ts| ((now - ts).max(0)) / 1000),
            jobs_per_minute: aggregate.try_get::<i64, _>("jobs_per_minute")?,
            rows_per_minute,
            avg_job_duration_seconds: avg_job_duration_ms.map(|value| value / 1000.0),
            top_queued_tables: queued_tables
                .into_iter()
                .map(|row| CdcBatchLoadQueueTableSummary {
                    table_key: row.try_get("table_key").unwrap_or_default(),
                    queued_jobs: row.try_get("queued_jobs").unwrap_or_default(),
                    queued_rows: row.try_get("queued_rows").unwrap_or_default(),
                })
                .collect(),
            top_loaded_tables: loaded_tables
                .into_iter()
                .map(|row| CdcBatchLoadLoadedTableSummary {
                    table_key: row.try_get("table_key").unwrap_or_default(),
                    succeeded_jobs: row.try_get("succeeded_jobs").unwrap_or_default(),
                    loaded_rows: row.try_get("loaded_rows").unwrap_or_default(),
                    total_duration_seconds: row
                        .try_get::<f64, _>("total_duration_ms")
                        .unwrap_or_default()
                        / 1000.0,
                })
                .collect(),
            latest_failed_error: failed
                .as_ref()
                .and_then(|row| row.try_get::<Option<String>, _>("last_error").ok())
                .flatten(),
            latest_failed_retry_class: failed
                .as_ref()
                .and_then(|row| row.try_get::<Option<String>, _>("retry_class").ok())
                .flatten(),
            latest_failed_at: failed
                .as_ref()
                .and_then(|row| row.try_get::<i64, _>("updated_at").ok())
                .and_then(datetime_from_millis),
        })
    }

    pub async fn load_cdc_commit_fragments(
        &self,
        connection_id: &str,
        statuses: &[CdcCommitFragmentStatus],
    ) -> anyhow::Result<Vec<CdcCommitFragmentRecord>> {
        let status_values: Vec<&str> = statuses.iter().map(|status| status.as_str()).collect();
        let rows = sqlx::query(&format!(
            r#"
            select fragment_id, job_id, sequence, commit_lsn, table_key, status, row_count,
                   upserted_count, deleted_count, last_error, stage, artifact_uri, staging_table,
                   load_job_id, merge_job_id, primary_key_lane, barrier_kind, ledger_metadata_json,
                   coalesce(expected_fragments, 0) as expected_fragments, created_at, updated_at
            from {}
            where connection_id = $1
              and status = any($2)
            order by sequence asc, created_at asc
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(&status_values)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let sequence = row.try_get::<i64, _>("sequence")?;
                let status = CdcCommitFragmentStatus::from_str(row.try_get("status")?)?;
                let stage = row
                    .try_get::<Option<String>, _>("stage")?
                    .map(|value| CdcLedgerStage::from_str(&value))
                    .transpose()?
                    .unwrap_or_else(|| CdcLedgerStage::from_fragment_status(status));
                Ok(CdcCommitFragmentRecord {
                    fragment_id: row.try_get("fragment_id")?,
                    job_id: row.try_get("job_id")?,
                    sequence: u64::try_from(sequence)
                        .context("cdc commit fragment sequence must be non-negative")?,
                    commit_lsn: row.try_get("commit_lsn")?,
                    table_key: row.try_get("table_key")?,
                    status,
                    stage,
                    expected_fragments: row
                        .try_get::<Option<i64>, _>("expected_fragments")?
                        .unwrap_or_default(),
                    row_count: row.try_get("row_count")?,
                    upserted_count: row.try_get("upserted_count")?,
                    deleted_count: row.try_get("deleted_count")?,
                    last_error: row.try_get("last_error")?,
                    artifact_uri: row.try_get("artifact_uri")?,
                    staging_table: row.try_get("staging_table")?,
                    load_job_id: row.try_get("load_job_id")?,
                    merge_job_id: row.try_get("merge_job_id")?,
                    primary_key_lane: row.try_get("primary_key_lane")?,
                    barrier_kind: row.try_get("barrier_kind")?,
                    ledger_metadata_json: row.try_get("ledger_metadata_json")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn load_cdc_durable_apply_frontier(
        &self,
        connection_id: &str,
        from_sequence: u64,
        max_sequences: usize,
    ) -> anyhow::Result<Option<CdcDurableApplyFrontier>> {
        let rows = sqlx::query(&format!(
            r#"
            select sequences.sequence,
                   sequences.commit_lsn,
                   sequences.expected_fragments,
                   count(distinct (fragments.table_key, coalesce(fragments.primary_key_lane, '')))
                       filter (where fragments.status = $3)::bigint as succeeded_fragments
            from {} sequences
            left join {} fragments
              on fragments.connection_id = sequences.connection_id
             and fragments.sequence = sequences.sequence
            where sequences.connection_id = $1
              and sequences.sequence >= $2
            group by sequences.sequence, sequences.commit_lsn, sequences.expected_fragments
            order by sequences.sequence asc
            limit $4
            "#,
            self.table("cdc_commit_sequences"),
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(saturating_u64_to_i64(from_sequence))
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(saturating_usize_to_i64(max_sequences.max(1)))
        .fetch_all(&self.pool)
        .await?;

        let mut next_sequence = from_sequence;
        let mut commit_lsn = None;
        for row in rows {
            let sequence = u64::try_from(row.try_get::<i64, _>("sequence")?)
                .context("cdc commit fragment sequence must be non-negative")?;
            let expected_fragments = row.try_get::<i64, _>("expected_fragments")?.max(1);
            let succeeded_fragments = row.try_get::<i64, _>("succeeded_fragments")?;
            if sequence != next_sequence || succeeded_fragments < expected_fragments {
                break;
            }
            commit_lsn = row.try_get::<Option<String>, _>("commit_lsn")?;
            next_sequence = next_sequence.saturating_add(1);
        }

        if next_sequence == from_sequence {
            return Ok(None);
        }
        let commit_lsn = commit_lsn.context("durable CDC frontier missing commit LSN")?;
        Ok(Some(CdcDurableApplyFrontier {
            next_sequence_to_ack: next_sequence,
            commit_lsn,
        }))
    }

    pub async fn save_cdc_feedback_state(
        &self,
        connection_id: &str,
        state: &CdcWatermarkState,
    ) -> anyhow::Result<()> {
        let updated_at = state
            .updated_at
            .map_or_else(now_millis, |value| value.timestamp_millis());
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                next_sequence_to_ack,
                last_received_lsn,
                last_flushed_lsn,
                last_persisted_lsn,
                last_status_update_sent_at,
                last_keepalive_reply_at,
                last_slot_feedback_lsn,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            on conflict(connection_id) do update set
                next_sequence_to_ack = excluded.next_sequence_to_ack,
                last_received_lsn = excluded.last_received_lsn,
                last_flushed_lsn = excluded.last_flushed_lsn,
                last_persisted_lsn = excluded.last_persisted_lsn,
                last_status_update_sent_at = excluded.last_status_update_sent_at,
                last_keepalive_reply_at = excluded.last_keepalive_reply_at,
                last_slot_feedback_lsn = excluded.last_slot_feedback_lsn,
                updated_at = excluded.updated_at
            "#,
            self.table("cdc_feedback_state")
        ))
        .bind(connection_id)
        .bind(saturating_u64_to_i64(state.next_sequence_to_ack))
        .bind(state.last_received_lsn.clone())
        .bind(state.last_flushed_lsn.clone())
        .bind(state.last_persisted_lsn.clone())
        .bind(
            state
                .last_status_update_sent_at
                .map(|value| value.timestamp_millis()),
        )
        .bind(
            state
                .last_keepalive_reply_at
                .map(|value| value.timestamp_millis()),
        )
        .bind(state.last_slot_feedback_lsn.clone())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_cdc_watermark_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<CdcWatermarkState>> {
        let legacy = self.load_legacy_cdc_watermark_state(connection_id).await?;
        let enqueue = self.load_cdc_enqueue_state(connection_id).await?;
        let feedback = self.load_cdc_feedback_state(connection_id).await?;
        Ok(merge_cdc_watermark_state(legacy, enqueue, feedback))
    }

    pub async fn load_cdc_feedback_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<CdcWatermarkState>> {
        let row = sqlx::query(&format!(
            r#"
            select next_sequence_to_ack,
                   last_received_lsn,
                   last_flushed_lsn, last_persisted_lsn,
                   last_status_update_sent_at,
                   last_keepalive_reply_at, last_slot_feedback_lsn,
                   updated_at
            from {}
            where connection_id = $1
            "#,
            self.table("cdc_feedback_state")
        ))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let next_sequence_to_ack = row.try_get::<i64, _>("next_sequence_to_ack")?;
            Ok(CdcWatermarkState {
                next_sequence_to_ack: u64::try_from(next_sequence_to_ack)
                    .context("cdc watermark next_sequence_to_ack must be non-negative")?,
                last_enqueued_sequence: None,
                last_received_lsn: row.try_get("last_received_lsn")?,
                last_flushed_lsn: row.try_get("last_flushed_lsn")?,
                last_persisted_lsn: row.try_get("last_persisted_lsn")?,
                last_relevant_change_seen_at: None,
                last_status_update_sent_at: row
                    .try_get::<Option<i64>, _>("last_status_update_sent_at")?
                    .and_then(datetime_from_millis),
                last_keepalive_reply_at: row
                    .try_get::<Option<i64>, _>("last_keepalive_reply_at")?
                    .and_then(datetime_from_millis),
                last_slot_feedback_lsn: row.try_get("last_slot_feedback_lsn")?,
                updated_at: row
                    .try_get::<i64, _>("updated_at")
                    .ok()
                    .and_then(datetime_from_millis),
            })
        })
        .transpose()
    }

    async fn load_legacy_cdc_watermark_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<CdcWatermarkState>> {
        let row = sqlx::query(&format!(
            r#"
            select next_sequence_to_ack, last_enqueued_sequence, last_received_lsn,
                   last_flushed_lsn, last_persisted_lsn,
                   last_relevant_change_seen_at, last_status_update_sent_at,
                   last_keepalive_reply_at, last_slot_feedback_lsn,
                   updated_at
            from {}
            where connection_id = $1
            "#,
            self.table("cdc_watermark_state")
        ))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| legacy_cdc_watermark_state_from_row(&row))
            .transpose()
    }

    async fn load_cdc_enqueue_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<CdcWatermarkState>> {
        let row = sqlx::query(&format!(
            r#"
            select sequence, commit_lsn, created_at
            from (
                (
                select sequence, commit_lsn, created_at
                from {}
                where connection_id = $1
                  and status = $2
                order by sequence desc, created_at desc
                limit 1
                )

                union all

                (
                select sequence, commit_lsn, created_at
                from {}
                where connection_id = $1
                  and status = $3
                order by sequence desc, created_at desc
                limit 1
                )

                union all

                (
                select sequence, commit_lsn, created_at
                from {}
                where connection_id = $1
                  and status = $4
                order by sequence desc, created_at desc
                limit 1
                )
            ) latest
            order by sequence desc, created_at desc
            limit 1
            "#,
            self.table("cdc_commit_fragments"),
            self.table("cdc_commit_fragments"),
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(CdcCommitFragmentStatus::Pending.as_str())
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(CdcCommitFragmentStatus::Failed.as_str())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let sequence = row.try_get::<i64, _>("sequence")?;
            let created_at = row.try_get::<i64, _>("created_at").ok();
            Ok(CdcWatermarkState {
                next_sequence_to_ack: 0,
                last_enqueued_sequence: Some(
                    u64::try_from(sequence)
                        .context("cdc enqueue state last_enqueued_sequence must be non-negative")?,
                ),
                last_received_lsn: row.try_get("commit_lsn")?,
                last_flushed_lsn: None,
                last_persisted_lsn: None,
                last_relevant_change_seen_at: created_at.and_then(datetime_from_millis),
                last_status_update_sent_at: None,
                last_keepalive_reply_at: None,
                last_slot_feedback_lsn: None,
                updated_at: created_at.and_then(datetime_from_millis),
            })
        })
        .transpose()
    }

    pub async fn load_cdc_coordinator_summary(
        &self,
        connection_id: &str,
        wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary> {
        let now = now_millis();
        let watermark = self.load_cdc_watermark_state(connection_id).await?;
        let pending = self
            .load_cdc_commit_fragments(connection_id, &[CdcCommitFragmentStatus::Pending])
            .await?;
        let failed = self
            .load_cdc_commit_fragments(connection_id, &[CdcCommitFragmentStatus::Failed])
            .await?;

        let oldest_pending = pending
            .iter()
            .min_by_key(|fragment| (fragment.sequence, fragment.created_at));
        let latest_failed = failed
            .iter()
            .max_by_key(|fragment| (fragment.updated_at, fragment.sequence));

        Ok(CdcCoordinatorSummary {
            next_sequence_to_ack: watermark
                .as_ref()
                .map(|state| state.next_sequence_to_ack)
                .unwrap_or_default(),
            last_enqueued_sequence: watermark
                .as_ref()
                .and_then(|state| state.last_enqueued_sequence),
            sequence_lag: watermark
                .as_ref()
                .and_then(|state| {
                    state
                        .last_enqueued_sequence
                        .map(|last| (state.next_sequence_to_ack, last))
                })
                .map(|(next, last)| saturating_u64_to_i64(last.saturating_sub(next))),
            pending_fragments: saturating_usize_to_i64(pending.len()),
            failed_fragments: saturating_usize_to_i64(failed.len()),
            oldest_pending_sequence: oldest_pending.map(|fragment| fragment.sequence),
            oldest_pending_age_seconds: oldest_pending
                .map(|fragment| ((now - fragment.created_at).max(0)) / 1000),
            latest_failed_sequence: latest_failed.map(|fragment| fragment.sequence),
            latest_failed_error: latest_failed.and_then(|fragment| fragment.last_error.clone()),
            last_received_lsn: watermark
                .as_ref()
                .and_then(|state| state.last_received_lsn.clone()),
            last_flushed_lsn: watermark
                .as_ref()
                .and_then(|state| state.last_flushed_lsn.clone()),
            last_persisted_lsn: watermark
                .as_ref()
                .and_then(|state| state.last_persisted_lsn.clone()),
            last_relevant_change_seen_at: watermark
                .as_ref()
                .and_then(|state| state.last_relevant_change_seen_at),
            last_status_update_sent_at: watermark
                .as_ref()
                .and_then(|state| state.last_status_update_sent_at),
            last_keepalive_reply_at: watermark
                .as_ref()
                .and_then(|state| state.last_keepalive_reply_at),
            last_slot_feedback_lsn: watermark
                .as_ref()
                .and_then(|state| state.last_slot_feedback_lsn.clone()),
            wal_bytes_unattributed_or_idle: if pending.is_empty() {
                wal_bytes_behind_confirmed
            } else {
                None
            },
            updated_at: watermark.and_then(|state| state.updated_at),
        })
    }

    pub async fn request_postgres_table_resync(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                source_table,
                requested_at
            ) values ($1, $2, $3)
            on conflict (connection_id, source_table) do update set
                requested_at = excluded.requested_at
            "#,
            self.table("postgres_table_resync_requests")
        ))
        .bind(connection_id)
        .bind(source_table)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_postgres_table_resync_requests(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Vec<PostgresTableResyncRequest>> {
        let rows = sqlx::query(&format!(
            r#"
            select source_table, requested_at
            from {}
            where connection_id = $1
            order by requested_at asc, source_table asc
            "#,
            self.table("postgres_table_resync_requests")
        ))
        .bind(connection_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PostgresTableResyncRequest {
                    source_table: row.try_get("source_table")?,
                    requested_at: datetime_from_millis(row.try_get("requested_at")?)
                        .context("invalid requested_at for postgres table resync request")?,
                })
            })
            .collect()
    }

    pub async fn clear_postgres_table_resync_request(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "delete from {} where connection_id = $1 and source_table = $2",
            self.table("postgres_table_resync_requests")
        ))
        .bind(connection_id)
        .bind(source_table)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn legacy_cdc_watermark_state_from_row(
    row: &sqlx::postgres::PgRow,
) -> anyhow::Result<CdcWatermarkState> {
    let next_sequence_to_ack = row.try_get::<i64, _>("next_sequence_to_ack")?;
    Ok(CdcWatermarkState {
        next_sequence_to_ack: u64::try_from(next_sequence_to_ack)
            .context("cdc watermark next_sequence_to_ack must be non-negative")?,
        last_enqueued_sequence: row
            .try_get::<Option<i64>, _>("last_enqueued_sequence")?
            .map(|value| {
                u64::try_from(value)
                    .context("cdc watermark last_enqueued_sequence must be non-negative")
            })
            .transpose()?,
        last_received_lsn: row.try_get("last_received_lsn")?,
        last_flushed_lsn: row.try_get("last_flushed_lsn")?,
        last_persisted_lsn: row.try_get("last_persisted_lsn")?,
        last_relevant_change_seen_at: row
            .try_get::<Option<i64>, _>("last_relevant_change_seen_at")?
            .and_then(datetime_from_millis),
        last_status_update_sent_at: row
            .try_get::<Option<i64>, _>("last_status_update_sent_at")?
            .and_then(datetime_from_millis),
        last_keepalive_reply_at: row
            .try_get::<Option<i64>, _>("last_keepalive_reply_at")?
            .and_then(datetime_from_millis),
        last_slot_feedback_lsn: row.try_get("last_slot_feedback_lsn")?,
        updated_at: row
            .try_get::<i64, _>("updated_at")
            .ok()
            .and_then(datetime_from_millis),
    })
}

fn merge_cdc_watermark_state(
    legacy: Option<CdcWatermarkState>,
    enqueue: Option<CdcWatermarkState>,
    feedback: Option<CdcWatermarkState>,
) -> Option<CdcWatermarkState> {
    let base = match (legacy, enqueue) {
        (None, None) => None,
        (Some(legacy), None) => Some(legacy),
        (None, Some(enqueue)) => Some(enqueue),
        (Some(legacy), Some(enqueue)) => Some(CdcWatermarkState {
            next_sequence_to_ack: legacy.next_sequence_to_ack,
            last_enqueued_sequence: enqueue
                .last_enqueued_sequence
                .or(legacy.last_enqueued_sequence),
            last_received_lsn: enqueue.last_received_lsn.or(legacy.last_received_lsn),
            last_flushed_lsn: legacy.last_flushed_lsn,
            last_persisted_lsn: legacy.last_persisted_lsn,
            last_relevant_change_seen_at: enqueue
                .last_relevant_change_seen_at
                .or(legacy.last_relevant_change_seen_at),
            last_status_update_sent_at: legacy.last_status_update_sent_at,
            last_keepalive_reply_at: legacy.last_keepalive_reply_at,
            last_slot_feedback_lsn: legacy.last_slot_feedback_lsn,
            updated_at: match (legacy.updated_at, enqueue.updated_at) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(value), None) | (None, Some(value)) => Some(value),
                (None, None) => None,
            },
        }),
    };

    match (base, feedback) {
        (None, None) => None,
        (Some(base), None) => Some(base),
        (None, Some(feedback)) => Some(feedback),
        (Some(base), Some(feedback)) => Some(CdcWatermarkState {
            next_sequence_to_ack: feedback.next_sequence_to_ack,
            last_enqueued_sequence: base.last_enqueued_sequence,
            last_received_lsn: feedback.last_received_lsn.or(base.last_received_lsn),
            last_flushed_lsn: feedback.last_flushed_lsn.or(base.last_flushed_lsn),
            last_persisted_lsn: feedback.last_persisted_lsn.or(base.last_persisted_lsn),
            last_relevant_change_seen_at: base
                .last_relevant_change_seen_at
                .or(feedback.last_relevant_change_seen_at),
            last_status_update_sent_at: feedback
                .last_status_update_sent_at
                .or(base.last_status_update_sent_at),
            last_keepalive_reply_at: feedback
                .last_keepalive_reply_at
                .or(base.last_keepalive_reply_at),
            last_slot_feedback_lsn: feedback
                .last_slot_feedback_lsn
                .or(base.last_slot_feedback_lsn),
            updated_at: match (base.updated_at, feedback.updated_at) {
                (Some(left), Some(right)) => Some(left.max(right)),
                (Some(value), None) | (None, Some(value)) => Some(value),
                (None, None) => None,
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_cdc_watermark_state_prefers_feedback_fields_and_keeps_enqueue_fields() {
        let legacy = CdcWatermarkState {
            next_sequence_to_ack: 7,
            last_enqueued_sequence: Some(11),
            last_received_lsn: Some("0/BBB".to_string()),
            last_flushed_lsn: Some("0/AAA".to_string()),
            last_persisted_lsn: Some("0/AAA".to_string()),
            last_relevant_change_seen_at: Some(Utc::now()),
            last_status_update_sent_at: None,
            last_keepalive_reply_at: None,
            last_slot_feedback_lsn: None,
            updated_at: Some(Utc::now()),
        };
        let feedback = CdcWatermarkState {
            next_sequence_to_ack: 12,
            last_enqueued_sequence: None,
            last_received_lsn: None,
            last_flushed_lsn: Some("0/CCC".to_string()),
            last_persisted_lsn: Some("0/DDD".to_string()),
            last_relevant_change_seen_at: None,
            last_status_update_sent_at: Some(Utc::now()),
            last_keepalive_reply_at: Some(Utc::now()),
            last_slot_feedback_lsn: Some("0/EEE".to_string()),
            updated_at: Some(Utc::now()),
        };

        let merged = merge_cdc_watermark_state(Some(legacy), None, Some(feedback))
            .expect("merged watermark state");

        assert_eq!(merged.next_sequence_to_ack, 12);
        assert_eq!(merged.last_enqueued_sequence, Some(11));
        assert_eq!(merged.last_received_lsn.as_deref(), Some("0/BBB"));
        assert_eq!(merged.last_flushed_lsn.as_deref(), Some("0/CCC"));
        assert_eq!(merged.last_persisted_lsn.as_deref(), Some("0/DDD"));
        assert_eq!(merged.last_slot_feedback_lsn.as_deref(), Some("0/EEE"));
        assert!(merged.last_status_update_sent_at.is_some());
        assert!(merged.last_keepalive_reply_at.is_some());
    }

    #[test]
    fn merge_cdc_watermark_state_prefers_append_only_enqueue_facts_over_legacy() {
        let legacy = CdcWatermarkState {
            next_sequence_to_ack: 3,
            last_enqueued_sequence: Some(4),
            last_received_lsn: Some("0/AAA".to_string()),
            last_flushed_lsn: None,
            last_persisted_lsn: None,
            last_relevant_change_seen_at: None,
            last_status_update_sent_at: None,
            last_keepalive_reply_at: None,
            last_slot_feedback_lsn: None,
            updated_at: None,
        };
        let enqueue = CdcWatermarkState {
            next_sequence_to_ack: 0,
            last_enqueued_sequence: Some(9),
            last_received_lsn: Some("0/BBB".to_string()),
            last_flushed_lsn: None,
            last_persisted_lsn: None,
            last_relevant_change_seen_at: Some(Utc::now()),
            last_status_update_sent_at: None,
            last_keepalive_reply_at: None,
            last_slot_feedback_lsn: None,
            updated_at: Some(Utc::now()),
        };

        let merged = merge_cdc_watermark_state(Some(legacy), Some(enqueue), None)
            .expect("merged watermark state");

        assert_eq!(merged.next_sequence_to_ack, 3);
        assert_eq!(merged.last_enqueued_sequence, Some(9));
        assert_eq!(merged.last_received_lsn.as_deref(), Some("0/BBB"));
        assert!(merged.last_relevant_change_seen_at.is_some());
    }
}
