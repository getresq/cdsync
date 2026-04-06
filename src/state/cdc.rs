use super::*;

impl SyncStateStore {
    pub async fn enqueue_cdc_batch_load_job(
        &self,
        connection_id: &str,
        job: &CdcBatchLoadJobRecord,
    ) -> anyhow::Result<CdcBatchLoadJobRecord> {
        let table = self.table("cdc_batch_load_jobs");
        let row = sqlx::query(&format!(
            r#"
            insert into {table} (
                connection_id,
                job_id,
                table_key,
                first_sequence,
                status,
                payload_json,
                attempt_count,
                last_error,
                created_at,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
                payload_json = case
                    when {table}.status = 'failed' then excluded.payload_json
                    else {table}.payload_json
                end,
                attempt_count = {table}.attempt_count,
                last_error = case
                    when {table}.status = 'failed' then null
                    else {table}.last_error
                end,
                updated_at = case
                    when {table}.status = 'failed' then excluded.updated_at
                    else {table}.updated_at
                end
            returning job_id, table_key, first_sequence, status, payload_json, attempt_count,
                      last_error, created_at, updated_at
            "#,
            table = table,
        ))
        .bind(connection_id)
        .bind(&job.job_id)
        .bind(&job.table_key)
        .bind(saturating_u64_to_i64(job.first_sequence))
        .bind(job.status.as_str())
        .bind(&job.payload_json)
        .bind(job.attempt_count)
        .bind(job.last_error.clone())
        .bind(job.created_at)
        .bind(job.updated_at)
        .fetch_one(&self.pool)
        .await?;
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
            select job_id, table_key, first_sequence, status, payload_json, attempt_count, last_error, created_at, updated_at
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

    pub async fn claim_next_cdc_batch_load_job(
        &self,
        connection_id: &str,
        stale_running_before_ms: i64,
    ) -> anyhow::Result<Option<CdcBatchLoadJobRecord>> {
        let now = now_millis();
        let row = sqlx::query(&format!(
            r#"
            with candidate as (
                select j.job_id
                from {} j
                where j.connection_id = $1
                  and (
                    j.status = $2
                    or (j.status = $3 and j.updated_at < $4)
                  )
                  and not exists (
                    select 1
                    from {} blockers
                    where blockers.connection_id = j.connection_id
                      and blockers.table_key = j.table_key
                      and blockers.job_id <> j.job_id
                      and blockers.first_sequence < j.first_sequence
                      and (
                        blockers.status = $2
                        or blockers.status = $5
                        or (blockers.status = $3 and blockers.updated_at >= $4)
                      )
                  )
                order by j.first_sequence asc, j.created_at asc
                for update skip locked
                limit 1
            )
            update {} jobs
            set status = $3,
                attempt_count = jobs.attempt_count + 1,
                last_error = null,
                updated_at = $6
            from candidate
            where jobs.connection_id = $1
              and jobs.job_id = candidate.job_id
            returning jobs.job_id, jobs.table_key, jobs.first_sequence, jobs.status,
                      jobs.payload_json, jobs.attempt_count, jobs.last_error,
                      jobs.created_at, jobs.updated_at
            "#,
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs"),
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .bind(stale_running_before_ms)
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref()
            .map(cdc_batch_load_job_record_from_row)
            .transpose()
    }

    pub async fn heartbeat_cdc_batch_load_job(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            update {}
            set updated_at = $3
            where connection_id = $1
              and job_id = $2
              and status = $4
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn requeue_cdc_batch_load_running_jobs(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<u64> {
        let result = sqlx::query(&format!(
            r#"
            update {}
            set status = $2,
                updated_at = $3
            where connection_id = $1
              and status = $4
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Running.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn requeue_retryable_failed_cdc_batch_load_jobs(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<u64> {
        let result = sqlx::query(&format!(
            r#"
            update {}
            set status = $2,
                last_error = null,
                updated_at = $3
            where connection_id = $1
              and status = $4
              and (
                last_error ilike '%failed to process CDC batch-load job%'
                or last_error ilike '%merging staging BigQuery table%'
                or last_error ilike '%BigQuery query returned errors%'
              )
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(CdcBatchLoadJobStatus::Pending.as_str())
        .bind(now_millis())
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn mark_cdc_batch_load_job_succeeded(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                last_error = null,
                updated_at = $4
            where connection_id = $1 and job_id = $2
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcBatchLoadJobStatus::Succeeded.as_str())
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_cdc_batch_load_job_failed(
        &self,
        connection_id: &str,
        job_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                last_error = $4,
                updated_at = $5
            where connection_id = $1 and job_id = $2
            "#,
            self.table("cdc_batch_load_jobs")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcBatchLoadJobStatus::Failed.as_str())
        .bind(error)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
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
                min(created_at) filter (where status = 'pending') as oldest_pending_ms,
                min(updated_at) filter (where status = 'running') as oldest_running_ms,
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
            select last_error, updated_at
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
        let avg_job_duration_ms: Option<f64> = aggregate.try_get("avg_job_duration_ms")?;

        Ok(CdcBatchLoadQueueSummary {
            total_jobs: aggregate.try_get::<i64, _>("total_jobs")?,
            pending_jobs: aggregate.try_get::<i64, _>("pending_jobs")?,
            running_jobs: aggregate.try_get::<i64, _>("running_jobs")?,
            succeeded_jobs: aggregate.try_get::<i64, _>("succeeded_jobs")?,
            failed_jobs: aggregate.try_get::<i64, _>("failed_jobs")?,
            oldest_pending_age_seconds: oldest_pending_ms.map(|ts| ((now - ts).max(0)) / 1000),
            oldest_running_age_seconds: oldest_running_ms.map(|ts| ((now - ts).max(0)) / 1000),
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
            latest_failed_at: failed
                .as_ref()
                .and_then(|row| row.try_get::<i64, _>("updated_at").ok())
                .and_then(datetime_from_millis),
        })
    }

    pub async fn upsert_cdc_commit_fragments(
        &self,
        connection_id: &str,
        fragments: &[CdcCommitFragmentRecord],
    ) -> anyhow::Result<()> {
        let table = self.table("cdc_commit_fragments");
        for fragment in fragments {
            sqlx::query(&format!(
                r#"
                insert into {table} (
                    connection_id,
                    fragment_id,
                    job_id,
                    sequence,
                    commit_lsn,
                    table_key,
                    status,
                    row_count,
                    upserted_count,
                    deleted_count,
                    last_error,
                    created_at,
                    updated_at
                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                on conflict(fragment_id) do update set
                    job_id = excluded.job_id,
                    sequence = excluded.sequence,
                    commit_lsn = excluded.commit_lsn,
                    table_key = excluded.table_key,
                    status = case
                        when {table}.status = 'failed' then excluded.status
                        else {table}.status
                    end,
                    row_count = excluded.row_count,
                    upserted_count = excluded.upserted_count,
                    deleted_count = excluded.deleted_count,
                    last_error = case
                        when {table}.status = 'failed' then excluded.last_error
                        else {table}.last_error
                    end,
                    updated_at = case
                        when {table}.status = 'failed' then excluded.updated_at
                        else {table}.updated_at
                    end
                "#,
                table = table,
            ))
            .bind(connection_id)
            .bind(&fragment.fragment_id)
            .bind(&fragment.job_id)
            .bind(saturating_u64_to_i64(fragment.sequence))
            .bind(&fragment.commit_lsn)
            .bind(&fragment.table_key)
            .bind(fragment.status.as_str())
            .bind(fragment.row_count)
            .bind(fragment.upserted_count)
            .bind(fragment.deleted_count)
            .bind(fragment.last_error.clone())
            .bind(fragment.created_at)
            .bind(fragment.updated_at)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
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
                   upserted_count, deleted_count, last_error, created_at, updated_at
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
                Ok(CdcCommitFragmentRecord {
                    fragment_id: row.try_get("fragment_id")?,
                    job_id: row.try_get("job_id")?,
                    sequence: u64::try_from(sequence)
                        .context("cdc commit fragment sequence must be non-negative")?,
                    commit_lsn: row.try_get("commit_lsn")?,
                    table_key: row.try_get("table_key")?,
                    status: CdcCommitFragmentStatus::from_str(row.try_get("status")?)?,
                    row_count: row.try_get("row_count")?,
                    upserted_count: row.try_get("upserted_count")?,
                    deleted_count: row.try_get("deleted_count")?,
                    last_error: row.try_get("last_error")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                })
            })
            .collect()
    }

    pub async fn mark_cdc_commit_fragments_succeeded_for_job(
        &self,
        connection_id: &str,
        job_id: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                last_error = null,
                updated_at = $4
            where connection_id = $1 and job_id = $2
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcCommitFragmentStatus::Succeeded.as_str())
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_cdc_commit_fragments_failed_for_job(
        &self,
        connection_id: &str,
        job_id: &str,
        error: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            update {}
            set status = $3,
                last_error = $4,
                updated_at = $5
            where connection_id = $1 and job_id = $2
            "#,
            self.table("cdc_commit_fragments")
        ))
        .bind(connection_id)
        .bind(job_id)
        .bind(CdcCommitFragmentStatus::Failed.as_str())
        .bind(error)
        .bind(now_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn save_cdc_watermark_state(
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
                last_enqueued_sequence,
                last_received_lsn,
                last_flushed_lsn,
                last_persisted_lsn,
                last_relevant_change_seen_at,
                last_status_update_sent_at,
                last_keepalive_reply_at,
                last_slot_feedback_lsn,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            on conflict(connection_id) do update set
                next_sequence_to_ack = excluded.next_sequence_to_ack,
                last_enqueued_sequence = excluded.last_enqueued_sequence,
                last_received_lsn = excluded.last_received_lsn,
                last_flushed_lsn = excluded.last_flushed_lsn,
                last_persisted_lsn = excluded.last_persisted_lsn,
                last_relevant_change_seen_at = excluded.last_relevant_change_seen_at,
                last_status_update_sent_at = excluded.last_status_update_sent_at,
                last_keepalive_reply_at = excluded.last_keepalive_reply_at,
                last_slot_feedback_lsn = excluded.last_slot_feedback_lsn,
                updated_at = excluded.updated_at
            "#,
            self.table("cdc_watermark_state")
        ))
        .bind(connection_id)
        .bind(saturating_u64_to_i64(state.next_sequence_to_ack))
        .bind(state.last_enqueued_sequence.map(saturating_u64_to_i64))
        .bind(state.last_received_lsn.clone())
        .bind(state.last_flushed_lsn.clone())
        .bind(state.last_persisted_lsn.clone())
        .bind(
            state
                .last_relevant_change_seen_at
                .map(|value| value.timestamp_millis()),
        )
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

        row.map(|row| {
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
