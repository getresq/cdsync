use crate::config::StateConfig;
use crate::types::TableCheckpoint;
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPoolOptions, PgRow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

const LOCK_TTL_SECONDS: u64 = 60;
const LOCK_HEARTBEAT_SECONDS: u64 = 15;
static STATE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/state");

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncState {
    #[serde(default)]
    pub connections: HashMap<String, ConnectionState>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PostgresCdcState {
    pub last_lsn: Option<String>,
    pub slot_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CdcBatchLoadJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl CdcBatchLoadJobStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => anyhow::bail!("unknown CDC batch load job status {}", other),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcBatchLoadJobRecord {
    pub job_id: String,
    pub table_key: String,
    pub first_sequence: u64,
    pub status: CdcBatchLoadJobStatus,
    pub payload_json: String,
    pub attempt_count: i32,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CdcCommitFragmentStatus {
    Pending,
    Succeeded,
    Failed,
}

impl CdcCommitFragmentStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            other => anyhow::bail!("unknown CDC commit fragment status {}", other),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcCommitFragmentRecord {
    pub fragment_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub commit_lsn: String,
    pub table_key: String,
    pub status: CdcCommitFragmentStatus,
    pub row_count: i64,
    pub upserted_count: i64,
    pub deleted_count: i64,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcWatermarkState {
    pub next_sequence_to_ack: u64,
    pub last_enqueued_sequence: Option<u64>,
    pub last_received_lsn: Option<String>,
    pub last_flushed_lsn: Option<String>,
    pub last_persisted_lsn: Option<String>,
    pub last_relevant_change_seen_at: Option<DateTime<Utc>>,
    pub last_status_update_sent_at: Option<DateTime<Utc>>,
    pub last_keepalive_reply_at: Option<DateTime<Utc>>,
    pub last_slot_feedback_lsn: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcCoordinatorSummary {
    pub next_sequence_to_ack: u64,
    pub last_enqueued_sequence: Option<u64>,
    pub pending_fragments: i64,
    pub failed_fragments: i64,
    pub oldest_pending_sequence: Option<u64>,
    pub oldest_pending_age_seconds: Option<i64>,
    pub latest_failed_sequence: Option<u64>,
    pub latest_failed_error: Option<String>,
    pub last_received_lsn: Option<String>,
    pub last_flushed_lsn: Option<String>,
    pub last_persisted_lsn: Option<String>,
    pub last_relevant_change_seen_at: Option<DateTime<Utc>>,
    pub last_status_update_sent_at: Option<DateTime<Utc>>,
    pub last_keepalive_reply_at: Option<DateTime<Utc>>,
    pub last_slot_feedback_lsn: Option<String>,
    pub wal_bytes_unattributed_or_idle: Option<i64>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcBatchLoadQueueSummary {
    pub total_jobs: i64,
    pub pending_jobs: i64,
    pub running_jobs: i64,
    pub succeeded_jobs: i64,
    pub failed_jobs: i64,
    pub oldest_pending_age_seconds: Option<i64>,
    pub oldest_running_age_seconds: Option<i64>,
    pub jobs_per_minute: i64,
    pub rows_per_minute: i64,
    pub avg_job_duration_seconds: Option<f64>,
    pub top_queued_tables: Vec<CdcBatchLoadQueueTableSummary>,
    pub top_loaded_tables: Vec<CdcBatchLoadLoadedTableSummary>,
    pub latest_failed_error: Option<String>,
    pub latest_failed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcBatchLoadQueueTableSummary {
    pub table_key: String,
    pub queued_jobs: i64,
    pub queued_rows: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcBatchLoadLoadedTableSummary {
    pub table_key: String,
    pub succeeded_jobs: i64,
    pub loaded_rows: i64,
    pub total_duration_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionState {
    #[serde(default)]
    pub postgres: HashMap<String, TableCheckpoint>,
    pub postgres_cdc: Option<PostgresCdcState>,
    #[serde(default)]
    pub salesforce: HashMap<String, TableCheckpoint>,
    pub last_sync_started_at: Option<DateTime<Utc>>,
    pub last_sync_finished_at: Option<DateTime<Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct SyncStateStore {
    pool: PgPool,
    schema: String,
    lock_ttl: Duration,
}

#[derive(Clone)]
pub struct StateHandle {
    store: SyncStateStore,
    connection_id: String,
}

pub struct ConnectionLease {
    cleanup: Option<Arc<LeaseCleanup>>,
    stop_tx: Option<oneshot::Sender<()>>,
    heartbeat_task: Option<JoinHandle<()>>,
}

struct LeaseCleanup {
    store: SyncStateStore,
    connection_id: String,
    owner_id: String,
}

impl SyncState {
    pub async fn load_with_config(config: &StateConfig) -> anyhow::Result<Self> {
        let store = SyncStateStore::open_with_config(config).await?;
        store.load_state().await
    }
}

impl SyncStateStore {
    pub async fn migrate_with_config(config: &StateConfig) -> anyhow::Result<()> {
        validate_schema_name(config.schema_name())?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.url)
            .await?;
        let store = Self {
            pool,
            schema: config.schema_name().to_string(),
            lock_ttl: Duration::from_secs(LOCK_TTL_SECONDS),
        };
        store.migrate().await
    }

    pub async fn open_with_config(config: &StateConfig) -> anyhow::Result<Self> {
        Self::open(&config.url, config.schema_name()).await
    }

    pub async fn open(url: &str, schema: &str) -> anyhow::Result<Self> {
        validate_schema_name(schema)?;
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        let store = Self {
            pool,
            schema: schema.to_string(),
            lock_ttl: Duration::from_secs(LOCK_TTL_SECONDS),
        };
        store.init().await?;
        Ok(store)
    }

    pub fn handle(&self, connection_id: &str) -> StateHandle {
        StateHandle {
            store: self.clone(),
            connection_id: connection_id.to_string(),
        }
    }

    pub async fn load_state(&self) -> anyhow::Result<SyncState> {
        let rows = sqlx::query(&format!(
            "select connection_id, last_sync_started_at, last_sync_finished_at, last_sync_status, last_error, postgres_cdc_last_lsn, postgres_cdc_slot_name, updated_at from {}",
            self.table("connection_state")
        ))
        .fetch_all(&self.pool)
        .await?;

        let checkpoint_rows = sqlx::query(&format!(
            "select connection_id, source_kind, entity_name, checkpoint_json from {}",
            self.table("table_checkpoints")
        ))
        .fetch_all(&self.pool)
        .await?;

        let mut connections = HashMap::new();
        let mut latest_updated_at = None;

        for row in rows {
            let connection_id: String = row.try_get("connection_id")?;
            let updated_at_ms: i64 = row.try_get("updated_at")?;
            let connection_state = ConnectionState {
                postgres: HashMap::new(),
                postgres_cdc: load_cdc_state_from_row(&row)?,
                salesforce: HashMap::new(),
                last_sync_started_at: parse_optional_rfc3339(row.try_get("last_sync_started_at")?),
                last_sync_finished_at: parse_optional_rfc3339(
                    row.try_get("last_sync_finished_at")?,
                ),
                last_sync_status: row.try_get("last_sync_status")?,
                last_error: row.try_get("last_error")?,
            };
            latest_updated_at = max_updated_at(latest_updated_at, updated_at_ms);
            connections.insert(connection_id, connection_state);
        }

        for row in checkpoint_rows {
            let connection_id: String = row.try_get("connection_id")?;
            let source_kind: String = row.try_get("source_kind")?;
            let entity_name: String = row.try_get("entity_name")?;
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            let checkpoint: TableCheckpoint =
                serde_json::from_str(&checkpoint_json).with_context(|| {
                    format!("parsing checkpoint for {}:{}", connection_id, entity_name)
                })?;
            let connection = connections.entry(connection_id).or_default();
            match source_kind.as_str() {
                "postgres" => {
                    connection.postgres.insert(entity_name, checkpoint);
                }
                "salesforce" => {
                    connection.salesforce.insert(entity_name, checkpoint);
                }
                _ => {}
            }
        }

        Ok(SyncState {
            connections,
            updated_at: latest_updated_at.and_then(datetime_from_millis),
        })
    }

    pub async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>> {
        let row = sqlx::query(&format!(
            "select last_sync_started_at, last_sync_finished_at, last_sync_status, last_error, postgres_cdc_last_lsn, postgres_cdc_slot_name from {} where connection_id = $1",
            self.table("connection_state")
        ))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(ConnectionState {
            postgres: HashMap::new(),
            postgres_cdc: load_cdc_state_from_row(&row)?,
            salesforce: HashMap::new(),
            last_sync_started_at: parse_optional_rfc3339(row.try_get("last_sync_started_at")?),
            last_sync_finished_at: parse_optional_rfc3339(row.try_get("last_sync_finished_at")?),
            last_sync_status: row.try_get("last_sync_status")?,
            last_error: row.try_get("last_error")?,
        }))
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let _: i32 = sqlx::query_scalar("select 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    pub async fn save_connection_state(
        &self,
        connection_id: &str,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.save_connection_meta(connection_id, connection_state)
            .await?;
        for (table_name, checkpoint) in &connection_state.postgres {
            self.save_table_checkpoint(connection_id, "postgres", table_name, checkpoint)
                .await?;
        }
        for (object_name, checkpoint) in &connection_state.salesforce {
            self.save_table_checkpoint(connection_id, "salesforce", object_name, checkpoint)
                .await?;
        }
        Ok(())
    }

    pub async fn save_connection_meta(
        &self,
        connection_id: &str,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        let cdc_state = connection_state.postgres_cdc.as_ref();
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                last_sync_started_at,
                last_sync_finished_at,
                last_sync_status,
                last_error,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8)
            on conflict(connection_id) do update set
                last_sync_started_at = excluded.last_sync_started_at,
                last_sync_finished_at = excluded.last_sync_finished_at,
                last_sync_status = excluded.last_sync_status,
                last_error = excluded.last_error,
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
            self.table("connection_state")
        ))
        .bind(connection_id)
        .bind(
            connection_state
                .last_sync_started_at
                .map(|dt| dt.to_rfc3339()),
        )
        .bind(
            connection_state
                .last_sync_finished_at
                .map(|dt| dt.to_rfc3339()),
        )
        .bind(connection_state.last_sync_status.clone())
        .bind(connection_state.last_error.clone())
        .bind(cdc_state.and_then(|state| state.last_lsn.clone()))
        .bind(cdc_state.and_then(|state| state.slot_name.clone()))
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, "connection_meta");
        Ok(())
    }

    pub async fn save_table_checkpoint(
        &self,
        connection_id: &str,
        source_kind: &str,
        entity_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        let checkpoint_json = serde_json::to_string(checkpoint)?;
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                source_kind,
                entity_name,
                checkpoint_json,
                updated_at
            ) values ($1, $2, $3, $4, $5)
            on conflict(connection_id, source_kind, entity_name) do update set
                checkpoint_json = excluded.checkpoint_json,
                updated_at = excluded.updated_at
            "#,
            self.table("table_checkpoints")
        ))
        .bind(connection_id)
        .bind(source_kind)
        .bind(entity_name)
        .bind(checkpoint_json)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, source_kind);
        Ok(())
    }

    pub async fn save_postgres_cdc_state(
        &self,
        connection_id: &str,
        cdc_state: &PostgresCdcState,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values ($1, $2, $3, $4)
            on conflict(connection_id) do update set
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
            self.table("connection_state")
        ))
        .bind(connection_id)
        .bind(cdc_state.last_lsn.clone())
        .bind(cdc_state.slot_name.clone())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, "postgres_cdc");
        Ok(())
    }

    pub async fn load_table_checkpoint(
        &self,
        connection_id: &str,
        source_kind: &str,
        entity_name: &str,
    ) -> anyhow::Result<Option<TableCheckpoint>> {
        let row = sqlx::query(&format!(
            "select checkpoint_json from {} where connection_id = $1 and source_kind = $2 and entity_name = $3",
            self.table("table_checkpoints")
        ))
        .bind(connection_id)
        .bind(source_kind)
        .bind(entity_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            serde_json::from_str(&checkpoint_json).with_context(|| {
                format!(
                    "parsing checkpoint for {}:{}:{}",
                    connection_id, source_kind, entity_name
                )
            })
        })
        .transpose()
    }

    pub async fn load_all_table_checkpoints(
        &self,
        connection_id: &str,
        source_kind: &str,
    ) -> anyhow::Result<HashMap<String, TableCheckpoint>> {
        let rows = sqlx::query(&format!(
            r#"
            select entity_name, checkpoint_json
            from {}
            where connection_id = $1 and source_kind = $2
            "#,
            self.table("table_checkpoints")
        ))
        .bind(connection_id)
        .bind(source_kind)
        .fetch_all(&self.pool)
        .await?;

        let mut checkpoints = HashMap::with_capacity(rows.len());
        for row in rows {
            let entity_name: String = row.try_get("entity_name")?;
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            let checkpoint: TableCheckpoint =
                serde_json::from_str(&checkpoint_json).with_context(|| {
                    format!(
                        "parsing checkpoint for {}:{}:{}",
                        connection_id, source_kind, entity_name
                    )
                })?;
            checkpoints.insert(entity_name, checkpoint);
        }
        Ok(checkpoints)
    }

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
        .bind(job.first_sequence as i64)
        .bind(job.status.as_str())
        .bind(&job.payload_json)
        .bind(job.attempt_count)
        .bind(job.last_error.clone())
        .bind(job.created_at)
        .bind(job.updated_at)
        .fetch_one(&self.pool)
        .await?;
        cdc_batch_load_job_record_from_row(row)
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
            .map(cdc_batch_load_job_record_from_row)
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

        row.map(cdc_batch_load_job_record_from_row).transpose()
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
            .bind(fragment.sequence as i64)
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
                Ok(CdcCommitFragmentRecord {
                    fragment_id: row.try_get("fragment_id")?,
                    job_id: row.try_get("job_id")?,
                    sequence: row.try_get::<i64, _>("sequence")? as u64,
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
            .map(|value| value.timestamp_millis())
            .unwrap_or_else(now_millis);
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
        .bind(state.next_sequence_to_ack as i64)
        .bind(state.last_enqueued_sequence.map(|value| value as i64))
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
            Ok(CdcWatermarkState {
                next_sequence_to_ack: row.try_get::<i64, _>("next_sequence_to_ack")? as u64,
                last_enqueued_sequence: row
                    .try_get::<Option<i64>, _>("last_enqueued_sequence")?
                    .map(|value| value as u64),
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
            pending_fragments: pending.len() as i64,
            failed_fragments: failed.len() as i64,
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

    pub async fn acquire_connection_lock(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<ConnectionLease> {
        let owner_id = Uuid::new_v4().to_string();
        self.try_acquire_lock(connection_id, &owner_id).await?;

        let (stop_tx, mut stop_rx) = oneshot::channel();
        let store = self.clone();
        let connection = connection_id.to_string();
        let owner = owner_id.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(LOCK_HEARTBEAT_SECONDS)) => {
                        let _ = store.heartbeat_lock(&connection, &owner).await;
                    }
                    _ = &mut stop_rx => {
                        break;
                    }
                }
            }
        });

        Ok(ConnectionLease {
            cleanup: Some(Arc::new(LeaseCleanup {
                store: self.clone(),
                connection_id: connection_id.to_string(),
                owner_id,
            })),
            stop_tx: Some(stop_tx),
            heartbeat_task: Some(heartbeat_task),
        })
    }

    async fn init(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        ensure_schema_exists(&mut conn, &self.schema).await?;
        ensure_table_exists(&mut conn, &self.schema, "_sqlx_migrations").await?;
        ensure_table_exists(&mut conn, &self.schema, "connection_state").await?;
        ensure_table_exists(&mut conn, &self.schema, "table_checkpoints").await?;
        ensure_table_exists(&mut conn, &self.schema, "connection_locks").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_batch_load_jobs").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_commit_fragments").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_watermark_state").await?;
        Ok(())
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        create_schema_if_missing(&mut conn, &self.schema).await?;
        sqlx::query(&format!("set search_path to {}", quote_ident(&self.schema)))
            .execute(&mut *conn)
            .await?;
        STATE_MIGRATOR.run_direct(&mut *conn).await?;
        Ok(())
    }

    async fn try_acquire_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        let now = now_millis();
        let stale_before = now - self.lock_ttl.as_millis() as i64;
        let row = sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                owner_id,
                acquired_at,
                heartbeat_at
            ) values ($1, $2, $3, $3)
            on conflict(connection_id) do update set
                owner_id = excluded.owner_id,
                acquired_at = excluded.acquired_at,
                heartbeat_at = excluded.heartbeat_at
            where {}.owner_id = excluded.owner_id
               or {}.heartbeat_at < $4
            returning owner_id
            "#,
            self.table("connection_locks"),
            self.table("connection_locks"),
            self.table("connection_locks"),
        ))
        .bind(connection_id)
        .bind(owner_id)
        .bind(now)
        .bind(stale_before)
        .fetch_optional(&self.pool)
        .await?;

        if row.is_none() {
            anyhow::bail!("connection {} is already locked", connection_id);
        }
        Ok(())
    }

    async fn heartbeat_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "update {} set heartbeat_at = $1 where connection_id = $2 and owner_id = $3",
            self.table("connection_locks")
        ))
        .bind(now_millis())
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "delete from {} where connection_id = $1 and owner_id = $2",
            self.table("connection_locks")
        ))
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn table(&self, table_name: &str) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(table_name))
    }
}

impl StateHandle {
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

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

    pub async fn save_salesforce_checkpoint(
        &self,
        object_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        self.store
            .save_table_checkpoint(&self.connection_id, "salesforce", object_name, checkpoint)
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

    pub async fn load_salesforce_checkpoint(
        &self,
        object_name: &str,
    ) -> anyhow::Result<Option<TableCheckpoint>> {
        self.store
            .load_table_checkpoint(&self.connection_id, "salesforce", object_name)
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
                    warn!(error = %err, "failed to release connection lease on drop");
                }
            });
        } else {
            warn!(
                "dropping connection lease without an active tokio runtime; lock cleanup skipped"
            );
        }
    }
}

fn validate_schema_name(schema: &str) -> anyhow::Result<()> {
    let mut chars = schema.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => anyhow::bail!("invalid postgres state schema `{}`", schema),
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        anyhow::bail!("invalid postgres state schema `{}`", schema);
    }
    Ok(())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn cdc_batch_load_job_record_from_row(row: PgRow) -> anyhow::Result<CdcBatchLoadJobRecord> {
    Ok(CdcBatchLoadJobRecord {
        job_id: row.try_get("job_id")?,
        table_key: row.try_get("table_key")?,
        first_sequence: row.try_get::<i64, _>("first_sequence")? as u64,
        status: CdcBatchLoadJobStatus::from_str(row.try_get("status")?)?,
        payload_json: row.try_get("payload_json")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn ensure_schema_exists(conn: &mut sqlx::PgConnection, schema: &str) -> anyhow::Result<()> {
    let exists: bool =
        sqlx::query_scalar("select exists (select 1 from pg_namespace where nspname = $1)")
            .bind(schema)
            .fetch_one(&mut *conn)
            .await?;
    if !exists {
        anyhow::bail!("required schema {} does not exist", schema);
    }
    Ok(())
}

async fn create_schema_if_missing(
    conn: &mut sqlx::PgConnection,
    schema: &str,
) -> anyhow::Result<()> {
    sqlx::query(&format!(
        "create schema if not exists {}",
        quote_ident(schema)
    ))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn ensure_table_exists(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    table: &str,
) -> anyhow::Result<()> {
    let exists: bool = sqlx::query_scalar(
        r#"
        select exists (
            select 1
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = $1
              and c.relname = $2
              and c.relkind in ('r', 'p')
        )
        "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_one(&mut *conn)
    .await?;
    if !exists {
        anyhow::bail!(
            "required table {}.{} does not exist; run `cdsync migrate --config ...` first",
            schema,
            table
        );
    }
    Ok(())
}

fn load_cdc_state_from_row(row: &PgRow) -> anyhow::Result<Option<PostgresCdcState>> {
    let last_lsn: Option<String> = row.try_get("postgres_cdc_last_lsn")?;
    let slot_name: Option<String> = row.try_get("postgres_cdc_slot_name")?;
    if last_lsn.is_none() && slot_name.is_none() {
        return Ok(None);
    }
    Ok(Some(PostgresCdcState {
        last_lsn,
        slot_name,
    }))
}

fn parse_optional_rfc3339(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn datetime_from_millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value)
}

fn max_updated_at(current: Option<i64>, next: i64) -> Option<i64> {
    match current {
        Some(current) => Some(current.max(next)),
        None => Some(next),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state_config() -> Option<StateConfig> {
        let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
        Some(StateConfig {
            url,
            schema: Some(format!("cdsync_state_test_{}", Uuid::new_v4().simple())),
        })
    }

    #[tokio::test]
    async fn state_store_round_trips_connection_state() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;
        let handle = store.handle("app");

        let mut state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };
        state.postgres.insert(
            "public.accounts".to_string(),
            TableCheckpoint {
                last_primary_key: Some("42".to_string()),
                ..Default::default()
            },
        );
        state.postgres_cdc = Some(PostgresCdcState {
            last_lsn: Some("0/16B6C50".to_string()),
            slot_name: Some("slot".to_string()),
        });

        handle.save_connection_state(&state).await?;

        let loaded = store.load_state().await?;
        let connection = loaded
            .connections
            .get("app")
            .context("missing connection")?;
        assert_eq!(connection.last_sync_status.as_deref(), Some("running"));
        assert_eq!(
            connection
                .postgres
                .get("public.accounts")
                .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
            Some("42")
        );
        assert_eq!(
            connection
                .postgres_cdc
                .as_ref()
                .and_then(|cdc| cdc.last_lsn.as_deref()),
            Some("0/16B6C50")
        );
        Ok(())
    }

    #[tokio::test]
    async fn state_store_load_connection_state_reads_single_connection_meta() -> anyhow::Result<()>
    {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;
        let handle = store.handle("app");

        let mut state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };
        state.postgres.insert(
            "public.accounts".to_string(),
            TableCheckpoint {
                last_primary_key: Some("42".to_string()),
                ..Default::default()
            },
        );
        state.postgres_cdc = Some(PostgresCdcState {
            last_lsn: Some("0/16B6C50".to_string()),
            slot_name: Some("slot".to_string()),
        });

        handle.save_connection_state(&state).await?;

        let loaded = store
            .load_connection_state("app")
            .await?
            .context("missing connection")?;
        assert_eq!(loaded.last_sync_status.as_deref(), Some("running"));
        assert!(loaded.postgres.is_empty());
        assert!(loaded.salesforce.is_empty());
        assert_eq!(
            loaded
                .postgres_cdc
                .as_ref()
                .and_then(|cdc| cdc.last_lsn.as_deref()),
            Some("0/16B6C50")
        );
        Ok(())
    }

    #[tokio::test]
    async fn connection_locks_block_second_owner() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;

        let lease = store.acquire_connection_lock("app").await?;
        let second = store.acquire_connection_lock("app").await;
        assert!(second.is_err());
        lease.release().await?;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_connection_lease_releases_lock() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;

        {
            let _lease = store.acquire_connection_lock("app").await?;
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match store.acquire_connection_lock("app").await {
                    Ok(lease) => {
                        lease.release().await?;
                        return Ok::<(), anyhow::Error>(());
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .context("lock was not released after dropping lease")??;

        Ok(())
    }
}
