use super::*;
use serde::{Deserialize, Serialize};

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
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub(super) fn from_str(value: &str) -> anyhow::Result<Self> {
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
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub(super) fn from_str(value: &str) -> anyhow::Result<Self> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableResyncRequest {
    pub source_table: String,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionState {
    #[serde(default)]
    pub postgres: HashMap<String, TableCheckpoint>,
    pub postgres_cdc: Option<PostgresCdcState>,
    pub last_sync_started_at: Option<DateTime<Utc>>,
    pub last_sync_finished_at: Option<DateTime<Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct SyncStateStore {
    pub(super) pool: PgPool,
    pub(super) schema: String,
    pub(super) lock_ttl: Duration,
}

#[derive(Clone)]
pub struct StateHandle {
    pub(super) store: SyncStateStore,
    pub(super) connection_id: String,
}

pub struct ConnectionLease {
    pub(super) cleanup: Option<Arc<LeaseCleanup>>,
    pub(super) stop_tx: Option<oneshot::Sender<()>>,
    pub(super) heartbeat_task: Option<JoinHandle<()>>,
}

pub(super) struct LeaseCleanup {
    pub(super) store: SyncStateStore,
    pub(super) connection_id: String,
    pub(super) owner_id: String,
}
