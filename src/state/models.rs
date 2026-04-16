use super::*;
use crate::retry::{ErrorReasonCode, SyncRetryClass};
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DynamoDbShardState {
    pub sequence_number: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DynamoDbFollowState {
    pub table_name: String,
    pub stream_arn: Option<String>,
    pub cutover_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub shard_checkpoints: HashMap<String, DynamoDbShardState>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CdcReplayCleanupSummary {
    pub discarded_jobs: u64,
    pub discarded_fragments: u64,
    pub repaired_terminal_fragments: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CdcBatchLoadJobStatus {
    #[default]
    Pending,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CdcLedgerStage {
    #[default]
    Received,
    Staged,
    Loaded,
    ApplyPending,
    Applying,
    Applied,
    Failed,
    Blocked,
}

impl CdcLedgerStage {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Staged => "staged",
            Self::Loaded => "loaded",
            Self::ApplyPending => "apply_pending",
            Self::Applying => "applying",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }

    pub(super) fn from_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "received" => Ok(Self::Received),
            "staged" => Ok(Self::Staged),
            "loaded" => Ok(Self::Loaded),
            "apply_pending" => Ok(Self::ApplyPending),
            "applying" => Ok(Self::Applying),
            "applied" => Ok(Self::Applied),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            other => anyhow::bail!("unknown CDC ledger stage {}", other),
        }
    }

    pub(super) fn from_job_status(status: CdcBatchLoadJobStatus) -> Self {
        match status {
            CdcBatchLoadJobStatus::Pending => Self::Received,
            CdcBatchLoadJobStatus::Running => Self::Applying,
            CdcBatchLoadJobStatus::Succeeded => Self::Applied,
            CdcBatchLoadJobStatus::Failed => Self::Failed,
        }
    }

    pub(super) fn from_fragment_status(status: CdcCommitFragmentStatus) -> Self {
        match status {
            CdcCommitFragmentStatus::Pending => Self::Received,
            CdcCommitFragmentStatus::Succeeded => Self::Applied,
            CdcCommitFragmentStatus::Failed => Self::Failed,
        }
    }

    pub(super) fn normalize_for_job_status(stage: Self, status: CdcBatchLoadJobStatus) -> Self {
        if stage == Self::Received && !matches!(status, CdcBatchLoadJobStatus::Pending) {
            Self::from_job_status(status)
        } else {
            stage
        }
    }

    pub(super) fn normalize_for_fragment_status(
        stage: Self,
        status: CdcCommitFragmentStatus,
    ) -> Self {
        if stage == Self::Received && !matches!(status, CdcCommitFragmentStatus::Pending) {
            Self::from_fragment_status(status)
        } else {
            stage
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcBatchLoadJobRecord {
    pub job_id: String,
    pub table_key: String,
    pub first_sequence: u64,
    pub status: CdcBatchLoadJobStatus,
    pub stage: CdcLedgerStage,
    pub payload_json: String,
    pub attempt_count: i32,
    pub retry_class: Option<SyncRetryClass>,
    pub last_error: Option<String>,
    pub staging_table: Option<String>,
    pub artifact_uri: Option<String>,
    pub load_job_id: Option<String>,
    pub merge_job_id: Option<String>,
    pub primary_key_lane: Option<String>,
    pub barrier_kind: Option<String>,
    pub ledger_metadata_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum CdcCommitFragmentStatus {
    #[default]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcCommitFragmentRecord {
    pub fragment_id: String,
    pub job_id: String,
    pub sequence: u64,
    pub commit_lsn: String,
    pub table_key: String,
    pub status: CdcCommitFragmentStatus,
    pub stage: CdcLedgerStage,
    pub row_count: i64,
    pub upserted_count: i64,
    pub deleted_count: i64,
    pub last_error: Option<String>,
    pub artifact_uri: Option<String>,
    pub staging_table: Option<String>,
    pub load_job_id: Option<String>,
    pub merge_job_id: Option<String>,
    pub primary_key_lane: Option<String>,
    pub barrier_kind: Option<String>,
    pub ledger_metadata_json: Option<String>,
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
pub struct CdcDurableApplyFrontier {
    pub next_sequence_to_ack: u64,
    pub commit_lsn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CdcCoordinatorSummary {
    pub next_sequence_to_ack: u64,
    pub last_enqueued_sequence: Option<u64>,
    pub sequence_lag: Option<i64>,
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
    #[serde(default)]
    pub failed_retryable_jobs: i64,
    #[serde(default)]
    pub failed_permanent_jobs: i64,
    #[serde(default)]
    pub failed_snapshot_handoff_jobs: i64,
    #[serde(default)]
    pub failed_unclassified_jobs: i64,
    pub received_jobs: i64,
    pub staged_jobs: i64,
    pub loaded_jobs: i64,
    #[serde(default)]
    pub blocked_jobs: i64,
    #[serde(default)]
    pub snapshot_handoff_waiting_jobs: i64,
    pub applying_jobs: i64,
    pub applied_jobs: i64,
    pub failed_stage_jobs: i64,
    pub oldest_pending_age_seconds: Option<i64>,
    pub oldest_running_age_seconds: Option<i64>,
    pub oldest_received_age_seconds: Option<i64>,
    pub oldest_loaded_age_seconds: Option<i64>,
    pub oldest_blocked_age_seconds: Option<i64>,
    pub oldest_applying_age_seconds: Option<i64>,
    pub jobs_per_minute: i64,
    pub rows_per_minute: i64,
    pub avg_job_duration_seconds: Option<f64>,
    pub top_queued_tables: Vec<CdcBatchLoadQueueTableSummary>,
    pub top_loaded_tables: Vec<CdcBatchLoadLoadedTableSummary>,
    pub latest_failed_error: Option<String>,
    pub latest_failed_retry_class: Option<String>,
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
    #[serde(default)]
    pub dynamodb: HashMap<String, TableCheckpoint>,
    pub postgres_cdc: Option<PostgresCdcState>,
    pub dynamodb_follow: Option<DynamoDbFollowState>,
    pub last_sync_started_at: Option<DateTime<Utc>>,
    pub last_sync_finished_at: Option<DateTime<Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error_reason: Option<ErrorReasonCode>,
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
