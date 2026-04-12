use super::*;
use crate::types::TableRuntimeState;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct HealthResponse {
    pub(super) ok: bool,
}

#[derive(Serialize)]
pub(super) struct ReadyResponse {
    pub(super) ok: bool,
}

#[derive(Serialize)]
pub(super) struct StatusResponse {
    pub(super) service: &'static str,
    pub(super) version: &'static str,
    pub(super) started_at: DateTime<Utc>,
    pub(super) mode: String,
    pub(super) connection_id: String,
    pub(super) connection_count: usize,
    pub(super) config_hash: String,
    pub(super) deploy_revision: Option<String>,
    pub(super) last_restart_reason: String,
}

#[derive(Serialize)]
pub(super) struct ConnectionSummary {
    pub(super) id: String,
    pub(super) enabled: bool,
    pub(super) source_kind: &'static str,
    pub(super) destination_kind: &'static str,
    pub(super) last_sync_started_at: Option<DateTime<Utc>>,
    pub(super) last_sync_finished_at: Option<DateTime<Utc>>,
    pub(super) last_sync_status: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) mode: &'static str,
    pub(super) phase: &'static str,
    pub(super) reason_code: &'static str,
    pub(super) max_checkpoint_age_seconds: Option<i64>,
}

#[derive(Serialize)]
pub(super) struct ConnectionDetail {
    pub(super) config: ScrubbedConnectionConfig,
    pub(super) state: Option<ConnectionState>,
}

#[derive(Serialize)]
pub(super) struct CheckpointResponse {
    pub(super) connection_id: String,
    pub(super) state: Option<ConnectionState>,
}

#[derive(Clone, Serialize)]
pub(super) struct ProgressResponse {
    pub(super) connection_id: String,
    pub(super) state: Option<ConnectionState>,
    pub(super) current_run: Option<RunSummary>,
    pub(super) runtime: ConnectionRuntime,
    pub(super) cdc: ConnectionCdcSnapshot,
    pub(super) cdc_progress: Option<CdcProgressInsight>,
    pub(super) batch_load_queue: Option<CdcBatchLoadQueueSummary>,
    pub(super) cdc_coordinator: Option<CdcCoordinatorSummary>,
    pub(super) tables: Vec<TableProgress>,
}

#[derive(Clone, Serialize)]
pub(super) struct TableProgress {
    pub(super) table_name: String,
    pub(super) checkpoint: Option<TableCheckpoint>,
    pub(super) runtime: Option<TableRuntimeState>,
    pub(super) stats: Option<TableStatsSnapshot>,
    pub(super) phase: &'static str,
    pub(super) reason_code: &'static str,
    pub(super) checkpoint_age_seconds: Option<i64>,
    pub(super) lag_seconds: Option<i64>,
    pub(super) snapshot_chunks_total: usize,
    pub(super) snapshot_chunks_complete: usize,
}

#[derive(Clone, Serialize)]
pub(super) struct CdcProgressInsight {
    pub(super) status: &'static str,
    pub(super) primary_blocker: &'static str,
    pub(super) detail: String,
    pub(super) sequence_lag: Option<i64>,
    pub(super) wal_bytes_behind: Option<i64>,
    pub(super) pending_fragments: Option<i64>,
    pub(super) failed_fragments: Option<i64>,
    pub(super) pending_jobs: Option<i64>,
    pub(super) running_jobs: Option<i64>,
    pub(super) jobs_per_minute: Option<i64>,
    pub(super) rows_per_minute: Option<i64>,
}

#[derive(Clone, Serialize)]
pub(super) struct ConnectionRuntime {
    pub(super) connection_id: String,
    pub(super) mode: &'static str,
    pub(super) phase: &'static str,
    pub(super) reason_code: &'static str,
    pub(super) last_sync_started_at: Option<DateTime<Utc>>,
    pub(super) last_sync_finished_at: Option<DateTime<Utc>>,
    pub(super) last_sync_status: Option<String>,
    pub(super) last_error: Option<String>,
    pub(super) max_checkpoint_age_seconds: Option<i64>,
    pub(super) config_hash: String,
    pub(super) deploy_revision: Option<String>,
    pub(super) last_restart_reason: String,
}

#[derive(Serialize)]
pub(super) struct ScrubbedConfig {
    pub(super) state: ScrubbedStateConfig,
    pub(super) metadata: Option<MetadataConfig>,
    pub(super) logging: Option<LoggingConfig>,
    pub(super) admin_api: Option<ScrubbedAdminApiConfig>,
    pub(super) observability: Option<ScrubbedObservabilityConfig>,
    pub(super) sync: Option<SyncConfig>,
    pub(super) stats: Option<ScrubbedStatsConfig>,
    pub(super) connections: Vec<ScrubbedConnectionConfig>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedAdminApiConfig {
    pub(super) enabled: Option<bool>,
    pub(super) bind: Option<String>,
    pub(super) auth: Option<ScrubbedAdminApiAuthConfig>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedAdminApiAuthConfig {
    pub(super) service_jwt_allowed_issuers: Vec<String>,
    pub(super) service_jwt_allowed_audiences: Vec<String>,
    pub(super) required_scopes: Vec<String>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedStateConfig {
    pub(super) url: String,
    pub(super) schema: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedStatsConfig {
    pub(super) url: Option<String>,
    pub(super) schema: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedObservabilityConfig {
    pub(super) service_name: Option<String>,
    pub(super) otlp_traces_endpoint: Option<String>,
    pub(super) otlp_metrics_endpoint: Option<String>,
    pub(super) otlp_headers: Option<std::collections::HashMap<String, String>>,
    pub(super) metrics_interval_seconds: Option<u64>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedConnectionConfig {
    pub(super) id: String,
    pub(super) enabled: Option<bool>,
    pub(super) source: ScrubbedSourceConfig,
    pub(super) destination: ScrubbedDestinationConfig,
    pub(super) schedule: Option<crate::config::ScheduleConfig>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub(super) enum ScrubbedSourceConfig {
    #[serde(rename = "postgres")]
    Postgres(ScrubbedPostgresConfig),
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub(super) enum ScrubbedDestinationConfig {
    #[serde(rename = "bigquery")]
    BigQuery(ScrubbedBigQueryConfig),
}

#[derive(Serialize)]
pub(super) struct ScrubbedPostgresConfig {
    pub(super) url: String,
    pub(super) tables: Option<Vec<crate::config::PostgresTableConfig>>,
    pub(super) table_selection: Option<crate::config::TableSelectionConfig>,
    pub(super) batch_size: Option<usize>,
    pub(super) cdc: Option<bool>,
    pub(super) publication: Option<String>,
    pub(super) publication_mode: Option<crate::config::PostgresPublicationMode>,
    pub(super) schema_changes: Option<crate::config::SchemaChangePolicy>,
    pub(super) cdc_pipeline_id: Option<u64>,
    pub(super) cdc_batch_size: Option<usize>,
    pub(super) cdc_apply_concurrency: Option<usize>,
    pub(super) cdc_batch_load_worker_count: Option<usize>,
    pub(super) cdc_batch_load_staging_worker_count: Option<usize>,
    pub(super) cdc_batch_load_reducer_worker_count: Option<usize>,
    pub(super) cdc_max_inflight_commits: Option<usize>,
    pub(super) cdc_batch_load_reducer_max_jobs: Option<usize>,
    pub(super) cdc_batch_load_reducer_enabled: Option<bool>,
    pub(super) cdc_backlog_max_pending_fragments: Option<usize>,
    pub(super) cdc_backlog_max_oldest_pending_seconds: Option<u64>,
    pub(super) cdc_max_fill_ms: Option<u64>,
    pub(super) cdc_max_pending_events: Option<usize>,
    pub(super) cdc_idle_timeout_seconds: Option<u64>,
    pub(super) cdc_tls: Option<bool>,
    pub(super) cdc_tls_ca_path: Option<std::path::PathBuf>,
    pub(super) cdc_tls_ca: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ScrubbedBigQueryConfig {
    pub(super) project_id: String,
    pub(super) dataset: String,
    pub(super) location: Option<String>,
    pub(super) service_account_key_path: Option<std::path::PathBuf>,
    pub(super) partition_by_synced_at: Option<bool>,
    pub(super) batch_load_bucket: Option<String>,
    pub(super) batch_load_prefix: Option<String>,
    pub(super) emulator_http: Option<String>,
    pub(super) emulator_grpc: Option<String>,
}

#[derive(serde::Deserialize)]
pub(super) struct RunsQuery {
    pub(super) limit: Option<usize>,
}

#[derive(serde::Deserialize)]
pub(super) struct StreamQuery {
    pub(super) connection: String,
}

#[derive(serde::Deserialize)]
pub(super) struct ResyncTableRequest {
    pub(super) table: String,
}

#[derive(Serialize)]
pub(super) struct ResyncTableResponse {
    pub(super) connection_id: String,
    pub(super) table: String,
    pub(super) requested: bool,
    pub(super) restart_requested: bool,
}

#[derive(Clone, Copy)]
pub(super) struct RuntimeMetadata<'a> {
    pub(super) config_hash: &'a str,
    pub(super) deploy_revision: Option<&'a str>,
    pub(super) last_restart_reason: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PostgresCdcRuntimeState {
    Following,
    Initializing,
    ContinuityLost,
    Unknown,
}

#[derive(Debug, Clone)]
pub(super) struct CachedPostgresCdcSlotState {
    pub(super) sampler_status: &'static str,
    pub(super) sampled_at: Option<DateTime<Utc>>,
    pub(super) snapshot: Option<PostgresCdcSlotSnapshot>,
}

#[derive(Debug, Clone)]
pub(super) struct PostgresCdcSlotSnapshot {
    pub(super) slot_name: Option<String>,
    pub(super) active: bool,
    pub(super) restart_lsn: Option<String>,
    pub(super) confirmed_flush_lsn: Option<String>,
    pub(super) current_wal_lsn: Option<String>,
    pub(super) wal_bytes_retained_by_slot: Option<i64>,
    pub(super) wal_bytes_behind_confirmed: Option<i64>,
    pub(super) continuity_lost: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ConnectionCdcSnapshot {
    pub(super) sampler_status: &'static str,
    pub(super) sampled_at: Option<DateTime<Utc>>,
    pub(super) slot_name: Option<String>,
    pub(super) slot_active: Option<bool>,
    pub(super) current_wal_lsn: Option<String>,
    pub(super) restart_lsn: Option<String>,
    pub(super) confirmed_flush_lsn: Option<String>,
    pub(super) wal_bytes_retained_by_slot: Option<i64>,
    pub(super) wal_bytes_behind_confirmed: Option<i64>,
}

pub(super) type CdcSlotSamplerCache =
    Arc<HashMap<String, watch::Sender<CachedPostgresCdcSlotState>>>;

impl CachedPostgresCdcSlotState {
    pub(super) fn unknown() -> Self {
        Self {
            sampler_status: "unknown",
            sampled_at: Some(Utc::now()),
            snapshot: None,
        }
    }

    pub(super) fn sampled(snapshot: Option<PostgresCdcSlotSnapshot>) -> Self {
        Self {
            sampler_status: "ok",
            sampled_at: Some(Utc::now()),
            snapshot,
        }
    }
}

impl ConnectionCdcSnapshot {
    pub(super) fn from_cached(state: Option<&CachedPostgresCdcSlotState>) -> Self {
        let snapshot = state.and_then(|state| state.snapshot.as_ref());
        Self {
            sampler_status: state.map_or("disabled", |state| state.sampler_status),
            sampled_at: state.and_then(|state| state.sampled_at),
            slot_name: snapshot.and_then(|snapshot| snapshot.slot_name.clone()),
            slot_active: snapshot.map(|snapshot| snapshot.active),
            current_wal_lsn: snapshot.and_then(|snapshot| snapshot.current_wal_lsn.clone()),
            restart_lsn: snapshot.and_then(|snapshot| snapshot.restart_lsn.clone()),
            confirmed_flush_lsn: snapshot.and_then(|snapshot| snapshot.confirmed_flush_lsn.clone()),
            wal_bytes_retained_by_slot: snapshot
                .and_then(|snapshot| snapshot.wal_bytes_retained_by_slot),
            wal_bytes_behind_confirmed: snapshot
                .and_then(|snapshot| snapshot.wal_bytes_behind_confirmed),
        }
    }
}

pub struct AdminApiHandle {
    pub(super) thread: thread::JoinHandle<anyhow::Result<()>>,
}

impl AdminApiHandle {
    pub fn join(self) -> anyhow::Result<()> {
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("admin api thread panicked"))?
    }
}

#[derive(Serialize)]
pub(super) struct StreamEnvelope<T> {
    #[serde(rename = "type")]
    pub(super) event_type: &'static str,
    pub(super) connection_id: String,
    pub(super) seq: u64,
    pub(super) at: DateTime<Utc>,
    pub(super) data: T,
}

#[derive(Serialize)]
pub(super) struct ServiceHeartbeatData {
    pub(super) service: &'static str,
    pub(super) version: &'static str,
    pub(super) started_at: DateTime<Utc>,
    pub(super) uptime_seconds: i64,
    pub(super) mode: String,
    pub(super) managed_connections: usize,
    pub(super) deploy_revision: Option<String>,
    pub(super) config_hash: String,
    pub(super) last_restart_reason: String,
}

#[derive(Serialize)]
pub(super) struct ConnectionThroughputData {
    pub(super) run_id: Option<String>,
    pub(super) status: Option<String>,
    pub(super) rows_read_total: i64,
    pub(super) rows_written_total: i64,
    pub(super) rows_deleted_total: i64,
    pub(super) rows_upserted_total: i64,
    pub(super) extract_ms_total: i64,
    pub(super) load_ms_total: i64,
    pub(super) rows_read_per_sec: Option<f64>,
    pub(super) rows_written_per_sec: Option<f64>,
    pub(super) rows_deleted_per_sec: Option<f64>,
    pub(super) rows_upserted_per_sec: Option<f64>,
}

#[derive(Serialize)]
pub(super) struct ConnectionCdcData {
    pub(super) sampler_status: &'static str,
    pub(super) sampled_at: Option<DateTime<Utc>>,
    pub(super) slot_name: Option<String>,
    pub(super) slot_active: Option<bool>,
    pub(super) current_wal_lsn: Option<String>,
    pub(super) restart_lsn: Option<String>,
    pub(super) confirmed_flush_lsn: Option<String>,
    pub(super) wal_bytes_retained_by_slot: Option<i64>,
    pub(super) wal_bytes_behind_confirmed: Option<i64>,
}

#[derive(Serialize)]
pub(super) struct StreamErrorData {
    pub(super) message: String,
}

pub(super) struct StreamCursor {
    pub(super) state: AdminApiState,
    pub(super) connection_id: String,
    pub(super) seq: u64,
    pub(super) interval: tokio::time::Interval,
    pub(super) pending_events: VecDeque<SseEvent>,
    pub(super) previous_run_snapshot: Option<(DateTime<Utc>, RunStatsSnapshot)>,
}
