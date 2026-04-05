#[cfg(test)]
mod route_tests;

use crate::config::{
    AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
    DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
    SalesforceConfig, SourceConfig, SyncConfig,
};
use crate::runner::ShutdownSignal;
use crate::state::{
    CdcBatchLoadQueueSummary, CdcCoordinatorSummary, ConnectionState, SyncState, SyncStateStore,
};
use crate::stats::{
    RunStatsSnapshot, RunSummary, StatsDb, TableStatsSnapshot, live_run_snapshot, summarize_run,
};
use crate::types::TableCheckpoint;
use anyhow::Context;
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream};
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Builder as TokioRuntimeBuilder;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, timeout};
use tracing::warn;
use url::Url;

const STREAM_INTERVAL: Duration = Duration::from_secs(2);
const STREAM_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const STREAM_ACTIVE_TABLE_LIMIT: usize = 25;
const CDC_SLOT_SAMPLER_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct AdminApiState {
    cfg: Arc<Config>,
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
    cdc_slot_sampler_cache: CdcSlotSamplerCache,
    auth_verifier: Arc<AdminApiServiceJwtVerifier>,
    started_at: DateTime<Utc>,
    mode: String,
    connection_id: String,
    managed_connection_count: usize,
    config_hash: String,
    deploy_revision: Option<String>,
    last_restart_reason: String,
}

#[derive(Clone)]
struct AdminApiServiceJwtVerifier {
    public_keys: HashMap<String, DecodingKey>,
    allowed_issuers: Vec<String>,
    allowed_audiences: Vec<String>,
    required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum AdminApiServiceJwtAudience {
    Single(String),
    Multiple(Vec<String>),
}

impl AdminApiServiceJwtAudience {
    fn is_empty(&self) -> bool {
        match self {
            Self::Single(value) => value.trim().is_empty(),
            Self::Multiple(values) => values.iter().all(|value| value.trim().is_empty()),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct AdminApiServiceJwtClaims {
    exp: usize,
    iat: usize,
    iss: String,
    aud: AdminApiServiceJwtAudience,
    sub: String,
    jti: String,
    scope: String,
}

#[derive(Debug, Clone, Copy)]
struct AdminApiAuthContext;

#[async_trait]
trait AdminStateBackend: Send + Sync {
    async fn ping(&self) -> anyhow::Result<()>;
    async fn load_state(&self) -> anyhow::Result<SyncState>;
    async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>>;
    async fn load_cdc_batch_load_queue_summary(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadQueueSummary>;
    async fn load_cdc_coordinator_summary(
        &self,
        connection_id: &str,
        wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary>;
}

#[async_trait]
trait AdminStatsBackend: Send + Sync {
    async fn ping(&self) -> anyhow::Result<()>;
    async fn recent_runs(
        &self,
        connection_id: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<RunSummary>>;
    async fn run_tables(&self, run_id: &str) -> anyhow::Result<Vec<TableStatsSnapshot>>;
}

#[async_trait]
impl AdminStateBackend for SyncStateStore {
    async fn ping(&self) -> anyhow::Result<()> {
        SyncStateStore::ping(self).await
    }

    async fn load_state(&self) -> anyhow::Result<SyncState> {
        SyncStateStore::load_state(self).await
    }

    async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>> {
        SyncStateStore::load_connection_state(self, connection_id).await
    }

    async fn load_cdc_batch_load_queue_summary(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadQueueSummary> {
        SyncStateStore::load_cdc_batch_load_queue_summary(self, connection_id).await
    }

    async fn load_cdc_coordinator_summary(
        &self,
        connection_id: &str,
        wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary> {
        SyncStateStore::load_cdc_coordinator_summary(
            self,
            connection_id,
            wal_bytes_behind_confirmed,
        )
        .await
    }
}

#[async_trait]
impl AdminStatsBackend for StatsDb {
    async fn ping(&self) -> anyhow::Result<()> {
        StatsDb::ping(self).await
    }

    async fn recent_runs(
        &self,
        connection_id: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<RunSummary>> {
        StatsDb::recent_runs(self, connection_id, limit).await
    }

    async fn run_tables(&self, run_id: &str) -> anyhow::Result<Vec<TableStatsSnapshot>> {
        StatsDb::run_tables(self, run_id).await
    }
}

#[derive(Debug)]
pub struct AdminApiError(anyhow::Error);

impl IntoResponse for AdminApiError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": self.0.to_string(),
            })),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for AdminApiError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
struct AdminApiAuthError(&'static str);

impl IntoResponse for AdminApiAuthError {
    fn into_response(self) -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": self.0,
            })),
        )
            .into_response()
    }
}

impl AdminApiServiceJwtVerifier {
    fn from_config(config: &AdminApiAuthConfig) -> anyhow::Result<Self> {
        install_jwt_crypto_provider();
        let public_keys = config
            .resolved_public_keys()?
            .into_iter()
            .map(|(kid, pem)| {
                let key = DecodingKey::from_rsa_pem(pem.as_bytes())
                    .with_context(|| format!("invalid RS256 public key for kid {}", kid))?;
                Ok((kid, key))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()?;

        Ok(Self {
            public_keys,
            allowed_issuers: config.resolved_allowed_issuers(),
            allowed_audiences: config.resolved_allowed_audiences(),
            required_scopes: config.resolved_required_scopes(),
        })
    }

    fn validate_bearer(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<AdminApiAuthContext, AdminApiAuthError> {
        let auth_header = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(AdminApiAuthError("missing Authorization header"))?;
        if !auth_header.starts_with("Bearer ") {
            return Err(AdminApiAuthError("invalid Authorization header format"));
        }
        let token = auth_header.trim_start_matches("Bearer ").trim();
        if token.is_empty() {
            return Err(AdminApiAuthError("missing bearer token"));
        }
        self.validate_token(token)
    }

    fn validate_token(&self, token: &str) -> Result<AdminApiAuthContext, AdminApiAuthError> {
        let header = decode_header(token).map_err(|_| AdminApiAuthError("invalid token"))?;
        if header.alg != Algorithm::RS256 {
            return Err(AdminApiAuthError("invalid token algorithm"));
        }
        let key_id = header
            .kid
            .as_deref()
            .ok_or(AdminApiAuthError("missing token key id"))?;
        let public_key = self
            .public_keys
            .get(key_id)
            .ok_or(AdminApiAuthError("unknown token key id"))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_required_spec_claims(&["exp", "iat", "iss", "aud", "sub", "jti"]);
        validation.set_issuer(&self.allowed_issuers);
        validation.set_audience(&self.allowed_audiences);

        let decoded = decode::<AdminApiServiceJwtClaims>(token, public_key, &validation)
            .map_err(map_jwt_decode_error)?;

        if !key_id.starts_with(&format!("{}-", decoded.claims.iss)) {
            return Err(AdminApiAuthError("token key id does not match issuer"));
        }
        if decoded.claims.exp == 0
            || decoded.claims.iat == 0
            || decoded.claims.aud.is_empty()
            || decoded.claims.sub.trim().is_empty()
            || decoded.claims.jti.trim().is_empty()
        {
            return Err(AdminApiAuthError("invalid token"));
        }

        let scopes = normalize_scopes(&decoded.claims.scope);
        if !self
            .required_scopes
            .iter()
            .any(|required| scopes.iter().any(|scope| scope == required))
        {
            return Err(AdminApiAuthError("service token missing required scope"));
        }

        let _ = (decoded.claims.sub, decoded.claims.iss, scopes, key_id);
        Ok(AdminApiAuthContext)
    }
}

fn normalize_scopes(scope: &str) -> Vec<String> {
    scope
        .split_whitespace()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn install_jwt_crypto_provider() {
    let _ = JWT_CRYPTO_PROVIDER.install_default();
}

fn map_jwt_decode_error(err: jsonwebtoken::errors::Error) -> AdminApiAuthError {
    if matches!(
        err.kind(),
        jsonwebtoken::errors::ErrorKind::ExpiredSignature
    ) {
        AdminApiAuthError("token has expired")
    } else {
        AdminApiAuthError("invalid token")
    }
}

async fn require_admin_api_auth(
    State(state): State<AdminApiState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AdminApiAuthError> {
    let auth = state.auth_verifier.validate_bearer(request.headers())?;
    request.extensions_mut().insert(auth);
    Ok(next.run(request).await)
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct ReadyResponse {
    ok: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    service: &'static str,
    version: &'static str,
    started_at: DateTime<Utc>,
    mode: String,
    connection_id: String,
    connection_count: usize,
    config_hash: String,
    deploy_revision: Option<String>,
    last_restart_reason: String,
}

#[derive(Serialize)]
struct ConnectionSummary {
    id: String,
    enabled: bool,
    source_kind: &'static str,
    destination_kind: &'static str,
    last_sync_started_at: Option<DateTime<Utc>>,
    last_sync_finished_at: Option<DateTime<Utc>>,
    last_sync_status: Option<String>,
    last_error: Option<String>,
    phase: &'static str,
    reason_code: &'static str,
    max_checkpoint_age_seconds: Option<i64>,
}

#[derive(Serialize)]
struct ConnectionDetail {
    config: ScrubbedConnectionConfig,
    state: Option<ConnectionState>,
}

#[derive(Serialize)]
struct CheckpointResponse {
    connection_id: String,
    state: Option<ConnectionState>,
}

#[derive(Clone, Serialize)]
struct ProgressResponse {
    connection_id: String,
    state: Option<ConnectionState>,
    current_run: Option<RunSummary>,
    runtime: ConnectionRuntime,
    cdc: ConnectionCdcSnapshot,
    batch_load_queue: Option<CdcBatchLoadQueueSummary>,
    cdc_coordinator: Option<CdcCoordinatorSummary>,
    tables: Vec<TableProgress>,
}

#[derive(Clone, Serialize)]
struct TableProgress {
    table_name: String,
    checkpoint: Option<TableCheckpoint>,
    stats: Option<TableStatsSnapshot>,
    phase: &'static str,
    reason_code: &'static str,
    checkpoint_age_seconds: Option<i64>,
    lag_seconds: Option<i64>,
    snapshot_chunks_total: usize,
    snapshot_chunks_complete: usize,
}

#[derive(Clone, Serialize)]
struct ConnectionRuntime {
    connection_id: String,
    phase: &'static str,
    reason_code: &'static str,
    last_sync_started_at: Option<DateTime<Utc>>,
    last_sync_finished_at: Option<DateTime<Utc>>,
    last_sync_status: Option<String>,
    last_error: Option<String>,
    max_checkpoint_age_seconds: Option<i64>,
    config_hash: String,
    deploy_revision: Option<String>,
    last_restart_reason: String,
}

#[derive(Serialize)]
struct ScrubbedConfig {
    state: ScrubbedStateConfig,
    metadata: Option<MetadataConfig>,
    logging: Option<LoggingConfig>,
    admin_api: Option<ScrubbedAdminApiConfig>,
    observability: Option<ScrubbedObservabilityConfig>,
    sync: Option<SyncConfig>,
    stats: Option<ScrubbedStatsConfig>,
    connections: Vec<ScrubbedConnectionConfig>,
}

#[derive(Serialize)]
struct ScrubbedAdminApiConfig {
    enabled: Option<bool>,
    bind: Option<String>,
    auth: Option<ScrubbedAdminApiAuthConfig>,
}

#[derive(Serialize)]
struct ScrubbedAdminApiAuthConfig {
    service_jwt_allowed_issuers: Vec<String>,
    service_jwt_allowed_audiences: Vec<String>,
    required_scopes: Vec<String>,
}

#[derive(Serialize)]
struct ScrubbedStateConfig {
    url: String,
    schema: Option<String>,
}

#[derive(Serialize)]
struct ScrubbedStatsConfig {
    url: Option<String>,
    schema: Option<String>,
}

#[derive(Serialize)]
struct ScrubbedObservabilityConfig {
    service_name: Option<String>,
    otlp_traces_endpoint: Option<String>,
    otlp_metrics_endpoint: Option<String>,
    otlp_headers: Option<std::collections::HashMap<String, String>>,
    metrics_interval_seconds: Option<u64>,
}

#[derive(Serialize)]
struct ScrubbedConnectionConfig {
    id: String,
    enabled: Option<bool>,
    source: ScrubbedSourceConfig,
    destination: ScrubbedDestinationConfig,
    schedule: Option<crate::config::ScheduleConfig>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ScrubbedSourceConfig {
    #[serde(rename = "postgres")]
    Postgres(ScrubbedPostgresConfig),
    #[serde(rename = "salesforce")]
    Salesforce(ScrubbedSalesforceConfig),
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum ScrubbedDestinationConfig {
    #[serde(rename = "bigquery")]
    BigQuery(ScrubbedBigQueryConfig),
}

#[derive(Serialize)]
struct ScrubbedPostgresConfig {
    url: String,
    tables: Option<Vec<crate::config::PostgresTableConfig>>,
    table_selection: Option<crate::config::TableSelectionConfig>,
    batch_size: Option<usize>,
    cdc: Option<bool>,
    publication: Option<String>,
    publication_mode: Option<crate::config::PostgresPublicationMode>,
    schema_changes: Option<crate::config::SchemaChangePolicy>,
    cdc_pipeline_id: Option<u64>,
    cdc_batch_size: Option<usize>,
    cdc_apply_concurrency: Option<usize>,
    cdc_max_fill_ms: Option<u64>,
    cdc_max_pending_events: Option<usize>,
    cdc_idle_timeout_seconds: Option<u64>,
    cdc_tls: Option<bool>,
    cdc_tls_ca_path: Option<std::path::PathBuf>,
    cdc_tls_ca: Option<String>,
}

#[derive(Serialize)]
struct ScrubbedSalesforceConfig {
    login_url: Option<String>,
    instance_url: Option<String>,
    client_id: String,
    objects: Option<Vec<crate::config::SalesforceObjectConfig>>,
    object_selection: Option<crate::config::ObjectSelectionConfig>,
    polling_interval_seconds: Option<u64>,
    api_version: Option<String>,
}

#[derive(Serialize)]
struct ScrubbedBigQueryConfig {
    project_id: String,
    dataset: String,
    location: Option<String>,
    service_account_key_path: Option<std::path::PathBuf>,
    partition_by_synced_at: Option<bool>,
    storage_write_enabled: Option<bool>,
    batch_load_bucket: Option<String>,
    batch_load_prefix: Option<String>,
    emulator_http: Option<String>,
    emulator_grpc: Option<String>,
}

#[derive(serde::Deserialize)]
struct RunsQuery {
    limit: Option<usize>,
}

#[derive(serde::Deserialize)]
struct StreamQuery {
    connection: String,
}

#[derive(Clone, Copy)]
struct RuntimeMetadata<'a> {
    config_hash: &'a str,
    deploy_revision: Option<&'a str>,
    last_restart_reason: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresCdcRuntimeState {
    Following,
    Initializing,
    ContinuityLost,
    Unknown,
}

#[derive(Debug, Clone)]
struct CachedPostgresCdcSlotState {
    sampler_status: &'static str,
    sampled_at: Option<DateTime<Utc>>,
    snapshot: Option<PostgresCdcSlotSnapshot>,
}

#[derive(Debug, Clone)]
struct PostgresCdcSlotSnapshot {
    slot_name: Option<String>,
    active: bool,
    restart_lsn: Option<String>,
    confirmed_flush_lsn: Option<String>,
    current_wal_lsn: Option<String>,
    wal_bytes_retained_by_slot: Option<i64>,
    wal_bytes_behind_confirmed: Option<i64>,
    continuity_lost: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ConnectionCdcSnapshot {
    sampler_status: &'static str,
    sampled_at: Option<DateTime<Utc>>,
    slot_name: Option<String>,
    slot_active: Option<bool>,
    current_wal_lsn: Option<String>,
    restart_lsn: Option<String>,
    confirmed_flush_lsn: Option<String>,
    wal_bytes_retained_by_slot: Option<i64>,
    wal_bytes_behind_confirmed: Option<i64>,
}

type CdcSlotSamplerCache = Arc<HashMap<String, watch::Sender<CachedPostgresCdcSlotState>>>;

impl CachedPostgresCdcSlotState {
    fn unknown() -> Self {
        Self {
            sampler_status: "unknown",
            sampled_at: Some(Utc::now()),
            snapshot: None,
        }
    }

    fn sampled(snapshot: Option<PostgresCdcSlotSnapshot>) -> Self {
        Self {
            sampler_status: "ok",
            sampled_at: Some(Utc::now()),
            snapshot,
        }
    }
}

impl ConnectionCdcSnapshot {
    fn from_cached(state: Option<&CachedPostgresCdcSlotState>) -> Self {
        let snapshot = state.and_then(|state| state.snapshot.as_ref());
        Self {
            sampler_status: state
                .map(|state| state.sampler_status)
                .unwrap_or("disabled"),
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
    thread: thread::JoinHandle<anyhow::Result<()>>,
}

impl AdminApiHandle {
    pub fn join(self) -> anyhow::Result<()> {
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("admin api thread panicked"))?
    }
}

fn spawn_admin_server_thread(
    state: AdminApiState,
    bind: SocketAddr,
    shutdown: ShutdownSignal,
) -> anyhow::Result<(AdminApiHandle, oneshot::Receiver<anyhow::Result<()>>)> {
    let (ready_tx, ready_rx) = oneshot::channel::<anyhow::Result<()>>();

    let handle = thread::Builder::new()
        .name("cdsync-admin".to_string())
        .spawn(move || -> anyhow::Result<()> {
            let runtime = TokioRuntimeBuilder::new_current_thread()
                .enable_all()
                .build()
                .context("building admin API runtime")?;
            runtime.block_on(async move {
                let listener = match TcpListener::bind(bind).await {
                    Ok(listener) => {
                        let _ = ready_tx.send(Ok(()));
                        listener
                    }
                    Err(err) => {
                        let err = anyhow::Error::from(err);
                        let _ = ready_tx.send(Err(anyhow::anyhow!(err.to_string())));
                        return Err(err);
                    }
                };

                spawn_cdc_slot_sampler_tasks(
                    state.cfg.clone(),
                    state.state_store.clone(),
                    state.cdc_slot_sampler_cache.clone(),
                    shutdown.clone(),
                );

                let app = router(state);
                let mut shutdown = shutdown;
                axum::serve(listener, app)
                    .with_graceful_shutdown(async move {
                        let _ = shutdown.changed().await;
                    })
                    .await
                    .map_err(anyhow::Error::from)
            })
        })
        .context("spawning admin API thread")?;

    Ok((AdminApiHandle { thread: handle }, ready_rx))
}

#[derive(Serialize)]
struct StreamEnvelope<T> {
    #[serde(rename = "type")]
    event_type: &'static str,
    connection_id: String,
    seq: u64,
    at: DateTime<Utc>,
    data: T,
}

#[derive(Serialize)]
struct ServiceHeartbeatData {
    service: &'static str,
    version: &'static str,
    started_at: DateTime<Utc>,
    uptime_seconds: i64,
    mode: String,
    deploy_revision: Option<String>,
    config_hash: String,
    last_restart_reason: String,
}

#[derive(Serialize)]
struct ConnectionThroughputData {
    run_id: Option<String>,
    status: Option<String>,
    rows_read_total: i64,
    rows_written_total: i64,
    rows_deleted_total: i64,
    rows_upserted_total: i64,
    extract_ms_total: i64,
    load_ms_total: i64,
    rows_read_per_sec: Option<f64>,
    rows_written_per_sec: Option<f64>,
    rows_deleted_per_sec: Option<f64>,
    rows_upserted_per_sec: Option<f64>,
}

#[derive(Serialize)]
struct ConnectionCdcData {
    sampler_status: &'static str,
    sampled_at: Option<DateTime<Utc>>,
    slot_name: Option<String>,
    slot_active: Option<bool>,
    current_wal_lsn: Option<String>,
    restart_lsn: Option<String>,
    confirmed_flush_lsn: Option<String>,
    wal_bytes_retained_by_slot: Option<i64>,
    wal_bytes_behind_confirmed: Option<i64>,
}

#[derive(Serialize)]
struct StreamErrorData {
    message: String,
}

struct StreamCursor {
    state: AdminApiState,
    connection_id: String,
    seq: u64,
    interval: tokio::time::Interval,
    pending_events: VecDeque<SseEvent>,
    previous_run_snapshot: Option<(DateTime<Utc>, RunStatsSnapshot)>,
}

const CDC_SLOT_INSPECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

fn config_hash(cfg: &Config) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(cfg).context("serializing config for hashing")?;
    canonicalize_json_value(&mut value);
    let bytes = serde_json::to_vec(&value).context("serializing canonical config for hashing")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn is_postgres_cdc_connection(connection: &ConnectionConfig) -> bool {
    matches!(
        &connection.source,
        SourceConfig::Postgres(pg) if pg.cdc.unwrap_or(true)
    )
}

fn uses_cdc_batch_load_queue(connection: &ConnectionConfig) -> bool {
    matches!(
        &connection.destination,
        crate::config::DestinationConfig::BigQuery(bq)
            if bq.batch_load_bucket.is_some() && bq.emulator_http.is_none()
    )
}

fn build_cdc_slot_sampler_cache(cfg: &Config) -> CdcSlotSamplerCache {
    let mut cache = HashMap::new();
    for connection in &cfg.connections {
        if is_postgres_cdc_connection(connection) {
            let (tx, _rx) = watch::channel(CachedPostgresCdcSlotState {
                sampler_status: "initializing",
                sampled_at: None,
                snapshot: None,
            });
            cache.insert(connection.id.clone(), tx);
        }
    }
    Arc::new(cache)
}

async fn sample_cached_postgres_cdc_slot_state(
    connection: &ConnectionConfig,
    state_store: &Arc<dyn AdminStateBackend>,
) -> CachedPostgresCdcSlotState {
    let connection_state = match state_store.load_connection_state(&connection.id).await {
        Ok(connection_state) => connection_state,
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to load connection state for postgres CDC slot sampler"
            );
            return CachedPostgresCdcSlotState::unknown();
        }
    };
    match load_postgres_cdc_slot_snapshot(connection, connection_state.as_ref()).await {
        Ok(snapshot) => CachedPostgresCdcSlotState::sampled(snapshot),
        Err(()) => CachedPostgresCdcSlotState::unknown(),
    }
}

fn publish_cached_postgres_cdc_slot_state(
    tx: &watch::Sender<CachedPostgresCdcSlotState>,
    sample: CachedPostgresCdcSlotState,
) {
    tx.send_replace(sample);
}

fn datetime_age_seconds(value: Option<DateTime<Utc>>, now: DateTime<Utc>) -> Option<u64> {
    value.map(|value| (now - value).num_seconds().max(0) as u64)
}

fn spawn_cdc_slot_sampler_tasks(
    cfg: Arc<Config>,
    state_store: Arc<dyn AdminStateBackend>,
    cache: CdcSlotSamplerCache,
    shutdown: ShutdownSignal,
) {
    for connection in &cfg.connections {
        if !is_postgres_cdc_connection(connection) {
            continue;
        }
        let Some(tx) = cache.get(&connection.id).cloned() else {
            continue;
        };

        let connection = connection.clone();
        let state_store = state_store.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let initial = sample_cached_postgres_cdc_slot_state(&connection, &state_store).await;
            publish_cached_postgres_cdc_slot_state(&tx, initial);
            let mut interval = tokio::time::interval(CDC_SLOT_SAMPLER_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let sample = sample_cached_postgres_cdc_slot_state(&connection, &state_store).await;
                        if uses_cdc_batch_load_queue(&connection)
                            && let Ok(summary) = state_store
                                .load_cdc_coordinator_summary(
                                    &connection.id,
                                    sample
                                        .snapshot
                                        .as_ref()
                                        .and_then(|snapshot| snapshot.wal_bytes_behind_confirmed),
                                )
                                .await
                        {
                            let now = Utc::now();
                            crate::telemetry::record_cdc_coordinator_diagnostics(
                                &connection.id,
                                summary.pending_fragments.max(0) as u64,
                                summary.failed_fragments.max(0) as u64,
                                summary.next_sequence_to_ack,
                                summary.oldest_pending_age_seconds.map(|value| value.max(0) as u64),
                                datetime_age_seconds(summary.last_relevant_change_seen_at, now),
                                datetime_age_seconds(summary.last_status_update_sent_at, now),
                                datetime_age_seconds(summary.last_keepalive_reply_at, now),
                                summary.wal_bytes_unattributed_or_idle.map(|value| value.max(0) as u64),
                            );
                        }
                        publish_cached_postgres_cdc_slot_state(&tx, sample);
                    }
                    changed = shutdown.changed() => {
                        let _ = changed;
                        break;
                    }
                }
            }
        });
    }
}

fn cached_postgres_cdc_slot_state(
    state: &AdminApiState,
    connection: &ConnectionConfig,
) -> Option<CachedPostgresCdcSlotState> {
    if !is_postgres_cdc_connection(connection) {
        return None;
    }
    state
        .cdc_slot_sampler_cache
        .get(&connection.id)
        .map(|sender| sender.borrow().clone())
        .or(Some(CachedPostgresCdcSlotState {
            sampler_status: "disabled",
            sampled_at: None,
            snapshot: None,
        }))
}

fn cached_postgres_cdc_runtime_state(
    state: &AdminApiState,
    connection: &ConnectionConfig,
) -> Option<PostgresCdcRuntimeState> {
    match cached_postgres_cdc_slot_state(state, connection) {
        Some(slot_state) if slot_state.sampler_status == "ok" => {
            postgres_cdc_runtime_state_from_snapshot(slot_state.snapshot.as_ref())
        }
        Some(slot_state) if matches!(slot_state.sampler_status, "unknown" | "initializing") => {
            Some(PostgresCdcRuntimeState::Unknown)
        }
        None => None,
        Some(_) => None,
    }
}

pub async fn spawn_admin_api(
    cfg: &Config,
    connection_id: &str,
    managed_connection_count: usize,
    mode: &str,
    shutdown: ShutdownSignal,
) -> anyhow::Result<Option<AdminApiHandle>> {
    let Some(admin_cfg) = cfg.admin_api.as_ref() else {
        return Ok(None);
    };
    if !admin_cfg.enabled() {
        return Ok(None);
    }

    let bind = admin_cfg
        .bind
        .as_deref()
        .unwrap_or("127.0.0.1:8080")
        .parse::<SocketAddr>()
        .with_context(|| "invalid admin_api.bind")?;
    let auth_cfg = admin_cfg
        .auth
        .as_ref()
        .context("admin_api.auth is required when admin_api.enabled=true")?;
    let auth_verifier = Arc::new(AdminApiServiceJwtVerifier::from_config(auth_cfg)?);

    let state_store: Arc<dyn AdminStateBackend> =
        Arc::new(SyncStateStore::open_with_config(&cfg.state).await?);
    let stats_db = if let Some(stats_cfg) = &cfg.stats {
        Some(Arc::new(StatsDb::new(stats_cfg, &cfg.state.url).await?) as Arc<dyn AdminStatsBackend>)
    } else {
        None
    };
    let cdc_slot_sampler_cache = build_cdc_slot_sampler_cache(cfg);

    let state = AdminApiState {
        cfg: Arc::new(cfg.clone()),
        state_store,
        stats_db,
        cdc_slot_sampler_cache,
        auth_verifier,
        started_at: Utc::now(),
        mode: mode.to_string(),
        connection_id: connection_id.to_string(),
        managed_connection_count,
        config_hash: config_hash(cfg)?,
        deploy_revision: std::env::var("CDSYNC_DEPLOY_REVISION")
            .ok()
            .filter(|value| !value.trim().is_empty()),
        last_restart_reason: std::env::var("CDSYNC_LAST_RESTART_REASON")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "startup".to_string()),
    };

    let (handle, ready_rx) = spawn_admin_server_thread(state, bind, shutdown)?;
    ready_rx
        .await
        .context("admin API startup signal channel closed")??;
    Ok(Some(handle))
}

fn router(state: AdminApiState) -> Router {
    let protected = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state.clone());
    let v1 = Router::new()
        .route("/v1/status", get(status))
        .route("/v1/config", get(config))
        .route("/v1/stream", get(stream))
        .route("/v1/connections", get(connections))
        .route("/v1/connections/{id}", get(connection))
        .route("/v1/connections/{id}/runtime", get(connection_runtime))
        .route("/v1/connections/{id}/checkpoints", get(checkpoints))
        .route("/v1/connections/{id}/progress", get(progress))
        .route("/v1/connections/{id}/tables", get(connection_tables))
        .route("/v1/connections/{id}/runs", get(runs))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_api_auth,
        ))
        .with_state(state.clone());

    protected.merge(v1).with_state(state)
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn readyz(State(state): State<AdminApiState>) -> Result<Json<ReadyResponse>, AdminApiError> {
    state.state_store.ping().await?;
    if let Some(stats) = &state.stats_db {
        stats.ping().await?;
    }
    Ok(Json(ReadyResponse { ok: true }))
}

async fn status(State(state): State<AdminApiState>) -> Json<StatusResponse> {
    Json(StatusResponse {
        service: "cdsync",
        version: env!("CARGO_PKG_VERSION"),
        started_at: state.started_at,
        mode: state.mode,
        connection_id: state.connection_id,
        connection_count: state.managed_connection_count,
        config_hash: state.config_hash,
        deploy_revision: state.deploy_revision,
        last_restart_reason: state.last_restart_reason,
    })
}

async fn config(State(state): State<AdminApiState>) -> Json<ScrubbedConfig> {
    Json(scrub_config(&state.cfg))
}

async fn connections(
    State(state): State<AdminApiState>,
) -> Result<Json<Vec<ConnectionSummary>>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let now = Utc::now();
    let mut items = Vec::with_capacity(state.cfg.connections.len());
    for connection in &state.cfg.connections {
        let current = sync_state.connections.get(&connection.id).cloned();
        let cdc_runtime_state = cached_postgres_cdc_runtime_state(&state, connection);
        let current_ref = current.as_ref();
        let runtime = derive_connection_runtime(
            connection,
            current_ref,
            None,
            cdc_runtime_state,
            now,
            RuntimeMetadata {
                config_hash: &state.config_hash,
                deploy_revision: state.deploy_revision.as_deref(),
                last_restart_reason: &state.last_restart_reason,
            },
        );
        items.push(ConnectionSummary {
            id: connection.id.clone(),
            enabled: connection.enabled(),
            source_kind: source_kind(&connection.source),
            destination_kind: destination_kind(&connection.destination),
            last_sync_started_at: current_ref.and_then(|c| c.last_sync_started_at),
            last_sync_finished_at: current_ref.and_then(|c| c.last_sync_finished_at),
            last_sync_status: current_ref.and_then(|c| c.last_sync_status.clone()),
            last_error: current_ref.and_then(|c| c.last_error.clone()),
            phase: runtime.phase,
            reason_code: runtime.reason_code,
            max_checkpoint_age_seconds: runtime.max_checkpoint_age_seconds,
        });
    }
    Ok(Json(items))
}

async fn connection(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
) -> Result<Json<ConnectionDetail>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let config = state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;
    Ok(Json(ConnectionDetail {
        config: scrub_connection(config),
        state: sync_state.connections.get(&connection_id).cloned(),
    }))
}

async fn checkpoints(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
) -> Result<Json<CheckpointResponse>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    Ok(Json(CheckpointResponse {
        connection_id: connection_id.clone(),
        state: sync_state.connections.get(&connection_id).cloned(),
    }))
}

async fn runs(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Vec<RunSummary>>, AdminApiError> {
    let stats = state
        .stats_db
        .as_ref()
        .context("stats are disabled for this service")?;
    let limit = query.limit.unwrap_or(20).max(1);
    let runs = stats.recent_runs(Some(&connection_id), limit).await?;
    Ok(Json(runs))
}

async fn connection_runtime(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
) -> Result<Json<ConnectionRuntime>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let connection = state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;
    let cdc_runtime_state = cached_postgres_cdc_runtime_state(&state, connection);
    let runtime = derive_connection_runtime(
        connection,
        sync_state.connections.get(&connection_id),
        None,
        cdc_runtime_state,
        Utc::now(),
        RuntimeMetadata {
            config_hash: &state.config_hash,
            deploy_revision: state.deploy_revision.as_deref(),
            last_restart_reason: &state.last_restart_reason,
        },
    );
    Ok(Json(runtime))
}

async fn progress(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
) -> Result<Json<ProgressResponse>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let connection_state = sync_state.connections.get(&connection_id).cloned();
    let config = state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;
    let (current_run, run_tables) = if let Some(stats_db) = &state.stats_db {
        let runs = stats_db.recent_runs(Some(&connection_id), 1).await?;
        if let Some(run) = runs.into_iter().next() {
            let tables = stats_db.run_tables(&run.run_id).await?;
            (Some(run), tables)
        } else {
            (None, Vec::new())
        }
    } else {
        (None, Vec::new())
    };
    let now = Utc::now();
    let cdc_runtime_state = cached_postgres_cdc_runtime_state(&state, config);
    let runtime = derive_connection_runtime(
        config,
        connection_state.as_ref(),
        current_run.as_ref(),
        cdc_runtime_state,
        now,
        RuntimeMetadata {
            config_hash: &state.config_hash,
            deploy_revision: state.deploy_revision.as_deref(),
            last_restart_reason: &state.last_restart_reason,
        },
    );
    let tables = build_table_progress(
        config,
        connection_state.as_ref(),
        &run_tables,
        now,
        runtime.reason_code,
        cdc_runtime_state,
    );
    let cdc =
        ConnectionCdcSnapshot::from_cached(cached_postgres_cdc_slot_state(&state, config).as_ref());
    let batch_load_queue = if uses_cdc_batch_load_queue(config) {
        Some(
            state
                .state_store
                .load_cdc_batch_load_queue_summary(&connection_id)
                .await?,
        )
    } else {
        None
    };
    let cdc_coordinator = if uses_cdc_batch_load_queue(config) {
        Some(
            state
                .state_store
                .load_cdc_coordinator_summary(&connection_id, cdc.wal_bytes_behind_confirmed)
                .await?,
        )
    } else {
        None
    };

    Ok(Json(ProgressResponse {
        connection_id,
        state: connection_state,
        current_run,
        runtime,
        cdc,
        batch_load_queue,
        cdc_coordinator,
        tables,
    }))
}

async fn stream(
    State(state): State<AdminApiState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, AdminApiError> {
    state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == query.connection)
        .context("connection not found")?;

    let mut interval = tokio::time::interval(STREAM_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let stream = stream::unfold(
        StreamCursor {
            state,
            connection_id: query.connection,
            seq: 0,
            interval,
            pending_events: VecDeque::new(),
            previous_run_snapshot: None,
        },
        |mut cursor| async move {
            loop {
                if let Some(event) = cursor.pending_events.pop_front() {
                    return Some((Ok(event), cursor));
                }

                cursor.interval.tick().await;
                cursor.pending_events = match build_stream_events(&mut cursor).await {
                    Ok(events) => events,
                    Err(err) => {
                        let mut events = VecDeque::new();
                        if let Ok(event) = next_stream_event(
                            &mut cursor.seq,
                            &cursor.connection_id,
                            "stream.error",
                            StreamErrorData {
                                message: err.to_string(),
                            },
                        ) {
                            events.push_back(event);
                        }
                        events
                    }
                };
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(STREAM_KEEP_ALIVE_INTERVAL)
            .text("keep-alive"),
    ))
}

async fn connection_tables(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
) -> Result<Json<Vec<TableProgress>>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let connection_state = sync_state.connections.get(&connection_id).cloned();
    let config = state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;
    let run_tables = if let Some(stats_db) = &state.stats_db {
        let runs = stats_db.recent_runs(Some(&connection_id), 1).await?;
        if let Some(run) = runs.into_iter().next() {
            stats_db.run_tables(&run.run_id).await?
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let cdc_runtime_state = cached_postgres_cdc_runtime_state(&state, config);
    let runtime = derive_connection_runtime(
        config,
        connection_state.as_ref(),
        None,
        cdc_runtime_state,
        Utc::now(),
        RuntimeMetadata {
            config_hash: &state.config_hash,
            deploy_revision: state.deploy_revision.as_deref(),
            last_restart_reason: &state.last_restart_reason,
        },
    );
    Ok(Json(build_table_progress(
        config,
        connection_state.as_ref(),
        &run_tables,
        Utc::now(),
        runtime.reason_code,
        cdc_runtime_state,
    )))
}

async fn build_stream_events(cursor: &mut StreamCursor) -> anyhow::Result<VecDeque<SseEvent>> {
    let now = Utc::now();
    let connection = cursor
        .state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == cursor.connection_id)
        .context("connection not found")?;
    let sync_state = cursor.state.state_store.load_state().await?;
    let connection_state = sync_state.connections.get(&cursor.connection_id).cloned();
    let (current_run, run_tables, live_snapshot) =
        load_current_run_view(&cursor.state, &cursor.connection_id).await?;
    let cached_cdc_state = cached_postgres_cdc_slot_state(&cursor.state, connection);
    let cdc_probe = match cached_cdc_state.as_ref() {
        Some(slot_state) if slot_state.sampler_status == "ok" => Ok(slot_state.snapshot.clone()),
        Some(_) => Err(()),
        None => Ok(None),
    };
    let cdc_slot_snapshot = match &cdc_probe {
        Ok(snapshot) => snapshot.clone(),
        Err(()) => None,
    };
    let cdc_runtime_state = match &cdc_probe {
        Ok(snapshot) => postgres_cdc_runtime_state_from_snapshot(snapshot.as_ref()),
        Err(()) => Some(PostgresCdcRuntimeState::Unknown),
    };
    let runtime = derive_connection_runtime(
        connection,
        connection_state.as_ref(),
        current_run.as_ref(),
        cdc_runtime_state,
        now,
        RuntimeMetadata {
            config_hash: &cursor.state.config_hash,
            deploy_revision: cursor.state.deploy_revision.as_deref(),
            last_restart_reason: &cursor.state.last_restart_reason,
        },
    );
    let tables = build_table_progress(
        connection,
        connection_state.as_ref(),
        &run_tables,
        now,
        runtime.reason_code,
        cdc_runtime_state,
    );

    let mut events = VecDeque::new();
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "service.heartbeat",
        ServiceHeartbeatData {
            service: "cdsync",
            version: env!("CARGO_PKG_VERSION"),
            started_at: cursor.state.started_at,
            uptime_seconds: (now - cursor.state.started_at).num_seconds().max(0),
            mode: cursor.state.mode.clone(),
            deploy_revision: cursor.state.deploy_revision.clone(),
            config_hash: cursor.state.config_hash.clone(),
            last_restart_reason: cursor.state.last_restart_reason.clone(),
        },
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.runtime",
        &runtime,
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.throughput",
        build_connection_throughput_event(
            current_run.as_ref(),
            live_snapshot.as_ref(),
            &mut cursor.previous_run_snapshot,
            now,
        ),
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.cdc",
        ConnectionCdcData {
            sampler_status: cached_cdc_state
                .as_ref()
                .map(|state| state.sampler_status)
                .unwrap_or("disabled"),
            sampled_at: cached_cdc_state.as_ref().and_then(|state| state.sampled_at),
            slot_name: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.slot_name.clone()),
            slot_active: cdc_slot_snapshot.as_ref().map(|snapshot| snapshot.active),
            current_wal_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.current_wal_lsn.clone()),
            restart_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.restart_lsn.clone()),
            confirmed_flush_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.confirmed_flush_lsn.clone()),
            wal_bytes_retained_by_slot: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.wal_bytes_retained_by_slot),
            wal_bytes_behind_confirmed: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.wal_bytes_behind_confirmed),
        },
    )?);

    let active_tables = select_active_tables(&tables);
    if !active_tables.is_empty() {
        events.push_back(next_stream_event(
            &mut cursor.seq,
            &cursor.connection_id,
            "table.progress",
            &active_tables,
        )?);
    }

    let snapshot_tables = select_snapshot_tables(&tables);
    if !snapshot_tables.is_empty() {
        events.push_back(next_stream_event(
            &mut cursor.seq,
            &cursor.connection_id,
            "snapshot.progress",
            &snapshot_tables,
        )?);
    }

    Ok(events)
}

async fn load_current_run_view(
    state: &AdminApiState,
    connection_id: &str,
) -> anyhow::Result<(
    Option<RunSummary>,
    Vec<TableStatsSnapshot>,
    Option<RunStatsSnapshot>,
)> {
    if let Some(snapshot) = live_run_snapshot(connection_id) {
        return Ok((
            Some(summarize_run(&snapshot)),
            snapshot.tables.clone(),
            Some(snapshot),
        ));
    }

    if let Some(stats_db) = &state.stats_db {
        let runs = stats_db.recent_runs(Some(connection_id), 1).await?;
        if let Some(run) = runs.into_iter().next() {
            let tables = stats_db.run_tables(&run.run_id).await?;
            return Ok((Some(run), tables, None));
        }
    }

    Ok((None, Vec::new(), None))
}

fn build_connection_throughput_event(
    current_run: Option<&RunSummary>,
    live_snapshot: Option<&RunStatsSnapshot>,
    previous_run_snapshot: &mut Option<(DateTime<Utc>, RunStatsSnapshot)>,
    now: DateTime<Utc>,
) -> ConnectionThroughputData {
    let mut event = ConnectionThroughputData {
        run_id: current_run.map(|run| run.run_id.clone()),
        status: current_run.and_then(|run| run.status.clone()),
        rows_read_total: current_run.map(|run| run.rows_read).unwrap_or_default(),
        rows_written_total: current_run.map(|run| run.rows_written).unwrap_or_default(),
        rows_deleted_total: current_run.map(|run| run.rows_deleted).unwrap_or_default(),
        rows_upserted_total: current_run.map(|run| run.rows_upserted).unwrap_or_default(),
        extract_ms_total: current_run.map(|run| run.extract_ms).unwrap_or_default(),
        load_ms_total: current_run.map(|run| run.load_ms).unwrap_or_default(),
        rows_read_per_sec: None,
        rows_written_per_sec: None,
        rows_deleted_per_sec: None,
        rows_upserted_per_sec: None,
    };

    let Some(live_snapshot) = live_snapshot else {
        return event;
    };

    if let Some((previous_at, previous_snapshot)) = previous_run_snapshot.as_ref()
        && previous_snapshot.run_id == live_snapshot.run_id
    {
        let elapsed_ms = (now - *previous_at).num_milliseconds().max(1) as f64;
        let elapsed_seconds = elapsed_ms / 1000.0;
        event.rows_read_per_sec = Some(
            ((live_snapshot.rows_read - previous_snapshot.rows_read).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_written_per_sec = Some(
            ((live_snapshot.rows_written - previous_snapshot.rows_written).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_deleted_per_sec = Some(
            ((live_snapshot.rows_deleted - previous_snapshot.rows_deleted).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_upserted_per_sec = Some(
            ((live_snapshot.rows_upserted - previous_snapshot.rows_upserted).max(0) as f64)
                / elapsed_seconds,
        );
    }

    *previous_run_snapshot = Some((now, live_snapshot.clone()));
    event
}

fn select_active_tables(tables: &[TableProgress]) -> Vec<TableProgress> {
    tables
        .iter()
        .filter(|table| {
            matches!(table.phase, "error" | "blocked")
                || table.snapshot_chunks_total > 0
                || table.stats.as_ref().is_some_and(|stats| {
                    stats.rows_read > 0
                        || stats.rows_written > 0
                        || stats.rows_deleted > 0
                        || stats.rows_upserted > 0
                })
        })
        .take(STREAM_ACTIVE_TABLE_LIMIT)
        .cloned()
        .collect()
}

fn select_snapshot_tables(tables: &[TableProgress]) -> Vec<TableProgress> {
    tables
        .iter()
        .filter(|table| {
            table.snapshot_chunks_total > 0
                && table.snapshot_chunks_complete < table.snapshot_chunks_total
        })
        .take(STREAM_ACTIVE_TABLE_LIMIT)
        .cloned()
        .collect()
}

fn next_stream_event<T: Serialize>(
    seq: &mut u64,
    connection_id: &str,
    event_type: &'static str,
    data: T,
) -> anyhow::Result<SseEvent> {
    *seq += 1;
    Ok(SseEvent::default()
        .event(event_type)
        .json_data(StreamEnvelope {
            event_type,
            connection_id: connection_id.to_string(),
            seq: *seq,
            at: Utc::now(),
            data,
        })?)
}

fn active_checkpoint_map<'a>(
    state: &'a ConnectionState,
    source: &SourceConfig,
) -> &'a std::collections::HashMap<String, TableCheckpoint> {
    match source {
        SourceConfig::Postgres(_) => &state.postgres,
        SourceConfig::Salesforce(_) => &state.salesforce,
    }
}

fn configured_entity_names(connection: &ConnectionConfig) -> std::collections::BTreeSet<String> {
    match &connection.source {
        SourceConfig::Postgres(pg) => pg
            .tables
            .as_ref()
            .map(|tables| tables.iter().map(|table| table.name.clone()).collect())
            .unwrap_or_default(),
        SourceConfig::Salesforce(sf) => sf
            .objects
            .as_ref()
            .map(|objects| objects.iter().map(|object| object.name.clone()).collect())
            .unwrap_or_default(),
    }
}

fn checkpoint_age_seconds(checkpoint: Option<&TableCheckpoint>, now: DateTime<Utc>) -> Option<i64> {
    checkpoint
        .and_then(|checkpoint| checkpoint.last_synced_at)
        .map(|last_synced_at| (now - last_synced_at).num_seconds().max(0))
}

fn max_checkpoint_age_seconds(
    state: Option<&ConnectionState>,
    connection: &ConnectionConfig,
    now: DateTime<Utc>,
) -> Option<i64> {
    state.and_then(|state| {
        let configured = configured_entity_names(connection);
        active_checkpoint_map(state, &connection.source)
            .iter()
            .filter(|(entity_name, _)| configured.is_empty() || configured.contains(*entity_name))
            .map(|(_, checkpoint)| checkpoint)
            .filter_map(|checkpoint| checkpoint_age_seconds(Some(checkpoint), now))
            .max()
    })
}

fn classify_error_reason(error: Option<&str>) -> &'static str {
    let Some(error) = error else {
        return "healthy";
    };
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("schema change") || normalized.contains("incompatible schema") {
        "schema_blocked"
    } else if normalized.contains("publication") {
        "publication_blocked"
    } else if normalized.contains("already locked") {
        "lock_contention"
    } else {
        "last_run_failed"
    }
}

async fn load_postgres_cdc_slot_snapshot(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
) -> Result<Option<PostgresCdcSlotSnapshot>, ()> {
    let SourceConfig::Postgres(pg) = &connection.source else {
        return Ok(None);
    };
    if !pg.cdc.unwrap_or(true) {
        return Ok(None);
    }

    let slot_name = state
        .and_then(|state| state.postgres_cdc.as_ref())
        .and_then(|cdc_state| cdc_state.slot_name.clone())
        .or_else(|| {
            crate::sources::postgres::admin_cdc_slot_name(&connection.id, pg.cdc_pipeline_id).ok()
        });
    let has_last_lsn = state
        .and_then(|state| state.postgres_cdc.as_ref())
        .and_then(|cdc_state| cdc_state.last_lsn.as_ref())
        .is_some();

    let Some(slot_name) = slot_name else {
        return Ok(Some(PostgresCdcSlotSnapshot {
            slot_name: None,
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            current_wal_lsn: None,
            wal_bytes_retained_by_slot: None,
            wal_bytes_behind_confirmed: None,
            continuity_lost: false,
        }));
    };

    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(CDC_SLOT_INSPECTION_TIMEOUT)
        .connect_lazy(&pg.url)
    {
        Ok(pool) => pool,
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to inspect postgres CDC slot snapshot"
            );
            return Err(());
        }
    };

    let row = match timeout(
        CDC_SLOT_INSPECTION_TIMEOUT,
        sqlx::query(
            r#"
            select active,
                   restart_lsn::text as restart_lsn,
                   confirmed_flush_lsn::text as confirmed_flush_lsn,
                   pg_current_wal_lsn()::text as current_wal_lsn,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint as retained_bytes,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint as behind_confirmed_bytes
            from pg_replication_slots
            where slot_type = 'logical'
              and slot_name = $1
            "#,
        )
        .bind(&slot_name)
        .fetch_optional(&pool),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(err)) => {
            warn!(
                connection = %connection.id,
                slot_name = %slot_name,
                error = %err,
                "failed to query postgres CDC slot snapshot"
            );
            return Err(());
        }
        Err(_) => {
            warn!(
                connection = %connection.id,
                slot_name = %slot_name,
                "timed out while inspecting postgres CDC slot snapshot"
            );
            return Err(());
        }
    };

    Ok(match row {
        Some(row) => Some(PostgresCdcSlotSnapshot {
            slot_name: Some(slot_name),
            active: row.try_get("active").unwrap_or(false),
            restart_lsn: row.try_get("restart_lsn").ok(),
            confirmed_flush_lsn: row.try_get("confirmed_flush_lsn").ok(),
            current_wal_lsn: row.try_get("current_wal_lsn").ok(),
            wal_bytes_retained_by_slot: row.try_get("retained_bytes").ok(),
            wal_bytes_behind_confirmed: row.try_get("behind_confirmed_bytes").ok(),
            continuity_lost: false,
        }),
        None => Some(PostgresCdcSlotSnapshot {
            slot_name: Some(slot_name),
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            current_wal_lsn: None,
            wal_bytes_retained_by_slot: None,
            wal_bytes_behind_confirmed: None,
            continuity_lost: has_last_lsn,
        }),
    })
}

fn postgres_cdc_runtime_state_from_snapshot(
    snapshot: Option<&PostgresCdcSlotSnapshot>,
) -> Option<PostgresCdcRuntimeState> {
    let snapshot = snapshot?;
    if snapshot.active {
        Some(PostgresCdcRuntimeState::Following)
    } else if snapshot.continuity_lost {
        Some(PostgresCdcRuntimeState::ContinuityLost)
    } else {
        Some(PostgresCdcRuntimeState::Initializing)
    }
}

fn checkpoint_has_incomplete_snapshot(checkpoint: Option<&TableCheckpoint>) -> bool {
    checkpoint.is_some_and(|checkpoint| {
        !checkpoint.snapshot_chunks.is_empty()
            && checkpoint
                .snapshot_chunks
                .iter()
                .any(|chunk| !chunk.complete)
    })
}

fn derive_connection_runtime(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
    _current_run: Option<&RunSummary>,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
    now: DateTime<Utc>,
    metadata: RuntimeMetadata<'_>,
) -> ConnectionRuntime {
    let (phase, reason_code) = match state.and_then(|state| state.last_sync_status.as_deref()) {
        Some("running")
            if state.is_some_and(|state| {
                active_checkpoint_map(state, &connection.source)
                    .values()
                    .any(|checkpoint| checkpoint_has_incomplete_snapshot(Some(checkpoint)))
            }) =>
        {
            ("snapshotting", "snapshot_in_progress")
        }
        Some("running") => match cdc_runtime_state {
            Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
            Some(PostgresCdcRuntimeState::ContinuityLost) => ("blocked", "cdc_continuity_lost"),
            Some(PostgresCdcRuntimeState::Unknown) => ("starting", "cdc_state_unknown"),
            Some(PostgresCdcRuntimeState::Initializing) => ("starting", "cdc_initializing"),
            None => ("syncing", "sync_in_progress"),
        },
        Some("failed") => {
            let reason = classify_error_reason(state.and_then(|state| state.last_error.as_deref()));
            if matches!(reason, "schema_blocked" | "publication_blocked") {
                ("blocked", reason)
            } else {
                ("error", reason)
            }
        }
        Some("success") => ("healthy", "healthy"),
        _ => ("idle", "never_synced"),
    };

    ConnectionRuntime {
        connection_id: connection.id.clone(),
        phase,
        reason_code,
        last_sync_started_at: state.and_then(|state| state.last_sync_started_at),
        last_sync_finished_at: state.and_then(|state| state.last_sync_finished_at),
        last_sync_status: state.and_then(|state| state.last_sync_status.clone()),
        last_error: state.and_then(|state| state.last_error.clone()),
        max_checkpoint_age_seconds: max_checkpoint_age_seconds(state, connection, now),
        config_hash: metadata.config_hash.to_string(),
        deploy_revision: metadata.deploy_revision.map(ToOwned::to_owned),
        last_restart_reason: metadata.last_restart_reason.to_string(),
    }
}

fn build_table_progress(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
    run_tables: &[TableStatsSnapshot],
    now: DateTime<Utc>,
    connection_reason_code: &'static str,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
) -> Vec<TableProgress> {
    let mut table_names = configured_entity_names(connection);
    if let Some(state) = state {
        table_names.extend(
            active_checkpoint_map(state, &connection.source)
                .keys()
                .cloned(),
        );
    }
    table_names.extend(run_tables.iter().map(|table| table.table_name.clone()));

    let run_table_map: std::collections::HashMap<_, _> = run_tables
        .iter()
        .cloned()
        .map(|table| (table.table_name.clone(), table))
        .collect();

    table_names
        .into_iter()
        .map(|table_name| {
            let checkpoint = state.and_then(|state| {
                active_checkpoint_map(state, &connection.source)
                    .get(&table_name)
                    .cloned()
            });
            let stats = run_table_map.get(&table_name).cloned();
            let checkpoint_age_seconds = checkpoint_age_seconds(checkpoint.as_ref(), now);
            let snapshot_chunks_total = checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.snapshot_chunks.len())
                .unwrap_or(0);
            let snapshot_chunks_complete = checkpoint
                .as_ref()
                .map(|checkpoint| {
                    checkpoint
                        .snapshot_chunks
                        .iter()
                        .filter(|chunk| chunk.complete)
                        .count()
                })
                .unwrap_or(0);
            let (phase, reason_code) = match state
                .and_then(|state| state.last_sync_status.as_deref())
            {
                Some("failed")
                    if matches!(
                        connection_reason_code,
                        "schema_blocked" | "publication_blocked"
                    ) =>
                {
                    ("blocked", connection_reason_code)
                }
                Some("failed") => ("error", connection_reason_code),
                Some("running") if checkpoint_has_incomplete_snapshot(checkpoint.as_ref()) => {
                    ("snapshotting", "snapshot_in_progress")
                }
                Some("running") => match cdc_runtime_state {
                    Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
                    Some(PostgresCdcRuntimeState::ContinuityLost) => {
                        ("blocked", "cdc_continuity_lost")
                    }
                    Some(PostgresCdcRuntimeState::Unknown) => ("pending", "cdc_state_unknown"),
                    Some(PostgresCdcRuntimeState::Initializing) => ("pending", "cdc_initializing"),
                    None => ("syncing", "sync_in_progress"),
                },
                Some("success") if checkpoint.is_some() => ("healthy", "healthy"),
                _ => ("pending", "never_synced"),
            };

            TableProgress {
                table_name,
                checkpoint,
                stats,
                phase,
                reason_code,
                checkpoint_age_seconds,
                lag_seconds: checkpoint_age_seconds,
                snapshot_chunks_total,
                snapshot_chunks_complete,
            }
        })
        .collect()
}

fn scrub_config(cfg: &Config) -> ScrubbedConfig {
    ScrubbedConfig {
        state: ScrubbedStateConfig {
            url: scrub_url(&cfg.state.url),
            schema: cfg.state.schema.clone(),
        },
        metadata: cfg.metadata.clone(),
        logging: cfg.logging.clone(),
        admin_api: cfg.admin_api.as_ref().map(scrub_admin_api_config),
        observability: cfg.observability.as_ref().map(scrub_observability_config),
        sync: cfg.sync.clone(),
        stats: cfg.stats.as_ref().map(|stats| ScrubbedStatsConfig {
            url: stats.url.as_deref().map(scrub_url),
            schema: stats.schema.clone(),
        }),
        connections: cfg.connections.iter().map(scrub_connection).collect(),
    }
}

fn scrub_admin_api_config(admin_api: &AdminApiConfig) -> ScrubbedAdminApiConfig {
    ScrubbedAdminApiConfig {
        enabled: admin_api.enabled,
        bind: admin_api.bind.clone(),
        auth: admin_api
            .auth
            .as_ref()
            .map(|auth| ScrubbedAdminApiAuthConfig {
                service_jwt_allowed_issuers: auth.resolved_allowed_issuers(),
                service_jwt_allowed_audiences: auth.resolved_allowed_audiences(),
                required_scopes: auth.resolved_required_scopes(),
            }),
    }
}

fn scrub_observability_config(obs: &ObservabilityConfig) -> ScrubbedObservabilityConfig {
    ScrubbedObservabilityConfig {
        service_name: obs.service_name.clone(),
        otlp_traces_endpoint: obs.otlp_traces_endpoint.clone(),
        otlp_metrics_endpoint: obs.otlp_metrics_endpoint.clone(),
        otlp_headers: obs.otlp_headers.as_ref().map(|headers| {
            headers
                .keys()
                .map(|key| (key.clone(), "***".to_string()))
                .collect()
        }),
        metrics_interval_seconds: obs.metrics_interval_seconds,
    }
}

fn scrub_connection(connection: &ConnectionConfig) -> ScrubbedConnectionConfig {
    ScrubbedConnectionConfig {
        id: connection.id.clone(),
        enabled: connection.enabled,
        source: match &connection.source {
            SourceConfig::Postgres(pg) => ScrubbedSourceConfig::Postgres(scrub_postgres_config(pg)),
            SourceConfig::Salesforce(sf) => {
                ScrubbedSourceConfig::Salesforce(scrub_salesforce_config(sf))
            }
        },
        destination: match &connection.destination {
            DestinationConfig::BigQuery(bq) => {
                ScrubbedDestinationConfig::BigQuery(scrub_bigquery_config(bq))
            }
        },
        schedule: connection.schedule.clone(),
    }
}

fn scrub_postgres_config(pg: &PostgresConfig) -> ScrubbedPostgresConfig {
    ScrubbedPostgresConfig {
        url: scrub_url(&pg.url),
        tables: pg.tables.clone(),
        table_selection: pg.table_selection.clone(),
        batch_size: pg.batch_size,
        cdc: pg.cdc,
        publication: pg.publication.clone(),
        publication_mode: pg.publication_mode.clone(),
        schema_changes: pg.schema_changes.clone(),
        cdc_pipeline_id: pg.cdc_pipeline_id,
        cdc_batch_size: pg.cdc_batch_size,
        cdc_apply_concurrency: pg.cdc_apply_concurrency,
        cdc_max_fill_ms: pg.cdc_max_fill_ms,
        cdc_max_pending_events: pg.cdc_max_pending_events,
        cdc_idle_timeout_seconds: pg.cdc_idle_timeout_seconds,
        cdc_tls: pg.cdc_tls,
        cdc_tls_ca_path: pg.cdc_tls_ca_path.clone(),
        cdc_tls_ca: pg.cdc_tls_ca.as_ref().map(|_| "***".to_string()),
    }
}

fn scrub_salesforce_config(sf: &SalesforceConfig) -> ScrubbedSalesforceConfig {
    ScrubbedSalesforceConfig {
        login_url: sf.login_url.clone(),
        instance_url: sf.instance_url.clone(),
        client_id: sf.client_id.clone(),
        objects: sf.objects.clone(),
        object_selection: sf.object_selection.clone(),
        polling_interval_seconds: sf.polling_interval_seconds,
        api_version: sf.api_version.clone(),
    }
}

fn scrub_bigquery_config(bq: &BigQueryConfig) -> ScrubbedBigQueryConfig {
    ScrubbedBigQueryConfig {
        project_id: bq.project_id.clone(),
        dataset: bq.dataset.clone(),
        location: bq.location.clone(),
        service_account_key_path: bq.service_account_key_path.clone(),
        partition_by_synced_at: bq.partition_by_synced_at,
        storage_write_enabled: bq.storage_write_enabled,
        batch_load_bucket: bq.batch_load_bucket.clone(),
        batch_load_prefix: bq.batch_load_prefix.clone(),
        emulator_http: bq.emulator_http.clone(),
        emulator_grpc: bq.emulator_grpc.clone(),
    }
}

fn source_kind(source: &crate::config::SourceConfig) -> &'static str {
    match source {
        crate::config::SourceConfig::Postgres(_) => "postgres",
        crate::config::SourceConfig::Salesforce(_) => "salesforce",
    }
}

fn destination_kind(destination: &crate::config::DestinationConfig) -> &'static str {
    match destination {
        crate::config::DestinationConfig::BigQuery(_) => "bigquery",
    }
}

fn scrub_url(raw: &str) -> String {
    if let Ok(mut url) = Url::parse(raw) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        return url.to_string();
    }
    raw.to_string()
}

fn canonicalize_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = std::mem::take(object).into_iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (_, child) in &mut entries {
                canonicalize_json_value(child);
            }
            object.extend(entries);
        }
        serde_json::Value::Array(values) => {
            for child in values {
                canonicalize_json_value(child);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
        DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
        PostgresTableConfig, SourceConfig, StateConfig, StatsConfig, SyncConfig,
    };
    use crate::types::TableCheckpoint;
    use chrono::TimeZone;
    use std::collections::HashMap;

    fn test_hash_config(
        otlp_headers: HashMap<String, String>,
        public_keys: HashMap<String, String>,
    ) -> Config {
        Config {
            connections: vec![ConnectionConfig {
                id: "app".to_string(),
                enabled: Some(true),
                source: SourceConfig::Postgres(PostgresConfig {
                    url: "postgres://postgres:secret@example.com:5432/app".to_string(),
                    tables: Some(vec![PostgresTableConfig {
                        name: "public.accounts".to_string(),
                        primary_key: Some("id".to_string()),
                        updated_at_column: Some("updated_at".to_string()),
                        soft_delete: Some(false),
                        soft_delete_column: None,
                        where_clause: None,
                        columns: None,
                    }]),
                    table_selection: None,
                    batch_size: Some(1000),
                    cdc: Some(true),
                    publication: Some("cdsync_pub".to_string()),
                    publication_mode: None,
                    schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                    cdc_pipeline_id: Some(1),
                    cdc_batch_size: Some(1000),
                    cdc_apply_concurrency: Some(8),
                    cdc_max_fill_ms: Some(2000),
                    cdc_max_pending_events: Some(100_000),
                    cdc_idle_timeout_seconds: Some(10),
                    cdc_tls: Some(false),
                    cdc_tls_ca_path: None,
                    cdc_tls_ca: None,
                }),
                destination: DestinationConfig::BigQuery(BigQueryConfig {
                    project_id: "proj".to_string(),
                    dataset: "dataset".to_string(),
                    location: Some("US".to_string()),
                    service_account_key_path: None,
                    service_account_key: None,
                    partition_by_synced_at: Some(true),
                    storage_write_enabled: Some(false),
                    batch_load_bucket: None,
                    batch_load_prefix: None,
                    emulator_http: Some("http://localhost:9050".to_string()),
                    emulator_grpc: Some("localhost:9051".to_string()),
                }),
                schedule: None,
            }],
            state: StateConfig {
                url: "postgres://postgres:secret@example.com:5432/state".to_string(),
                schema: Some("cdsync_state".to_string()),
            },
            metadata: Some(MetadataConfig {
                synced_at_column: Some("_synced".to_string()),
                deleted_at_column: Some("_deleted".to_string()),
            }),
            logging: Some(LoggingConfig {
                level: Some("info".to_string()),
                json: Some(false),
            }),
            admin_api: Some(AdminApiConfig {
                enabled: Some(true),
                bind: Some("127.0.0.1:8080".to_string()),
                auth: Some(AdminApiAuthConfig {
                    service_jwt_public_keys: public_keys,
                    service_jwt_public_keys_json: None,
                    service_jwt_allowed_issuers: vec!["caller-service".to_string()],
                    service_jwt_allowed_audiences: vec!["cdsync".to_string()],
                    required_scopes: vec!["cdsync:admin".to_string()],
                }),
            }),
            observability: Some(ObservabilityConfig {
                service_name: Some("cdsync".to_string()),
                otlp_traces_endpoint: Some("https://trace.example".to_string()),
                otlp_metrics_endpoint: Some("https://metrics.example".to_string()),
                otlp_headers: Some(otlp_headers),
                metrics_interval_seconds: Some(30),
            }),
            sync: Some(SyncConfig {
                default_batch_size: Some(1000),
                max_retries: Some(5),
                retry_backoff_ms: Some(1000),
                max_concurrency: Some(4),
            }),
            stats: Some(StatsConfig {
                url: Some("postgres://postgres:secret@example.com:5432/stats".to_string()),
                schema: Some("cdsync_stats".to_string()),
            }),
        }
    }

    #[test]
    fn scrub_observability_config_redacts_header_values() {
        let obs = ObservabilityConfig {
            service_name: Some("svc".to_string()),
            otlp_traces_endpoint: Some("https://trace.example".to_string()),
            otlp_metrics_endpoint: Some("https://metrics.example".to_string()),
            otlp_headers: Some(
                [("authorization".to_string(), "Bearer secret".to_string())]
                    .into_iter()
                    .collect(),
            ),
            metrics_interval_seconds: Some(30),
        };

        let scrubbed = scrub_observability_config(&obs);
        assert_eq!(
            scrubbed
                .otlp_headers
                .as_ref()
                .and_then(|headers| headers.get("authorization"))
                .map(String::as_str),
            Some("***")
        );
    }

    #[test]
    fn active_checkpoint_map_uses_salesforce_state_for_salesforce_connections() {
        let mut state = ConnectionState::default();
        state.salesforce.insert(
            "Account".to_string(),
            TableCheckpoint {
                last_primary_key: Some("001".to_string()),
                ..Default::default()
            },
        );
        let source = SourceConfig::Salesforce(crate::config::SalesforceConfig {
            client_id: "id".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: "refresh".to_string(),
            login_url: None,
            instance_url: None,
            api_version: None,
            objects: Some(vec![crate::config::SalesforceObjectConfig {
                name: "Account".to_string(),
                primary_key: Some("Id".to_string()),
                fields: None,
                soft_delete: Some(true),
            }]),
            object_selection: None,
            polling_interval_seconds: None,
            rate_limit: None,
        });

        let checkpoints = active_checkpoint_map(&state, &source);
        assert_eq!(
            checkpoints
                .get("Account")
                .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
            Some("001")
        );
    }

    #[test]
    fn config_hash_is_stable_for_map_backed_config() {
        let config_a = test_hash_config(
            HashMap::from([
                ("authorization".to_string(), "Bearer secret".to_string()),
                ("x-api-key".to_string(), "key-a".to_string()),
            ]),
            HashMap::from([
                ("caller-service-a".to_string(), "pem-a".to_string()),
                ("caller-service-b".to_string(), "pem-b".to_string()),
            ]),
        );
        let config_b = test_hash_config(
            HashMap::from([
                ("x-api-key".to_string(), "key-a".to_string()),
                ("authorization".to_string(), "Bearer secret".to_string()),
            ]),
            HashMap::from([
                ("caller-service-b".to_string(), "pem-b".to_string()),
                ("caller-service-a".to_string(), "pem-a".to_string()),
            ]),
        );

        assert_eq!(
            config_hash(&config_a).expect("hash a"),
            config_hash(&config_b).expect("hash b")
        );
    }

    #[test]
    fn max_checkpoint_age_seconds_ignores_removed_config_entities() {
        let connection = ConnectionConfig {
            id: "app".to_string(),
            enabled: Some(true),
            source: SourceConfig::Postgres(PostgresConfig {
                url: "postgres://postgres:secret@example.com:5432/app".to_string(),
                tables: Some(vec![PostgresTableConfig {
                    name: "public.accounts".to_string(),
                    primary_key: Some("id".to_string()),
                    updated_at_column: Some("updated_at".to_string()),
                    soft_delete: Some(false),
                    soft_delete_column: None,
                    where_clause: None,
                    columns: None,
                }]),
                table_selection: None,
                batch_size: Some(1000),
                cdc: Some(false),
                publication: None,
                publication_mode: None,
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: None,
                cdc_batch_size: None,
                cdc_apply_concurrency: None,
                cdc_max_fill_ms: None,
                cdc_max_pending_events: None,
                cdc_idle_timeout_seconds: None,
                cdc_tls: None,
                cdc_tls_ca_path: None,
                cdc_tls_ca: None,
            }),
            destination: DestinationConfig::BigQuery(BigQueryConfig {
                project_id: "proj".to_string(),
                dataset: "dataset".to_string(),
                location: Some("US".to_string()),
                service_account_key_path: None,
                service_account_key: None,
                partition_by_synced_at: Some(true),
                storage_write_enabled: Some(false),
                batch_load_bucket: None,
                batch_load_prefix: None,
                emulator_http: Some("http://localhost:9050".to_string()),
                emulator_grpc: Some("localhost:9051".to_string()),
            }),
            schedule: None,
        };
        let now = Utc.with_ymd_and_hms(2026, 4, 1, 12, 0, 0).unwrap();
        let mut state = ConnectionState::default();
        state.postgres.insert(
            "public.accounts".to_string(),
            TableCheckpoint {
                last_synced_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 11, 59, 0).unwrap()),
                ..Default::default()
            },
        );
        state.postgres.insert(
            "public.removed_table".to_string(),
            TableCheckpoint {
                last_synced_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 8, 0, 0).unwrap()),
                ..Default::default()
            },
        );

        assert_eq!(
            max_checkpoint_age_seconds(Some(&state), &connection, now),
            Some(60)
        );
    }

    fn test_postgres_cdc_connection() -> ConnectionConfig {
        ConnectionConfig {
            id: "app_staging".to_string(),
            enabled: Some(true),
            source: SourceConfig::Postgres(PostgresConfig {
                url: "postgres://postgres:secret@example.com:5432/app".to_string(),
                tables: Some(vec![PostgresTableConfig {
                    name: "public.accounts".to_string(),
                    primary_key: Some("id".to_string()),
                    updated_at_column: Some("updated_at".to_string()),
                    soft_delete: Some(false),
                    soft_delete_column: None,
                    where_clause: None,
                    columns: None,
                }]),
                table_selection: None,
                batch_size: Some(1000),
                cdc: Some(true),
                publication: Some("cdsync_pub".to_string()),
                publication_mode: None,
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: Some(1101),
                cdc_batch_size: Some(1000),
                cdc_apply_concurrency: Some(8),
                cdc_max_fill_ms: Some(2000),
                cdc_max_pending_events: Some(100_000),
                cdc_idle_timeout_seconds: Some(10),
                cdc_tls: Some(false),
                cdc_tls_ca_path: None,
                cdc_tls_ca: None,
            }),
            destination: DestinationConfig::BigQuery(BigQueryConfig {
                project_id: "proj".to_string(),
                dataset: "dataset".to_string(),
                location: Some("US".to_string()),
                service_account_key_path: None,
                service_account_key: None,
                partition_by_synced_at: Some(true),
                storage_write_enabled: Some(false),
                batch_load_bucket: None,
                batch_load_prefix: None,
                emulator_http: Some("http://localhost:9050".to_string()),
                emulator_grpc: Some("localhost:9051".to_string()),
            }),
            schedule: None,
        }
    }

    #[test]
    fn derive_connection_runtime_requires_real_cdc_follow_state() {
        let connection = test_postgres_cdc_connection();
        let state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };

        let runtime = derive_connection_runtime(
            &connection,
            Some(&state),
            None,
            Some(PostgresCdcRuntimeState::Initializing),
            Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
            RuntimeMetadata {
                config_hash: "hash",
                deploy_revision: Some("deploy"),
                last_restart_reason: "startup",
            },
        );

        assert_eq!(runtime.phase, "starting");
        assert_eq!(runtime.reason_code, "cdc_initializing");
    }

    #[test]
    fn build_table_progress_requires_real_cdc_follow_state() {
        let connection = test_postgres_cdc_connection();
        let state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };

        let tables = build_table_progress(
            &connection,
            Some(&state),
            &[],
            Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
            "cdc_initializing",
            Some(PostgresCdcRuntimeState::Initializing),
        );

        assert!(!tables.is_empty());
        assert!(
            tables
                .iter()
                .all(|table| table.reason_code == "cdc_initializing")
        );
    }

    #[test]
    fn derive_connection_runtime_surfaces_unknown_cdc_probe_state() {
        let connection = test_postgres_cdc_connection();
        let state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };

        let runtime = derive_connection_runtime(
            &connection,
            Some(&state),
            None,
            Some(PostgresCdcRuntimeState::Unknown),
            Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
            RuntimeMetadata {
                config_hash: "hash",
                deploy_revision: Some("deploy"),
                last_restart_reason: "startup",
            },
        );

        assert_eq!(runtime.phase, "starting");
        assert_eq!(runtime.reason_code, "cdc_state_unknown");
    }

    #[test]
    fn select_active_tables_prefers_busy_snapshot_or_blocked_tables() {
        let idle = TableProgress {
            table_name: "public.a_idle".to_string(),
            checkpoint: None,
            stats: None,
            phase: "running",
            reason_code: "cdc_following",
            checkpoint_age_seconds: None,
            lag_seconds: None,
            snapshot_chunks_total: 0,
            snapshot_chunks_complete: 0,
        };
        let busy = TableProgress {
            table_name: "public.z_busy".to_string(),
            checkpoint: None,
            stats: Some(TableStatsSnapshot {
                run_id: "run-1".to_string(),
                connection_id: "app".to_string(),
                table_name: "public.z_busy".to_string(),
                rows_read: 1,
                rows_written: 1,
                rows_deleted: 0,
                rows_upserted: 1,
                extract_ms: 1,
                load_ms: 1,
            }),
            phase: "running",
            reason_code: "cdc_following",
            checkpoint_age_seconds: None,
            lag_seconds: None,
            snapshot_chunks_total: 0,
            snapshot_chunks_complete: 0,
        };
        let blocked = TableProgress {
            table_name: "public.m_blocked".to_string(),
            checkpoint: None,
            stats: None,
            phase: "blocked",
            reason_code: "schema_blocked",
            checkpoint_age_seconds: None,
            lag_seconds: None,
            snapshot_chunks_total: 0,
            snapshot_chunks_complete: 0,
        };
        let snapshotting = TableProgress {
            table_name: "public.n_snapshot".to_string(),
            checkpoint: None,
            stats: None,
            phase: "snapshotting",
            reason_code: "snapshot_in_progress",
            checkpoint_age_seconds: None,
            lag_seconds: None,
            snapshot_chunks_total: 4,
            snapshot_chunks_complete: 2,
        };

        let selected =
            select_active_tables(&[idle, busy.clone(), blocked.clone(), snapshotting.clone()]);
        let names: Vec<_> = selected
            .iter()
            .map(|table| table.table_name.as_str())
            .collect();

        assert_eq!(
            names,
            vec!["public.z_busy", "public.m_blocked", "public.n_snapshot"]
        );
    }
}
