#[cfg(test)]
mod route_tests;

use crate::config::{
    AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
    DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
    SalesforceConfig, SourceConfig, SyncConfig,
};
use crate::runner::ShutdownSignal;
use crate::state::{ConnectionState, SyncState, SyncStateStore};
use crate::stats::{RunSummary, StatsDb, TableStatsSnapshot};
use crate::types::TableCheckpoint;
use anyhow::Context;
use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::warn;
use url::Url;

#[derive(Clone)]
pub struct AdminApiState {
    cfg: Arc<Config>,
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
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

#[derive(Serialize)]
struct ProgressResponse {
    connection_id: String,
    state: Option<ConnectionState>,
    current_run: Option<RunSummary>,
    runtime: ConnectionRuntime,
    tables: Vec<TableProgress>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
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

const CDC_SLOT_INSPECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

fn config_hash(cfg: &Config) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(cfg).context("serializing config for hashing")?;
    canonicalize_json_value(&mut value);
    let bytes = serde_json::to_vec(&value).context("serializing canonical config for hashing")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub async fn spawn_admin_api(
    cfg: &Config,
    connection_id: &str,
    managed_connection_count: usize,
    mode: &str,
    shutdown: ShutdownSignal,
) -> anyhow::Result<Option<JoinHandle<anyhow::Result<()>>>> {
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

    let state = AdminApiState {
        cfg: Arc::new(cfg.clone()),
        state_store,
        stats_db,
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

    let app = router(state);

    let listener = TcpListener::bind(bind).await?;
    let mut shutdown = shutdown;
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.changed().await;
            })
            .await
            .map_err(anyhow::Error::from)
    });

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
    for (connection, current, cdc_runtime_state) in
        probe_connection_runtime_states(&state.cfg.connections, &sync_state).await
    {
        let current = current.as_ref();
        let runtime = derive_connection_runtime(
            &connection,
            current,
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
            last_sync_started_at: current.and_then(|c| c.last_sync_started_at),
            last_sync_finished_at: current.and_then(|c| c.last_sync_finished_at),
            last_sync_status: current.and_then(|c| c.last_sync_status.clone()),
            last_error: current.and_then(|c| c.last_error.clone()),
            phase: runtime.phase,
            reason_code: runtime.reason_code,
            max_checkpoint_age_seconds: runtime.max_checkpoint_age_seconds,
        });
    }
    Ok(Json(items))
}

async fn probe_connection_runtime_states(
    connections: &[ConnectionConfig],
    sync_state: &SyncState,
) -> Vec<(
    ConnectionConfig,
    Option<ConnectionState>,
    Option<PostgresCdcRuntimeState>,
)> {
    collect_connection_runtime_states(connections, sync_state, |connection, current| async move {
        load_postgres_cdc_runtime_state(&connection, current.as_ref()).await
    })
    .await
}

async fn collect_connection_runtime_states<T, F, Fut>(
    connections: &[ConnectionConfig],
    sync_state: &SyncState,
    inspect: F,
) -> Vec<(ConnectionConfig, Option<ConnectionState>, T)>
where
    F: Fn(ConnectionConfig, Option<ConnectionState>) -> Fut + Copy,
    Fut: Future<Output = T>,
{
    join_all(connections.iter().map(|connection| {
        let connection = connection.clone();
        let current = sync_state.connections.get(&connection.id).cloned();
        async move {
            let inspected = inspect(connection.clone(), current.clone()).await;
            (connection, current, inspected)
        }
    }))
    .await
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
    let cdc_runtime_state =
        load_postgres_cdc_runtime_state(connection, sync_state.connections.get(&connection_id))
            .await;
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
    let cdc_runtime_state =
        load_postgres_cdc_runtime_state(config, connection_state.as_ref()).await;
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

    Ok(Json(ProgressResponse {
        connection_id,
        state: connection_state,
        current_run,
        runtime,
        tables,
    }))
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
    let cdc_runtime_state =
        load_postgres_cdc_runtime_state(config, connection_state.as_ref()).await;
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

async fn load_postgres_cdc_runtime_state(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
) -> Option<PostgresCdcRuntimeState> {
    let SourceConfig::Postgres(pg) = &connection.source else {
        return None;
    };
    if !pg.cdc.unwrap_or(true) {
        return None;
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
        return Some(PostgresCdcRuntimeState::Initializing);
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
                "failed to inspect postgres CDC slot state"
            );
            return Some(PostgresCdcRuntimeState::Unknown);
        }
    };

    let row = match timeout(
        CDC_SLOT_INSPECTION_TIMEOUT,
        sqlx::query(
            r#"
            select active
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
                "failed to query postgres CDC slot state"
            );
            return Some(PostgresCdcRuntimeState::Unknown);
        }
        Err(_) => {
            warn!(
                connection = %connection.id,
                slot_name = %slot_name,
                "timed out while inspecting postgres CDC slot state"
            );
            return Some(PostgresCdcRuntimeState::Unknown);
        }
    };

    match row {
        Some(row) => {
            let active: bool = row.try_get("active").unwrap_or(false);
            if active {
                Some(PostgresCdcRuntimeState::Following)
            } else {
                Some(PostgresCdcRuntimeState::Initializing)
            }
        }
        None if has_last_lsn => Some(PostgresCdcRuntimeState::ContinuityLost),
        None => Some(PostgresCdcRuntimeState::Initializing),
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
}
