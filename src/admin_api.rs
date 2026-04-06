mod models;
mod runtime_helpers;
#[cfg(test)]
mod route_tests;
mod scrub;

use crate::config::{
    AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
    DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
    SourceConfig, SyncConfig,
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
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::stream::{self, Stream};
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashSet;
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

use self::models::*;
use self::runtime_helpers::*;
use self::scrub::*;

const STREAM_INTERVAL: Duration = Duration::from_secs(2);
const STREAM_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);
const STREAM_ACTIVE_TABLE_LIMIT: usize = 25;
const CDC_SLOT_SAMPLER_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct AdminApiState {
    cfg: Arc<Config>,
    state_store: Arc<dyn AdminStateBackend>,
    runtime_control: Arc<dyn AdminRuntimeBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
    cdc_slot_sampler_cache: CdcSlotSamplerCache,
    auth_verifier: Arc<AdminApiServiceJwtVerifier>,
    started_at: DateTime<Utc>,
    mode: String,
    connection_id: String,
    managed_connection_count: usize,
    managed_connection_ids: Arc<HashSet<String>>,
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
    async fn request_postgres_table_resync(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()>;
    async fn clear_postgres_table_resync_request(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()>;
}

#[async_trait]
pub trait AdminRuntimeBackend: Send + Sync {
    async fn request_connection_restart(&self, connection_id: &str) -> anyhow::Result<()>;
}

#[derive(Default)]
struct NoopAdminRuntimeBackend;

#[async_trait]
impl AdminRuntimeBackend for NoopAdminRuntimeBackend {
    async fn request_connection_restart(&self, _connection_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl AdminRuntimeBackend for crate::runner::ConnectionRestartRegistry {
    async fn request_connection_restart(&self, connection_id: &str) -> anyhow::Result<()> {
        self.request_restart(connection_id)
    }
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

    async fn request_postgres_table_resync(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        SyncStateStore::request_postgres_table_resync(self, connection_id, source_table).await
    }

    async fn clear_postgres_table_resync_request(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        SyncStateStore::clear_postgres_table_resync_request(self, connection_id, source_table)
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


pub async fn spawn_admin_api(
    cfg: &Config,
    connection_id: &str,
    managed_connection_ids: &[String],
    managed_connection_count: usize,
    mode: &str,
    runtime_control: Option<Arc<dyn AdminRuntimeBackend>>,
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
        runtime_control: runtime_control.unwrap_or_else(|| Arc::new(NoopAdminRuntimeBackend)),
        stats_db,
        cdc_slot_sampler_cache,
        auth_verifier,
        started_at: Utc::now(),
        mode: mode.to_string(),
        connection_id: connection_id.to_string(),
        managed_connection_count,
        managed_connection_ids: Arc::new(managed_connection_ids.iter().cloned().collect()),
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
        .route("/v1/connections/{id}/resync-table", post(request_resync_table))
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

async fn request_resync_table(
    State(state): State<AdminApiState>,
    Path(connection_id): Path<String>,
    Json(request): Json<ResyncTableRequest>,
) -> Result<impl IntoResponse, AdminApiError> {
    let connection = state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == connection_id)
        .context("connection not found")?;
    if !state.managed_connection_ids.contains(&connection_id) {
        return Err(anyhow::anyhow!(
            "connection {} is not managed by this CDSync process",
            connection_id
        )
        .into());
    }
    let SourceConfig::Postgres(pg) = &connection.source;
    if !pg.cdc.unwrap_or(true) {
        return Err(
            anyhow::anyhow!("table resync is only supported for postgres CDC connections").into(),
        );
    }

    let requested_table = request.table.trim();
    if requested_table.is_empty() {
        return Err(anyhow::anyhow!("table is required").into());
    }
    let table_is_configured = if pg.tables.as_ref().is_some_and(|tables| !tables.is_empty()) {
        pg.tables
            .as_ref()
            .is_some_and(|tables| tables.iter().any(|table| table.name == requested_table))
    } else {
        let source = crate::sources::postgres::PostgresSource::new(
            pg.clone(),
            state.cfg.metadata_columns(),
        )
        .await?;
        let resolved_tables = source.resolve_tables().await?;
        resolved_tables
            .iter()
            .any(|table| table.name == requested_table)
    };
    if !table_is_configured {
        return Err(anyhow::anyhow!(
            "table {} is not currently configured for connection {}",
            requested_table,
            connection_id
        )
        .into());
    }

    state
        .state_store
        .request_postgres_table_resync(&connection_id, requested_table)
        .await?;
    if let Err(err) = state
        .runtime_control
        .request_connection_restart(&connection_id)
        .await
    {
        let _ = state
            .state_store
            .clear_postgres_table_resync_request(&connection_id, requested_table)
            .await;
        return Err(err.into());
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(ResyncTableResponse {
            connection_id,
            table: requested_table.to_string(),
            requested: true,
            restart_requested: true,
        }),
    ))
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
