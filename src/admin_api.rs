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
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
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
}

#[derive(Clone)]
struct AdminApiServiceJwtVerifier {
    public_keys: HashMap<String, DecodingKey>,
    allowed_issuers: Vec<String>,
    allowed_audiences: Vec<String>,
    required_scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AdminApiServiceJwtClaims {
    exp: usize,
    iat: usize,
    iss: String,
    aud: String,
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
            || decoded.claims.aud.trim().is_empty()
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
    tables: Vec<TableProgress>,
}

#[derive(Serialize)]
struct TableProgress {
    table_name: String,
    checkpoint: Option<TableCheckpoint>,
    stats: Option<TableStatsSnapshot>,
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
    schema_changes: Option<crate::config::SchemaChangePolicy>,
    cdc_pipeline_id: Option<u64>,
    cdc_batch_size: Option<usize>,
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

pub async fn spawn_admin_api(
    cfg: &Config,
    connection_id: &str,
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
        .route("/v1/connections/{id}/checkpoints", get(checkpoints))
        .route("/v1/connections/{id}/progress", get(progress))
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
        connection_count: state.cfg.connections.len(),
    })
}

async fn config(State(state): State<AdminApiState>) -> Json<ScrubbedConfig> {
    Json(scrub_config(&state.cfg))
}

async fn connections(
    State(state): State<AdminApiState>,
) -> Result<Json<Vec<ConnectionSummary>>, AdminApiError> {
    let sync_state = state.state_store.load_state().await?;
    let items = state
        .cfg
        .connections
        .iter()
        .map(|connection| {
            let current = sync_state.connections.get(&connection.id);
            ConnectionSummary {
                id: connection.id.clone(),
                enabled: connection.enabled(),
                source_kind: source_kind(&connection.source),
                destination_kind: destination_kind(&connection.destination),
                last_sync_started_at: current.and_then(|c| c.last_sync_started_at),
                last_sync_finished_at: current.and_then(|c| c.last_sync_finished_at),
                last_sync_status: current.and_then(|c| c.last_sync_status.clone()),
                last_error: current.and_then(|c| c.last_error.clone()),
            }
        })
        .collect();
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

    let mut table_names = std::collections::BTreeSet::new();
    if let Some(state) = &connection_state {
        let checkpoint_map = active_checkpoint_map(state, &config.source);
        table_names.extend(checkpoint_map.keys().cloned());
    }
    table_names.extend(run_tables.iter().map(|table| table.table_name.clone()));

    let run_table_map: std::collections::HashMap<_, _> = run_tables
        .into_iter()
        .map(|table| (table.table_name.clone(), table))
        .collect();

    let tables = table_names
        .into_iter()
        .map(|table_name| TableProgress {
            checkpoint: connection_state.as_ref().and_then(|state| {
                active_checkpoint_map(state, &config.source)
                    .get(&table_name)
                    .cloned()
            }),
            stats: run_table_map.get(&table_name).cloned(),
            table_name,
        })
        .collect();

    Ok(Json(ProgressResponse {
        connection_id,
        state: connection_state,
        current_run,
        tables,
    }))
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
        schema_changes: pg.schema_changes.clone(),
        cdc_pipeline_id: pg.cdc_pipeline_id,
        cdc_batch_size: pg.cdc_batch_size,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TableCheckpoint;

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
}
