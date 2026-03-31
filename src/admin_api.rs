use crate::config::{
    AdminApiConfig, BigQueryConfig, Config, ConnectionConfig, DestinationConfig, LoggingConfig,
    MetadataConfig, ObservabilityConfig, PostgresConfig, SalesforceConfig, SourceConfig,
    SyncConfig,
};
use crate::runner::ShutdownSignal;
use crate::state::{ConnectionState, SyncStateStore};
use crate::stats::{RunSummary, StatsDb};
use anyhow::Context;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use url::Url;

#[derive(Clone)]
pub struct AdminApiState {
    cfg: Arc<Config>,
    state_store: SyncStateStore,
    stats_db: Option<StatsDb>,
    started_at: DateTime<Utc>,
    mode: String,
    connection_id: String,
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
struct ScrubbedConfig {
    state: ScrubbedStateConfig,
    metadata: Option<MetadataConfig>,
    logging: Option<LoggingConfig>,
    admin_api: Option<AdminApiConfig>,
    observability: Option<ScrubbedObservabilityConfig>,
    sync: Option<SyncConfig>,
    stats: Option<ScrubbedStatsConfig>,
    connections: Vec<ScrubbedConnectionConfig>,
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

    let state_store = SyncStateStore::open_with_config(&cfg.state).await?;
    let stats_db = if let Some(stats_cfg) = &cfg.stats {
        Some(StatsDb::new(stats_cfg, &cfg.state.url).await?)
    } else {
        None
    };

    let state = AdminApiState {
        cfg: Arc::new(cfg.clone()),
        state_store,
        stats_db,
        started_at: Utc::now(),
        mode: mode.to_string(),
        connection_id: connection_id.to_string(),
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/status", get(status))
        .route("/v1/config", get(config))
        .route("/v1/connections", get(connections))
        .route("/v1/connections/{id}", get(connection))
        .route("/v1/connections/{id}/checkpoints", get(checkpoints))
        .route("/v1/connections/{id}/runs", get(runs))
        .with_state(state);

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

fn scrub_config(cfg: &Config) -> ScrubbedConfig {
    ScrubbedConfig {
        state: ScrubbedStateConfig {
            url: scrub_url(&cfg.state.url),
            schema: cfg.state.schema.clone(),
        },
        metadata: cfg.metadata.clone(),
        logging: cfg.logging.clone(),
        admin_api: cfg.admin_api.clone(),
        observability: cfg.observability.as_ref().map(scrub_observability_config),
        sync: cfg.sync.clone(),
        stats: cfg.stats.as_ref().map(|stats| ScrubbedStatsConfig {
            url: stats.url.as_deref().map(scrub_url),
            schema: stats.schema.clone(),
        }),
        connections: cfg.connections.iter().map(scrub_connection).collect(),
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
}
