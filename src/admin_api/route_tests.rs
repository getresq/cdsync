use super::*;
use crate::config::{
    BigQueryConfig, Config, ConnectionConfig, LoggingConfig, MetadataConfig, ObservabilityConfig,
    PostgresConfig, PostgresTableConfig, SourceConfig, StateConfig, StatsConfig, SyncConfig,
};
use crate::state::{ConnectionState, PostgresCdcState};
use crate::stats::{RunSummary, TableStatsSnapshot};
use crate::types::TableCheckpoint;
use async_trait::async_trait;
use axum::http::StatusCode;
use chrono::{TimeZone, Utc};
use reqwest::Client;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone)]
struct FakeStateBackend {
    state: SyncState,
    ping_error: Option<String>,
}

#[async_trait]
impl AdminStateBackend for FakeStateBackend {
    async fn ping(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.ping_error {
            anyhow::bail!(error.clone());
        }
        Ok(())
    }

    async fn load_state(&self) -> anyhow::Result<SyncState> {
        Ok(self.state.clone())
    }
}

#[derive(Clone, Default)]
struct FakeStatsBackend {
    runs: Vec<RunSummary>,
    run_tables: HashMap<String, Vec<TableStatsSnapshot>>,
    ping_error: Option<String>,
}

#[async_trait]
impl AdminStatsBackend for FakeStatsBackend {
    async fn ping(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.ping_error {
            anyhow::bail!(error.clone());
        }
        Ok(())
    }

    async fn recent_runs(
        &self,
        connection_id: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<RunSummary>> {
        let filtered = self
            .runs
            .iter()
            .filter(|run| connection_id.is_none_or(|id| run.connection_id == id))
            .take(limit)
            .cloned()
            .collect();
        Ok(filtered)
    }

    async fn run_tables(&self, run_id: &str) -> anyhow::Result<Vec<TableStatsSnapshot>> {
        Ok(self.run_tables.get(run_id).cloned().unwrap_or_default())
    }
}

fn test_config() -> Config {
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
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: Some(1),
                cdc_batch_size: Some(1000),
                cdc_max_fill_ms: Some(2000),
                cdc_max_pending_events: Some(100_000),
                cdc_idle_timeout_seconds: Some(10),
                cdc_tls: Some(false),
                cdc_tls_ca_path: None,
                cdc_tls_ca: Some("secret-ca".to_string()),
            }),
            destination: crate::config::DestinationConfig::BigQuery(BigQueryConfig {
                project_id: "proj".to_string(),
                dataset: "dataset".to_string(),
                location: Some("US".to_string()),
                service_account_key_path: None,
                service_account_key: Some("secret-key".to_string()),
                partition_by_synced_at: Some(true),
                storage_write_enabled: Some(true),
                batch_load_bucket: Some("bucket".to_string()),
                batch_load_prefix: Some("prefix".to_string()),
                emulator_http: Some("http://localhost:9050".to_string()),
                emulator_grpc: Some("localhost:9051".to_string()),
            }),
            schedule: Some(crate::config::ScheduleConfig {
                every: Some("10m".to_string()),
            }),
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
            bind: Some("127.0.0.1:0".to_string()),
        }),
        observability: Some(ObservabilityConfig {
            service_name: Some("cdsync".to_string()),
            otlp_traces_endpoint: Some("https://trace.example".to_string()),
            otlp_metrics_endpoint: Some("https://metrics.example".to_string()),
            otlp_headers: Some(
                [("authorization".to_string(), "Bearer secret".to_string())]
                    .into_iter()
                    .collect(),
            ),
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

fn test_state() -> SyncState {
    let mut connection = ConnectionState {
        last_sync_started_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 10, 0, 0).unwrap()),
        last_sync_finished_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 10, 1, 0).unwrap()),
        last_sync_status: Some("success".to_string()),
        last_error: None,
        postgres_cdc: Some(PostgresCdcState {
            last_lsn: Some("0/16B6C50".to_string()),
            slot_name: Some("slot_app".to_string()),
        }),
        ..Default::default()
    };
    connection.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            ..Default::default()
        },
    );

    let mut connections = HashMap::new();
    connections.insert("app".to_string(), connection);
    SyncState {
        connections,
        updated_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 10, 1, 0).unwrap()),
    }
}

fn test_runs() -> (Vec<RunSummary>, HashMap<String, Vec<TableStatsSnapshot>>) {
    let run = RunSummary {
        run_id: "run-1".to_string(),
        connection_id: "app".to_string(),
        started_at: Utc.with_ymd_and_hms(2026, 4, 1, 10, 0, 0).unwrap(),
        finished_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 10, 1, 0).unwrap()),
        status: Some("success".to_string()),
        error: None,
        rows_read: 10,
        rows_written: 10,
        rows_deleted: 0,
        rows_upserted: 10,
        extract_ms: 100,
        load_ms: 200,
        api_calls: 0,
        rate_limit_hits: 0,
    };
    let table = TableStatsSnapshot {
        run_id: "run-1".to_string(),
        connection_id: "app".to_string(),
        table_name: "public.accounts".to_string(),
        rows_read: 10,
        rows_written: 10,
        rows_deleted: 0,
        rows_upserted: 10,
        extract_ms: 100,
        load_ms: 200,
    };
    let mut run_tables = HashMap::new();
    run_tables.insert("run-1".to_string(), vec![table]);
    (vec![run], run_tables)
}

async fn spawn_test_server(
    state: AdminApiState,
) -> anyhow::Result<(String, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    let app = router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr: SocketAddr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .map_err(anyhow::Error::from)
    });
    Ok((format!("http://{}", addr), handle))
}

fn test_admin_state(
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
) -> AdminApiState {
    AdminApiState {
        cfg: Arc::new(test_config()),
        state_store,
        stats_db,
        started_at: Utc.with_ymd_and_hms(2026, 4, 1, 9, 59, 0).unwrap(),
        mode: "run".to_string(),
        connection_id: "app".to_string(),
    }
}

#[tokio::test]
async fn admin_api_in_process_smoke_routes_work() -> anyhow::Result<()> {
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();

    let health = client.get(format!("{base_url}/healthz")).send().await?;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(health.json::<serde_json::Value>().await?["ok"], true);

    let ready = client.get(format!("{base_url}/readyz")).send().await?;
    assert_eq!(ready.status(), StatusCode::OK);
    assert_eq!(ready.json::<serde_json::Value>().await?["ok"], true);

    let status = client.get(format!("{base_url}/v1/status")).send().await?;
    assert_eq!(status.status(), StatusCode::OK);
    let status_json = status.json::<serde_json::Value>().await?;
    assert_eq!(status_json["mode"], "run");
    assert_eq!(status_json["connection_id"], "app");
    assert_eq!(status_json["connection_count"], 1);

    let config = client.get(format!("{base_url}/v1/config")).send().await?;
    assert_eq!(config.status(), StatusCode::OK);
    let config_json = config.json::<serde_json::Value>().await?;
    assert_eq!(
        config_json["state"]["url"],
        "postgres://postgres:***@example.com:5432/state"
    );
    assert_eq!(
        config_json["observability"]["otlp_headers"]["authorization"],
        "***"
    );
    assert_eq!(
        config_json["connections"][0]["source"]["url"],
        "postgres://postgres:***@example.com:5432/app"
    );
    assert_eq!(config_json["connections"][0]["source"]["cdc_tls_ca"], "***");

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_in_process_stateful_routes_work() -> anyhow::Result<()> {
    let (runs, run_tables) = test_runs();
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
        }),
        Some(Arc::new(FakeStatsBackend {
            runs,
            run_tables,
            ping_error: None,
        })),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();

    let connections = client
        .get(format!("{base_url}/v1/connections"))
        .send()
        .await?;
    assert_eq!(connections.status(), StatusCode::OK);
    let connections_json = connections.json::<serde_json::Value>().await?;
    assert_eq!(connections_json[0]["id"], "app");
    assert_eq!(connections_json[0]["last_sync_status"], "success");

    let connection = client
        .get(format!("{base_url}/v1/connections/app"))
        .send()
        .await?;
    assert_eq!(connection.status(), StatusCode::OK);
    let connection_json = connection.json::<serde_json::Value>().await?;
    assert_eq!(connection_json["config"]["id"], "app");
    assert_eq!(
        connection_json["state"]["postgres_cdc"]["slot_name"],
        "slot_app"
    );

    let progress = client
        .get(format!("{base_url}/v1/connections/app/progress"))
        .send()
        .await?;
    assert_eq!(progress.status(), StatusCode::OK);
    let progress_json = progress.json::<serde_json::Value>().await?;
    assert_eq!(progress_json["current_run"]["run_id"], "run-1");
    assert_eq!(progress_json["tables"][0]["table_name"], "public.accounts");
    assert_eq!(
        progress_json["tables"][0]["checkpoint"]["last_primary_key"],
        "42"
    );
    assert_eq!(progress_json["tables"][0]["stats"]["rows_written"], 10);

    let runs = client
        .get(format!("{base_url}/v1/connections/app/runs?limit=1"))
        .send()
        .await?;
    assert_eq!(runs.status(), StatusCode::OK);
    let runs_json = runs.json::<serde_json::Value>().await?;
    assert_eq!(runs_json[0]["run_id"], "run-1");

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_runs_route_returns_500_when_stats_disabled() -> anyhow::Result<()> {
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
        }),
        None,
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();

    let runs = client
        .get(format!("{base_url}/v1/connections/app/runs"))
        .send()
        .await?;
    assert_eq!(runs.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let runs_json = runs.json::<serde_json::Value>().await?;
    assert_eq!(runs_json["error"], "stats are disabled for this service");

    handle.abort();
    let _ = handle.await;
    Ok(())
}
