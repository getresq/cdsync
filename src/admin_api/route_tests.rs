use super::*;
use crate::config::{
    AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
    DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
    PostgresTableConfig, SourceConfig, StateConfig, StatsConfig, SyncConfig,
};
use crate::state::{
    CdcBatchLoadQueueSummary, CdcCoordinatorSummary, ConnectionState, PostgresCdcState,
};
use crate::stats::{RunSummary, StatsHandle, TableStatsSnapshot};
use crate::types::TableCheckpoint;
use async_trait::async_trait;
use axum::http::StatusCode;
use chrono::{TimeZone, Utc};
use futures::StreamExt;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::{Duration, Instant, sleep, timeout};

#[derive(Clone)]
struct FakeStateBackend {
    state: SyncState,
    ping_error: Option<String>,
    load_delay: Option<Duration>,
    batch_load_queue_summary: Option<CdcBatchLoadQueueSummary>,
    cdc_coordinator_summary: Option<CdcCoordinatorSummary>,
    requested_resyncs: Arc<Mutex<Vec<(String, String)>>>,
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
        if let Some(delay) = self.load_delay {
            sleep(delay).await;
        }
        Ok(self.state.clone())
    }

    async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>> {
        if let Some(delay) = self.load_delay {
            sleep(delay).await;
        }
        Ok(self.state.connections.get(connection_id).cloned())
    }

    async fn load_cdc_batch_load_queue_summary(
        &self,
        _connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadQueueSummary> {
        Ok(self.batch_load_queue_summary.clone().unwrap_or_default())
    }

    async fn load_cdc_coordinator_summary(
        &self,
        _connection_id: &str,
        _wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary> {
        Ok(self.cdc_coordinator_summary.clone().unwrap_or_default())
    }

    async fn request_postgres_table_resync(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        self.requested_resyncs
            .lock()
            .expect("requested_resyncs lock")
            .push((connection_id.to_string(), source_table.to_string()));
        Ok(())
    }

    async fn clear_postgres_table_resync_request(
        &self,
        connection_id: &str,
        source_table: &str,
    ) -> anyhow::Result<()> {
        self.requested_resyncs
            .lock()
            .expect("requested_resyncs lock")
            .retain(|(stored_connection, stored_table)| {
                stored_connection != connection_id || stored_table != source_table
            });
        Ok(())
    }
}

#[derive(Clone)]
struct CountingStateBackend {
    state: SyncState,
    load_state_calls: Arc<AtomicUsize>,
    load_connection_state_calls: Arc<AtomicUsize>,
    load_queue_summary_calls: Arc<AtomicUsize>,
    load_coordinator_summary_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AdminStateBackend for CountingStateBackend {
    async fn ping(&self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn load_state(&self) -> anyhow::Result<SyncState> {
        self.load_state_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.clone())
    }

    async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>> {
        self.load_connection_state_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(self.state.connections.get(connection_id).cloned())
    }

    async fn load_cdc_batch_load_queue_summary(
        &self,
        _connection_id: &str,
    ) -> anyhow::Result<CdcBatchLoadQueueSummary> {
        self.load_queue_summary_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CdcBatchLoadQueueSummary::default())
    }

    async fn load_cdc_coordinator_summary(
        &self,
        _connection_id: &str,
        _wal_bytes_behind_confirmed: Option<i64>,
    ) -> anyhow::Result<CdcCoordinatorSummary> {
        self.load_coordinator_summary_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(CdcCoordinatorSummary::default())
    }

    async fn request_postgres_table_resync(
        &self,
        _connection_id: &str,
        _source_table: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn clear_postgres_table_resync_request(
        &self,
        _connection_id: &str,
        _source_table: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct FakeRuntimeBackend {
    restarted_connections: Mutex<Vec<String>>,
}

#[async_trait]
impl AdminRuntimeBackend for FakeRuntimeBackend {
    async fn request_connection_restart(&self, connection_id: &str) -> anyhow::Result<()> {
        self.restarted_connections
            .lock()
            .expect("restarted_connections lock")
            .push(connection_id.to_string());
        Ok(())
    }
}

struct FailingRuntimeBackend {
    message: String,
}

#[async_trait]
impl AdminRuntimeBackend for FailingRuntimeBackend {
    async fn request_connection_restart(&self, _connection_id: &str) -> anyhow::Result<()> {
        anyhow::bail!(self.message.clone());
    }
}

#[derive(Clone, Default)]
struct FakeStatsBackend {
    runs: Vec<RunSummary>,
    run_tables: HashMap<String, Vec<TableStatsSnapshot>>,
    ping_error: Option<String>,
}

const TEST_KID: &str = "caller-service-test-20260401-01";
const TEST_PRIVATE_KEY_PEM: &str = r"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCnqmojl7pWw+Vq
/zX8CpkQLCpOgvT8JuChGB5d15nl7pTt8jeYH+yCznxYu5nyOo1OXqYAhRzKaQ01
IQXgrOo4GFKIPc+7XWThFF62Ay3jIYyeeauyD/s2rfIYDheJTvLPl+E7cbvX8hxy
DxSemUNd9Mn38PiYILzb1s3zOn830rkTSD7iPpYsx9ItvCxpWmq2euBfjflC6voE
CxbLvBMK/Z8e4eXvlRuEEPrKDnuB2w3CLdjr3klglT8XHhkRORRMSQRGKvJg/Jir
GSEdGTMw+VIMBAKSVla4WjbyZGXVesA4KOP+7kfKqvAJGXALKd+JycngFHPpA6sU
1aOgV2RLAgMBAAECggEAGNuqvdkuftOvbWQmKFaX5+5sXVSMJuBKuIefZPFkt1Le
kMKzHGJLSf98LxmtUtz8e0yMFxKlOJtHooNhYDSyyxtMDTgA1vobTUWcXybshDrC
ovJOEunMqIg0lv1r3ucuF7ogYhRUMcmLDxwORg9aDhGPaiu3Z7Ke3Yck5LVdDDT7
nHVFv436NVZ+n+x3cVhsRIhblLACaHbfP9Qb96bG9acsQZbbH72stV4kWSt1CwBi
yJFo2v+h1AHn7AUrxikFulUACf0z5BJ600ISotkWyyVHc+gM146Yyc5ZeRL6Tnmq
PACxMsLXCqcYh45RLu6pBpAmP4sSe5Xah7bjdlzwLQKBgQDi7b6V8+BhCy0kF513
FtbIV7Zwji8kDcl1G+JHD3MHt4YFu/PNqbEma2j0oak7acMw6oxNoFEhXFCg/3y+
t71+wY/DTFRYX8iL/xeRryiDZvD12NV3j5mH6fK8Oiu4sxMXwolusUC+8pyFuWI/
eIV4h9zsf0HWN+udHrN+Zu72tQKBgQC9JRsnyHLlaQng1S9+q6mfoZswCz02LZn8
IngKxaiMiINkTbsiXIBeCPh+vVBCaepd/wcdEzwyXgn/Wp1rYyTumS6eoFuT5eyq
SUKt9FfYKOE0vZGqtAcVfAbiWBiMov3sGc5FfCchf5EGgVS2NgluSVxAW8uRRzkd
7QhcOuLO/wKBgQCzRdqwoA982tV4k+dkM3jOoOySEuGO/A1RJQwn0z6us/9+/DLp
IMvAbE5oJGaLd0wqksDwelxdnI5eAjhMet+LCeNHCEAB6PmID6hRAS1iUaq+reRG
Jf3Gb73BkbsEmQPWW2szNXjO4N9ijUfemJno1HxloUsjrt3GLIDktPDHmQKBgA8E
KiK/bDfAXhNmeW3SDRZqSxrGWaa6ehYlWmhohtgZYm0NKsUwmNReW/Qb7YpIRF4Q
CC2LwGSzSJHoTMUgyubSbHwVeQ/F2kMuq8eJtYuouzBnuG/X+RQAk79WhSRtMEGV
TuX/VE/5g7cDf4kzww3pbxSA9SlkgSlaDybbWfRbAoGBAK/vWfHGKpCxUKefbwnW
n39PxGMwuG94QQd8sRRxkwgzs8phKwLXBwTzN8cZ1vLjrjQaHiRAarJ6Nl28lfTP
BJDvfWrriikStbF6cX5uEWyUgvIZBjtd+Q+c7IRAm2zl/loZ+nYbk4+G13yTXkjb
RZZXUhlVF0i1Uviw8GQfe9su
-----END PRIVATE KEY-----";
const TEST_PUBLIC_KEY_PEM: &str = r"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp6pqI5e6VsPlav81/AqZ
ECwqToL0/CbgoRgeXdeZ5e6U7fI3mB/sgs58WLuZ8jqNTl6mAIUcymkNNSEF4Kzq
OBhSiD3Pu11k4RRetgMt4yGMnnmrsg/7Nq3yGA4XiU7yz5fhO3G71/Iccg8UnplD
XfTJ9/D4mCC829bN8zp/N9K5E0g+4j6WLMfSLbwsaVpqtnrgX435Qur6BAsWy7wT
Cv2fHuHl75UbhBD6yg57gdsNwi3Y695JYJU/Fx4ZETkUTEkERiryYPyYqxkhHRkz
MPlSDAQCklZWuFo28mRl1XrAOCjj/u5HyqrwCRlwCynficnJ4BRz6QOrFNWjoFdk
SwIDAQAB
-----END PUBLIC KEY-----";

#[derive(serde::Serialize)]
struct TestServiceJwtClaims {
    exp: usize,
    iat: usize,
    iss: String,
    aud: serde_json::Value,
    sub: String,
    jti: String,
    scope: String,
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
                publication_mode: None,
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: Some(1),
                cdc_batch_size: Some(1000),
                cdc_apply_concurrency: Some(8),
                cdc_batch_load_worker_count: Some(8),
                cdc_batch_load_staging_worker_count: None,
                cdc_batch_load_reducer_worker_count: None,
                cdc_max_inflight_commits: None,
                cdc_batch_load_reducer_max_jobs: None,
                cdc_batch_load_reducer_enabled: None,
                cdc_backlog_max_pending_fragments: None,
                cdc_backlog_max_oldest_pending_seconds: None,
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
            auth: Some(AdminApiAuthConfig {
                service_jwt_public_keys: HashMap::from([(
                    TEST_KID.to_string(),
                    TEST_PUBLIC_KEY_PEM.to_string(),
                )]),
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

fn test_cdc_slot_sampler_cache(cfg: &Config) -> CdcSlotSamplerCache {
    let mut cache = HashMap::new();
    for connection in &cfg.connections {
        if !super::is_postgres_cdc_connection(connection) {
            continue;
        }
        let (tx, _rx) = watch::channel(CachedPostgresCdcSlotState::sampled(Some(
            PostgresCdcSlotSnapshot {
                slot_name: Some("slot_app".to_string()),
                active: true,
                restart_lsn: Some("0/16B6C40".to_string()),
                confirmed_flush_lsn: Some("0/16B6C50".to_string()),
                current_wal_lsn: Some("0/16B6C60".to_string()),
                wal_bytes_retained_by_slot: Some(16),
                wal_bytes_behind_confirmed: Some(16),
                continuity_lost: false,
            },
        )));
        cache.insert(connection.id.clone(), tx);
    }
    Arc::new(cache)
}

fn auth_header(scopes: &[&str]) -> String {
    auth_header_with_audiences(scopes, &[serde_json::Value::String("cdsync".to_string())])
}

fn auth_header_with_audiences(scopes: &[&str], audiences: &[serde_json::Value]) -> String {
    install_jwt_crypto_provider();
    let now = Utc::now().timestamp() as usize;
    let claims = TestServiceJwtClaims {
        exp: now + 600,
        iat: now,
        iss: "caller-service".to_string(),
        aud: if audiences.len() == 1 {
            audiences[0].clone()
        } else {
            serde_json::Value::Array(audiences.to_vec())
        },
        sub: "caller-service-admin".to_string(),
        jti: uuid::Uuid::new_v4().to_string(),
        scope: scopes.join(" "),
    };
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let token = encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).expect("encoding key"),
    )
    .expect("encode token");
    format!("Bearer {token}")
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
            last_synced_at: Some(Utc.with_ymd_and_hms(2026, 4, 1, 9, 58, 0).unwrap()),
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

#[tokio::test]
async fn publish_cached_postgres_cdc_slot_state_overwrites_snapshot_with_unknown() {
    let cache = super::build_cdc_slot_sampler_cache(&test_config());
    let tx = cache.get("app").expect("slot cache sender");
    super::publish_cached_postgres_cdc_slot_state(tx, CachedPostgresCdcSlotState::unknown());

    let updated = tx.borrow().clone();
    assert_eq!(updated.sampler_status, "unknown");
    assert!(updated.snapshot.is_none());
    assert!(updated.sampled_at.is_some());
}

#[tokio::test]
async fn postgres_cdc_slot_sampler_loads_connection_state_without_full_state_scan() {
    let mut cfg = test_config();
    let mut connection = cfg.connections[0].clone();
    let SourceConfig::Postgres(pg) = &mut connection.source else {
        panic!("expected postgres source in test config");
    };
    pg.url = "postgres://127.0.0.1:1/postgres".to_string();
    let load_state_calls = Arc::new(AtomicUsize::new(0));
    let load_connection_state_calls = Arc::new(AtomicUsize::new(0));
    let backend: Arc<dyn AdminStateBackend> = Arc::new(CountingStateBackend {
        state: test_state(),
        load_state_calls: Arc::clone(&load_state_calls),
        load_connection_state_calls: Arc::clone(&load_connection_state_calls),
        load_queue_summary_calls: Arc::new(AtomicUsize::new(0)),
        load_coordinator_summary_calls: Arc::new(AtomicUsize::new(0)),
    });
    cfg.connections = vec![connection];

    let _sample = super::sample_cached_postgres_cdc_slot_state(&cfg.connections[0], &backend).await;

    assert_eq!(load_state_calls.load(Ordering::SeqCst), 0);
    assert_eq!(load_connection_state_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sample_and_publish_postgres_cdc_state_marks_cache_unknown_when_slot_sampling_fails() {
    let cfg = test_config();
    let mut connection = cfg.connections[0].clone();
    let SourceConfig::Postgres(pg) = &mut connection.source else {
        panic!("expected postgres source in test config");
    };
    pg.url = "postgres://127.0.0.1:1/postgres".to_string();

    let backend: Arc<dyn AdminStateBackend> = Arc::new(FakeStateBackend {
        state: test_state(),
        ping_error: None,
        load_delay: None,
        batch_load_queue_summary: None,
        cdc_coordinator_summary: None,
        requested_resyncs: Arc::new(Mutex::new(Vec::new())),
    });
    let (tx, _rx) = watch::channel(CachedPostgresCdcSlotState::sampled(Some(
        PostgresCdcSlotSnapshot {
            slot_name: Some("slot_app".to_string()),
            active: true,
            restart_lsn: Some("0/16B6C40".to_string()),
            confirmed_flush_lsn: Some("0/16B6C50".to_string()),
            current_wal_lsn: Some("0/16B6C60".to_string()),
            wal_bytes_retained_by_slot: Some(16),
            wal_bytes_behind_confirmed: Some(16),
            continuity_lost: false,
        },
    )));

    super::sample_and_publish_postgres_cdc_state(&connection, &backend, &tx).await;

    let updated = tx.borrow().clone();
    assert_eq!(updated.sampler_status, "unknown");
    assert!(updated.snapshot.is_none());
    assert!(updated.sampled_at.is_some());
}

#[tokio::test]
async fn cdc_slot_sampler_samples_queue_and_coordinator_summaries_for_batch_load_connections() {
    let cfg = test_config();
    let mut connection = cfg.connections[0].clone();
    let SourceConfig::Postgres(pg) = &mut connection.source else {
        panic!("expected postgres source in test config");
    };
    pg.url = "postgres://127.0.0.1:1/postgres".to_string();
    let DestinationConfig::BigQuery(bigquery) = &mut connection.destination;
    bigquery.emulator_http = None;
    bigquery.emulator_grpc = None;

    let load_state_calls = Arc::new(AtomicUsize::new(0));
    let load_connection_state_calls = Arc::new(AtomicUsize::new(0));
    let load_queue_summary_calls = Arc::new(AtomicUsize::new(0));
    let load_coordinator_summary_calls = Arc::new(AtomicUsize::new(0));
    let backend: Arc<dyn AdminStateBackend> = Arc::new(CountingStateBackend {
        state: test_state(),
        load_state_calls,
        load_connection_state_calls,
        load_queue_summary_calls: Arc::clone(&load_queue_summary_calls),
        load_coordinator_summary_calls: Arc::clone(&load_coordinator_summary_calls),
    });
    let (tx, _rx) = watch::channel(CachedPostgresCdcSlotState::unknown());

    super::sample_and_publish_postgres_cdc_state(&connection, &backend, &tx).await;

    assert_eq!(load_queue_summary_calls.load(Ordering::SeqCst), 1);
    assert_eq!(load_coordinator_summary_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn spawn_cdc_slot_sampler_tasks_samples_batch_load_summaries_on_interval()
-> anyhow::Result<()> {
    let mut cfg = test_config();
    let mut connection = cfg.connections[0].clone();
    let SourceConfig::Postgres(pg) = &mut connection.source else {
        panic!("expected postgres source in test config");
    };
    pg.url = "postgres://127.0.0.1:1/postgres".to_string();
    let DestinationConfig::BigQuery(bigquery) = &mut connection.destination;
    bigquery.emulator_http = None;
    bigquery.emulator_grpc = None;
    cfg.connections = vec![connection];

    let load_state_calls = Arc::new(AtomicUsize::new(0));
    let load_connection_state_calls = Arc::new(AtomicUsize::new(0));
    let load_queue_summary_calls = Arc::new(AtomicUsize::new(0));
    let load_coordinator_summary_calls = Arc::new(AtomicUsize::new(0));
    let backend: Arc<dyn AdminStateBackend> = Arc::new(CountingStateBackend {
        state: test_state(),
        load_state_calls,
        load_connection_state_calls,
        load_queue_summary_calls: Arc::clone(&load_queue_summary_calls),
        load_coordinator_summary_calls: Arc::clone(&load_coordinator_summary_calls),
    });
    let cache = super::build_cdc_slot_sampler_cache(&cfg);
    let (shutdown_controller, shutdown_signal) = crate::runner::ShutdownController::new();

    super::spawn_cdc_slot_sampler_tasks(Arc::new(cfg), backend, cache, shutdown_signal);
    sleep(CDC_SLOT_SAMPLER_INTERVAL + Duration::from_millis(200)).await;
    shutdown_controller.shutdown();

    assert!(load_queue_summary_calls.load(Ordering::SeqCst) >= 1);
    assert!(load_coordinator_summary_calls.load(Ordering::SeqCst) >= 1);
    Ok(())
}

#[tokio::test]
async fn spawn_admin_server_thread_surfaces_bind_failures_before_returning() -> anyhow::Result<()> {
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let (_shutdown_controller, shutdown_signal) = crate::runner::ShutdownController::new();

    let (handle, ready_rx) = super::spawn_admin_server_thread(state, addr, shutdown_signal)?;
    let err = ready_rx
        .await
        .expect("startup result")
        .expect_err("expected bind failure");
    assert!(
        err.to_string().contains("Address already in use")
            || err.to_string().contains("address in use")
    );

    let _ = handle.join();
    drop(listener);
    Ok(())
}

#[tokio::test]
async fn spawn_admin_server_thread_does_not_wait_for_initial_slot_samples() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.connections = vec![
        ConnectionConfig {
            id: "app1".to_string(),
            ..cfg.connections[0].clone()
        },
        ConnectionConfig {
            id: "app2".to_string(),
            ..cfg.connections[0].clone()
        },
    ];
    let state = test_admin_state_with_config(
        cfg,
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: Some(Duration::from_millis(300)),
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    drop(listener);

    let (shutdown_controller, shutdown_signal) = crate::runner::ShutdownController::new();
    let start = Instant::now();
    let (handle, ready_rx) = super::spawn_admin_server_thread(state, addr, shutdown_signal)?;
    ready_rx.await.expect("startup result")?;
    let elapsed = start.elapsed();

    shutdown_controller.shutdown();
    let _ = handle.join();

    assert!(
        elapsed < Duration::from_millis(200),
        "startup took {:?}",
        elapsed
    );
    Ok(())
}

fn test_admin_state(
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
) -> AdminApiState {
    test_admin_state_with_config(test_config(), state_store, stats_db)
}

fn test_admin_state_with_config(
    cfg: Config,
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
) -> AdminApiState {
    test_admin_state_with_runtime(
        cfg,
        state_store,
        stats_db,
        Arc::new(FakeRuntimeBackend::default()),
    )
}

fn test_admin_state_with_runtime(
    cfg: Config,
    state_store: Arc<dyn AdminStateBackend>,
    stats_db: Option<Arc<dyn AdminStatsBackend>>,
    runtime_control: Arc<dyn AdminRuntimeBackend>,
) -> AdminApiState {
    let managed_connection_ids: HashSet<String> = cfg
        .connections
        .iter()
        .map(|connection| connection.id.clone())
        .collect();
    let cdc_slot_sampler_cache = test_cdc_slot_sampler_cache(&cfg);
    let auth_verifier = Arc::new(
        AdminApiServiceJwtVerifier::from_config(
            cfg.admin_api
                .as_ref()
                .and_then(|admin_api| admin_api.auth.as_ref())
                .expect("admin api auth"),
        )
        .expect("auth verifier"),
    );
    AdminApiState {
        cfg: Arc::new(cfg),
        state_store,
        runtime_control,
        stats_db,
        cdc_slot_sampler_cache,
        auth_verifier,
        started_at: Utc.with_ymd_and_hms(2026, 4, 1, 9, 59, 0).unwrap(),
        mode: "run".to_string(),
        connection_id: "app".to_string(),
        managed_connection_count: 1,
        managed_connection_ids: Arc::new(managed_connection_ids),
        config_hash: "config-hash".to_string(),
        deploy_revision: Some("deploy-123".to_string()),
        last_restart_reason: "startup".to_string(),
    }
}

#[tokio::test]
async fn load_current_run_view_prefers_live_run_snapshot() -> anyhow::Result<()> {
    let connection_id = "app_live_stats";
    let stats = StatsHandle::new(connection_id);
    stats.record_extract("public.accounts", 7, 15).await;
    stats.record_load("public.accounts", 7, 7, 0, 20).await;

    let mut cfg = test_config();
    cfg.connections[0].id = connection_id.to_string();
    let mut sync_state = test_state();
    let connection_state = sync_state
        .connections
        .remove("app")
        .expect("seed connection state");
    sync_state
        .connections
        .insert(connection_id.to_string(), connection_state);

    let state = test_admin_state_with_config(
        cfg,
        Arc::new(FakeStateBackend {
            state: sync_state,
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );

    let (current_run, tables, live_snapshot) =
        super::load_current_run_view(&state, connection_id).await?;
    assert_eq!(current_run.expect("current run").rows_read, 7);
    assert_eq!(tables[0].rows_written, 7);
    assert_eq!(live_snapshot.expect("live snapshot").load_ms, 20);
    Ok(())
}

#[tokio::test]
async fn admin_api_stream_route_emits_sse_frames() -> anyhow::Result<()> {
    let mut cfg = test_config();
    let SourceConfig::Postgres(pg) = &mut cfg.connections[0].source else {
        panic!("expected postgres source in test config");
    };
    pg.cdc = Some(false);

    let state = test_admin_state_with_config(
        cfg,
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .get(format!("{base_url}/v1/stream?connection=app"))
        .header("Authorization", &auth)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );

    let mut bytes_stream = response.bytes_stream();
    let first_chunk = timeout(Duration::from_secs(5), bytes_stream.next())
        .await?
        .context("missing first SSE chunk")??;
    let body = String::from_utf8(first_chunk.to_vec()).expect("utf8 chunk");
    assert!(body.contains("event: service.heartbeat"));
    assert!(body.contains("\"connection_id\":\"app\""));

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_stream_route_keeps_polling_connections_in_syncing_state() -> anyhow::Result<()> {
    let mut cfg = test_config();
    let SourceConfig::Postgres(pg) = &mut cfg.connections[0].source else {
        panic!("expected postgres source in test config");
    };
    pg.cdc = Some(false);

    let mut sync_state = test_state();
    let connection_state = sync_state
        .connections
        .get_mut("app")
        .expect("seed connection state");
    connection_state.last_sync_status = Some("running".to_string());
    connection_state.last_sync_finished_at = None;
    connection_state.postgres_cdc = None;

    let state = test_admin_state_with_config(
        cfg,
        Arc::new(FakeStateBackend {
            state: sync_state,
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .get(format!("{base_url}/v1/stream?connection=app"))
        .header("Authorization", &auth)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let mut bytes_stream = response.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut body = String::new();
    while !body.contains("event: connection.runtime") && tokio::time::Instant::now() < deadline {
        let chunk = timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            bytes_stream.next(),
        )
        .await?
        .context("missing SSE chunk")??;
        body.push_str(std::str::from_utf8(&chunk).expect("utf8 chunk"));
    }

    assert!(body.contains("event: connection.runtime"));
    assert!(body.contains("\"phase\":\"syncing\""));
    assert!(body.contains("\"reason_code\":\"sync_in_progress\""));
    assert!(!body.contains("cdc_state_unknown"));

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_stream_route_emits_cached_cdc_snapshot() -> anyhow::Result<()> {
    let mut cfg = test_config();
    let DestinationConfig::BigQuery(bq) = &mut cfg.connections[0].destination;
    bq.emulator_http = None;
    bq.emulator_grpc = None;

    let state = test_admin_state_with_config(
        cfg,
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: Some(CdcBatchLoadQueueSummary {
                pending_jobs: 2,
                running_jobs: 1,
                jobs_per_minute: 3,
                rows_per_minute: 120,
                ..Default::default()
            }),
            cdc_coordinator_summary: Some(CdcCoordinatorSummary {
                next_sequence_to_ack: 42,
                last_enqueued_sequence: Some(45),
                sequence_lag: Some(3),
                pending_fragments: 4,
                ..Default::default()
            }),
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .get(format!("{base_url}/v1/stream?connection=app"))
        .header("Authorization", &auth)
        .send()
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

    let mut bytes_stream = response.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut body = String::new();
    while !body.contains("event: connection.cdc_coordinator")
        && tokio::time::Instant::now() < deadline
    {
        let chunk = timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            bytes_stream.next(),
        )
        .await?
        .context("missing SSE chunk")??;
        body.push_str(std::str::from_utf8(&chunk).expect("utf8 chunk"));
    }
    assert!(body.contains("event: connection.cdc"));
    assert!(body.contains("event: connection.cdc_progress"));
    assert!(body.contains("event: connection.cdc_queue"));
    assert!(body.contains("event: connection.cdc_coordinator"));
    assert!(body.contains("\"slot_active\":true"));
    assert!(body.contains("\"confirmed_flush_lsn\":\"0/16B6C50\""));
    assert!(body.contains("\"pending_jobs\":2"));
    assert!(body.contains("\"rows_per_minute\":120"));
    assert!(body.contains("\"next_sequence_to_ack\":42"));
    assert!(body.contains("\"pending_fragments\":4"));

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_in_process_smoke_routes_work() -> anyhow::Result<()> {
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
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

    let auth = auth_header(&["cdsync:admin"]);

    let status = client
        .get(format!("{base_url}/v1/status"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(status.status(), StatusCode::OK);
    let status_json = status.json::<serde_json::Value>().await?;
    assert_eq!(status_json["mode"], "run");
    assert_eq!(status_json["connection_id"], "app");
    assert_eq!(status_json["connection_count"], 1);
    assert_eq!(status_json["config_hash"], "config-hash");
    assert_eq!(status_json["deploy_revision"], "deploy-123");
    assert_eq!(status_json["last_restart_reason"], "startup");

    let config = client
        .get(format!("{base_url}/v1/config"))
        .header("Authorization", &auth)
        .send()
        .await?;
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
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend {
            runs,
            run_tables,
            ping_error: None,
        })),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let connections = client
        .get(format!("{base_url}/v1/connections"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(connections.status(), StatusCode::OK);
    let connections_json = connections.json::<serde_json::Value>().await?;
    assert_eq!(connections_json[0]["id"], "app");
    assert_eq!(connections_json[0]["last_sync_status"], "success");
    assert_eq!(connections_json[0]["phase"], "healthy");
    assert_eq!(connections_json[0]["reason_code"], "healthy");
    assert!(
        connections_json[0]["max_checkpoint_age_seconds"]
            .as_i64()
            .is_some()
    );

    let connection = client
        .get(format!("{base_url}/v1/connections/app"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(connection.status(), StatusCode::OK);
    let connection_json = connection.json::<serde_json::Value>().await?;
    assert_eq!(connection_json["config"]["id"], "app");
    assert_eq!(
        connection_json["state"]["postgres_cdc"]["slot_name"],
        "slot_app"
    );

    let runtime = client
        .get(format!("{base_url}/v1/connections/app/runtime"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(runtime.status(), StatusCode::OK);
    let runtime_json = runtime.json::<serde_json::Value>().await?;
    assert_eq!(runtime_json["phase"], "healthy");
    assert_eq!(runtime_json["reason_code"], "healthy");
    assert_eq!(runtime_json["config_hash"], "config-hash");
    assert_eq!(runtime_json["deploy_revision"], "deploy-123");
    assert!(
        runtime_json["max_checkpoint_age_seconds"]
            .as_i64()
            .is_some()
    );

    let progress = client
        .get(format!("{base_url}/v1/connections/app/progress"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(progress.status(), StatusCode::OK);
    let progress_json = progress.json::<serde_json::Value>().await?;
    assert_eq!(progress_json["current_run"]["run_id"], "run-1");
    assert_eq!(progress_json["runtime"]["phase"], "healthy");
    assert_eq!(progress_json["cdc"]["sampler_status"], "ok");
    assert_eq!(progress_json["cdc"]["slot_active"], true);
    assert_eq!(progress_json["cdc"]["confirmed_flush_lsn"], "0/16B6C50");
    assert!(progress_json["cdc"]["sampled_at"].is_string());
    assert_eq!(progress_json["cdc_progress"]["status"], "idle");
    assert_eq!(progress_json["cdc_progress"]["primary_blocker"], "none");
    assert_eq!(progress_json["tables"][0]["table_name"], "public.accounts");
    assert_eq!(
        progress_json["tables"][0]["checkpoint"]["last_primary_key"],
        "42"
    );
    assert_eq!(progress_json["tables"][0]["stats"]["rows_written"], 10);
    assert_eq!(progress_json["tables"][0]["phase"], "healthy");
    assert_eq!(progress_json["tables"][0]["reason_code"], "healthy");
    assert!(
        progress_json["tables"][0]["checkpoint_age_seconds"]
            .as_i64()
            .is_some()
    );
    assert!(progress_json["tables"][0]["lag_seconds"].as_i64().is_some());

    let tables = client
        .get(format!("{base_url}/v1/connections/app/tables"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(tables.status(), StatusCode::OK);
    let tables_json = tables.json::<serde_json::Value>().await?;
    assert_eq!(tables_json[0]["table_name"], "public.accounts");
    assert_eq!(tables_json[0]["phase"], "healthy");
    assert_eq!(tables_json[0]["reason_code"], "healthy");

    let runs = client
        .get(format!("{base_url}/v1/connections/app/runs?limit=1"))
        .header("Authorization", &auth)
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
async fn admin_api_resync_table_route_persists_request_and_restarts_connection()
-> anyhow::Result<()> {
    let requested_resyncs = Arc::new(Mutex::new(Vec::new()));
    let runtime_backend = Arc::new(FakeRuntimeBackend::default());
    let state = test_admin_state_with_runtime(
        test_config(),
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::clone(&requested_resyncs),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
        runtime_backend.clone(),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .post(format!("{base_url}/v1/connections/app/resync-table"))
        .header("Authorization", &auth)
        .json(&serde_json::json!({ "table": "public.accounts" }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["connection_id"], "app");
    assert_eq!(body["table"], "public.accounts");
    assert_eq!(body["requested"], true);
    assert_eq!(body["restart_requested"], true);

    assert_eq!(
        requested_resyncs
            .lock()
            .expect("requested_resyncs lock")
            .as_slice(),
        &[("app".to_string(), "public.accounts".to_string())]
    );
    assert_eq!(
        runtime_backend
            .restarted_connections
            .lock()
            .expect("restarted connections lock")
            .as_slice(),
        &["app".to_string()]
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_resync_table_route_rejects_unmanaged_connection_without_persisting()
-> anyhow::Result<()> {
    let requested_resyncs = Arc::new(Mutex::new(Vec::new()));
    let runtime_backend = Arc::new(FakeRuntimeBackend::default());
    let mut cfg = test_config();
    cfg.connections.push(ConnectionConfig {
        id: "disabled_app".to_string(),
        enabled: Some(false),
        ..cfg.connections[0].clone()
    });
    let mut state = test_admin_state_with_runtime(
        cfg,
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::clone(&requested_resyncs),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
        runtime_backend.clone(),
    );
    state.managed_connection_ids = Arc::new(HashSet::from(["app".to_string()]));
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .post(format!(
            "{base_url}/v1/connections/disabled_app/resync-table"
        ))
        .header("Authorization", &auth)
        .json(&serde_json::json!({ "table": "public.accounts" }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(
        body["error"],
        "connection disabled_app is not managed by this CDSync process"
    );
    assert!(
        requested_resyncs
            .lock()
            .expect("requested_resyncs lock")
            .is_empty()
    );
    assert!(
        runtime_backend
            .restarted_connections
            .lock()
            .expect("restarted connections lock")
            .is_empty()
    );

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_resync_table_route_rolls_back_request_when_restart_fails() -> anyhow::Result<()>
{
    let requested_resyncs = Arc::new(Mutex::new(Vec::new()));
    let runtime_backend = Arc::new(FailingRuntimeBackend {
        message: "restart failed".to_string(),
    });
    let state = test_admin_state_with_runtime(
        test_config(),
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::clone(&requested_resyncs),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
        runtime_backend,
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let response = client
        .post(format!("{base_url}/v1/connections/app/resync-table"))
        .header("Authorization", &auth)
        .json(&serde_json::json!({ "table": "public.accounts" }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.json::<serde_json::Value>().await?;
    assert_eq!(body["error"], "restart failed");
    assert!(
        requested_resyncs
            .lock()
            .expect("requested_resyncs lock")
            .is_empty()
    );

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
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        None,
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();
    let auth = auth_header(&["cdsync:admin"]);

    let runs = client
        .get(format!("{base_url}/v1/connections/app/runs"))
        .header("Authorization", &auth)
        .send()
        .await?;
    assert_eq!(runs.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let runs_json = runs.json::<serde_json::Value>().await?;
    assert_eq!(runs_json["error"], "stats are disabled for this service");

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_rejects_missing_or_wrong_scope_tokens() -> anyhow::Result<()> {
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            state: test_state(),
            ping_error: None,
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend::default())),
    );
    let (base_url, handle) = spawn_test_server(state).await?;
    let client = Client::new();

    let missing = client.get(format!("{base_url}/v1/status")).send().await?;
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let wrong_scope = client
        .get(format!("{base_url}/v1/status"))
        .header("Authorization", auth_header(&["other:scope"]))
        .send()
        .await?;
    assert_eq!(wrong_scope.status(), StatusCode::UNAUTHORIZED);

    handle.abort();
    let _ = handle.await;
    Ok(())
}

#[tokio::test]
async fn admin_api_accepts_array_audience_tokens() -> anyhow::Result<()> {
    let (runs, run_tables) = test_runs();
    let state = test_admin_state(
        Arc::new(FakeStateBackend {
            ping_error: None,
            state: test_state(),
            load_delay: None,
            batch_load_queue_summary: None,
            cdc_coordinator_summary: None,
            requested_resyncs: Arc::new(Mutex::new(Vec::new())),
        }),
        Some(Arc::new(FakeStatsBackend {
            ping_error: None,
            runs,
            run_tables,
        })),
    );
    let (base_url, handle) = spawn_test_server(state).await?;

    let response = reqwest::Client::new()
        .get(format!("{base_url}/v1/status"))
        .header(
            "Authorization",
            auth_header_with_audiences(
                &["cdsync:admin"],
                &[
                    serde_json::Value::String("other-service".to_string()),
                    serde_json::Value::String("cdsync".to_string()),
                ],
            ),
        )
        .send()
        .await?;

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    handle.abort();
    Ok(())
}
