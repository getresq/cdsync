use super::*;
use crate::config::{
    AdminApiAuthConfig, AdminApiConfig, BigQueryConfig, Config, ConnectionConfig,
    DestinationConfig, LoggingConfig, MetadataConfig, ObservabilityConfig, PostgresConfig,
    PostgresTableConfig, SourceConfig, StateConfig, StatsConfig, SyncConfig,
};
use crate::retry::ErrorReasonCode;
use crate::types::{
    SnapshotChunkCheckpoint, TableCheckpoint, TableRuntimeState, TableRuntimeStatus,
};
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
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
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
fn cdc_progress_insight_marks_recent_completion_as_moving() {
    let cdc = ConnectionCdcSnapshot {
        sampler_status: "ok",
        sampled_at: None,
        slot_name: Some("slot".to_string()),
        slot_active: Some(true),
        current_wal_lsn: Some("0/B".to_string()),
        restart_lsn: Some("0/A".to_string()),
        confirmed_flush_lsn: Some("0/A".to_string()),
        wal_bytes_retained_by_slot: Some(16),
        wal_bytes_behind_confirmed: Some(8),
    };
    let queue = CdcBatchLoadQueueSummary {
        jobs_per_minute: 3,
        rows_per_minute: 1_200,
        ..Default::default()
    };
    let coordinator = CdcCoordinatorSummary {
        sequence_lag: Some(2),
        ..Default::default()
    };

    let insight = build_cdc_progress_insight(&cdc, Some(&queue), Some(&coordinator))
        .expect("progress insight");

    assert_eq!(insight.status, "moving");
    assert_eq!(insight.primary_blocker, "none");
    assert_eq!(insight.sequence_lag, Some(2));
}

#[test]
fn cdc_progress_insight_surfaces_failed_work_as_blocker() {
    let cdc = ConnectionCdcSnapshot {
        sampler_status: "ok",
        sampled_at: None,
        slot_name: Some("slot".to_string()),
        slot_active: Some(true),
        current_wal_lsn: None,
        restart_lsn: None,
        confirmed_flush_lsn: None,
        wal_bytes_retained_by_slot: None,
        wal_bytes_behind_confirmed: None,
    };
    let queue = CdcBatchLoadQueueSummary {
        failed_jobs: 1,
        ..Default::default()
    };

    let insight = build_cdc_progress_insight(&cdc, Some(&queue), None).expect("progress insight");

    assert_eq!(insight.status, "blocked");
    assert_eq!(insight.primary_blocker, "failed_work");
}

#[test]
fn build_table_progress_prefers_table_runtime_retry_state() {
    let connection = test_postgres_cdc_connection();
    let mut state = ConnectionState {
        last_sync_status: Some("running".to_string()),
        ..Default::default()
    };
    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            runtime: Some(TableRuntimeState {
                status: TableRuntimeStatus::Retrying,
                attempts: 4,
                reason: Some(ErrorReasonCode::BigqueryDmlQuota),
                last_error: Some("Quota exceeded: Your table exceeded quota for total number of dml jobs writing to a table".to_string()),
                next_retry_at: Some(Utc.with_ymd_and_hms(2026, 4, 2, 8, 5, 0).unwrap()),
                updated_at: Some(Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap()),
            }),
            ..Default::default()
        },
    );

    let tables = build_table_progress(
        &connection,
        Some(&state),
        &[],
        Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
        "cdc_initializing",
        Some(PostgresCdcRuntimeState::Initializing),
    );

    let table = tables
        .iter()
        .find(|table| table.table_name == "public.accounts")
        .expect("runtime table present");
    assert_eq!(table.phase, "retrying");
    assert_eq!(table.reason_code, "bigquery_dml_quota");
    assert!(matches!(
        table
            .runtime
            .as_ref()
            .map(|runtime| (&runtime.status, runtime.attempts)),
        Some((TableRuntimeStatus::Retrying, 4))
    ));
}

#[test]
fn build_table_progress_prefers_table_runtime_blocked_state() {
    let connection = test_postgres_cdc_connection();
    let mut state = ConnectionState {
        last_sync_status: Some("running".to_string()),
        ..Default::default()
    };
    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            runtime: Some(TableRuntimeState {
                status: TableRuntimeStatus::Blocked,
                attempts: 1,
                reason: Some(ErrorReasonCode::SnapshotBlocked),
                last_error: Some("permanent schema mismatch".to_string()),
                next_retry_at: None,
                updated_at: Some(Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap()),
            }),
            ..Default::default()
        },
    );

    let tables = build_table_progress(
        &connection,
        Some(&state),
        &[],
        Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
        "cdc_initializing",
        Some(PostgresCdcRuntimeState::Initializing),
    );

    let table = tables
        .iter()
        .find(|table| table.table_name == "public.accounts")
        .expect("runtime table present");
    assert_eq!(table.phase, "blocked");
    assert_eq!(table.reason_code, "snapshot_blocked");
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
fn derive_connection_runtime_uses_mixed_mode_when_snapshot_and_cdc_overlap() {
    let connection = test_postgres_cdc_connection();
    let mut state = ConnectionState {
        last_sync_status: Some("running".to_string()),
        ..Default::default()
    };
    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            snapshot_chunks: vec![SnapshotChunkCheckpoint {
                start_primary_key: Some("1".to_string()),
                end_primary_key: Some("100".to_string()),
                last_primary_key: Some("50".to_string()),
                complete: false,
            }],
            ..Default::default()
        },
    );

    let runtime = derive_connection_runtime(
        &connection,
        Some(&state),
        None,
        Some(PostgresCdcRuntimeState::Following),
        Utc.with_ymd_and_hms(2026, 4, 2, 8, 0, 0).unwrap(),
        RuntimeMetadata {
            config_hash: "hash",
            deploy_revision: Some("deploy"),
            last_restart_reason: "startup",
        },
    );

    assert_eq!(runtime.mode, "mixed");
    assert_eq!(runtime.phase, "snapshotting");
    assert_eq!(runtime.reason_code, "snapshot_in_progress");
}

#[test]
fn select_active_tables_prefers_busy_snapshot_or_blocked_tables() {
    let idle = TableProgress {
        table_name: "public.a_idle".to_string(),
        checkpoint: None,
        runtime: None,
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
        runtime: None,
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
        runtime: Some(TableRuntimeState {
            status: TableRuntimeStatus::Blocked,
            attempts: 1,
            reason: Some(ErrorReasonCode::SchemaBlocked),
            last_error: Some("schema blocked".to_string()),
            next_retry_at: None,
            updated_at: None,
        }),
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
        runtime: None,
        stats: None,
        phase: "snapshotting",
        reason_code: "snapshot_in_progress",
        checkpoint_age_seconds: None,
        lag_seconds: None,
        snapshot_chunks_total: 4,
        snapshot_chunks_complete: 2,
    };

    let selected = super::streaming::select_active_tables(&[
        idle,
        busy.clone(),
        blocked.clone(),
        snapshotting.clone(),
    ]);
    let names: Vec<_> = selected
        .iter()
        .map(|table| table.table_name.as_str())
        .collect();

    assert_eq!(
        names,
        vec!["public.z_busy", "public.m_blocked", "public.n_snapshot"]
    );
}
