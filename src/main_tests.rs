use super::*;

mod reconcile_tests {
    #[test]
    fn reconcile_count_match_accounts_for_soft_deleted_rows() {
        let source_summary = crate::sources::postgres::PostgresTableSummary {
            row_count: 10,
            max_updated_at: None,
        };
        let destination_summary = crate::destinations::bigquery::DestinationTableSummary {
            row_count: 12,
            max_synced_at: None,
            deleted_rows: 2,
        };
        let table = crate::sources::postgres::ResolvedPostgresTable {
            name: "public.accounts".to_string(),
            primary_key: "id".to_string(),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: true,
            soft_delete_column: Some("deleted_at".to_string()),
            where_clause: None,
            columns: crate::config::ColumnSelection {
                include: Vec::new(),
                exclude: Vec::new(),
            },
        };

        assert!(crate::ops::reconcile_count_match(
            &source_summary,
            &destination_summary,
            &table
        ));
    }

    #[test]
    fn reconcile_count_match_uses_raw_count_without_soft_delete() {
        let source_summary = crate::sources::postgres::PostgresTableSummary {
            row_count: 10,
            max_updated_at: None,
        };
        let destination_summary = crate::destinations::bigquery::DestinationTableSummary {
            row_count: 12,
            max_synced_at: None,
            deleted_rows: 2,
        };
        let table = crate::sources::postgres::ResolvedPostgresTable {
            name: "public.accounts".to_string(),
            primary_key: "id".to_string(),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: false,
            soft_delete_column: None,
            where_clause: None,
            columns: crate::config::ColumnSelection {
                include: Vec::new(),
                exclude: Vec::new(),
            },
        };

        assert!(!crate::ops::reconcile_count_match(
            &source_summary,
            &destination_summary,
            &table
        ));
    }
}

mod sync_selection_tests {
    use super::*;
    use crate::config::StatsConfig;
    use tokio::time::{Duration, sleep};

    fn test_connection(id: &str, enabled: Option<bool>) -> crate::config::ConnectionConfig {
        crate::config::ConnectionConfig {
            id: id.to_string(),
            enabled,
            source: crate::config::SourceConfig::Postgres(crate::config::PostgresConfig {
                url: "postgres://user:pass@localhost/db".to_string(),
                tables: Some(vec![crate::config::PostgresTableConfig {
                    name: "public.accounts".to_string(),
                    primary_key: Some("id".to_string()),
                    updated_at_column: Some("updated_at".to_string()),
                    soft_delete: Some(false),
                    soft_delete_column: None,
                    where_clause: None,
                    columns: None,
                }]),
                table_selection: None,
                batch_size: Some(100),
                cdc: Some(false),
                publication: None,
                schema_changes: Some(crate::config::SchemaChangePolicy::Auto),
                cdc_pipeline_id: None,
                cdc_batch_size: None,
                cdc_max_fill_ms: None,
                cdc_max_pending_events: None,
                cdc_idle_timeout_seconds: None,
                cdc_tls: None,
                cdc_tls_ca_path: None,
                cdc_tls_ca: None,
            }),
            destination: crate::config::DestinationConfig::BigQuery(
                crate::config::BigQueryConfig {
                    project_id: "proj".to_string(),
                    dataset: "dataset".to_string(),
                    location: None,
                    service_account_key_path: None,
                    service_account_key: None,
                    partition_by_synced_at: Some(false),
                    storage_write_enabled: Some(false),
                    batch_load_bucket: None,
                    batch_load_prefix: None,
                    emulator_http: Some("http://localhost:9050".to_string()),
                    emulator_grpc: Some("localhost:9051".to_string()),
                },
            ),
            schedule: Some(crate::config::ScheduleConfig {
                every: Some("10m".to_string()),
            }),
        }
    }

    fn test_state_config() -> Option<crate::config::StateConfig> {
        std::env::var("CDSYNC_TEST_STATE_URL")
            .ok()
            .map(|url| crate::config::StateConfig { url, schema: None })
    }

    fn test_stats_config() -> Option<(String, StatsConfig)> {
        let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
        let config = StatsConfig {
            url: Some(url.clone()),
            schema: Some(format!(
                "cdsync_stats_flush_test_{}",
                uuid::Uuid::new_v4().simple()
            )),
        };
        Some((url, config))
    }

    #[test]
    fn select_sync_connections_returns_only_enabled_connections_without_filter() {
        let connections = vec![
            test_connection("enabled", Some(true)),
            test_connection("disabled", Some(false)),
            test_connection("default-enabled", None),
        ];

        let selected = select_sync_connections(&connections, None).expect("selected connections");
        let ids: Vec<&str> = selected
            .iter()
            .map(|connection| connection.id.as_str())
            .collect();
        assert_eq!(ids, vec!["enabled", "default-enabled"]);
    }

    #[test]
    fn select_sync_connections_errors_when_filter_missing() {
        let connections = vec![test_connection("enabled", Some(true))];
        let err = select_sync_connections(&connections, Some("missing")).expect_err("missing");
        assert!(err.to_string().contains("connection not found"));
    }

    #[test]
    fn select_sync_connections_errors_when_filtered_connection_is_disabled() {
        let connections = vec![test_connection("disabled", Some(false))];
        let err = select_sync_connections(&connections, Some("disabled")).expect_err("disabled");
        assert!(err.to_string().contains("is disabled"));
    }

    #[tokio::test]
    async fn load_latest_postgres_checkpoint_prefers_persisted_progress() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;
        let handle = store.handle("app");
        let persisted = TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            ..Default::default()
        };
        handle
            .save_postgres_checkpoint("public.accounts", &persisted)
            .await?;

        let fallback = TableCheckpoint {
            last_primary_key: Some("1".to_string()),
            ..Default::default()
        };
        let loaded = load_latest_postgres_checkpoint(&handle, "public.accounts", &fallback).await;

        assert_eq!(loaded.last_primary_key.as_deref(), Some("42"));
        Ok(())
    }

    #[tokio::test]
    async fn load_latest_salesforce_checkpoint_prefers_persisted_progress() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;
        let handle = store.handle("app");
        let persisted = TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            ..Default::default()
        };
        handle
            .save_salesforce_checkpoint("Account", &persisted)
            .await?;

        let fallback = TableCheckpoint {
            last_primary_key: Some("1".to_string()),
            ..Default::default()
        };
        let loaded = load_latest_salesforce_checkpoint(&handle, "Account", &fallback).await;

        assert_eq!(loaded.last_primary_key.as_deref(), Some("42"));
        Ok(())
    }

    #[tokio::test]
    async fn refresh_postgres_checkpoints_from_store_overwrites_stale_in_memory_state()
    -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        SyncStateStore::migrate_with_config(&config).await?;
        let store = SyncStateStore::open_with_config(&config).await?;
        let handle = store.handle("app");

        let persisted = TableCheckpoint {
            last_primary_key: Some("99".to_string()),
            ..Default::default()
        };
        handle
            .save_postgres_checkpoint("public.accounts", &persisted)
            .await?;

        let mut state = ConnectionState::default();
        state.postgres.insert(
            "public.accounts".to_string(),
            TableCheckpoint {
                last_primary_key: Some("1".to_string()),
                ..Default::default()
            },
        );

        refresh_postgres_checkpoints_from_store(&handle, &mut state).await;

        assert_eq!(
            state
                .postgres
                .get("public.accounts")
                .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
            Some("99")
        );
        Ok(())
    }

    #[tokio::test]
    async fn periodic_stats_flush_persists_live_run_progress() -> anyhow::Result<()> {
        let Some((default_url, config)) = test_stats_config() else {
            return Ok(());
        };
        StatsDb::migrate_with_config(&config, &default_url).await?;
        let db = StatsDb::new(&config, &default_url).await?;
        let handle = StatsHandle::new("app");
        handle.record_extract("public.accounts", 7, 11).await;
        handle.record_load("public.accounts", 7, 7, 0, 13).await;

        let flush = spawn_stats_flush_task_with_interval(
            db.clone(),
            handle.clone(),
            Duration::from_millis(10),
        );
        sleep(Duration::from_millis(40)).await;
        stop_stats_flush_task(Some(flush)).await;

        let runs = db.recent_runs(Some("app"), 5).await?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status.as_deref(), Some("running"));
        assert_eq!(runs[0].rows_read, 7);
        assert_eq!(runs[0].rows_written, 7);

        let tables = db.run_tables(&runs[0].run_id).await?;
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].table_name, "public.accounts");
        assert_eq!(tables[0].rows_read, 7);
        assert_eq!(tables[0].rows_upserted, 7);
        Ok(())
    }
}
