use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{PostgresSource, TableSyncRequest};
use cdsync::state::ConnectionState;
use cdsync::types::{SyncMode, destination_table_name};
mod support;
use sqlx::postgres::PgPoolOptions;
use std::env;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn e2e_schema_addition_auto_alters_destination() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_schema_add_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let http_client = reqwest::Client::new();
    support::delete_table_if_exists(&http_client, &bq_http, &project_id, &dataset, &dest_table)
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, '1')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: None,
            soft_delete: Some(false),
            soft_delete_column: None,
            where_clause: None,
            columns: None,
        }]),
        table_selection: None,
        batch_size: Some(1000),
        cdc: Some(false),
        publication: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_max_fill_ms: None,
        cdc_max_pending_events: None,
        cdc_idle_timeout_seconds: None,
        cdc_tls: None,
        cdc_tls_ca_path: None,
        cdc_tls_ca: None,
    };

    let bq_config = BigQueryConfig {
        project_id: project_id.clone(),
        dataset: dataset.clone(),
        location: Some("US".to_string()),
        service_account_key_path: None,
        service_account_key: None,
        partition_by_synced_at: Some(false),
        storage_write_enabled: Some(true),
        emulator_http: Some(bq_http.clone()),
        emulator_grpc: Some(bq_grpc.clone()),
    };

    let source = PostgresSource::new(pg_config.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config.clone(), false).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    let checkpoint = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    sqlx::query(&format!(
        "alter table {} add column extra text",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, extra) values (2, 'beta', 'extra')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config).await?;
    let tables = source.resolve_tables().await?;
    let checkpoint = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    let fields =
        support::fetch_table_fields(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    assert!(fields.iter().any(|field| field == "extra"));
    let rows =
        support::fetch_table_rows(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    let mapped = support::map_rows(&fields, rows)?;
    assert_eq!(mapped.len(), 2);
    let extra_value = mapped
        .iter()
        .find(|row| support::value_to_string(row.get("id").unwrap()) == Some("2".to_string()))
        .and_then(|row| row.get("extra"))
        .and_then(support::value_to_string);
    assert_eq!(extra_value, Some("extra".to_string()));
    Ok(())
}

#[tokio::test]
#[ignore]
async fn e2e_schema_change_fail_fast() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_schema_fail_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, '1')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let pg_config = PostgresConfig {
        url: pg_url.clone(),
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: None,
            soft_delete: Some(false),
            soft_delete_column: None,
            where_clause: None,
            columns: None,
        }]),
        table_selection: None,
        batch_size: Some(1000),
        cdc: Some(false),
        publication: None,
        schema_changes: Some(SchemaChangePolicy::Fail),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_max_fill_ms: None,
        cdc_max_pending_events: None,
        cdc_idle_timeout_seconds: None,
        cdc_tls: None,
        cdc_tls_ca_path: None,
        cdc_tls_ca: None,
    };

    let bq_config = BigQueryConfig {
        project_id,
        dataset,
        location: Some("US".to_string()),
        service_account_key_path: None,
        service_account_key: None,
        partition_by_synced_at: Some(false),
        storage_write_enabled: Some(true),
        emulator_http: Some(bq_http),
        emulator_grpc: Some(bq_grpc),
    };

    let source = PostgresSource::new(pg_config.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    let checkpoint = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    sqlx::query(&format!(
        "alter table {} alter column name type int using name::int",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config).await?;
    let tables = source.resolve_tables().await?;
    let err = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await
        .expect_err("expected schema change failure");
    assert!(err.to_string().contains("schema change detected"));
    Ok(())
}

#[tokio::test]
#[ignore]
async fn e2e_schema_removal_resyncs_table() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_schema_drop_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);

    let http_client = reqwest::Client::new();
    support::delete_table_if_exists(&http_client, &bq_http, &project_id, &dataset, &dest_table)
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text, extra text)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, extra) values (1, 'alpha', 'extra')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: None,
            soft_delete: Some(false),
            soft_delete_column: None,
            where_clause: None,
            columns: None,
        }]),
        table_selection: None,
        batch_size: Some(1000),
        cdc: Some(false),
        publication: None,
        schema_changes: Some(SchemaChangePolicy::Resync),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_max_fill_ms: None,
        cdc_max_pending_events: None,
        cdc_idle_timeout_seconds: None,
        cdc_tls: None,
        cdc_tls_ca_path: None,
        cdc_tls_ca: None,
    };

    let bq_config = BigQueryConfig {
        project_id: project_id.clone(),
        dataset: dataset.clone(),
        location: Some("US".to_string()),
        service_account_key_path: None,
        service_account_key: None,
        partition_by_synced_at: Some(false),
        storage_write_enabled: Some(true),
        emulator_http: Some(bq_http.clone()),
        emulator_grpc: Some(bq_grpc),
    };

    let source = PostgresSource::new(pg_config.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    let checkpoint = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    sqlx::query(&format!(
        "alter table {} drop column extra",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (2, 'beta')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config).await?;
    let tables = source.resolve_tables().await?;
    let checkpoint = source
        .sync_table(TableSyncRequest {
            table: &tables[0],
            dest: &dest,
            checkpoint: state
                .postgres
                .get(&qualified_table)
                .cloned()
                .unwrap_or_default(),
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            default_batch_size: 1000,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    let fields =
        support::fetch_table_fields(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    let rows =
        support::fetch_table_rows(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    let mapped = support::map_rows(&fields, rows)?;

    assert_eq!(mapped.len(), 2);
    let mut ids: Vec<String> = mapped
        .iter()
        .filter_map(|row| row.get("id"))
        .filter_map(support::value_to_string)
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
    for row in &mapped {
        if let Some(extra) = row.get("extra") {
            assert!(extra.is_null());
        }
    }
    Ok(())
}
