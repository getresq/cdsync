use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource, TableSyncRequest};
use cdsync::state::ConnectionState;
use cdsync::types::{SyncMode, destination_table_name};
mod support;
use sqlx::postgres::PgPoolOptions;
use std::env;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn e2e_cdc_soft_delete_sets_deleted_at() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_cdc_delete_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_pub_del_{}", &suffix[..8]);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!("drop publication if exists {}", publication))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, 'alpha')",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "create publication {} for table {}",
        publication, qualified_table
    ))
    .execute(&pool)
    .await?;

    let pipeline_id = Uuid::new_v4().as_u128() as u64;
    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: None,
            soft_delete: Some(true),
            soft_delete_column: None,
            where_clause: None,
            columns: None,
        }]),
        table_selection: None,
        batch_size: Some(1000),
        cdc: Some(true),
        publication: Some(publication.clone()),
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(1000),
        cdc_max_fill_ms: Some(2000),
        cdc_max_pending_events: Some(10_000),
        cdc_idle_timeout_seconds: Some(1),
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

    let http_client = reqwest::Client::new();
    support::delete_table_if_exists(&http_client, &bq_http, &project_id, &dataset, &dest_table)
        .await?;

    let source = PostgresSource::new(pg_config.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config.clone(), false).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    source
        .sync_cdc(CdcSyncRequest {
            dest: &dest,
            state: &mut state,
            state_handle: None,
            mode: SyncMode::Full,
            dry_run: false,
            follow: false,
            default_batch_size: 1000,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

    sqlx::query(&format!("delete from {} where id = 1", qualified_table))
        .execute(&pool)
        .await?;

    source
        .sync_cdc(CdcSyncRequest {
            dest: &dest,
            state: &mut state,
            state_handle: None,
            mode: SyncMode::Incremental,
            dry_run: false,
            follow: false,
            default_batch_size: 1000,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

    let fields =
        support::fetch_table_fields(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    let rows =
        support::fetch_table_rows(&http_client, &bq_http, &project_id, &dataset, &dest_table)
            .await?;
    let mapped = support::map_rows(&fields, rows)?;

    let deleted_rows: Vec<_> = mapped
        .iter()
        .filter(|row| support::value_to_string(row.get("id").unwrap()) == Some("1".to_string()))
        .collect();
    anyhow::ensure!(!deleted_rows.is_empty(), "missing deleted row");
    let has_deleted_at = deleted_rows.iter().any(|row| {
        row.get("_cdsync_deleted_at")
            .map(|value| value.is_string())
            .unwrap_or(false)
    });
    assert!(has_deleted_at, "expected _cdsync_deleted_at to be set");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn e2e_polling_soft_delete_sets_deleted_at() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_poll_delete_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (id bigint primary key, name text, updated_at timestamptz default now(), deleted_at timestamptz)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, 'alpha')",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: Some(true),
            soft_delete_column: Some("deleted_at".to_string()),
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

    let http_client = reqwest::Client::new();
    support::delete_table_if_exists(&http_client, &bq_http, &project_id, &dataset, &dest_table)
        .await?;

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
        "update {} set deleted_at = now(), updated_at = now() where id = 1",
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
            mode: SyncMode::Incremental,
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

    let deleted_rows: Vec<_> = mapped
        .iter()
        .filter(|row| support::value_to_string(row.get("id").unwrap()) == Some("1".to_string()))
        .collect();
    anyhow::ensure!(!deleted_rows.is_empty(), "missing deleted row");
    let has_deleted_at = deleted_rows.iter().any(|row| {
        row.get("_cdsync_deleted_at")
            .map(|value| value.is_string())
            .unwrap_or(false)
    });
    assert!(has_deleted_at, "expected _cdsync_deleted_at to be set");
    Ok(())
}
