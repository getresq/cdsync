use anyhow::Result;
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource, TableSyncRequest};
use cdsync::state::ConnectionState;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/emulator_delete.rs"]
mod emulator_delete_support;
#[path = "support/emulator_read.rs"]
mod emulator_read_support;

#[tokio::test]
async fn e2e_cdc_soft_delete_sets_deleted_at() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let Some(bq_http) = std::env::var("CDSYNC_E2E_BQ_HTTP").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let Some(bq_grpc) = std::env::var("CDSYNC_E2E_BQ_GRPC").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let project_id = std::env::var("CDSYNC_E2E_BQ_PROJECT").ok().filter(|value| !value.is_empty()).unwrap_or_else(|| "cdsync".to_string());
    let dataset = std::env::var("CDSYNC_E2E_BQ_DATASET").ok().filter(|value| !value.is_empty()).unwrap_or_else(|| "cdsync_e2e".to_string());

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
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(1000),
        cdc_apply_concurrency: Some(8),
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
        batch_load_bucket: None,
        batch_load_prefix: None,
        emulator_http: Some(bq_http.clone()),
        emulator_grpc: Some(bq_grpc.clone()),
    };

    let http_client = reqwest::Client::new();
    emulator_delete_support::delete_table_if_exists(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
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
            snapshot_concurrency: 1,
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
            snapshot_concurrency: 1,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

    let fields = emulator_read_support::fetch_table_fields(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    let rows = emulator_read_support::fetch_table_rows(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    let mapped = emulator_read_support::map_rows(&fields, rows)?;

    let deleted_rows: Vec<_> = mapped
        .iter()
        .filter(|row| {
            emulator_read_support::value_to_string(row.get("id").unwrap()) == Some("1".to_string())
        })
        .collect();
    anyhow::ensure!(!deleted_rows.is_empty(), "missing deleted row");
    let has_deleted_at = deleted_rows.iter().any(|row| {
        row.get("_cdsync_deleted_at")
            .is_some_and(serde_json::Value::is_string)
    });
    assert!(has_deleted_at, "expected _cdsync_deleted_at to be set");
    Ok(())
}

#[tokio::test]
async fn e2e_polling_soft_delete_sets_deleted_at() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let Some(bq_http) = std::env::var("CDSYNC_E2E_BQ_HTTP").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let Some(bq_grpc) = std::env::var("CDSYNC_E2E_BQ_GRPC").ok().filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    let project_id = std::env::var("CDSYNC_E2E_BQ_PROJECT").ok().filter(|value| !value.is_empty()).unwrap_or_else(|| "cdsync".to_string());
    let dataset = std::env::var("CDSYNC_E2E_BQ_DATASET").ok().filter(|value| !value.is_empty()).unwrap_or_else(|| "cdsync_e2e".to_string());

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
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_apply_concurrency: None,
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
        batch_load_bucket: None,
        batch_load_prefix: None,
        emulator_http: Some(bq_http.clone()),
        emulator_grpc: Some(bq_grpc.clone()),
    };

    let http_client = reqwest::Client::new();
    emulator_delete_support::delete_table_if_exists(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
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

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
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

    let fields = emulator_read_support::fetch_table_fields(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    let rows = emulator_read_support::fetch_table_rows(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    let mapped = emulator_read_support::map_rows(&fields, rows)?;

    let deleted_rows: Vec<_> = mapped
        .iter()
        .filter(|row| {
            emulator_read_support::value_to_string(row.get("id").unwrap()) == Some("1".to_string())
        })
        .collect();
    anyhow::ensure!(!deleted_rows.is_empty(), "missing deleted row");
    let has_deleted_at = deleted_rows.iter().any(|row| {
        row.get("_cdsync_deleted_at")
            .is_some_and(serde_json::Value::is_string)
    });
    assert!(has_deleted_at, "expected _cdsync_deleted_at to be set");
    Ok(())
}
