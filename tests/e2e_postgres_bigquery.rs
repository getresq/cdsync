use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{PostgresSource, TableSyncRequest};
use cdsync::types::TableCheckpoint;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use sqlx::postgres::PgPoolOptions;
use url::Url;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/emulator_read.rs"]
mod emulator_read_support;

#[tokio::test]
async fn e2e_postgres_bigquery_full_refresh() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_http_raw) = std::env::var("CDSYNC_E2E_BQ_HTTP")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_grpc_raw) = std::env::var("CDSYNC_E2E_BQ_GRPC")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let project_id = std::env::var("CDSYNC_E2E_BQ_PROJECT")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "cdsync".to_string());
    let dataset = std::env::var("CDSYNC_E2E_BQ_DATASET")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "cdsync_e2e".to_string());

    let bq_http = normalize_http(&bq_http_raw)?;
    let bq_grpc = normalize_grpc(&bq_grpc_raw)?;

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_e2e_{}", &suffix[..8]);
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
        "create table {} (id bigint primary key, name text, updated_at timestamptz default now())",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name) values (1, 'alpha'), (2, 'beta')",
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
        schema_changes: Some(SchemaChangePolicy::Auto),
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

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false, MetadataColumns::default()).await?;
    dest.validate().await?;

    for table in &tables {
        source
            .sync_table(TableSyncRequest {
                table,
                dest: &dest,
                checkpoint: TableCheckpoint::default(),
                state_handle: None,
                mode: SyncMode::Full,
                dry_run: false,
                default_batch_size: 1000,
                schema_diff_enabled: false,
                stats: None,
            })
            .await?;
    }

    let http_client = reqwest::Client::new();
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

    assert_eq!(mapped.len(), 2);
    let mut ids: Vec<String> = mapped
        .iter()
        .filter_map(|row| row.get("id"))
        .filter_map(emulator_read_support::value_to_string)
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
    for row in &mapped {
        let synced_at = row
            .get("_cdsync_synced_at")
            .context("missing _cdsync_synced_at")?;
        assert!(synced_at.is_string());
        let deleted_at = row
            .get("_cdsync_deleted_at")
            .context("missing _cdsync_deleted_at")?;
        assert!(deleted_at.is_null());
    }
    Ok(())
}

#[tokio::test]
async fn e2e_postgres_bigquery_custom_metadata_columns() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_http_raw) = std::env::var("CDSYNC_E2E_BQ_HTTP")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_grpc_raw) = std::env::var("CDSYNC_E2E_BQ_GRPC")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let project_id = std::env::var("CDSYNC_E2E_BQ_PROJECT")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "cdsync".to_string());
    let dataset = std::env::var("CDSYNC_E2E_BQ_DATASET")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "cdsync_e2e".to_string());

    let bq_http = normalize_http(&bq_http_raw)?;
    let bq_grpc = normalize_grpc(&bq_grpc_raw)?;
    let metadata = MetadataColumns {
        synced_at: "_synced_custom".to_string(),
        deleted_at: "_deleted_custom".to_string(),
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_meta_{}", &suffix[..8]);
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
        "create table {} (
            id bigint primary key,
            name text,
            updated_at timestamptz default now(),
            deleted_at timestamptz
        )",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, deleted_at) values (1, 'alpha', now())",
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

    let source = PostgresSource::new(pg_config, metadata.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false, metadata.clone()).await?;
    dest.validate().await?;

    for table in &tables {
        source
            .sync_table(TableSyncRequest {
                table,
                dest: &dest,
                checkpoint: TableCheckpoint::default(),
                state_handle: None,
                mode: SyncMode::Full,
                dry_run: false,
                default_batch_size: 1000,
                schema_diff_enabled: false,
                stats: None,
            })
            .await?;
    }

    let http_client = reqwest::Client::new();
    let fields = emulator_read_support::fetch_table_fields(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    assert!(fields.iter().any(|field| field == "_synced_custom"));
    assert!(fields.iter().any(|field| field == "_deleted_custom"));
    let rows = emulator_read_support::fetch_table_rows(
        &http_client,
        &bq_http,
        &project_id,
        &dataset,
        &dest_table,
    )
    .await?;
    let mapped = emulator_read_support::map_rows(&fields, rows)?;
    let row = mapped.first().context("missing row")?;
    assert!(
        row.get("_synced_custom")
            .is_some_and(serde_json::Value::is_string)
    );
    assert!(
        row.get("_deleted_custom")
            .is_some_and(serde_json::Value::is_string)
    );
    Ok(())
}

fn normalize_http(raw: &str) -> Result<String> {
    if raw.contains("://") {
        Ok(raw.to_string())
    } else {
        Ok(format!("http://{raw}"))
    }
}

fn normalize_grpc(raw: &str) -> Result<String> {
    if raw.contains("://") {
        let url = Url::parse(raw).context("invalid CDSYNC_E2E_BQ_GRPC url")?;
        let host = url.host_str().context("CDSYNC_E2E_BQ_GRPC missing host")?;
        let port = url.port().unwrap_or(default_port(url.scheme()));
        Ok(format!("{host}:{port}"))
    } else {
        Ok(raw.to_string())
    }
}

fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}
