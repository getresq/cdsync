use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource};
use cdsync::state::ConnectionState;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/emulator_read.rs"]
mod emulator_read_support;

#[tokio::test]
async fn e2e_postgres_cdc_snapshot_with_row_filter() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_http) = std::env::var("CDSYNC_E2E_BQ_HTTP")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let Some(bq_grpc) = std::env::var("CDSYNC_E2E_BQ_GRPC")
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

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_cdc_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_pub_{}", &suffix[..8]);

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
        "create table {} (id bigint primary key, tenant_id int, name text)",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, tenant_id, name) values (1, 1, 'alpha'), (2, 2, 'beta'), (3, 1, 'gamma')",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "create publication {} for table {} where (tenant_id = 1)",
        publication, qualified_table
    ))
    .execute(&pool)
    .await?;

    let pipeline_id = Utc::now().timestamp_millis() as u64 % 1_000_000 + 1;
    let pg_config = PostgresConfig {
        url: pg_url,
        tables: Some(vec![PostgresTableConfig {
            name: qualified_table.clone(),
            primary_key: Some("id".to_string()),
            updated_at_column: None,
            soft_delete: Some(false),
            soft_delete_column: None,
            where_clause: Some("tenant_id = 1".to_string()),
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

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config, false, MetadataColumns::default()).await?;
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
            retry_backoff_ms: 1_000,
            snapshot_concurrency: 1,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

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
    let tenant_values: Vec<String> = mapped
        .iter()
        .filter_map(|row| row.get("tenant_id"))
        .filter_map(emulator_read_support::value_to_string)
        .collect();
    assert!(tenant_values.iter().all(|v| v == "1"));
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
