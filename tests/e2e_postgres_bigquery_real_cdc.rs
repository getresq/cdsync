use anyhow::Result;
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource};
use cdsync::state::ConnectionState;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use chrono::Utc;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::path::PathBuf;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/real_bigquery.rs"]
mod real_bigquery_support;

#[tokio::test]
#[ignore]
async fn e2e_postgres_bigquery_real_cdc_heavy_sync() -> Result<()> {
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();

    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .unwrap_or_else(|_| "postgres://cdsync:cdsync@localhost:5433/cdsync".to_string());
    let real_bq = real_bigquery_support::load_env()?;

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_real_cdc_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_real_pub_{}", &suffix[..8]);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!("drop publication if exists {}", publication))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (
            id bigint primary key,
            name text,
            status text,
            updated_at timestamptz not null default now(),
            deleted_at timestamptz
        )",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "alter table {} replica identity full",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status)
         select gs, concat('name-', gs), 'seed'
         from generate_series(1, 1000) as gs",
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

    let pipeline_id = Utc::now().timestamp_millis() as u64 % 1_000_000 + 10_000;
    let pg_config = PostgresConfig {
        url: pg_url.clone(),
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
        batch_size: Some(200),
        cdc: Some(true),
        publication: Some(publication.clone()),
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(200),
        cdc_max_fill_ms: Some(2000),
        cdc_max_pending_events: Some(20_000),
        cdc_idle_timeout_seconds: Some(1),
        cdc_tls: None,
        cdc_tls_ca_path: None,
        cdc_tls_ca: None,
    };

    let bq_config = BigQueryConfig {
        project_id: real_bq.project_id.clone(),
        dataset: real_bq.dataset.clone(),
        location: Some(real_bq.location.clone()),
        service_account_key_path: Some(PathBuf::from(&real_bq.key_path)),
        service_account_key: None,
        partition_by_synced_at: Some(false),
        storage_write_enabled: Some(true),
        batch_load_bucket: None,
        batch_load_prefix: None,
        emulator_http: None,
        emulator_grpc: None,
    };

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
            default_batch_size: 200,
            snapshot_concurrency: 1,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

    let initial_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(initial_dest.row_count, 1000);
    assert_eq!(initial_dest.deleted_rows, 0);

    sqlx::query(&format!(
        "update {} set status = 'updated', updated_at = now() where id between 1 and 220",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "delete from {} where id between 221 and 340",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status)
         select gs, concat('new-', gs), 'inserted'
         from generate_series(1001, 1160) as gs",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "alter table {} add column extra text",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "update {} set extra = 'extra', updated_at = now() where id between 1 and 80",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "update {} set extra = 'new-extra', updated_at = now() where id between 1001 and 1040",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    source
        .sync_cdc(CdcSyncRequest {
            dest: &dest,
            state: &mut state,
            state_handle: None,
            mode: SyncMode::Incremental,
            dry_run: false,
            follow: false,
            default_batch_size: 200,
            snapshot_concurrency: 1,
            tables: &tables,
            schema_diff_enabled: false,
            stats: None,
            shutdown: None,
        })
        .await?;

    let final_source = source.summarize_table(&tables[0]).await?;
    let final_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(final_source.row_count, 1040);
    assert_eq!(final_dest.row_count, 1160);
    assert_eq!(final_dest.deleted_rows, 120);

    let client = real_bigquery_support::client(&real_bq.key_path).await?;
    let schema_fields = real_bigquery_support::fetch_live_table_fields(
        &client,
        &real_bq.project_id,
        &real_bq.dataset,
        &dest_table,
    )
    .await?;
    assert!(schema_fields.iter().any(|field| field == "extra"));

    let extra_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where extra is not null",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(extra_count, 120);

    let deleted_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where _cdsync_deleted_at is not null",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(deleted_count, 120);

    Ok(())
}
