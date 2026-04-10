use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{PostgresSource, TableSyncRequest};
use cdsync::state::ConnectionState;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use sqlx::postgres::PgPoolOptions;
use std::path::PathBuf;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/real_bigquery.rs"]
mod real_bigquery_support;

#[tokio::test]
async fn e2e_postgres_bigquery_real_heavy_sync() -> Result<()> {
    if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
        anyhow::bail!("set CDSYNC_RUN_REAL_BQ_TESTS=1 to run real BigQuery tests");
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let real_bq = real_bigquery_support::load_env()?;
    let Some(batch_load_bucket) = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        anyhow::bail!("set CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET to a writable GCS bucket");
    };
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_E2E_PG_URL for real BigQuery e2e tests")?;

    let suffix = Uuid::new_v4().simple().to_string();
    let table_name = format!("cdsync_real_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&pg_url)
        .await?;
    sqlx::query(&format!("drop table if exists {}", qualified_table))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create table {} (
            id bigint primary key,
            name text,
            status text,
            amount numeric(10,2),
            updated_at timestamptz not null default now(),
            deleted_at timestamptz
        )",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status, amount)
         select gs, concat('name-', gs), 'seed', (gs::numeric / 10.0)
         from generate_series(1, 1000) as gs",
        qualified_table
    ))
    .execute(&pool)
    .await?;

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
        cdc: Some(false),
        publication: None,
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
        cdc_apply_concurrency: None,
        cdc_batch_load_worker_count: None,
        cdc_max_fill_ms: None,
        cdc_max_pending_events: None,
        cdc_idle_timeout_seconds: None,
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
        batch_load_bucket: Some(batch_load_bucket),
        batch_load_prefix: Some(format!("cdsync-e2e-real-heavy/{}", &suffix[..8])),
        emulator_http: None,
        emulator_grpc: None,
    };

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
            default_batch_size: 200,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    let full_source = source.summarize_table(&tables[0]).await?;
    let full_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(full_source.row_count, 1000);
    assert_eq!(full_dest.row_count, 1000);
    assert_eq!(full_dest.deleted_rows, 0);

    sqlx::query(&format!(
        "update {} set
            name = concat(name, '-u'),
            status = 'updated',
            updated_at = now()
         where id between 1 and 250",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "update {} set
            deleted_at = now(),
            updated_at = now()
         where id between 251 and 400",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status, amount)
         select gs, concat('new-', gs), 'inserted', (gs::numeric / 10.0)
         from generate_series(1001, 1120) as gs",
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
        "update {} set extra = 'extra', updated_at = now() where id between 1 and 100",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "update {} set extra = 'new-extra', updated_at = now() where id between 1001 and 1050",
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
            default_batch_size: 200,
            schema_diff_enabled: false,
            stats: None,
        })
        .await?;
    state.postgres.insert(qualified_table.clone(), checkpoint);

    let final_source = source.summarize_table(&tables[0]).await?;
    let final_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(final_source.row_count, 1120);
    assert_eq!(final_dest.row_count, 1120);
    assert_eq!(final_dest.deleted_rows, 150);

    let client = real_bigquery_support::client(&real_bq.key_path).await?;
    let schema_fields = real_bigquery_support::fetch_live_table_fields(
        &client,
        &real_bq.project_id,
        &real_bq.dataset,
        &dest_table,
    )
    .await?;
    assert!(schema_fields.iter().any(|field| field == "extra"));
    assert!(
        schema_fields
            .iter()
            .any(|field| field == "_cdsync_synced_at")
    );
    assert!(
        schema_fields
            .iter()
            .any(|field| field == "_cdsync_deleted_at")
    );

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
    assert_eq!(extra_count, 150);

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
    assert_eq!(deleted_count, 150);

    Ok(())
}
