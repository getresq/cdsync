use anyhow::{Context, Result};
use cdsync::config::{
    BigQueryConfig, CdcAckBoundary, PostgresConfig, PostgresTableConfig, SchemaChangePolicy,
    StateConfig, StatsConfig,
};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::retry::ConnectionLockError;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource};
use cdsync::state::{
    CdcBatchLoadQueueSummary, ConnectionState, StateHandle, SyncState, SyncStateStore,
};
use cdsync::stats::StatsDb;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use etl_postgres::replication::slots::APPLY_WORKER_PREFIX;
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{Instant, sleep};
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/real_bigquery.rs"]
mod real_bigquery_support;

struct FollowRunnerConfigInput<'a> {
    connection_id: &'a str,
    pg_url: &'a str,
    project_id: &'a str,
    dataset: &'a str,
    location: &'a str,
    key_path: &'a str,
    state_schema: &'a str,
    stats_schema: &'a str,
    publication: &'a str,
    pipeline_id: u64,
    idle_timeout_seconds: u64,
    reducer_max_jobs: Option<usize>,
    reducer_max_fill_ms: Option<u64>,
    ack_boundary: Option<CdcAckBoundary>,
    tables: &'a [PostgresTableConfig],
    bq_config: &'a BigQueryConfig,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct DurableQueueJobSnapshot {
    job_id: String,
    first_sequence: i64,
    max_sequence: i64,
    status: String,
    stage: Option<String>,
    total_fragments: i64,
    durable_fragments: i64,
    created_at: i64,
}

struct ChildTerminationGuard {
    pid: Option<u32>,
}

impl ChildTerminationGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for ChildTerminationGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid.take() {
            let _ = std::process::Command::new("kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .status();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_postgres_bigquery_real_cdc_heavy_sync() -> Result<()> {
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
    let table_name = format!("cdsync_real_cdc_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_real_pub_{}", &suffix[..8]);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&pg_url)
        .await?;
    cleanup_stale_apply_slots(&pool).await?;
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
         from generate_series(1, 300) as gs",
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

    let pipeline_id = 10_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
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
        cdc_apply_concurrency: Some(8),
        cdc_batch_load_worker_count: Some(8),
        cdc_batch_load_staging_worker_count: None,
        cdc_batch_load_reducer_worker_count: None,
        cdc_max_inflight_commits: None,
        cdc_batch_load_reducer_max_jobs: None,
        cdc_batch_load_reducer_max_fill_ms: None,
        cdc_batch_load_reducer_enabled: None,
        cdc_ack_boundary: None,
        cdc_backlog_max_pending_fragments: None,
        cdc_backlog_max_oldest_pending_seconds: None,
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
        batch_load_bucket: Some(batch_load_bucket),
        batch_load_prefix: Some(format!("cdsync-e2e-real-cdc/{}", &suffix[..8])),
        emulator_http: None,
        emulator_grpc: None,
    };

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: None,
        mode: SyncMode::Full,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;

    let initial_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(initial_dest.row_count, 300);
    assert_eq!(initial_dest.deleted_rows, 0);

    sqlx::query(&format!(
        "update {} set status = 'updated', updated_at = now() where id between 1 and 80",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "delete from {} where id between 81 and 120",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status)
         select gs, concat('new-', gs), 'inserted'
         from generate_series(301, 360) as gs",
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
        "update {} set extra = 'extra', updated_at = now() where id between 1 and 30",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "update {} set extra = 'new-extra', updated_at = now() where id between 301 and 320",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: None,
        mode: SyncMode::Incremental,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;

    let final_source = source.summarize_table(&tables[0]).await?;
    let final_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(final_source.row_count, 320);
    assert_eq!(final_dest.row_count, 360);
    assert_eq!(final_dest.deleted_rows, 40);

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
    assert_eq!(extra_count, 50);

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
    assert_eq!(deleted_count, 40);

    drop(client);
    drop(dest);
    drop(source);
    pool.close().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_postgres_bigquery_real_cdc_durable_enqueue_materializes_from_queue() -> Result<()> {
    if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
        anyhow::bail!("set CDSYNC_RUN_REAL_BQ_TESTS=1 to run real BigQuery tests");
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let real_bq = real_bigquery_support::load_env()?;
    let batch_load_bucket = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET to a writable GCS bucket")?;
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_E2E_PG_URL for real BigQuery e2e tests")?;

    let suffix = Uuid::new_v4().simple().to_string();
    let short_suffix = &suffix[..8];
    let table_name = format!("cdsync_real_durable_{short_suffix}");
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_real_durable_pub_{short_suffix}");
    let state_schema = format!("cdsync_state_real_durable_{short_suffix}");
    let connection_id = format!("real_durable_{short_suffix}");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&pg_url)
        .await?;
    cleanup_stale_apply_slots(&pool).await?;
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
            updated_at timestamptz not null default now()
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
         select gs, concat('seed-', gs), 'seed'
         from generate_series(1, 40) as gs",
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

    let state_config = StateConfig {
        url: pg_url.clone(),
        schema: Some(state_schema),
    };
    SyncStateStore::migrate_with_config(&state_config, 16).await?;
    let state_store = SyncStateStore::open_with_config(&state_config, 16).await?;
    let state_handle = state_store.handle(&connection_id);

    let pipeline_id = 30_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let pg_config = PostgresConfig {
        url: pg_url.clone(),
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
        batch_size: Some(200),
        cdc: Some(true),
        publication: Some(publication.clone()),
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(200),
        cdc_apply_concurrency: Some(2),
        cdc_batch_load_worker_count: Some(2),
        cdc_batch_load_staging_worker_count: None,
        cdc_batch_load_reducer_worker_count: None,
        cdc_max_inflight_commits: None,
        cdc_batch_load_reducer_max_jobs: Some(4),
        cdc_batch_load_reducer_max_fill_ms: Some(30_000),
        cdc_batch_load_reducer_enabled: None,
        cdc_ack_boundary: Some(CdcAckBoundary::DurableEnqueue),
        cdc_backlog_max_pending_fragments: Some(10_000),
        cdc_backlog_max_oldest_pending_seconds: Some(300),
        cdc_max_fill_ms: Some(1000),
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
        batch_load_bucket: Some(batch_load_bucket),
        batch_load_prefix: Some(format!("cdsync-e2e-real-durable/{short_suffix}")),
        emulator_http: None,
        emulator_grpc: None,
    };

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: Some(state_handle.clone()),
        mode: SyncMode::Full,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;

    let initial_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(initial_dest.row_count, 40);

    sqlx::query(&format!(
        "update {} set status = 'updated', updated_at = now() where id between 1 and 12",
        qualified_table
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status)
         select gs, concat('new-', gs), 'inserted'
         from generate_series(41, 55) as gs",
        qualified_table
    ))
    .execute(&pool)
    .await?;

    let source = PostgresSource::new(pg_config, MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: Some(state_handle.clone()),
        mode: SyncMode::Incremental,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;

    let pre_apply_summary = wait_for_durable_frontier_before_queue_drain(
        &state_store,
        &state_handle,
        &connection_id,
        Duration::from_secs(20),
    )
    .await?;
    assert!(
        pre_apply_summary.succeeded_jobs < pre_apply_summary.total_jobs,
        "durable_enqueue must expose an ACK frontier before target materialization finishes"
    );

    let queue_summary =
        wait_for_batch_load_queue_drained(&state_store, &connection_id, Duration::from_secs(180))
            .await?;
    assert!(queue_summary.total_jobs > 0);
    assert_eq!(queue_summary.failed_jobs, 0);

    let final_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(final_dest.row_count, 55);

    let client = real_bigquery_support::client(&real_bq.key_path).await?;
    let updated_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where status = 'updated'",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(updated_count, 12);

    let inserted_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where status = 'inserted'",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(inserted_count, 15);

    drop(client);
    drop(dest);
    drop(source);
    drop(state_store);
    pool.close().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_postgres_bigquery_real_cdc_durable_enqueue_replays_after_restart() -> Result<()> {
    if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
        anyhow::bail!("set CDSYNC_RUN_REAL_BQ_TESTS=1 to run real BigQuery tests");
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let real_bq = real_bigquery_support::load_env()?;
    let batch_load_bucket = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET to a writable GCS bucket")?;
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_E2E_PG_URL for real BigQuery e2e tests")?;

    let suffix = Uuid::new_v4().simple().to_string();
    let short_suffix = &suffix[..8];
    let table_name = format!("cdsync_real_durable_restart_{short_suffix}");
    let qualified_table = format!("public.{table_name}");
    let dest_table = destination_table_name(&qualified_table);
    let publication = format!("cdsync_real_durable_restart_pub_{short_suffix}");
    let state_schema = format!("cdsync_state_real_durable_restart_{short_suffix}");
    let stats_schema = format!("cdsync_stats_real_durable_restart_{short_suffix}");
    let connection_id = format!("runner_real_durable_restart_{short_suffix}");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&pg_url)
        .await?;
    cleanup_stale_apply_slots(&pool).await?;
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
            updated_at timestamptz not null default now()
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
         select gs, concat('seed-', gs), 'seed'
         from generate_series(1, 30) as gs",
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

    prepare_runner_state(&pg_url, &state_schema, &stats_schema).await?;
    let state_config = StateConfig {
        url: pg_url.clone(),
        schema: Some(state_schema.clone()),
    };
    let state_store = SyncStateStore::open_with_config(&state_config, 16).await?;
    let state_handle = state_store.handle(&connection_id);

    let pipeline_id = 40_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let table_config = PostgresTableConfig {
        name: qualified_table.clone(),
        primary_key: Some("id".to_string()),
        updated_at_column: Some("updated_at".to_string()),
        soft_delete: Some(false),
        soft_delete_column: None,
        where_clause: None,
        columns: None,
    };
    let pg_config = PostgresConfig {
        url: pg_url.clone(),
        tables: Some(vec![table_config.clone()]),
        table_selection: None,
        batch_size: Some(200),
        cdc: Some(true),
        publication: Some(publication.clone()),
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(200),
        cdc_apply_concurrency: Some(2),
        cdc_batch_load_worker_count: Some(2),
        cdc_batch_load_staging_worker_count: None,
        cdc_batch_load_reducer_worker_count: None,
        cdc_max_inflight_commits: None,
        cdc_batch_load_reducer_max_jobs: Some(4),
        cdc_batch_load_reducer_max_fill_ms: Some(30_000),
        cdc_batch_load_reducer_enabled: None,
        cdc_ack_boundary: Some(CdcAckBoundary::DurableEnqueue),
        cdc_backlog_max_pending_fragments: Some(10_000),
        cdc_backlog_max_oldest_pending_seconds: Some(300),
        cdc_max_fill_ms: Some(1000),
        cdc_max_pending_events: Some(20_000),
        cdc_idle_timeout_seconds: Some(60),
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
        batch_load_prefix: Some(format!("cdsync-e2e-real-durable-restart/{short_suffix}")),
        emulator_http: None,
        emulator_grpc: None,
    };

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: Some(state_handle.clone()),
        mode: SyncMode::Full,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;
    state_handle.save_connection_state(&state).await?;
    assert_eq!(dest.summarize_table(&dest_table).await?.row_count, 30);

    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "update {} set status = 'updated', updated_at = now() where id between 1 and 8",
        qualified_table
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "insert into {} (id, name, status)
         select gs, concat('restart-', gs), 'inserted'
         from generate_series(31, 36) as gs",
        qualified_table
    ))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("config.yaml");
    let first_log_path = temp_dir.path().join("runner-first.log");
    let second_log_path = temp_dir.path().join("runner-second.log");
    tokio::fs::write(
        &config_path,
        build_follow_runner_config(FollowRunnerConfigInput {
            connection_id: &connection_id,
            pg_url: &pg_url,
            project_id: &real_bq.project_id,
            dataset: &real_bq.dataset,
            location: &real_bq.location,
            key_path: real_bq.key_path.to_str().unwrap_or_default(),
            state_schema: &state_schema,
            stats_schema: &stats_schema,
            publication: &publication,
            pipeline_id,
            idle_timeout_seconds: 60,
            reducer_max_jobs: Some(4),
            reducer_max_fill_ms: Some(30_000),
            ack_boundary: Some(CdcAckBoundary::DurableEnqueue),
            tables: std::slice::from_ref(&table_config),
            bq_config: &bq_config,
        }),
    )
    .await?;

    cleanup_stale_follow_runners(&connection_id).await?;
    let mut first_child = runner_process_with_logs(&config_path, &connection_id, &first_log_path)?;
    let first_pid = first_child
        .id()
        .ok_or_else(|| anyhow::anyhow!("runner pid"))?;
    let mut first_guard = ChildTerminationGuard::new(first_pid);
    sleep(Duration::from_secs(2)).await;
    ensure_child_still_running(
        &mut first_child,
        &first_log_path,
        "first durable runner startup",
    )
    .await?;

    let (pre_apply_summary, pre_restart_jobs) = match wait_for_durable_cdc_jobs_before_queue_drain(
        &pool,
        &state_schema,
        &state_store,
        &state_handle,
        &connection_id,
        Duration::from_secs(30),
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            anyhow::bail!(
                "durable_enqueue restart test did not observe pre-apply ACK: {}\n\nLast logs:\n{}",
                err,
                tail_log(&first_log_path, 200).await
            );
        }
    };
    assert!(
        pre_apply_summary.succeeded_jobs < pre_apply_summary.total_jobs,
        "durable enqueue ACK should be visible before target materialization"
    );
    assert!(
        pre_restart_jobs.iter().any(|job| job.status != "succeeded"),
        "at least one pre-crash durable job should still need materialization"
    );
    ensure_child_still_running(
        &mut first_child,
        &first_log_path,
        "first durable runner pre-crash",
    )
    .await?;
    kill_child_forcefully(&mut first_child, first_pid, &first_log_path).await?;
    first_guard.disarm();
    wait_for_connection_lock_reclaim(&state_store, &connection_id, Duration::from_secs(75)).await?;

    assert_eq!(
        dest.summarize_table(&dest_table).await?.row_count,
        30,
        "target table should not materialize queued durable work before restart"
    );

    let mut second_child =
        runner_process_with_logs(&config_path, &connection_id, &second_log_path)?;
    let second_pid = second_child
        .id()
        .ok_or_else(|| anyhow::anyhow!("runner pid"))?;
    let mut second_guard = ChildTerminationGuard::new(second_pid);
    sleep(Duration::from_secs(2)).await;
    ensure_child_still_running(
        &mut second_child,
        &second_log_path,
        "second durable runner startup",
    )
    .await?;

    let queue_summary = match wait_for_batch_load_queue_drained(
        &state_store,
        &connection_id,
        Duration::from_secs(240),
    )
    .await
    {
        Ok(summary) => summary,
        Err(err) => {
            anyhow::bail!(
                "durable queued work did not drain after restart: {}\n\nLast logs:\n{}",
                err,
                tail_log(&second_log_path, 200).await
            );
        }
    };
    assert!(queue_summary.total_jobs > 0);
    assert_eq!(queue_summary.failed_jobs, 0);
    let post_restart_jobs =
        load_durable_queue_job_snapshots(&pool, &state_schema, &connection_id).await?;
    let pre_restart_identities = durable_queue_job_identities(&pre_restart_jobs);
    let post_restart_identities = durable_queue_job_identities(&post_restart_jobs);
    for identity in &pre_restart_identities {
        assert!(
            post_restart_identities.contains(identity),
            "restart should preserve and drain pre-crash durable queue row {identity:?}, not replace it via WAL replay; post_restart_jobs={post_restart_jobs:?}"
        );
    }
    assert!(
        post_restart_jobs
            .iter()
            .all(|job| job.status == "succeeded" && job.stage.as_deref() == Some("applied")),
        "all durable queue jobs should be applied after restart: {post_restart_jobs:?}"
    );
    terminate_and_wait_child(&mut second_child, second_pid, &second_log_path).await?;
    second_guard.disarm();

    let final_dest = dest.summarize_table(&dest_table).await?;
    assert_eq!(final_dest.row_count, 36);

    let client = real_bigquery_support::client(&real_bq.key_path).await?;
    let updated_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where status = 'updated'",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(updated_count, 8);
    let inserted_count = real_bigquery_support::query_i64(
        &client,
        &real_bq.project_id,
        &real_bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where status = 'inserted'",
            project = real_bq.project_id,
            dataset = real_bq.dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(inserted_count, 6);

    drop(client);
    drop(dest);
    drop(source);
    drop(state_store);
    pool.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_postgres_bigquery_real_cdc_follow_batch_load_relation_stress() -> Result<()> {
    if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
        anyhow::bail!("set CDSYNC_RUN_REAL_BQ_TESTS=1 to run real BigQuery tests");
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let real_bq = real_bigquery_support::load_env()?;
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_E2E_PG_URL for real BigQuery e2e tests")?;
    let batch_load_bucket = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
        .context("set CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET to a writable GCS bucket")?;

    let suffix = Uuid::new_v4().simple().to_string();
    let publication = format!("cdsync_real_follow_pub_{}", &suffix[..8]);
    let state_schema = format!("cdsync_state_real_follow_{}", &suffix[..8]);
    let stats_schema = format!("cdsync_stats_real_follow_{}", &suffix[..8]);
    let connection_id = "runner_real_batchload_demo";
    let table_count = env::var("CDSYNC_REAL_STRESS_TABLE_COUNT")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(2usize);
    let rows_per_table = env::var("CDSYNC_REAL_STRESS_ROWS_PER_TABLE")
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(2i64);
    let relation_rounds = env::var("CDSYNC_REAL_STRESS_RELATION_ROUNDS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(1usize);

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&pg_url)
        .await?;
    cleanup_stale_apply_slots(&pool).await?;

    let mut table_names = Vec::with_capacity(table_count);
    let mut table_configs = Vec::with_capacity(table_count);
    for idx in 0..table_count {
        let table_name = format!("cdsync_real_follow_{}_{}", &suffix[..8], idx);
        let qualified_table = format!("public.{table_name}");
        table_names.push(qualified_table.clone());

        sqlx::query(&format!("drop table if exists {}", qualified_table))
            .execute(&pool)
            .await?;
        sqlx::query(&format!(
            "create table {} (
                id bigint primary key,
                payload text,
                updated_at timestamptz not null default now()
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
            "insert into {} (id, payload)
             select gs, concat('seed-', gs)
             from generate_series(1, {}) as gs",
            qualified_table, rows_per_table
        ))
        .execute(&pool)
        .await?;

        table_configs.push(PostgresTableConfig {
            name: qualified_table,
            primary_key: Some("id".to_string()),
            updated_at_column: Some("updated_at".to_string()),
            soft_delete: Some(false),
            soft_delete_column: None,
            where_clause: None,
            columns: None,
        });
    }

    sqlx::query(&format!("drop publication if exists {}", publication))
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "create publication {} for table {}",
        publication,
        table_names.join(", ")
    ))
    .execute(&pool)
    .await?;

    prepare_runner_state(&pg_url, &state_schema, &stats_schema).await?;

    let pipeline_id = 20_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let runner_idle_timeout_seconds = 30;
    let pg_config = PostgresConfig {
        url: pg_url.clone(),
        tables: Some(table_configs.clone()),
        table_selection: None,
        batch_size: Some(200),
        cdc: Some(true),
        publication: Some(publication.clone()),
        publication_mode: None,
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: Some(pipeline_id),
        cdc_batch_size: Some(200),
        cdc_apply_concurrency: Some(2),
        cdc_batch_load_worker_count: Some(2),
        cdc_batch_load_staging_worker_count: None,
        cdc_batch_load_reducer_worker_count: None,
        cdc_max_inflight_commits: None,
        cdc_batch_load_reducer_max_jobs: None,
        cdc_batch_load_reducer_max_fill_ms: None,
        cdc_batch_load_reducer_enabled: None,
        cdc_ack_boundary: None,
        cdc_backlog_max_pending_fragments: None,
        cdc_backlog_max_oldest_pending_seconds: None,
        cdc_max_fill_ms: Some(1000),
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
        batch_load_bucket: Some(batch_load_bucket),
        batch_load_prefix: Some(format!("cdsync-e2e-real-follow/{}", &suffix[..8])),
        emulator_http: None,
        emulator_grpc: None,
    };

    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(bq_config.clone(), false, MetadataColumns::default()).await?;
    dest.validate().await?;

    let mut state = ConnectionState::default();
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state: &mut state,
        state_handle: None,
        mode: SyncMode::Full,
        dry_run: false,
        follow: false,
        default_batch_size: 200,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 4,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await?;

    let state_store = SyncStateStore::open_with_config(
        &StateConfig {
            url: pg_url.clone(),
            schema: Some(state_schema.clone()),
        },
        16,
    )
    .await?;
    state_store
        .handle(connection_id)
        .save_connection_state(&state)
        .await?;
    let baseline_lsn = state
        .postgres_cdc
        .as_ref()
        .and_then(|cdc| cdc.last_lsn.clone())
        .ok_or_else(|| anyhow::anyhow!("missing baseline CDC LSN after initial sync"))?;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("config.yaml");
    let log_path = temp_dir.path().join("runner.log");
    tokio::fs::write(
        &config_path,
        build_follow_runner_config(FollowRunnerConfigInput {
            connection_id,
            pg_url: &pg_url,
            project_id: &real_bq.project_id,
            dataset: &real_bq.dataset,
            location: &real_bq.location,
            key_path: real_bq.key_path.to_str().unwrap_or_default(),
            state_schema: &state_schema,
            stats_schema: &stats_schema,
            publication: &publication,
            pipeline_id,
            idle_timeout_seconds: runner_idle_timeout_seconds,
            reducer_max_jobs: None,
            reducer_max_fill_ms: None,
            ack_boundary: None,
            tables: &table_configs,
            bq_config: &bq_config,
        }),
    )
    .await?;

    cleanup_stale_follow_runners(connection_id).await?;
    let mut child = runner_process_with_logs(&config_path, connection_id, &log_path)?;
    let child_pid = child.id().ok_or_else(|| anyhow::anyhow!("runner pid"))?;
    let mut child_guard = ChildTerminationGuard::new(child_pid);
    sleep(Duration::from_secs(2)).await;
    ensure_child_still_running(&mut child, &log_path, "follow runner startup").await?;

    for round in 0..relation_rounds {
        for (idx, table_name) in table_names.iter().enumerate() {
            let column_name = format!("stress_col_{}", round);
            sqlx::query(&format!(
                "alter table {} add column if not exists {} text",
                table_name, column_name
            ))
            .execute(&pool)
            .await?;
            sqlx::query(&format!(
                "update {} set {} = $1, payload = concat(payload, '-r{}'), updated_at = now() where id = $2",
                table_name, column_name, round
            ))
            .bind(format!("value-{}-{}", round, idx))
            .bind(((idx as i64) % rows_per_table) + 1)
            .execute(&pool)
            .await?;
        }
    }

    let advanced_lsn = wait_for_lsn_advance(
        &StateConfig {
            url: pg_url.clone(),
            schema: Some(state_schema.clone()),
        },
        connection_id,
        &baseline_lsn,
        Duration::from_secs(180),
    )
    .await;

    terminate_and_wait_child(&mut child, child_pid, &log_path).await?;
    child_guard.disarm();

    let advanced_lsn = match advanced_lsn {
        Ok(lsn) => lsn,
        Err(err) => {
            let log_tail = tail_log(&log_path, 200).await;
            anyhow::bail!(
                "follow-mode batch-load relation stress did not advance LSN from {}: {}\n\nLast logs:\n{}",
                baseline_lsn,
                err,
                log_tail
            );
        }
    };

    assert_ne!(advanced_lsn, baseline_lsn);
    drop(state_store);
    drop(dest);
    drop(source);
    pool.close().await;
    Ok(())
}

fn build_follow_runner_config(input: FollowRunnerConfigInput<'_>) -> String {
    let table_yaml = input
        .tables
        .iter()
        .map(|table| {
            format!(
                "        - name: \"{}\"\n          primary_key: \"{}\"\n          updated_at_column: \"updated_at\"",
                table.name,
                table.primary_key.as_deref().unwrap_or("id")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let mut cdc_extra_yaml = String::new();
    if let Some(max_jobs) = input.reducer_max_jobs {
        cdc_extra_yaml.push_str(&format!(
            "      cdc_batch_load_reducer_max_jobs: {max_jobs}\n"
        ));
    }
    if let Some(max_fill_ms) = input.reducer_max_fill_ms {
        cdc_extra_yaml.push_str(&format!(
            "      cdc_batch_load_reducer_max_fill_ms: {max_fill_ms}\n"
        ));
    }
    if let Some(ack_boundary) = input.ack_boundary {
        let value = match ack_boundary {
            CdcAckBoundary::TargetApply => "target_apply",
            CdcAckBoundary::DurableEnqueue => "durable_enqueue",
        };
        cdc_extra_yaml.push_str(&format!("      cdc_ack_boundary: {value}\n"));
        if ack_boundary == CdcAckBoundary::DurableEnqueue {
            cdc_extra_yaml.push_str("      cdc_backlog_max_pending_fragments: 10000\n");
            cdc_extra_yaml.push_str("      cdc_backlog_max_oldest_pending_seconds: 300\n");
        }
    }
    format!(
        r#"
state:
  url: "{pg_url}"
  schema: "{state_schema}"
logging:
  level: "info"
  json: false
observability:
  service_name: "cdsync"
sync:
  default_batch_size: 200
  max_retries: 3
  retry_backoff_ms: 1000
  max_concurrency: 1
stats:
  url: "{pg_url}"
  schema: "{stats_schema}"
connections:
  - id: "{connection_id}"
    enabled: true
    source:
      type: postgres
      url: "{pg_url}"
      cdc: true
      publication: "{publication}"
      cdc_pipeline_id: {pipeline_id}
      cdc_batch_size: 200
      cdc_apply_concurrency: 2
      cdc_batch_load_worker_count: 2
{cdc_extra_yaml}
      cdc_max_fill_ms: 1000
      cdc_idle_timeout_seconds: {idle_timeout_seconds}
      schema_changes: auto
      tables:
{table_yaml}
    destination:
      type: bigquery
      project_id: "{project_id}"
      dataset: "{dataset}"
      location: "{location}"
      service_account_key_path: "{key_path}"
      batch_load_bucket: "{batch_load_bucket}"
      batch_load_prefix: "{batch_load_prefix}"
"#,
        connection_id = input.connection_id,
        pg_url = input.pg_url,
        state_schema = input.state_schema,
        stats_schema = input.stats_schema,
        publication = input.publication,
        pipeline_id = input.pipeline_id,
        idle_timeout_seconds = input.idle_timeout_seconds,
        cdc_extra_yaml = cdc_extra_yaml,
        project_id = input.project_id,
        dataset = input.dataset,
        location = input.location,
        key_path = input.key_path,
        table_yaml = table_yaml,
        batch_load_bucket = input
            .bq_config
            .batch_load_bucket
            .as_deref()
            .unwrap_or_default(),
        batch_load_prefix = input
            .bq_config
            .batch_load_prefix
            .as_deref()
            .unwrap_or_default(),
    )
}

fn runner_process_with_logs(
    config_path: &PathBuf,
    connection_id: &str,
    log_path: &PathBuf,
) -> Result<Child> {
    let log_file = File::create(log_path)?;
    let stderr_file = log_file.try_clone()?;
    Ok(Command::new(env!("CARGO_BIN_EXE_cdsync"))
        .arg("run")
        .arg("--config")
        .arg(config_path)
        .arg("--connection")
        .arg(connection_id)
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?)
}

async fn prepare_runner_state(pg_url: &str, state_schema: &str, stats_schema: &str) -> Result<()> {
    SyncStateStore::migrate_with_config(
        &StateConfig {
            url: pg_url.to_string(),
            schema: Some(state_schema.to_string()),
        },
        16,
    )
    .await?;
    StatsDb::migrate_with_config(
        &StatsConfig {
            url: Some(pg_url.to_string()),
            schema: Some(stats_schema.to_string()),
        },
        pg_url,
    )
    .await?;
    Ok(())
}

async fn terminate_child(pid: u32) -> Result<()> {
    let status: std::process::ExitStatus = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "failed to send SIGTERM");
    Ok(())
}

async fn kill_child_forcefully(child: &mut Child, pid: u32, log_path: &PathBuf) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        anyhow::bail!(
            "runner exited before crash simulation with {status}\n\nLast logs:\n{}",
            tail_log(log_path, 200).await
        );
    }
    let status: std::process::ExitStatus = Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "failed to send SIGKILL");
    let _ = child.wait().await?;
    Ok(())
}

async fn wait_for_connection_lock_reclaim(
    state_store: &SyncStateStore,
    connection_id: &str,
    timeout_after: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        match state_store.acquire_connection_lock(connection_id).await {
            Ok(lease) => {
                lease.release().await?;
                return Ok(());
            }
            Err(err) if err.downcast_ref::<ConnectionLockError>().is_some() => {
                if start.elapsed() >= timeout_after {
                    anyhow::bail!(
                        "timed out waiting for crashed runner connection lock to become stale"
                    );
                }
                sleep(Duration::from_secs(1)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

async fn load_durable_queue_job_snapshots(
    pool: &PgPool,
    state_schema: &str,
    connection_id: &str,
) -> Result<Vec<DurableQueueJobSnapshot>> {
    let rows = sqlx::query(&format!(
        r#"
        select
            jobs.job_id,
            jobs.first_sequence,
            coalesce(max(fragments.sequence), jobs.first_sequence) as max_sequence,
            jobs.status,
            jobs.stage,
            count(fragments.fragment_id) as total_fragments,
            count(fragments.fragment_id) filter (
                where fragments.status = 'succeeded'
                  and fragments.stage in ('apply_pending', 'applied')
            ) as durable_fragments,
            jobs.created_at
        from {schema}.cdc_batch_load_jobs jobs
        left join {schema}.cdc_commit_fragments fragments
          on fragments.connection_id = jobs.connection_id
         and fragments.job_id = jobs.job_id
        where jobs.connection_id = $1
        group by jobs.job_id, jobs.first_sequence, jobs.status, jobs.stage, jobs.created_at
        order by jobs.job_id
        "#,
        schema = state_schema
    ))
    .bind(connection_id)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(DurableQueueJobSnapshot {
                job_id: row.try_get("job_id")?,
                first_sequence: row.try_get("first_sequence")?,
                max_sequence: row.try_get("max_sequence")?,
                status: row.try_get("status")?,
                stage: row.try_get("stage")?,
                total_fragments: row.try_get("total_fragments")?,
                durable_fragments: row.try_get("durable_fragments")?,
                created_at: row.try_get("created_at")?,
            })
        })
        .collect()
}

fn durable_queue_job_identities(jobs: &[DurableQueueJobSnapshot]) -> Vec<(String, i64, i64)> {
    jobs.iter()
        .map(|job| (job.job_id.clone(), job.max_sequence, job.created_at))
        .collect()
}

async fn terminate_and_wait_child(child: &mut Child, pid: u32, log_path: &PathBuf) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        anyhow::ensure!(
            status.success(),
            "follow runner exited before teardown with {status}\n\nLast logs:\n{}",
            tail_log(log_path, 200).await
        );
        return Ok(());
    }
    terminate_child(pid).await?;
    match tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
        Ok(wait_result) => {
            let _ = wait_result?;
            Ok(())
        }
        Err(_) => {
            let kill_status = Command::new("kill")
                .arg("-KILL")
                .arg(pid.to_string())
                .status()
                .await?;
            anyhow::ensure!(kill_status.success(), "failed to send SIGKILL");
            let _ = child.wait().await?;
            Ok(())
        }
    }
}

async fn cleanup_stale_follow_runners(connection_id: &str) -> Result<()> {
    let output = Command::new("pgrep")
        .arg("-f")
        .arg(format!("--connection {}", connection_id))
        .output()
        .await?;
    if !output.status.success() {
        return Ok(());
    }

    for pid in String::from_utf8_lossy(&output.stdout).lines() {
        let pid = pid.trim();
        if pid.is_empty() {
            continue;
        }
        let status = Command::new("kill").arg("-TERM").arg(pid).status().await?;
        anyhow::ensure!(
            status.success(),
            "failed to terminate stale runner pid {pid}"
        );
    }
    sleep(Duration::from_secs(1)).await;
    Ok(())
}

async fn ensure_child_still_running(
    child: &mut Child,
    log_path: &PathBuf,
    context: &str,
) -> Result<()> {
    if let Some(status) = child.try_wait()? {
        anyhow::bail!(
            "{context} exited early with {status}\n\nLast logs:\n{}",
            tail_log(log_path, 200).await
        );
    }
    Ok(())
}

async fn wait_for_lsn_advance(
    state_config: &StateConfig,
    connection_id: &str,
    baseline_lsn: &str,
    timeout_after: Duration,
) -> Result<String> {
    let start = Instant::now();
    loop {
        let state = SyncState::load_with_config_and_pool(state_config, 16).await?;
        if let Some(current_lsn) = state
            .connections
            .get(connection_id)
            .and_then(|connection| connection.postgres_cdc.as_ref())
            .and_then(|cdc| cdc.last_lsn.clone())
            && current_lsn != baseline_lsn
        {
            return Ok(current_lsn);
        }
        if start.elapsed() >= timeout_after {
            anyhow::bail!("timed out waiting for CDC LSN advance");
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn wait_for_durable_frontier_before_queue_drain(
    state_store: &SyncStateStore,
    state_handle: &StateHandle,
    connection_id: &str,
    timeout_after: Duration,
) -> Result<CdcBatchLoadQueueSummary> {
    let start = Instant::now();
    loop {
        let frontier = state_handle.load_cdc_durable_apply_frontier(1, 100).await?;
        let feedback = state_handle.load_cdc_feedback_state().await?;
        let summary = state_store
            .load_cdc_batch_load_queue_summary(connection_id)
            .await?;
        if summary.failed_jobs > 0 {
            anyhow::bail!(
                "CDC batch-load queue failed before durable_enqueue boundary could be observed: failed={} latest_error={:?}",
                summary.failed_jobs,
                summary.latest_failed_error
            );
        }
        let durable_next_sequence = frontier
            .as_ref()
            .map_or(1, |frontier| frontier.next_sequence_to_ack);
        let feedback_next_sequence = feedback
            .as_ref()
            .map_or(1, |feedback| feedback.next_sequence_to_ack);
        if durable_next_sequence > 1
            && feedback_next_sequence >= durable_next_sequence
            && summary.total_jobs > 0
            && summary.succeeded_jobs < summary.total_jobs
            && summary.running_jobs == 0
            && summary.applying_jobs == 0
        {
            return Ok(summary);
        }
        if start.elapsed() >= timeout_after {
            anyhow::bail!(
                "timed out waiting for durable_enqueue persisted ACK before target apply: frontier={:?} feedback={:?} total={} pending={} running={} succeeded={} failed={} received={} staged={} loaded={} applying={} applied={}",
                frontier,
                feedback,
                summary.total_jobs,
                summary.pending_jobs,
                summary.running_jobs,
                summary.succeeded_jobs,
                summary.failed_jobs,
                summary.received_jobs,
                summary.staged_jobs,
                summary.loaded_jobs,
                summary.applying_jobs,
                summary.applied_jobs
            );
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_durable_cdc_jobs_before_queue_drain(
    pool: &PgPool,
    state_schema: &str,
    state_store: &SyncStateStore,
    state_handle: &StateHandle,
    connection_id: &str,
    timeout_after: Duration,
) -> Result<(CdcBatchLoadQueueSummary, Vec<DurableQueueJobSnapshot>)> {
    let start = Instant::now();
    loop {
        let feedback = state_handle.load_cdc_feedback_state().await?;
        let summary = state_store
            .load_cdc_batch_load_queue_summary(connection_id)
            .await?;
        if summary.failed_jobs > 0 {
            anyhow::bail!(
                "CDC batch-load queue failed before durable_enqueue restart boundary could be observed: failed={} latest_error={:?}",
                summary.failed_jobs,
                summary.latest_failed_error
            );
        }
        let feedback_next_sequence = feedback
            .as_ref()
            .map_or(0, |feedback| feedback.next_sequence_to_ack);
        let jobs = load_durable_queue_job_snapshots(pool, state_schema, connection_id).await?;
        let durable_jobs_ready = jobs.iter().all(|job| {
            job.total_fragments > 0
                && job.durable_fragments == job.total_fragments
                && feedback_next_sequence > u64::try_from(job.max_sequence).unwrap_or_default()
        });
        if durable_jobs_ready
            && !jobs.is_empty()
            && jobs.iter().any(|job| job.status != "succeeded")
            && summary.total_jobs > 0
            && summary.succeeded_jobs < summary.total_jobs
            && summary.running_jobs == 0
            && summary.applying_jobs == 0
        {
            return Ok((summary, jobs));
        }
        if start.elapsed() >= timeout_after {
            anyhow::bail!(
                "timed out waiting for durable_enqueue CDC jobs before target apply: feedback={:?} jobs={:?} total={} pending={} running={} succeeded={} failed={} received={} staged={} loaded={} applying={} applied={}",
                feedback,
                jobs,
                summary.total_jobs,
                summary.pending_jobs,
                summary.running_jobs,
                summary.succeeded_jobs,
                summary.failed_jobs,
                summary.received_jobs,
                summary.staged_jobs,
                summary.loaded_jobs,
                summary.applying_jobs,
                summary.applied_jobs
            );
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_batch_load_queue_drained(
    state_store: &SyncStateStore,
    connection_id: &str,
    timeout_after: Duration,
) -> Result<CdcBatchLoadQueueSummary> {
    let start = Instant::now();
    loop {
        let summary = state_store
            .load_cdc_batch_load_queue_summary(connection_id)
            .await?;
        if summary.total_jobs > 0
            && summary.pending_jobs == 0
            && summary.running_jobs == 0
            && summary.failed_jobs == 0
            && summary.succeeded_jobs == summary.total_jobs
        {
            return Ok(summary);
        }
        if start.elapsed() >= timeout_after {
            anyhow::bail!(
                "timed out waiting for CDC batch-load queue to drain: total={} pending={} running={} succeeded={} failed={} received={} staged={} loaded={} applying={} applied={}",
                summary.total_jobs,
                summary.pending_jobs,
                summary.running_jobs,
                summary.succeeded_jobs,
                summary.failed_jobs,
                summary.received_jobs,
                summary.staged_jobs,
                summary.loaded_jobs,
                summary.applying_jobs,
                summary.applied_jobs
            );
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn tail_log(log_path: &PathBuf, lines: usize) -> String {
    let contents = tokio::fs::read_to_string(log_path)
        .await
        .unwrap_or_default();
    let all = contents.lines().collect::<Vec<_>>();
    let start = all.len().saturating_sub(lines);
    all[start..].join("\n")
}

async fn cleanup_stale_apply_slots(pool: &sqlx::PgPool) -> Result<()> {
    let slot_names: Vec<String> = sqlx::query_scalar(
        r#"
        select slot_name
        from pg_replication_slots
        where slot_type = 'logical'
          and active = false
          and slot_name like $1
        "#,
    )
    .bind(format!("{APPLY_WORKER_PREFIX}_%"))
    .fetch_all(pool)
    .await?;

    for slot_name in slot_names {
        sqlx::query("select pg_drop_replication_slot($1)")
            .bind(slot_name)
            .execute(pool)
            .await?;
    }
    Ok(())
}
