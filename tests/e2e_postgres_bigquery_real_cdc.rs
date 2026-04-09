use anyhow::Result;
use cdsync::config::{
    BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy, StateConfig,
    StatsConfig,
};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource};
use cdsync::state::{ConnectionState, SyncState, SyncStateStore};
use cdsync::stats::StatsDb;
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use chrono::Utc;
use etl_postgres::replication::slots::APPLY_WORKER_PREFIX;
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use sqlx::postgres::PgPoolOptions;
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
    pg_url: &'a str,
    project_id: &'a str,
    dataset: &'a str,
    location: &'a str,
    key_path: &'a str,
    state_schema: &'a str,
    stats_schema: &'a str,
    publication: &'a str,
    pipeline_id: u64,
    tables: &'a [PostgresTableConfig],
    bq_config: &'a BigQueryConfig,
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
        return Ok(());
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let Ok(real_bq) = real_bigquery_support::load_env() else {
        return Ok(());
    };
    let Some(batch_load_bucket) = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres://cdsync:cdsync@localhost:5433/cdsync".to_string());

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
        cdc_apply_concurrency: Some(8),
        cdc_batch_load_worker_count: Some(8),
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
async fn e2e_postgres_bigquery_real_cdc_follow_batch_load_relation_stress() -> Result<()> {
    if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }
    dotenv_support::load_dotenv()?;
    real_bigquery_support::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();

    let Ok(real_bq) = real_bigquery_support::load_env() else {
        return Ok(());
    };
    let pg_url = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "postgres://cdsync:cdsync@localhost:5433/cdsync".to_string());
    let batch_load_bucket = std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "cdsync-e2e-test-loads".to_string());

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

    let pipeline_id = Utc::now().timestamp_millis() as u64 % 1_000_000 + 20_000;
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
            pg_url: &pg_url,
            project_id: &real_bq.project_id,
            dataset: &real_bq.dataset,
            location: &real_bq.location,
            key_path: real_bq.key_path.to_str().unwrap_or_default(),
            state_schema: &state_schema,
            stats_schema: &stats_schema,
            publication: &publication,
            pipeline_id,
            tables: &table_configs,
            bq_config: &bq_config,
        }),
    )
    .await?;

    cleanup_stale_follow_runners(connection_id).await?;
    let mut child = runner_process_with_logs(&config_path, connection_id, &log_path)?;
    let child_pid = child.id().ok_or_else(|| anyhow::anyhow!("runner pid"))?;
    let mut child_guard = ChildTerminationGuard::new(child_pid);
    wait_for_log_line(
        &log_path,
        "starting logical replication",
        Duration::from_secs(60),
    )
    .await?;

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

    terminate_and_wait_child(&mut child, child_pid).await?;
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
  - id: "runner_real_batchload_demo"
    enabled: true
    source:
      type: postgres
      url: "{pg_url}"
      cdc: true
      publication: "{publication}"
      cdc_pipeline_id: {pipeline_id}
      cdc_batch_size: 200
      cdc_apply_concurrency: 2
      cdc_max_fill_ms: 1000
      cdc_idle_timeout_seconds: 1
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
        pg_url = input.pg_url,
        state_schema = input.state_schema,
        stats_schema = input.stats_schema,
        publication = input.publication,
        pipeline_id = input.pipeline_id,
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

async fn terminate_and_wait_child(child: &mut Child, pid: u32) -> Result<()> {
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

async fn wait_for_log_line(
    log_path: &PathBuf,
    needle: &str,
    timeout_after: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        let contents = tokio::fs::read_to_string(log_path)
            .await
            .unwrap_or_default();
        if contents.contains(needle) {
            return Ok(());
        }
        if start.elapsed() >= timeout_after {
            anyhow::bail!("timed out waiting for log line: {}", needle);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_lsn_advance(
    state_config: &StateConfig,
    connection_id: &str,
    baseline_lsn: &str,
    timeout_after: Duration,
) -> Result<String> {
    let start = Instant::now();
    loop {
        let state = SyncState::load_with_config(state_config).await?;
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
