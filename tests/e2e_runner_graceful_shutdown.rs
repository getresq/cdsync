#![cfg(unix)]

use anyhow::{Context, Result};
use cdsync::config::{StateConfig, StatsConfig};
use cdsync::state::{SyncState, SyncStateStore};
use cdsync::stats::StatsDb;
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;

#[tokio::test]
async fn e2e_runner_polling_graceful_shutdown() -> Result<()> {
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
    let table_name = format!("cdsync_runner_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");

    let pool = sqlx::postgres::PgPoolOptions::new()
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

    let temp_dir = tempfile::tempdir()?;
    let state_schema = format!("cdsync_state_runner_{}", &suffix[..8]);
    let stats_schema = format!("cdsync_stats_runner_{}", &suffix[..8]);
    let config_path = temp_dir.path().join("config.yaml");
    tokio::fs::write(
        &config_path,
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
  default_batch_size: 1000
  max_retries: 3
  retry_backoff_ms: 1000
  max_concurrency: 1
stats:
  url: "{pg_url}"
  schema: "{stats_schema}"
connections:
  - id: "runner_demo"
    enabled: true
    schedule:
      every: "1s"
    source:
      type: postgres
      url: "{pg_url}"
      cdc: false
      schema_changes: auto
      tables:
        - name: "{qualified_table}"
          primary_key: "id"
          updated_at_column: "updated_at"
    destination:
      type: bigquery
      project_id: "{project_id}"
      dataset: "{dataset}"
      location: "US"
      batch_load_bucket:
      batch_load_prefix:
      emulator_http: "{bq_http}"
      emulator_grpc: "{bq_grpc}"
"#,
            state_schema = state_schema,
            stats_schema = stats_schema,
        ),
    )
    .await?;
    prepare_runner_state(&pg_url, &state_schema, &stats_schema).await?;

    let mut child = runner_process(&config_path, "runner_demo")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    terminate_child(child.id().context("runner pid")?).await?;
    let status: std::process::ExitStatus = timeout(Duration::from_secs(15), child.wait())
        .await
        .context("runner did not exit in time")??;
    assert!(status.success());

    let state = SyncState::load_with_config(&StateConfig {
        url: pg_url.clone(),
        schema: Some(state_schema.clone()),
    })
    .await?;
    let connection = state
        .connections
        .get("runner_demo")
        .context("missing runner connection state")?;
    assert_eq!(connection.last_sync_status.as_deref(), Some("success"));
    assert!(connection.postgres.contains_key(&qualified_table));
    Ok(())
}

#[tokio::test]
async fn e2e_runner_cdc_graceful_shutdown() -> Result<()> {
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
    let table_name = format!("cdsync_runner_cdc_{}", &suffix[..8]);
    let qualified_table = format!("public.{table_name}");
    let publication = format!("cdsync_runner_pub_{}", &suffix[..8]);

    let pool = sqlx::postgres::PgPoolOptions::new()
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

    let temp_dir = tempfile::tempdir()?;
    let state_schema = format!("cdsync_state_runner_cdc_{}", &suffix[..8]);
    let stats_schema = format!("cdsync_stats_runner_cdc_{}", &suffix[..8]);
    let config_path = temp_dir.path().join("config.yaml");
    tokio::fs::write(
        &config_path,
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
  default_batch_size: 1000
  max_retries: 3
  retry_backoff_ms: 1000
  max_concurrency: 1
stats:
  url: "{pg_url}"
  schema: "{stats_schema}"
connections:
  - id: "runner_cdc_demo"
    enabled: true
    source:
      type: postgres
      url: "{pg_url}"
      cdc: true
      publication: "{publication}"
      cdc_pipeline_id: 7
      cdc_batch_size: 1000
      cdc_idle_timeout_seconds: 1
      schema_changes: auto
      tables:
        - name: "{qualified_table}"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "{project_id}"
      dataset: "{dataset}"
      location: "US"
      batch_load_bucket:
      batch_load_prefix:
      emulator_http: "{bq_http}"
      emulator_grpc: "{bq_grpc}"
"#,
            state_schema = state_schema,
            stats_schema = stats_schema,
        ),
    )
    .await?;
    prepare_runner_state(&pg_url, &state_schema, &stats_schema).await?;

    let mut child = runner_process(&config_path, "runner_cdc_demo")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    terminate_child(child.id().context("runner pid")?).await?;
    let status: std::process::ExitStatus = timeout(Duration::from_secs(15), child.wait())
        .await
        .context("runner did not exit in time")??;
    assert!(status.success());

    let state = SyncState::load_with_config(&StateConfig {
        url: pg_url.clone(),
        schema: Some(state_schema.clone()),
    })
    .await?;
    let connection = state
        .connections
        .get("runner_cdc_demo")
        .context("missing runner connection state")?;
    assert_eq!(connection.last_sync_status.as_deref(), Some("success"));
    assert!(
        connection
            .postgres_cdc
            .as_ref()
            .and_then(|cdc| cdc.slot_name.as_deref())
            .is_some()
    );
    Ok(())
}

fn runner_process(config_path: &PathBuf, connection_id: &str) -> Result<tokio::process::Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_cdsync"))
        .arg("run")
        .arg("--config")
        .arg(config_path)
        .arg("--connection")
        .arg(connection_id)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?)
}

async fn prepare_runner_state(pg_url: &str, state_schema: &str, stats_schema: &str) -> Result<()> {
    SyncStateStore::migrate_with_config(&StateConfig {
        url: pg_url.to_string(),
        schema: Some(state_schema.to_string()),
    })
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
