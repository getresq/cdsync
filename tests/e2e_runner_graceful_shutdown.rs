#![cfg(unix)]

use anyhow::{Context, Result};
use cdsync::state::SyncState;
use std::env;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn e2e_runner_polling_graceful_shutdown() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

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
    let state_path = temp_dir.path().join("state.db");
    let stats_path = temp_dir.path().join("stats.db");
    let config_path = temp_dir.path().join("config.yaml");
    tokio::fs::write(
        &config_path,
        format!(
            r#"
state:
  path: "{state_path}"
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
  path: "{stats_path}"
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
      storage_write_enabled: true
      emulator_http: "{bq_http}"
      emulator_grpc: "{bq_grpc}"
"#,
            state_path = state_path.display(),
            stats_path = stats_path.display(),
        ),
    )
    .await?;

    let mut child = runner_process(&config_path, "runner_demo")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    terminate_child(child.id().context("runner pid")?).await?;
    let status: std::process::ExitStatus = timeout(Duration::from_secs(15), child.wait())
        .await
        .context("runner did not exit in time")??;
    assert!(status.success());

    let state = SyncState::load(&state_path).await?;
    let connection = state
        .connections
        .get("runner_demo")
        .context("missing runner connection state")?;
    assert_eq!(connection.last_sync_status.as_deref(), Some("success"));
    assert!(connection.postgres.contains_key(&qualified_table));
    Ok(())
}

#[tokio::test]
#[ignore]
async fn e2e_runner_cdc_graceful_shutdown() -> Result<()> {
    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .context("set CDSYNC_E2E_PG_URL to a Postgres connection string")?;
    let bq_http = env::var("CDSYNC_E2E_BQ_HTTP")
        .context("set CDSYNC_E2E_BQ_HTTP to the BigQuery emulator HTTP base URL")?;
    let bq_grpc = env::var("CDSYNC_E2E_BQ_GRPC")
        .context("set CDSYNC_E2E_BQ_GRPC to the BigQuery emulator gRPC host:port")?;
    let project_id = env::var("CDSYNC_E2E_BQ_PROJECT").unwrap_or_else(|_| "cdsync".to_string());
    let dataset = env::var("CDSYNC_E2E_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e".to_string());

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
    let state_path = temp_dir.path().join("state.db");
    let stats_path = temp_dir.path().join("stats.db");
    let config_path = temp_dir.path().join("config.yaml");
    tokio::fs::write(
        &config_path,
        format!(
            r#"
state:
  path: "{state_path}"
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
  path: "{stats_path}"
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
      storage_write_enabled: true
      emulator_http: "{bq_http}"
      emulator_grpc: "{bq_grpc}"
"#,
            state_path = state_path.display(),
            stats_path = stats_path.display(),
        ),
    )
    .await?;

    let mut child = runner_process(&config_path, "runner_cdc_demo")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    terminate_child(child.id().context("runner pid")?).await?;
    let status: std::process::ExitStatus = timeout(Duration::from_secs(15), child.wait())
        .await
        .context("runner did not exit in time")??;
    assert!(status.success());

    let state = SyncState::load(&state_path).await?;
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

async fn terminate_child(pid: u32) -> Result<()> {
    let status: std::process::ExitStatus = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await?;
    anyhow::ensure!(status.success(), "failed to send SIGTERM");
    Ok(())
}
