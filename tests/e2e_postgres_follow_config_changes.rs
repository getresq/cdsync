use anyhow::Result;
use cdsync::config::{PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::sources::postgres::PostgresSource;
use cdsync::types::MetadataColumns;
use sqlx::Executor;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;

fn tracked_table(name: String) -> PostgresTableConfig {
    PostgresTableConfig {
        name,
        primary_key: Some("id".to_string()),
        updated_at_column: Some("updated_at".to_string()),
        soft_delete: Some(false),
        soft_delete_column: None,
        where_clause: None,
        columns: None,
    }
}

#[tokio::test]
async fn e2e_configured_source_table_must_exist() -> Result<()> {
    dotenv_support::load_dotenv()?;
    let Some(pg_url) = std::env::var("CDSYNC_E2E_PG_URL")
        .ok()
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let existing_table = format!("public.cdsync_exists_{}", &suffix[..8]);
    let missing_table = format!("public.cdsync_missing_{}", &suffix[..8]);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg_url)
        .await?;
    pool.execute(format!("drop table if exists {existing_table}").as_str())
        .await?;
    pool.execute(
        format!(
            "create table {existing_table} (id bigint primary key, name text, updated_at timestamptz not null default now())"
        )
        .as_str(),
    )
    .await?;

    let source = PostgresSource::new(
        PostgresConfig {
            url: pg_url,
            tables: Some(vec![
                tracked_table(existing_table),
                tracked_table(missing_table.clone()),
            ]),
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
        },
        MetadataColumns::default(),
    )
    .await?;
    let err = source
        .resolve_tables()
        .await
        .expect_err("missing source table");
    assert!(
        err.to_string()
            .contains(&format!("source table {missing_table} does not exist"))
    );
    Ok(())
}
