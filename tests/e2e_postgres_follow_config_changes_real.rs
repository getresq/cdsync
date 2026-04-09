use anyhow::{Context, Result};
use cdsync::config::{
    BigQueryConfig, PostgresConfig, PostgresPublicationMode, PostgresTableConfig,
    SchemaChangePolicy,
};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{CdcSyncRequest, PostgresSource};
use cdsync::state::{ConnectionState, StateHandle, SyncStateStore};
use cdsync::types::{MetadataColumns, SyncMode, destination_table_name};
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use std::path::PathBuf;
use uuid::Uuid;
#[path = "support/dotenv.rs"]
mod dotenv_support;
#[path = "support/real_bigquery.rs"]
mod real_bigquery_support;

struct RealCdcE2eEnv {
    pg_url: String,
    bq: real_bigquery_support::RealBigQueryEnv,
    batch_load_bucket: String,
}

impl RealCdcE2eEnv {
    fn load() -> Result<Self> {
        if std::env::var("CDSYNC_RUN_REAL_BQ_TESTS").ok().as_deref() != Some("1") {
            anyhow::bail!(
                "real BigQuery follow tests are disabled; set CDSYNC_RUN_REAL_BQ_TESTS=1"
            );
        }
        dotenv_support::load_dotenv()?;
        real_bigquery_support::install_rustls_provider();
        let _ = JWT_CRYPTO_PROVIDER.install_default();
        Ok(Self {
            pg_url: std::env::var("CDSYNC_E2E_PG_URL")
                .unwrap_or_else(|_| "postgres://cdsync:cdsync@localhost:5433/cdsync".to_string()),
            bq: real_bigquery_support::load_env()?,
            batch_load_bucket: std::env::var("CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET")
                .context("set CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET to a writable GCS bucket")?,
        })
    }

    fn bq_config(&self, prefix: &str) -> BigQueryConfig {
        BigQueryConfig {
            project_id: self.bq.project_id.clone(),
            dataset: self.bq.dataset.clone(),
            location: Some(self.bq.location.clone()),
            service_account_key_path: Some(PathBuf::from(&self.bq.key_path)),
            service_account_key: None,
            partition_by_synced_at: Some(false),
            batch_load_bucket: Some(self.batch_load_bucket.clone()),
            batch_load_prefix: Some(prefix.to_string()),
            emulator_http: None,
            emulator_grpc: None,
        }
    }

    fn pg_config(
        &self,
        publication: &str,
        pipeline_id: u64,
        policy: SchemaChangePolicy,
        tables: Vec<PostgresTableConfig>,
    ) -> PostgresConfig {
        PostgresConfig {
            url: self.pg_url.clone(),
            tables: Some(tables),
            table_selection: None,
            batch_size: Some(1_000),
            cdc: Some(true),
            publication: Some(publication.to_string()),
            publication_mode: Some(PostgresPublicationMode::Manage),
            schema_changes: Some(policy),
            cdc_pipeline_id: Some(pipeline_id),
            cdc_batch_size: Some(1_000),
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: Some(8),
            cdc_max_fill_ms: Some(200),
            cdc_max_pending_events: Some(20_000),
            cdc_idle_timeout_seconds: Some(1),
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        }
    }
}

async fn query_string(
    client: &gcloud_bigquery::client::Client,
    project_id: &str,
    location: &str,
    sql: &str,
) -> Result<String> {
    use gcloud_bigquery::http::job::query::QueryRequest;
    use gcloud_bigquery::query::row::Row as BigQueryRow;

    let request = QueryRequest {
        query: sql.to_string(),
        use_legacy_sql: false,
        location: location.to_string(),
        ..Default::default()
    };
    let mut iter = client.query::<BigQueryRow>(project_id, request).await?;
    let row = iter.next().await?.context("missing query row")?;
    row.column::<String>(0)
        .map_err(|err| anyhow::anyhow!("decoding query result failed: {}", err))
}

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

async fn connect_pool(pg_url: &str) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(pg_url)
        .await
        .context("connect postgres")
}

async fn run_cdc_once(
    env: &RealCdcE2eEnv,
    state: &mut ConnectionState,
    pg_config: PostgresConfig,
    prefix: &str,
    mode: SyncMode,
    state_handle: Option<StateHandle>,
) -> Result<()> {
    let source = PostgresSource::new(pg_config.clone(), MetadataColumns::default()).await?;
    let tables = source.resolve_tables().await?;
    let dest =
        BigQueryDestination::new(env.bq_config(prefix), false, MetadataColumns::default()).await?;
    dest.validate().await?;
    Box::pin(source.sync_cdc(CdcSyncRequest {
        dest: &dest,
        state,
        state_handle,
        mode,
        dry_run: false,
        follow: false,
        default_batch_size: 1_000,
        retry_backoff_ms: 1_000,
        snapshot_concurrency: 1,
        tables: &tables,
        schema_diff_enabled: false,
        stats: None,
        shutdown: None,
    }))
    .await
}

#[tokio::test]
async fn e2e_follow_adding_table_to_config_bootstraps_it_with_real_bigquery() -> Result<()> {
    let Ok(env) = RealCdcE2eEnv::load() else {
        return Ok(());
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let table_a = format!("public.cdsync_cfg_add_real_a_{}", &suffix[..8]);
    let table_b = format!("public.cdsync_cfg_add_real_b_{}", &suffix[..8]);
    let publication = format!("cdsync_cfg_add_real_pub_{}", &suffix[..8]);
    let pipeline_id = 5_000_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let prefix = format!("cdsync-e2e-follow-add/{}", &suffix[..8]);

    let pool = connect_pool(&env.pg_url).await?;
    pool.execute(format!("drop table if exists {table_a}").as_str())
        .await?;
    pool.execute(format!("drop table if exists {table_b}").as_str())
        .await?;
    pool.execute(format!("drop publication if exists {publication}").as_str())
        .await?;
    for table in [&table_a, &table_b] {
        pool.execute(
            format!(
                "create table {table} (id bigint primary key, name text, updated_at timestamptz not null default now())"
            )
            .as_str(),
        )
        .await?;
    }
    pool.execute(format!("insert into {table_a} (id, name) values (1, 'alpha')").as_str())
        .await?;

    let mut state = ConnectionState::default();
    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(
            &publication,
            pipeline_id,
            SchemaChangePolicy::Auto,
            vec![tracked_table(table_a.clone())],
        ),
        &prefix,
        SyncMode::Full,
        None,
    ))
    .await?;

    pool.execute(
        format!("update {table_a} set name = 'alpha-updated', updated_at = now() where id = 1")
            .as_str(),
    )
    .await?;
    pool.execute(format!("insert into {table_b} (id, name) values (1, 'beta')").as_str())
        .await?;

    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(
            &publication,
            pipeline_id,
            SchemaChangePolicy::Auto,
            vec![
                tracked_table(table_a.clone()),
                tracked_table(table_b.clone()),
            ],
        ),
        &prefix,
        SyncMode::Incremental,
        None,
    ))
    .await?;

    let client = real_bigquery_support::client(&env.bq.key_path).await?;
    let name_a = query_string(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select name from `{project}.{dataset}.{table}` where id = 1 limit 1",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_a),
        ),
    )
    .await?;
    let fields_a = real_bigquery_support::fetch_live_table_fields(
        &client,
        &env.bq.project_id,
        &env.bq.dataset,
        &destination_table_name(&table_a),
    )
    .await?;
    let count_b = real_bigquery_support::query_i64(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where id = 1",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_b),
        ),
    )
    .await?;
    assert_eq!(name_a, "alpha-updated");
    assert!(fields_a.iter().any(|field| field == "name"));
    assert_eq!(count_b, 1);
    Ok(())
}

#[tokio::test]
async fn e2e_follow_removing_table_from_config_stops_tracking_it_with_real_bigquery() -> Result<()>
{
    let Ok(env) = RealCdcE2eEnv::load() else {
        return Ok(());
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let table_a = format!("public.cdsync_cfg_remove_real_a_{}", &suffix[..8]);
    let table_b = format!("public.cdsync_cfg_remove_real_b_{}", &suffix[..8]);
    let publication = format!("cdsync_cfg_remove_real_pub_{}", &suffix[..8]);
    let pipeline_id = 6_000_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let prefix = format!("cdsync-e2e-follow-remove/{}", &suffix[..8]);

    let pool = connect_pool(&env.pg_url).await?;
    pool.execute(format!("drop table if exists {table_a}").as_str())
        .await?;
    pool.execute(format!("drop table if exists {table_b}").as_str())
        .await?;
    pool.execute(format!("drop publication if exists {publication}").as_str())
        .await?;
    for table in [&table_a, &table_b] {
        pool.execute(
            format!(
                "create table {table} (id bigint primary key, name text, updated_at timestamptz not null default now())"
            )
            .as_str(),
        )
        .await?;
    }
    pool.execute(format!("insert into {table_a} (id, name) values (1, 'alpha')").as_str())
        .await?;
    pool.execute(format!("insert into {table_b} (id, name) values (1, 'beta')").as_str())
        .await?;

    let mut state = ConnectionState::default();
    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(
            &publication,
            pipeline_id,
            SchemaChangePolicy::Auto,
            vec![
                tracked_table(table_a.clone()),
                tracked_table(table_b.clone()),
            ],
        ),
        &prefix,
        SyncMode::Full,
        None,
    ))
    .await?;

    pool.execute(
        format!("update {table_a} set name = 'alpha-updated', updated_at = now() where id = 1")
            .as_str(),
    )
    .await?;
    pool.execute(
        format!("update {table_b} set name = 'beta-updated', updated_at = now() where id = 1")
            .as_str(),
    )
    .await?;

    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(
            &publication,
            pipeline_id,
            SchemaChangePolicy::Auto,
            vec![tracked_table(table_a.clone())],
        ),
        &prefix,
        SyncMode::Incremental,
        None,
    ))
    .await?;

    let client = real_bigquery_support::client(&env.bq.key_path).await?;
    let name_a = query_string(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select name from `{project}.{dataset}.{table}` where id = 1 limit 1",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_a),
        ),
    )
    .await?;
    let name_b = query_string(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select name from `{project}.{dataset}.{table}` where id = 1 limit 1",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_b),
        ),
    )
    .await?;
    assert_eq!(name_a, "alpha-updated");
    assert_eq!(name_b, "beta");
    Ok(())
}

#[tokio::test]
async fn e2e_follow_single_table_resync_preserves_other_table_backlog_with_real_bigquery()
-> Result<()> {
    let Ok(env) = RealCdcE2eEnv::load() else {
        return Ok(());
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let table_a = format!("public.cdsync_resync_real_a_{}", &suffix[..8]);
    let table_b = format!("public.cdsync_resync_real_b_{}", &suffix[..8]);
    let publication = format!("cdsync_resync_real_pub_{}", &suffix[..8]);
    let pipeline_id = 7_000_000 + (Uuid::new_v4().as_u128() as u64 % 1_000_000);
    let prefix = format!("cdsync-e2e-follow-resync/{}", &suffix[..8]);

    let pool = connect_pool(&env.pg_url).await?;
    pool.execute(format!("drop table if exists {table_a}").as_str())
        .await?;
    pool.execute(format!("drop table if exists {table_b}").as_str())
        .await?;
    pool.execute(format!("drop publication if exists {publication}").as_str())
        .await?;
    pool.execute(
        format!(
            "create table {table_a} (id bigint primary key, name text, extra text, updated_at timestamptz not null default now())"
        )
        .as_str(),
    )
    .await?;
    pool.execute(
        format!(
            "create table {table_b} (id bigint primary key, name text, updated_at timestamptz not null default now())"
        )
        .as_str(),
    )
    .await?;
    pool.execute(
        format!("insert into {table_a} (id, name, extra) values (1, 'alpha', 'x')").as_str(),
    )
    .await?;
    pool.execute(format!("insert into {table_b} (id, name) values (1, 'beta')").as_str())
        .await?;

    let state_config = cdsync::config::StateConfig {
        url: env.pg_url.clone(),
        schema: Some(format!("cdsync_state_resync_{}", &suffix[..8])),
    };
    SyncStateStore::migrate_with_config(&state_config, 16).await?;
    let state_store = SyncStateStore::open_with_config(&state_config, 16).await?;
    let state_handle = state_store.handle("app");
    let mut state = ConnectionState::default();
    let tracked = vec![
        tracked_table(table_a.clone()),
        tracked_table(table_b.clone()),
    ];
    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(
            &publication,
            pipeline_id,
            SchemaChangePolicy::Auto,
            tracked.clone(),
        ),
        &prefix,
        SyncMode::Full,
        Some(state_handle.clone()),
    ))
    .await?;

    pool.execute(format!("alter table {table_a} drop column extra").as_str())
        .await?;
    pool.execute(format!("insert into {table_a} (id, name) values (2, 'alpha-two')").as_str())
        .await?;
    pool.execute(
        format!("update {table_b} set name = 'beta-updated', updated_at = now() where id = 1")
            .as_str(),
    )
    .await?;

    state_store
        .request_postgres_table_resync("app", &table_a)
        .await?;
    Box::pin(run_cdc_once(
        &env,
        &mut state,
        env.pg_config(&publication, pipeline_id, SchemaChangePolicy::Auto, tracked),
        &prefix,
        SyncMode::Incremental,
        Some(state_handle),
    ))
    .await?;

    let client = real_bigquery_support::client(&env.bq.key_path).await?;
    let count_a = real_bigquery_support::query_i64(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}`",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_a),
        ),
    )
    .await?;
    let name_b = query_string(
        &client,
        &env.bq.project_id,
        &env.bq.location,
        &format!(
            "select name from `{project}.{dataset}.{table}` where id = 1 limit 1",
            project = env.bq.project_id,
            dataset = env.bq.dataset,
            table = destination_table_name(&table_b),
        ),
    )
    .await?;
    assert_eq!(count_a, 2);
    assert_eq!(name_b, "beta-updated");
    Ok(())
}
