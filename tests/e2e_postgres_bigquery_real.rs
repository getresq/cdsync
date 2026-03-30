use anyhow::{Context, Result};
use cdsync::config::{BigQueryConfig, PostgresConfig, PostgresTableConfig, SchemaChangePolicy};
use cdsync::destinations::bigquery::BigQueryDestination;
use cdsync::sources::postgres::{PostgresSource, TableSyncRequest};
use cdsync::state::ConnectionState;
use cdsync::types::{SyncMode, destination_table_name};
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::job::query::QueryRequest;
use gcloud_bigquery::query::row::Row as BigQueryRow;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Once;
use uuid::Uuid;

static RUSTLS_PROVIDER: Once = Once::new();

#[tokio::test]
#[ignore]
async fn e2e_postgres_bigquery_real_heavy_sync() -> Result<()> {
    install_rustls_provider();

    let pg_url = env::var("CDSYNC_E2E_PG_URL")
        .unwrap_or_else(|_| "postgres://cdsync:cdsync@localhost:5433/cdsync".to_string());
    let project_id =
        env::var("CDSYNC_REAL_BQ_PROJECT").unwrap_or_else(|_| "resq-develop".to_string());
    let dataset =
        env::var("CDSYNC_REAL_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e_real".to_string());
    let location = env::var("CDSYNC_REAL_BQ_LOCATION").unwrap_or_else(|_| "US".to_string());
    let key_path = env::var("CDSYNC_REAL_BQ_KEY_PATH")
        .unwrap_or_else(|_| ".secrets/resq-develop-f0c7ed0c3d65.json".to_string());
    anyhow::ensure!(
        Path::new(&key_path).exists(),
        "set CDSYNC_REAL_BQ_KEY_PATH or place the key in .secrets/"
    );

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
        schema_changes: Some(SchemaChangePolicy::Auto),
        cdc_pipeline_id: None,
        cdc_batch_size: None,
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
        location: Some(location.clone()),
        service_account_key_path: Some(PathBuf::from(&key_path)),
        service_account_key: None,
        partition_by_synced_at: Some(false),
        storage_write_enabled: Some(true),
        emulator_http: None,
        emulator_grpc: None,
    };

    let source = PostgresSource::new(pg_config.clone()).await?;
    let tables = source.resolve_tables().await?;
    let dest = BigQueryDestination::new(bq_config.clone(), false).await?;
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

    let source = PostgresSource::new(pg_config).await?;
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

    let client = real_bigquery_client(&key_path).await?;
    let schema_fields =
        fetch_live_table_fields(&client, &project_id, &dataset, &dest_table).await?;
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

    let extra_count = query_i64(
        &client,
        &project_id,
        &location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where extra is not null",
            project = project_id,
            dataset = dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(extra_count, 150);

    let deleted_count = query_i64(
        &client,
        &project_id,
        &location,
        &format!(
            "select count(1) from `{project}.{dataset}.{table}` where _cdsync_deleted_at is not null",
            project = project_id,
            dataset = dataset,
            table = dest_table
        ),
    )
    .await?;
    assert_eq!(deleted_count, 150);

    Ok(())
}

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("installing rustls ring provider");
    });
}

async fn real_bigquery_client(key_path: &str) -> Result<Client> {
    let key = CredentialsFile::new_from_file(key_path.to_string()).await?;
    let (config, _project) = ClientConfig::new_with_credentials(key).await?;
    Ok(Client::new(config).await?)
}

async fn fetch_live_table_fields(
    client: &Client,
    project_id: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<String>> {
    let table = client.table().get(project_id, dataset, table).await?;
    let schema = table.schema.context("missing table schema")?;
    Ok(schema.fields.into_iter().map(|field| field.name).collect())
}

async fn query_i64(client: &Client, project_id: &str, location: &str, sql: &str) -> Result<i64> {
    let request = QueryRequest {
        query: sql.to_string(),
        use_legacy_sql: false,
        location: location.to_string(),
        ..Default::default()
    };
    let mut iter = client.query::<BigQueryRow>(project_id, request).await?;
    let row = iter.next().await?.context("missing query row")?;
    row.column::<i64>(0)
        .map_err(|err| anyhow::anyhow!("decoding query result failed: {}", err))
}
