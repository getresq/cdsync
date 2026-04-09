use anyhow::{Context, Result};
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::job::query::QueryRequest;
use gcloud_bigquery::query::row::Row as BigQueryRow;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Once;

static RUSTLS_PROVIDER: Once = Once::new();

pub struct RealBigQueryEnv {
    pub project_id: String,
    pub dataset: String,
    pub location: String,
    pub key_path: PathBuf,
}

pub fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("installing rustls ring provider");
    });
}

pub fn load_env() -> Result<RealBigQueryEnv> {
    crate::dotenv_support::load_dotenv()?;
    let project_id =
        env::var("CDSYNC_REAL_BQ_PROJECT").unwrap_or_else(|_| "your-gcp-project".to_string());
    let dataset =
        env::var("CDSYNC_REAL_BQ_DATASET").unwrap_or_else(|_| "cdsync_e2e_real".to_string());
    let location = env::var("CDSYNC_REAL_BQ_LOCATION").unwrap_or_else(|_| "US".to_string());
    let key_path = env::var("CDSYNC_REAL_BQ_KEY_PATH")
        .unwrap_or_else(|_| ".secrets/your-service-account.json".to_string());
    anyhow::ensure!(
        Path::new(&key_path).exists(),
        "set CDSYNC_REAL_BQ_KEY_PATH or place the key in .secrets/"
    );
    Ok(RealBigQueryEnv {
        project_id,
        dataset,
        location,
        key_path: PathBuf::from(key_path),
    })
}

pub async fn client(key_path: &Path) -> Result<Client> {
    let key = CredentialsFile::new_from_file(key_path.to_string_lossy().to_string()).await?;
    let (config, _project) = ClientConfig::new_with_credentials(key).await?;
    Ok(Client::new(config).await?)
}

pub async fn fetch_live_table_fields(
    client: &Client,
    project_id: &str,
    dataset: &str,
    table: &str,
) -> Result<Vec<String>> {
    let table = client.table().get(project_id, dataset, table).await?;
    let schema = table.schema.context("missing table schema")?;
    Ok(schema.fields.into_iter().map(|field| field.name).collect())
}

pub async fn query_i64(
    client: &Client,
    project_id: &str,
    location: &str,
    sql: &str,
) -> Result<i64> {
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
