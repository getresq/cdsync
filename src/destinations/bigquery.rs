mod batch_load;
mod values;

use crate::config::BigQueryConfig;
use crate::destinations::{ChangeApplier, Destination, WriteMode, with_metadata_schema};
use crate::sources::contract::records_to_dataframe;
use crate::tls;
use crate::types::{
    ColumnSchema, DataType, META_SOURCE_EVENT_AT, META_SOURCE_EVENT_ID, MetadataColumns,
    SourceChangeBatch, TableSchema,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::google_cloud_auth::project::Config as GoogleAuthConfig;
use gcloud_bigquery::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use gcloud_bigquery::client::{Client, ClientConfig, HttpClientConfig};
use gcloud_bigquery::http::error::Error as BqError;
use gcloud_bigquery::http::job::get::GetJobRequest;
use gcloud_bigquery::http::job::get_query_results::GetQueryResultsRequest;
use gcloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use gcloud_bigquery::http::job::{Job, JobReference, JobState};
use gcloud_bigquery::http::table::{
    Clustering, Table, TableReference, TableSchema as BqTableSchema, TimePartitionType,
    TimePartitioning,
};
use polars::prelude::DataFrame;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use token_source::{TokenSource, TokenSourceProvider};
use tokio::sync::Mutex;
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
use tracing::{Instrument, error, info, info_span, warn};
use url::Url;
use uuid::Uuid;

pub(crate) use self::batch_load::BATCH_LOAD_JOB_HARD_TIMEOUT;
use self::values::{
    bq_fields_from_schema, bq_ident, dataframe_to_json_rows, default_port, tuple_value_as_datetime,
    tuple_value_as_i64,
};

#[derive(Clone)]
pub struct BigQueryDestination {
    client: Client,
    bq_http_token_source: Option<Arc<dyn TokenSource>>,
    bq_query_results_base_url: String,
    gcs_token_source: Option<Arc<dyn TokenSource>>,
    config: BigQueryConfig,
    dry_run: bool,
    metadata: MetadataColumns,
    cleanup_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    merge_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationTableSummary {
    pub row_count: i64,
    pub max_synced_at: Option<DateTime<Utc>>,
    pub deleted_rows: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CdcBatchLoadJobStep {
    pub(crate) staging_table: String,
    pub(crate) object_uri: String,
    #[serde(default)]
    pub(crate) load_job_id: Option<String>,
    pub(crate) row_count: usize,
    pub(crate) upserted_count: usize,
    pub(crate) deleted_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CdcBatchLoadJobPayload {
    pub(crate) job_id: String,
    pub(crate) source_table: String,
    pub(crate) target_table: String,
    pub(crate) schema: TableSchema,
    #[serde(default)]
    pub(crate) staging_schema: Option<TableSchema>,
    pub(crate) primary_key: String,
    pub(crate) truncate: bool,
    pub(crate) steps: Vec<CdcBatchLoadJobStep>,
}

const EMULATOR_INSERT_BLOCKING_ROWS: usize = 512;
pub(super) const BIGQUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const BIGQUERY_QUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(110);
const BIGQUERY_JOB_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const CDC_STAGING_SEQUENCE_COLUMN: &str = "_cdsync_sequence";
const CDC_STAGING_TX_ORDINAL_COLUMN: &str = "_cdsync_tx_ordinal";

impl BigQueryDestination {
    pub(crate) fn cdc_batch_load_queue_enabled(&self) -> bool {
        self.config.batch_load_bucket.is_some() && self.config.emulator_http.is_none()
    }

    pub(crate) fn batch_load_prefix(&self) -> Option<&str> {
        self.config.batch_load_prefix.as_deref()
    }

    pub async fn new(
        mut config: BigQueryConfig,
        dry_run: bool,
        metadata: MetadataColumns,
    ) -> Result<Self> {
        tls::install_rustls_provider();
        if config.emulator_http.is_none() && config.emulator_grpc.is_some() {
            anyhow::bail!("bigquery.emulator_grpc requires emulator_http");
        }

        let (
            client_config,
            bq_http_token_source,
            bq_query_results_base_url,
            gcs_token_source,
            _project_from_auth,
        ) = if let Some(raw_http) = &config.emulator_http {
            let emulator_http = if raw_http.contains("://") {
                raw_http.to_string()
            } else {
                format!("http://{raw_http}")
            };

            let emulator_grpc = if let Some(raw_grpc) = &config.emulator_grpc {
                if raw_grpc.contains("://") {
                    let url = Url::parse(raw_grpc).context("invalid bigquery.emulator_grpc url")?;
                    let host = url
                        .host_str()
                        .context("bigquery.emulator_grpc missing host")?;
                    let port = url.port().unwrap_or(default_port(url.scheme()));
                    format!("{host}:{port}")
                } else {
                    raw_grpc.to_string()
                }
            } else {
                let url =
                    Url::parse(&emulator_http).context("invalid bigquery.emulator_http url")?;
                let host = url
                    .host_str()
                    .context("bigquery.emulator_http missing host")?;
                let port = url.port().unwrap_or(default_port(url.scheme()));
                format!("{host}:{port}")
            };

            config.emulator_http = Some(emulator_http.clone());
            config.emulator_grpc = Some(emulator_grpc.clone());

            (
                ClientConfig::new_with_emulator(&emulator_grpc, emulator_http),
                None,
                format!(
                    "{}/bigquery/v2",
                    config
                        .emulator_http
                        .as_deref()
                        .unwrap_or("")
                        .trim_end_matches('/')
                ),
                None,
                None,
            )
        } else if let Some(path) = &config.service_account_key_path {
            let key = CredentialsFile::new_from_file(path.to_string_lossy().to_string()).await?;
            let bq_http_token_source = HttpClientConfig::default_token_provider_with(key.clone())
                .await?
                .token_source();
            let (bq_config, project) = ClientConfig::new_with_credentials(key.clone()).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(key),
            )
            .await?;
            (
                bq_config,
                Some(bq_http_token_source),
                "https://bigquery.googleapis.com/bigquery/v2".to_string(),
                Some(token_source.token_source()),
                project,
            )
        } else if let Some(raw_key) = &config.service_account_key {
            let key = CredentialsFile::new_from_str(raw_key).await?;
            let bq_http_token_source = HttpClientConfig::default_token_provider_with(key.clone())
                .await?
                .token_source();
            let (bq_config, project) = ClientConfig::new_with_credentials(key.clone()).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(key),
            )
            .await?;
            (
                bq_config,
                Some(bq_http_token_source),
                "https://bigquery.googleapis.com/bigquery/v2".to_string(),
                Some(token_source.token_source()),
                project,
            )
        } else {
            let bq_http_token_source = HttpClientConfig::default_token_provider()
                .await?
                .token_source();
            let (bq_config, project) = ClientConfig::new_with_auth().await?;
            let token_source = DefaultTokenSourceProvider::new(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
            )
            .await?;
            (
                bq_config,
                Some(bq_http_token_source),
                "https://bigquery.googleapis.com/bigquery/v2".to_string(),
                Some(token_source.token_source()),
                project,
            )
        };
        let client = Client::new(client_config).await?;
        Ok(Self {
            client,
            bq_http_token_source,
            bq_query_results_base_url,
            gcs_token_source,
            config,
            dry_run,
            metadata,
            cleanup_tasks: Arc::new(Mutex::new(Vec::new())),
            merge_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn table_merge_lock(&self, table: &str) -> Arc<tokio::sync::Semaphore> {
        let mut guard = self.merge_locks.lock().await;
        guard
            .entry(table.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(1)))
            .clone()
    }

    pub async fn validate(&self) -> Result<()> {
        if self.dry_run {
            info!("dry-run: skipping BigQuery validation");
            return Ok(());
        }
        if self.config.emulator_http.is_some() {
            info!("bigquery emulator: skipping validation");
            return Ok(());
        }
        match self
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
            .await
        {
            Ok(_) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn summarize_table(&self, table_id: &str) -> Result<DestinationTableSummary> {
        let sql = format!(
            "SELECT COUNT(1) AS row_count, \
             MAX(`{synced}`) AS max_synced_at, \
             COUNTIF(`{deleted}` IS NOT NULL) AS deleted_rows \
             FROM `{project}.{dataset}.{table}`",
            synced = self.metadata.synced_at,
            deleted = self.metadata.deleted_at,
            project = self.config.project_id,
            dataset = self.config.dataset,
            table = table_id
        );
        let response = self.run_query(&sql).await?;
        let row = response
            .rows
            .unwrap_or_default()
            .into_iter()
            .next()
            .context("reconciliation query returned no rows")?;
        Ok(DestinationTableSummary {
            row_count: tuple_value_as_i64(&row, 0)?,
            max_synced_at: tuple_value_as_datetime(&row, 1)?,
            deleted_rows: tuple_value_as_i64(&row, 2)?,
        })
    }

    pub(super) async fn await_with_timeout<T, E, F>(
        label: impl Into<String>,
        duration: Duration,
        fut: F,
    ) -> Result<T>
    where
        F: Future<Output = std::result::Result<T, E>>,
        E: Into<anyhow::Error>,
    {
        let label = label.into();
        match timeout(duration, fut).await {
            Ok(result) => result.map_err(Into::into).with_context(|| label.clone()),
            Err(_) => anyhow::bail!("{} timed out after {}s", label, duration.as_secs()),
        }
    }

    async fn await_bq_timeout<T, F>(
        label: impl Into<String>,
        duration: Duration,
        fut: F,
    ) -> std::result::Result<T, BqError>
    where
        F: Future<Output = std::result::Result<T, BqError>>,
    {
        let label = label.into();
        match timeout(duration, fut).await {
            Ok(result) => result,
            Err(_) => Err(BqError::HttpMiddleware(anyhow::anyhow!(
                "{} timed out after {}s",
                label,
                duration.as_secs()
            ))),
        }
    }

    async fn ensure_dataset(&self) -> Result<()> {
        if self.dry_run {
            info!("dry-run: ensure dataset {}", self.config.dataset);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/datasets/{}",
                emulator_http, self.config.project_id, self.config.dataset
            );
            let client = reqwest::Client::new();
            let response = Self::await_with_timeout(
                format!("fetching BigQuery emulator dataset {}", self.config.dataset),
                BIGQUERY_REQUEST_TIMEOUT,
                client.get(&url).send(),
            )
            .await?;
            if response.status().is_success() {
                return Ok(());
            }
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                let create_url = format!(
                    "{}/projects/{}/datasets",
                    emulator_http, self.config.project_id
                );
                let body = json!({
                    "datasetReference": {
                        "projectId": self.config.project_id,
                        "datasetId": self.config.dataset
                    }
                });
                let response = Self::await_with_timeout(
                    format!("creating BigQuery emulator dataset {}", self.config.dataset),
                    BIGQUERY_REQUEST_TIMEOUT,
                    client.post(create_url).json(&body).send(),
                )
                .await?;
                if response.status().is_success() {
                    return Ok(());
                }
                anyhow::bail!(
                    "failed to create dataset in emulator: {}",
                    response.status()
                );
            }
            anyhow::bail!(
                "failed to fetch dataset from emulator: {}",
                response.status()
            );
        }

        match Self::await_bq_timeout(
            format!("fetching BigQuery dataset {}", self.config.dataset),
            BIGQUERY_REQUEST_TIMEOUT,
            self.client
                .dataset()
                .get(&self.config.project_id, &self.config.dataset),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => {
                let location = self
                    .config
                    .location
                    .clone()
                    .context("dataset not found; location required to create dataset")?;
                let mut dataset = gcloud_bigquery::http::dataset::Dataset::default();
                dataset.dataset_reference = gcloud_bigquery::http::dataset::DatasetReference {
                    project_id: self.config.project_id.clone(),
                    dataset_id: self.config.dataset.clone(),
                };
                dataset.location = location;
                Self::await_bq_timeout(
                    format!("creating BigQuery dataset {}", self.config.dataset),
                    BIGQUERY_REQUEST_TIMEOUT,
                    self.client.dataset().create(&dataset),
                )
                .await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn run_query(&self, sql: &str) -> Result<QueryResponse> {
        self.run_query_with_request_id(sql, None).await
    }

    async fn run_query_with_request_id(
        &self,
        sql: &str,
        request_id: Option<String>,
    ) -> Result<QueryResponse> {
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: self.config.location.clone().unwrap_or_default(),
            timeout_ms: Some(BIGQUERY_QUERY_REQUEST_TIMEOUT.as_millis() as i64),
            request_id,
            ..Default::default()
        };

        let response = if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/queries",
                emulator_http, self.config.project_id
            );
            let response = Self::await_with_timeout(
                "posting BigQuery emulator query",
                BIGQUERY_REQUEST_TIMEOUT,
                reqwest::Client::new().post(url).json(&request).send(),
            )
            .await?;
            Self::await_with_timeout(
                "decoding BigQuery emulator query response",
                BIGQUERY_REQUEST_TIMEOUT,
                response.json::<QueryResponse>(),
            )
            .await?
        } else {
            Self::await_with_timeout(
                "running BigQuery query",
                BIGQUERY_REQUEST_TIMEOUT,
                self.client.job().query(&self.config.project_id, &request),
            )
            .await?
        };

        let response = if response.job_complete {
            response
        } else {
            if self.config.emulator_http.is_some() {
                anyhow::bail!(
                    "BigQuery emulator query returned incomplete job {}",
                    response.job_reference.job_id
                );
            }
            self.wait_for_query_job_completion("BigQuery query", &response.job_reference)
                .await?;
            self.fetch_query_results_response(&response.job_reference)
                .await?
        };

        if let Some(errors) = &response.errors
            && !errors.is_empty()
        {
            error!(errors = ?errors, "BigQuery query returned errors");
            anyhow::bail!("BigQuery query returned errors: {:?}", errors);
        }

        Ok(response)
    }

    async fn wait_for_query_job_completion(
        &self,
        label: &str,
        job_reference: &JobReference,
    ) -> Result<()> {
        if job_reference.job_id.is_empty() {
            anyhow::bail!("{label} returned incomplete response without a BigQuery job id");
        }
        let location = job_reference
            .location
            .clone()
            .or_else(|| self.config.location.clone());
        let project_id = query_job_project_id(&self.config.project_id, job_reference);
        let started_at = std::time::Instant::now();
        let mut last_log_at = started_at;
        loop {
            let current = BigQueryDestination::await_with_timeout(
                format!("fetching BigQuery query job {}", job_reference.job_id),
                BIGQUERY_REQUEST_TIMEOUT,
                self.client.job().get(
                    project_id,
                    &job_reference.job_id,
                    &GetJobRequest {
                        location: location.clone(),
                    },
                ),
            )
            .await?;

            if query_job_completed(label, &current)? {
                return Ok(());
            }

            if last_log_at.elapsed() >= BIGQUERY_JOB_PROGRESS_LOG_INTERVAL {
                info!(
                    job_id = %job_reference.job_id,
                    state = ?current.status.state,
                    elapsed_secs = started_at.elapsed().as_secs(),
                    "waiting for BigQuery query job completion"
                );
                last_log_at = std::time::Instant::now();
            }

            if started_at.elapsed() >= BATCH_LOAD_JOB_HARD_TIMEOUT {
                anyhow::bail!(
                    "{} job {} exceeded hard timeout of {}s",
                    label,
                    job_reference.job_id,
                    BATCH_LOAD_JOB_HARD_TIMEOUT.as_secs()
                );
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn fetch_query_results_response(
        &self,
        job_reference: &JobReference,
    ) -> Result<QueryResponse> {
        let location = job_reference
            .location
            .clone()
            .or_else(|| self.config.location.clone());
        let token_source = self
            .bq_http_token_source
            .as_ref()
            .context("missing BigQuery HTTP token source")?;
        fetch_query_results_response_from_http(
            token_source.as_ref(),
            &self.bq_query_results_base_url,
            &self.config.project_id,
            job_reference,
            location,
        )
        .await
    }

    async fn ensure_table_internal(
        &self,
        schema: &TableSchema,
        table_id: &str,
        with_partition: bool,
    ) -> Result<()> {
        let ensure_started_at = std::time::Instant::now();
        info!(table = table_id, with_partition, "ensuring BigQuery table");
        info!(
            table = table_id,
            "ensuring BigQuery dataset before table ensure"
        );
        self.ensure_dataset().await?;
        info!(
            table = table_id,
            "BigQuery dataset ensured before table ensure"
        );
        let schema = with_metadata_schema(schema, &self.metadata);
        let desired_clustering = primary_key_clustering(&schema);
        let desired_fields = bq_fields_from_schema(&schema.columns);
        let bq_schema = BqTableSchema {
            fields: desired_fields.clone(),
        };

        if self.dry_run {
            info!("dry-run: ensure table {}", table_id);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}",
                emulator_http, self.config.project_id, self.config.dataset, table_id
            );
            let response = Self::await_with_timeout(
                format!("fetching BigQuery emulator table {}", table_id),
                BIGQUERY_REQUEST_TIMEOUT,
                client.get(&url).send(),
            )
            .await?;
            if response.status().is_success() {
                return Ok(());
            }
            if response.status() != reqwest::StatusCode::NOT_FOUND {
                anyhow::bail!("emulator table lookup failed: {}", response.status());
            }
            let mut body = json!({
                "tableReference": {
                    "projectId": self.config.project_id,
                    "datasetId": self.config.dataset,
                    "tableId": table_id
                },
                "schema": bq_schema
            });
            if with_partition && self.config.partition_by_synced_at.unwrap_or(false) {
                body["timePartitioning"] = json!({
                    "type": "DAY",
                    "field": self.metadata.synced_at
                });
            }
            let create_url = format!(
                "{}/projects/{}/datasets/{}/tables",
                emulator_http, self.config.project_id, self.config.dataset
            );
            let response = Self::await_with_timeout(
                format!("creating BigQuery emulator table {}", table_id),
                BIGQUERY_REQUEST_TIMEOUT,
                client.post(create_url).json(&body).send(),
            )
            .await?;
            if response.status().is_success() {
                return Ok(());
            }
            anyhow::bail!("emulator table create failed: {}", response.status());
        }

        info!(table = table_id, "fetching BigQuery table metadata");
        match Self::await_bq_timeout(
            format!("fetching BigQuery table {}", table_id),
            BIGQUERY_REQUEST_TIMEOUT,
            self.client
                .table()
                .get(&self.config.project_id, &self.config.dataset, table_id),
        )
        .await
        {
            Ok(mut table) => {
                info!(table = table_id, "fetched BigQuery table metadata");
                if update_table_for_missing_fields_and_clustering(
                    &mut table,
                    &desired_fields,
                    desired_clustering.as_ref(),
                ) {
                    info!(table = table_id, "patching BigQuery table schema");
                    Self::await_bq_timeout(
                        format!("patching BigQuery table {}", table_id),
                        BIGQUERY_REQUEST_TIMEOUT,
                        self.client.table().patch(&table),
                    )
                    .await?;
                    info!(table = table_id, "patched BigQuery table schema");
                }
                info!(
                    table = table_id,
                    ensure_ms = ensure_started_at.elapsed().as_millis() as u64,
                    action = "patch-or-noop-existing",
                    "BigQuery table ensured"
                );
                Ok(())
            }
            Err(BqError::Response(err)) if err.code == 404 => {
                let table = Table {
                    table_reference: TableReference {
                        project_id: self.config.project_id.clone(),
                        dataset_id: self.config.dataset.clone(),
                        table_id: table_id.to_string(),
                    },
                    time_partitioning: if with_partition
                        && self.config.partition_by_synced_at.unwrap_or(false)
                    {
                        Some(TimePartitioning {
                            partition_type: TimePartitionType::Day,
                            expiration_ms: None,
                            field: Some(self.metadata.synced_at.clone()),
                        })
                    } else {
                        None
                    },
                    schema: Some(bq_schema),
                    clustering: desired_clustering,
                    ..Default::default()
                };
                info!(table = table_id, "creating BigQuery table");
                Self::await_bq_timeout(
                    format!("creating BigQuery table {}", table_id),
                    BIGQUERY_REQUEST_TIMEOUT,
                    self.client.table().create(&table),
                )
                .await?;
                info!(table = table_id, "created BigQuery table");
                info!(
                    table = table_id,
                    ensure_ms = ensure_started_at.elapsed().as_millis() as u64,
                    action = "created",
                    "BigQuery table ensured"
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn append_rows(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
        primary_key: Option<&str>,
    ) -> Result<()> {
        if frame.height() == 0 {
            return Ok(());
        }
        info!(
            table = table_id,
            rows = frame.height(),
            batch_load_enabled = self.config.emulator_http.is_none(),
            emulator_enabled = self.config.emulator_http.is_some(),
            has_primary_key = primary_key.is_some(),
            "append_rows entry"
        );
        if self.dry_run {
            info!("dry-run: insert {} rows into {}", frame.height(), table_id);
            return Ok(());
        }
        if self.config.emulator_http.is_some() {
            return self.append_rows_via_emulator(table_id, frame).await;
        }
        self.append_rows_via_batch_load(table_id, schema, frame)
            .await?;
        Ok(())
    }

    async fn append_rows_via_emulator(&self, table_id: &str, frame: &DataFrame) -> Result<()> {
        let rows = emulator_insert_rows(frame).await?;
        let request = json!({
            "rows": rows.into_iter().map(|row| json!({ "json": row })).collect::<Vec<_>>()
        });
        let emulator_http = self
            .config
            .emulator_http
            .as_deref()
            .context("missing bigquery.emulator_http")?;
        let url = format!(
            "{}/projects/{}/datasets/{}/tables/{}/insertAll",
            emulator_http, self.config.project_id, self.config.dataset, table_id
        );
        let response = Self::await_with_timeout(
            format!("posting emulator insertAll for {}", table_id),
            BIGQUERY_REQUEST_TIMEOUT,
            reqwest::Client::new().post(url).json(&request).send(),
        )
        .await?;
        if !response.status().is_success() {
            error!(
                table = %table_id,
                status = %response.status(),
                rows = frame.height(),
                "BigQuery emulator insert request failed"
            );
            anyhow::bail!("emulator insert failed: {}", response.status());
        }
        let payload: serde_json::Value = Self::await_with_timeout(
            format!("decoding emulator insertAll response for {}", table_id),
            BIGQUERY_REQUEST_TIMEOUT,
            response.json(),
        )
        .await?;
        if let Some(errors) = payload.get("insertErrors") {
            let row_errors = errors
                .as_array()
                .map_or(1_u64, |rows| u64::try_from(rows.len()).unwrap_or(u64::MAX));
            crate::telemetry::record_bigquery_row_errors(table_id, row_errors);
            error!(
                table = %table_id,
                row_errors,
                rows = frame.height(),
                "BigQuery emulator insert returned row errors"
            );
            anyhow::bail!(
                "BigQuery emulator insert errors for {}: {}",
                table_id,
                errors
            );
        }
        Ok(())
    }

    pub(crate) async fn upload_cdc_batch_load_artifact_with_object_name(
        &self,
        schema: &TableSchema,
        frame: &DataFrame,
        object_name: &str,
    ) -> Result<String> {
        if !self.cdc_batch_load_queue_enabled() {
            anyhow::bail!("CDC batch-load queue requires batch_load_bucket");
        }
        let bucket = self
            .config
            .batch_load_bucket
            .as_deref()
            .context("missing batch_load_bucket")?;
        let token_source = self
            .gcs_token_source
            .as_ref()
            .context("missing GCS token source for CDC batch-load queue")?;

        let schema = with_metadata_schema(schema, &self.metadata);
        let object_uri = format!("gs://{}/{}", bucket, object_name);
        let body = batch_load::parquet_payload(frame, &schema).await?;
        self.upload_batch_load_object(
            token_source,
            bucket,
            object_name,
            batch_load::PARQUET_CONTENT_TYPE,
            body,
        )
        .await
        .with_context(|| format!("uploading CDC batch load object {}", object_uri))?;
        Ok(object_uri)
    }

    pub(crate) async fn stage_cdc_batch_load_job(
        &self,
        connection_id: &str,
        payload: &CdcBatchLoadJobPayload,
    ) -> Result<()> {
        let staging_schema = payload.staging_schema.as_ref().unwrap_or(&payload.schema);
        for step in &payload.steps {
            info!(
                component = "consumer",
                event = "cdc_consumer_ensure_staging_started",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count,
                "ensuring staging table for queued BigQuery merge"
            );
            let ensure_staging_started_at = std::time::Instant::now();
            let ensure_staging_span = info_span!(
                "cdc_batch_load_job.ensure_staging",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count
            );
            {
                self.ensure_table_internal(staging_schema, &step.staging_table, false)
                    .instrument(ensure_staging_span)
                    .await?;
            }
            crate::telemetry::record_cdc_batch_load_stage_duration(
                connection_id,
                &payload.target_table,
                "ensure_staging",
                ensure_staging_started_at.elapsed().as_secs_f64() * 1000.0,
                step.row_count as u64,
            );
            info!(
                component = "consumer",
                event = "cdc_consumer_ensure_staging_completed",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count,
                duration_ms = ensure_staging_started_at.elapsed().as_millis() as u64,
                "queued staging table ensured for BigQuery merge"
            );
            let load_started_at = std::time::Instant::now();
            let load_span = info_span!(
                "cdc_batch_load_job.load_job",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count
            );
            {
                self.run_load_job(
                    &step.staging_table,
                    &with_metadata_schema(staging_schema, &self.metadata),
                    &step.object_uri,
                    gcloud_bigquery::http::job::WriteDisposition::WriteTruncate,
                    step.load_job_id.as_deref(),
                )
                .instrument(load_span)
                .await?;
            }
            crate::telemetry::record_cdc_batch_load_stage_duration(
                connection_id,
                &payload.target_table,
                "load_job",
                load_started_at.elapsed().as_secs_f64() * 1000.0,
                step.row_count as u64,
            );
            info!(
                component = "consumer",
                event = "cdc_consumer_load_job_completed",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count,
                duration_ms = load_started_at.elapsed().as_millis() as u64,
                "queued BigQuery load job completed"
            );
        }
        Ok(())
    }

    pub(crate) async fn apply_cdc_batch_load_job(
        &self,
        connection_id: &str,
        payload: &CdcBatchLoadJobPayload,
        apply_attempt_key: &str,
    ) -> Result<()> {
        let target_started_at = std::time::Instant::now();
        let target_span = info_span!(
            "cdc_batch_load_job.ensure_target",
            connection_id = connection_id,
            table = %payload.target_table
        );
        {
            self.ensure_table(&payload.schema)
                .instrument(target_span)
                .await?;
        }
        crate::telemetry::record_cdc_batch_load_stage_duration(
            connection_id,
            &payload.target_table,
            "ensure_target",
            target_started_at.elapsed().as_secs_f64() * 1000.0,
            payload.steps.iter().map(|step| step.row_count as u64).sum(),
        );
        info!(
            component = "consumer",
            event = "cdc_consumer_ensure_target_completed",
            connection_id = connection_id,
            table = %payload.target_table,
            duration_ms = target_started_at.elapsed().as_millis() as u64,
            "queued ensure target completed"
        );
        if payload.truncate {
            let truncate_started_at = std::time::Instant::now();
            self.truncate_table(&payload.target_table).await?;
            info!(
                table = %payload.target_table,
                duration_ms = truncate_started_at.elapsed().as_millis() as u64,
                "queued truncate target completed"
            );
        }
        if payload.staging_schema.is_some() && !payload.steps.is_empty() {
            let merge_started_at = std::time::Instant::now();
            info!(
                component = "consumer",
                event = "cdc_consumer_reducer_merge_started",
                connection_id = connection_id,
                table = %payload.target_table,
                step_count = payload.steps.len(),
                rows = payload.steps.iter().map(|step| step.row_count).sum::<usize>(),
                "starting reduced queued BigQuery merge from staging tables"
            );
            let merge_span = info_span!(
                "cdc_batch_load_job.reducer_merge",
                connection_id = connection_id,
                table = %payload.target_table,
                step_count = payload.steps.len()
            );
            self.merge_cdc_staging_steps(
                &payload.target_table,
                &payload.steps,
                &payload.schema,
                &payload.primary_key,
                Some(apply_attempt_key),
            )
            .instrument(merge_span)
            .await?;
            crate::telemetry::record_cdc_batch_load_stage_duration(
                connection_id,
                &payload.target_table,
                "merge",
                merge_started_at.elapsed().as_secs_f64() * 1000.0,
                payload.steps.iter().map(|step| step.row_count as u64).sum(),
            );
            info!(
                component = "consumer",
                event = "cdc_consumer_reducer_merge_completed",
                connection_id = connection_id,
                table = %payload.target_table,
                step_count = payload.steps.len(),
                duration_ms = merge_started_at.elapsed().as_millis() as u64,
                "completed reduced queued BigQuery merge from staging tables"
            );
            for step in &payload.steps {
                self.spawn_drop_table_if_exists(step.staging_table.clone())
                    .await;
            }
            return Ok(());
        }
        for step in &payload.steps {
            info!(
                component = "consumer",
                event = "cdc_consumer_merge_started",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count,
                "starting queued BigQuery merge from staging table"
            );
            let merge_started_at = std::time::Instant::now();
            let merge_span = info_span!(
                "cdc_batch_load_job.merge",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count
            );
            {
                self.merge_staging(
                    &payload.target_table,
                    &step.staging_table,
                    &payload.schema,
                    &payload.primary_key,
                    Some(apply_attempt_key),
                )
                .instrument(merge_span)
                .await?;
            }
            crate::telemetry::record_cdc_batch_load_stage_duration(
                connection_id,
                &payload.target_table,
                "merge",
                merge_started_at.elapsed().as_secs_f64() * 1000.0,
                step.row_count as u64,
            );
            info!(
                component = "consumer",
                event = "cdc_consumer_merge_completed",
                connection_id = connection_id,
                table = %payload.target_table,
                staging_table = %step.staging_table,
                rows = step.row_count,
                duration_ms = merge_started_at.elapsed().as_millis() as u64,
                "completed queued BigQuery merge from staging table"
            );
            self.spawn_drop_table_if_exists(step.staging_table.clone())
                .await;
        }
        Ok(())
    }

    async fn merge_staging(
        &self,
        target: &str,
        staging: &str,
        schema: &TableSchema,
        primary_key: &str,
        apply_attempt_key: Option<&str>,
    ) -> Result<()> {
        if self.dry_run {
            info!("dry-run: merge staging {} into {}", staging, target);
            return Ok(());
        }

        let merge_lock = self.table_merge_lock(target).await;
        let _merge_permit = merge_lock
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("failed to acquire BigQuery merge lock for {}", target))?;

        let schema = with_metadata_schema(schema, &self.metadata);
        let sql = staging_merge_sql(
            &self.config.project_id,
            &self.config.dataset,
            target,
            staging,
            &schema,
            primary_key,
        );

        self.run_query_with_request_id(
            &sql,
            Some(merge_query_request_id(
                &self.config.dataset,
                target,
                staging,
                apply_attempt_key,
            )),
        )
        .await
        .with_context(|| format!("merging staging BigQuery table {} into {}", staging, target))?;
        Ok(())
    }

    async fn merge_cdc_staging_steps(
        &self,
        target: &str,
        steps: &[CdcBatchLoadJobStep],
        schema: &TableSchema,
        primary_key: &str,
        apply_attempt_key: Option<&str>,
    ) -> Result<()> {
        if steps.is_empty() {
            return Ok(());
        }
        if self.dry_run {
            info!(
                "dry-run: reduced merge {} staging tables into {}",
                steps.len(),
                target
            );
            return Ok(());
        }

        let merge_lock = self.table_merge_lock(target).await;
        let _merge_permit = merge_lock
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("failed to acquire BigQuery merge lock for {}", target))?;

        let sql = cdc_reducer_merge_sql(
            &self.config.project_id,
            &self.config.dataset,
            target,
            steps,
            schema,
            &self.metadata,
            primary_key,
        );
        let staging_key = steps
            .iter()
            .map(|step| step.staging_table.as_str())
            .collect::<Vec<_>>()
            .join(",");
        self.run_query_with_request_id(
            &sql,
            Some(merge_query_request_id(
                &self.config.dataset,
                target,
                &staging_key,
                apply_attempt_key,
            )),
        )
        .await
        .with_context(|| {
            format!(
                "merging {} CDC staging BigQuery tables into {}",
                steps.len(),
                target
            )
        })?;
        Ok(())
    }

    async fn merge_change_batch_staging(
        &self,
        target: &str,
        staging: &str,
        schema: &TableSchema,
        primary_key: &str,
    ) -> Result<()> {
        if self.dry_run {
            info!("dry-run: merge change staging {} into {}", staging, target);
            return Ok(());
        }

        if self.config.emulator_http.is_some() {
            return self
                .merge_staging(target, staging, schema, primary_key, None)
                .await;
        }

        let merge_lock = self.table_merge_lock(target).await;
        let _merge_permit = merge_lock
            .acquire_owned()
            .await
            .map_err(|_| anyhow::anyhow!("failed to acquire BigQuery merge lock for {}", target))?;
        let sql = dynamodb_change_merge_sql(
            &self.config.project_id,
            &self.config.dataset,
            target,
            staging,
            schema,
            &self.metadata,
            primary_key,
        );
        self.run_query_with_request_id(
            &sql,
            Some(stable_query_request_id(
                "dynamodb_merge",
                &format!("{}:{}:{}", self.config.dataset, target, staging),
            )),
        )
        .await
        .with_context(|| format!("merging change staging {} into {}", staging, target))?;
        Ok(())
    }

    async fn drop_table_if_exists(&self, table: &str) -> Result<()> {
        if self.dry_run {
            info!("dry-run: drop {}", table);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}",
                emulator_http, self.config.project_id, self.config.dataset, table
            );
            let response = Self::await_with_timeout(
                format!("dropping emulator BigQuery table {}", table),
                BIGQUERY_REQUEST_TIMEOUT,
                client.delete(url).send(),
            )
            .await?;
            if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            anyhow::bail!("emulator delete table failed: {}", response.status());
        }
        match Self::await_bq_timeout(
            format!("dropping BigQuery table {}", table),
            BIGQUERY_REQUEST_TIMEOUT,
            self.client
                .table()
                .delete(&self.config.project_id, &self.config.dataset, table),
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn spawn_drop_table_if_exists(&self, table: String) {
        if self.dry_run {
            return;
        }

        if let Err(err) = reap_finished_cleanup_task_handles(&self.cleanup_tasks).await {
            warn!(error = %err, "failed to reap completed BigQuery cleanup tasks");
        }

        let dest = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = dest.drop_table_if_exists(&table).await {
                warn!(
                    table = %table,
                    error = %err,
                    "failed to drop BigQuery staging table after upsert"
                );
            }
        });
        self.cleanup_tasks.lock().await.push(handle);
    }

    async fn drain_cleanup_tasks(&self) -> Result<()> {
        drain_cleanup_task_handles(&self.cleanup_tasks).await
    }
}

async fn reap_finished_cleanup_task_handles(
    cleanup_tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>,
) -> Result<()> {
    let finished = {
        let mut guard = cleanup_tasks.lock().await;
        let mut finished = Vec::new();
        let mut idx = 0;
        while idx < guard.len() {
            if guard[idx].is_finished() {
                finished.push(guard.swap_remove(idx));
            } else {
                idx += 1;
            }
        }
        finished
    };

    join_cleanup_task_handles(finished).await
}

async fn drain_cleanup_task_handles(cleanup_tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>) -> Result<()> {
    let handles = {
        let mut guard = cleanup_tasks.lock().await;
        std::mem::take(&mut *guard)
    };

    join_cleanup_task_handles(handles).await
}

async fn join_cleanup_task_handles(handles: Vec<JoinHandle<()>>) -> Result<()> {
    for handle in handles {
        handle
            .await
            .map_err(|err| anyhow::anyhow!("cleanup task join failed: {}", err))?;
    }

    Ok(())
}

#[async_trait]
impl Destination for BigQueryDestination {
    async fn ensure_table(&self, schema: &TableSchema) -> Result<()> {
        self.ensure_table_internal(schema, &schema.name, true).await
    }

    async fn truncate_table(&self, table: &str) -> Result<()> {
        if self.dry_run {
            info!("dry-run: truncate {}", table);
            return Ok(());
        }
        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}",
                emulator_http, self.config.project_id, self.config.dataset, table
            );
            let response = Self::await_with_timeout(
                format!("truncating emulator BigQuery table {}", table),
                BIGQUERY_REQUEST_TIMEOUT,
                client.delete(url).send(),
            )
            .await?;
            if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            error!(
                table = %table,
                status = %response.status(),
                "BigQuery emulator delete table failed"
            );
            anyhow::bail!("emulator delete table failed: {}", response.status());
        }
        let sql = format!(
            "TRUNCATE TABLE `{project}.{dataset}.{table}`",
            project = self.config.project_id,
            dataset = self.config.dataset,
            table = table
        );
        self.run_query(&sql)
            .await
            .with_context(|| format!("truncating BigQuery table {}", table))?;
        Ok(())
    }

    async fn write_batch(
        &self,
        table: &str,
        schema: &TableSchema,
        frame: &DataFrame,
        mode: WriteMode,
        primary_key: Option<&str>,
    ) -> Result<()> {
        match mode {
            WriteMode::Append => {
                self.append_rows(table, schema, frame, primary_key).await?;
            }
            WriteMode::Upsert => {
                if let Some(pk) = primary_key {
                    if self.config.emulator_http.is_some() {
                        warn!(
                            table,
                            "bigquery emulator: upsert unsupported; falling back to append"
                        );
                        self.append_rows(table, schema, frame, Some(pk)).await?;
                        return Ok(());
                    }
                    let staging = upsert_staging_table_id(table);
                    info!(
                        table = table,
                        staging_table = %staging,
                        rows = frame.height(),
                        "ensuring staging table for BigQuery merge"
                    );
                    self.ensure_table_internal(schema, &staging, false).await?;
                    info!(
                        table = table,
                        staging_table = %staging,
                        rows = frame.height(),
                        "staging table ensured for BigQuery merge"
                    );
                    let result: Result<()> = async {
                        self.append_rows(&staging, schema, frame, Some(pk)).await?;
                        info!(
                            table = table,
                            staging_table = %staging,
                            rows = frame.height(),
                            "starting BigQuery merge from staging table"
                        );
                        self.merge_staging(table, &staging, schema, pk, None).await
                    }
                    .await;
                    if result.is_ok() {
                        info!(
                            table = table,
                            staging_table = %staging,
                            rows = frame.height(),
                            "completed BigQuery merge from staging table"
                        );
                    }
                    self.spawn_drop_table_if_exists(staging).await;
                    result?;
                } else {
                    warn!("no primary key for {table}; falling back to append");
                    self.append_rows(table, schema, frame, None).await?;
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.drain_cleanup_tasks().await
    }
}

#[async_trait]
impl ChangeApplier for BigQueryDestination {
    async fn apply_change_batch(&self, batch: &SourceChangeBatch) -> Result<()> {
        if batch.records.is_empty() {
            return Ok(());
        }

        self.ensure_table(&batch.entity.schema).await?;
        let staging = upsert_staging_table_id(&batch.entity.destination_name);
        self.ensure_table_internal(&batch.entity.schema, &staging, false)
            .await?;

        let records = batch
            .records
            .iter()
            .map(|record| record.record.clone())
            .collect::<Vec<_>>();
        let frame = records_to_dataframe(&batch.entity.schema, &records, &self.metadata)?;

        let result = async {
            self.append_rows(
                &staging,
                &batch.entity.schema,
                &frame,
                Some(batch.entity.primary_key.as_str()),
            )
            .await?;
            self.merge_change_batch_staging(
                &batch.entity.destination_name,
                &staging,
                &batch.entity.schema,
                &batch.entity.primary_key,
            )
            .await
        }
        .await;

        self.spawn_drop_table_if_exists(staging).await;
        result
    }
}

async fn emulator_insert_rows(frame: &DataFrame) -> Result<Vec<Map<String, Value>>> {
    if frame.height() < EMULATOR_INSERT_BLOCKING_ROWS {
        return dataframe_to_json_rows(frame);
    }

    let frame = frame.clone();
    task::spawn_blocking(move || dataframe_to_json_rows(&frame))
        .await
        .map_err(|err| anyhow::anyhow!("failed to join emulator row conversion task: {}", err))?
}

pub(crate) fn upsert_staging_table_id(table: &str) -> String {
    format!("{table}_staging_{}", Uuid::new_v4().simple())
}

pub(crate) fn stable_cdc_staging_table_id(table: &str, job_id: &str, step_kind: &str) -> String {
    let suffix = job_id.rsplit('_').next().unwrap_or(job_id);
    format!("{table}_staging_{step_kind}_{suffix}")
}

pub(crate) fn stable_cdc_batch_load_object_name(
    prefix: Option<&str>,
    table: &str,
    key: &str,
) -> String {
    batch_load::batch_load_object_name_for_key(
        prefix,
        table,
        key,
        batch_load::PARQUET_FILE_EXTENSION,
    )
}

fn primary_key_column<'a>(schema: &'a TableSchema, primary_key: &str) -> Option<&'a ColumnSchema> {
    schema
        .columns
        .iter()
        .find(|column| column.name == primary_key)
}

fn primary_key_clustering(schema: &TableSchema) -> Option<Clustering> {
    let primary_key = schema.primary_key.as_deref()?;
    let column = primary_key_column(schema, primary_key)?;
    bigquery_clustering_supported(&column.data_type).then(|| Clustering {
        fields: vec![primary_key.to_string()],
    })
}

fn update_table_for_missing_fields_and_clustering(
    table: &mut Table,
    desired_fields: &[gcloud_bigquery::http::table::TableFieldSchema],
    desired_clustering: Option<&Clustering>,
) -> bool {
    let existing_fields = table
        .schema
        .as_ref()
        .map(|schema| schema.fields.clone())
        .unwrap_or_default();
    let existing_names: HashSet<String> = existing_fields
        .iter()
        .map(|field| field.name.to_lowercase())
        .collect();

    let mut updated_fields = existing_fields;
    for field in desired_fields {
        if !existing_names.contains(&field.name.to_lowercase()) {
            updated_fields.push(field.clone());
        }
    }

    let mut needs_patch = false;
    if updated_fields.len() != existing_names.len() {
        table.schema = Some(BqTableSchema {
            fields: updated_fields,
        });
        needs_patch = true;
    }
    if let Some(clustering) = desired_clustering
        && table.clustering.as_ref() != Some(clustering)
    {
        table.clustering = Some(clustering.clone());
        needs_patch = true;
    }

    needs_patch
}

fn bigquery_clustering_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::String
            | DataType::Int64
            | DataType::Bool
            | DataType::Timestamp
            | DataType::Date
            | DataType::Bytes
            | DataType::Numeric
    )
}

fn primary_key_range_type(schema: &TableSchema, primary_key: &str) -> Option<&'static str> {
    let column = primary_key_column(schema, primary_key)?;
    match &column.data_type {
        DataType::String | DataType::Bytes => Some("STRING"),
        DataType::Int64 => Some("INT64"),
        DataType::Bool => Some("BOOL"),
        DataType::Timestamp => Some("TIMESTAMP"),
        DataType::Date => Some("DATE"),
        DataType::Numeric => Some("BIGNUMERIC"),
        DataType::Float64 | DataType::Interval | DataType::Json => None,
    }
}

fn primary_key_range_merge_parts(
    schema: &TableSchema,
    primary_key: &str,
    range_source_sql: &str,
) -> (String, String) {
    let Some(range_type) = primary_key_range_type(schema, primary_key) else {
        return (String::new(), String::new());
    };
    let pk = bq_ident(primary_key);
    let declaration = format!(
        "DECLARE _cdsync_pk_range STRUCT<min_pk {range_type}, max_pk {range_type}> DEFAULT \
         (SELECT AS STRUCT MIN({pk}) AS min_pk, MAX({pk}) AS max_pk FROM ({range_source_sql})); "
    );
    let predicate =
        format!(" AND T.{pk} BETWEEN _cdsync_pk_range.min_pk AND _cdsync_pk_range.max_pk");
    (declaration, predicate)
}

fn table_primary_key_range_source(
    project: &str,
    dataset: &str,
    table: &str,
    primary_key: &str,
) -> String {
    format!(
        "SELECT {pk} FROM `{project}.{dataset}.{table}`",
        pk = bq_ident(primary_key)
    )
}

fn staging_merge_sql(
    project: &str,
    dataset: &str,
    target: &str,
    staging: &str,
    schema: &TableSchema,
    primary_key: &str,
) -> String {
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let updates: Vec<String> = columns
        .iter()
        .map(|col| format!("{} = S.{}", bq_ident(col), bq_ident(col)))
        .collect();
    let insert_cols: Vec<String> = columns.iter().map(|col| bq_ident(col)).collect();
    let insert_vals: Vec<String> = columns
        .iter()
        .map(|col| format!("S.{}", bq_ident(col)))
        .collect();
    let pk = bq_ident(primary_key);
    let range_source = table_primary_key_range_source(project, dataset, staging, primary_key);
    let (range_declaration, target_range_predicate) =
        primary_key_range_merge_parts(schema, primary_key, &range_source);

    format!(
        "{range_declaration}MERGE `{project}.{dataset}.{target}` T USING `{project}.{dataset}.{staging}` S \
         ON T.{pk} = S.{pk}{target_range_predicate} \
         WHEN MATCHED THEN UPDATE SET {updates} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        updates = updates.join(", "),
        insert_cols = insert_cols.join(", "),
        insert_vals = insert_vals.join(", ")
    )
}

fn cdc_reducer_merge_sql(
    project: &str,
    dataset: &str,
    target: &str,
    steps: &[CdcBatchLoadJobStep],
    schema: &TableSchema,
    metadata: &MetadataColumns,
    primary_key: &str,
) -> String {
    let schema = with_metadata_schema(schema, metadata);
    let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let projection = columns
        .iter()
        .map(|col| bq_ident(col))
        .collect::<Vec<_>>()
        .join(", ");
    let mut reducer_columns = columns.iter().map(|col| bq_ident(col)).collect::<Vec<_>>();
    reducer_columns.push(bq_ident(CDC_STAGING_SEQUENCE_COLUMN));
    reducer_columns.push(bq_ident(CDC_STAGING_TX_ORDINAL_COLUMN));
    let reducer_projection = reducer_columns.join(", ");
    let pk = bq_ident(primary_key);
    let union_sources = steps
        .iter()
        .map(|step| {
            format!(
                "SELECT {reducer_projection} FROM `{project}.{dataset}.{staging}`",
                staging = step.staging_table
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let range_sources = steps
        .iter()
        .map(|step| {
            format!(
                "SELECT {pk} FROM `{project}.{dataset}.{staging}`",
                staging = step.staging_table
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let updates = columns
        .iter()
        .map(|col| format!("{} = S.{}", bq_ident(col), bq_ident(col)))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = columns
        .iter()
        .map(|col| bq_ident(col))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|col| format!("S.{}", bq_ident(col)))
        .collect::<Vec<_>>()
        .join(", ");
    let sequence = bq_ident(CDC_STAGING_SEQUENCE_COLUMN);
    let tx_ordinal = bq_ident(CDC_STAGING_TX_ORDINAL_COLUMN);
    let (range_declaration, target_range_predicate) =
        primary_key_range_merge_parts(&schema, primary_key, &range_sources);

    format!(
        "{range_declaration}MERGE `{project}.{dataset}.{target}` T USING (\
         SELECT {projection} FROM (\
         SELECT {projection}, ROW_NUMBER() OVER (PARTITION BY {pk} \
         ORDER BY {sequence} DESC, {tx_ordinal} DESC) AS _cdsync_reducer_rank \
         FROM ({union_sources}) _cdsync_reducer_source) \
         WHERE _cdsync_reducer_rank = 1) S ON T.{pk} = S.{pk}{target_range_predicate} \
         WHEN MATCHED THEN UPDATE SET {updates} \
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
}

fn dynamodb_change_merge_sql(
    project: &str,
    dataset: &str,
    target: &str,
    staging: &str,
    schema: &TableSchema,
    metadata: &MetadataColumns,
    primary_key: &str,
) -> String {
    let schema = with_metadata_schema(schema, metadata);
    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let projection = columns
        .iter()
        .map(|column| bq_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let updates = columns
        .iter()
        .map(|column| format!("{} = S.{}", bq_ident(column), bq_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = columns
        .iter()
        .map(|column| bq_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|column| format!("S.{}", bq_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let pk = bq_ident(primary_key);
    let source_event_at = bq_ident(META_SOURCE_EVENT_AT);
    let source_event_id = bq_ident(META_SOURCE_EVENT_ID);
    let deleted_at = bq_ident(&metadata.deleted_at);
    let range_source = table_primary_key_range_source(project, dataset, staging, primary_key);
    let (range_declaration, target_range_predicate) =
        primary_key_range_merge_parts(&schema, primary_key, &range_source);

    format!(
        "{range_declaration}MERGE `{project}.{dataset}.{target}` T USING (\
         SELECT {projection} FROM (\
         SELECT {projection}, ROW_NUMBER() OVER (PARTITION BY {pk} \
         ORDER BY {source_event_at} DESC, {source_event_id} DESC, {deleted_at} DESC) AS _cdsync_rank \
         FROM `{project}.{dataset}.{staging}`) \
         WHERE _cdsync_rank = 1) S ON T.{pk} = S.{pk}{target_range_predicate} \
         WHEN MATCHED AND (\
             T.{source_event_at} IS NULL \
             OR S.{source_event_at} > T.{source_event_at} \
             OR (S.{source_event_at} = T.{source_event_at} AND S.{source_event_id} >= T.{source_event_id})\
         ) THEN UPDATE SET {updates} \
         WHEN NOT MATCHED AND S.{deleted_at} IS NULL THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
}

fn stable_query_request_id(prefix: &str, key: &str) -> String {
    const BIGQUERY_REQUEST_ID_MAX_LEN: usize = 36;
    const DEFAULT_HASH_LEN: usize = 24;

    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b"\0");
    hasher.update(key.as_bytes());
    let digest = hex::encode(hasher.finalize());
    let hash_len = DEFAULT_HASH_LEN.min(
        BIGQUERY_REQUEST_ID_MAX_LEN
            .checked_sub(prefix.len() + 1)
            .expect("BigQuery request id prefix must leave room for a hash suffix"),
    );
    format!("{prefix}_{}", &digest[..hash_len])
}

fn merge_query_request_id(
    dataset: &str,
    target: &str,
    staging_key: &str,
    apply_attempt_key: Option<&str>,
) -> String {
    let base = format!("{dataset}:{target}:{staging_key}");
    let key = if let Some(attempt) = apply_attempt_key {
        format!("{base}:{attempt}")
    } else {
        base
    };
    stable_query_request_id("merge", &key)
}

fn query_job_completed(label: &str, job: &Job) -> Result<bool> {
    if job.status.state != JobState::Done {
        return Ok(false);
    }
    if let Some(error_result) = &job.status.error_result {
        anyhow::bail!(
            "{} job {} failed: {:?}",
            label,
            job.job_reference.job_id,
            error_result
        );
    }
    if let Some(errors) = &job.status.errors
        && !errors.is_empty()
    {
        anyhow::bail!(
            "{} job {} reported errors: {:?}",
            label,
            job.job_reference.job_id,
            errors
        );
    }
    Ok(true)
}

fn query_job_project_id<'a>(fallback: &'a str, job_reference: &'a JobReference) -> &'a str {
    if job_reference.project_id.is_empty() {
        fallback
    } else {
        &job_reference.project_id
    }
}

async fn fetch_query_results_response_from_http(
    token_source: &dyn TokenSource,
    base_url: &str,
    fallback_project_id: &str,
    job_reference: &JobReference,
    location: Option<String>,
) -> Result<QueryResponse> {
    let project_id = query_job_project_id(fallback_project_id, job_reference);
    let token = token_source
        .token()
        .await
        .map_err(|err| anyhow::anyhow!("BigQuery HTTP token source failed: {err}"))?;
    let mut url = Url::parse(base_url).context("building BigQuery getQueryResults URL")?;
    url.path_segments_mut()
        .map_err(|()| anyhow::anyhow!("BigQuery getQueryResults URL cannot be a base"))?
        .extend([
            "projects",
            project_id,
            "queries",
            job_reference.job_id.as_str(),
        ]);
    let request = GetQueryResultsRequest {
        start_index: 0,
        page_token: None,
        max_results: None,
        timeout_ms: Some(BIGQUERY_QUERY_REQUEST_TIMEOUT.as_millis() as i64),
        location,
        format_options: None,
    };
    let response = BigQueryDestination::await_with_timeout(
        format!(
            "fetching BigQuery query results for {}",
            job_reference.job_id
        ),
        BIGQUERY_REQUEST_TIMEOUT,
        reqwest::Client::new()
            .get(url)
            .header("X-Goog-Api-Client", "rust")
            .header(reqwest::header::USER_AGENT, "google-cloud-bigquery")
            .header(reqwest::header::AUTHORIZATION, token)
            .query(&request)
            .send(),
    )
    .await?;
    let status = response.status();
    if !status.is_success() {
        let body = BigQueryDestination::await_with_timeout(
            format!(
                "decoding BigQuery query results error for {}",
                job_reference.job_id
            ),
            BIGQUERY_REQUEST_TIMEOUT,
            response.text(),
        )
        .await
        .unwrap_or_else(|err| format!("<failed to decode response body: {err}>"));
        anyhow::bail!(
            "fetching BigQuery query results for {} failed with {}: {}",
            job_reference.job_id,
            status,
            body
        );
    }
    let result = BigQueryDestination::await_with_timeout(
        format!(
            "decoding BigQuery query results for {}",
            job_reference.job_id
        ),
        BIGQUERY_REQUEST_TIMEOUT,
        response.json::<QueryResponse>(),
    )
    .await?;
    get_query_results_to_query_response(result)
}

fn get_query_results_to_query_response(result: QueryResponse) -> Result<QueryResponse> {
    if !result.job_complete {
        anyhow::bail!(
            "BigQuery query results for job {} were still incomplete after job reached DONE",
            result.job_reference.job_id
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{
        BIGQUERY_REQUEST_TIMEOUT, BigQueryDestination, CdcBatchLoadJobStep, bq_fields_from_schema,
        cdc_reducer_merge_sql, drain_cleanup_task_handles, dynamodb_change_merge_sql,
        fetch_query_results_response_from_http, get_query_results_to_query_response,
        merge_query_request_id, primary_key_clustering, query_job_completed,
        reap_finished_cleanup_task_handles, stable_query_request_id, staging_merge_sql,
        update_table_for_missing_fields_and_clustering, upsert_staging_table_id,
    };
    use crate::types::{
        ColumnSchema, DataType, META_SOURCE_EVENT_AT, META_SOURCE_EVENT_ID, MetadataColumns,
        TableSchema,
    };
    use gcloud_bigquery::http::error::Error as BqError;
    use gcloud_bigquery::http::job::query::QueryResponse;
    use gcloud_bigquery::http::job::{
        Job, JobConfiguration, JobReference, JobState, JobStatus, JobType,
    };
    use gcloud_bigquery::http::table::{Clustering, Table, TableSchema as BqTableSchema};
    use serde_json::json;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use token_source::TokenSource;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Debug)]
    struct StaticTokenSource(&'static str);

    #[async_trait::async_trait]
    impl TokenSource for StaticTokenSource {
        async fn token(
            &self,
        ) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.0.to_string())
        }
    }

    #[test]
    fn upsert_staging_table_id_is_unique_per_write() {
        let first = upsert_staging_table_id("public__accounts");
        let second = upsert_staging_table_id("public__accounts");

        assert_ne!(first, second);
        assert!(first.starts_with("public__accounts_staging_"));
        assert!(second.starts_with("public__accounts_staging_"));
    }

    #[test]
    fn primary_key_clustering_uses_supported_primary_key() {
        let schema = TableSchema {
            name: "public__accounts".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };

        let clustering = primary_key_clustering(&schema).expect("clusterable primary key");
        assert_eq!(clustering.fields, vec!["id"]);
    }

    #[test]
    fn primary_key_clustering_skips_unsupported_primary_key() {
        let schema = TableSchema {
            name: "public__measurements".to_string(),
            columns: vec![ColumnSchema {
                name: "score".to_string(),
                data_type: DataType::Float64,
                nullable: false,
            }],
            primary_key: Some("score".to_string()),
        };

        assert!(primary_key_clustering(&schema).is_none());
    }

    #[test]
    fn existing_table_patch_plan_is_noop_when_schema_and_clustering_match() {
        let desired_fields = bq_fields_from_schema(&[
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ]);
        let clustering = Clustering {
            fields: vec!["id".to_string()],
        };
        let mut table = Table {
            schema: Some(BqTableSchema {
                fields: desired_fields.clone(),
            }),
            clustering: Some(clustering.clone()),
            ..Default::default()
        };

        assert!(!update_table_for_missing_fields_and_clustering(
            &mut table,
            &desired_fields,
            Some(&clustering),
        ));
    }

    #[test]
    fn existing_table_patch_plan_adds_missing_clustering() {
        let desired_fields = bq_fields_from_schema(&[ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]);
        let clustering = Clustering {
            fields: vec!["id".to_string()],
        };
        let mut table = Table {
            schema: Some(BqTableSchema {
                fields: desired_fields.clone(),
            }),
            clustering: None,
            ..Default::default()
        };

        assert!(update_table_for_missing_fields_and_clustering(
            &mut table,
            &desired_fields,
            Some(&clustering),
        ));
        assert_eq!(table.clustering, Some(clustering));
    }

    #[test]
    fn existing_table_patch_plan_adds_missing_schema_fields() {
        let existing_fields = bq_fields_from_schema(&[ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]);
        let desired_fields = bq_fields_from_schema(&[
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ]);
        let mut table = Table {
            schema: Some(BqTableSchema {
                fields: existing_fields,
            }),
            ..Default::default()
        };

        assert!(update_table_for_missing_fields_and_clustering(
            &mut table,
            &desired_fields,
            None,
        ));
        assert_eq!(table.schema.expect("patched schema").fields.len(), 2);
    }

    #[test]
    fn existing_table_patch_plan_skips_clustering_when_not_desired() {
        let desired_fields = bq_fields_from_schema(&[ColumnSchema {
            name: "score".to_string(),
            data_type: DataType::Float64,
            nullable: false,
        }]);
        let mut table = Table {
            schema: Some(BqTableSchema {
                fields: desired_fields.clone(),
            }),
            ..Default::default()
        };

        assert!(!update_table_for_missing_fields_and_clustering(
            &mut table,
            &desired_fields,
            None,
        ));
        assert!(table.clustering.is_none());
    }

    #[tokio::test]
    async fn generic_timeout_helper_times_out() {
        let result = BigQueryDestination::await_with_timeout(
            "pending future",
            Duration::from_millis(10),
            pending::<Result<(), anyhow::Error>>(),
        )
        .await;

        let err = result.expect_err("expected timeout");
        assert!(err.to_string().contains("pending future timed out"));
    }

    #[tokio::test]
    async fn bq_timeout_helper_returns_bq_error() {
        let result = BigQueryDestination::await_bq_timeout(
            "pending bigquery future",
            Duration::from_millis(10),
            pending::<std::result::Result<(), BqError>>(),
        )
        .await;

        match result.expect_err("expected timeout") {
            BqError::HttpMiddleware(err) => {
                assert!(
                    err.to_string()
                        .contains("pending bigquery future timed out")
                )
            }
            other => panic!("expected HttpMiddleware timeout error, got {other:?}"),
        }
    }

    #[test]
    fn request_timeout_is_longer_than_zero() {
        assert!(BIGQUERY_REQUEST_TIMEOUT > Duration::ZERO);
    }

    #[test]
    fn stable_query_request_id_fits_bigquery_request_id_limit() {
        let request_id = stable_query_request_id(
            "merge",
            "cdsync_app_public_production:public__accounts:public__accounts_staging_upsert_abc",
        );

        assert_eq!(request_id.len(), 30);
        assert_eq!(
            request_id,
            stable_query_request_id(
                "merge",
                "cdsync_app_public_production:public__accounts:public__accounts_staging_upsert_abc"
            )
        );
    }

    #[test]
    fn stable_query_request_id_shortens_hash_for_longer_prefixes() {
        let request_id = stable_query_request_id(
            "dynamodb_merge",
            "cdsync_sites_public_production:resq_sites_production_RevisionTableTable_zbhdrvwk:resq_sites_production_RevisionTableTable_zbhdrvwk_staging_50fe4f3b39074ae3b813bcbcdf6c47c7",
        );

        assert_eq!(request_id.len(), 36);
        assert!(request_id.starts_with("dynamodb_merge_"));
        assert_eq!(
            request_id,
            stable_query_request_id(
                "dynamodb_merge",
                "cdsync_sites_public_production:resq_sites_production_RevisionTableTable_zbhdrvwk:resq_sites_production_RevisionTableTable_zbhdrvwk_staging_50fe4f3b39074ae3b813bcbcdf6c47c7"
            )
        );
    }

    #[test]
    fn merge_query_request_id_rotates_by_apply_attempt_key() {
        let first_attempt = merge_query_request_id(
            "cdsync_app_public_production",
            "public__core_workordernote",
            "public__core_workordernote_staging_upsert_abc",
            Some("cdc_job_abc:1"),
        );
        let retry_attempt = merge_query_request_id(
            "cdsync_app_public_production",
            "public__core_workordernote",
            "public__core_workordernote_staging_upsert_abc",
            Some("cdc_job_abc:2"),
        );

        assert_eq!(first_attempt.len(), 30);
        assert_ne!(first_attempt, retry_attempt);
        assert_eq!(
            first_attempt,
            merge_query_request_id(
                "cdsync_app_public_production",
                "public__core_workordernote",
                "public__core_workordernote_staging_upsert_abc",
                Some("cdc_job_abc:1"),
            )
        );
    }

    #[test]
    fn staging_merge_sql_adds_static_primary_key_range_for_orderable_key() {
        let sql = staging_merge_sql(
            "project",
            "dataset",
            "public__accounts",
            "stage_accounts",
            &TableSchema {
                name: "public__accounts".to_string(),
                columns: vec![
                    ColumnSchema {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "name".to_string(),
                        data_type: DataType::String,
                        nullable: true,
                    },
                ],
                primary_key: Some("id".to_string()),
            },
            "id",
        );

        assert!(
            sql.starts_with("DECLARE _cdsync_pk_range STRUCT<min_pk INT64, max_pk INT64> DEFAULT")
        );
        assert!(sql.contains("FROM (SELECT `id` FROM `project.dataset.stage_accounts`)"));
        assert!(
            sql.contains(
                "ON T.`id` = S.`id` AND T.`id` BETWEEN _cdsync_pk_range.min_pk AND _cdsync_pk_range.max_pk"
            )
        );
    }

    #[test]
    fn staging_merge_sql_adds_static_primary_key_range_for_bool_key() {
        let sql = staging_merge_sql(
            "project",
            "dataset",
            "public__flags",
            "stage_flags",
            &TableSchema {
                name: "public__flags".to_string(),
                columns: vec![ColumnSchema {
                    name: "enabled".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                }],
                primary_key: Some("enabled".to_string()),
            },
            "enabled",
        );

        assert!(
            sql.starts_with("DECLARE _cdsync_pk_range STRUCT<min_pk BOOL, max_pk BOOL> DEFAULT")
        );
        assert!(
            sql.contains(
                "ON T.`enabled` = S.`enabled` AND T.`enabled` BETWEEN _cdsync_pk_range.min_pk AND _cdsync_pk_range.max_pk"
            )
        );
    }

    #[test]
    fn staging_merge_sql_skips_primary_key_range_for_unordered_float_key() {
        let sql = staging_merge_sql(
            "project",
            "dataset",
            "public__measurements",
            "stage_measurements",
            &TableSchema {
                name: "public__measurements".to_string(),
                columns: vec![ColumnSchema {
                    name: "score".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                }],
                primary_key: Some("score".to_string()),
            },
            "score",
        );

        assert!(!sql.contains("DECLARE _cdsync_pk_range"));
        assert!(!sql.contains("BETWEEN _cdsync_pk_range.min_pk"));
        assert!(sql.contains("ON T.`score` = S.`score`"));
    }

    #[test]
    fn cdc_reducer_merge_sql_deduplicates_union_by_latest_sequence() {
        let sql = cdc_reducer_merge_sql(
            "project",
            "dataset",
            "public__accounts",
            &[
                CdcBatchLoadJobStep {
                    staging_table: "stage_1".to_string(),
                    object_uri: "gs://bucket/stage_1.parquet".to_string(),
                    load_job_id: None,
                    row_count: 2,
                    upserted_count: 2,
                    deleted_count: 0,
                },
                CdcBatchLoadJobStep {
                    staging_table: "stage_2".to_string(),
                    object_uri: "gs://bucket/stage_2.parquet".to_string(),
                    load_job_id: None,
                    row_count: 3,
                    upserted_count: 3,
                    deleted_count: 0,
                },
            ],
            &TableSchema {
                name: "public__accounts".to_string(),
                columns: vec![
                    ColumnSchema {
                        name: "id".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "name".to_string(),
                        data_type: DataType::String,
                        nullable: true,
                    },
                ],
                primary_key: Some("id".to_string()),
            },
            &MetadataColumns::default(),
            "id",
        );

        assert!(sql.contains("`project.dataset.stage_1`"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("`project.dataset.stage_2`"));
        assert!(sql.contains("ROW_NUMBER() OVER (PARTITION BY `id`"));
        assert!(sql.contains("ORDER BY `_cdsync_sequence` DESC, `_cdsync_tx_ordinal` DESC"));
        assert!(sql.contains("WHERE _cdsync_reducer_rank = 1"));
        assert!(sql.contains("DECLARE _cdsync_pk_range STRUCT<min_pk INT64, max_pk INT64>"));
        assert!(sql.contains("SELECT `id` FROM `project.dataset.stage_1` UNION ALL SELECT `id` FROM `project.dataset.stage_2`"));
        assert!(
            sql.contains(
                "ON T.`id` = S.`id` AND T.`id` BETWEEN _cdsync_pk_range.min_pk AND _cdsync_pk_range.max_pk"
            )
        );
    }

    #[test]
    fn dynamodb_change_merge_sql_adds_static_primary_key_range() {
        let sql = dynamodb_change_merge_sql(
            "project",
            "dataset",
            "sites_revision",
            "stage_revision",
            &TableSchema {
                name: "sites_revision".to_string(),
                columns: vec![
                    ColumnSchema {
                        name: "id".to_string(),
                        data_type: DataType::String,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: META_SOURCE_EVENT_AT.to_string(),
                        data_type: DataType::Timestamp,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: META_SOURCE_EVENT_ID.to_string(),
                        data_type: DataType::String,
                        nullable: false,
                    },
                ],
                primary_key: Some("id".to_string()),
            },
            &MetadataColumns::default(),
            "id",
        );

        assert!(
            sql.starts_with(
                "DECLARE _cdsync_pk_range STRUCT<min_pk STRING, max_pk STRING> DEFAULT"
            )
        );
        assert!(sql.contains("FROM (SELECT `id` FROM `project.dataset.stage_revision`)"));
        assert!(
            sql.contains(
                "ON T.`id` = S.`id` AND T.`id` BETWEEN _cdsync_pk_range.min_pk AND _cdsync_pk_range.max_pk"
            )
        );
    }

    #[test]
    fn incomplete_query_response_requires_polling() {
        let response = QueryResponse {
            job_complete: false,
            job_reference: JobReference {
                project_id: "project".to_string(),
                job_id: "query_job_1".to_string(),
                location: Some("US".to_string()),
            },
            ..Default::default()
        };

        assert!(!response.job_complete);
        assert_eq!(response.job_reference.job_id, "query_job_1");
    }

    #[test]
    fn query_job_completion_tracks_running_then_done() {
        let mut job = query_test_job(JobState::Running);
        assert!(!query_job_completed("test query", &job).expect("running query job"));

        job.status.state = JobState::Done;
        assert!(query_job_completed("test query", &job).expect("done query job"));
    }

    #[test]
    fn get_query_results_response_preserves_final_rows() {
        let response = get_query_results_to_query_response(QueryResponse {
            kind: "bigquery#queryResponse".to_string(),
            job_reference: JobReference {
                project_id: "result-project".to_string(),
                job_id: "query_job_1".to_string(),
                location: Some("US".to_string()),
            },
            total_rows: Some(2),
            job_complete: true,
            ..Default::default()
        })
        .expect("query results response");

        assert!(response.job_complete);
        assert_eq!(response.total_rows, Some(2));
        assert_eq!(response.job_reference.project_id, "result-project");
    }

    #[test]
    fn get_query_results_response_allows_completed_dml_without_total_rows() {
        let response: QueryResponse = serde_json::from_value(serde_json::json!({
            "kind": "bigquery#queryResponse",
            "jobReference": {
                "projectId": "result-project",
                "jobId": "query_job_1",
                "location": "US"
            },
            "jobComplete": true,
            "numDmlAffectedRows": "12"
        }))
        .expect("query results response");

        let response =
            get_query_results_to_query_response(response).expect("query results response");

        assert!(response.job_complete);
        assert_eq!(response.total_rows, None);
        assert_eq!(response.num_dml_affected_rows, Some(12));
        assert_eq!(response.job_reference.job_id, "query_job_1");
    }

    #[tokio::test]
    async fn fetch_query_results_response_allows_http_dml_without_total_rows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/projects/result-project/queries/query_job_1"))
            .and(query_param("location", "US"))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "kind": "bigquery#queryResponse",
                "jobReference": {
                    "projectId": "result-project",
                    "jobId": "query_job_1",
                    "location": "US"
                },
                "jobComplete": true,
                "numDmlAffectedRows": "12"
            })))
            .mount(&server)
            .await;

        let token_source = StaticTokenSource("Bearer test-token");
        let response = fetch_query_results_response_from_http(
            &token_source,
            &server.uri(),
            "fallback-project",
            &JobReference {
                project_id: "result-project".to_string(),
                job_id: "query_job_1".to_string(),
                location: Some("US".to_string()),
            },
            Some("US".to_string()),
        )
        .await
        .expect("query results response");

        assert!(response.job_complete);
        assert_eq!(response.total_rows, None);
        assert_eq!(response.num_dml_affected_rows, Some(12));
        assert_eq!(response.job_reference.job_id, "query_job_1");
    }

    #[test]
    fn get_query_results_response_rejects_incomplete_results() {
        let err = get_query_results_to_query_response(QueryResponse {
            job_reference: JobReference {
                project_id: "project".to_string(),
                job_id: "query_job_1".to_string(),
                location: Some("US".to_string()),
            },
            job_complete: false,
            ..Default::default()
        })
        .expect_err("incomplete query results should fail");

        assert!(
            err.to_string()
                .contains("were still incomplete after job reached DONE")
        );
    }

    #[tokio::test]
    async fn drain_cleanup_task_handles_waits_for_spawned_work() {
        let cleanup_tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = Arc::clone(&finished);

        cleanup_tasks.lock().await.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            finished_clone.store(true, Ordering::SeqCst);
        }));

        drain_cleanup_task_handles(&cleanup_tasks)
            .await
            .expect("cleanup tasks drained");

        assert!(finished.load(Ordering::SeqCst));
    }

    fn query_test_job(state: JobState) -> Job {
        Job {
            job_reference: JobReference {
                project_id: "project".to_string(),
                job_id: "query_job_1".to_string(),
                location: Some("US".to_string()),
            },
            configuration: JobConfiguration {
                job_type: "QUERY".to_string(),
                job: JobType::Query(Default::default()),
                dry_run: None,
                job_timeout_ms: None,
                labels: None,
            },
            status: JobStatus {
                state,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn reap_finished_cleanup_task_handles_discards_completed_handles() {
        let cleanup_tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

        cleanup_tasks.lock().await.push(tokio::spawn(async {}));
        cleanup_tasks.lock().await.push(tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }));

        tokio::time::sleep(Duration::from_millis(10)).await;

        reap_finished_cleanup_task_handles(&cleanup_tasks)
            .await
            .expect("finished cleanup handles reaped");

        assert_eq!(cleanup_tasks.lock().await.len(), 1);
    }
}
