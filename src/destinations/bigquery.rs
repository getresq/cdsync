mod batch_load;
mod values;

use crate::config::BigQueryConfig;
use crate::destinations::{Destination, WriteMode, with_metadata_schema};
use crate::tls;
use crate::types::{MetadataColumns, TableSchema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::google_cloud_auth::project::Config as GoogleAuthConfig;
use gcloud_bigquery::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::error::Error as BqError;
use gcloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use gcloud_bigquery::http::table::{
    Table, TableReference, TableSchema as BqTableSchema, TimePartitionType, TimePartitioning,
};
use polars::prelude::DataFrame;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use token_source::{TokenSource, TokenSourceProvider};
use tokio::sync::Mutex;
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
use tracing::{error, info, info_span, warn};
use url::Url;
use uuid::Uuid;

pub(crate) use self::batch_load::BATCH_LOAD_JOB_HARD_TIMEOUT;
use self::values::{
    bq_fields_from_schema, bq_ident, dataframe_to_json_rows, default_port,
    tuple_value_as_datetime, tuple_value_as_i64,
};

#[derive(Clone)]
pub struct BigQueryDestination {
    client: Client,
    gcs_token_source: Option<Arc<dyn TokenSource>>,
    config: BigQueryConfig,
    dry_run: bool,
    metadata: MetadataColumns,
    cleanup_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
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
    pub(crate) primary_key: String,
    pub(crate) truncate: bool,
    pub(crate) steps: Vec<CdcBatchLoadJobStep>,
}

const EMULATOR_INSERT_BLOCKING_ROWS: usize = 512;
pub(super) const BIGQUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

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

        let (client_config, gcs_token_source, _project_from_auth) = if let Some(raw_http) =
            &config.emulator_http
        {
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
                None,
            )
        } else if let Some(path) = &config.service_account_key_path {
            let key = CredentialsFile::new_from_file(path.to_string_lossy().to_string()).await?;
            let (bq_config, project) = ClientConfig::new_with_credentials(key.clone()).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(key),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        } else if let Some(raw_key) = &config.service_account_key {
            let key = CredentialsFile::new_from_str(raw_key).await?;
            let (bq_config, project) = ClientConfig::new_with_credentials(key).await?;
            let token_source = DefaultTokenSourceProvider::new_with_credentials(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
                Box::new(CredentialsFile::new_from_str(raw_key).await?),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        } else {
            let (bq_config, project) = ClientConfig::new_with_auth().await?;
            let token_source = DefaultTokenSourceProvider::new(
                GoogleAuthConfig::default()
                    .with_scopes(&["https://www.googleapis.com/auth/devstorage.read_write"]),
            )
            .await?;
            (bq_config, Some(token_source.token_source()), project)
        };
        let client = Client::new(client_config).await?;
        Ok(Self {
            client,
            gcs_token_source,
            config,
            dry_run,
            metadata,
            cleanup_tasks: Arc::new(Mutex::new(Vec::new())),
        })
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
        let request = QueryRequest {
            query: sql.to_string(),
            use_legacy_sql: false,
            location: self.config.location.clone().unwrap_or_default(),
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

        if let Some(errors) = &response.errors
            && !errors.is_empty()
        {
            error!(errors = ?errors, "BigQuery query returned errors");
            anyhow::bail!("BigQuery query returned errors: {:?}", errors);
        }

        Ok(response)
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
                let existing_fields = table
                    .schema
                    .as_ref()
                    .map(|s| s.fields.clone())
                    .unwrap_or_default();
                let existing_names: HashSet<String> = existing_fields
                    .iter()
                    .map(|f| f.name.to_lowercase())
                    .collect();

                let mut updated_fields = existing_fields;
                for field in desired_fields.clone() {
                    if !existing_names.contains(&field.name.to_lowercase()) {
                        updated_fields.push(field);
                    }
                }

                if updated_fields.len() != existing_names.len() {
                    table.schema = Some(BqTableSchema {
                        fields: updated_fields,
                    });
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
        self.append_rows_via_batch_load(table_id, schema, frame).await?;
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

    pub(crate) async fn process_cdc_batch_load_job(
        &self,
        connection_id: &str,
        payload: &CdcBatchLoadJobPayload,
    ) -> Result<()> {
        let target_started_at = std::time::Instant::now();
        let target_span = info_span!(
            "cdc_batch_load_job.ensure_target",
            connection_id = connection_id,
            table = %payload.target_table
        );
        {
            let _target_span = target_span.enter();
            self.ensure_table(&payload.schema).await?;
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
                let _ensure_staging_span = ensure_staging_span.enter();
                self.ensure_table_internal(&payload.schema, &step.staging_table, false)
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
                let _load_span = load_span.enter();
                self.run_load_job(
                    &step.staging_table,
                    &with_metadata_schema(&payload.schema, &self.metadata),
                    &step.object_uri,
                    gcloud_bigquery::http::job::WriteDisposition::WriteTruncate,
                )
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
                let _merge_span = merge_span.enter();
                self.merge_staging(
                    &payload.target_table,
                    &step.staging_table,
                    &payload.schema,
                    &payload.primary_key,
                )
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
    ) -> Result<()> {
        if self.dry_run {
            info!("dry-run: merge staging {} into {}", staging, target);
            return Ok(());
        }

        let schema = with_metadata_schema(schema, &self.metadata);
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

        let sql = format!(
            "MERGE `{project}.{dataset}.{target}` T USING `{project}.{dataset}.{staging}` S ON T.{pk} = S.{pk} \
             WHEN MATCHED THEN UPDATE SET {updates} \
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
            project = self.config.project_id,
            dataset = self.config.dataset,
            target = target,
            staging = staging,
            pk = bq_ident(primary_key),
            updates = updates.join(", "),
            insert_cols = insert_cols.join(", "),
            insert_vals = insert_vals.join(", ")
        );

        self.run_query(&sql).await.with_context(|| {
            format!("merging staging BigQuery table {} into {}", staging, target)
        })?;
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
                        self.merge_staging(table, &staging, schema, pk).await
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

#[cfg(test)]
mod tests {
    use super::{
        BIGQUERY_REQUEST_TIMEOUT, BigQueryDestination, drain_cleanup_task_handles,
        reap_finished_cleanup_task_handles, upsert_staging_table_id,
    };
    use gcloud_bigquery::http::error::Error as BqError;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::task::JoinHandle;

    #[test]
    fn upsert_staging_table_id_is_unique_per_write() {
        let first = upsert_staging_table_id("public__accounts");
        let second = upsert_staging_table_id("public__accounts");

        assert_ne!(first, second);
        assert!(first.starts_with("public__accounts_staging_"));
        assert!(second.starts_with("public__accounts_staging_"));
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
