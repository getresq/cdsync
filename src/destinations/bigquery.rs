mod batch_load;
mod storage_write;
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
use gcloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use polars::prelude::DataFrame;
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use token_source::{TokenSource, TokenSourceProvider};
use tokio::sync::Mutex;
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

use self::storage_write::StorageWriteTableWriter;
use self::values::{
    bq_fields_from_schema, bq_ident, dataframe_to_json_rows, default_port, tuple_value_as_datetime,
    tuple_value_as_i64, value_to_insert_id,
};

#[derive(Clone)]
pub struct BigQueryDestination {
    client: Client,
    gcs_token_source: Option<Arc<dyn TokenSource>>,
    config: BigQueryConfig,
    dry_run: bool,
    metadata: MetadataColumns,
    storage_writers: Arc<Mutex<HashMap<String, Arc<StorageWriteTableWriter>>>>,
    cleanup_tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DestinationTableSummary {
    pub row_count: i64,
    pub max_synced_at: Option<DateTime<Utc>>,
    pub deleted_rows: i64,
}

const INSERT_ALL_BLOCKING_ROWS: usize = 512;
pub(super) const BIGQUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

impl BigQueryDestination {
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
            storage_writers: Arc::new(Mutex::new(HashMap::new())),
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
        self.ensure_dataset().await?;
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
                    Self::await_bq_timeout(
                        format!("patching BigQuery table {}", table_id),
                        BIGQUERY_REQUEST_TIMEOUT,
                        self.client.table().patch(&table),
                    )
                    .await?;
                }
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
                Self::await_bq_timeout(
                    format!("creating BigQuery table {}", table_id),
                    BIGQUERY_REQUEST_TIMEOUT,
                    self.client.table().create(&table),
                )
                .await?;
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
        if self.dry_run {
            info!("dry-run: insert {} rows into {}", frame.height(), table_id);
            return Ok(());
        }
        if self
            .append_rows_via_batch_load(table_id, schema, frame)
            .await?
        {
            return Ok(());
        }
        if self
            .append_rows_via_storage_write(table_id, schema, frame)
            .await?
        {
            return Ok(());
        }

        let rows = insert_all_rows(frame).await?;
        let mut request: InsertAllRequest<Map<String, Value>> = InsertAllRequest::default();
        for row in rows {
            let insert_id = primary_key
                .and_then(|pk| row.get(pk))
                .and_then(value_to_insert_id);
            request.rows.push(Row {
                insert_id,
                json: row,
            });
        }

        if let Some(emulator_http) = &self.config.emulator_http {
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
                let row_errors = errors.as_array().map(|rows| rows.len()).unwrap_or(1) as u64;
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
            return Ok(());
        }

        let response = Self::await_with_timeout(
            format!("running BigQuery insertAll for {}", table_id),
            BIGQUERY_REQUEST_TIMEOUT,
            self.client.tabledata().insert(
                &self.config.project_id,
                &self.config.dataset,
                table_id,
                &request,
            ),
        )
        .await?;
        if let Some(errors) = response.insert_errors {
            crate::telemetry::record_bigquery_row_errors(table_id, errors.len() as u64);
            error!(
                table = %table_id,
                row_errors = errors.len(),
                rows = frame.height(),
                "BigQuery insertAll returned row errors"
            );
            anyhow::bail!(
                "BigQuery insert errors for {}: {} rows",
                table_id,
                errors.len()
            );
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
        self.invalidate_storage_writer(table).await;
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
            Ok(_) => Ok(()),
            Err(BqError::Response(err)) if err.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn spawn_drop_table_if_exists(&self, table: String) {
        if self.dry_run {
            return;
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

async fn drain_cleanup_task_handles(cleanup_tasks: &Arc<Mutex<Vec<JoinHandle<()>>>>) -> Result<()> {
    let handles = {
        let mut guard = cleanup_tasks.lock().await;
        std::mem::take(&mut *guard)
    };

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
        self.invalidate_storage_writer(table).await;
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
                    self.ensure_table_internal(schema, &staging, false).await?;
                    let result: Result<()> = async {
                        self.append_rows(&staging, schema, frame, Some(pk)).await?;
                        self.merge_staging(table, &staging, schema, pk).await
                    }
                    .await;
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

async fn insert_all_rows(frame: &DataFrame) -> Result<Vec<Map<String, Value>>> {
    if frame.height() < INSERT_ALL_BLOCKING_ROWS {
        return dataframe_to_json_rows(frame);
    }

    let frame = frame.clone();
    task::spawn_blocking(move || dataframe_to_json_rows(&frame))
        .await
        .map_err(|err| anyhow::anyhow!("failed to join insertAll row conversion task: {}", err))?
}

fn upsert_staging_table_id(table: &str) -> String {
    format!("{table}_staging_{}", Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::{
        BIGQUERY_REQUEST_TIMEOUT, BigQueryDestination, drain_cleanup_task_handles,
        upsert_staging_table_id,
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
}
