use crate::config::BigQueryConfig;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use crate::destinations::{with_metadata_schema, Destination, WriteMode};
use crate::types::{ColumnSchema, DataType, TableSchema};
use anyhow::{Context, Result};
use async_trait::async_trait;
use gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::error::Error as BqError;
use gcloud_bigquery::http::job::query::QueryRequest;
use gcloud_bigquery::http::table::{
    Table, TableFieldSchema, TableFieldType, TableReference, TableSchema as BqTableSchema, TimePartitioning,
    TimePartitionType,
};
use gcloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame};
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use reqwest::StatusCode;
use tracing::{info, warn};
use url::Url;

#[derive(Clone)]
pub struct BigQueryDestination {
    client: Client,
    config: BigQueryConfig,
    dry_run: bool,
}

impl BigQueryDestination {
    pub async fn new(mut config: BigQueryConfig, dry_run: bool) -> Result<Self> {
        if config.emulator_http.is_none() && config.emulator_grpc.is_some() {
            anyhow::bail!("bigquery.emulator_grpc requires emulator_http");
        }

        let (client_config, _project_from_auth) = if let Some(raw_http) = &config.emulator_http {
            let emulator_http = if raw_http.contains("://") {
                raw_http.to_string()
            } else {
                format!("http://{raw_http}")
            };

            let emulator_grpc = if let Some(raw_grpc) = &config.emulator_grpc {
                if raw_grpc.contains("://") {
                    let url = Url::parse(raw_grpc)
                        .context("invalid bigquery.emulator_grpc url")?;
                    let host = url
                        .host_str()
                        .context("bigquery.emulator_grpc missing host")?;
                    let port = url.port().unwrap_or(default_port(url.scheme()));
                    format!("{host}:{port}")
                } else {
                    raw_grpc.to_string()
                }
            } else {
                let url = Url::parse(&emulator_http)
                    .context("invalid bigquery.emulator_http url")?;
                let host = url
                    .host_str()
                    .context("bigquery.emulator_http missing host")?;
                let port = url.port().unwrap_or(default_port(url.scheme()));
                format!("{host}:{port}")
            };

            config.emulator_http = Some(emulator_http.clone());
            config.emulator_grpc = Some(emulator_grpc.clone());

            (ClientConfig::new_with_emulator(&emulator_grpc, emulator_http), None)
        } else if let Some(path) = &config.service_account_key_path {
            let key = CredentialsFile::new_from_file(path.to_string_lossy().to_string()).await?;
            ClientConfig::new_with_credentials(key).await?
        } else if let Some(raw_key) = &config.service_account_key {
            let key = CredentialsFile::new_from_str(raw_key).await?;
            ClientConfig::new_with_credentials(key).await?
        } else {
            ClientConfig::new_with_auth().await?
        };
        let client = Client::new(client_config).await?;
        Ok(Self {
            client,
            config,
            dry_run,
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
            let response = client.get(&url).send().await?;
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
                let response = client.post(create_url).json(&body).send().await?;
                if response.status().is_success() {
                    return Ok(());
                }
                anyhow::bail!("failed to create dataset in emulator: {}", response.status());
            }
            anyhow::bail!("failed to fetch dataset from emulator: {}", response.status());
        }

        match self
            .client
            .dataset()
            .get(&self.config.project_id, &self.config.dataset)
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
                self.client.dataset().create(&dataset).await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn ensure_table_internal(&self, schema: &TableSchema, table_id: &str, with_partition: bool) -> Result<()> {
        self.ensure_dataset().await?;
        let schema = with_metadata_schema(schema);
        let desired_fields = bq_fields_from_schema(&schema.columns);
        let bq_schema = BqTableSchema { fields: desired_fields.clone() };

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
            let response = client.get(&url).send().await?;
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
                    "field": crate::types::META_SYNCED_AT
                });
            }
            let create_url = format!(
                "{}/projects/{}/datasets/{}/tables",
                emulator_http, self.config.project_id, self.config.dataset
            );
            let response = client.post(create_url).json(&body).send().await?;
            if response.status().is_success() {
                return Ok(());
            }
            anyhow::bail!("emulator table create failed: {}", response.status());
        }

        match self
            .client
            .table()
            .get(&self.config.project_id, &self.config.dataset, table_id)
            .await
        {
            Ok(mut table) => {
                let existing_fields = table.schema.as_ref().map(|s| s.fields.clone()).unwrap_or_default();
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
                    table.schema = Some(BqTableSchema { fields: updated_fields });
                    self.client.table().patch(&table).await?;
                }
                Ok(())
            }
            Err(BqError::Response(err)) if err.code == 404 => {
                let mut table = Table::default();
                table.table_reference = TableReference {
                    project_id: self.config.project_id.clone(),
                    dataset_id: self.config.dataset.clone(),
                    table_id: table_id.to_string(),
                };
                if with_partition && self.config.partition_by_synced_at.unwrap_or(false) {
                    table.time_partitioning = Some(TimePartitioning {
                        partition_type: TimePartitionType::Day,
                        expiration_ms: None,
                        field: Some(crate::types::META_SYNCED_AT.to_string()),
                    });
                }
                table.schema = Some(bq_schema);
                self.client.table().create(&table).await?;
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn append_rows(
        &self,
        table_id: &str,
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

        let rows = dataframe_to_json_rows(frame)?;
        let mut request: InsertAllRequest<Map<String, Value>> = InsertAllRequest::default();
        for row in rows {
            let insert_id = primary_key
                .and_then(|pk| row.get(pk))
                .and_then(value_to_insert_id);
            request.rows.push(Row { insert_id, json: row });
        }

        if let Some(emulator_http) = &self.config.emulator_http {
            let url = format!(
                "{}/projects/{}/datasets/{}/tables/{}/insertAll",
                emulator_http, self.config.project_id, self.config.dataset, table_id
            );
            let response = reqwest::Client::new().post(url).json(&request).send().await?;
            if !response.status().is_success() {
                anyhow::bail!("emulator insert failed: {}", response.status());
            }
            let payload: serde_json::Value = response.json().await?;
            if let Some(errors) = payload.get("insertErrors") {
                warn!("BigQuery emulator insert errors for {}: {}", table_id, errors);
            }
            return Ok(());
        }

        let response = self
            .client
            .tabledata()
            .insert(&self.config.project_id, &self.config.dataset, table_id, &request)
            .await?;
        if let Some(errors) = response.insert_errors {
            warn!("BigQuery insert errors for {}: {} rows", table_id, errors.len());
        }
        Ok(())
    }

    async fn merge_staging(&self, target: &str, staging: &str, schema: &TableSchema, primary_key: &str) -> Result<()> {
        if self.dry_run {
            info!("dry-run: merge staging {} into {}", staging, target);
            return Ok(());
        }

        let schema = with_metadata_schema(schema);
        let columns: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
        let updates: Vec<String> = columns
            .iter()
            .map(|col| format!("{} = S.{}", bq_ident(col), bq_ident(col)))
            .collect();
        let insert_cols: Vec<String> = columns.iter().map(|col| bq_ident(col)).collect();
        let insert_vals: Vec<String> = columns.iter().map(|col| format!("S.{}", bq_ident(col))).collect();

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

        let mut request = QueryRequest::default();
        request.query = sql;
        request.use_legacy_sql = false;
        if let Some(location) = &self.config.location {
            request.location = location.clone();
        }

        if let Some(emulator_http) = &self.config.emulator_http {
            let client = reqwest::Client::new();
            let url = format!("{}/projects/{}/queries", emulator_http, self.config.project_id);
            let response = client.post(url).json(&request).send().await?;
            if response.status().is_success() {
                return Ok(());
            }
            anyhow::bail!("emulator query failed: {}", response.status());
        }
        self.client.job().query(&self.config.project_id, &request).await?;
        Ok(())
    }
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
            let response = client.delete(url).send().await?;
            if response.status().is_success()
                || response.status() == StatusCode::NOT_FOUND
            {
                return Ok(());
            }
            anyhow::bail!("emulator delete table failed: {}", response.status());
        }
        let sql = format!(
            "TRUNCATE TABLE `{project}.{dataset}.{table}`",
            project = self.config.project_id,
            dataset = self.config.dataset,
            table = table
        );
        let mut request = QueryRequest::default();
        request.query = sql;
        request.use_legacy_sql = false;
        if let Some(location) = &self.config.location {
            request.location = location.clone();
        }
        self.client.job().query(&self.config.project_id, &request).await?;
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
                self.append_rows(table, frame, primary_key).await?;
            }
            WriteMode::Upsert => {
                if let Some(pk) = primary_key {
                    if self.config.emulator_http.is_some() {
                        warn!(
                            table,
                            "bigquery emulator: upsert unsupported; falling back to append"
                        );
                        self.append_rows(table, frame, Some(pk)).await?;
                        return Ok(());
                    }
                    let staging = format!("{table}_staging");
                    self.ensure_table_internal(schema, &staging, false).await?;
                    self.truncate_table(&staging).await?;
                    self.append_rows(&staging, frame, Some(pk)).await?;
                    self.merge_staging(table, &staging, schema, pk).await?;
                } else {
                    warn!("no primary key for {table}; falling back to append");
                    self.append_rows(table, frame, None).await?;
                }
            }
        }
        Ok(())
    }
}

fn bq_fields_from_schema(columns: &[ColumnSchema]) -> Vec<TableFieldSchema> {
    columns
        .iter()
        .map(|col| {
            let data_type = match col.data_type {
                DataType::String => TableFieldType::String,
                DataType::Int64 => TableFieldType::Int64,
                DataType::Float64 => TableFieldType::Float64,
                DataType::Bool => TableFieldType::Bool,
                DataType::Timestamp => TableFieldType::Timestamp,
                DataType::Date => TableFieldType::Date,
                DataType::Bytes => TableFieldType::Bytes,
                DataType::Numeric => TableFieldType::Numeric,
                DataType::Json => TableFieldType::Json,
            };
            TableFieldSchema {
                name: col.name.clone(),
                data_type,
                mode: if col.nullable {
                    Some(gcloud_bigquery::http::table::TableFieldMode::Nullable)
                } else {
                    Some(gcloud_bigquery::http::table::TableFieldMode::Required)
                },
                ..Default::default()
            }
        })
        .collect()
}

fn bq_ident(name: &str) -> String {
    let escaped = name.replace('`', "\\`");
    format!("`{}`", escaped)
}

fn dataframe_to_json_rows(frame: &DataFrame) -> Result<Vec<Map<String, Value>>> {
    let columns = frame.get_column_names();
    let height = frame.height();
    let mut output = Vec::with_capacity(height);
    let mut row = PolarsRow::new(vec![AnyValue::Null; columns.len()]);
    for idx in 0..height {
        frame.get_row_amortized(idx, &mut row)?;
        let mut map = Map::with_capacity(columns.len());
        for (col_name, value) in columns.iter().zip(row.0.iter()) {
            map.insert(col_name.to_string(), anyvalue_to_json(value));
        }
        output.push(map);
    }
    Ok(output)
}

fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int64(v) => Value::Number((*v).into()),
        AnyValue::Int32(v) => Value::Number((*v as i64).into()),
        AnyValue::UInt64(v) => Value::Number((*v).into()),
        AnyValue::UInt32(v) => Value::Number((*v as u64).into()),
        AnyValue::Float64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::Float32(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::StringOwned(v) => Value::String(v.to_string()),
        AnyValue::Binary(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::BinaryOwned(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::Datetime(ts, unit, _) => Value::String(datetime_to_rfc3339(*ts, *unit)),
        AnyValue::DatetimeOwned(ts, unit, _) => Value::String(datetime_to_rfc3339(*ts, *unit)),
        AnyValue::Date(days) => Value::String(date_to_string(*days)),
        AnyValue::Decimal(v, _) => Value::String(v.to_string()),
        other => Value::String(other.to_string()),
    }
}

fn datetime_to_rfc3339(ts: i64, unit: polars::prelude::TimeUnit) -> String {
    let nanos = match unit {
        polars::prelude::TimeUnit::Nanoseconds => ts,
        polars::prelude::TimeUnit::Microseconds => ts * 1_000,
        polars::prelude::TimeUnit::Milliseconds => ts * 1_000_000,
    };
    let seconds = nanos / 1_000_000_000;
    let nanos_part = (nanos % 1_000_000_000) as u32;
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos_part)
        .unwrap_or_else(chrono::Utc::now);
    dt.to_rfc3339()
}

fn date_to_string(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date"));
    let date = epoch + chrono::Duration::days(days as i64);
    date.format("%Y-%m-%d").to_string()
}

fn value_to_insert_id(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        other => serde_json::to_string(other).ok(),
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}
