use crate::config::{
    FieldSelection, SalesforceConfig, SalesforceObjectConfig, SalesforceObjectDefaults,
};
use crate::destinations::{Destination, WriteMode};
use crate::state::StateHandle;
use crate::stats::StatsHandle;
use crate::types::{
    ColumnSchema, DataType, META_DELETED_AT, META_SYNCED_AT, TableCheckpoint, TableSchema,
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use globset::{Glob, GlobSet};
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::sleep;

pub struct SalesforceSource {
    config: SalesforceConfig,
    client: Client,
    auth: Mutex<Option<AuthState>>,
}

#[derive(Clone, Debug)]
pub struct ResolvedSalesforceObject {
    pub name: String,
    pub primary_key: String,
    pub fields: Option<FieldSelection>,
    pub soft_delete: bool,
}

pub struct SalesforceSyncRequest<'a> {
    pub object: &'a ResolvedSalesforceObject,
    pub dest: &'a dyn Destination,
    pub checkpoint: TableCheckpoint,
    pub state_handle: Option<StateHandle>,
    pub mode: crate::types::SyncMode,
    pub dry_run: bool,
    pub stats: Option<StatsHandle>,
}

#[derive(Clone, Debug)]
struct AuthState {
    access_token: String,
    instance_url: String,
}

impl SalesforceSource {
    pub fn new(config: SalesforceConfig) -> Result<Self> {
        Ok(Self {
            config,
            client: Client::new(),
            auth: Mutex::new(None),
        })
    }

    pub async fn resolve_objects(&self) -> Result<Vec<ResolvedSalesforceObject>> {
        let mut object_map: std::collections::HashMap<String, SalesforceObjectConfig> =
            std::collections::HashMap::new();

        if let Some(objects) = &self.config.objects {
            for object in objects {
                object_map.insert(object.name.clone(), object.clone());
            }
        }

        if let Some(selection) = &self.config.object_selection {
            if !selection.include.is_empty() || !selection.exclude.is_empty() {
                let all_objects = self.list_objects().await?;
                let include_set = build_globset(&selection.include)?;
                let exclude_set = build_globset(&selection.exclude)?;

                for name in all_objects {
                    if !selection.include.is_empty() && !include_set.is_match(&name) {
                        continue;
                    }
                    if !selection.exclude.is_empty() && exclude_set.is_match(&name) {
                        continue;
                    }
                    object_map
                        .entry(name.clone())
                        .or_insert(SalesforceObjectConfig {
                            name,
                            primary_key: None,
                            fields: None,
                            soft_delete: None,
                        });
                }
            }

            if !selection.exclude.is_empty() {
                let exclude_set = build_globset(&selection.exclude)?;
                object_map.retain(|name, _| !exclude_set.is_match(name));
            }
        }

        if object_map.is_empty() {
            anyhow::bail!("no salesforce objects resolved from config");
        }

        let defaults = self
            .config
            .object_selection
            .as_ref()
            .and_then(|sel| sel.defaults.clone());

        let mut resolved = Vec::with_capacity(object_map.len());
        for object in object_map.values() {
            resolved.push(apply_object_defaults(object, defaults.as_ref()));
        }
        resolved.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(resolved)
    }

    pub async fn sync_object(&self, request: SalesforceSyncRequest<'_>) -> Result<TableCheckpoint> {
        let SalesforceSyncRequest {
            object,
            dest,
            checkpoint,
            state_handle,
            mode,
            dry_run,
            stats,
        } = request;
        let auth = self.authenticate(stats.as_ref()).await?;
        let api_version = self
            .config
            .api_version
            .clone()
            .unwrap_or_else(|| "v59.0".to_string());

        let describe = self
            .describe_object(&auth, &api_version, &object.name, stats.as_ref())
            .await?;
        let mut fields = match &object.fields {
            Some(selection) => selection
                .include_list()
                .unwrap_or_else(|| describe.fields.iter().map(|f| f.name.clone()).collect()),
            None => describe.fields.iter().map(|f| f.name.clone()).collect(),
        };
        if let Some(selection) = &object.fields {
            let exclude = selection.exclude_list();
            fields.retain(|f| !exclude.contains(f));
        }
        ensure_field(&mut fields, &object.primary_key);
        ensure_field(&mut fields, "SystemModstamp");
        ensure_field(&mut fields, "IsDeleted");

        let schema = TableSchema {
            name: object.name.clone(),
            columns: describe
                .fields
                .iter()
                .filter(|f| fields.contains(&f.name))
                .map(|f| ColumnSchema {
                    name: f.name.clone(),
                    data_type: salesforce_type_to_data_type(&f.r#type),
                    nullable: f.nillable.unwrap_or(true),
                })
                .collect(),
            primary_key: Some(object.primary_key.clone()),
        };

        dest.ensure_table(&schema).await?;

        let mut checkpoint = checkpoint;
        let mut last_seen = checkpoint.last_synced_at;
        let mut query = build_soql(&fields, &object.name, mode, last_seen);

        loop {
            let extract_start = Instant::now();
            let response = self
                .query(&auth, &api_version, &query, stats.as_ref())
                .await?;
            let extract_ms = extract_start.elapsed().as_millis() as u64;
            if response.records.is_empty() {
                break;
            }

            let synced_at = Utc::now();
            let frame = records_to_frame(&schema, &response.records, object, synced_at)?;
            if let Some(stats) = &stats {
                stats
                    .record_extract(&object.name, response.records.len(), extract_ms)
                    .await;
            }
            if !dry_run {
                let load_start = Instant::now();
                dest.write_batch(
                    &schema.name,
                    &schema,
                    &frame,
                    WriteMode::Upsert,
                    schema.primary_key.as_deref(),
                )
                .await?;
                if let Some(stats) = &stats {
                    let deleted = if object.soft_delete {
                        response
                            .records
                            .iter()
                            .filter(|r| matches!(r.get("IsDeleted"), Some(Value::Bool(true))))
                            .count()
                    } else {
                        0
                    };
                    stats
                        .record_load(
                            &object.name,
                            response.records.len(),
                            response.records.len(),
                            deleted,
                            load_start.elapsed().as_millis() as u64,
                        )
                        .await;
                }
            }

            last_seen = response
                .records
                .iter()
                .filter_map(|r| r.get("SystemModstamp"))
                .filter_map(|v| v.as_str())
                .filter_map(parse_datetime)
                .max()
                .or(last_seen);
            checkpoint.last_synced_at = last_seen.or(Some(Utc::now()));
            if let Some(state_handle) = &state_handle {
                state_handle
                    .save_salesforce_checkpoint(&object.name, &checkpoint)
                    .await?;
            }

            if let Some(next) = response.next_records_url {
                query = next;
            } else {
                break;
            }
        }

        checkpoint.last_synced_at = last_seen.or(Some(Utc::now()));
        if let Some(state_handle) = &state_handle {
            state_handle
                .save_salesforce_checkpoint(&object.name, &checkpoint)
                .await?;
        }

        Ok(checkpoint)
    }

    pub async fn validate(&self) -> Result<()> {
        let auth = self.authenticate(None).await?;
        let api_version = self
            .config
            .api_version
            .clone()
            .unwrap_or_else(|| "v59.0".to_string());
        let url = format!("{}/services/data/{}/", auth.instance_url, api_version);
        let response = self
            .client
            .get(url)
            .bearer_auth(auth.access_token)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "salesforce validation failed: {}",
                response.status()
            ))
        }
    }

    async fn list_objects(&self) -> Result<Vec<String>> {
        let auth = self.authenticate(None).await?;
        let api_version = self
            .config
            .api_version
            .clone()
            .unwrap_or_else(|| "v59.0".to_string());
        let url = format!(
            "{}/services/data/{}/sobjects",
            auth.instance_url, api_version
        );
        let response = self
            .send_with_retry(
                || self.client.get(url.clone()).bearer_auth(&auth.access_token),
                None,
            )
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "list sobjects failed: {}",
                response.status()
            ));
        }
        let payload: SobjectsResponse = response.json().await?;
        Ok(payload.sobjects.into_iter().map(|o| o.name).collect())
    }

    async fn authenticate(&self, stats: Option<&StatsHandle>) -> Result<AuthState> {
        let mut guard = self.auth.lock().await;
        if let Some(auth) = guard.clone() {
            return Ok(auth);
        }

        let login_url = self
            .config
            .login_url
            .clone()
            .unwrap_or_else(|| "https://login.salesforce.com".to_string());
        let url = format!("{}/services/oauth2/token", login_url);

        let params = [
            ("grant_type", "refresh_token"),
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
            ("refresh_token", self.config.refresh_token.as_str()),
        ];

        let response = self
            .send_with_retry(|| self.client.post(url.clone()).form(&params), stats)
            .await
            .context("salesforce token request")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "salesforce auth failed: {}",
                response.status()
            ));
        }

        let payload: AuthResponse = response.json().await?;
        let instance_url = payload
            .instance_url
            .or(self.config.instance_url.clone())
            .context("missing instance_url from salesforce auth")?;
        let auth = AuthState {
            access_token: payload.access_token,
            instance_url,
        };
        *guard = Some(auth.clone());
        Ok(auth)
    }

    async fn describe_object(
        &self,
        auth: &AuthState,
        api_version: &str,
        object: &str,
        stats: Option<&StatsHandle>,
    ) -> Result<DescribeResponse> {
        let url = format!(
            "{}/services/data/{}/sobjects/{}/describe",
            auth.instance_url, api_version, object
        );
        let response = self
            .send_with_retry(
                || self.client.get(url.clone()).bearer_auth(&auth.access_token),
                stats,
            )
            .await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "describe {} failed: {}",
                object,
                response.status()
            ));
        }
        let payload: DescribeResponse = response.json().await?;
        Ok(payload)
    }

    async fn query(
        &self,
        auth: &AuthState,
        api_version: &str,
        query: &str,
        stats: Option<&StatsHandle>,
    ) -> Result<QueryResponse> {
        let url = if query.starts_with("/services") {
            format!("{}{}", auth.instance_url, query)
        } else {
            let encoded = urlencoding::encode(query);
            format!(
                "{}/services/data/{}/query/?q={}",
                auth.instance_url, api_version, encoded
            )
        };
        let response = self
            .send_with_retry(
                || self.client.get(url.clone()).bearer_auth(&auth.access_token),
                stats,
            )
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "salesforce query failed: {}",
                response.status()
            ));
        }
        let payload: QueryResponse = response.json().await?;
        Ok(payload)
    }

    async fn send_with_retry<F>(
        &self,
        build: F,
        stats: Option<&StatsHandle>,
    ) -> Result<reqwest::Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let (max_retries, mut backoff_ms, max_backoff_ms) = self.retry_settings();
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let response = build().send().await?;
            if response.status().is_success() {
                if let Some(stats) = stats {
                    stats.record_api_call(false).await;
                }
                return Ok(response);
            }

            let status = response.status();
            if let Some(stats) = stats {
                stats
                    .record_api_call(status == StatusCode::TOO_MANY_REQUESTS)
                    .await;
            }
            if is_retryable_status(status) && attempt <= max_retries {
                let retry_after_ms = parse_retry_after_ms(&response);
                let wait_ms = retry_after_ms.unwrap_or(backoff_ms);
                sleep(Duration::from_millis(wait_ms)).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(max_backoff_ms);
                continue;
            }

            return Err(anyhow::anyhow!("salesforce request failed: {}", status));
        }
    }

    fn retry_settings(&self) -> (u32, u64, u64) {
        let defaults = (5, 1_000, 30_000);
        if let Some(config) = &self.config.rate_limit {
            let max_retries = config.max_retries.unwrap_or(defaults.0);
            let backoff_ms = config.backoff_ms.unwrap_or(defaults.1);
            let max_backoff_ms = config.max_backoff_ms.unwrap_or(defaults.2);
            (max_retries, backoff_ms, max_backoff_ms)
        } else {
            defaults
        }
    }
}

fn build_soql(
    fields: &[String],
    object: &str,
    mode: crate::types::SyncMode,
    since: Option<DateTime<Utc>>,
) -> String {
    let mut select_fields = fields.to_vec();
    if !select_fields.iter().any(|f| f == "SystemModstamp") {
        select_fields.push("SystemModstamp".to_string());
    }
    if !select_fields.iter().any(|f| f == "IsDeleted") {
        select_fields.push("IsDeleted".to_string());
    }

    let mut soql = format!("SELECT {} FROM {}", select_fields.join(", "), object);
    if let crate::types::SyncMode::Incremental = mode
        && let Some(since) = since
    {
        soql.push_str(&format!(
            " WHERE SystemModstamp > {}",
            format_salesforce_ts(since)
        ));
    }
    soql
}

fn format_salesforce_ts(ts: DateTime<Utc>) -> String {
    format!("{}", ts.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

fn salesforce_type_to_data_type(field_type: &str) -> DataType {
    match field_type {
        "boolean" => DataType::Bool,
        "double" | "currency" | "percent" => DataType::Float64,
        "int" => DataType::Int64,
        "date" => DataType::Date,
        "datetime" => DataType::Timestamp,
        "base64" => DataType::Bytes,
        "json" => DataType::Json,
        _ => DataType::String,
    }
}

fn ensure_field(fields: &mut Vec<String>, name: &str) {
    if !fields.iter().any(|f| f == name) {
        fields.push(name.to_string());
    }
}

fn polars_schema_with_metadata(schema: &TableSchema) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len() + 2);
    for column in &schema.columns {
        let dtype = match column.data_type {
            DataType::Int64 => PolarsDataType::Int64,
            DataType::Float64 => PolarsDataType::Float64,
            DataType::Bool => PolarsDataType::Boolean,
            _ => PolarsDataType::String,
        };
        fields.push(Field::new(column.name.as_str().into(), dtype));
    }
    fields.push(Field::new(META_SYNCED_AT.into(), PolarsDataType::String));
    fields.push(Field::new(META_DELETED_AT.into(), PolarsDataType::String));
    Schema::from_iter(fields)
}

fn records_to_frame(
    schema: &TableSchema,
    records: &[Map<String, Value>],
    object: &ResolvedSalesforceObject,
    synced_at: DateTime<Utc>,
) -> Result<DataFrame> {
    let polars_schema = polars_schema_with_metadata(schema);
    let mut rows: Vec<PolarsRow> = Vec::with_capacity(records.len());
    for record in records {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for column in &schema.columns {
            let value = record.get(&column.name).cloned().unwrap_or(Value::Null);
            values.push(salesforce_value_to_anyvalue(&value, &column.data_type));
        }
        values.push(AnyValue::StringOwned(PlSmallStr::from(
            synced_at.to_rfc3339(),
        )));
        let deleted_at = if object.soft_delete {
            match record.get("IsDeleted") {
                Some(Value::Bool(true)) => Value::String(synced_at.to_rfc3339()),
                _ => Value::Null,
            }
        } else {
            Value::Null
        };
        match deleted_at {
            Value::String(value) => values.push(AnyValue::StringOwned(PlSmallStr::from(value))),
            _ => values.push(AnyValue::Null),
        }
        rows.push(PolarsRow::new(values));
    }
    Ok(DataFrame::from_rows_and_schema(&rows, &polars_schema)?)
}

fn salesforce_value_to_anyvalue(value: &Value, data_type: &DataType) -> AnyValue<'static> {
    match data_type {
        DataType::Int64 => match value {
            Value::Number(num) => num.as_i64().map(AnyValue::Int64).unwrap_or(AnyValue::Null),
            Value::String(s) => s
                .parse::<i64>()
                .ok()
                .map(AnyValue::Int64)
                .unwrap_or(AnyValue::Null),
            _ => AnyValue::Null,
        },
        DataType::Float64 => match value {
            Value::Number(num) => num
                .as_f64()
                .map(AnyValue::Float64)
                .unwrap_or(AnyValue::Null),
            Value::String(s) => s
                .parse::<f64>()
                .ok()
                .map(AnyValue::Float64)
                .unwrap_or(AnyValue::Null),
            _ => AnyValue::Null,
        },
        DataType::Bool => match value {
            Value::Bool(b) => AnyValue::Boolean(*b),
            Value::String(s) => s
                .parse::<bool>()
                .ok()
                .map(AnyValue::Boolean)
                .unwrap_or(AnyValue::Null),
            _ => AnyValue::Null,
        },
        _ => match value {
            Value::String(s) => AnyValue::StringOwned(PlSmallStr::from(s.clone())),
            Value::Null => AnyValue::Null,
            other => AnyValue::StringOwned(PlSmallStr::from(other.to_string())),
        },
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::REQUEST_TIMEOUT
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
            | StatusCode::INTERNAL_SERVER_ERROR
    )
}

fn parse_retry_after_ms(response: &reqwest::Response) -> Option<u64> {
    let header = response.headers().get("retry-after")?;
    let value = header.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds.saturating_mul(1000));
    }
    None
}

fn apply_object_defaults(
    object: &SalesforceObjectConfig,
    defaults: Option<&SalesforceObjectDefaults>,
) -> ResolvedSalesforceObject {
    let primary_key = object
        .primary_key
        .clone()
        .or_else(|| defaults.and_then(|d| d.primary_key.clone()))
        .unwrap_or_else(|| "Id".to_string());
    let soft_delete = object
        .soft_delete
        .or_else(|| defaults.and_then(|d| d.soft_delete))
        .unwrap_or(true);
    let fields = object
        .fields
        .clone()
        .or_else(|| defaults.and_then(|d| d.fields.clone()));

    ResolvedSalesforceObject {
        name: object.name.clone(),
        primary_key,
        fields,
        soft_delete,
    }
}

fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

#[derive(Debug, Deserialize)]
struct AuthResponse {
    access_token: String,
    instance_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DescribeResponse {
    fields: Vec<DescribeField>,
}

#[derive(Debug, Deserialize)]
struct DescribeField {
    name: String,
    #[serde(rename = "type")]
    r#type: String,
    nillable: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct QueryResponse {
    records: Vec<Map<String, Value>>,
    #[serde(rename = "nextRecordsUrl")]
    next_records_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SobjectsResponse {
    sobjects: Vec<SobjectSummary>,
}

#[derive(Debug, Deserialize)]
struct SobjectSummary {
    name: String,
}
