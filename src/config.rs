use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub connections: Vec<ConnectionConfig>,
    pub state: StateConfig,
    pub metadata: Option<MetadataConfig>,
    pub logging: Option<LoggingConfig>,
    pub admin_api: Option<AdminApiConfig>,
    pub observability: Option<ObservabilityConfig>,
    pub sync: Option<SyncConfig>,
    pub stats: Option<StatsConfig>,
}

impl Config {
    pub async fn load(path: &Path) -> anyhow::Result<Self> {
        let contents = tokio::fs::read_to_string(path).await?;
        let cfg: Config = if path.extension().and_then(|e| e.to_str()) == Some("toml") {
            toml::from_str(&contents)?
        } else {
            yaml_serde::from_str(&contents)?
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.connections.is_empty() {
            anyhow::bail!("config must include at least one connection");
        }
        if self.admin_api.as_ref().is_some_and(AdminApiConfig::enabled) {
            let auth = self
                .admin_api
                .as_ref()
                .and_then(|admin_api| admin_api.auth.as_ref())
                .context("admin_api.auth is required when admin_api.enabled=true")?;
            auth.validate()?;
        }
        for connection in &self.connections {
            connection.validate()?;
            self.validate_cdc_control_plane_isolated(connection)?;
        }
        Ok(())
    }

    fn validate_cdc_control_plane_isolated(
        &self,
        connection: &ConnectionConfig,
    ) -> anyhow::Result<()> {
        let SourceConfig::Postgres(pg) = &connection.source else {
            return Ok(());
        };
        if !pg.cdc.unwrap_or(true) || pg.cdc_ack_boundary() != CdcAckBoundary::DurableEnqueue {
            return Ok(());
        }
        if postgres_urls_share_authority(&self.state.url, &pg.url)?
            && !postgres_url_uses_loopback_host(&pg.url)?
        {
            anyhow::bail!(
                "connection `{}` uses postgres.cdc_ack_boundary=durable_enqueue but state.url shares the source Postgres host/port; use a dedicated state database outside the source writer",
                connection.id
            );
        }
        Ok(())
    }

    pub fn metadata_columns(&self) -> crate::types::MetadataColumns {
        let mut metadata = crate::types::MetadataColumns::default();
        if let Some(config) = &self.metadata {
            if let Some(synced_at) = &config.synced_at_column {
                metadata.synced_at = synced_at.clone();
            }
            if let Some(deleted_at) = &config.deleted_at_column {
                metadata.deleted_at = deleted_at.clone();
            }
        }
        metadata
    }

    pub fn template(path: &Path) -> TemplateConfig {
        TemplateConfig {
            path: path.to_path_buf(),
            content: DEFAULT_TEMPLATE.to_string(),
        }
    }

    pub fn state_pool_max_connections(&self) -> u32 {
        const MIN_STATE_POOL_MAX_CONNECTIONS: u32 = 16;
        const STATE_POOL_CONTROL_HEADROOM_PER_CDC_CONNECTION: u32 = 4;
        const STATE_POOL_BASE_HEADROOM: u32 = 4;

        let fallback_snapshot_concurrency = self
            .sync
            .as_ref()
            .and_then(|sync| sync.max_concurrency)
            .unwrap_or(1)
            .max(1);

        let mut cdc_connection_count = 0_u32;
        let mut batch_load_workers = 0_u32;

        for connection in self
            .connections
            .iter()
            .filter(|connection| connection.enabled())
        {
            if let SourceConfig::Postgres(pg) = &connection.source {
                if !pg.cdc.unwrap_or(true) {
                    continue;
                }
                cdc_connection_count = cdc_connection_count.saturating_add(1);
                let apply_concurrency = pg.cdc_apply_concurrency(fallback_snapshot_concurrency);
                let worker_count = pg
                    .cdc_batch_load_staging_worker_count(apply_concurrency)
                    .saturating_add(pg.cdc_batch_load_reducer_worker_count(apply_concurrency));
                batch_load_workers = batch_load_workers
                    .saturating_add(u32::try_from(worker_count).unwrap_or(u32::MAX));
            }
        }

        let control_headroom = cdc_connection_count
            .saturating_mul(STATE_POOL_CONTROL_HEADROOM_PER_CDC_CONNECTION)
            .saturating_add(STATE_POOL_BASE_HEADROOM);

        MIN_STATE_POOL_MAX_CONNECTIONS.max(batch_load_workers.saturating_add(control_headroom))
    }
}

fn postgres_urls_share_authority(left: &str, right: &str) -> anyhow::Result<bool> {
    let left = Url::parse(left).context("invalid state.url")?;
    let right = Url::parse(right).context("invalid postgres source url")?;
    if left.scheme() != "postgres" && left.scheme() != "postgresql" {
        return Ok(false);
    }
    if right.scheme() != "postgres" && right.scheme() != "postgresql" {
        return Ok(false);
    }
    Ok(left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default())
}

fn postgres_url_uses_loopback_host(value: &str) -> anyhow::Result<bool> {
    let url = Url::parse(value).context("invalid postgres source url")?;
    Ok(matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "::1")
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    pub synced_at_column: Option<String>,
    pub deleted_at_column: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub default_batch_size: Option<usize>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub max_concurrency: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: Option<String>,
    pub json: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminApiConfig {
    pub enabled: Option<bool>,
    pub bind: Option<String>,
    pub auth: Option<AdminApiAuthConfig>,
}

impl AdminApiConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AdminApiAuthConfig {
    #[serde(default)]
    pub service_jwt_public_keys: HashMap<String, String>,
    pub service_jwt_public_keys_json: Option<String>,
    #[serde(default)]
    pub service_jwt_allowed_issuers: Vec<String>,
    #[serde(default)]
    pub service_jwt_allowed_audiences: Vec<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
}

impl AdminApiAuthConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.resolved_public_keys()?.is_empty() {
            anyhow::bail!(
                "admin_api.auth requires service JWT public keys via service_jwt_public_keys, service_jwt_public_keys_json, or CDSYNC_SERVICE_JWT_PUBLIC_KEYS_JSON"
            );
        }
        if self.resolved_allowed_issuers().is_empty() {
            anyhow::bail!(
                "admin_api.auth requires allowed issuers via service_jwt_allowed_issuers or CDSYNC_SERVICE_JWT_ALLOWED_ISSUERS"
            );
        }
        if self.resolved_allowed_audiences().is_empty() {
            anyhow::bail!(
                "admin_api.auth requires allowed audiences via service_jwt_allowed_audiences or CDSYNC_SERVICE_JWT_ALLOWED_AUDIENCES"
            );
        }
        Ok(())
    }

    pub fn resolved_allowed_issuers(&self) -> Vec<String> {
        if !self.service_jwt_allowed_issuers.is_empty() {
            return self.service_jwt_allowed_issuers.clone();
        }
        split_csv_env(std::env::var("CDSYNC_SERVICE_JWT_ALLOWED_ISSUERS").ok())
    }

    pub fn resolved_allowed_audiences(&self) -> Vec<String> {
        if !self.service_jwt_allowed_audiences.is_empty() {
            return self.service_jwt_allowed_audiences.clone();
        }
        let env_values = split_csv_env(std::env::var("CDSYNC_SERVICE_JWT_ALLOWED_AUDIENCES").ok());
        if !env_values.is_empty() {
            return env_values;
        }
        vec!["cdsync".to_string()]
    }

    pub fn resolved_required_scopes(&self) -> Vec<String> {
        if !self.required_scopes.is_empty() {
            return self.required_scopes.clone();
        }
        vec!["cdsync:admin".to_string()]
    }

    pub fn resolved_public_keys(&self) -> anyhow::Result<HashMap<String, String>> {
        if !self.service_jwt_public_keys.is_empty() {
            return Ok(normalize_public_keys(&self.service_jwt_public_keys));
        }
        let raw = self
            .service_jwt_public_keys_json
            .clone()
            .or_else(|| std::env::var("CDSYNC_SERVICE_JWT_PUBLIC_KEYS_JSON").ok());
        let Some(raw) = raw else {
            return Ok(HashMap::new());
        };
        let parsed: HashMap<String, String> =
            serde_json::from_str(&raw).context("invalid service JWT public keys JSON")?;
        Ok(normalize_public_keys(&parsed))
    }
}

fn split_csv_env(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_public_keys(raw: &HashMap<String, String>) -> HashMap<String, String> {
    raw.iter()
        .filter_map(|(kid, pem)| {
            let normalized = pem.replace("\\n", "\n").trim().to_string();
            (!kid.trim().is_empty() && !normalized.is_empty()).then(|| (kid.clone(), normalized))
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub service_name: Option<String>,
    pub otlp_traces_endpoint: Option<String>,
    pub otlp_metrics_endpoint: Option<String>,
    pub otlp_headers: Option<std::collections::HashMap<String, String>>,
    pub metrics_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub url: String,
    pub schema: Option<String>,
}

impl StateConfig {
    pub fn schema_name(&self) -> &str {
        self.schema.as_deref().unwrap_or("cdsync_state")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    pub url: Option<String>,
    pub schema: Option<String>,
}

impl StatsConfig {
    pub fn schema_name(&self) -> &str {
        self.schema.as_deref().unwrap_or("cdsync_stats")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    pub enabled: Option<bool>,
    pub source: SourceConfig,
    pub destination: DestinationConfig,
    pub schedule: Option<ScheduleConfig>,
}

impl ConnectionConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.source.validate()?;
        self.destination.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConfig {
    pub every: Option<String>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "postgres")]
    Postgres(PostgresConfig),
    #[serde(rename = "dynamodb")]
    DynamoDb(DynamoDbConfig),
}

impl SourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            SourceConfig::Postgres(pg) => pg.validate(),
            SourceConfig::DynamoDb(dynamo) => dynamo.validate(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DestinationConfig {
    #[serde(rename = "bigquery")]
    BigQuery(BigQueryConfig),
}

impl DestinationConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            DestinationConfig::BigQuery(bq) => bq.validate(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    pub tables: Option<Vec<PostgresTableConfig>>,
    pub table_selection: Option<TableSelectionConfig>,
    pub batch_size: Option<usize>,
    pub cdc: Option<bool>,
    pub publication: Option<String>,
    pub publication_mode: Option<PostgresPublicationMode>,
    pub schema_changes: Option<SchemaChangePolicy>,
    pub cdc_pipeline_id: Option<u64>,
    pub cdc_batch_size: Option<usize>,
    pub cdc_apply_concurrency: Option<usize>,
    pub cdc_batch_load_worker_count: Option<usize>,
    pub cdc_batch_load_staging_worker_count: Option<usize>,
    pub cdc_batch_load_reducer_worker_count: Option<usize>,
    pub cdc_max_inflight_commits: Option<usize>,
    pub cdc_batch_load_reducer_max_jobs: Option<usize>,
    pub cdc_batch_load_reducer_max_fill_ms: Option<u64>,
    pub cdc_batch_load_reducer_enabled: Option<bool>,
    pub cdc_ack_boundary: Option<CdcAckBoundary>,
    pub cdc_backlog_max_pending_fragments: Option<usize>,
    pub cdc_backlog_max_oldest_pending_seconds: Option<u64>,
    pub cdc_max_fill_ms: Option<u64>,
    pub cdc_max_pending_events: Option<usize>,
    pub cdc_idle_timeout_seconds: Option<u64>,
    pub cdc_tls: Option<bool>,
    pub cdc_tls_ca_path: Option<PathBuf>,
    pub cdc_tls_ca: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbConfig {
    pub table_name: String,
    pub region: String,
    pub export_bucket: String,
    pub export_prefix: Option<String>,
    pub kinesis_stream_name: Option<String>,
    pub kinesis_stream_arn: Option<String>,
    pub raw_item_column: Option<String>,
    #[serde(default)]
    pub key_attributes: Vec<String>,
    #[serde(default)]
    pub attributes: Vec<DynamoDbAttributeConfig>,
}

impl DynamoDbConfig {
    pub fn raw_item_column_name(&self) -> &str {
        self.raw_item_column
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("raw_item_json")
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.table_name.trim().is_empty() {
            anyhow::bail!("dynamodb.table_name is required");
        }
        if self.region.trim().is_empty() {
            anyhow::bail!("dynamodb.region is required");
        }
        if self.export_bucket.trim().is_empty() {
            anyhow::bail!("dynamodb.export_bucket is required");
        }
        if self.key_attributes.is_empty() {
            anyhow::bail!("dynamodb.key_attributes must include at least one key field");
        }
        if self.key_attributes.len() != 1 {
            anyhow::bail!("dynamodb currently supports exactly one key attribute per connection");
        }
        let has_stream_name = self
            .kinesis_stream_name
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        let has_stream_arn = self
            .kinesis_stream_arn
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        if has_stream_name == has_stream_arn {
            anyhow::bail!(
                "dynamodb must set exactly one of dynamodb.kinesis_stream_name or dynamodb.kinesis_stream_arn"
            );
        }

        let raw_item_column = self.raw_item_column_name();
        if self
            .attributes
            .iter()
            .any(|attribute| attribute.name == raw_item_column)
        {
            anyhow::bail!("dynamodb.raw_item_column must not reuse an attribute name");
        }

        let mut seen_keys = std::collections::HashSet::new();
        for key in &self.key_attributes {
            if key.trim().is_empty() {
                anyhow::bail!("dynamodb.key_attributes entries must not be empty");
            }
            if !seen_keys.insert(key.clone()) {
                anyhow::bail!("duplicate dynamodb.key_attributes entry `{}`", key);
            }
        }
        let mut seen_attributes = std::collections::HashSet::new();
        for attribute in &self.attributes {
            attribute.validate()?;
            if !seen_attributes.insert(attribute.name.clone()) {
                anyhow::bail!("duplicate dynamodb attribute `{}`", attribute.name);
            }
        }
        for key in &self.key_attributes {
            if !self
                .attributes
                .iter()
                .any(|attribute| attribute.name == *key)
            {
                anyhow::bail!(
                    "dynamodb key attribute `{}` must also appear in dynamodb.attributes",
                    key
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoDbAttributeConfig {
    pub name: String,
    pub data_type: DynamoDbAttributeType,
    pub nullable: Option<bool>,
}

impl DynamoDbAttributeConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.name.trim().is_empty() {
            anyhow::bail!("dynamodb.attributes[].name is required");
        }
        Ok(())
    }

    pub fn to_column_schema(&self) -> crate::types::ColumnSchema {
        crate::types::ColumnSchema {
            name: self.name.clone(),
            data_type: self.data_type.to_data_type(),
            nullable: self.nullable.unwrap_or(true),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DynamoDbAttributeType {
    String,
    Int64,
    Float64,
    Bool,
    Timestamp,
    Date,
    Bytes,
    Numeric,
    Json,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CdcAckBoundary {
    TargetApply,
    DurableEnqueue,
}

impl DynamoDbAttributeType {
    fn to_data_type(&self) -> crate::types::DataType {
        match self {
            Self::String => crate::types::DataType::String,
            Self::Int64 => crate::types::DataType::Int64,
            Self::Float64 => crate::types::DataType::Float64,
            Self::Bool => crate::types::DataType::Bool,
            Self::Timestamp => crate::types::DataType::Timestamp,
            Self::Date => crate::types::DataType::Date,
            Self::Bytes => crate::types::DataType::Bytes,
            Self::Numeric => crate::types::DataType::Numeric,
            Self::Json => crate::types::DataType::Json,
        }
    }
}

impl PostgresConfig {
    pub fn publication_mode(&self) -> PostgresPublicationMode {
        self.publication_mode
            .clone()
            .unwrap_or(PostgresPublicationMode::Validate)
    }

    pub fn schema_policy(&self) -> SchemaChangePolicy {
        self.schema_changes
            .clone()
            .unwrap_or(SchemaChangePolicy::Auto)
    }

    pub fn cdc_apply_concurrency(&self, fallback: usize) -> usize {
        self.cdc_apply_concurrency.unwrap_or(fallback).max(1)
    }

    pub fn cdc_batch_load_worker_count(&self, fallback: usize) -> usize {
        self.cdc_batch_load_worker_count.unwrap_or(fallback).max(1)
    }

    pub fn cdc_batch_load_staging_worker_count(&self, fallback: usize) -> usize {
        self.cdc_batch_load_staging_worker_count
            .unwrap_or_else(|| self.cdc_batch_load_worker_count(fallback))
            .max(1)
    }

    pub fn cdc_batch_load_reducer_worker_count(&self, fallback: usize) -> usize {
        self.cdc_batch_load_reducer_worker_count
            .unwrap_or_else(|| self.cdc_batch_load_worker_count(fallback))
            .max(1)
    }

    pub fn cdc_max_inflight_commits(&self, apply_concurrency: usize) -> usize {
        self.cdc_max_inflight_commits
            .unwrap_or_else(|| apply_concurrency.max(1).saturating_mul(4))
            .max(1)
    }

    pub fn cdc_batch_load_reducer_max_jobs(&self) -> usize {
        if self.cdc_batch_load_reducer_enabled() {
            self.cdc_batch_load_reducer_max_jobs.unwrap_or(16).max(1)
        } else {
            1
        }
    }

    pub fn cdc_batch_load_reducer_max_fill_ms(&self) -> u64 {
        if !self.cdc_batch_load_reducer_enabled() || self.cdc_batch_load_reducer_max_jobs() <= 1 {
            return 0;
        }
        self.cdc_batch_load_reducer_max_fill_ms.unwrap_or(0)
    }

    pub fn cdc_batch_load_reducer_enabled(&self) -> bool {
        self.cdc_batch_load_reducer_enabled.unwrap_or(true)
    }

    pub fn cdc_ack_boundary(&self) -> CdcAckBoundary {
        self.cdc_ack_boundary.unwrap_or(CdcAckBoundary::TargetApply)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let has_tables = self
            .tables
            .as_ref()
            .is_some_and(|tables| !tables.is_empty());
        let has_selection = self.table_selection.as_ref().is_some_and(|selection| {
            !(selection.include.is_empty() && selection.exclude.is_empty())
        });
        if !has_tables && !has_selection {
            anyhow::bail!("postgres requires tables or table_selection.include/exclude");
        }
        if self.cdc.unwrap_or(true) {
            let publication = self.publication.as_deref().unwrap_or("");
            if publication.is_empty() {
                anyhow::bail!("postgres.publication is required when CDC is enabled");
            }
            if self.cdc_pipeline_id.is_none() {
                anyhow::bail!("postgres.cdc_pipeline_id is required when CDC is enabled");
            }
            if self.cdc_ack_boundary() == CdcAckBoundary::DurableEnqueue
                && self.cdc_backlog_max_pending_fragments.is_none()
                && self.cdc_backlog_max_oldest_pending_seconds.is_none()
            {
                anyhow::bail!(
                    "postgres.cdc_ack_boundary=durable_enqueue requires postgres.cdc_backlog_max_pending_fragments or postgres.cdc_backlog_max_oldest_pending_seconds"
                );
            }
            if self.cdc_tls.unwrap_or(false)
                && self.cdc_tls_ca.is_none()
                && self.cdc_tls_ca_path.is_none()
            {
                anyhow::bail!(
                    "postgres.cdc_tls_ca or postgres.cdc_tls_ca_path required when postgres.cdc_tls=true"
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PostgresPublicationMode {
    Validate,
    Manage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSelectionConfig {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    pub defaults: Option<PostgresTableDefaults>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableDefaults {
    pub primary_key: Option<String>,
    pub updated_at_column: Option<String>,
    pub soft_delete: Option<bool>,
    pub soft_delete_column: Option<String>,
    pub where_clause: Option<String>,
    pub columns: Option<ColumnSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresTableConfig {
    pub name: String,
    pub primary_key: Option<String>,
    pub updated_at_column: Option<String>,
    pub soft_delete: Option<bool>,
    pub soft_delete_column: Option<String>,
    #[serde(alias = "row_filter")]
    pub where_clause: Option<String>,
    pub columns: Option<ColumnSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangePolicy {
    Auto,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSelection {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BigQueryConfig {
    pub project_id: String,
    pub dataset: String,
    pub location: Option<String>,
    pub service_account_key_path: Option<PathBuf>,
    pub service_account_key: Option<String>,
    pub partition_by_synced_at: Option<bool>,
    pub batch_load_bucket: Option<String>,
    pub batch_load_prefix: Option<String>,
    pub emulator_http: Option<String>,
    pub emulator_grpc: Option<String>,
}

impl BigQueryConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.project_id.trim().is_empty() {
            anyhow::bail!("bigquery.project_id is required");
        }
        if self.dataset.trim().is_empty() {
            anyhow::bail!("bigquery.dataset is required");
        }
        if self.emulator_http.is_none()
            && self
                .batch_load_bucket
                .as_deref()
                .is_none_or(|bucket| bucket.trim().is_empty())
        {
            anyhow::bail!(
                "bigquery.batch_load_bucket is required unless bigquery.emulator_http is set"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TemplateConfig {
    pub path: PathBuf,
    pub content: String,
}

const DEFAULT_TEMPLATE: &str = r#"# CDSync configuration (YAML)

state:
  url: "postgres://user:pass@host:5432/db"
  schema: "cdsync_state"

metadata:
  synced_at_column: "_cdsync_synced_at"
  deleted_at_column: "_cdsync_deleted_at"

logging:
  level: "info"
  json: false

admin_api:
  enabled: false
  bind: "127.0.0.1:8080"
  auth:
    service_jwt_allowed_issuers: []
    service_jwt_allowed_audiences: []
    required_scopes: ["cdsync:admin"]
    # Public keys should come from the environment, not inline config.
    # Set CDSYNC_SERVICE_JWT_* in the runtime environment or replace these
    # fields with your own issuer/audience values.

observability:
  service_name: "cdsync"
  # otlp_traces_endpoint: "http://localhost:4318/v1/traces"
  # otlp_metrics_endpoint: "http://localhost:4318/v1/metrics"
  metrics_interval_seconds: 30

sync:
  default_batch_size: 10000
  max_retries: 5
  retry_backoff_ms: 1000
  # Polling uses this for per-table parallelism; Postgres CDC uses it to bound
  # in-flight snapshot batch writes during initial copy/resync.
  max_concurrency: 4

stats:
  # If omitted, reporting is disabled.
  url: "postgres://user:pass@host:5432/db"
  schema: "cdsync_stats"

connections:
  - id: "app"
    enabled: true
    schedule:
      every: "30m"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: true
      publication: "cdsync_publication"
      publication_mode: validate
      schema_changes: auto
      cdc_pipeline_id: 1
      cdc_batch_size: 10000
      cdc_apply_concurrency: 8
      # Set split worker counts explicitly in production. The legacy
      # cdc_batch_load_worker_count fallback hides actual downstream concurrency.
      cdc_batch_load_staging_worker_count: 8
      cdc_batch_load_reducer_worker_count: 8
      # Defaults to cdc_apply_concurrency * 4.
      # This is the current in-memory read-ahead cap, not the future durable backlog budget.
      # cdc_max_inflight_commits: 32
      # cdc_batch_load_reducer_max_jobs: 16
      # Defaults to 0 for immediate claim. Set above 0 to let same-table
      # reducer windows coalesce before BigQuery MERGE.
      # cdc_batch_load_reducer_max_fill_ms: 5000
      # cdc_batch_load_reducer_enabled: true
      # ACK Postgres after target apply by default. durable_enqueue is experimental:
      # it ACKs after durable CDC landing and materializes BigQuery asynchronously.
      # cdc_ack_boundary: target_apply
      # CDC backlog backpressure caps. durable_enqueue requires at least one of
      # these so WAL intake stays bounded by durable or target materialization
      # backlog.
      # cdc_backlog_max_pending_fragments: 10000
      # cdc_backlog_max_oldest_pending_seconds: 300
      # Queued CDC staging/reducer claim SQL has a hard 5s timeout.
      cdc_max_fill_ms: 2000
      cdc_max_pending_events: 100000
      cdc_idle_timeout_seconds: 10
      cdc_tls: false
      # cdc_tls_ca_path: "/path/to/ca.pem"
      table_selection:
        include: ["public.*"]
        exclude: []
        defaults:
          primary_key: "id"
          updated_at_column: "updated_at"
          soft_delete: true
          soft_delete_column: "deleted_at"
      tables:
        - name: "public.accounts"
          primary_key: "id"
          updated_at_column: "updated_at"
          soft_delete: true
          soft_delete_column: "deleted_at"
          columns:
            include: ["id", "name", "email"]
            exclude: ["password"]
    destination:
      type: bigquery
      project_id: "your-project"
      dataset: "cdsync"
      service_account_key_path: "/path/to/service-account.json"
      partition_by_synced_at: true
      batch_load_bucket: "my-cdsync-loads"
      # batch_load_prefix: "staging/app"
      # emulator_http: "http://localhost:9050"
      # emulator_grpc: "localhost:9051"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_requires_publication_when_cdc_enabled() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: true
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_requires_cdc_pipeline_id_when_cdc_enabled() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: true
      publication: "cdsync_pub"
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        let err = cfg.validate().expect_err("missing cdc pipeline id");
        assert!(
            err.to_string()
                .contains("postgres.cdc_pipeline_id is required when CDC is enabled")
        );
    }

    #[test]
    fn validate_allows_cdc_disabled_without_publication() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: false
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_requires_batch_load_bucket_without_emulator() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: false
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        let err = cfg.validate().expect_err("missing batch-load bucket");
        assert!(
            err.to_string()
                .contains("bigquery.batch_load_bucket is required")
        );
    }

    #[test]
    fn validate_allows_dynamodb_key_attribute_projection() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
connections:
  - id: "sites_build"
    source:
      type: dynamodb
      table_name: "build"
      region: "us-east-1"
      export_bucket: "exports"
      kinesis_stream_name: "build-stream"
      key_attributes: ["id"]
      attributes:
        - name: "id"
          data_type: "string"
          nullable: false
        - name: "status"
          data_type: "string"
          nullable: true
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn metadata_columns_override_defaults() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
metadata:
  synced_at_column: "_synced_custom"
  deleted_at_column: "_deleted_custom"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: false
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        let metadata = cfg.metadata_columns();
        assert_eq!(metadata.synced_at, "_synced_custom");
        assert_eq!(metadata.deleted_at, "_deleted_custom");
    }

    #[test]
    fn cdc_apply_concurrency_defaults_to_fallback() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: None,
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_apply_concurrency(6), 6);
    }

    #[test]
    fn cdc_apply_concurrency_honors_explicit_override() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(9),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_apply_concurrency(4), 9);
    }

    #[test]
    fn cdc_batch_load_worker_count_defaults_to_apply_concurrency() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_batch_load_worker_count(8), 8);
    }

    #[test]
    fn cdc_batch_load_worker_count_honors_explicit_override() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: Some(12),
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_batch_load_worker_count(8), 12);
    }

    #[test]
    fn cdc_batch_load_split_worker_counts_default_to_legacy_worker_count() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: Some(12),
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_batch_load_staging_worker_count(8), 12);
        assert_eq!(config.cdc_batch_load_reducer_worker_count(8), 12);
    }

    #[test]
    fn cdc_batch_load_split_worker_counts_honor_explicit_overrides() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: Some(12),
            cdc_batch_load_staging_worker_count: Some(20),
            cdc_batch_load_reducer_worker_count: Some(4),
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_batch_load_staging_worker_count(8), 20);
        assert_eq!(config.cdc_batch_load_reducer_worker_count(8), 4);
    }

    #[test]
    fn cdc_max_inflight_commits_defaults_to_apply_concurrency_multiplier() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_max_inflight_commits(8), 32);
    }

    #[test]
    fn cdc_max_inflight_commits_honors_explicit_override() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: Some(128),
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_max_inflight_commits(8), 128);
    }

    #[test]
    fn cdc_batch_load_reducer_max_jobs_defaults_and_honors_override() {
        let default_config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };
        let override_config = PostgresConfig {
            cdc_batch_load_reducer_max_jobs: Some(64),
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            ..default_config.clone()
        };

        assert_eq!(default_config.cdc_batch_load_reducer_max_jobs(), 16);
        assert_eq!(override_config.cdc_batch_load_reducer_max_jobs(), 64);
    }

    #[test]
    fn cdc_batch_load_reducer_max_fill_defaults_to_immediate_and_honors_override() {
        let default_config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: Some(32),
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: Some(5_000),
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };
        let override_config = PostgresConfig {
            cdc_batch_load_reducer_max_fill_ms: Some(750),
            ..default_config.clone()
        };
        let disabled_config = PostgresConfig {
            cdc_batch_load_reducer_enabled: Some(false),
            ..default_config.clone()
        };
        let single_job_config = PostgresConfig {
            cdc_batch_load_reducer_max_jobs: Some(1),
            ..default_config.clone()
        };

        assert_eq!(default_config.cdc_batch_load_reducer_max_fill_ms(), 0);
        assert_eq!(override_config.cdc_batch_load_reducer_max_fill_ms(), 750);
        assert_eq!(disabled_config.cdc_batch_load_reducer_max_fill_ms(), 0);
        assert_eq!(single_job_config.cdc_batch_load_reducer_max_fill_ms(), 0);
    }

    #[test]
    fn cdc_batch_load_reducer_max_jobs_respects_rollout_gate() {
        let config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: Some(64),
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: Some(false),
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        assert_eq!(config.cdc_batch_load_reducer_max_jobs(), 1);
    }

    #[test]
    fn cdc_ack_boundary_defaults_to_target_apply_and_honors_override() {
        let default_config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: None,
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: None,
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: None,
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };
        let durable_config = PostgresConfig {
            cdc_ack_boundary: Some(CdcAckBoundary::DurableEnqueue),
            ..default_config.clone()
        };

        assert_eq!(
            default_config.cdc_ack_boundary(),
            CdcAckBoundary::TargetApply
        );
        assert_eq!(
            durable_config.cdc_ack_boundary(),
            CdcAckBoundary::DurableEnqueue
        );
    }

    #[test]
    fn durable_enqueue_requires_backpressure_budget() {
        let mut config = PostgresConfig {
            url: "postgres://user:pass@host:5432/db".to_string(),
            tables: None,
            table_selection: Some(TableSelectionConfig {
                include: vec!["public.*".to_string()],
                exclude: Vec::new(),
                defaults: None,
            }),
            batch_size: None,
            cdc: Some(true),
            publication: Some("cdsync_pub".to_string()),
            publication_mode: None,
            schema_changes: None,
            cdc_pipeline_id: Some(1),
            cdc_batch_size: None,
            cdc_apply_concurrency: Some(8),
            cdc_batch_load_worker_count: None,
            cdc_batch_load_staging_worker_count: None,
            cdc_batch_load_reducer_worker_count: None,
            cdc_max_inflight_commits: None,
            cdc_batch_load_reducer_max_jobs: None,
            cdc_batch_load_reducer_max_fill_ms: None,
            cdc_batch_load_reducer_enabled: None,
            cdc_ack_boundary: Some(CdcAckBoundary::DurableEnqueue),
            cdc_backlog_max_pending_fragments: None,
            cdc_backlog_max_oldest_pending_seconds: None,
            cdc_max_fill_ms: None,
            cdc_max_pending_events: None,
            cdc_idle_timeout_seconds: None,
            cdc_tls: None,
            cdc_tls_ca_path: None,
            cdc_tls_ca: None,
        };

        let err = config.validate().expect_err("durable enqueue needs caps");
        assert!(
            err.to_string()
                .contains("durable_enqueue requires postgres.cdc_backlog")
        );

        config.cdc_backlog_max_pending_fragments = Some(10_000);
        config
            .validate()
            .expect("backpressure cap should be enough");
    }

    #[test]
    fn durable_enqueue_requires_state_outside_source_writer() {
        let raw = r#"
state:
  url: "postgres://user:pass@source-host:5432/cdsync"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@source-host:5432/app"
      cdc: true
      publication: "cdsync_pub"
      cdc_pipeline_id: 1
      cdc_ack_boundary: durable_enqueue
      cdc_backlog_max_pending_fragments: 10000
      table_selection:
        include: ["public.*"]
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        let err = cfg
            .validate()
            .expect_err("colocated durable queue is unsafe");
        assert!(
            err.to_string()
                .contains("state.url shares the source Postgres host/port")
        );
    }

    #[test]
    fn durable_enqueue_allows_dedicated_state_host() {
        let raw = r#"
state:
  url: "postgres://user:pass@state-host:5432/cdsync"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@source-host:5432/app"
      cdc: true
      publication: "cdsync_pub"
      cdc_pipeline_id: 1
      cdc_ack_boundary: durable_enqueue
      cdc_backlog_max_pending_fragments: 10000
      table_selection:
        include: ["public.*"]
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        cfg.validate().expect("dedicated state host is allowed");
    }

    #[test]
    fn durable_enqueue_allows_loopback_state_in_tests() {
        let raw = r#"
state:
  url: "postgres://user:pass@localhost:5432/cdsync"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@localhost:5432/app"
      cdc: true
      publication: "cdsync_pub"
      cdc_pipeline_id: 1
      cdc_ack_boundary: durable_enqueue
      cdc_backlog_max_pending_fragments: 10000
      table_selection:
        include: ["public.*"]
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        cfg.validate()
            .expect("local integration tests can share loopback Postgres");
    }

    #[test]
    fn state_pool_max_connections_uses_minimum_floor() {
        let cfg = Config {
            connections: vec![ConnectionConfig {
                id: "app".to_string(),
                enabled: Some(true),
                source: SourceConfig::Postgres(PostgresConfig {
                    url: "postgres://user:pass@host:5432/db".to_string(),
                    tables: None,
                    table_selection: Some(TableSelectionConfig {
                        include: vec!["public.*".to_string()],
                        exclude: vec![],
                        defaults: None,
                    }),
                    batch_size: None,
                    cdc: Some(true),
                    publication: Some("cdsync_pub".to_string()),
                    publication_mode: None,
                    schema_changes: None,
                    cdc_pipeline_id: Some(1),
                    cdc_batch_size: None,
                    cdc_apply_concurrency: Some(8),
                    cdc_batch_load_worker_count: Some(8),
                    cdc_batch_load_staging_worker_count: None,
                    cdc_batch_load_reducer_worker_count: None,
                    cdc_max_inflight_commits: None,
                    cdc_batch_load_reducer_max_jobs: None,
                    cdc_batch_load_reducer_max_fill_ms: None,
                    cdc_batch_load_reducer_enabled: None,
                    cdc_ack_boundary: None,
                    cdc_backlog_max_pending_fragments: None,
                    cdc_backlog_max_oldest_pending_seconds: None,
                    cdc_max_fill_ms: None,
                    cdc_max_pending_events: None,
                    cdc_idle_timeout_seconds: None,
                    cdc_tls: None,
                    cdc_tls_ca_path: None,
                    cdc_tls_ca: None,
                }),
                destination: DestinationConfig::BigQuery(BigQueryConfig {
                    project_id: "proj".to_string(),
                    dataset: "ds".to_string(),
                    location: None,
                    service_account_key_path: None,
                    service_account_key: None,
                    partition_by_synced_at: None,
                    batch_load_bucket: None,
                    batch_load_prefix: None,
                    emulator_http: None,
                    emulator_grpc: None,
                }),
                schedule: None,
            }],
            state: StateConfig {
                url: "postgres://user:pass@host:5432/state".to_string(),
                schema: None,
            },
            metadata: None,
            logging: None,
            admin_api: None,
            observability: None,
            sync: Some(SyncConfig {
                default_batch_size: None,
                max_retries: None,
                retry_backoff_ms: None,
                max_concurrency: Some(4),
            }),
            stats: None,
        };

        assert_eq!(cfg.state_pool_max_connections(), 24);
    }

    #[test]
    fn state_pool_max_connections_scales_with_batch_load_workers() {
        let cfg = Config {
            connections: vec![ConnectionConfig {
                id: "app".to_string(),
                enabled: Some(true),
                source: SourceConfig::Postgres(PostgresConfig {
                    url: "postgres://user:pass@host:5432/db".to_string(),
                    tables: None,
                    table_selection: Some(TableSelectionConfig {
                        include: vec!["public.*".to_string()],
                        exclude: vec![],
                        defaults: None,
                    }),
                    batch_size: None,
                    cdc: Some(true),
                    publication: Some("cdsync_pub".to_string()),
                    publication_mode: None,
                    schema_changes: None,
                    cdc_pipeline_id: Some(1),
                    cdc_batch_size: None,
                    cdc_apply_concurrency: Some(32),
                    cdc_batch_load_worker_count: Some(32),
                    cdc_batch_load_staging_worker_count: None,
                    cdc_batch_load_reducer_worker_count: None,
                    cdc_max_inflight_commits: None,
                    cdc_batch_load_reducer_max_jobs: None,
                    cdc_batch_load_reducer_max_fill_ms: None,
                    cdc_batch_load_reducer_enabled: None,
                    cdc_ack_boundary: None,
                    cdc_backlog_max_pending_fragments: None,
                    cdc_backlog_max_oldest_pending_seconds: None,
                    cdc_max_fill_ms: None,
                    cdc_max_pending_events: None,
                    cdc_idle_timeout_seconds: None,
                    cdc_tls: None,
                    cdc_tls_ca_path: None,
                    cdc_tls_ca: None,
                }),
                destination: DestinationConfig::BigQuery(BigQueryConfig {
                    project_id: "proj".to_string(),
                    dataset: "ds".to_string(),
                    location: None,
                    service_account_key_path: None,
                    service_account_key: None,
                    partition_by_synced_at: None,
                    batch_load_bucket: None,
                    batch_load_prefix: None,
                    emulator_http: None,
                    emulator_grpc: None,
                }),
                schedule: None,
            }],
            state: StateConfig {
                url: "postgres://user:pass@host:5432/state".to_string(),
                schema: None,
            },
            metadata: None,
            logging: None,
            admin_api: None,
            observability: None,
            sync: Some(SyncConfig {
                default_batch_size: None,
                max_retries: None,
                retry_backoff_ms: None,
                max_concurrency: Some(4),
            }),
            stats: None,
        };

        assert_eq!(cfg.state_pool_max_connections(), 72);
    }

    #[test]
    fn validate_requires_admin_api_auth_when_enabled() {
        let raw = r#"
state:
  url: "postgres://user:pass@host:5432/db"
admin_api:
  enabled: true
  bind: "127.0.0.1:8080"
connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/db"
      cdc: false
      tables:
        - name: "public.accounts"
          primary_key: "id"
    destination:
      type: bigquery
      project_id: "proj"
      dataset: "ds"
      emulator_http: "http://localhost:9050"
"#;
        let cfg: Config = yaml_serde::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_err());
    }
}
