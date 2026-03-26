use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub connections: Vec<ConnectionConfig>,
    pub state: StateConfig,
    pub logging: Option<LoggingConfig>,
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
            serde_yaml::from_str(&contents)?
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.connections.is_empty() {
            anyhow::bail!("config must include at least one connection");
        }
        for connection in &self.connections {
            connection.validate()?;
        }
        Ok(())
    }

    pub fn template(path: &Path) -> TemplateConfig {
        TemplateConfig {
            path: path.to_path_buf(),
            content: DEFAULT_TEMPLATE.to_string(),
        }
    }
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
pub struct ObservabilityConfig {
    pub service_name: Option<String>,
    pub otlp_traces_endpoint: Option<String>,
    pub otlp_metrics_endpoint: Option<String>,
    pub otlp_headers: Option<std::collections::HashMap<String, String>>,
    pub metrics_interval_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsConfig {
    pub path: PathBuf,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "postgres")]
    Postgres(PostgresConfig),
    #[serde(rename = "salesforce")]
    Salesforce(SalesforceConfig),
}

impl SourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            SourceConfig::Postgres(pg) => pg.validate(),
            SourceConfig::Salesforce(sf) => sf.validate(),
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
            DestinationConfig::BigQuery(_) => Ok(()),
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
    pub schema_changes: Option<SchemaChangePolicy>,
    pub cdc_pipeline_id: Option<u64>,
    pub cdc_batch_size: Option<usize>,
    pub cdc_max_fill_ms: Option<u64>,
    pub cdc_max_pending_events: Option<usize>,
    pub cdc_idle_timeout_seconds: Option<u64>,
    pub cdc_tls: Option<bool>,
    pub cdc_tls_ca_path: Option<PathBuf>,
    pub cdc_tls_ca: Option<String>,
}

impl PostgresConfig {
    pub fn schema_policy(&self) -> SchemaChangePolicy {
        self.schema_changes
            .clone()
            .unwrap_or(SchemaChangePolicy::Auto)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let has_tables = self.tables.as_ref().map(|t| !t.is_empty()).unwrap_or(false);
        let has_selection = self
            .table_selection
            .as_ref()
            .map(|s| !(s.include.is_empty() && s.exclude.is_empty()))
            .unwrap_or(false);
        if !has_tables && !has_selection {
            anyhow::bail!("postgres requires tables or table_selection.include/exclude");
        }
        if self.cdc.unwrap_or(true) {
            let publication = self.publication.as_deref().unwrap_or("");
            if publication.is_empty() {
                anyhow::bail!("postgres.publication is required when CDC is enabled");
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
pub struct SalesforceConfig {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    pub login_url: Option<String>,
    pub instance_url: Option<String>,
    pub api_version: Option<String>,
    pub objects: Option<Vec<SalesforceObjectConfig>>,
    pub object_selection: Option<ObjectSelectionConfig>,
    pub polling_interval_seconds: Option<u64>,
    pub rate_limit: Option<SalesforceRateLimitConfig>,
}

impl SalesforceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let has_objects = self
            .objects
            .as_ref()
            .map(|o| !o.is_empty())
            .unwrap_or(false);
        let has_selection = self
            .object_selection
            .as_ref()
            .map(|s| !(s.include.is_empty() && s.exclude.is_empty()))
            .unwrap_or(false);
        if !has_objects && !has_selection {
            anyhow::bail!("salesforce requires objects or object_selection.include/exclude");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSelectionConfig {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    pub defaults: Option<SalesforceObjectDefaults>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesforceObjectDefaults {
    pub primary_key: Option<String>,
    pub soft_delete: Option<bool>,
    pub fields: Option<FieldSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesforceObjectConfig {
    pub name: String,
    pub primary_key: Option<String>,
    pub fields: Option<FieldSelection>,
    pub soft_delete: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SalesforceRateLimitConfig {
    pub max_retries: Option<u32>,
    pub backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangePolicy {
    Auto,
    Fail,
    Resync,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldSelection {
    List(Vec<String>),
    Filtered {
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        exclude: Vec<String>,
    },
}

impl FieldSelection {
    pub fn include_list(&self) -> Option<Vec<String>> {
        match self {
            FieldSelection::List(list) => Some(list.clone()),
            FieldSelection::Filtered { include, .. } if !include.is_empty() => {
                Some(include.clone())
            }
            _ => None,
        }
    }

    pub fn exclude_list(&self) -> Vec<String> {
        match self {
            FieldSelection::Filtered { exclude, .. } => exclude.clone(),
            _ => Vec::new(),
        }
    }
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
    pub storage_write_enabled: Option<bool>,
    pub emulator_http: Option<String>,
    pub emulator_grpc: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TemplateConfig {
    pub path: PathBuf,
    pub content: String,
}

const DEFAULT_TEMPLATE: &str = r#"# CDSync configuration (YAML)

state:
  path: "./cdsync_state.db"

logging:
  level: "info"
  json: false

observability:
  service_name: "cdsync"
  # otlp_traces_endpoint: "http://localhost:4318/v1/traces"
  # otlp_metrics_endpoint: "http://localhost:4318/v1/metrics"
  metrics_interval_seconds: 30

sync:
  default_batch_size: 10000
  max_retries: 5
  retry_backoff_ms: 1000
  max_concurrency: 4

stats:
  path: "./cdsync_stats.db"

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
      schema_changes: auto
      cdc_pipeline_id: 1
      cdc_batch_size: 10000
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
      storage_write_enabled: true
      # emulator_http: "http://localhost:9050"
      # emulator_grpc: "localhost:9051"

  - id: "salesforce"
    enabled: false
    source:
      type: salesforce
      client_id: "YOUR_CLIENT_ID"
      client_secret: "YOUR_CLIENT_SECRET"
      refresh_token: "YOUR_REFRESH_TOKEN"
      login_url: "https://login.salesforce.com"
      api_version: "v59.0"
      rate_limit:
        max_retries: 5
        backoff_ms: 1000
        max_backoff_ms: 30000
      object_selection:
        include: ["Account"]
        exclude: []
      objects:
        - name: "Account"
          primary_key: "Id"
          fields:
            include: ["Id", "Name"]
            exclude: []
    destination:
      type: bigquery
      project_id: "your-project"
      dataset: "cdsync"
      service_account_key_path: "/path/to/service-account.json"
      partition_by_synced_at: true
      storage_write_enabled: true
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
  path: "./state.json"
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
"#;
        let cfg: Config = serde_yaml::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_allows_cdc_disabled_without_publication() {
        let raw = r#"
state:
  path: "./state.json"
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
        let cfg: Config = serde_yaml::from_str(raw).expect("config parses");
        assert!(cfg.validate().is_ok());
    }
}
