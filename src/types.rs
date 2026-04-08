use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const META_SYNCED_AT: &str = "_cdsync_synced_at";
pub const META_DELETED_AT: &str = "_cdsync_deleted_at";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataColumns {
    pub synced_at: String,
    pub deleted_at: String,
}

impl Default for MetadataColumns {
    fn default() -> Self {
        Self {
            synced_at: META_SYNCED_AT.to_string(),
            deleted_at: META_DELETED_AT.to_string(),
        }
    }
}

pub fn destination_table_name(source_name: &str) -> String {
    source_name.replace('.', "__")
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Int64,
    Float64,
    Bool,
    Timestamp,
    Date,
    Interval,
    Bytes,
    Numeric,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaFieldSnapshot {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotChunkCheckpoint {
    pub start_primary_key: Option<String>,
    pub end_primary_key: Option<String>,
    pub last_primary_key: Option<String>,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TableRuntimeStatus {
    Retrying,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableRuntimeState {
    pub status: TableRuntimeStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableCheckpoint {
    pub last_synced_at: Option<DateTime<Utc>>,
    pub last_primary_key: Option<String>,
    pub last_lsn: Option<String>,
    pub schema_hash: Option<String>,
    pub schema_snapshot: Option<Vec<SchemaFieldSnapshot>>,
    pub schema_primary_key: Option<String>,
    pub snapshot_start_lsn: Option<String>,
    #[serde(default)]
    pub snapshot_preserve_backlog: bool,
    #[serde(default)]
    pub snapshot_chunks: Vec<SnapshotChunkCheckpoint>,
    #[serde(default)]
    pub runtime: Option<TableRuntimeState>,
}
