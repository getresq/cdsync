use chrono::{DateTime, Utc};
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

pub const META_SYNCED_AT: &str = "_cdsync_synced_at";
pub const META_DELETED_AT: &str = "_cdsync_deleted_at";

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RowBatch {
    pub table: String,
    pub schema: TableSchema,
    pub frame: DataFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableCheckpoint {
    pub last_synced_at: Option<DateTime<Utc>>,
    pub last_primary_key: Option<String>,
    pub last_lsn: Option<String>,
    pub schema_hash: Option<String>,
    pub schema_snapshot: Option<Vec<SchemaFieldSnapshot>>,
}
