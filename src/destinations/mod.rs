use async_trait::async_trait;

use crate::types::{MetadataColumns, SourceChangeBatch, TableSchema};
use polars::prelude::DataFrame;

pub mod bigquery;
pub mod etl_bigquery;

#[derive(Debug, Clone, Copy)]
pub enum WriteMode {
    Append,
    Upsert,
}

#[async_trait]
pub trait Destination: Send + Sync {
    async fn ensure_table(&self, schema: &TableSchema) -> anyhow::Result<()>;
    async fn truncate_table(&self, table: &str) -> anyhow::Result<()>;
    async fn write_batch(
        &self,
        table: &str,
        schema: &TableSchema,
        frame: &DataFrame,
        mode: WriteMode,
        primary_key: Option<&str>,
    ) -> anyhow::Result<()>;

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait ChangeApplier: Send + Sync {
    async fn apply_change_batch(&self, batch: &SourceChangeBatch) -> anyhow::Result<()>;
}

pub fn with_metadata_schema(schema: &TableSchema, metadata: &MetadataColumns) -> TableSchema {
    let mut columns = schema.columns.clone();
    if !columns.iter().any(|c| c.name == metadata.synced_at) {
        columns.push(crate::types::ColumnSchema {
            name: metadata.synced_at.clone(),
            data_type: crate::types::DataType::Timestamp,
            nullable: false,
        });
    }
    if !columns.iter().any(|c| c.name == metadata.deleted_at) {
        columns.push(crate::types::ColumnSchema {
            name: metadata.deleted_at.clone(),
            data_type: crate::types::DataType::Timestamp,
            nullable: true,
        });
    }
    TableSchema {
        name: schema.name.clone(),
        columns,
        primary_key: schema.primary_key.clone(),
    }
}
