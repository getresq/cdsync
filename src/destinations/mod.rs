use async_trait::async_trait;

use crate::types::TableSchema;
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
    #[allow(dead_code)]
    async fn finalize_table(&self, _table: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

pub fn with_metadata_schema(schema: &TableSchema) -> TableSchema {
    let mut columns = schema.columns.clone();
    if !columns
        .iter()
        .any(|c| c.name == crate::types::META_SYNCED_AT)
    {
        columns.push(crate::types::ColumnSchema {
            name: crate::types::META_SYNCED_AT.to_string(),
            data_type: crate::types::DataType::Timestamp,
            nullable: false,
        });
    }
    if !columns
        .iter()
        .any(|c| c.name == crate::types::META_DELETED_AT)
    {
        columns.push(crate::types::ColumnSchema {
            name: crate::types::META_DELETED_AT.to_string(),
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
