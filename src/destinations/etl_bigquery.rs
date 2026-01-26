use crate::destinations::bigquery::BigQueryDestination;
use crate::destinations::{Destination as CdsDestination, WriteMode};
use crate::stats::StatsHandle;
use crate::types::{DataType, TableSchema, META_DELETED_AT, META_SYNCED_AT};
use anyhow::Result;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use chrono::{DateTime, NaiveTime, TimeZone, Utc};
use etl::destination::Destination as EtlDestination;
use etl::error::{EtlError, EtlResult, ErrorKind};
use etl::types::{Cell, Event, TableId, TableRow};
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::warn;

#[derive(Clone)]
pub struct EtlBigQueryDestination {
    inner: BigQueryDestination,
    tables: Arc<RwLock<HashMap<TableId, CdcTableInfo>>>,
    stats: Option<StatsHandle>,
}

#[derive(Clone)]
pub struct CdcTableInfo {
    pub table_id: TableId,
    pub source_name: String,
    pub dest_name: String,
    pub schema: TableSchema,
    pub primary_key: String,
    pub soft_delete: bool,
    dest_source_indices: Vec<usize>,
    source_soft_delete_index: Option<usize>,
}

impl CdcTableInfo {
    pub fn new(
        table_id: TableId,
        source_name: String,
        dest_name: String,
        schema: TableSchema,
        primary_key: String,
        soft_delete: bool,
        soft_delete_column: Option<String>,
        etl_schema: &etl::types::TableSchema,
    ) -> Result<Self> {
        let source_index_by_name: HashMap<String, usize> = etl_schema
            .column_schemas
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.clone(), idx))
            .collect();

        let mut dest_source_indices = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let idx = source_index_by_name
                .get(&column.name)
                .copied()
                .ok_or_else(|| anyhow::anyhow!("column {} not found in source schema", column.name))?;
            dest_source_indices.push(idx);
        }

        if !source_index_by_name.contains_key(&primary_key) {
            return Err(anyhow::anyhow!(
                "primary key {} not found in source schema",
                primary_key
            ));
        }

        let source_soft_delete_index = soft_delete_column
            .as_ref()
            .and_then(|name| source_index_by_name.get(name).copied());

        Ok(Self {
            table_id,
            source_name,
            dest_name,
            schema,
            primary_key,
            soft_delete,
            dest_source_indices,
            source_soft_delete_index,
        })
    }
}

impl EtlBigQueryDestination {
    pub fn new(
        inner: BigQueryDestination,
        tables: HashMap<TableId, CdcTableInfo>,
        stats: Option<StatsHandle>,
    ) -> Self {
        Self {
            inner,
            tables: Arc::new(RwLock::new(tables)),
            stats,
        }
    }

    async fn get_table(&self, table_id: TableId) -> EtlResult<CdcTableInfo> {
        let guard = self.tables.read().await;
        guard
            .get(&table_id)
            .cloned()
            .ok_or_else(|| etl::etl_error!(ErrorKind::MissingTableSchema, "table not configured"))
    }

    pub async fn update_table_info(&self, info: CdcTableInfo) -> EtlResult<()> {
        let mut guard = self.tables.write().await;
        guard.insert(info.table_id, info);
        Ok(())
    }

    async fn ensure_table(&self, info: &CdcTableInfo) -> EtlResult<()> {
        self.ensure_table_schema(&info.schema).await
    }

    pub async fn ensure_table_schema(&self, schema: &TableSchema) -> EtlResult<()> {
        self.inner
            .ensure_table(schema)
            .await
            .map_err(|err| {
                etl::etl_error!(
                    ErrorKind::DestinationError,
                    "failed to ensure BigQuery table",
                    err.to_string()
                )
            })
    }

    async fn write_rows(
        &self,
        info: &CdcTableInfo,
        rows: &[TableRow],
        mode: WriteMode,
        synced_at: DateTime<Utc>,
        deleted_at_override: Option<DateTime<Utc>>,
    ) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.ensure_table(info).await?;
        let frame =
            table_rows_to_frame(info, rows, synced_at, deleted_at_override).map_err(|err| {
                etl::etl_error!(ErrorKind::ConversionError, "failed to build CDC batch", err.to_string())
            })?;
        let load_start = Instant::now();
        self.inner
            .write_batch(
                &info.dest_name,
                &info.schema,
                &frame,
                mode,
                Some(info.primary_key.as_str()),
            )
            .await
            .map_err(|err| {
                etl::etl_error!(ErrorKind::DestinationError, "failed to write CDC batch", err.to_string())
            })?;
        if let Some(stats) = &self.stats {
            let deleted = if deleted_at_override.is_some() {
                rows.len()
            } else {
                0
            };
            let upserted = if matches!(mode, WriteMode::Upsert) {
                rows.len()
            } else {
                0
            };
            stats
                .record_load(
                    &info.source_name,
                    rows.len(),
                    upserted,
                    deleted,
                    load_start.elapsed().as_millis() as u64,
                )
                .await;
        }
        Ok(())
    }

    async fn flush_pending(
        &self,
        pending: &mut HashMap<TableId, Vec<TableRow>>,
        pending_deletes: &mut HashMap<TableId, Vec<TableRow>>,
        truncate_tables: &mut Vec<TableId>,
    ) -> EtlResult<()> {
        let synced_at = Utc::now();
        let delete_synced_at = synced_at;

        for table_id in truncate_tables.drain(..) {
            if let Ok(info) = self.get_table(table_id).await {
                if let Err(err) = self.inner.truncate_table(&info.dest_name).await {
                    return Err(etl::etl_error!(
                        ErrorKind::DestinationError,
                        "failed to truncate table from CDC event",
                        err.to_string()
                    ));
                }
            }
        }

        for (table_id, rows) in pending.drain() {
            let info = self.get_table(table_id).await?;
            self.write_rows(&info, &rows, WriteMode::Upsert, synced_at, None)
                .await?;
        }

        for (table_id, rows) in pending_deletes.drain() {
            let info = self.get_table(table_id).await?;
            self.write_rows(
                &info,
                &rows,
                WriteMode::Upsert,
                delete_synced_at,
                Some(delete_synced_at),
            )
            .await?;
        }

        Ok(())
    }
}

impl EtlDestination for EtlBigQueryDestination {
    fn name() -> &'static str {
        "cdsync_bigquery"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let info = self.get_table(table_id).await?;
        self.inner
            .truncate_table(&info.dest_name)
            .await
            .map_err(|err| {
                etl::etl_error!(ErrorKind::DestinationError, "failed to truncate table", err.to_string())
            })
    }

    async fn write_table_rows(&self, table_id: TableId, table_rows: Vec<TableRow>) -> EtlResult<()> {
        let info = self.get_table(table_id).await?;
        let synced_at = Utc::now();
        self.write_rows(&info, &table_rows, WriteMode::Append, synced_at, None)
            .await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut pending: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut pending_deletes: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut truncate_tables: Vec<TableId> = Vec::new();

        for event in events {
            match event {
                Event::Insert(insert_event) => {
                    if let Ok(info) = self.get_table(insert_event.table_id).await {
                        pending
                            .entry(info.table_id)
                            .or_default()
                            .push(insert_event.table_row);
                    }
                }
                Event::Update(update_event) => {
                    if let Ok(info) = self.get_table(update_event.table_id).await {
                        pending
                            .entry(info.table_id)
                            .or_default()
                            .push(update_event.table_row);
                    }
                }
                Event::Delete(delete_event) => {
                    if let Ok(info) = self.get_table(delete_event.table_id).await {
                        if !info.soft_delete {
                            continue;
                        }
                        if let Some((_, old_row)) = &delete_event.old_table_row {
                            pending_deletes
                                .entry(info.table_id)
                                .or_default()
                                .push(old_row.clone());
                        } else {
                            warn!(
                                table = %info.source_name,
                                "skipping delete event without primary key"
                            );
                        }
                    }
                }
                Event::Truncate(truncate_event) => {
                    for rel_id in truncate_event.rel_ids {
                        truncate_tables.push(TableId::new(rel_id));
                    }
                }
                Event::Commit(_) => {
                    self.flush_pending(
                        &mut pending,
                        &mut pending_deletes,
                        &mut truncate_tables,
                    )
                    .await?;
                }
                Event::Begin(_) | Event::Relation(_) | Event::Unsupported => {}
            }
        }

        if !pending.is_empty() || !pending_deletes.is_empty() || !truncate_tables.is_empty() {
            self.flush_pending(&mut pending, &mut pending_deletes, &mut truncate_tables)
                .await?;
        }

        Ok(())
    }
}

fn table_rows_to_frame(
    info: &CdcTableInfo,
    rows: &[TableRow],
    synced_at: DateTime<Utc>,
    deleted_at_override: Option<DateTime<Utc>>,
) -> Result<DataFrame, EtlError> {
    let polars_schema = polars_schema_with_metadata(&info.schema)?;
    let mut output: Vec<PolarsRow> = Vec::with_capacity(rows.len());
    for row in rows {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for source_idx in &info.dest_source_indices {
            let cell = row.values.get(*source_idx).unwrap_or(&Cell::Null);
            values.push(cell_to_anyvalue(cell));
        }

        values.push(AnyValue::StringOwned(PlSmallStr::from(synced_at.to_rfc3339())));

        let deleted_at = if info.soft_delete {
            if let Some(ts) = deleted_at_override {
                Some(ts.to_rfc3339())
            } else {
                derive_deleted_at_from_row(info, row)
            }
        } else {
            None
        };

        match deleted_at {
            Some(ts) => values.push(AnyValue::StringOwned(PlSmallStr::from(ts))),
            None => values.push(AnyValue::Null),
        }

        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(|err| {
        etl::etl_error!(ErrorKind::ConversionError, "failed to build CDC dataframe", err.to_string())
    })
}

fn derive_deleted_at_from_row(info: &CdcTableInfo, row: &TableRow) -> Option<String> {
    let idx = info.source_soft_delete_index?;
    let cell = row.values.get(idx)?;
    match cell {
        Cell::Null => None,
        Cell::Bool(value) => {
            if *value {
                Some(Utc::now().to_rfc3339())
            } else {
                None
            }
        }
        Cell::Timestamp(value) => Some(Utc.from_utc_datetime(value).to_rfc3339()),
        Cell::TimestampTz(value) => Some(value.to_rfc3339()),
        Cell::Date(value) => Some(value.format("%Y-%m-%d").to_string()),
        Cell::String(value) => Some(value.clone()),
        _ => None,
    }
}

fn cell_to_anyvalue(cell: &Cell) -> AnyValue<'static> {
    match cell {
        Cell::Null => AnyValue::Null,
        Cell::Bool(value) => AnyValue::Boolean(*value),
        Cell::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
        Cell::I16(value) => AnyValue::Int64(*value as i64),
        Cell::I32(value) => AnyValue::Int64(*value as i64),
        Cell::U32(value) => AnyValue::Int64(*value as i64),
        Cell::I64(value) => AnyValue::Int64(*value),
        Cell::F32(value) => AnyValue::Float64(*value as f64),
        Cell::F64(value) => AnyValue::Float64(*value),
        Cell::Numeric(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Date(value) => AnyValue::StringOwned(PlSmallStr::from(value.format("%Y-%m-%d").to_string())),
        Cell::Time(value) => AnyValue::StringOwned(PlSmallStr::from(format_time(value))),
        Cell::Timestamp(value) => AnyValue::StringOwned(PlSmallStr::from(Utc.from_utc_datetime(value).to_rfc3339())),
        Cell::TimestampTz(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_rfc3339())),
        Cell::Uuid(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Json(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        Cell::Bytes(value) => AnyValue::StringOwned(PlSmallStr::from(encode_base64(value))),
        Cell::Array(array) => {
            let json = serde_json::to_string(&array_to_json(array)).unwrap_or_default();
            AnyValue::StringOwned(PlSmallStr::from(json))
        }
    }
}

fn array_to_json(array: &etl::types::ArrayCell) -> serde_json::Value {
    match array {
        etl::types::ArrayCell::Bool(values) => serde_json::Value::Array(values.iter().map(|v| v.map(serde_json::Value::Bool).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::String(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|s| serde_json::Value::String(s.clone())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::I16(values) => serde_json::Value::Array(values.iter().map(|v| v.map(|n| serde_json::Value::Number((n as i64).into())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::I32(values) => serde_json::Value::Array(values.iter().map(|v| v.map(|n| serde_json::Value::Number((n as i64).into())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::U32(values) => serde_json::Value::Array(values.iter().map(|v| v.map(|n| serde_json::Value::Number((n as u64).into())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::I64(values) => serde_json::Value::Array(values.iter().map(|v| v.map(|n| serde_json::Value::Number(n.into())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::F32(values) => serde_json::Value::Array(values.iter().map(|v| v.and_then(|n| serde_json::Number::from_f64(n as f64).map(serde_json::Value::Number)).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::F64(values) => serde_json::Value::Array(values.iter().map(|v| v.and_then(|n| serde_json::Number::from_f64(n).map(serde_json::Value::Number)).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Numeric(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|n| serde_json::Value::String(n.to_string())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Date(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Time(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|t| serde_json::Value::String(format_time(t))).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Timestamp(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|t| serde_json::Value::String(Utc.from_utc_datetime(t).to_rfc3339())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::TimestampTz(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|t| serde_json::Value::String(t.to_rfc3339())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Uuid(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|u| serde_json::Value::String(u.to_string())).unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Json(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().cloned().unwrap_or(serde_json::Value::Null)).collect()),
        etl::types::ArrayCell::Bytes(values) => serde_json::Value::Array(values.iter().map(|v| v.as_ref().map(|b| serde_json::Value::String(encode_base64(b))).unwrap_or(serde_json::Value::Null)).collect()),
    }
}

fn polars_schema_with_metadata(schema: &TableSchema) -> EtlResult<Schema> {
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
    Ok(Schema::from_iter(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType, TableSchema};
    use chrono::{DateTime, TimeZone, Utc};
    use etl::types::{Cell, ColumnSchema as EtlColumnSchema, TableId, TableName, TableRow, Type};

    fn build_info() -> CdcTableInfo {
        let table_id = TableId::new(1);
        let table_name = TableName::new("public".to_string(), "items".to_string());
        let etl_schema = etl::types::TableSchema::new(
            table_id,
            table_name,
            vec![
                EtlColumnSchema::new("id".to_string(), Type::INT8, -1, false, true),
                EtlColumnSchema::new(
                    "deleted_at".to_string(),
                    Type::TIMESTAMPTZ,
                    -1,
                    true,
                    false,
                ),
            ],
        );

        let schema = TableSchema {
            name: "public__items".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "deleted_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };

        CdcTableInfo::new(
            table_id,
            "public.items".to_string(),
            "public__items".to_string(),
            schema,
            "id".to_string(),
            true,
            Some("deleted_at".to_string()),
            &etl_schema,
        )
        .expect("cdc table info")
    }

    #[test]
    fn derive_deleted_at_handles_timestamptz() {
        let info = build_info();
        let ts = Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap();
        let row = TableRow::new(vec![Cell::I64(1), Cell::TimestampTz(ts)]);
        let value = derive_deleted_at_from_row(&info, &row).expect("deleted_at");
        assert_eq!(value, ts.to_rfc3339());
    }

    #[test]
    fn derive_deleted_at_handles_bool_and_null() {
        let info = build_info();
        let row = TableRow::new(vec![Cell::I64(1), Cell::Bool(true)]);
        let value = derive_deleted_at_from_row(&info, &row).expect("deleted_at");
        assert!(DateTime::parse_from_rfc3339(&value).is_ok());

        let row = TableRow::new(vec![Cell::I64(2), Cell::Bool(false)]);
        assert!(derive_deleted_at_from_row(&info, &row).is_none());

        let row = TableRow::new(vec![Cell::I64(3), Cell::Null]);
        assert!(derive_deleted_at_from_row(&info, &row).is_none());
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

fn format_time(value: &NaiveTime) -> String {
    value.format("%H:%M:%S%.f").to_string()
}
