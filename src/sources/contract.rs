use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use polars::frame::row::Row as PolarsRow;
use polars::prelude::{AnyValue, DataFrame, DataType as PolarsDataType, Field, PlSmallStr, Schema};
use serde_json::Value;

use crate::types::{DataType, MetadataColumns, SourceRecord, TableSchema};

pub(crate) fn records_to_dataframe(
    schema: &TableSchema,
    records: &[SourceRecord],
    metadata: &MetadataColumns,
) -> Result<DataFrame> {
    let polars_schema = polars_schema_with_metadata(schema, metadata);
    let mut output: Vec<PolarsRow> = Vec::with_capacity(records.len());

    for record in records {
        let mut values: Vec<AnyValue> = Vec::with_capacity(polars_schema.len());
        for column in &schema.columns {
            let value = record.values.get(&column.name).unwrap_or(&Value::Null);
            values.push(json_value_to_anyvalue(value, &column.data_type));
        }
        values.push(AnyValue::StringOwned(PlSmallStr::from(
            record.synced_at.to_rfc3339(),
        )));
        values.push(match record.deleted_at {
            Some(deleted_at) => AnyValue::StringOwned(PlSmallStr::from(deleted_at.to_rfc3339())),
            None => AnyValue::Null,
        });
        output.push(PolarsRow::new(values));
    }

    DataFrame::from_rows_and_schema(&output, &polars_schema).map_err(Into::into)
}

fn polars_schema_with_metadata(schema: &TableSchema, metadata: &MetadataColumns) -> Schema {
    let mut fields: Vec<Field> = Vec::with_capacity(schema.columns.len() + 2);
    for column in &schema.columns {
        let dtype = match column.data_type {
            DataType::Int64 => PolarsDataType::Int64,
            DataType::Float64 | DataType::Interval => PolarsDataType::Float64,
            DataType::Numeric => PolarsDataType::String,
            DataType::Bool => PolarsDataType::Boolean,
            _ => PolarsDataType::String,
        };
        fields.push(Field::new(column.name.as_str().into(), dtype));
    }
    fields.push(Field::new(
        metadata.synced_at.as_str().into(),
        PolarsDataType::String,
    ));
    fields.push(Field::new(
        metadata.deleted_at.as_str().into(),
        PolarsDataType::String,
    ));
    Schema::from_iter(fields)
}

fn json_value_to_anyvalue(value: &Value, data_type: &DataType) -> AnyValue<'static> {
    match data_type {
        DataType::String => match value {
            Value::Null => AnyValue::Null,
            Value::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
            Value::Bool(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
            Value::Number(value) => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
            _ => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        },
        DataType::Int64 => match value.as_i64() {
            Some(value) => AnyValue::Int64(value),
            None => value
                .as_str()
                .and_then(|raw| raw.parse::<i64>().ok())
                .map_or(AnyValue::Null, AnyValue::Int64),
        },
        DataType::Float64 | DataType::Interval => match value.as_f64() {
            Some(value) => AnyValue::Float64(value),
            None => value
                .as_str()
                .and_then(|raw| raw.parse::<f64>().ok())
                .map_or(AnyValue::Null, AnyValue::Float64),
        },
        DataType::Bool => match value {
            Value::Bool(value) => AnyValue::Boolean(*value),
            Value::String(value) => value
                .parse::<bool>()
                .map_or(AnyValue::Null, AnyValue::Boolean),
            _ => AnyValue::Null,
        },
        DataType::Timestamp | DataType::Date | DataType::Numeric | DataType::Json => match value {
            Value::Null => AnyValue::Null,
            Value::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
            _ => AnyValue::StringOwned(PlSmallStr::from(value.to_string())),
        },
        DataType::Bytes => match value {
            Value::Null => AnyValue::Null,
            Value::String(value) => AnyValue::StringOwned(PlSmallStr::from(value.as_str())),
            Value::Array(parts) => {
                let bytes = parts
                    .iter()
                    .filter_map(|part| part.as_u64().and_then(|value| u8::try_from(value).ok()))
                    .collect::<Vec<_>>();
                AnyValue::StringOwned(PlSmallStr::from(STANDARD.encode(bytes)))
            }
            _ => AnyValue::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, META_DELETED_AT, META_SYNCED_AT};
    use chrono::TimeZone;
    use std::collections::BTreeMap;

    #[test]
    fn records_to_dataframe_keeps_declared_and_metadata_columns() -> anyhow::Result<()> {
        let schema = TableSchema {
            name: "example".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::String,
                    nullable: false,
                },
                ColumnSchema {
                    name: "count".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };
        let synced_at = chrono::Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 0, 0)
            .single()
            .expect("valid timestamp");
        let deleted_at = chrono::Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 5, 0)
            .single()
            .expect("valid timestamp");
        let record = SourceRecord {
            values: BTreeMap::from([
                ("id".to_string(), Value::String("abc".to_string())),
                ("count".to_string(), Value::Number(42.into())),
            ]),
            synced_at,
            deleted_at: Some(deleted_at),
        };

        let frame = records_to_dataframe(&schema, &[record], &MetadataColumns::default())?;

        assert_eq!(frame.height(), 1);
        assert_eq!(frame.width(), 4);
        assert_eq!(
            frame.get_column_names(),
            ["id", "count", META_SYNCED_AT, META_DELETED_AT]
        );
        Ok(())
    }

    #[test]
    fn records_to_dataframe_supports_float64_columns() -> anyhow::Result<()> {
        let schema = TableSchema {
            name: "example".to_string(),
            columns: vec![ColumnSchema {
                name: "score".to_string(),
                data_type: DataType::Float64,
                nullable: true,
            }],
            primary_key: None,
        };
        let record = SourceRecord {
            values: BTreeMap::from([("score".to_string(), Value::String("1.5".to_string()))]),
            synced_at: chrono::Utc
                .with_ymd_and_hms(2026, 4, 15, 12, 0, 0)
                .single()
                .expect("valid timestamp"),
            deleted_at: None,
        };

        let frame = records_to_dataframe(&schema, &[record], &MetadataColumns::default())?;
        let score = frame.column("score")?.f64()?.get(0);

        assert_eq!(score, Some(1.5));
        Ok(())
    }
}
