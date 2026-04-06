use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, NaiveDate, Utc};
use gcloud_bigquery::http::table::{TableFieldSchema, TableFieldType};
use gcloud_bigquery::http::tabledata::list::{Tuple as BqTuple, Value as BqValue};
use polars::frame::DataFrame;
use polars::frame::row::Row as PolarsRow;
use polars::prelude::AnyValue;
use serde_json::{Map, Value};

use crate::types::{ColumnSchema, DataType};

pub(super) fn bq_fields_from_schema(columns: &[ColumnSchema]) -> Vec<TableFieldSchema> {
    columns
        .iter()
        .map(|col| {
            let data_type = match col.data_type {
                DataType::String | DataType::Bytes | DataType::Json => TableFieldType::String,
                DataType::Int64 => TableFieldType::Int64,
                DataType::Float64 | DataType::Interval => TableFieldType::Float64,
                DataType::Bool => TableFieldType::Bool,
                DataType::Timestamp => TableFieldType::Timestamp,
                DataType::Date => TableFieldType::Date,
                DataType::Numeric => TableFieldType::Bignumeric,
            };
            TableFieldSchema {
                name: col.name.clone(),
                data_type,
                // Replication batches may legitimately contain NULLs for fields that are
                // declared NOT NULL at the source, for example during CDC updates that omit
                // unchanged toasted columns. Keeping destination fields nullable avoids
                // load-job failures while preserving data values and metadata.
                mode: Some(gcloud_bigquery::http::table::TableFieldMode::Nullable),
                ..Default::default()
            }
        })
        .collect()
}

pub(super) fn bq_ident(name: &str) -> String {
    let escaped = name.replace('`', "\\`");
    format!("`{}`", escaped)
}

pub(super) fn dataframe_to_json_rows(frame: &DataFrame) -> Result<Vec<Map<String, Value>>> {
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

pub(super) fn anyvalue_to_json(value: &AnyValue) -> Value {
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int64(v) => Value::Number((*v).into()),
        AnyValue::Int32(v) => Value::Number(i64::from(*v).into()),
        AnyValue::UInt64(v) => Value::Number((*v).into()),
        AnyValue::UInt32(v) => Value::Number(u64::from(*v).into()),
        AnyValue::Float64(v) => serde_json::Number::from_f64(*v).map_or(Value::Null, Value::Number),
        AnyValue::Float32(v) => serde_json::Number::from_f64(f64::from(*v)).map_or(Value::Null, Value::Number),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::StringOwned(v) => Value::String(v.to_string()),
        AnyValue::Binary(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::BinaryOwned(bytes) => Value::String(encode_base64(bytes)),
        AnyValue::Datetime(ts, unit, _) | AnyValue::DatetimeOwned(ts, unit, _) => {
            Value::String(datetime_to_rfc3339(*ts, *unit))
        }
        AnyValue::Date(days) => Value::String(date_to_string(*days)),
        AnyValue::Decimal(v, _) => Value::String(v.to_string()),
        other => Value::String(other.to_string()),
    }
}

pub(super) fn datetime_to_rfc3339(ts: i64, unit: polars::prelude::TimeUnit) -> String {
    let nanos = match unit {
        polars::prelude::TimeUnit::Nanoseconds => ts,
        polars::prelude::TimeUnit::Microseconds => ts * 1_000,
        polars::prelude::TimeUnit::Milliseconds => ts * 1_000_000,
    };
    let seconds = nanos / 1_000_000_000;
    let nanos_part = u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0);
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, nanos_part)
        .unwrap_or_else(chrono::Utc::now);
    dt.to_rfc3339()
}

pub(super) fn date_to_string(days: i32) -> String {
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date"));
    let date = epoch + chrono::Duration::days(days as i64);
    date.format("%Y-%m-%d").to_string()
}

pub(super) fn encode_base64(bytes: &[u8]) -> String {
    STANDARD.encode(bytes)
}

pub(super) fn default_port(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}

pub(super) fn tuple_value_as_i64(tuple: &BqTuple, index: usize) -> Result<i64> {
    match tuple.f.get(index).map(|cell| &cell.v) {
        Some(BqValue::String(value)) => value
            .parse::<i64>()
            .with_context(|| format!("parsing bigint cell at index {}", index)),
        Some(BqValue::Null) | None => Ok(0),
        other => anyhow::bail!("unexpected bigint cell at index {}: {:?}", index, other),
    }
}

pub(super) fn tuple_value_as_datetime(
    tuple: &BqTuple,
    index: usize,
) -> Result<Option<DateTime<Utc>>> {
    match tuple.f.get(index).map(|cell| &cell.v) {
        Some(BqValue::String(value)) if !value.is_empty() => {
            if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
                return Ok(Some(parsed.with_timezone(&Utc)));
            }
            let (whole_seconds, nanos) =
                parse_epoch_seconds(value).with_context(|| format!("parsing datetime cell at index {}", index))?;
            Ok(DateTime::<Utc>::from_timestamp(whole_seconds, nanos))
        }
        None | Some(BqValue::Null | BqValue::String(_)) => Ok(None),
        other => anyhow::bail!("unexpected datetime cell at index {}: {:?}", index, other),
    }
}

pub(super) fn anyvalue_to_owned_string(value: &AnyValue) -> String {
    match value {
        AnyValue::String(value) => value.to_string(),
        AnyValue::StringOwned(value) => value.to_string(),
        AnyValue::Date(value) => date_to_string(*value),
        AnyValue::Datetime(ts, unit, _) => datetime_to_rfc3339(*ts, *unit),
        AnyValue::DatetimeOwned(ts, unit, _) => datetime_to_rfc3339(*ts, *unit),
        AnyValue::Binary(bytes) => encode_base64(bytes),
        AnyValue::BinaryOwned(bytes) => encode_base64(bytes),
        AnyValue::Boolean(value) => value.to_string(),
        AnyValue::Int64(value) => value.to_string(),
        AnyValue::Int32(value) => value.to_string(),
        AnyValue::UInt64(value) => value.to_string(),
        AnyValue::UInt32(value) => value.to_string(),
        AnyValue::Float64(value) => value.to_string(),
        AnyValue::Float32(value) => value.to_string(),
        AnyValue::Decimal(value, _) => value.to_string(),
        other => other.to_string(),
    }
}

pub(super) fn anyvalue_to_i64(value: &AnyValue) -> Result<i64> {
    match value {
        AnyValue::Int64(value) => Ok(*value),
        AnyValue::Int32(value) => Ok(i64::from(*value)),
        AnyValue::UInt64(value) => i64::try_from(*value).context("uint64 value outside int64 range"),
        AnyValue::UInt32(value) => Ok(i64::from(*value)),
        AnyValue::String(value) => value
            .parse::<i64>()
            .with_context(|| format!("parsing int64 value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<i64>()
            .with_context(|| format!("parsing int64 value {}", value)),
        other => anyhow::bail!("unsupported int64 value {:?}", other),
    }
}

pub(super) fn anyvalue_to_f64(value: &AnyValue) -> Result<f64> {
    match value {
        AnyValue::Float64(value) => Ok(*value),
        AnyValue::Float32(value) => Ok(f64::from(*value)),
        AnyValue::Int64(value) => Ok(i64_to_f64(*value)),
        AnyValue::Int32(value) => Ok(f64::from(*value)),
        AnyValue::String(value) => value
            .parse::<f64>()
            .with_context(|| format!("parsing float value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<f64>()
            .with_context(|| format!("parsing float value {}", value)),
        other => anyhow::bail!("unsupported float value {:?}", other),
    }
}

pub(super) fn anyvalue_to_bool(value: &AnyValue) -> Result<bool> {
    match value {
        AnyValue::Boolean(value) => Ok(*value),
        AnyValue::String(value) => value
            .parse::<bool>()
            .with_context(|| format!("parsing bool value {}", value)),
        AnyValue::StringOwned(value) => value
            .to_string()
            .parse::<bool>()
            .with_context(|| format!("parsing bool value {}", value)),
        other => anyhow::bail!("unsupported bool value {:?}", other),
    }
}

pub(super) fn anyvalue_to_timestamp_micros(value: &AnyValue) -> Result<i64> {
    match value {
        AnyValue::Int64(value) => Ok(*value),
        AnyValue::String(value) => timestamp_string_to_micros(value),
        AnyValue::StringOwned(value) => timestamp_string_to_micros(&value.to_string()),
        AnyValue::Datetime(ts, unit, _) => Ok(match unit {
            polars::prelude::TimeUnit::Nanoseconds => *ts / 1_000,
            polars::prelude::TimeUnit::Microseconds => *ts,
            polars::prelude::TimeUnit::Milliseconds => *ts * 1_000,
        }),
        AnyValue::DatetimeOwned(ts, unit, _) => Ok(match unit {
            polars::prelude::TimeUnit::Nanoseconds => *ts / 1_000,
            polars::prelude::TimeUnit::Microseconds => *ts,
            polars::prelude::TimeUnit::Milliseconds => *ts * 1_000,
        }),
        other => anyhow::bail!("unsupported timestamp value {:?}", other),
    }
}

pub(super) fn anyvalue_to_date_days(value: &AnyValue) -> Result<i32> {
    match value {
        AnyValue::Date(value) => Ok(*value),
        AnyValue::String(value) => date_string_to_days(value),
        AnyValue::StringOwned(value) => date_string_to_days(&value.to_string()),
        other => anyhow::bail!("unsupported date value {:?}", other),
    }
}

fn i64_to_f64(value: i64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0)
}

fn parse_epoch_seconds(value: &str) -> Result<(i64, u32)> {
    let trimmed = value.trim();
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let (whole_str, frac_str) = unsigned.split_once('.').map_or((unsigned, ""), |parts| parts);
    let mut whole_seconds = whole_str
        .parse::<i64>()
        .with_context(|| format!("parsing epoch seconds {}", value))?;
    let frac_digits = &frac_str[..frac_str.len().min(9)];
    let mut nanos_str = frac_digits.to_string();
    while nanos_str.len() < 9 {
        nanos_str.push('0');
    }
    let nanos = if nanos_str.is_empty() {
        0
    } else {
        nanos_str
            .parse::<u32>()
            .with_context(|| format!("parsing epoch fraction {}", value))?
    };
    if negative && nanos > 0 {
        whole_seconds = -whole_seconds - 1;
        return Ok((whole_seconds, 1_000_000_000 - nanos));
    }
    if negative {
        whole_seconds = -whole_seconds;
    }
    Ok((whole_seconds, nanos))
}

pub(super) fn date_string_to_days(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("parsing date {}", value))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    let days = date.signed_duration_since(epoch).num_days();
    i32::try_from(days).with_context(|| format!("date {} outside supported range", value))
}

pub(super) fn timestamp_string_to_micros(value: &str) -> Result<i64> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%#z"))
        .with_context(|| format!("parsing timestamp {}", value))?
        .with_timezone(&Utc);
    Ok(timestamp.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType};
    use gcloud_bigquery::http::table::TableFieldType;

    #[test]
    fn bq_fields_from_schema_maps_json_to_string() {
        let fields = bq_fields_from_schema(&[ColumnSchema {
            name: "payload".to_string(),
            data_type: DataType::Json,
            nullable: true,
        }]);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type, TableFieldType::String);
    }

    #[test]
    fn bq_fields_from_schema_maps_interval_to_float64() {
        let fields = bq_fields_from_schema(&[ColumnSchema {
            name: "elapsed".to_string(),
            data_type: DataType::Interval,
            nullable: true,
        }]);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type, TableFieldType::Float64);
    }

    #[test]
    fn bq_fields_from_schema_maps_bytes_to_string() {
        let fields = bq_fields_from_schema(&[ColumnSchema {
            name: "payload".to_string(),
            data_type: DataType::Bytes,
            nullable: true,
        }]);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type, TableFieldType::String);
    }

    #[test]
    fn bq_fields_from_schema_maps_numeric_to_bignumeric() {
        let fields = bq_fields_from_schema(&[ColumnSchema {
            name: "amount".to_string(),
            data_type: DataType::Numeric,
            nullable: true,
        }]);

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].data_type, TableFieldType::Bignumeric);
    }
}
