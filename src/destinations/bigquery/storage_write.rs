use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use gcloud_bigquery::storage_write::AppendRowsRequestBuilder;
use gcloud_bigquery::storage_write::stream::committed::CommittedStream;
use gcloud_googleapis::cloud::bigquery::storage::v1::AppendRowsResponse;
use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response as AppendRowsStreamResponse;
use polars::frame::DataFrame;
use polars::frame::row::Row as PolarsRow;
use polars::prelude::AnyValue;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value as ReflectValue};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use tokio::sync::Mutex;
use tonic::Code;
use tracing::{error, warn};

use super::BigQueryDestination;
use crate::destinations::bigquery::values::{
    anyvalue_to_bool, anyvalue_to_bytes, anyvalue_to_f64, anyvalue_to_i64,
    anyvalue_to_owned_string, anyvalue_to_timestamp_micros,
};
use crate::destinations::with_metadata_schema;
use crate::types::{DataType, TableSchema};

pub(super) struct StorageWriteTableWriter {
    pub(super) stream: Arc<CommittedStream>,
    pub(super) next_offset: Mutex<i64>,
    pub(super) schema_key: String,
    pub(super) descriptor_proto: DescriptorProto,
    pub(super) message_descriptor: MessageDescriptor,
}

impl BigQueryDestination {
    fn storage_write_enabled(&self) -> bool {
        self.config.storage_write_enabled.unwrap_or(true) && self.config.emulator_http.is_none()
    }

    pub(super) async fn append_rows_via_storage_write(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
    ) -> Result<bool> {
        if !self.storage_write_enabled() {
            return Ok(false);
        }

        let full_schema = with_metadata_schema(schema, &self.metadata);
        if !supports_storage_write_schema(&full_schema) {
            warn!(
                table = table_id,
                "storage write disabled for schema; falling back to insertAll"
            );
            return Ok(false);
        }

        let writer = self
            .get_or_create_storage_writer(table_id, &full_schema)
            .await?;
        let rows = encode_storage_write_rows(frame, &full_schema, &writer.message_descriptor)?;
        if rows.is_empty() {
            return Ok(true);
        }

        let mut offset_guard = writer.next_offset.lock().await;
        let offset = *offset_guard;
        let batch_len = rows.len() as i64;
        let request = AppendRowsRequestBuilder::new(writer.descriptor_proto.clone(), rows)
            .with_offset(offset);
        let append_result = writer.stream.append_rows(vec![request]).await;
        let mut responses = match append_result {
            Ok(responses) => responses,
            Err(err) if err.code() == Code::AlreadyExists => {
                *offset_guard += batch_len;
                return Ok(true);
            }
            Err(err) => {
                error!(
                    table = %table_id,
                    rows = batch_len,
                    error = %err,
                    "BigQuery Storage Write append failed"
                );
                return Err(anyhow::anyhow!("storage write append failed: {}", err));
            }
        };

        while let Some(response) = responses.next().await {
            let response = response.map_err(|err| {
                error!(
                    table = %table_id,
                    rows = batch_len,
                    error = %err,
                    "BigQuery Storage Write response failed"
                );
                anyhow::anyhow!("storage write response failed: {}", err)
            })?;
            if let Some(advance_offset) = validate_storage_write_response(table_id, &response)?
                && advance_offset
            {
                *offset_guard += batch_len;
                return Ok(true);
            }
        }

        *offset_guard += batch_len;
        Ok(true)
    }

    async fn get_or_create_storage_writer(
        &self,
        table_id: &str,
        schema: &TableSchema,
    ) -> Result<Arc<StorageWriteTableWriter>> {
        let schema_key = storage_write_schema_key(schema);
        let mut guard = self.storage_writers.lock().await;
        if let Some(existing) = guard.get(table_id)
            && existing.schema_key == schema_key
        {
            return Ok(existing.clone());
        }

        let descriptor_proto = build_storage_write_descriptor(schema);
        let message_descriptor = build_message_descriptor(&descriptor_proto)?;
        let fqtn = format!(
            "projects/{}/datasets/{}/tables/{}",
            self.config.project_id, self.config.dataset, table_id
        );
        let stream = self
            .client
            .committed_storage_writer()
            .create_write_stream(&fqtn)
            .await
            .map_err(|err| anyhow::anyhow!("creating storage write stream failed: {}", err))?;
        let writer = Arc::new(StorageWriteTableWriter {
            stream: Arc::new(stream),
            next_offset: Mutex::new(0),
            schema_key,
            descriptor_proto,
            message_descriptor,
        });
        guard.insert(table_id.to_string(), writer.clone());
        Ok(writer)
    }

    pub(super) async fn invalidate_storage_writer(&self, table_id: &str) {
        let mut guard = self.storage_writers.lock().await;
        guard.remove(table_id);
    }
}

fn supports_storage_write_schema(schema: &TableSchema) -> bool {
    schema
        .columns
        .iter()
        .all(|column| is_valid_proto_field_name(&column.name))
}

fn is_valid_proto_field_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn storage_write_schema_key(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|column| format!("{}:{:?}:{}", column.name, column.data_type, column.nullable))
        .collect::<Vec<_>>()
        .join("|")
}

fn build_storage_write_descriptor(schema: &TableSchema) -> DescriptorProto {
    DescriptorProto {
        name: Some("CdsyncRow".to_string()),
        field: schema
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| FieldDescriptorProto {
                name: Some(column.name.clone()),
                number: Some((idx + 1) as i32),
                label: Some(Label::Optional as i32),
                r#type: Some(storage_write_field_type(column.data_type.clone()) as i32),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn storage_write_field_type(data_type: DataType) -> Type {
    match data_type {
        DataType::String | DataType::Date | DataType::Numeric | DataType::Json => Type::String,
        DataType::Int64 => Type::Int64,
        DataType::Float64 | DataType::Interval => Type::Double,
        DataType::Bool => Type::Bool,
        DataType::Timestamp => Type::Int64,
        DataType::Bytes => Type::Bytes,
    }
}

fn build_message_descriptor(descriptor_proto: &DescriptorProto) -> Result<MessageDescriptor> {
    let file_descriptor = FileDescriptorProto {
        name: Some("cdsync_storage_write.proto".to_string()),
        syntax: Some("proto2".to_string()),
        message_type: vec![descriptor_proto.clone()],
        ..Default::default()
    };
    let file_descriptor_set = FileDescriptorSet {
        file: vec![file_descriptor],
    };
    let descriptor_pool = DescriptorPool::decode(file_descriptor_set.encode_to_vec().as_slice())
        .context("building storage write descriptor pool")?;
    descriptor_pool
        .get_message_by_name("CdsyncRow")
        .context("missing storage write message descriptor")
}

fn encode_storage_write_rows(
    frame: &DataFrame,
    schema: &TableSchema,
    message_descriptor: &MessageDescriptor,
) -> Result<Vec<Vec<u8>>> {
    let columns = frame.get_column_names();
    let height = frame.height();
    let mut rows = Vec::with_capacity(height);
    let mut row = PolarsRow::new(vec![AnyValue::Null; columns.len()]);
    for idx in 0..height {
        frame.get_row_amortized(idx, &mut row)?;
        let mut message = DynamicMessage::new(message_descriptor.clone());
        for (column, value) in schema.columns.iter().zip(row.0.iter()) {
            if let Some(reflect_value) = anyvalue_to_storage_write_value(&column.data_type, value)?
            {
                let field = message_descriptor
                    .get_field_by_name(&column.name)
                    .with_context(|| format!("missing proto field {}", column.name))?;
                message.set_field(&field, reflect_value);
            }
        }
        rows.push(message.encode_to_vec());
    }
    Ok(rows)
}

fn anyvalue_to_storage_write_value(
    data_type: &DataType,
    value: &AnyValue,
) -> Result<Option<ReflectValue>> {
    if matches!(value, AnyValue::Null) {
        return Ok(None);
    }
    let reflect_value = match data_type {
        DataType::String | DataType::Date | DataType::Numeric | DataType::Json => {
            ReflectValue::String(anyvalue_to_owned_string(value)?)
        }
        DataType::Int64 => ReflectValue::I64(anyvalue_to_i64(value)?),
        DataType::Float64 | DataType::Interval => ReflectValue::F64(anyvalue_to_f64(value)?),
        DataType::Bool => ReflectValue::Bool(anyvalue_to_bool(value)?),
        DataType::Timestamp => ReflectValue::I64(anyvalue_to_timestamp_micros(value)?),
        DataType::Bytes => ReflectValue::Bytes(Bytes::from(anyvalue_to_bytes(value)?)),
    };
    Ok(Some(reflect_value))
}

fn validate_storage_write_response(
    table_id: &str,
    response: &AppendRowsResponse,
) -> Result<Option<bool>> {
    if !response.row_errors.is_empty() {
        crate::telemetry::record_bigquery_row_errors(table_id, response.row_errors.len() as u64);
        anyhow::bail!(
            "storage write row errors for {}: {} rows",
            table_id,
            response.row_errors.len()
        );
    }

    match &response.response {
        Some(AppendRowsStreamResponse::AppendResult(_)) | None => Ok(None),
        Some(AppendRowsStreamResponse::Error(status))
            if status.code == Code::AlreadyExists as i32 =>
        {
            Ok(Some(true))
        }
        Some(AppendRowsStreamResponse::Error(status)) => {
            anyhow::bail!(
                "storage write stream error for {}: {}",
                table_id,
                status.message
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType, TableSchema};
    use chrono::{DateTime, TimeZone, Utc};
    use gcloud_googleapis::cloud::bigquery::storage::v1::AppendRowsResponse;
    use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::{
        AppendResult, Response,
    };
    use gcloud_googleapis::rpc::Status;
    use polars::frame::DataFrame;
    use polars::prelude::NamedFrom;
    use prost_reflect::Value as ReflectValue;
    use std::borrow::Cow;

    use crate::destinations::bigquery::values::timestamp_string_to_micros;

    fn schema() -> TableSchema {
        TableSchema {
            name: "public__items".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "updated_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    data_type: DataType::Json,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        }
    }

    #[test]
    fn storage_write_schema_supports_valid_names() {
        assert!(supports_storage_write_schema(&schema()));
        assert!(!supports_storage_write_schema(&TableSchema {
            name: "bad".to_string(),
            columns: vec![ColumnSchema {
                name: "bad-name".to_string(),
                data_type: DataType::String,
                nullable: true,
            }],
            primary_key: None,
        }));
    }

    #[test]
    fn storage_write_descriptor_round_trip_encodes_rows() {
        let schema = schema();
        let descriptor = build_storage_write_descriptor(&schema);
        let message_descriptor =
            build_message_descriptor(&descriptor).expect("message descriptor should build");

        let updated_at = Utc.with_ymd_and_hms(2026, 3, 26, 12, 0, 0).unwrap();
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1i64]).into(),
            polars::prelude::Series::new("updated_at".into(), &[updated_at.to_rfc3339()]).into(),
            polars::prelude::Series::new("payload".into(), &[r#"{"k":"v"}"#]).into(),
        ])
        .expect("frame");

        let rows =
            encode_storage_write_rows(&frame, &schema, &message_descriptor).expect("encoded rows");
        assert_eq!(rows.len(), 1);

        let message = DynamicMessage::decode(message_descriptor, rows[0].as_slice()).unwrap();
        let id = message
            .get_field_by_name("id")
            .expect("id field should exist");
        assert_eq!(id, Cow::Owned(ReflectValue::I64(1)));
    }

    #[test]
    fn storage_write_response_errors_fail() {
        let response = AppendRowsResponse {
            response: Some(Response::Error(Status {
                code: Code::Internal as i32,
                message: "boom".to_string(),
                details: Vec::new(),
            })),
            ..Default::default()
        };

        let err = validate_storage_write_response("items", &response).expect_err("should fail");
        assert!(err.to_string().contains("storage write stream error"));
    }

    #[test]
    fn storage_write_already_exists_response_is_idempotent() {
        let response = AppendRowsResponse {
            response: Some(Response::Error(Status {
                code: Code::AlreadyExists as i32,
                message: "duplicate".to_string(),
                details: Vec::new(),
            })),
            ..Default::default()
        };

        assert_eq!(
            validate_storage_write_response("items", &response).expect("should be handled"),
            Some(true)
        );

        let success = AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset: Some(1) })),
            ..Default::default()
        };
        assert_eq!(
            validate_storage_write_response("items", &success).expect("append result"),
            None
        );
    }

    #[test]
    fn timestamp_string_to_micros_accepts_postgres_style_timestamptz() {
        let micros = timestamp_string_to_micros("2026-03-30 01:36:12.186373+00")
            .expect("postgres timestamptz");
        let dt = DateTime::<Utc>::from_timestamp_micros(micros).expect("valid micros");
        assert_eq!(dt.to_rfc3339(), "2026-03-30T01:36:12.186373+00:00");
    }
}
