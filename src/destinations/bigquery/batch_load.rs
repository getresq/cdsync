use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use polars::io::parquet::write::{ParquetCompression, ParquetWriter};
use polars::prelude::{
    AnyValue, BinaryChunked, DataFrame, IntoSeries, NamedFrom, NewChunkedArray, Series,
};
use token_source::TokenSource;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use gcloud_bigquery::http::job::get::GetJobRequest;
use gcloud_bigquery::http::job::{
    CreateDisposition, Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState,
    JobType, WriteDisposition,
};
use gcloud_bigquery::http::table::{
    ParquetOptions, SourceFormat, TableReference, TableSchema as BqTableSchema,
};

use super::BigQueryDestination;
use crate::destinations::bigquery::values::{
    anyvalue_to_bool, anyvalue_to_bytes, anyvalue_to_date_days, anyvalue_to_f64, anyvalue_to_i64,
    anyvalue_to_owned_string, anyvalue_to_timestamp_micros, bq_fields_from_schema,
    dataframe_to_json_rows,
};
use crate::destinations::with_metadata_schema;
use crate::types::{ColumnSchema, DataType, TableSchema};

impl BigQueryDestination {
    fn batch_load_enabled(&self) -> bool {
        self.config.batch_load_bucket.is_some() && self.config.emulator_http.is_none()
    }

    pub(super) async fn append_rows_via_batch_load(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
    ) -> Result<bool> {
        if !self.batch_load_enabled() {
            return Ok(false);
        }

        let bucket = match &self.config.batch_load_bucket {
            Some(bucket) => bucket,
            None => return Ok(false),
        };
        let token_source = match &self.gcs_token_source {
            Some(token_source) => token_source,
            None => anyhow::bail!("GCS batch load requested but token source is unavailable"),
        };

        let schema = with_metadata_schema(schema, &self.metadata);
        let format = if parquet_batch_load_supported(&schema) {
            BatchLoadFormat::Parquet
        } else {
            warn!(
                table = %table_id,
                "parquet batch load unsupported for schema; falling back to ndjson"
            );
            BatchLoadFormat::Ndjson
        };

        let object_name = batch_load_object_name(
            self.config.batch_load_prefix.as_deref(),
            table_id,
            format.file_extension(),
        );
        let object_uri = format!("gs://{}/{}", bucket, object_name);
        let body = match format {
            BatchLoadFormat::Ndjson => dataframe_to_ndjson_bytes(frame)?,
            BatchLoadFormat::Parquet => dataframe_to_parquet_bytes(frame, &schema)?,
        };
        self.upload_batch_load_object(
            token_source,
            bucket,
            &object_name,
            format.content_type(),
            body,
        )
        .await
        .with_context(|| format!("uploading batch load object {}", object_uri))?;

        self.run_load_job(table_id, &schema, &object_uri, format)
            .await?;
        Ok(true)
    }

    async fn upload_batch_load_object(
        &self,
        token_source: &Arc<dyn TokenSource>,
        bucket: &str,
        object_name: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<()> {
        let token = token_source
            .token()
            .await
            .map_err(|err| anyhow::anyhow!("fetching GCS access token failed: {}", err))?;
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            bucket,
            urlencoding::encode(object_name)
        );
        let response = reqwest::Client::new()
            .post(url)
            .header("Authorization", token)
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("GCS upload failed: {} {}", status, body);
        }
        Ok(())
    }

    async fn run_load_job(
        &self,
        table_id: &str,
        schema: &TableSchema,
        source_uri: &str,
        format: BatchLoadFormat,
    ) -> Result<()> {
        let job_id = format!("cdsync_load_{}", Uuid::new_v4().simple());
        let location = self.config.location.clone();
        let job = Job {
            job_reference: JobReference {
                project_id: self.config.project_id.clone(),
                job_id: job_id.clone(),
                location: location.clone(),
            },
            configuration: JobConfiguration {
                job_type: "LOAD".to_string(),
                job: JobType::Load(JobConfigurationLoad {
                    source_uris: vec![source_uri.to_string()],
                    schema: Some(BqTableSchema {
                        fields: bq_fields_from_schema(&schema.columns),
                    }),
                    destination_table: TableReference {
                        project_id: self.config.project_id.clone(),
                        dataset_id: self.config.dataset.clone(),
                        table_id: table_id.to_string(),
                    },
                    create_disposition: Some(CreateDisposition::CreateIfNeeded),
                    write_disposition: Some(WriteDisposition::WriteAppend),
                    source_format: Some(format.source_format()),
                    max_bad_records: Some(0),
                    autodetect: Some(false),
                    ignore_unknown_values: Some(false),
                    parquet_options: match format {
                        BatchLoadFormat::Parquet => Some(ParquetOptions::default()),
                        BatchLoadFormat::Ndjson => None,
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let created = self
            .client
            .job()
            .create(&job)
            .await
            .with_context(|| format!("creating BigQuery load job for {}", source_uri))?;

        self.wait_for_job_completion(&created).await
    }

    async fn wait_for_job_completion(&self, job: &Job) -> Result<()> {
        let job_id = &job.job_reference.job_id;
        let location = job.job_reference.location.clone();
        loop {
            let current = self
                .client
                .job()
                .get(
                    &self.config.project_id,
                    job_id,
                    &GetJobRequest {
                        location: location.clone(),
                    },
                )
                .await
                .with_context(|| format!("fetching BigQuery job {}", job_id))?;

            if current.status.state == JobState::Done {
                if let Some(error_result) = current.status.error_result {
                    anyhow::bail!("BigQuery load job {} failed: {:?}", job_id, error_result);
                }
                if let Some(errors) = current.status.errors
                    && !errors.is_empty()
                {
                    anyhow::bail!("BigQuery load job {} reported errors: {:?}", job_id, errors);
                }
                return Ok(());
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

#[derive(Clone, Copy)]
enum BatchLoadFormat {
    Ndjson,
    Parquet,
}

fn batch_load_object_name(prefix: Option<&str>, table_id: &str, extension: &str) -> String {
    let base = format!(
        "{}_{}.{}",
        table_id.replace('.', "_"),
        Uuid::new_v4().simple(),
        extension
    );
    match prefix.map(str::trim).filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), base),
        None => base,
    }
}

impl BatchLoadFormat {
    fn file_extension(self) -> &'static str {
        match self {
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Ndjson => "application/x-ndjson",
            Self::Parquet => "application/vnd.apache.parquet",
        }
    }

    fn source_format(self) -> SourceFormat {
        match self {
            Self::Ndjson => SourceFormat::NewlineDelimitedJson,
            Self::Parquet => SourceFormat::Parquet,
        }
    }
}

fn dataframe_to_ndjson_bytes(frame: &DataFrame) -> Result<Vec<u8>> {
    let rows = dataframe_to_json_rows(frame)?;
    let mut out = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut out, &row)?;
        out.push(b'\n');
    }
    Ok(out)
}

fn dataframe_to_parquet_bytes(frame: &DataFrame, schema: &TableSchema) -> Result<Vec<u8>> {
    let mut parquet_frame = parquet_batch_load_frame(frame, schema)?;
    let mut cursor = Cursor::new(Vec::new());
    ParquetWriter::new(&mut cursor)
        .with_compression(ParquetCompression::Snappy)
        .finish(&mut parquet_frame)
        .context("writing parquet batch load payload")?;
    Ok(cursor.into_inner())
}

fn parquet_batch_load_supported(schema: &TableSchema) -> bool {
    schema
        .columns
        .iter()
        .all(|column| !matches!(column.data_type, DataType::Numeric | DataType::Json))
}

fn parquet_batch_load_frame(frame: &DataFrame, schema: &TableSchema) -> Result<DataFrame> {
    let mut parquet_frame = frame.clone();
    for column in &schema.columns {
        let idx = parquet_frame
            .try_get_column_index(&column.name)
            .with_context(|| format!("missing batch load column {}", column.name))?;
        let series = parquet_batch_load_series(
            parquet_frame
                .column(&column.name)
                .with_context(|| format!("missing batch load column {}", column.name))?
                .as_materialized_series(),
            column,
        )?;
        parquet_frame
            .replace_column(idx, series)
            .with_context(|| format!("replacing batch load column {}", column.name))?;
    }
    Ok(parquet_frame)
}

fn parquet_batch_load_series(series: &Series, column: &ColumnSchema) -> Result<Series> {
    match column.data_type {
        DataType::String => {
            let values = collect_parquet_string_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Int64 => {
            let values = collect_parquet_int64_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Float64 => {
            let values = collect_parquet_float64_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Bool => {
            let values = collect_parquet_bool_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Timestamp => {
            let values = collect_parquet_timestamp_values(series)?;
            let mut out = Series::new(column.name.as_str().into(), values);
            out = out.cast(&polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Microseconds,
                None,
            ))?;
            Ok(out)
        }
        DataType::Date => {
            let values = collect_parquet_date_values(series)?;
            let mut out = Series::new(column.name.as_str().into(), values);
            out = out.cast(&polars::prelude::DataType::Date)?;
            Ok(out)
        }
        DataType::Bytes => {
            let values = collect_parquet_binary_values(series)?;
            let refs: Vec<Option<&[u8]>> = values.iter().map(|value| value.as_deref()).collect();
            Ok(BinaryChunked::from_slice_options(column.name.as_str().into(), &refs).into_series())
        }
        DataType::Numeric | DataType::Json => anyhow::bail!(
            "parquet batch load does not support {:?} columns yet: {}",
            column.data_type,
            column.name
        ),
    }
}

fn collect_parquet_string_values(series: &Series) -> Result<Vec<Option<String>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_owned_string(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_int64_values(series: &Series) -> Result<Vec<Option<i64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_i64(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_float64_values(series: &Series) -> Result<Vec<Option<f64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_f64(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_bool_values(series: &Series) -> Result<Vec<Option<bool>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_bool(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_timestamp_values(series: &Series) -> Result<Vec<Option<i64>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_timestamp_micros(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_date_values(series: &Series) -> Result<Vec<Option<i32>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_date_days(&value).map(Some)
            }
        })
        .collect()
}

fn collect_parquet_binary_values(series: &Series) -> Result<Vec<Option<Vec<u8>>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                anyvalue_to_bytes(&value).map(Some)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnSchema, DataType, MetadataColumns, TableSchema};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use polars::io::SerReader;
    use polars::io::parquet::read::ParquetReader;
    use polars::prelude::NamedFrom;
    use std::io::Cursor;

    fn parquet_schema() -> TableSchema {
        TableSchema {
            name: "public__events".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "occurred_at".to_string(),
                    data_type: DataType::Timestamp,
                    nullable: false,
                },
                ColumnSchema {
                    name: "event_date".to_string(),
                    data_type: DataType::Date,
                    nullable: false,
                },
                ColumnSchema {
                    name: "raw_bytes".to_string(),
                    data_type: DataType::Bytes,
                    nullable: true,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        }
    }

    #[test]
    fn batch_load_object_name_uses_prefix_when_present() {
        let name = batch_load_object_name(Some("staging/app"), "public__items", "parquet");
        assert!(name.starts_with("staging/app/public__items_"));
        assert!(name.ends_with(".parquet"));
    }

    #[test]
    fn parquet_batch_load_supported_rejects_json_and_numeric() {
        let json_schema = TableSchema {
            name: "public__items".to_string(),
            columns: vec![ColumnSchema {
                name: "payload".to_string(),
                data_type: DataType::Json,
                nullable: true,
            }],
            primary_key: Some("id".to_string()),
        };
        assert!(!parquet_batch_load_supported(&json_schema));
        let numeric_schema = TableSchema {
            name: "public__prices".to_string(),
            columns: vec![ColumnSchema {
                name: "amount".to_string(),
                data_type: DataType::Numeric,
                nullable: true,
            }],
            primary_key: None,
        };
        assert!(!parquet_batch_load_supported(&numeric_schema));
        assert!(parquet_batch_load_supported(&parquet_schema()));
    }

    #[test]
    fn dataframe_to_parquet_bytes_round_trips_supported_batch_load_types() {
        let metadata = MetadataColumns::default();
        let schema = parquet_schema();
        let full_schema = with_metadata_schema(&schema, &metadata);
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1_i64]).into(),
            polars::prelude::Series::new(
                "occurred_at".into(),
                &["2026-03-30T01:36:12.186373+00:00"],
            )
            .into(),
            polars::prelude::Series::new("event_date".into(), &["2026-03-30"]).into(),
            polars::prelude::Series::new("raw_bytes".into(), &[STANDARD.encode(b"abc")]).into(),
            polars::prelude::Series::new("name".into(), &["alpha"]).into(),
            polars::prelude::Series::new(
                metadata.synced_at.as_str().into(),
                &["2026-03-30T01:36:12.186373+00:00"],
            )
            .into(),
            polars::prelude::Series::new(
                metadata.deleted_at.as_str().into(),
                &[Option::<&str>::None],
            )
            .into(),
        ])
        .expect("frame");

        let payload = dataframe_to_parquet_bytes(&frame, &full_schema).expect("parquet bytes");
        let parquet = ParquetReader::new(Cursor::new(payload))
            .finish()
            .expect("read parquet");

        assert_eq!(
            parquet.column("id").expect("id").dtype(),
            &polars::prelude::DataType::Int64
        );
        assert!(matches!(
            parquet.column("occurred_at").expect("occurred_at").dtype(),
            polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Microseconds, _)
        ));
        assert_eq!(
            parquet.column("event_date").expect("event_date").dtype(),
            &polars::prelude::DataType::Date
        );
        assert_eq!(
            parquet.column("raw_bytes").expect("raw_bytes").dtype(),
            &polars::prelude::DataType::Binary
        );
        assert_eq!(
            parquet.column("name").expect("name").dtype(),
            &polars::prelude::DataType::String
        );
        assert!(matches!(
            parquet
                .column(metadata.synced_at.as_str())
                .expect("synced_at")
                .dtype(),
            polars::prelude::DataType::Datetime(polars::prelude::TimeUnit::Microseconds, _)
        ));
        assert_eq!(
            parquet
                .column(metadata.deleted_at.as_str())
                .expect("deleted_at")
                .null_count(),
            1
        );
    }

    #[test]
    fn dataframe_to_ndjson_bytes_emits_one_json_object_per_line() {
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1_i64, 2_i64]).into(),
            polars::prelude::Series::new("name".into(), &["alpha", "beta"]).into(),
        ])
        .expect("frame");

        let payload = dataframe_to_ndjson_bytes(&frame).expect("ndjson bytes");
        let text = String::from_utf8(payload).expect("utf8");
        let lines: Vec<&str> = text.lines().collect();

        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[0].contains("\"name\":\"alpha\""));
        assert!(lines[1].contains("\"id\":2"));
        assert!(lines[1].contains("\"name\":\"beta\""));
    }
}
