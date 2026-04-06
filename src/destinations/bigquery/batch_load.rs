use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use polars::io::parquet::write::{ParquetCompression, ParquetWriter};
use polars::prelude::{AnyValue, DataFrame, NamedFrom, Series};
use token_source::TokenSource;
use tokio::task;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use gcloud_bigquery::http::job::get::GetJobRequest;
use gcloud_bigquery::http::job::{
    CreateDisposition, Job, JobConfiguration, JobConfigurationLoad, JobReference, JobState,
    JobType, WriteDisposition,
};
use gcloud_bigquery::http::table::{
    ParquetOptions, SourceFormat, TableReference, TableSchema as BqTableSchema,
};

use super::BIGQUERY_REQUEST_TIMEOUT;
use super::BigQueryDestination;
use crate::destinations::bigquery::values::{
    anyvalue_to_bool, anyvalue_to_date_days, anyvalue_to_f64, anyvalue_to_i64,
    anyvalue_to_owned_string, anyvalue_to_timestamp_micros, bq_fields_from_schema,
};
use crate::destinations::with_metadata_schema;
use crate::types::{ColumnSchema, DataType, TableSchema};

impl BigQueryDestination {
    pub(super) async fn append_rows_via_batch_load(
        &self,
        table_id: &str,
        schema: &TableSchema,
        frame: &DataFrame,
    ) -> Result<()> {
        let bucket = self
            .config
            .batch_load_bucket
            .as_deref()
            .context("missing bigquery.batch_load_bucket")?;
        let Some(token_source) = &self.gcs_token_source else {
            anyhow::bail!("GCS batch load requested but token source is unavailable");
        };

        let schema = with_metadata_schema(schema, &self.metadata);
        let object_name = batch_load_object_name(
            self.config.batch_load_prefix.as_deref(),
            table_id,
            PARQUET_FILE_EXTENSION,
        );
        let object_uri = format!("gs://{}/{}", bucket, object_name);
        tracing::info!(
            table = table_id,
            rows = frame.height(),
            object_uri = %object_uri,
            "starting batch-load append via GCS object upload"
        );
        let body = parquet_payload(frame, &schema).await?;
        self.upload_batch_load_object(
            token_source,
            bucket,
            &object_name,
            PARQUET_CONTENT_TYPE,
            body,
        )
        .await
        .with_context(|| format!("uploading batch load object {}", object_uri))?;

        self.run_load_job(
            table_id,
            &schema,
            &object_uri,
            WriteDisposition::WriteAppend,
        )
        .await?;
        tracing::info!(
            table = table_id,
            rows = frame.height(),
            object_uri = %object_uri,
            "batch-load append completed"
        );
        Ok(())
    }

    pub(super) async fn upload_batch_load_object(
        &self,
        token_source: &Arc<dyn TokenSource>,
        bucket: &str,
        object_name: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<()> {
        let token = match timeout(BIGQUERY_REQUEST_TIMEOUT, token_source.token()).await {
            Ok(Ok(token)) => token,
            Ok(Err(err)) => {
                return Err(anyhow::anyhow!("fetching GCS access token failed: {}", err));
            }
            Err(_) => {
                anyhow::bail!(
                    "fetching GCS access token for {} timed out after {}s",
                    object_name,
                    BIGQUERY_REQUEST_TIMEOUT.as_secs()
                );
            }
        };
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
            .send();
        let response = BigQueryDestination::await_with_timeout(
            format!("uploading GCS batch load object {}", object_name),
            BIGQUERY_REQUEST_TIMEOUT,
            response,
        )
        .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = BigQueryDestination::await_with_timeout(
                format!("reading GCS batch load error body for {}", object_name),
                BIGQUERY_REQUEST_TIMEOUT,
                response.text(),
            )
            .await
            .unwrap_or_default();
            anyhow::bail!("GCS upload failed: {} {}", status, body);
        }
        Ok(())
    }

    pub(super) async fn run_load_job(
        &self,
        table_id: &str,
        schema: &TableSchema,
        source_uri: &str,
        write_disposition: WriteDisposition,
    ) -> Result<()> {
        let job_id = format!("cdsync_load_{}", Uuid::new_v4().simple());
        let location = self.config.location.clone();
        let job = build_load_job(
            &self.config.project_id,
            &self.config.dataset,
            table_id,
            schema,
            source_uri,
            &job_id,
            location.as_deref(),
            write_disposition,
        );

        let created = BigQueryDestination::await_with_timeout(
            format!("creating BigQuery load job for {}", source_uri),
            BIGQUERY_REQUEST_TIMEOUT,
            self.client.job().create(&job),
        )
        .await?;

        tracing::info!(
            table = table_id,
            source_uri = source_uri,
            job_id = created.job_reference.job_id,
            "created BigQuery load job"
        );
        self.wait_for_job_completion(table_id, &created).await
    }

    async fn wait_for_job_completion(&self, table_id: &str, job: &Job) -> Result<()> {
        let job_id = &job.job_reference.job_id;
        let location = job.job_reference.location.clone();
        let started_at = std::time::Instant::now();
        let mut last_log_at = started_at;
        loop {
            let current = BigQueryDestination::await_with_timeout(
                format!("fetching BigQuery job {}", job_id),
                BIGQUERY_REQUEST_TIMEOUT,
                self.client.job().get(
                    &self.config.project_id,
                    job_id,
                    &GetJobRequest {
                        location: location.clone(),
                    },
                ),
            )
            .await?;

            if last_log_at.elapsed() >= BATCH_LOAD_JOB_PROGRESS_LOG_INTERVAL {
                tracing::info!(
                    table = table_id,
                    job_id = job_id,
                    state = ?current.status.state,
                    elapsed_secs = started_at.elapsed().as_secs(),
                    "waiting for BigQuery load job completion"
                );
                last_log_at = std::time::Instant::now();
            }

            if current.status.state == JobState::Done {
                if let Some(error_result) = current.status.error_result {
                    anyhow::bail!("BigQuery load job {} failed: {:?}", job_id, error_result);
                }
                if let Some(errors) = current.status.errors
                    && !errors.is_empty()
                {
                    anyhow::bail!("BigQuery load job {} reported errors: {:?}", job_id, errors);
                }
                tracing::info!(
                    table = table_id,
                    job_id = job_id,
                    elapsed_secs = started_at.elapsed().as_secs(),
                    "BigQuery load job completed"
                );
                return Ok(());
            }

            if started_at.elapsed() >= BATCH_LOAD_JOB_HARD_TIMEOUT {
                anyhow::bail!(
                    "BigQuery load job {} for {} exceeded hard timeout of {}s",
                    job_id,
                    table_id,
                    BATCH_LOAD_JOB_HARD_TIMEOUT.as_secs()
                );
            }

            sleep(Duration::from_secs(1)).await;
        }
    }
}

pub(super) const PARQUET_FILE_EXTENSION: &str = "parquet";
pub(super) const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";
const BATCH_LOAD_PARQUET_BLOCKING_ROWS: usize = 1024;
const BATCH_LOAD_JOB_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const BATCH_LOAD_JOB_HARD_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 2);

pub(super) async fn parquet_payload(frame: &DataFrame, schema: &TableSchema) -> Result<Vec<u8>> {
    if frame.height() < BATCH_LOAD_PARQUET_BLOCKING_ROWS {
        return dataframe_to_parquet_bytes(frame, schema);
    }

    let frame = frame.clone();
    let schema = schema.clone();
    task::spawn_blocking(move || dataframe_to_parquet_bytes(&frame, &schema))
        .await
        .map_err(|err| anyhow::anyhow!("failed to join batch-load parquet task: {}", err))?
}

fn build_load_job(
    project_id: &str,
    dataset_id: &str,
    table_id: &str,
    schema: &TableSchema,
    source_uri: &str,
    job_id: &str,
    location: Option<&str>,
    write_disposition: WriteDisposition,
) -> Job {
    Job {
        job_reference: JobReference {
            project_id: project_id.to_string(),
            job_id: job_id.to_string(),
            location: location.map(ToOwned::to_owned),
        },
        configuration: JobConfiguration {
            job_type: "LOAD".to_string(),
            job: JobType::Load(JobConfigurationLoad {
                source_uris: vec![source_uri.to_string()],
                schema: Some(BqTableSchema {
                    fields: bq_fields_from_schema(&schema.columns),
                }),
                destination_table: TableReference {
                    project_id: project_id.to_string(),
                    dataset_id: dataset_id.to_string(),
                    table_id: table_id.to_string(),
                },
                create_disposition: Some(CreateDisposition::CreateIfNeeded),
                write_disposition: Some(write_disposition),
                source_format: Some(SourceFormat::Parquet),
                max_bad_records: Some(0),
                autodetect: Some(false),
                ignore_unknown_values: Some(false),
                parquet_options: Some(ParquetOptions::default()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    }
}

pub(super) fn batch_load_object_name(
    prefix: Option<&str>,
    table_id: &str,
    extension: &str,
) -> String {
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

pub(super) fn batch_load_object_name_for_key(
    prefix: Option<&str>,
    table_id: &str,
    key: &str,
    extension: &str,
) -> String {
    let base = format!("{}_{}.{}", table_id.replace('.', "_"), key, extension);
    match prefix.map(str::trim).filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => format!("{}/{}", prefix.trim_end_matches('/'), base),
        None => base,
    }
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
        DataType::Float64 | DataType::Interval => {
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
            let values = collect_parquet_string_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
        DataType::Numeric => collect_parquet_decimal_series(series, column),
        DataType::Json => {
            let values = collect_parquet_string_values(series)?;
            Ok(Series::new(column.name.as_str().into(), values))
        }
    }
}

fn collect_parquet_decimal_series(series: &Series, column: &ColumnSchema) -> Result<Series> {
    let values = collect_parquet_string_values(series)?;
    let refs: Vec<Option<&str>> = values.iter().map(|value| value.as_deref()).collect();
    let string_series = Series::new(column.name.as_str().into(), refs);
    string_series
        .str()
        .context("numeric parquet batch load requires string-backed values")?
        .to_decimal_infer(string_series.len())
        .context("converting numeric batch load column to parquet decimal")
}

fn collect_parquet_string_values(series: &Series) -> Result<Vec<Option<String>>> {
    series
        .iter()
        .map(|value| {
            if matches!(value, AnyValue::Null) {
                Ok(None)
            } else {
                Ok(Some(anyvalue_to_owned_string(&value)))
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
            &polars::prelude::DataType::String
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
    fn dataframe_to_parquet_bytes_round_trips_numeric_and_json_types() {
        let schema = TableSchema {
            name: "public__documents".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnSchema {
                    name: "amount".to_string(),
                    data_type: DataType::Numeric,
                    nullable: true,
                },
                ColumnSchema {
                    name: "payload".to_string(),
                    data_type: DataType::Json,
                    nullable: true,
                },
            ],
            primary_key: Some("id".to_string()),
        };
        let frame = DataFrame::new(vec![
            polars::prelude::Series::new("id".into(), &[1_i64]).into(),
            polars::prelude::Series::new("amount".into(), &["123.45"]).into(),
            polars::prelude::Series::new("payload".into(), &["{\"status\":\"ok\"}"]).into(),
        ])
        .expect("frame");

        let payload = dataframe_to_parquet_bytes(&frame, &schema).expect("parquet bytes");
        let parquet = ParquetReader::new(Cursor::new(payload))
            .finish()
            .expect("read parquet");

        assert!(matches!(
            parquet.column("amount").expect("amount").dtype(),
            polars::prelude::DataType::Decimal(_, Some(2))
        ));
        assert_eq!(
            parquet.column("payload").expect("payload").dtype(),
            &polars::prelude::DataType::String
        );
    }

    #[test]
    fn build_load_job_omits_decimal_target_types_when_schema_is_explicit() {
        let schema = TableSchema {
            name: "public__documents".to_string(),
            columns: vec![ColumnSchema {
                name: "amount".to_string(),
                data_type: DataType::Numeric,
                nullable: true,
            }],
            primary_key: None,
        };

        let job = build_load_job(
            "proj",
            "dataset",
            "public__documents",
            &schema,
            "gs://bucket/path.parquet",
            "job_123",
            Some("US"),
            WriteDisposition::WriteTruncate,
        );

        let JobType::Load(load) = job.configuration.job else {
            panic!("expected load job");
        };
        assert!(load.schema.is_some());
        assert!(load.decimal_target_types.is_none());
        assert_eq!(
            load.write_disposition,
            Some(WriteDisposition::WriteTruncate)
        );
    }

    #[test]
    fn build_load_job_preserves_append_write_disposition_for_direct_loads() {
        let schema = TableSchema {
            name: "public__documents".to_string(),
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }],
            primary_key: Some("id".to_string()),
        };

        let job = build_load_job(
            "proj",
            "dataset",
            "public__documents",
            &schema,
            "gs://bucket/path.parquet",
            "job_456",
            Some("US"),
            WriteDisposition::WriteAppend,
        );

        let JobType::Load(load) = job.configuration.job else {
            panic!("expected load job");
        };
        assert_eq!(load.write_disposition, Some(WriteDisposition::WriteAppend));
    }
}
