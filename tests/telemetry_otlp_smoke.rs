use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use cdsync::state::{CdcBatchLoadQueueSummary, CdcCoordinatorSummary};
use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{KeyValue as ProtoKeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_sdk::metrics as sdkmetrics;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader as AsyncPeriodicReader;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace as sdktrace;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use prost::Message;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn telemetry_emits_otlp_metrics_and_spans() -> Result<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/traces"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/metrics"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let headers = HashMap::from([("x-cdsync-test".to_string(), "smoke".to_string())]);
    let trace_exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", server.uri()))
        .with_headers(headers.clone())
        .build()
        .map_err(|err| anyhow::anyhow!("building trace exporter failed: {err}"))?;
    let tracer_provider = sdktrace::SdkTracerProvider::builder()
        .with_span_processor(BatchSpanProcessor::builder(trace_exporter, runtime::Tokio).build())
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/metrics", server.uri()))
        .with_headers(headers)
        .build()
        .map_err(|err| anyhow::anyhow!("building metric exporter failed: {err}"))?;
    let reader = AsyncPeriodicReader::builder(metric_exporter, runtime::Tokio)
        .with_interval(Duration::from_secs(1))
        .build();
    let meter_provider = sdkmetrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    cdsync::telemetry::record_cdc_batch_load_queue_summary(
        "app",
        &CdcBatchLoadQueueSummary {
            pending_jobs: 3,
            running_jobs: 1,
            failed_jobs: 2,
            jobs_per_minute: 5,
            rows_per_minute: 21,
            oldest_pending_age_seconds: Some(9),
            oldest_running_age_seconds: Some(4),
            ..Default::default()
        },
    );
    cdsync::telemetry::record_cdc_coordinator_summary(
        "app",
        &CdcCoordinatorSummary {
            next_sequence_to_ack: 41,
            last_enqueued_sequence: Some(44),
            pending_fragments: 7,
            failed_fragments: 1,
            oldest_pending_age_seconds: Some(8),
            wal_bytes_unattributed_or_idle: Some(77),
            ..Default::default()
        },
    );
    cdsync::telemetry::record_cdc_batch_load_job("app", "public__accounts", "succeeded", 123.0);
    cdsync::telemetry::record_cdc_batch_load_stage_duration(
        "app",
        "public__accounts",
        "merge",
        45.0,
        11,
    );

    let tracer = tracer_provider.tracer("telemetry-otlp-smoke");
    let mut root = tracer
        .span_builder("cdc_batch_load_job")
        .with_attributes(vec![
            KeyValue::new("connection_id", "app"),
            KeyValue::new("table", "public__accounts"),
            KeyValue::new("job_id", "job-1"),
        ])
        .start(&tracer);
    root.end();
    let mut merge = tracer
        .span_builder("cdc_batch_load_job.merge")
        .with_attributes(vec![
            KeyValue::new("connection_id", "app"),
            KeyValue::new("table", "public__accounts"),
            KeyValue::new("stage", "merge"),
        ])
        .start(&tracer);
    merge.end();

    tracer_provider.force_flush()?;
    meter_provider.force_flush()?;
    tracer_provider.shutdown()?;
    meter_provider.shutdown()?;

    let requests = wait_for_requests(&server, 2).await?;
    let trace_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/v1/traces"))
        .collect();
    let metric_requests: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path().ends_with("/v1/metrics"))
        .collect();

    anyhow::ensure!(!trace_requests.is_empty(), "expected OTLP trace export request");
    anyhow::ensure!(!metric_requests.is_empty(), "expected OTLP metrics export request");

    for request in &trace_requests {
        assert_eq!(
            request
                .headers
                .get("x-cdsync-test")
                .and_then(|value| value.to_str().ok()),
            Some("smoke")
        );
    }
    for request in &metric_requests {
        assert_eq!(
            request
                .headers
                .get("x-cdsync-test")
                .and_then(|value| value.to_str().ok()),
            Some("smoke")
        );
    }

    let exported_spans = collect_exported_spans(&trace_requests)?;
    assert!(exported_spans.iter().any(|span| span.name == "cdc_batch_load_job"));
    assert!(exported_spans
        .iter()
        .any(|span| span.name == "cdc_batch_load_job.merge"));
    let root_span = exported_spans
        .iter()
        .find(|span| span.name == "cdc_batch_load_job")
        .context("missing cdc_batch_load_job span")?;
    assert_eq!(
        find_string_attr(&root_span.attributes, "connection_id"),
        Some("app")
    );
    assert_eq!(
        find_string_attr(&root_span.attributes, "table"),
        Some("public__accounts")
    );

    let metrics = collect_exported_metrics(&metric_requests)?;
    assert_metric_with_connection(
        &metrics,
        "cdsync_cdc_batch_load_pending_jobs",
        "app",
    );
    assert_metric_with_connection(
        &metrics,
        "cdsync_cdc_batch_load_running_jobs",
        "app",
    );
    assert_metric_with_connection(
        &metrics,
        "cdsync_cdc_coordinator_pending_fragments",
        "app",
    );
    assert_metric_with_connection(
        &metrics,
        "cdsync_cdc_coordinator_next_sequence_to_ack",
        "app",
    );
    assert_metric_with_connection(
        &metrics,
        "cdsync_cdc_batch_load_stage_duration_ms",
        "app",
    );

    Ok(())
}

async fn wait_for_requests(
    server: &MockServer,
    min_requests: usize,
) -> Result<Vec<wiremock::Request>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let requests = server
            .received_requests()
            .await
            .context("reading mock server requests")?;
        if requests.len() >= min_requests {
            return Ok(requests);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for OTLP requests");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn collect_exported_spans(
    requests: &[&wiremock::Request],
) -> Result<Vec<opentelemetry_proto::tonic::trace::v1::Span>> {
    let mut spans = Vec::new();
    for request in requests {
        let export = ExportTraceServiceRequest::decode(request.body.as_slice())
            .context("decoding OTLP trace request")?;
        for resource in export.resource_spans {
            for scope in resource.scope_spans {
                spans.extend(scope.spans);
            }
        }
    }
    Ok(spans)
}

fn collect_exported_metrics(
    requests: &[&wiremock::Request],
) -> Result<Vec<opentelemetry_proto::tonic::metrics::v1::Metric>> {
    let mut metrics = Vec::new();
    for request in requests {
        let export = ExportMetricsServiceRequest::decode(request.body.as_slice())
            .context("decoding OTLP metrics request")?;
        for resource in export.resource_metrics {
            for scope in resource.scope_metrics {
                metrics.extend(scope.metrics);
            }
        }
    }
    Ok(metrics)
}

fn assert_metric_with_connection(
    metrics: &[opentelemetry_proto::tonic::metrics::v1::Metric],
    name: &str,
    connection_id: &str,
) {
    let metric = metrics
        .iter()
        .find(|metric| metric.name == name)
        .unwrap_or_else(|| panic!("missing metric {name}"));
    let has_attr = match metric.data.as_ref() {
        Some(MetricData::Histogram(histogram)) => histogram
            .data_points
            .iter()
            .any(|point| find_string_attr(&point.attributes, "connection_id") == Some(connection_id)),
        Some(MetricData::Sum(sum)) => sum
            .data_points
            .iter()
            .any(|point| find_string_attr(&point.attributes, "connection_id") == Some(connection_id)),
        Some(MetricData::Gauge(gauge)) => gauge
            .data_points
            .iter()
            .any(|point| find_string_attr(&point.attributes, "connection_id") == Some(connection_id)),
        other => panic!("unexpected metric data for {name}: {other:?}"),
    };
    assert!(has_attr, "metric {name} missing connection_id={connection_id}");
}

fn find_string_attr<'a>(attributes: &'a [ProtoKeyValue], key: &str) -> Option<&'a str> {
    attributes.iter().find_map(|attribute| {
        if attribute.key != key {
            return None;
        }
        match attribute.value.as_ref()?.value.as_ref()? {
            any_value::Value::StringValue(value) => Some(value.as_str()),
            _ => None,
        }
    })
}
