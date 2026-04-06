use crate::config::{LoggingConfig, ObservabilityConfig};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::metrics as sdkmetrics;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader as AsyncPeriodicReader;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::{Resource, runtime, trace as sdktrace};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use tracing::warn;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone, Debug)]
struct ReqwestHttpClient {
    inner: reqwest::Client,
}

#[async_trait::async_trait]
impl HttpClient for ReqwestHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let request = request.try_into()?;
        let mut response = self.inner.execute(request).await?.error_for_status()?;
        let headers = std::mem::take(response.headers_mut());
        let mut http_response = Response::builder()
            .status(response.status())
            .body(response.bytes().await?)?;
        *http_response.headers_mut() = headers;
        Ok(http_response)
    }
}

pub struct TelemetryGuard {
    tracer_provider: Option<sdktrace::SdkTracerProvider>,
    meter_provider: Option<sdkmetrics::SdkMeterProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

struct ReplicationMetrics {
    rows_read_total: Counter<u64>,
    rows_written_total: Counter<u64>,
    checkpoint_saves_total: Counter<u64>,
    bigquery_row_errors_total: Counter<u64>,
    retry_attempts_total: Counter<u64>,
    connection_worker_events_total: Counter<u64>,
    connection_checkpoint_age_seconds: Histogram<u64>,
    cdc_pending_events: Histogram<u64>,
    cdc_commit_queue_depth: Histogram<u64>,
    cdc_inflight_commits: Histogram<u64>,
    cdc_backpressure_waits_total: Counter<u64>,
    reconcile_tables_total: Counter<u64>,
    reconcile_mismatches_total: Counter<u64>,
    sync_runs_total: Counter<u64>,
    sync_run_duration_ms: Histogram<f64>,
    cdc_batch_load_jobs_total: Counter<u64>,
    cdc_batch_load_job_duration_ms: Histogram<f64>,
    cdc_batch_load_stage_duration_ms: Histogram<f64>,
}

static METRICS: OnceLock<ReplicationMetrics> = OnceLock::new();
static TELEMETRY_INITIALIZED: OnceLock<()> = OnceLock::new();

pub fn init(
    logging: Option<&LoggingConfig>,
    observability: Option<&ObservabilityConfig>,
) -> anyhow::Result<TelemetryGuard> {
    if TELEMETRY_INITIALIZED.get().is_some() {
        return Ok(TelemetryGuard {
            tracer_provider: None,
            meter_provider: None,
        });
    }

    let level = logging
        .and_then(|l| l.level.clone())
        .unwrap_or_else(|| "info".to_string());
    let json = logging.and_then(|l| l.json).unwrap_or(false);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    let service_name = observability
        .and_then(|obs| obs.service_name.clone())
        .unwrap_or_else(|| "cdsync".to_string());
    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .build();

    let (tracer_provider, trace_errors) = init_trace_provider(observability, &resource)?;
    let (meter_provider, metric_errors) = init_metrics_provider(observability, &resource)?;

    let trace_layer = tracer_provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer(service_name)));
    let registry = tracing_subscriber::registry().with(trace_layer);
    if json {
        registry
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_filter(filter.clone()),
            )
            .try_init()?;
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().with_filter(filter.clone()))
            .try_init()?;
    }

    for error in trace_errors.into_iter().chain(metric_errors) {
        warn!(error = %error, "telemetry exporter failed to initialize");
    }
    let _ = TELEMETRY_INITIALIZED.set(());

    Ok(TelemetryGuard {
        tracer_provider,
        meter_provider,
    })
}

pub fn record_rows_read(table: &str, rows: u64) {
    let metrics = metrics();
    metrics
        .rows_read_total
        .add(rows, &[KeyValue::new("table", table.to_string())]);
}

pub fn record_rows_written(table: &str, rows: u64) {
    let metrics = metrics();
    metrics
        .rows_written_total
        .add(rows, &[KeyValue::new("table", table.to_string())]);
}

pub fn record_checkpoint_save(connection_id: &str, scope: &str) {
    let metrics = metrics();
    metrics.checkpoint_saves_total.add(
        1,
        &[
            KeyValue::new("connection_id", connection_id.to_string()),
            KeyValue::new("scope", scope.to_string()),
        ],
    );
}

pub fn record_bigquery_row_errors(table: &str, rows: u64) {
    let metrics = metrics();
    metrics
        .bigquery_row_errors_total
        .add(rows, &[KeyValue::new("table", table.to_string())]);
}

pub fn record_retry_attempt(connection_id: &str, scope: &str) {
    let metrics = metrics();
    metrics.retry_attempts_total.add(
        1,
        &[
            KeyValue::new("connection_id", connection_id.to_string()),
            KeyValue::new("scope", scope.to_string()),
        ],
    );
}

pub fn record_connection_worker_event(connection_id: &str, mode: &str, event: &str) {
    let metrics = metrics();
    metrics.connection_worker_events_total.add(
        1,
        &[
            KeyValue::new("connection_id", connection_id.to_string()),
            KeyValue::new("mode", mode.to_string()),
            KeyValue::new("event", event.to_string()),
        ],
    );
}

pub fn record_connection_checkpoint_age(connection_id: &str, source_kind: &str, age_seconds: u64) {
    let metrics = metrics();
    metrics.connection_checkpoint_age_seconds.record(
        age_seconds,
        &[
            KeyValue::new("connection_id", connection_id.to_string()),
            KeyValue::new("source_kind", source_kind.to_string()),
        ],
    );
}

pub fn record_cdc_pipeline_depths(
    slot_name: &str,
    pending_events: u64,
    commit_queue_depth: u64,
    inflight_commits: u64,
) {
    let metrics = metrics();
    let attrs = [KeyValue::new("slot_name", slot_name.to_string())];
    metrics.cdc_pending_events.record(pending_events, &attrs);
    metrics
        .cdc_commit_queue_depth
        .record(commit_queue_depth, &attrs);
    metrics
        .cdc_inflight_commits
        .record(inflight_commits, &attrs);
}

pub fn record_cdc_backpressure_wait(slot_name: &str, queue_depth: u64, queue_capacity: u64) {
    let metrics = metrics();
    metrics.cdc_backpressure_waits_total.add(
        1,
        &[
            KeyValue::new("slot_name", slot_name.to_string()),
            KeyValue::new("queue_depth", queue_depth as i64),
            KeyValue::new("queue_capacity", queue_capacity as i64),
        ],
    );
}

pub fn record_reconcile_table(connection_id: &str, matched: bool) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("connection_id", connection_id.to_string()),
        KeyValue::new("matched", matched.to_string()),
    ];
    metrics.reconcile_tables_total.add(1, &attrs);
    if !matched {
        metrics.reconcile_mismatches_total.add(1, &attrs);
    }
}

pub fn record_sync_run(connection_id: &str, status: &str, duration_ms: f64) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("connection_id", connection_id.to_string()),
        KeyValue::new("status", status.to_string()),
    ];
    metrics.sync_runs_total.add(1, &attrs);
    if duration_ms.is_finite() && duration_ms >= 0.0 {
        metrics.sync_run_duration_ms.record(duration_ms, &attrs);
    }
}

pub fn record_cdc_batch_load_job(connection_id: &str, table: &str, status: &str, duration_ms: f64) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("connection_id", connection_id.to_string()),
        KeyValue::new("table", table.to_string()),
        KeyValue::new("status", status.to_string()),
    ];
    metrics.cdc_batch_load_jobs_total.add(1, &attrs);
    if duration_ms.is_finite() && duration_ms >= 0.0 {
        metrics
            .cdc_batch_load_job_duration_ms
            .record(duration_ms, &attrs);
    }
}

pub fn record_cdc_batch_load_stage_duration(
    connection_id: &str,
    table: &str,
    stage: &str,
    duration_ms: f64,
    rows: u64,
) {
    let metrics = metrics();
    let attrs = [
        KeyValue::new("connection_id", connection_id.to_string()),
        KeyValue::new("table", table.to_string()),
        KeyValue::new("stage", stage.to_string()),
        KeyValue::new("rows", rows as i64),
    ];
    if duration_ms.is_finite() && duration_ms >= 0.0 {
        metrics
            .cdc_batch_load_stage_duration_ms
            .record(duration_ms, &attrs);
    }
}

fn metrics() -> &'static ReplicationMetrics {
    METRICS.get_or_init(|| {
        let meter: Meter = global::meter("cdsync");
        ReplicationMetrics {
            rows_read_total: meter.u64_counter("cdsync_rows_read_total").build(),
            rows_written_total: meter.u64_counter("cdsync_rows_written_total").build(),
            checkpoint_saves_total: meter.u64_counter("cdsync_checkpoint_saves_total").build(),
            bigquery_row_errors_total: meter
                .u64_counter("cdsync_bigquery_row_errors_total")
                .build(),
            retry_attempts_total: meter.u64_counter("cdsync_retry_attempts_total").build(),
            connection_worker_events_total: meter
                .u64_counter("cdsync_connection_worker_events_total")
                .build(),
            connection_checkpoint_age_seconds: meter
                .u64_histogram("cdsync_connection_checkpoint_age_seconds")
                .build(),
            cdc_pending_events: meter.u64_histogram("cdsync_cdc_pending_events").build(),
            cdc_commit_queue_depth: meter.u64_histogram("cdsync_cdc_commit_queue_depth").build(),
            cdc_inflight_commits: meter.u64_histogram("cdsync_cdc_inflight_commits").build(),
            cdc_backpressure_waits_total: meter
                .u64_counter("cdsync_cdc_backpressure_waits_total")
                .build(),
            reconcile_tables_total: meter.u64_counter("cdsync_reconcile_tables_total").build(),
            reconcile_mismatches_total: meter
                .u64_counter("cdsync_reconcile_mismatches_total")
                .build(),
            sync_runs_total: meter.u64_counter("cdsync_sync_runs_total").build(),
            sync_run_duration_ms: meter.f64_histogram("cdsync_sync_run_duration_ms").build(),
            cdc_batch_load_jobs_total: meter
                .u64_counter("cdsync_cdc_batch_load_jobs_total")
                .build(),
            cdc_batch_load_job_duration_ms: meter
                .f64_histogram("cdsync_cdc_batch_load_job_duration_ms")
                .build(),
            cdc_batch_load_stage_duration_ms: meter
                .f64_histogram("cdsync_cdc_batch_load_stage_duration_ms")
                .build(),
        }
    })
}

fn init_trace_provider(
    observability: Option<&ObservabilityConfig>,
    resource: &Resource,
) -> anyhow::Result<(Option<sdktrace::SdkTracerProvider>, Vec<String>)> {
    let Some(endpoint) = observability.and_then(|obs| obs.otlp_traces_endpoint.as_deref()) else {
        return Ok((None, Vec::new()));
    };
    let exporter = match build_span_exporter(endpoint, headers(observability)) {
        Ok(exporter) => exporter,
        Err(err) => return Ok((None, vec![err])),
    };

    let provider = sdktrace::SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_span_processor(BatchSpanProcessor::builder(exporter, runtime::Tokio).build())
        .build();
    global::set_tracer_provider(provider.clone());
    Ok((Some(provider), Vec::new()))
}

fn init_metrics_provider(
    observability: Option<&ObservabilityConfig>,
    resource: &Resource,
) -> anyhow::Result<(Option<sdkmetrics::SdkMeterProvider>, Vec<String>)> {
    let Some(endpoint) = observability.and_then(|obs| obs.otlp_metrics_endpoint.as_deref()) else {
        return Ok((None, Vec::new()));
    };
    let exporter = match build_metric_exporter(endpoint, headers(observability)) {
        Ok(exporter) => exporter,
        Err(err) => return Ok((None, vec![err])),
    };

    let interval = Duration::from_secs(
        observability
            .and_then(|obs| obs.metrics_interval_seconds)
            .unwrap_or(30),
    );
    let reader = AsyncPeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(interval)
        .build();
    let provider = sdkmetrics::SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build();
    global::set_meter_provider(provider.clone());
    Ok((Some(provider), Vec::new()))
}

fn build_span_exporter(
    endpoint: &str,
    headers: HashMap<String, String>,
) -> Result<SpanExporter, String> {
    let mut exporter_builder = SpanExporter::builder().with_http().with_endpoint(endpoint);
    if !headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(headers);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| err.to_string())?;
    exporter_builder
        .with_http_client(ReqwestHttpClient { inner: client })
        .build()
        .map_err(|err| err.to_string())
}

fn build_metric_exporter(
    endpoint: &str,
    headers: HashMap<String, String>,
) -> Result<MetricExporter, String> {
    let mut exporter_builder = MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoint);
    if !headers.is_empty() {
        exporter_builder = exporter_builder.with_headers(headers);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|err| err.to_string())?;
    exporter_builder
        .with_http_client(ReqwestHttpClient { inner: client })
        .build()
        .map_err(|err| err.to_string())
}

fn headers(observability: Option<&ObservabilityConfig>) -> HashMap<String, String> {
    observability
        .and_then(|obs| obs.otlp_headers.clone())
        .unwrap_or_default()
}
