use super::*;

pub(super) fn config_hash(cfg: &Config) -> anyhow::Result<String> {
    let mut value = serde_json::to_value(cfg).context("serializing config for hashing")?;
    canonicalize_json_value(&mut value);
    let bytes = serde_json::to_vec(&value).context("serializing canonical config for hashing")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(super) fn scrub_config(cfg: &Config) -> ScrubbedConfig {
    ScrubbedConfig {
        state: ScrubbedStateConfig {
            url: scrub_url(&cfg.state.url),
            schema: cfg.state.schema.clone(),
        },
        metadata: cfg.metadata.clone(),
        logging: cfg.logging.clone(),
        admin_api: cfg.admin_api.as_ref().map(scrub_admin_api_config),
        observability: cfg.observability.as_ref().map(scrub_observability_config),
        sync: cfg.sync.clone(),
        stats: cfg.stats.as_ref().map(|stats| ScrubbedStatsConfig {
            url: stats.url.as_deref().map(scrub_url),
            schema: stats.schema.clone(),
        }),
        connections: cfg.connections.iter().map(scrub_connection).collect(),
    }
}

pub(super) fn scrub_admin_api_config(admin_api: &AdminApiConfig) -> ScrubbedAdminApiConfig {
    ScrubbedAdminApiConfig {
        enabled: admin_api.enabled,
        bind: admin_api.bind.clone(),
        auth: admin_api
            .auth
            .as_ref()
            .map(|auth| ScrubbedAdminApiAuthConfig {
                service_jwt_allowed_issuers: auth.resolved_allowed_issuers(),
                service_jwt_allowed_audiences: auth.resolved_allowed_audiences(),
                required_scopes: auth.resolved_required_scopes(),
            }),
    }
}

pub(super) fn scrub_observability_config(obs: &ObservabilityConfig) -> ScrubbedObservabilityConfig {
    ScrubbedObservabilityConfig {
        service_name: obs.service_name.clone(),
        otlp_traces_endpoint: obs.otlp_traces_endpoint.clone(),
        otlp_metrics_endpoint: obs.otlp_metrics_endpoint.clone(),
        otlp_headers: obs.otlp_headers.as_ref().map(|headers| {
            headers
                .keys()
                .map(|key| (key.clone(), "***".to_string()))
                .collect()
        }),
        metrics_interval_seconds: obs.metrics_interval_seconds,
    }
}

pub(super) fn scrub_connection(connection: &ConnectionConfig) -> ScrubbedConnectionConfig {
    ScrubbedConnectionConfig {
        id: connection.id.clone(),
        enabled: connection.enabled,
        source: match &connection.source {
            SourceConfig::Postgres(pg) => ScrubbedSourceConfig::Postgres(scrub_postgres_config(pg)),
        },
        destination: match &connection.destination {
            DestinationConfig::BigQuery(bq) => {
                ScrubbedDestinationConfig::BigQuery(scrub_bigquery_config(bq))
            }
        },
        schedule: connection.schedule.clone(),
    }
}

pub(super) fn scrub_postgres_config(pg: &PostgresConfig) -> ScrubbedPostgresConfig {
    ScrubbedPostgresConfig {
        url: scrub_url(&pg.url),
        tables: pg.tables.clone(),
        table_selection: pg.table_selection.clone(),
        batch_size: pg.batch_size,
        cdc: pg.cdc,
        publication: pg.publication.clone(),
        publication_mode: pg.publication_mode.clone(),
        schema_changes: pg.schema_changes.clone(),
        cdc_pipeline_id: pg.cdc_pipeline_id,
        cdc_batch_size: pg.cdc_batch_size,
        cdc_apply_concurrency: pg.cdc_apply_concurrency,
        cdc_max_fill_ms: pg.cdc_max_fill_ms,
        cdc_max_pending_events: pg.cdc_max_pending_events,
        cdc_idle_timeout_seconds: pg.cdc_idle_timeout_seconds,
        cdc_tls: pg.cdc_tls,
        cdc_tls_ca_path: pg.cdc_tls_ca_path.clone(),
        cdc_tls_ca: pg.cdc_tls_ca.as_ref().map(|_| "***".to_string()),
    }
}

pub(super) fn scrub_bigquery_config(bq: &BigQueryConfig) -> ScrubbedBigQueryConfig {
    ScrubbedBigQueryConfig {
        project_id: bq.project_id.clone(),
        dataset: bq.dataset.clone(),
        location: bq.location.clone(),
        service_account_key_path: bq.service_account_key_path.clone(),
        partition_by_synced_at: bq.partition_by_synced_at,
        batch_load_bucket: bq.batch_load_bucket.clone(),
        batch_load_prefix: bq.batch_load_prefix.clone(),
        emulator_http: bq.emulator_http.clone(),
        emulator_grpc: bq.emulator_grpc.clone(),
    }
}

pub(super) fn source_kind(source: &crate::config::SourceConfig) -> &'static str {
    match source {
        crate::config::SourceConfig::Postgres(_) => "postgres",
    }
}

pub(super) fn destination_kind(destination: &crate::config::DestinationConfig) -> &'static str {
    match destination {
        crate::config::DestinationConfig::BigQuery(_) => "bigquery",
    }
}

pub(super) fn scrub_url(raw: &str) -> String {
    if let Ok(mut url) = Url::parse(raw) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        return url.to_string();
    }
    raw.to_string()
}

pub(super) fn canonicalize_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = std::mem::take(object).into_iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (_, child) in &mut entries {
                canonicalize_json_value(child);
            }
            object.extend(entries);
        }
        serde_json::Value::Array(values) => {
            for child in values {
                canonicalize_json_value(child);
            }
        }
        _ => {}
    }
}
