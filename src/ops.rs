use crate::config::{Config, SourceConfig};
use crate::destinations::bigquery::BigQueryDestination;
use crate::sources::postgres::PostgresSource;
use crate::sources::salesforce::SalesforceSource;
use crate::state::SyncState;
use crate::stats::StatsDb;
use crate::{select_sync_connections, telemetry};
use anyhow::{Context, Result};
use serde::Serialize;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Serialize)]
pub(crate) struct ReportOutput {
    state: serde_json::Value,
    recent_runs: Vec<crate::stats::RunSummary>,
}

#[derive(Serialize)]
pub(crate) struct ReconciliationOutput {
    connection_id: String,
    tables: Vec<TableReconciliationReport>,
}

#[derive(Serialize)]
pub(crate) struct TableReconciliationReport {
    source_table: String,
    destination_table: String,
    source_row_count: Option<i64>,
    destination_row_count: Option<i64>,
    deleted_rows: Option<i64>,
    source_max_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    destination_max_synced_at: Option<chrono::DateTime<chrono::Utc>>,
    count_match: Option<bool>,
    error: Option<String>,
}

pub(crate) fn reconcile_count_match(
    source_summary: &crate::sources::postgres::PostgresTableSummary,
    destination_summary: &crate::destinations::bigquery::DestinationTableSummary,
    table: &crate::sources::postgres::ResolvedPostgresTable,
) -> bool {
    let live_destination_rows = if table.soft_delete {
        destination_summary
            .row_count
            .saturating_sub(destination_summary.deleted_rows)
    } else {
        destination_summary.row_count
    };
    source_summary.row_count == live_destination_rows
}

pub(crate) async fn cmd_status(config_path: PathBuf, connection: Option<String>) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let state = SyncState::load_with_config(&cfg.state).await?;
    match connection {
        Some(connection_id) => {
            let value = serde_json::to_value(state.connections.get(&connection_id))?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        None => println!("{}", serde_json::to_string_pretty(&state)?),
    }
    Ok(())
}

pub(crate) async fn cmd_validate(
    config_path: PathBuf,
    connection_filter: Option<String>,
    verbose: bool,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();
    let connections = select_sync_connections(&cfg.connections, connection_filter.as_deref())?;

    let mut ok = true;
    for connection in connections {
        info!("validating connection {}", connection.id);
        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
                let tables = source.resolve_tables().await?;
                if verbose {
                    for table in &tables {
                        let summary = source.summarize_table(table).await?;
                        info!(table = %table.name, row_count = summary.row_count, "table summary");
                    }
                }
                if source.cdc_enabled() {
                    source.validate_cdc_publication(&tables, verbose).await?;
                }
            }
            SourceConfig::Salesforce(sf) => {
                let source = SalesforceSource::new(sf.clone(), metadata.clone())?;
                source.validate().await?;
            }
        }
        match &connection.destination {
            crate::config::DestinationConfig::BigQuery(bq) => {
                let dest = BigQueryDestination::new(bq.clone(), false, metadata.clone()).await?;
                if let Err(err) = dest.validate().await {
                    ok = false;
                    error!(connection = %connection.id, error = %err, "destination validation failed");
                }
            }
        }
    }

    if ok {
        println!("validation OK");
        Ok(())
    } else {
        anyhow::bail!("one or more validations failed");
    }
}

pub(crate) async fn cmd_report(
    config_path: PathBuf,
    connection_filter: Option<String>,
    limit: usize,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let state = SyncState::load_with_config(&cfg.state).await?;
    let stats_db = if let Some(stats_cfg) = &cfg.stats {
        Some(StatsDb::new(stats_cfg, &cfg.state.url).await?)
    } else {
        None
    };

    let recent_runs = if let Some(stats_db) = &stats_db {
        stats_db
            .recent_runs(connection_filter.as_deref(), limit)
            .await?
    } else {
        Vec::new()
    };

    let state_json = match connection_filter {
        Some(connection_id) => serde_json::to_value(state.connections.get(&connection_id))?,
        None => serde_json::to_value(state)?,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&ReportOutput {
            state: state_json,
            recent_runs,
        })?
    );
    Ok(())
}

pub(crate) async fn cmd_reconcile(
    config_path: PathBuf,
    connection_id: String,
    table_filter: Option<String>,
) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();
    let connection = cfg
        .connections
        .iter()
        .find(|c| c.id == connection_id)
        .context("connection not found")?;

    let report = match (&connection.source, &connection.destination) {
        (SourceConfig::Postgres(pg), crate::config::DestinationConfig::BigQuery(bq)) => {
            let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
            let tables = source.resolve_tables().await?;
            let dest = BigQueryDestination::new(bq.clone(), false, metadata.clone()).await?;
            dest.validate().await?;

            let tables = match &table_filter {
                Some(filter) => tables
                    .into_iter()
                    .filter(|table| table.name == *filter)
                    .collect::<Vec<_>>(),
                None => tables,
            };

            let mut rows = Vec::with_capacity(tables.len());
            for table in tables {
                let destination_table = crate::types::destination_table_name(&table.name);
                let report = match tokio::join!(
                    source.summarize_table(&table),
                    dest.summarize_table(&destination_table),
                ) {
                    (Ok(source_summary), Ok(dest_summary)) => TableReconciliationReport {
                        source_table: table.name.clone(),
                        destination_table,
                        source_row_count: Some(source_summary.row_count),
                        destination_row_count: Some(dest_summary.row_count),
                        deleted_rows: Some(dest_summary.deleted_rows),
                        source_max_updated_at: source_summary.max_updated_at,
                        destination_max_synced_at: dest_summary.max_synced_at,
                        count_match: Some(reconcile_count_match(
                            &source_summary,
                            &dest_summary,
                            &table,
                        )),
                        error: None,
                    },
                    (source_result, dest_result) => TableReconciliationReport {
                        source_table: table.name.clone(),
                        destination_table,
                        source_row_count: None,
                        destination_row_count: None,
                        deleted_rows: None,
                        source_max_updated_at: None,
                        destination_max_synced_at: None,
                        count_match: None,
                        error: Some(format!(
                            "source={:?} destination={:?}",
                            source_result.err(),
                            dest_result.err()
                        )),
                    },
                };
                if let Some(count_match) = report.count_match {
                    telemetry::record_reconcile_table(&connection_id, count_match);
                }
                rows.push(report);
            }

            ReconciliationOutput {
                connection_id: connection.id.clone(),
                tables: rows,
            }
        }
        _ => anyhow::bail!("reconcile currently supports postgres -> bigquery only"),
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
