mod admin_api;
mod commands;
mod config;
mod destinations;
mod dotenv;
#[cfg(test)]
mod main_tests;
#[cfg(feature = "monitor-ui")]
mod monitor;
mod ops;
mod retry;
mod runner;
mod sources;
mod state;
mod stats;
mod supervisor;
mod telemetry;
mod tls;
mod types;

use crate::config::{Config, DestinationConfig, SourceConfig};
use crate::destinations::Destination;
use crate::destinations::bigquery::BigQueryDestination;
use crate::runner::{
    ConnectionRestartRegistry, ShutdownController, ShutdownSignal, schedule_interval,
};
use crate::state::{ConnectionState, SyncStateStore};
use crate::stats::{StatsDb, StatsHandle};
use crate::types::{SyncMode, TableCheckpoint};
use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use futures::stream::{FuturesUnordered, StreamExt};
use jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER as JWT_CRYPTO_PROVIDER;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use commands::*;
use supervisor::*;

const LIVE_STATS_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(name = "cdsync", version, about = "CDSync - Open-source data sync")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Init {
        #[arg(long)]
        config: PathBuf,
    },
    Run {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        full: bool,
        #[arg(long)]
        incremental: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        schema_diff: bool,
        #[arg(long)]
        follow: bool,
    },
    Migrate {
        #[arg(long)]
        config: PathBuf,
    },
    Status {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
    },
    Validate {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        verbose: bool,
    },
    Report {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    Reconcile {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        connection: String,
        #[arg(long)]
        table: Option<String>,
    },
    #[cfg(feature = "monitor-ui")]
    Monitor(monitor::MonitorArgs),
}

struct SyncConnectionRequest<'a> {
    connection: &'a crate::config::ConnectionConfig,
    state: &'a mut ConnectionState,
    state_handle: crate::state::StateHandle,
    metadata: crate::types::MetadataColumns,
    mode: SyncMode,
    dry_run: bool,
    follow: bool,
    default_batch_size: usize,
    max_retries: u32,
    max_concurrency: usize,
    retry_backoff_ms: u64,
    schema_diff_enabled: bool,
    run_id: Option<String>,
    stats: Option<StatsHandle>,
    shutdown: Option<ShutdownSignal>,
}

struct RunCommandRequest {
    config_path: PathBuf,
    connection_filter: Option<String>,
    once: bool,
    full: bool,
    incremental: bool,
    dry_run: bool,
    schema_diff_enabled: bool,
    follow: bool,
    shutdown: Option<ShutdownSignal>,
}

struct StatsFlushHandle {
    stop_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tls::install_rustls_provider();
    let _ = JWT_CRYPTO_PROVIDER.install_default();
    dotenv::load_dotenv()?;
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { config } => cmd_init(config).await,
        Commands::Run {
            config,
            connection,
            once,
            full,
            incremental,
            dry_run,
            schema_diff,
            follow,
        } => {
            Box::pin(cmd_run(RunCommandRequest {
                config_path: config,
                connection_filter: connection,
                once,
                full,
                incremental,
                dry_run,
                schema_diff_enabled: schema_diff,
                follow,
                shutdown: None,
            }))
            .await
        }
        Commands::Migrate { config } => cmd_migrate(config).await,
        Commands::Status { config, connection } => ops::cmd_status(config, connection).await,
        Commands::Validate {
            config,
            connection,
            verbose,
        } => ops::cmd_validate(config, connection, verbose).await,
        Commands::Report {
            config,
            connection,
            limit,
        } => ops::cmd_report(config, connection, limit).await,
        Commands::Reconcile {
            config,
            connection,
            table,
        } => ops::cmd_reconcile(config, connection, table).await,
        #[cfg(feature = "monitor-ui")]
        Commands::Monitor(args) => monitor::cmd_monitor(args).await,
    }
}
