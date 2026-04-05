use super::*;

pub(crate) fn connection_run_mode(connection: &crate::config::ConnectionConfig) -> &'static str {
    match (&connection.source, &connection.destination) {
        (SourceConfig::Postgres(pg), DestinationConfig::BigQuery(_)) if pg.cdc.unwrap_or(true) => {
            "cdc_follow"
        }
        _ => "scheduled_polling",
    }
}

pub(crate) async fn run_connection_worker(
    config_path: PathBuf,
    connection: crate::config::ConnectionConfig,
    shutdown_signal: ShutdownSignal,
    restart_rx: Option<watch::Receiver<u64>>,
) -> Result<()> {
    let mode = connection_run_mode(&connection);
    telemetry::record_connection_worker_event(&connection.id, mode, "started");
    match (&connection.source, &connection.destination) {
        (SourceConfig::Postgres(pg), DestinationConfig::BigQuery(_)) if pg.cdc.unwrap_or(true) => {
            let cfg = Config::load(&config_path).await?;
            let checkpoint_age_task =
                spawn_checkpoint_age_reporter(&cfg, &connection, shutdown_signal.clone()).await?;
            let restart_rx = restart_rx;
            let mut restart_generation = restart_rx
                .as_ref()
                .map(|receiver| *receiver.borrow())
                .unwrap_or_default();
            let result = loop {
                if shutdown_signal.is_shutdown() {
                    break Ok(());
                }
                let (run_shutdown_controller, run_shutdown_signal) = ShutdownController::new();
                let relay = tokio::spawn({
                    let mut shutdown = shutdown_signal.clone();
                    let mut restart_wait = restart_rx.clone();
                    async move {
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed {
                                    run_shutdown_controller.shutdown();
                                }
                            }
                            () = async {
                                if let Some(receiver) = &mut restart_wait {
                                    let _ = receiver.changed().await;
                                } else {
                                    std::future::pending::<()>().await;
                                }
                            } => {
                                run_shutdown_controller.shutdown();
                            }
                        }
                    }
                });

                let result = cmd_run_once(RunCommandRequest {
                    config_path: config_path.clone(),
                    connection_filter: Some(connection.id.clone()),
                    once: true,
                    full: false,
                    incremental: true,
                    dry_run: false,
                    schema_diff_enabled: false,
                    follow: true,
                    shutdown: Some(run_shutdown_signal),
                })
                .await;

                relay.abort();
                let _ = relay.await;

                if shutdown_signal.is_shutdown() {
                    break Ok(());
                }
                if let Some(receiver) = restart_rx.as_ref() {
                    let current = *receiver.borrow();
                    if current != restart_generation {
                        restart_generation = current;
                        info!(
                            connection = %connection.id,
                            restart_generation,
                            "restarting postgres CDC worker to apply runtime control request"
                        );
                        continue;
                    }
                }
                break result;
            };
            if let Some(task) = checkpoint_age_task {
                task.abort();
                let _ = task.await;
            }
            telemetry::record_connection_worker_event(
                &connection.id,
                mode,
                if result.is_ok() {
                    "succeeded"
                } else {
                    "failed"
                },
            );
            result
        }
        _ => {
            let interval = schedule_interval(&connection)?;
            loop {
                if shutdown_signal.is_shutdown() {
                    telemetry::record_connection_worker_event(&connection.id, mode, "stopped");
                    break;
                }
                let result = cmd_run_once(RunCommandRequest {
                    config_path: config_path.clone(),
                    connection_filter: Some(connection.id.clone()),
                    once: true,
                    full: false,
                    incremental: true,
                    dry_run: false,
                    schema_diff_enabled: false,
                    follow: false,
                    shutdown: Some(shutdown_signal.clone()),
                })
                .await;
                if result.is_err() {
                    telemetry::record_connection_worker_event(&connection.id, mode, "failed");
                }
                result?;
                if wait_backoff(interval, Some(shutdown_signal.clone())).await {
                    telemetry::record_connection_worker_event(&connection.id, mode, "stopped");
                    break;
                }
            }
            telemetry::record_connection_worker_event(&connection.id, mode, "succeeded");
            Ok(())
        }
    }
}

pub(crate) fn run_supervisor_mode(
    selected_connections: &[&crate::config::ConnectionConfig],
) -> String {
    if selected_connections.len() == 1 {
        connection_run_mode(selected_connections[0]).to_string()
    } else {
        "multi_connection".to_string()
    }
}

pub(crate) fn run_connection_label(
    selected_connections: &[&crate::config::ConnectionConfig],
) -> String {
    if selected_connections.len() == 1 {
        selected_connections[0].id.clone()
    } else {
        "all".to_string()
    }
}

pub(crate) fn connection_source_kind(
    connection: &crate::config::ConnectionConfig,
) -> &'static str {
    match &connection.source {
        SourceConfig::Postgres(_) => "postgres",
        SourceConfig::Salesforce(_) => "salesforce",
    }
}

fn configured_entity_names(
    connection: &crate::config::ConnectionConfig,
) -> std::collections::BTreeSet<String> {
    match &connection.source {
        SourceConfig::Postgres(pg) => pg
            .tables
            .as_ref()
            .map(|tables| tables.iter().map(|table| table.name.clone()).collect())
            .unwrap_or_default(),
        SourceConfig::Salesforce(sf) => sf
            .objects
            .as_ref()
            .map(|objects| objects.iter().map(|object| object.name.clone()).collect())
            .unwrap_or_default(),
    }
}

fn connection_checkpoint_age_seconds(
    sync_state: &crate::state::SyncState,
    connection: &crate::config::ConnectionConfig,
    now: chrono::DateTime<Utc>,
) -> Option<u64> {
    sync_state
        .connections
        .get(&connection.id)
        .and_then(|state| max_checkpoint_age_seconds(state, connection, now))
}

pub(crate) fn max_checkpoint_age_seconds(
    state: &ConnectionState,
    connection: &crate::config::ConnectionConfig,
    now: chrono::DateTime<Utc>,
) -> Option<u64> {
    let configured = configured_entity_names(connection);
    let checkpoints = match &connection.source {
        SourceConfig::Postgres(_) => state.postgres.iter().collect::<Vec<_>>(),
        SourceConfig::Salesforce(_) => state.salesforce.iter().collect::<Vec<_>>(),
    };
    checkpoints
        .into_iter()
        .filter(|(entity_name, _)| configured.is_empty() || configured.contains(*entity_name))
        .filter_map(|(_, checkpoint)| checkpoint.last_synced_at)
        .map(|last_synced_at| (now - last_synced_at).num_seconds().max(0) as u64)
        .max()
}

pub(crate) async fn spawn_checkpoint_age_reporter(
    cfg: &Config,
    connection: &crate::config::ConnectionConfig,
    shutdown_signal: ShutdownSignal,
) -> Result<Option<JoinHandle<()>>> {
    if !matches!(
        (&connection.source, &connection.destination),
        (SourceConfig::Postgres(pg), DestinationConfig::BigQuery(_)) if pg.cdc.unwrap_or(true)
    ) {
        return Ok(None);
    }

    let state_store = SyncStateStore::open_with_config(&cfg.state).await?;
    let interval = Duration::from_secs(
        cfg.observability
            .as_ref()
            .and_then(|obs| obs.metrics_interval_seconds)
            .unwrap_or(30),
    );
    let connection = connection.clone();
    let connection_id = connection.id.clone();
    let source_kind = connection_source_kind(&connection);

    Ok(Some(tokio::spawn(async move {
        run_checkpoint_age_reporter(
            interval,
            shutdown_signal,
            move || {
                let state_store = state_store.clone();
                let connection = connection.clone();
                async move {
                    let sync_state = state_store.load_state().await?;
                    Ok(connection_checkpoint_age_seconds(
                        &sync_state,
                        &connection,
                        Utc::now(),
                    ))
                }
            },
            move |age_seconds| {
                telemetry::record_connection_checkpoint_age(
                    &connection_id,
                    source_kind,
                    age_seconds,
                );
            },
        )
        .await;
    })))
}

pub(crate) async fn run_checkpoint_age_reporter<LoadAge, LoadFuture, RecordAge>(
    interval: Duration,
    shutdown_signal: ShutdownSignal,
    mut load_age: LoadAge,
    mut record_age: RecordAge,
) where
    LoadAge: FnMut() -> LoadFuture,
    LoadFuture: Future<Output = Result<Option<u64>>>,
    RecordAge: FnMut(u64),
{
    let mut shutdown_signal = shutdown_signal;
    loop {
        tokio::select! {
            () = tokio::time::sleep(interval) => {
                match load_age().await {
                    Ok(Some(age_seconds)) => record_age(age_seconds),
                    Ok(None) => {}
                    Err(err) => warn!(error = %err, "failed to record checkpoint age sample"),
                }
            }
            changed = shutdown_signal.changed() => {
                if changed {
                    break;
                }
            }
        }
    }
}

pub(crate) async fn cmd_run(request: RunCommandRequest) -> Result<()> {
    let RunCommandRequest {
        config_path,
        connection_filter,
        once,
        full,
        incremental,
        dry_run,
        schema_diff_enabled,
        follow,
        shutdown,
    } = request;
    if once {
        return cmd_run_once(RunCommandRequest {
            config_path,
            connection_filter,
            once,
            full,
            incremental,
            dry_run,
            schema_diff_enabled,
            follow,
            shutdown,
        })
        .await;
    }
    if full || incremental || dry_run || schema_diff_enabled || follow || shutdown.is_some() {
        anyhow::bail!(
            "--full, --incremental, --dry-run, --schema-diff, and --follow require --once"
        );
    }

    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let selected_connections =
        select_sync_connections(&cfg.connections, connection_filter.as_deref())?;
    let selected_connection_configs: Vec<crate::config::ConnectionConfig> =
        selected_connections.iter().map(|connection| (*connection).clone()).collect();
    let restart_registry = Arc::new(ConnectionRestartRegistry::new(&selected_connection_configs));

    let (shutdown_controller, shutdown_signal) = ShutdownController::new();
    let signal_controller = shutdown_controller.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_termination_signal().await;
        signal_controller.shutdown();
    });

    let mode = run_supervisor_mode(&selected_connections);
    let connection_label = run_connection_label(&selected_connections);
    let managed_connection_ids: Vec<String> = selected_connections
        .iter()
        .map(|connection| connection.id.clone())
        .collect();
    let admin_task = admin_api::spawn_admin_api(
        &cfg,
        &connection_label,
        &managed_connection_ids,
        selected_connections.len(),
        &mode,
        Some(restart_registry.clone()),
        shutdown_signal.clone(),
    )
    .await?;

    let result = if selected_connections.len() == 1 {
        run_connection_worker(
            config_path,
            (*selected_connections[0]).clone(),
            shutdown_signal.clone(),
            restart_registry.subscribe(&selected_connections[0].id),
        )
        .await
    } else {
        let mut workers = FuturesUnordered::new();
        for connection in selected_connections {
            workers.push(tokio::spawn(run_connection_worker(
                config_path.clone(),
                (*connection).clone(),
                shutdown_signal.clone(),
                restart_registry.subscribe(&connection.id),
            )));
        }

        let mut first_error: Option<anyhow::Error> = None;
        while let Some(result) = workers.next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                    shutdown_controller.shutdown();
                }
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(anyhow::Error::new(err));
                    }
                    shutdown_controller.shutdown();
                }
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    };
    shutdown_controller.shutdown();
    signal_task.abort();
    if let Some(task) = admin_task {
        let _ = task.join();
    }
    result
}

pub(crate) async fn wait_backoff(duration: Duration, shutdown: Option<ShutdownSignal>) -> bool {
    if let Some(mut shutdown) = shutdown {
        tokio::select! {
            () = tokio::time::sleep(duration) => false,
            changed = shutdown.changed() => changed,
        }
    } else {
        tokio::time::sleep(duration).await;
        false
    }
}

pub(crate) async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("installing SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
