use super::*;
use crate::retry::{
    SyncRetryClass, classify_error_reason, classify_sync_retry, compute_sync_retry_backoff,
};

pub(crate) fn select_sync_connections<'a>(
    connections: &'a [crate::config::ConnectionConfig],
    filter: Option<&str>,
) -> Result<Vec<&'a crate::config::ConnectionConfig>> {
    if let Some(filter) = filter {
        let connection = connections
            .iter()
            .find(|connection| connection.id == filter)
            .context("connection not found")?;
        if !connection.enabled() {
            anyhow::bail!("connection {} is disabled", connection.id);
        }
        return Ok(vec![connection]);
    }

    Ok(connections
        .iter()
        .filter(|connection| connection.enabled())
        .collect())
}

pub(crate) fn spawn_stats_flush_task(db: StatsDb, handle: StatsHandle) -> StatsFlushHandle {
    spawn_stats_flush_task_with_interval(db, handle, LIVE_STATS_FLUSH_INTERVAL)
}

pub(crate) fn spawn_stats_flush_task_with_interval(
    db: StatsDb,
    handle: StatsHandle,
    interval: Duration,
) -> StatsFlushHandle {
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {
                    if let Err(err) = db.persist_run(&handle).await {
                        warn!(error = %err, "failed to persist live run stats");
                    }
                }
                _ = &mut stop_rx => {
                    break;
                }
            }
        }
    });
    StatsFlushHandle {
        stop_tx: Some(stop_tx),
        task,
    }
}

pub(crate) async fn stop_stats_flush_task(flush: Option<StatsFlushHandle>) {
    let Some(mut flush) = flush else {
        return;
    };
    if let Some(stop_tx) = flush.stop_tx.take() {
        let _ = stop_tx.send(());
    }
    if let Err(err) = flush.task.await {
        warn!(error = %err, "stats flush task join failed");
    }
}

pub(crate) async fn load_latest_postgres_checkpoint(
    state_handle: &crate::state::StateHandle,
    table_name: &str,
    fallback: &TableCheckpoint,
) -> TableCheckpoint {
    match state_handle.load_postgres_checkpoint(table_name).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => fallback.clone(),
        Err(err) => {
            warn!(
                table = %table_name,
                error = %err,
                "failed to load persisted postgres checkpoint after interrupted retry; using in-memory checkpoint"
            );
            fallback.clone()
        }
    }
}

pub(crate) async fn refresh_postgres_checkpoints_from_store(
    state_handle: &crate::state::StateHandle,
    state: &mut ConnectionState,
) {
    match state_handle.load_all_postgres_checkpoints().await {
        Ok(checkpoints) => {
            if !checkpoints.is_empty() {
                state.postgres = checkpoints;
            }
        }
        Err(err) => {
            warn!(
                error = %err,
                "failed to refresh persisted postgres checkpoints before final state save"
            );
        }
    }
}

pub(crate) async fn cmd_init(config_path: PathBuf) -> Result<()> {
    if !config_path.exists() {
        let template = Config::template(&config_path);
        tokio::fs::write(&template.path, template.content).await?;
        println!("Created config template at {}", template.path.display());
        return Ok(());
    }

    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    for connection in &cfg.connections {
        if !connection.enabled() {
            continue;
        }
        match &connection.source {
            SourceConfig::Postgres(pg) => {
                let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
                let tables = source.resolve_tables().await?;
                for table in &tables {
                    let schema = source.discover_schema(table).await?;
                    info!(
                        "postgres schema: {} ({} columns)",
                        schema.name,
                        schema.columns.len()
                    );
                }
            }
        }

        match &connection.destination {
            DestinationConfig::BigQuery(bq) => {
                let dest = BigQueryDestination::new(bq.clone(), false, metadata.clone()).await?;
                dest.validate().await?;
            }
        }
    }

    println!("Config validation complete.");
    Ok(())
}

pub(crate) async fn cmd_migrate(config_path: PathBuf) -> Result<()> {
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let stats_cfg = cfg.stats.clone().unwrap_or(crate::config::StatsConfig {
        url: None,
        schema: None,
    });
    SyncStateStore::migrate_with_config(&cfg.state, cfg.state_pool_max_connections()).await?;
    info!(schema = %cfg.state.schema_name(), "state migrations applied");
    StatsDb::migrate_with_config(&stats_cfg, &cfg.state.url).await?;
    info!(schema = %stats_cfg.schema_name(), "stats migrations applied");
    println!("Migrations complete.");
    Ok(())
}

pub(crate) async fn cmd_run_once(request: RunCommandRequest) -> Result<()> {
    let RunCommandRequest {
        config_path,
        connection_filter,
        once: _,
        full,
        incremental,
        dry_run,
        schema_diff_enabled,
        follow,
        shutdown,
    } = request;
    let cfg = Config::load(&config_path).await?;
    let _telemetry = telemetry::init(cfg.logging.as_ref(), cfg.observability.as_ref())?;
    let metadata = cfg.metadata_columns();

    if follow && connection_filter.is_none() {
        anyhow::bail!("--follow requires --connection for a single postgres CDC connection");
    }

    let state_store =
        SyncStateStore::open_with_config(&cfg.state, cfg.state_pool_max_connections()).await?;
    let mode = match (full, incremental) {
        (true, false) => SyncMode::Full,
        (false, true) => SyncMode::Incremental,
        (false, false) => SyncMode::Incremental,
        (true, true) => {
            anyhow::bail!("--full and --incremental are mutually exclusive");
        }
    };

    let mut state = state_store.load_state().await?;
    let stats_db = if let Some(stats_cfg) = &cfg.stats {
        Some(StatsDb::new(stats_cfg, &cfg.state.url).await?)
    } else {
        None
    };
    let default_batch_size = cfg
        .sync
        .as_ref()
        .and_then(|s| s.default_batch_size)
        .unwrap_or(10_000);
    let max_retries = cfg.sync.as_ref().and_then(|s| s.max_retries).unwrap_or(3);
    let max_concurrency = cfg
        .sync
        .as_ref()
        .and_then(|s| s.max_concurrency)
        .unwrap_or(1)
        .max(1);
    let retry_backoff_ms = cfg
        .sync
        .as_ref()
        .and_then(|s| s.retry_backoff_ms)
        .unwrap_or(1000);
    let selected_connections =
        select_sync_connections(&cfg.connections, connection_filter.as_deref())?;

    for connection in selected_connections {
        let connection_state = state
            .connections
            .entry(connection.id.clone())
            .or_insert_with(ConnectionState::default);
        let lease = state_store.acquire_connection_lock(&connection.id).await?;
        let state_handle = state_store.handle(&connection.id);
        connection_state.last_sync_started_at = Some(Utc::now());
        connection_state.last_sync_status = Some("running".to_string());
        connection_state.last_error_reason = None;
        connection_state.last_error = None;
        state_handle.save_connection_meta(connection_state).await?;

        let stats_handle = stats_db.as_ref().map(|_| StatsHandle::new(&connection.id));
        let mut stats_flush = None;
        if let (Some(db), Some(handle)) = (&stats_db, &stats_handle) {
            db.persist_run(handle).await?;
            stats_flush = Some(spawn_stats_flush_task(db.clone(), handle.clone()));
        }
        let run_id = match &stats_handle {
            Some(handle) => Some(handle.run_id().await),
            None => None,
        };
        info!(
            connection = %connection.id,
            run_id = run_id.as_deref().unwrap_or("none"),
            mode = ?mode,
            dry_run,
            follow,
            "starting connection sync"
        );

        let result = Box::pin(sync_connection(SyncConnectionRequest {
            connection,
            state: connection_state,
            state_handle: state_handle.clone(),
            metadata: metadata.clone(),
            mode,
            dry_run,
            follow,
            default_batch_size,
            max_retries,
            max_concurrency,
            retry_backoff_ms,
            schema_diff_enabled,
            run_id: run_id.clone(),
            stats: stats_handle.clone(),
            shutdown: shutdown.clone(),
        }))
        .await;

        connection_state.last_sync_finished_at = Some(Utc::now());
        let error_string = result.as_ref().err().map(std::string::ToString::to_string);
        match &result {
            Ok(()) => {
                connection_state.last_sync_status = Some("success".to_string());
                connection_state.last_error_reason = None;
                info!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    "connection sync completed successfully"
                );
            }
            Err(err) => {
                connection_state.last_sync_status = Some("failed".to_string());
                connection_state.last_error_reason = Some(classify_error_reason(err));
                connection_state.last_error = error_string.clone();
                error!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    error = %err,
                    "connection sync failed"
                );
            }
        }
        stop_stats_flush_task(stats_flush).await;
        if let Some(handle) = &stats_handle {
            let status = if result.is_ok() { "success" } else { "failed" };
            handle.finish(status, error_string).await;
            if let Some(db) = &stats_db {
                db.persist_run(handle).await?;
            }
        }
        let duration_ms = connection_state
            .last_sync_started_at
            .zip(connection_state.last_sync_finished_at)
            .map_or(0.0, |(started_at, finished_at)| {
                (finished_at - started_at)
                    .num_milliseconds()
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0)
            });
        telemetry::record_sync_run(
            &connection.id,
            connection_state
                .last_sync_status
                .as_deref()
                .unwrap_or("unknown"),
            duration_ms,
        );
        refresh_postgres_checkpoints_from_store(&state_handle, connection_state).await;
        state_handle.save_connection_state(connection_state).await?;
        if let Some(age_seconds) =
            super::max_checkpoint_age_seconds(connection_state, connection, Utc::now())
        {
            telemetry::record_connection_checkpoint_age(
                &connection.id,
                super::connection_source_kind(connection),
                age_seconds,
            );
        }
        lease.release().await?;

        result?;
    }

    println!("Sync complete.");
    Ok(())
}

pub(crate) async fn sync_connection(request: SyncConnectionRequest<'_>) -> Result<()> {
    let SyncConnectionRequest {
        connection,
        state,
        state_handle,
        metadata,
        mode,
        dry_run,
        follow,
        default_batch_size,
        max_retries,
        max_concurrency,
        retry_backoff_ms,
        schema_diff_enabled,
        run_id,
        stats,
        shutdown,
    } = request;
    let dest = match &connection.destination {
        DestinationConfig::BigQuery(bq) => {
            BigQueryDestination::new(bq.clone(), dry_run, metadata.clone()).await?
        }
    };
    dest.validate().await?;

    let result = match &connection.source {
        SourceConfig::Postgres(pg) => {
            let source = PostgresSource::new(pg.clone(), metadata.clone()).await?;
            let tables = source.resolve_tables().await?;
            if follow && !source.cdc_enabled() {
                return Err(anyhow::anyhow!("--follow requires postgres.cdc=true"));
            }
            if source.cdc_enabled() {
                info!(
                    connection = %connection.id,
                    run_id = run_id.as_deref().unwrap_or("none"),
                    "syncing postgres via CDC"
                );
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    let result = Box::pin(source.sync_cdc(CdcSyncRequest {
                        dest: &dest,
                        state,
                        state_handle: Some(state_handle.clone()),
                        mode,
                        dry_run,
                        follow,
                        default_batch_size,
                        retry_backoff_ms,
                        snapshot_concurrency: max_concurrency,
                        tables: &tables,
                        schema_diff_enabled,
                        stats: stats.clone(),
                        shutdown: shutdown.clone(),
                    }))
                    .await
                    .with_context(|| "syncing postgres CDC");
                    match result {
                        Ok(()) => break,
                        Err(err)
                            if (follow || attempt < max_retries)
                                && !matches!(
                                    classify_sync_retry(&err),
                                    SyncRetryClass::Permanent
                                ) =>
                        {
                            let retry_class = classify_sync_retry(&err);
                            let backoff = compute_sync_retry_backoff(
                                &format!("{}:postgres_cdc", connection.id),
                                attempt,
                                retry_backoff_ms,
                            );
                            telemetry::record_retry_attempt(&connection.id, "postgres_cdc");
                            warn!(
                                connection = %connection.id,
                                run_id = run_id.as_deref().unwrap_or("none"),
                                attempt,
                                retry_class = ?retry_class,
                                backoff_ms = backoff.as_millis() as u64,
                                error = %format!("{err:#}"),
                                "postgres CDC sync attempt failed; retrying"
                            );
                            if super::wait_backoff(backoff, shutdown.clone()).await {
                                info!(
                                    connection = %connection.id,
                                    run_id = run_id.as_deref().unwrap_or("none"),
                                    attempt,
                                    "shutdown requested during postgres CDC retry backoff"
                                );
                                return Ok(());
                            }
                        }
                        Err(err) => {
                            let retry_class = classify_sync_retry(&err);
                            error!(
                                connection = %connection.id,
                                run_id = run_id.as_deref().unwrap_or("none"),
                                attempt,
                                retry_class = ?retry_class,
                                error = %format!("{err:#}"),
                                "postgres CDC sync attempt failed permanently"
                            );
                            return Err(err);
                        }
                    }
                }
                Ok(())
            } else {
                if max_concurrency > source.pool_max_connections() as usize {
                    warn!(
                        max_concurrency,
                        pool_max = source.pool_max_connections(),
                        "max_concurrency exceeds postgres pool size; reduce sync.max_concurrency or increase pool size"
                    );
                }
                let source = Arc::new(source);
                let dest = Arc::new(dest.clone());
                let semaphore = Arc::new(Semaphore::new(max_concurrency));
                let mut tasks = FuturesUnordered::new();

                for table in &tables {
                    let table = table.clone();
                    let checkpoint = state.postgres.get(&table.name).cloned().unwrap_or_default();
                    let source = Arc::clone(&source);
                    let dest = Arc::clone(&dest);
                    let stats = stats.clone();
                    let state_handle = state_handle.clone();
                    let shutdown = shutdown.clone();
                    let run_id = run_id.clone();
                    let connection_id = connection.id.clone();
                    let semaphore = Arc::clone(&semaphore);
                    tasks.push(async move {
                        let Ok(permit) = semaphore.acquire_owned().await else {
                            return (
                                table.name.clone(),
                                Err(anyhow::anyhow!("failed to acquire concurrency permit")),
                            );
                        };
                        let _permit = permit;
                        let mut attempt = 0;
                        let result = loop {
                            attempt += 1;
                            let attempt_result = source
                                .sync_table(TableSyncRequest {
                                    table: &table,
                                    dest: dest.as_ref(),
                                    checkpoint: checkpoint.clone(),
                                    state_handle: Some(state_handle.clone()),
                                    mode,
                                    dry_run,
                                    default_batch_size,
                                    schema_diff_enabled,
                                    stats: stats.clone(),
                                })
                                .await
                                .with_context(|| format!("syncing postgres table {}", table.name));
                            match attempt_result {
                                Ok(next_checkpoint) => break Ok(next_checkpoint),
                                Err(err)
                                    if attempt < max_retries
                                        && !matches!(
                                            classify_sync_retry(&err),
                                            SyncRetryClass::Permanent
                                        ) =>
                                {
                                    let retry_class = classify_sync_retry(&err);
                                    let backoff = compute_sync_retry_backoff(
                                        &format!("{}:postgres_table:{}", connection_id, table.name),
                                        attempt,
                                        retry_backoff_ms,
                                    );
                                    telemetry::record_retry_attempt(&connection_id, "postgres_table");
                                    warn!(
                                        connection = %connection_id,
                                        run_id = run_id.as_deref().unwrap_or("none"),
                                        table = %table.name,
                                        attempt,
                                        retry_class = ?retry_class,
                                        backoff_ms = backoff.as_millis() as u64,
                                        last_synced_at = ?checkpoint.last_synced_at,
                                        last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                        error = %err,
                                        "postgres table sync attempt failed; retrying"
                                    );
                                    if super::wait_backoff(backoff, shutdown.clone()).await {
                                        info!(
                                            connection = %connection_id,
                                            run_id = run_id.as_deref().unwrap_or("none"),
                                            table = %table.name,
                                            attempt,
                                            "shutdown requested during postgres table retry backoff"
                                        );
                                        break Ok(
                                            load_latest_postgres_checkpoint(
                                                &state_handle,
                                                &table.name,
                                                &checkpoint,
                                            )
                                            .await,
                                        );
                                    }
                                }
                                Err(err) => {
                                    let retry_class = classify_sync_retry(&err);
                                    error!(
                                        connection = %connection_id,
                                        run_id = run_id.as_deref().unwrap_or("none"),
                                        table = %table.name,
                                        attempt,
                                        retry_class = ?retry_class,
                                        last_synced_at = ?checkpoint.last_synced_at,
                                        last_primary_key = checkpoint.last_primary_key.as_deref().unwrap_or("none"),
                                        error = %err,
                                        "postgres table sync attempt failed permanently"
                                    );
                                    break Err(err);
                                }
                            }
                        };
                        (table.name.clone(), result)
                    });
                }

                let mut first_error: Option<anyhow::Error> = None;
                while let Some((table_name, result)) = tasks.next().await {
                    match result {
                        Ok(checkpoint) => {
                            state.postgres.insert(table_name, checkpoint);
                        }
                        Err(err) => {
                            if first_error.is_none() {
                                first_error = Some(err);
                            }
                        }
                    }
                }

                if let Some(err) = first_error {
                    return Err(err);
                }
                Ok(())
            }
        }
    };

    let shutdown_result = dest.shutdown().await;
    match (result, shutdown_result) {
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
        (Ok(()), Ok(())) => Ok(()),
    }
}
