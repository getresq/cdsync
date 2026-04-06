use super::*;

pub(super) const CDC_SLOT_INSPECTION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(1);

pub(super) fn is_postgres_cdc_connection(connection: &ConnectionConfig) -> bool {
    matches!(
        &connection.source,
        SourceConfig::Postgres(pg) if pg.cdc.unwrap_or(true)
    )
}

pub(super) fn uses_cdc_batch_load_queue(connection: &ConnectionConfig) -> bool {
    matches!(
        &connection.destination,
        DestinationConfig::BigQuery(bq)
            if bq.batch_load_bucket.is_some() && bq.emulator_http.is_none()
    )
}

pub(super) fn build_cdc_slot_sampler_cache(cfg: &Config) -> CdcSlotSamplerCache {
    Arc::new(
        cfg.connections
            .iter()
            .filter(|connection| is_postgres_cdc_connection(connection))
            .map(|connection| {
                let initial = CachedPostgresCdcSlotState {
                    sampler_status: "initializing",
                    sampled_at: None,
                    snapshot: None,
                };
                let (tx, _rx) = watch::channel(initial);
                (connection.id.clone(), tx)
            })
            .collect(),
    )
}

pub(super) async fn sample_cached_postgres_cdc_slot_state(
    connection: &ConnectionConfig,
    state_store: &Arc<dyn AdminStateBackend>,
) -> Option<CachedPostgresCdcSlotState> {
    let connection_state = match state_store.load_connection_state(&connection.id).await {
        Ok(state) => state,
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to load connection state for postgres CDC slot sampling"
            );
            return Some(CachedPostgresCdcSlotState::unknown());
        }
    };
    match load_postgres_cdc_slot_snapshot(connection, connection_state.as_ref()).await {
        Ok(snapshot) => Some(CachedPostgresCdcSlotState::sampled(snapshot)),
        Err(()) => None,
    }
}

pub(super) fn publish_cached_postgres_cdc_slot_state(
    sender: &watch::Sender<CachedPostgresCdcSlotState>,
    next_state: CachedPostgresCdcSlotState,
) {
    if let Err(err) = sender.send(next_state) {
        warn!(error = %err, "failed to publish postgres CDC slot snapshot");
    }
}

pub(super) fn spawn_cdc_slot_sampler_tasks(
    cfg: Arc<Config>,
    state_store: Arc<dyn AdminStateBackend>,
    cache: CdcSlotSamplerCache,
    shutdown: ShutdownSignal,
) {
    for connection in &cfg.connections {
        if !is_postgres_cdc_connection(connection) {
            continue;
        }
        let Some(sender) = cache.get(&connection.id).cloned() else {
            continue;
        };
        let connection = connection.clone();
        let state_store = state_store.clone();
        let mut shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CDC_SLOT_SAMPLER_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(next_state) =
                            sample_cached_postgres_cdc_slot_state(&connection, &state_store).await
                        {
                            publish_cached_postgres_cdc_slot_state(&sender, next_state);
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed {
                            break;
                        }
                    }
                }
            }
        });
    }
}

pub(super) fn cached_postgres_cdc_slot_state(
    state: &AdminApiState,
    connection: &ConnectionConfig,
) -> Option<CachedPostgresCdcSlotState> {
    state
        .cdc_slot_sampler_cache
        .get(&connection.id)
        .map(|sender| sender.borrow().clone())
        .or(Some(CachedPostgresCdcSlotState {
            sampler_status: "disabled",
            sampled_at: None,
            snapshot: None,
        }))
}

pub(super) fn cached_postgres_cdc_runtime_state(
    state: &AdminApiState,
    connection: &ConnectionConfig,
) -> Option<PostgresCdcRuntimeState> {
    match cached_postgres_cdc_slot_state(state, connection) {
        Some(slot_state) if slot_state.sampler_status == "ok" => {
            postgres_cdc_runtime_state_from_snapshot(slot_state.snapshot.as_ref())
        }
        Some(slot_state) if matches!(slot_state.sampler_status, "unknown" | "initializing") => {
            Some(PostgresCdcRuntimeState::Unknown)
        }
        Some(_) | None => None,
    }
}

pub(super) fn active_checkpoint_map<'a>(
    state: &'a ConnectionState,
    source: &SourceConfig,
) -> &'a std::collections::HashMap<String, TableCheckpoint> {
    match source {
        SourceConfig::Postgres(_) => &state.postgres,
    }
}

pub(super) fn configured_entity_names(
    connection: &ConnectionConfig,
) -> std::collections::BTreeSet<String> {
    match &connection.source {
        SourceConfig::Postgres(pg) => pg
            .tables
            .as_ref()
            .map(|tables| tables.iter().map(|table| table.name.clone()).collect())
            .unwrap_or_default(),
    }
}

pub(super) fn checkpoint_age_seconds(
    checkpoint: Option<&TableCheckpoint>,
    now: DateTime<Utc>,
) -> Option<i64> {
    checkpoint
        .and_then(|checkpoint| checkpoint.last_synced_at)
        .map(|last_synced_at| (now - last_synced_at).num_seconds().max(0))
}

pub(super) fn max_checkpoint_age_seconds(
    state: Option<&ConnectionState>,
    connection: &ConnectionConfig,
    now: DateTime<Utc>,
) -> Option<i64> {
    state.and_then(|state| {
        let configured = configured_entity_names(connection);
        active_checkpoint_map(state, &connection.source)
            .iter()
            .filter(|(entity_name, _)| configured.is_empty() || configured.contains(*entity_name))
            .map(|(_, checkpoint)| checkpoint)
            .filter_map(|checkpoint| checkpoint_age_seconds(Some(checkpoint), now))
            .max()
    })
}

pub(super) fn classify_error_reason(error: Option<&str>) -> &'static str {
    let Some(error) = error else {
        return "healthy";
    };
    let normalized = error.to_ascii_lowercase();
    if normalized.contains("schema change") || normalized.contains("incompatible schema") {
        "schema_blocked"
    } else if normalized.contains("publication") {
        "publication_blocked"
    } else if normalized.contains("already locked") {
        "lock_contention"
    } else {
        "last_run_failed"
    }
}

pub(super) async fn load_postgres_cdc_slot_snapshot(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
) -> Result<Option<PostgresCdcSlotSnapshot>, ()> {
    let SourceConfig::Postgres(pg) = &connection.source;
    if !pg.cdc.unwrap_or(true) {
        return Ok(None);
    }

    let slot_name = state
        .and_then(|state| state.postgres_cdc.as_ref())
        .and_then(|cdc_state| cdc_state.slot_name.clone())
        .or_else(|| {
            crate::sources::postgres::admin_cdc_slot_name(&connection.id, pg.cdc_pipeline_id).ok()
        });
    let has_last_lsn = state
        .and_then(|state| state.postgres_cdc.as_ref())
        .and_then(|cdc_state| cdc_state.last_lsn.as_ref())
        .is_some();

    let Some(slot_name) = slot_name else {
        return Ok(Some(PostgresCdcSlotSnapshot {
            slot_name: None,
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            current_wal_lsn: None,
            wal_bytes_retained_by_slot: None,
            wal_bytes_behind_confirmed: None,
            continuity_lost: false,
        }));
    };

    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(CDC_SLOT_INSPECTION_TIMEOUT)
        .connect_lazy(&pg.url)
    {
        Ok(pool) => pool,
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to inspect postgres CDC slot snapshot"
            );
            return Err(());
        }
    };

    let row = match timeout(
        CDC_SLOT_INSPECTION_TIMEOUT,
        sqlx::query(
            r"
            select active,
                   restart_lsn::text as restart_lsn,
                   confirmed_flush_lsn::text as confirmed_flush_lsn,
                   pg_current_wal_lsn()::text as current_wal_lsn,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint as retained_bytes,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint as behind_confirmed_bytes
            from pg_replication_slots
            where slot_type = 'logical'
              and slot_name = $1
            ",
        )
        .bind(&slot_name)
        .fetch_optional(&pool),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(err)) => {
            warn!(
                connection = %connection.id,
                slot_name = %slot_name,
                error = %err,
                "failed to query postgres CDC slot snapshot"
            );
            return Err(());
        }
        Err(_) => {
            warn!(
                connection = %connection.id,
                slot_name = %slot_name,
                "timed out while inspecting postgres CDC slot snapshot"
            );
            return Err(());
        }
    };

    Ok(match row {
        Some(row) => Some(PostgresCdcSlotSnapshot {
            slot_name: Some(slot_name),
            active: row.try_get("active").unwrap_or(false),
            restart_lsn: row.try_get("restart_lsn").ok(),
            confirmed_flush_lsn: row.try_get("confirmed_flush_lsn").ok(),
            current_wal_lsn: row.try_get("current_wal_lsn").ok(),
            wal_bytes_retained_by_slot: row.try_get("retained_bytes").ok(),
            wal_bytes_behind_confirmed: row.try_get("behind_confirmed_bytes").ok(),
            continuity_lost: false,
        }),
        None => Some(PostgresCdcSlotSnapshot {
            slot_name: Some(slot_name),
            active: false,
            restart_lsn: None,
            confirmed_flush_lsn: None,
            current_wal_lsn: None,
            wal_bytes_retained_by_slot: None,
            wal_bytes_behind_confirmed: None,
            continuity_lost: has_last_lsn,
        }),
    })
}

pub(super) fn postgres_cdc_runtime_state_from_snapshot(
    snapshot: Option<&PostgresCdcSlotSnapshot>,
) -> Option<PostgresCdcRuntimeState> {
    let snapshot = snapshot?;
    if snapshot.active {
        Some(PostgresCdcRuntimeState::Following)
    } else if snapshot.continuity_lost {
        Some(PostgresCdcRuntimeState::ContinuityLost)
    } else {
        Some(PostgresCdcRuntimeState::Initializing)
    }
}

pub(super) fn checkpoint_has_incomplete_snapshot(checkpoint: Option<&TableCheckpoint>) -> bool {
    checkpoint.is_some_and(|checkpoint| {
        !checkpoint.snapshot_chunks.is_empty()
            && checkpoint
                .snapshot_chunks
                .iter()
                .any(|chunk| !chunk.complete)
    })
}

pub(super) fn derive_connection_runtime(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
    _current_run: Option<&RunSummary>,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
    now: DateTime<Utc>,
    metadata: RuntimeMetadata<'_>,
) -> ConnectionRuntime {
    let (phase, reason_code) = match state.and_then(|state| state.last_sync_status.as_deref()) {
        Some("running")
            if state.is_some_and(|state| {
                active_checkpoint_map(state, &connection.source)
                    .values()
                    .any(|checkpoint| checkpoint_has_incomplete_snapshot(Some(checkpoint)))
            }) =>
        {
            ("snapshotting", "snapshot_in_progress")
        }
        Some("running") => match cdc_runtime_state {
            Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
            Some(PostgresCdcRuntimeState::ContinuityLost) => ("blocked", "cdc_continuity_lost"),
            Some(PostgresCdcRuntimeState::Unknown) => ("starting", "cdc_state_unknown"),
            Some(PostgresCdcRuntimeState::Initializing) => ("starting", "cdc_initializing"),
            None => ("syncing", "sync_in_progress"),
        },
        Some("failed") => {
            let reason = classify_error_reason(state.and_then(|state| state.last_error.as_deref()));
            if matches!(reason, "schema_blocked" | "publication_blocked") {
                ("blocked", reason)
            } else {
                ("error", reason)
            }
        }
        Some("success") => ("healthy", "healthy"),
        _ => ("idle", "never_synced"),
    };

    ConnectionRuntime {
        connection_id: connection.id.clone(),
        phase,
        reason_code,
        last_sync_started_at: state.and_then(|state| state.last_sync_started_at),
        last_sync_finished_at: state.and_then(|state| state.last_sync_finished_at),
        last_sync_status: state.and_then(|state| state.last_sync_status.clone()),
        last_error: state.and_then(|state| state.last_error.clone()),
        max_checkpoint_age_seconds: max_checkpoint_age_seconds(state, connection, now),
        config_hash: metadata.config_hash.to_string(),
        deploy_revision: metadata.deploy_revision.map(ToOwned::to_owned),
        last_restart_reason: metadata.last_restart_reason.to_string(),
    }
}

pub(super) fn build_table_progress(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
    run_tables: &[TableStatsSnapshot],
    now: DateTime<Utc>,
    connection_reason_code: &'static str,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
) -> Vec<TableProgress> {
    let mut table_names = configured_entity_names(connection);
    if let Some(state) = state {
        table_names.extend(
            active_checkpoint_map(state, &connection.source)
                .keys()
                .cloned(),
        );
    }
    table_names.extend(run_tables.iter().map(|table| table.table_name.clone()));

    let run_table_map: std::collections::HashMap<_, _> = run_tables
        .iter()
        .cloned()
        .map(|table| (table.table_name.clone(), table))
        .collect();

    table_names
        .into_iter()
        .map(|table_name| {
            let checkpoint = state.and_then(|state| {
                active_checkpoint_map(state, &connection.source)
                    .get(&table_name)
                    .cloned()
            });
            let table_stats = run_table_map.get(&table_name).cloned();
            let checkpoint_age_seconds = checkpoint_age_seconds(checkpoint.as_ref(), now);
            let snapshot_chunks_total = checkpoint
                .as_ref()
                .map_or(0, |checkpoint| checkpoint.snapshot_chunks.len());
            let snapshot_chunks_complete = checkpoint.as_ref().map_or(0, |checkpoint| {
                checkpoint
                    .snapshot_chunks
                    .iter()
                    .filter(|chunk| chunk.complete)
                    .count()
            });
            let (phase, reason_code) = match state
                .and_then(|state| state.last_sync_status.as_deref())
            {
                Some("failed")
                    if matches!(
                        connection_reason_code,
                        "schema_blocked" | "publication_blocked"
                    ) =>
                {
                    ("blocked", connection_reason_code)
                }
                Some("failed") => ("error", connection_reason_code),
                Some("running") if checkpoint_has_incomplete_snapshot(checkpoint.as_ref()) => {
                    ("snapshotting", "snapshot_in_progress")
                }
                Some("running") => match cdc_runtime_state {
                    Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
                    Some(PostgresCdcRuntimeState::ContinuityLost) => {
                        ("blocked", "cdc_continuity_lost")
                    }
                    Some(PostgresCdcRuntimeState::Unknown) => ("pending", "cdc_state_unknown"),
                    Some(PostgresCdcRuntimeState::Initializing) => ("pending", "cdc_initializing"),
                    None => ("syncing", "sync_in_progress"),
                },
                Some("success") if checkpoint.is_some() => ("healthy", "healthy"),
                _ => ("pending", "never_synced"),
            };

            TableProgress {
                table_name,
                checkpoint,
                stats: table_stats,
                phase,
                reason_code,
                checkpoint_age_seconds,
                lag_seconds: checkpoint_age_seconds,
                snapshot_chunks_total,
                snapshot_chunks_complete,
            }
        })
        .collect()
}
