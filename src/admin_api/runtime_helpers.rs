use super::*;
use crate::retry::ErrorReasonCode;
use crate::types::{TableRuntimeState, TableRuntimeStatus};

pub(super) const CDC_SLOT_INSPECTION_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(1);
const CDC_PROGRESS_IDLE_WAL_GAP_WATCH_BYTES: i64 = 1_000_000;

pub(super) fn is_postgres_cdc_connection(connection: &ConnectionConfig) -> bool {
    matches!(
        &connection.source,
        SourceConfig::Postgres(pg) if pg.cdc.unwrap_or(true)
    )
}

pub(super) fn uses_cdc_batch_load_queue(connection: &ConnectionConfig) -> bool {
    is_postgres_cdc_connection(connection)
        && matches!(
        &connection.destination,
        DestinationConfig::BigQuery(bq)
            if bq.batch_load_bucket.is_some() && bq.emulator_http.is_none()
        )
}

pub(super) fn build_cdc_batch_load_runtime_config(
    connection: &ConnectionConfig,
    sync: Option<&SyncConfig>,
) -> Option<CdcBatchLoadRuntimeConfig> {
    if !uses_cdc_batch_load_queue(connection) {
        return None;
    }
    let SourceConfig::Postgres(pg) = &connection.source else {
        return None;
    };
    let fallback_concurrency = sync
        .and_then(|sync| sync.max_concurrency)
        .unwrap_or(1)
        .max(1);
    let apply_concurrency = pg.cdc_apply_concurrency(fallback_concurrency);

    Some(CdcBatchLoadRuntimeConfig {
        queue_enabled: true,
        reducer_enabled: pg.cdc_batch_load_reducer_enabled(),
        reducer_max_jobs: pg.cdc_batch_load_reducer_max_jobs(),
        staging_worker_count: pg.cdc_batch_load_staging_worker_count(apply_concurrency),
        reducer_worker_count: pg.cdc_batch_load_reducer_worker_count(apply_concurrency),
    })
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
        Err(()) => Some(CachedPostgresCdcSlotState::unknown()),
    }
}

pub(super) fn publish_cached_postgres_cdc_slot_state(
    sender: &watch::Sender<CachedPostgresCdcSlotState>,
    next_state: CachedPostgresCdcSlotState,
) {
    sender.send_replace(next_state);
}

pub(super) async fn sample_and_publish_postgres_cdc_state(
    connection: &ConnectionConfig,
    state_store: &Arc<dyn AdminStateBackend>,
    sender: &watch::Sender<CachedPostgresCdcSlotState>,
) {
    let next_state = sample_cached_postgres_cdc_slot_state(connection, state_store).await;
    if let Some(next_state) = next_state.as_ref() {
        publish_cached_postgres_cdc_slot_state(sender, next_state.clone());
    }

    if !uses_cdc_batch_load_queue(connection) {
        return;
    }

    match state_store
        .load_cdc_batch_load_queue_summary(&connection.id)
        .await
    {
        Ok(summary) => {
            crate::telemetry::record_cdc_batch_load_queue_summary(&connection.id, &summary)
        }
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to load CDC batch-load queue summary for telemetry"
            );
        }
    }

    let wal_bytes_behind_confirmed = next_state
        .as_ref()
        .and_then(|state| state.snapshot.as_ref())
        .and_then(|snapshot| snapshot.wal_bytes_behind_confirmed);
    match state_store
        .load_cdc_coordinator_summary(&connection.id, wal_bytes_behind_confirmed)
        .await
    {
        Ok(summary) => crate::telemetry::record_cdc_coordinator_summary(&connection.id, &summary),
        Err(err) => {
            warn!(
                connection = %connection.id,
                error = %err,
                "failed to load CDC coordinator summary for telemetry"
            );
        }
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
                        sample_and_publish_postgres_cdc_state(&connection, &state_store, &sender).await;
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
    if !is_postgres_cdc_connection(connection) {
        return None;
    }

    state
        .cdc_slot_sampler_cache
        .get(&connection.id)
        .map(|sender| sender.borrow().clone())
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

pub(super) fn build_cdc_progress_insight(
    cdc: &ConnectionCdcSnapshot,
    batch_load_queue: Option<&CdcBatchLoadQueueSummary>,
    cdc_coordinator: Option<&CdcCoordinatorSummary>,
) -> Option<CdcProgressInsight> {
    if cdc.sampler_status == "disabled" && batch_load_queue.is_none() && cdc_coordinator.is_none() {
        return None;
    }

    let sequence_lag = cdc_coordinator.and_then(|summary| summary.sequence_lag);
    let pending_fragments = cdc_coordinator.map(|summary| summary.pending_fragments);
    let failed_fragments = cdc_coordinator.map(|summary| summary.failed_fragments);
    let pending_jobs = batch_load_queue.map(|summary| summary.pending_jobs);
    let running_jobs = batch_load_queue.map(|summary| summary.running_jobs);
    let snapshot_handoff_waiting_jobs =
        batch_load_queue.map(|summary| summary.snapshot_handoff_waiting_jobs);
    let jobs_per_minute = batch_load_queue.map(|summary| summary.jobs_per_minute);
    let rows_per_minute = batch_load_queue.map(|summary| summary.rows_per_minute);
    let failed_jobs = batch_load_queue.map(|summary| summary.failed_jobs);
    let failed_permanent_jobs = batch_load_queue.map(|summary| summary.failed_permanent_jobs);
    let failed_snapshot_handoff_jobs =
        batch_load_queue.map(|summary| summary.failed_snapshot_handoff_jobs);
    let failed_retryable_jobs = batch_load_queue.map(|summary| summary.failed_retryable_jobs);
    let failed_unclassified_jobs = batch_load_queue.map(|summary| summary.failed_unclassified_jobs);
    let has_recent_completions =
        rows_per_minute.unwrap_or_default() > 0 || jobs_per_minute.unwrap_or_default() > 0;

    let (status, primary_blocker, detail) = if cdc.sampler_status != "ok" {
        (
            "unknown",
            "cdc_sampler",
            format!("CDC sampler status is {}", cdc.sampler_status),
        )
    } else if failed_fragments.unwrap_or_default() > 0
        || failed_permanent_jobs.unwrap_or_default() > 0
    {
        (
            "blocked",
            "failed_work",
            "Failed CDC fragments or terminal batch-load jobs require retry or intervention"
                .to_string(),
        )
    } else if snapshot_handoff_waiting_jobs.unwrap_or_default() > 0 {
        (
            if has_recent_completions {
                "moving"
            } else {
                "backlogged"
            },
            "snapshot_handoff_wait",
            "CDC jobs are parked while table snapshots finish handoff".to_string(),
        )
    } else if failed_snapshot_handoff_jobs.unwrap_or_default() > 0
        && failed_snapshot_handoff_jobs == failed_jobs
    {
        (
            if has_recent_completions {
                "moving"
            } else {
                "backlogged"
            },
            "snapshot_handoff_retry",
            "CDC jobs are retrying while table snapshots finish handoff".to_string(),
        )
    } else if failed_retryable_jobs.unwrap_or_default() > 0 && failed_retryable_jobs == failed_jobs
    {
        (
            if has_recent_completions {
                "moving"
            } else {
                "backlogged"
            },
            "retryable_job_backoff",
            "Retryable CDC batch-load jobs are waiting for backoff; no terminal failure is visible"
                .to_string(),
        )
    } else if failed_unclassified_jobs.unwrap_or_default() > 0
        && failed_unclassified_jobs.unwrap_or_default() + failed_retryable_jobs.unwrap_or_default()
            == failed_jobs.unwrap_or_default()
    {
        (
            if has_recent_completions {
                "watch"
            } else {
                "backlogged"
            },
            "unclassified_failed_jobs",
            "Unclassified CDC batch-load failures are visible; inspect retry metadata if they do not drain"
                .to_string(),
        )
    } else if failed_jobs.unwrap_or_default() > 0 {
        (
            "blocked",
            "failed_work",
            "Failed CDC batch-load jobs require retry or intervention".to_string(),
        )
    } else if has_recent_completions {
        (
            "moving",
            "none",
            "Queued CDC work is completing recently".to_string(),
        )
    } else if cdc.wal_bytes_behind_confirmed.unwrap_or_default()
        >= CDC_PROGRESS_IDLE_WAL_GAP_WATCH_BYTES
        && pending_fragments.unwrap_or_default() == 0
        && pending_jobs.unwrap_or_default() == 0
        && running_jobs.unwrap_or_default() == 0
    {
        (
            "watch",
            "none",
            "Current WAL is ahead of confirmed flush while no queued CDC work is visible"
                .to_string(),
        )
    } else if pending_fragments.unwrap_or_default() > 0
        || pending_jobs.unwrap_or_default() > 0
        || running_jobs.unwrap_or_default() > 0
    {
        (
            "backlogged",
            "staging_or_reducer_backlog",
            "CDC work is queued or running but has no recent completion rate".to_string(),
        )
    } else {
        (
            "idle",
            "none",
            "No CDC backlog is visible in the admin summaries".to_string(),
        )
    };

    Some(CdcProgressInsight {
        status,
        primary_blocker,
        detail,
        sequence_lag,
        wal_bytes_behind: cdc.wal_bytes_behind_confirmed,
        pending_fragments,
        failed_fragments,
        pending_jobs,
        running_jobs,
        jobs_per_minute,
        rows_per_minute,
    })
}

pub(super) fn active_checkpoint_map<'a>(
    state: &'a ConnectionState,
    source: &SourceConfig,
) -> &'a std::collections::HashMap<String, TableCheckpoint> {
    match source {
        SourceConfig::Postgres(_) => &state.postgres,
        SourceConfig::DynamoDb(_) => &state.dynamodb,
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
        SourceConfig::DynamoDb(dynamo) => std::iter::once(dynamo.table_name.clone()).collect(),
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

pub(super) async fn load_postgres_cdc_slot_snapshot(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
) -> Result<Option<PostgresCdcSlotSnapshot>, ()> {
    let SourceConfig::Postgres(pg) = &connection.source else {
        return Ok(None);
    };
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

fn dynamodb_snapshot_in_progress(
    state: Option<&ConnectionState>,
    connection: &ConnectionConfig,
) -> bool {
    let SourceConfig::DynamoDb(dynamo) = &connection.source else {
        return false;
    };
    let Some(state) = state else {
        return false;
    };
    let Some(follow) = state
        .dynamodb_follow
        .as_ref()
        .filter(|follow| follow.table_name == dynamo.table_name)
    else {
        return false;
    };
    if follow.snapshot_in_progress {
        return true;
    }
    let checkpoint_time = state
        .dynamodb
        .get(&dynamo.table_name)
        .and_then(|checkpoint| checkpoint.last_synced_at);
    match (checkpoint_time, follow.cutover_time) {
        (Some(last_synced_at), Some(cutover_time)) => last_synced_at < cutover_time,
        (None, _) => true,
        _ => false,
    }
}

fn dynamodb_table_snapshot_in_progress(
    state: Option<&ConnectionState>,
    connection: &ConnectionConfig,
    table_name: &str,
) -> bool {
    let SourceConfig::DynamoDb(dynamo) = &connection.source else {
        return false;
    };
    table_name == dynamo.table_name && dynamodb_snapshot_in_progress(state, connection)
}

pub(super) fn derive_connection_mode(
    state: Option<&ConnectionState>,
    connection: &ConnectionConfig,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
) -> &'static str {
    let has_incomplete_snapshot = state.is_some_and(|state| {
        active_checkpoint_map(state, &connection.source)
            .values()
            .any(|checkpoint| checkpoint_has_incomplete_snapshot(Some(checkpoint)))
    });

    if has_incomplete_snapshot || dynamodb_snapshot_in_progress(state, connection) {
        if matches!(cdc_runtime_state, Some(PostgresCdcRuntimeState::Following)) {
            "mixed"
        } else {
            "snapshot"
        }
    } else if matches!(&connection.source, SourceConfig::DynamoDb(_)) {
        "kinesis"
    } else if is_postgres_cdc_connection(connection) {
        "cdc"
    } else {
        "scheduled_polling"
    }
}

pub(super) fn derive_connection_runtime(
    connection: &ConnectionConfig,
    state: Option<&ConnectionState>,
    _current_run: Option<&RunSummary>,
    cdc_runtime_state: Option<PostgresCdcRuntimeState>,
    now: DateTime<Utc>,
    metadata: RuntimeMetadata<'_>,
) -> ConnectionRuntime {
    let mode = derive_connection_mode(state, connection, cdc_runtime_state);
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
        Some("running") if dynamodb_snapshot_in_progress(state, connection) => {
            ("snapshotting", "snapshot_in_progress")
        }
        Some("running") if matches!(&connection.source, SourceConfig::DynamoDb(_)) => {
            ("running", "kinesis_following")
        }
        Some("running") => match cdc_runtime_state {
            Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
            Some(PostgresCdcRuntimeState::ContinuityLost) => ("blocked", "cdc_continuity_lost"),
            Some(PostgresCdcRuntimeState::Unknown) => ("starting", "cdc_state_unknown"),
            Some(PostgresCdcRuntimeState::Initializing) => ("starting", "cdc_initializing"),
            None => ("syncing", "sync_in_progress"),
        },
        Some("failed") => {
            let reason = state
                .and_then(|state| state.last_error_reason)
                .unwrap_or(ErrorReasonCode::LastRunFailed)
                .as_str();
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
        mode,
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
            let runtime = checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.runtime.clone());
            let (phase, reason_code) = if let Some(runtime) = runtime.as_ref() {
                table_runtime_phase_and_reason(runtime)
            } else {
                match state.and_then(|state| state.last_sync_status.as_deref()) {
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
                    Some("running")
                        if dynamodb_table_snapshot_in_progress(state, connection, &table_name) =>
                    {
                        ("snapshotting", "snapshot_in_progress")
                    }
                    Some("running") if matches!(&connection.source, SourceConfig::DynamoDb(_)) => {
                        ("running", "kinesis_following")
                    }
                    Some("running") => match cdc_runtime_state {
                        Some(PostgresCdcRuntimeState::Following) => ("running", "cdc_following"),
                        Some(PostgresCdcRuntimeState::ContinuityLost) => {
                            ("blocked", "cdc_continuity_lost")
                        }
                        Some(PostgresCdcRuntimeState::Unknown) => ("pending", "cdc_state_unknown"),
                        Some(PostgresCdcRuntimeState::Initializing) => {
                            ("pending", "cdc_initializing")
                        }
                        None => ("syncing", "sync_in_progress"),
                    },
                    Some("success") if checkpoint.is_some() => ("healthy", "healthy"),
                    _ => ("pending", "never_synced"),
                }
            };

            TableProgress {
                table_name,
                checkpoint,
                runtime,
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

fn table_runtime_phase_and_reason(runtime: &TableRuntimeState) -> (&'static str, &'static str) {
    let reason = runtime.reason.unwrap_or(match runtime.status {
        TableRuntimeStatus::Retrying => ErrorReasonCode::SnapshotRetryScheduled,
        TableRuntimeStatus::Blocked => ErrorReasonCode::SnapshotBlocked,
    });
    match runtime.status {
        TableRuntimeStatus::Retrying => ("retrying", reason.as_str()),
        TableRuntimeStatus::Blocked => ("blocked", reason.as_str()),
    }
}
