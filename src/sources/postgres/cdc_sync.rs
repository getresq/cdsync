use super::snapshot_sync::{release_exported_snapshot_slot, snapshot_shutdown_requested};
use super::*;
use crate::retry::CdcSyncPolicyError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy)]
pub(super) struct SnapshotTableWritePlan {
    pub write_mode: WriteMode,
    pub truncate_before_copy: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnapshotTaskFailureDisposition {
    Retry,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SnapshotTaskQueueSeed {
    Runnable {
        next_retry_at: Option<DateTime<Utc>>,
    },
    Blocked {
        last_error: Option<String>,
    },
}

pub(super) struct SnapshotTaskQueueEntry {
    pub(super) task: CdcSnapshotTask,
    pub(super) next_retry_at: Option<DateTime<Utc>>,
}

const SNAPSHOT_TASK_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(15 * 60);

pub(super) fn snapshot_task_retry_backoff(attempts: u32, retry_backoff_ms: u64) -> Duration {
    let exponent = attempts.saturating_sub(1).min(20);
    let multiplier = 1_u128 << exponent;
    let millis = u128::from(retry_backoff_ms).saturating_mul(multiplier);
    let capped = millis.min(SNAPSHOT_TASK_MAX_RETRY_BACKOFF.as_millis());
    Duration::from_millis(capped as u64)
}

pub(super) fn snapshot_task_failure_disposition(
    error: &anyhow::Error,
) -> SnapshotTaskFailureDisposition {
    match crate::retry::classify_sync_retry(error) {
        crate::retry::SyncRetryClass::Permanent => SnapshotTaskFailureDisposition::Block,
        crate::retry::SyncRetryClass::Backpressure | crate::retry::SyncRetryClass::Transient => {
            SnapshotTaskFailureDisposition::Retry
        }
    }
}

pub(super) fn snapshot_task_queue_seed(
    runtime: Option<&TableRuntimeState>,
    manual_resync_requested: bool,
) -> SnapshotTaskQueueSeed {
    match runtime {
        Some(TableRuntimeState {
            status: TableRuntimeStatus::Blocked,
            last_error,
            ..
        }) if !manual_resync_requested => SnapshotTaskQueueSeed::Blocked {
            last_error: last_error.clone(),
        },
        Some(TableRuntimeState {
            status: TableRuntimeStatus::Retrying,
            next_retry_at,
            ..
        }) => SnapshotTaskQueueSeed::Runnable {
            next_retry_at: *next_retry_at,
        },
        _ => SnapshotTaskQueueSeed::Runnable {
            next_retry_at: None,
        },
    }
}

pub(super) fn snapshot_task_requires_table_serialization(write_mode: WriteMode) -> bool {
    matches!(write_mode, WriteMode::Upsert)
}

pub(super) fn should_reset_snapshot_runtime(
    resume_snapshot_run: bool,
    manual_resync_requested: bool,
) -> bool {
    !resume_snapshot_run || manual_resync_requested
}

pub(super) fn should_clear_snapshot_runtime_on_success(
    has_pending_tasks: bool,
    active_task_count: usize,
    is_blocked: bool,
) -> bool {
    !has_pending_tasks && active_task_count == 0 && !is_blocked
}

pub(super) fn should_restart_blocked_resync_snapshot(
    checkpoint: &TableCheckpoint,
    manual_resync_requested: bool,
) -> bool {
    manual_resync_requested
        && table_has_snapshot_progress(checkpoint)
        && checkpoint
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.status == TableRuntimeStatus::Blocked)
}

pub(super) fn should_finalize_snapshot_checkpoint(
    table_name: &str,
    blocked_tables: &BTreeMap<String, String>,
) -> bool {
    !blocked_tables.contains_key(table_name)
}

pub(super) fn should_suppress_snapshot_retry_for_blocked_table(is_blocked: bool) -> bool {
    is_blocked
}

pub(super) fn refresh_snapshot_retry_resume_from_checkpoint(
    checkpoint: &TableCheckpoint,
    chunk: Option<SnapshotChunkRange>,
) -> Result<Option<String>> {
    checkpoint
        .snapshot_chunks
        .iter()
        .find(|candidate| snapshot_chunk_checkpoint_matches_range(candidate, chunk))
        .map(|candidate| candidate.last_primary_key.clone())
        .context("missing snapshot chunk checkpoint for retry resume")
}

async fn clear_snapshot_task_runtime_state(
    checkpoint_state: &Arc<Mutex<TableCheckpoint>>,
    table_name: &str,
    state_handle: Option<&StateHandle>,
) -> Result<()> {
    let checkpoint = {
        let mut checkpoint = checkpoint_state.lock().await;
        checkpoint.runtime = None;
        checkpoint.clone()
    };
    if let Some(state_handle) = state_handle {
        state_handle
            .save_postgres_checkpoint(table_name, &checkpoint)
            .await?;
    }
    Ok(())
}

async fn set_snapshot_task_runtime_state(
    checkpoint_state: &Arc<Mutex<TableCheckpoint>>,
    table_name: &str,
    state_handle: Option<&StateHandle>,
    status: TableRuntimeStatus,
    attempts: u32,
    error: &anyhow::Error,
    next_retry_at: Option<DateTime<Utc>>,
) -> Result<TableRuntimeState> {
    let checkpoint = {
        let mut checkpoint = checkpoint_state.lock().await;
        let blocked = matches!(status, TableRuntimeStatus::Blocked);
        let retry_class = crate::retry::classify_sync_retry(error);
        checkpoint.runtime = Some(TableRuntimeState {
            status,
            attempts,
            reason: Some(crate::retry::table_runtime_reason_for_retry_class(
                retry_class,
                blocked,
            )),
            last_error: Some(format!("{error:#}")),
            next_retry_at,
            updated_at: Some(Utc::now()),
        });
        checkpoint.clone()
    };
    if let Some(state_handle) = state_handle {
        state_handle
            .save_postgres_checkpoint(table_name, &checkpoint)
            .await?;
    }
    checkpoint
        .runtime
        .clone()
        .context("missing snapshot runtime state after update")
}

async fn next_snapshot_task_attempts(checkpoint_state: &Arc<Mutex<TableCheckpoint>>) -> u32 {
    let checkpoint = checkpoint_state.lock().await;
    checkpoint
        .runtime
        .as_ref()
        .map_or(0, |runtime| runtime.attempts)
        .saturating_add(1)
}

async fn wait_snapshot_task_backoff(duration: Duration, shutdown: Option<ShutdownSignal>) -> bool {
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

struct SnapshotPhaseResult {
    completed: bool,
    blocked_tables: BTreeSet<String>,
}

pub(super) fn snapshot_phase_should_proceed_to_cdc(
    follow: bool,
    blocked_table_count: usize,
) -> bool {
    follow && blocked_table_count > 0
}

pub(super) fn should_run_mixed_mode(
    follow: bool,
    has_snapshot_work: bool,
    supports_mixed_mode: bool,
) -> bool {
    follow && has_snapshot_work && supports_mixed_mode
}

pub(super) fn next_snapshot_task_table(
    table_order: &[String],
    next_cursor: usize,
    snapshot_task_queues: &BTreeMap<String, VecDeque<SnapshotTaskQueueEntry>>,
    active_serialized_tables: &HashSet<String>,
    now: DateTime<Utc>,
) -> (Option<String>, Option<DateTime<Utc>>, usize) {
    let mut earliest_retry_at: Option<DateTime<Utc>> = None;
    if table_order.is_empty() {
        return (None, None, next_cursor);
    }

    for offset in 0..table_order.len() {
        let index = (next_cursor + offset) % table_order.len();
        let table_name = &table_order[index];
        let Some(queue) = snapshot_task_queues.get(table_name) else {
            continue;
        };
        let Some(entry) = queue.front() else {
            continue;
        };
        if snapshot_task_requires_table_serialization(entry.task.write_mode)
            && active_serialized_tables.contains(table_name)
        {
            continue;
        }
        if let Some(next_retry_at) = entry.next_retry_at
            && next_retry_at > now
        {
            earliest_retry_at = Some(match earliest_retry_at {
                Some(current) => current.min(next_retry_at),
                None => next_retry_at,
            });
            continue;
        }
        return (
            Some(table_name.clone()),
            earliest_retry_at,
            (index + 1) % table_order.len(),
        );
    }

    (None, earliest_retry_at, next_cursor)
}

#[allow(clippy::too_many_arguments)]
async fn run_cdc_snapshot_phase(
    source: &PostgresSource,
    connection_label: &str,
    snapshot_tasks_to_run: Vec<CdcSnapshotTask>,
    snapshot_checkpoint_states: HashMap<String, Arc<Mutex<TableCheckpoint>>>,
    manual_resync_table_names: &HashSet<String>,
    snapshot_name: Option<String>,
    mut slot_guard: Option<ExportedSnapshotSlot>,
    snapshot_slot_name: String,
    preserve_existing_backlog: bool,
    dest: &crate::destinations::bigquery::BigQueryDestination,
    batch_size: usize,
    snapshot_concurrency: usize,
    retry_backoff_ms: u64,
    stats: Option<StatsHandle>,
    state_handle: Option<StateHandle>,
    shutdown: Option<ShutdownSignal>,
    follow: bool,
) -> Result<SnapshotPhaseResult> {
    let mut snapshot_task_queues: BTreeMap<String, VecDeque<SnapshotTaskQueueEntry>> =
        BTreeMap::new();
    let mut blocked_tables: BTreeMap<String, String> = BTreeMap::new();
    for snapshot_task in snapshot_tasks_to_run {
        let runtime = {
            let checkpoint = snapshot_task.checkpoint_state.lock().await;
            checkpoint.runtime.clone()
        };
        let manual_resync_requested =
            manual_resync_table_names.contains(&snapshot_task.info.source_name);
        match snapshot_task_queue_seed(runtime.as_ref(), manual_resync_requested) {
            SnapshotTaskQueueSeed::Runnable { next_retry_at } => {
                snapshot_task_queues
                    .entry(snapshot_task.info.source_name.clone())
                    .or_default()
                    .push_back(SnapshotTaskQueueEntry {
                        task: snapshot_task,
                        next_retry_at,
                    });
            }
            SnapshotTaskQueueSeed::Blocked { last_error } => {
                blocked_tables.insert(
                    snapshot_task.info.source_name.clone(),
                    last_error.unwrap_or_else(|| "snapshot blocked".to_string()),
                );
            }
        }
    }

    let mut active_serialized_tables = HashSet::new();
    let mut active_task_counts: HashMap<String, usize> = HashMap::new();
    let mut snapshot_tasks = FuturesUnordered::new();
    let table_order = snapshot_task_queues.keys().cloned().collect::<Vec<_>>();
    let mut next_table_cursor = 0usize;

    let snapshot_result: Result<()> = loop {
        let mut earliest_retry_at: Option<DateTime<Utc>> = None;
        while snapshot_tasks.len() < snapshot_concurrency {
            let now = Utc::now();
            let (selected_table_name, selection_retry_at, updated_cursor) =
                next_snapshot_task_table(
                    &table_order,
                    next_table_cursor,
                    &snapshot_task_queues,
                    &active_serialized_tables,
                    now,
                );
            earliest_retry_at = match (earliest_retry_at, selection_retry_at) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (None, Some(candidate)) => Some(candidate),
                (current, None) => current,
            };
            next_table_cursor = updated_cursor;

            let Some(table_name) = selected_table_name else {
                break;
            };
            let entry = snapshot_task_queues
                .get_mut(&table_name)
                .and_then(VecDeque::pop_front)
                .context("missing queued snapshot task for selected table")?;
            if snapshot_task_requires_table_serialization(entry.task.write_mode) {
                active_serialized_tables.insert(table_name.clone());
            }
            *active_task_counts.entry(table_name.clone()).or_default() += 1;
            let task_snapshot_name = snapshot_name.clone();
            let task_shutdown = shutdown.clone();
            let task_stats = stats.clone();
            let apply_dest = dest.clone();
            snapshot_tasks.push(async move {
                let SnapshotTaskQueueEntry {
                    task: snapshot_task,
                    next_retry_at,
                } = entry;
                info!(
                    table = %snapshot_task.info.source_name,
                    snapshot_name = task_snapshot_name.as_deref().unwrap_or("resume"),
                    batch_size,
                    snapshot_concurrency,
                    chunk_start = snapshot_task.chunk.map(|chunk| chunk.start_pk),
                    chunk_end = snapshot_task.chunk.map(|chunk| chunk.end_pk),
                    write_mode = match snapshot_task.write_mode {
                        WriteMode::Append => "append",
                        WriteMode::Upsert => "upsert",
                    },
                    "starting exported CDC snapshot copy"
                );
                let result = source
                    .run_snapshot_copy_with_exported_snapshot(
                        &snapshot_task.table,
                        &snapshot_task.info.schema,
                        snapshot_task.chunk,
                        SnapshotCopyContext {
                            dest: &apply_dest,
                            snapshot_name: task_snapshot_name.as_deref(),
                            batch_size,
                            stats: task_stats,
                            checkpoint_state: snapshot_task.checkpoint_state.clone(),
                            resume_from_primary_key: snapshot_task.resume_from_primary_key.clone(),
                            write_mode: snapshot_task.write_mode,
                            state_handle: snapshot_task.state_handle.clone(),
                            shutdown: task_shutdown,
                        },
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "copying exported snapshot for {}",
                            snapshot_task.info.source_name
                        )
                    });
                (
                    table_name,
                    SnapshotTaskQueueEntry {
                        task: snapshot_task,
                        next_retry_at,
                    },
                    result,
                )
            });
        }

        if snapshot_tasks.is_empty() {
            let has_pending_tasks = snapshot_task_queues.values().any(|queue| !queue.is_empty());
            if !has_pending_tasks {
                if blocked_tables.is_empty() {
                    break Ok(());
                }
                if snapshot_phase_should_proceed_to_cdc(follow, blocked_tables.len()) {
                    warn!(
                        connection = %connection_label,
                        blocked_tables = ?blocked_tables.keys().cloned().collect::<Vec<_>>(),
                        "snapshot drained runnable tables; continuing CDC for ready tables while blocked tables wait for manual resync"
                    );
                    break Ok(());
                }
                break Err(CdcSyncPolicyError::SnapshotBlocked {
                    tables: blocked_tables.keys().cloned().collect(),
                }
                .into());
            }
            if let Some(next_retry_at) = earliest_retry_at {
                let now = Utc::now();
                let wait = (next_retry_at - now)
                    .to_std()
                    .unwrap_or_else(|_| Duration::from_millis(0));
                if wait_snapshot_task_backoff(wait, shutdown.clone()).await {
                    break Ok(());
                }
                continue;
            }
            if blocked_tables.is_empty() {
                break Ok(());
            }
            if snapshot_phase_should_proceed_to_cdc(follow, blocked_tables.len()) {
                warn!(
                    connection = %connection_label,
                    blocked_tables = ?blocked_tables.keys().cloned().collect::<Vec<_>>(),
                    "snapshot has only blocked tables remaining; continuing CDC for ready tables while blocked tables wait for manual resync"
                );
                break Ok(());
            }
            break Err(CdcSyncPolicyError::SnapshotBlocked {
                tables: blocked_tables.keys().cloned().collect(),
            }
            .into());
        }

        let (table_name, mut entry, result) = snapshot_tasks
            .next()
            .await
            .context("snapshot task scheduler stopped unexpectedly")?;
        if snapshot_task_requires_table_serialization(entry.task.write_mode) {
            active_serialized_tables.remove(&table_name);
        }
        let active_task_count = match active_task_counts.get_mut(&table_name) {
            Some(count) => {
                *count = count.saturating_sub(1);
                let remaining = *count;
                if remaining == 0 {
                    active_task_counts.remove(&table_name);
                }
                remaining
            }
            None => 0,
        };
        match result {
            Ok(()) => {
                let has_pending_tasks = snapshot_task_queues
                    .get(&table_name)
                    .is_some_and(|queue| !queue.is_empty());
                let is_blocked = blocked_tables.contains_key(&table_name);
                if should_clear_snapshot_runtime_on_success(
                    has_pending_tasks,
                    active_task_count,
                    is_blocked,
                ) {
                    clear_snapshot_task_runtime_state(
                        &entry.task.checkpoint_state,
                        &entry.task.table.name,
                        entry.task.state_handle.as_ref(),
                    )
                    .await?;
                    blocked_tables.remove(&table_name);
                }
                if snapshot_task_queues
                    .get(&table_name)
                    .is_some_and(VecDeque::is_empty)
                {
                    snapshot_task_queues.remove(&table_name);
                }
            }
            Err(err) => match snapshot_task_failure_disposition(&err) {
                SnapshotTaskFailureDisposition::Retry => {
                    if should_suppress_snapshot_retry_for_blocked_table(
                        blocked_tables.contains_key(&table_name),
                    ) {
                        warn!(
                            connection = %connection_label,
                            table = %table_name,
                            error = %format!("{err:#}"),
                            "snapshot task failed retryably after table was already blocked; preserving blocked state"
                        );
                        continue;
                    }
                    let attempts = next_snapshot_task_attempts(&entry.task.checkpoint_state).await;
                    let backoff = snapshot_task_retry_backoff(attempts, retry_backoff_ms);
                    let next_retry_at = Utc::now()
                        + chrono::Duration::from_std(backoff)
                            .context("invalid snapshot retry backoff duration")?;
                    let _runtime = set_snapshot_task_runtime_state(
                        &entry.task.checkpoint_state,
                        &entry.task.table.name,
                        entry.task.state_handle.as_ref(),
                        TableRuntimeStatus::Retrying,
                        attempts,
                        &err,
                        Some(next_retry_at),
                    )
                    .await?;
                    warn!(
                        connection = %connection_label,
                        table = %table_name,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %format!("{err:#}"),
                        "snapshot task failed; scheduling table-local retry"
                    );
                    entry.task.resume_from_primary_key = {
                        let checkpoint = entry.task.checkpoint_state.lock().await;
                        refresh_snapshot_retry_resume_from_checkpoint(
                            &checkpoint,
                            entry.task.chunk,
                        )?
                    };
                    entry.next_retry_at = Some(next_retry_at);
                    snapshot_task_queues
                        .entry(table_name)
                        .or_default()
                        .push_front(entry);
                }
                SnapshotTaskFailureDisposition::Block => {
                    let attempts = next_snapshot_task_attempts(&entry.task.checkpoint_state).await;
                    let runtime = set_snapshot_task_runtime_state(
                        &entry.task.checkpoint_state,
                        &entry.task.table.name,
                        entry.task.state_handle.as_ref(),
                        TableRuntimeStatus::Blocked,
                        attempts,
                        &err,
                        None,
                    )
                    .await?;
                    warn!(
                        connection = %connection_label,
                        table = %table_name,
                        attempts = runtime.attempts,
                        error = %format!("{err:#}"),
                        "snapshot task blocked; preserving table-local state and continuing other tables"
                    );
                    blocked_tables.insert(table_name.clone(), format!("{err:#}"));
                    snapshot_task_queues.remove(&table_name);
                }
            },
        }
    };

    let snapshot_completed = cdc_snapshot_completed(snapshot_result.is_ok(), shutdown.as_ref());
    if let Some(slot_guard) = slot_guard.take() {
        release_exported_snapshot_slot(slot_guard, snapshot_completed).await?;
    }
    if preserve_existing_backlog {
        let client =
            PgReplicationClient::connect(source.build_pg_connection_config().await?).await?;
        if let Err(err) = client.delete_slot(&snapshot_slot_name).await
            && err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
        {
            return Err(err.into());
        }
    }
    snapshot_result?;
    if !snapshot_completed {
        return Ok(SnapshotPhaseResult {
            completed: false,
            blocked_tables: blocked_tables.keys().cloned().collect(),
        });
    }

    let completed_at = Utc::now();
    for (table_name, checkpoint_state) in snapshot_checkpoint_states {
        if !should_finalize_snapshot_checkpoint(&table_name, &blocked_tables) {
            continue;
        }
        let checkpoint = {
            let mut checkpoint = checkpoint_state.lock().await;
            checkpoint.last_synced_at = Some(completed_at);
            checkpoint.last_primary_key = None;
            checkpoint.snapshot_start_lsn = None;
            checkpoint.snapshot_preserve_backlog = false;
            checkpoint.snapshot_chunks.clear();
            checkpoint.clone()
        };
        if let Some(state_handle) = &state_handle {
            state_handle
                .save_postgres_checkpoint(&table_name, &checkpoint)
                .await?;
            if manual_resync_table_names.contains(&table_name) {
                state_handle
                    .clear_postgres_table_resync_request(&table_name)
                    .await?;
            }
        }
    }

    Ok(SnapshotPhaseResult {
        completed: true,
        blocked_tables: blocked_tables.keys().cloned().collect(),
    })
}

pub(super) fn checkpoint_has_material_sync_progress(checkpoint: &TableCheckpoint) -> bool {
    checkpoint.last_synced_at.is_some()
        || checkpoint.last_primary_key.is_some()
        || checkpoint.last_lsn.is_some()
        || checkpoint.snapshot_start_lsn.is_some()
        || !checkpoint.snapshot_chunks.is_empty()
}

pub(super) fn snapshot_progress_table_ids(
    table_ids: &HashMap<TableId, ResolvedPostgresTable>,
    state: &ConnectionState,
) -> HashSet<TableId> {
    table_ids
        .iter()
        .filter_map(|(table_id, table_cfg)| {
            state
                .postgres
                .get(&table_cfg.name)
                .is_some_and(table_has_snapshot_progress)
                .then_some(*table_id)
        })
        .collect()
}

pub(super) fn initial_snapshot_table_ids(
    table_ids: &HashMap<TableId, ResolvedPostgresTable>,
    state: &ConnectionState,
    last_lsn: Option<&str>,
) -> HashSet<TableId> {
    if last_lsn.is_none() {
        return HashSet::new();
    }

    table_ids
        .iter()
        .filter_map(|(table_id, table_cfg)| {
            state
                .postgres
                .get(&table_cfg.name)
                .filter(|checkpoint| checkpoint_has_material_sync_progress(checkpoint))
                .is_none()
                .then_some(*table_id)
        })
        .collect()
}

pub(super) fn should_preserve_existing_backlog(
    table_ids: &HashMap<TableId, ResolvedPostgresTable>,
    state: &ConnectionState,
    snapshot_progress_tables: &HashSet<TableId>,
    _resync_tables: &HashSet<TableId>,
    initial_snapshot_tables: &HashSet<TableId>,
    mode: crate::types::SyncMode,
    last_lsn: Option<&str>,
) -> bool {
    if mode == crate::types::SyncMode::Full || last_lsn.is_none() {
        return false;
    }

    if snapshot_progress_tables.iter().any(|table_id| {
        table_ids
            .get(table_id)
            .and_then(|table_cfg| state.postgres.get(&table_cfg.name))
            .is_some_and(|checkpoint| checkpoint.snapshot_preserve_backlog)
    }) {
        return true;
    }

    if !snapshot_progress_tables.is_empty() {
        return false;
    }

    !initial_snapshot_tables.is_empty() || !_resync_tables.is_empty()
}

pub(super) fn snapshot_table_ids(
    include_tables: &HashSet<TableId>,
    snapshot_progress_tables: &HashSet<TableId>,
    resync_tables: &HashSet<TableId>,
    initial_snapshot_tables: &HashSet<TableId>,
    mode: crate::types::SyncMode,
    last_lsn: Option<&str>,
    preserve_existing_backlog: bool,
) -> HashSet<TableId> {
    if mode == crate::types::SyncMode::Full || last_lsn.is_none() {
        include_tables.clone()
    } else if !snapshot_progress_tables.is_empty() {
        if preserve_existing_backlog {
            snapshot_progress_tables
                .union(resync_tables)
                .copied()
                .chain(initial_snapshot_tables.iter().copied())
                .collect()
        } else {
            include_tables.clone()
        }
    } else {
        resync_tables
            .union(initial_snapshot_tables)
            .copied()
            .collect()
    }
}

pub(super) fn snapshot_table_write_plan(
    mode: crate::types::SyncMode,
    resume_snapshot_run: bool,
    had_progress: bool,
    is_initial_snapshot_table: bool,
    is_resync_table: bool,
    force_snapshot_restart: bool,
) -> SnapshotTableWritePlan {
    let requires_fresh_snapshot = is_initial_snapshot_table || is_resync_table;
    if !resume_snapshot_run {
        SnapshotTableWritePlan {
            write_mode: WriteMode::Append,
            truncate_before_copy: mode == crate::types::SyncMode::Full
                || requires_fresh_snapshot
                || (force_snapshot_restart && had_progress),
        }
    } else if requires_fresh_snapshot && !had_progress {
        SnapshotTableWritePlan {
            write_mode: WriteMode::Append,
            truncate_before_copy: true,
        }
    } else {
        SnapshotTableWritePlan {
            write_mode: WriteMode::Upsert,
            truncate_before_copy: false,
        }
    }
}

pub(super) fn cdc_startup_requires_manual_resync(
    manual_resync_requested: bool,
    diff: Option<&SchemaDiff>,
    primary_key_changed_detected: bool,
) -> bool {
    manual_resync_requested
        && (diff.is_some_and(|diff| !diff.is_empty()) || primary_key_changed_detected)
}

fn bootstrap_snapshot_slot_name(slot_name: &str) -> Result<String> {
    let snapshot_slot_name = format!("{slot_name}_bootstrap");
    if snapshot_slot_name.len() > 63 {
        anyhow::bail!(
            "bootstrap snapshot slot name {} exceeds postgres limit",
            snapshot_slot_name
        );
    }
    Ok(snapshot_slot_name)
}

pub(super) fn cdc_slot_name(connection_id: &str, pipeline_id: Option<u64>) -> Result<String> {
    if let Some(pipeline_id) = pipeline_id {
        return EtlReplicationSlot::for_apply_worker(pipeline_id)
            .try_into()
            .context("building CDC replication slot name");
    }

    let mut normalized = String::with_capacity(connection_id.len());
    for ch in connection_id.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() || lower == '_' {
            normalized.push(lower);
        } else {
            normalized.push('_');
        }
    }
    while normalized.contains("__") {
        normalized = normalized.replace("__", "_");
    }
    normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() {
        anyhow::bail!(
            "connection id {} does not produce a valid CDC slot name",
            connection_id
        );
    }

    let slot_name = format!("cdsync_{}_cdc", normalized);
    if slot_name.len() > 63 {
        anyhow::bail!(
            "CDC replication slot name {} exceeds postgres limit",
            slot_name
        );
    }
    Ok(slot_name)
}

pub(super) fn cdc_snapshot_completed(
    run_succeeded: bool,
    shutdown: Option<&ShutdownSignal>,
) -> bool {
    run_succeeded && !snapshot_shutdown_requested(shutdown)
}

impl PostgresSource {
    pub async fn sync_cdc(&self, request: CdcSyncRequest<'_>) -> Result<()> {
        let CdcSyncRequest {
            dest,
            state,
            state_handle,
            mode,
            dry_run,
            follow,
            default_batch_size,
            retry_backoff_ms,
            snapshot_concurrency,
            tables,
            schema_diff_enabled,
            stats,
            shutdown,
        } = request;
        if dry_run {
            info!("dry-run: skipping CDC sync");
            return Ok(());
        }
        let connection_label = state_handle
            .as_ref()
            .map_or("postgres", StateHandle::connection_id);

        if !self.cdc_enabled() {
            return Err(CdcSyncPolicyError::CdcDisabled.into());
        }
        if tables.is_empty() {
            return Err(CdcSyncPolicyError::NoTablesConfigured.into());
        }
        let policy = self.config.schema_policy();

        let publication = self
            .config
            .publication
            .as_deref()
            .ok_or(CdcSyncPolicyError::PublicationRequired)?;
        let publication_mode = self.config.publication_mode();
        let pipeline_id = self.config.cdc_pipeline_id;
        let stream_pipeline_id = pipeline_id.unwrap_or(1);
        let batch_size = self
            .config
            .cdc_batch_size
            .or(self.config.batch_size)
            .unwrap_or(default_batch_size);
        let snapshot_concurrency = snapshot_concurrency.max(1);
        let cdc_apply_concurrency = self.config.cdc_apply_concurrency(snapshot_concurrency);
        let cdc_batch_load_worker_count = self
            .config
            .cdc_batch_load_worker_count(cdc_apply_concurrency);
        let cdc_apply_batch_size = batch_size.max(1);
        let cdc_apply_max_fill =
            Duration::from_millis(self.config.cdc_max_fill_ms.unwrap_or(2_000).max(1));
        let max_pending_events = self.config.cdc_max_pending_events.unwrap_or(100_000);
        let idle_timeout = Duration::from_secs(self.config.cdc_idle_timeout_seconds.unwrap_or(10));

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;

        self.validate_wal_level().await?;

        match publication_mode {
            crate::config::PostgresPublicationMode::Validate => {
                if !replication_client.publication_exists(publication).await? {
                    return Err(CdcSyncPolicyError::PublicationMissing {
                        publication: publication.to_string(),
                    }
                    .into());
                }
            }
            crate::config::PostgresPublicationMode::Manage => {
                self.reconcile_publication(publication, tables).await?;
            }
        }

        self.validate_publication_tables(&replication_client, publication, tables)
            .await?;
        self.validate_publication_filters(publication, tables, false)
            .await?;

        let table_ids = self.resolve_table_ids(tables).await?;

        let store = MemoryStore::new();
        let mut table_info_map: HashMap<TableId, CdcTableInfo> = HashMap::new();
        let mut etl_schemas: HashMap<TableId, EtlTableSchema> = HashMap::new();
        let mut include_tables: HashSet<TableId> = HashSet::new();
        let mut table_hashes: HashMap<TableId, String> = HashMap::new();
        let mut table_snapshots: HashMap<TableId, Vec<SchemaFieldSnapshot>> = HashMap::new();
        let mut resync_tables: HashSet<TableId> = HashSet::new();
        let manual_resync_table_names: HashSet<String> = match &state_handle {
            Some(handle) => handle
                .load_postgres_table_resync_requests()
                .await?
                .into_iter()
                .map(|request| request.source_table)
                .collect(),
            None => HashSet::new(),
        };

        for (table_id, table_cfg) in table_ids.iter() {
            let manual_resync_requested = manual_resync_table_names.contains(&table_cfg.name);
            let etl_schema = self.load_etl_table_schema(*table_id).await?;
            store.store_table_schema(etl_schema.clone()).await?;
            let info = cdc_table_info_from_schema(table_cfg, &etl_schema, &self.metadata)?;
            dest.ensure_table(&info.schema).await?;
            let schema_hash = schema_fingerprint(&info.schema);
            let checkpoint = state.postgres.get(&table_cfg.name);
            let prev_snapshot =
                checkpoint.and_then(|checkpoint| checkpoint.schema_snapshot.clone());
            let diff = schema_diff(prev_snapshot.as_deref(), &info.schema);
            let default_diff = SchemaDiff::default();
            let primary_key_changed_detected = primary_key_changed(
                checkpoint.and_then(|checkpoint| checkpoint.schema_primary_key.as_deref()),
                checkpoint.and_then(|checkpoint| checkpoint.schema_hash.as_deref()),
                &info.schema,
                &schema_hash,
                diff.as_ref().unwrap_or(&default_diff),
            );

            if let Some(diff) = diff
                && !diff.is_empty()
            {
                if cdc_startup_requires_manual_resync(
                    manual_resync_requested,
                    Some(&diff),
                    primary_key_changed_detected,
                ) {
                    resync_tables.insert(*table_id);
                    table_snapshots.insert(*table_id, schema_snapshot_from_schema(&info.schema));
                    table_hashes.insert(*table_id, schema_hash);
                    include_tables.insert(*table_id);
                    table_info_map.insert(*table_id, info);
                    etl_schemas.insert(*table_id, etl_schema);
                    continue;
                }
                if schema_diff_enabled {
                    log_schema_diff(&table_cfg.name, &diff);
                }
                if primary_key_changed_detected {
                    warn!(
                        table = %table_cfg.name,
                        previous_primary_key = checkpoint
                            .and_then(|checkpoint| checkpoint.schema_primary_key.as_deref())
                            .unwrap_or("unknown"),
                        current_primary_key = info.schema.primary_key.as_deref().unwrap_or("none"),
                        "schema change: primary key changed"
                    );
                }
                match policy {
                    SchemaChangePolicy::Fail => {
                        return Err(CdcSyncPolicyError::SchemaChangeDetected {
                            table: table_cfg.name.clone(),
                        }
                        .into());
                    }
                    SchemaChangePolicy::Auto => {
                        if diff.has_incompatible() || primary_key_changed_detected {
                            return Err(CdcSyncPolicyError::IncompatibleSchemaChange {
                                table: table_cfg.name.clone(),
                            }
                            .into());
                        }
                    }
                }
            } else if primary_key_changed_detected {
                if cdc_startup_requires_manual_resync(
                    manual_resync_requested,
                    None,
                    primary_key_changed_detected,
                ) {
                    resync_tables.insert(*table_id);
                    table_snapshots.insert(*table_id, schema_snapshot_from_schema(&info.schema));
                    table_hashes.insert(*table_id, schema_hash);
                    include_tables.insert(*table_id);
                    table_info_map.insert(*table_id, info);
                    etl_schemas.insert(*table_id, etl_schema);
                    continue;
                }
                warn!(
                    table = %table_cfg.name,
                    previous_primary_key = checkpoint
                        .and_then(|checkpoint| checkpoint.schema_primary_key.as_deref())
                        .unwrap_or("unknown"),
                    current_primary_key = info.schema.primary_key.as_deref().unwrap_or("none"),
                    "schema change: primary key changed"
                );
                match policy {
                    SchemaChangePolicy::Fail => {
                        return Err(CdcSyncPolicyError::SchemaChangeDetected {
                            table: table_cfg.name.clone(),
                        }
                        .into());
                    }
                    SchemaChangePolicy::Auto => {
                        return Err(CdcSyncPolicyError::IncompatibleSchemaChange {
                            table: table_cfg.name.clone(),
                        }
                        .into());
                    }
                }
            }
            if manual_resync_requested {
                resync_tables.insert(*table_id);
            }
            table_snapshots.insert(*table_id, schema_snapshot_from_schema(&info.schema));
            table_hashes.insert(*table_id, schema_hash);
            include_tables.insert(*table_id);
            table_info_map.insert(*table_id, info);
            etl_schemas.insert(*table_id, etl_schema);
        }

        let cdc_dest = EtlBigQueryDestination::new(
            dest.clone(),
            table_info_map.clone(),
            stats.clone(),
            cdc_apply_concurrency,
            cdc_batch_load_worker_count,
            state_handle.clone(),
            follow,
        )
        .await?;

        let slot_name = cdc_slot_name(connection_label, pipeline_id)?;

        {
            let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
            cdc_state.slot_name = Some(slot_name.clone());
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }
        let mut last_lsn = state.postgres_cdc.as_ref().and_then(|s| s.last_lsn.clone());
        for table_name in &manual_resync_table_names {
            let Some(checkpoint) = state.postgres.get_mut(table_name) else {
                continue;
            };
            if !should_restart_blocked_resync_snapshot(checkpoint, true) {
                continue;
            }
            checkpoint.snapshot_chunks.clear();
            checkpoint.snapshot_start_lsn = None;
            checkpoint.snapshot_preserve_backlog = false;
            if let Some(state_handle) = &state_handle {
                state_handle
                    .save_postgres_checkpoint(table_name, checkpoint)
                    .await?;
            }
        }
        let initial_snapshot_tables =
            initial_snapshot_table_ids(&table_ids, state, last_lsn.as_deref());
        let snapshot_progress_tables = snapshot_progress_table_ids(&table_ids, state);
        info!(
            connection = %connection_label,
            include_table_count = include_tables.len(),
            resync_table_count = resync_tables.len(),
            manual_resync_table_count = manual_resync_table_names.len(),
            initial_snapshot_table_count = initial_snapshot_tables.len(),
            snapshot_progress_table_count = snapshot_progress_tables.len(),
            last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
            slot_name = %slot_name,
            "resolved CDC snapshot inputs"
        );

        let has_snapshot_progress = !snapshot_progress_tables.is_empty();
        let mut snapshot_progress_missing_lsn = false;
        let snapshot_resume_lsn =
            tables
                .iter()
                .try_fold(None, |current, table| -> Result<Option<String>> {
                    let Some(checkpoint) = state.postgres.get(&table.name) else {
                        return Ok(current);
                    };
                    if !table_has_snapshot_progress(checkpoint) {
                        return Ok(current);
                    }
                    match (&current, checkpoint.snapshot_start_lsn.as_ref()) {
                        (None, Some(lsn)) => Ok(Some(lsn.clone())),
                        (Some(existing), Some(lsn)) if existing == lsn => Ok(current),
                        (Some(_), Some(_)) => {
                            Err(CdcSyncPolicyError::SnapshotProgressCheckpointMismatch.into())
                        }
                        (_, None) => {
                            snapshot_progress_missing_lsn = true;
                            Ok(current)
                        }
                    }
                })?;
        let force_snapshot_restart = has_snapshot_progress
            && (snapshot_resume_lsn.is_none() || snapshot_progress_missing_lsn);
        if force_snapshot_restart {
            warn!(
                "snapshot progress exists without a saved snapshot_start_lsn; restarting snapshot from scratch"
            );
        }
        let preserve_existing_backlog = should_preserve_existing_backlog(
            &table_ids,
            state,
            &snapshot_progress_tables,
            &resync_tables,
            &initial_snapshot_tables,
            mode,
            last_lsn.as_deref(),
        );

        let mut start_lsn = None;
        let mut needs_snapshot =
            mode == crate::types::SyncMode::Full || last_lsn.is_none() || has_snapshot_progress;
        if !resync_tables.is_empty() || !initial_snapshot_tables.is_empty() {
            needs_snapshot = true;
        }
        info!(
            connection = %connection_label,
            mode = ?mode,
            needs_snapshot,
            has_snapshot_progress,
            force_snapshot_restart,
            preserve_existing_backlog,
            last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
            "computed CDC snapshot decision"
        );

        let mut stream_already_ran = false;
        if needs_snapshot {
            let snapshot_table_ids = snapshot_table_ids(
                &include_tables,
                &snapshot_progress_tables,
                &resync_tables,
                &initial_snapshot_tables,
                mode,
                last_lsn.as_deref(),
                preserve_existing_backlog,
            );
            info!(
                connection = %connection_label,
                snapshot_table_count = snapshot_table_ids.len(),
                snapshot_table_names = ?snapshot_table_ids
                    .iter()
                    .filter_map(|table_id| table_ids.get(table_id).map(|table| table.name.as_str()))
                    .collect::<Vec<_>>(),
                "selected tables for CDC snapshot planning"
            );

            let resume_snapshot_run = snapshot_resume_lsn.is_some() && !force_snapshot_restart;
            let mut slot_guard = None;
            let mut snapshot_name = None;
            let snapshot_slot_name = if preserve_existing_backlog {
                bootstrap_snapshot_slot_name(&slot_name)?
            } else {
                slot_name.clone()
            };
            let snapshot_start_lsn = if resume_snapshot_run {
                snapshot_resume_lsn
                    .clone()
                    .context("missing snapshot start LSN for snapshot resume")?
            } else {
                if let Err(err) = replication_client.delete_slot(&snapshot_slot_name).await
                    && err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
                {
                    return Err(err.into());
                }
                let (guard, slot) = self
                    .create_exported_snapshot_slot(&snapshot_slot_name)
                    .await?;
                snapshot_name = Some(slot.snapshot_name);
                let consistent_point = slot.consistent_point;
                slot_guard = Some(guard);
                consistent_point
            };

            let mut snapshot_tasks_to_run = Vec::with_capacity(snapshot_table_ids.len());
            let mut snapshot_checkpoint_states = HashMap::new();
            for table_id in &snapshot_table_ids {
                let table = table_ids
                    .get(table_id)
                    .cloned()
                    .context("missing table config for CDC snapshot")?;
                let manual_resync_requested = manual_resync_table_names.contains(&table.name);
                let info = table_info_map
                    .get(table_id)
                    .cloned()
                    .context("missing table info for CDC snapshot")?;
                let resumed_from_saved_progress;
                let (write_mode, task_specs) = {
                    let entry = state.postgres.entry(table.name.clone()).or_default();
                    let had_progress = table_has_snapshot_progress(entry);
                    let is_initial_snapshot_table = initial_snapshot_tables.contains(table_id);
                    resumed_from_saved_progress = resume_snapshot_run && had_progress;
                    if should_reset_snapshot_runtime(resume_snapshot_run, manual_resync_requested) {
                        entry.runtime = None;
                    }
                    if let Some(schema_hash) = table_hashes.get(table_id) {
                        entry.schema_hash = Some(schema_hash.clone());
                    }
                    if let Some(snapshot) = table_snapshots.get(table_id) {
                        entry.schema_snapshot = Some(snapshot.clone());
                    }
                    entry
                        .schema_primary_key
                        .clone_from(&info.schema.primary_key);
                    let write_plan = snapshot_table_write_plan(
                        mode,
                        resume_snapshot_run,
                        had_progress,
                        is_initial_snapshot_table,
                        resync_tables.contains(table_id),
                        force_snapshot_restart,
                    );
                    if !resume_snapshot_run {
                        if force_snapshot_restart && had_progress {
                            entry.snapshot_chunks.clear();
                            entry.snapshot_start_lsn = None;
                        }
                        let snapshot_plan = self
                            .plan_snapshot_chunk_ranges(&table, batch_size, snapshot_concurrency)
                            .await?;
                        if !snapshot_plan.chunk_ranges.is_empty() {
                            let first_chunk = snapshot_plan.chunk_ranges.first().copied();
                            let last_chunk = snapshot_plan.chunk_ranges.last().copied();
                            info!(
                                table = %info.source_name,
                                row_count = snapshot_plan.row_count,
                                chunk_count = snapshot_plan.chunk_ranges.len(),
                                chunk_start = first_chunk.map(|chunk| chunk.start_pk),
                                chunk_end = last_chunk.map(|chunk| chunk.end_pk),
                                "chunking exported CDC snapshot table by PK range"
                            );
                        }
                        entry.snapshot_start_lsn = Some(snapshot_start_lsn.clone());
                        entry.snapshot_preserve_backlog = preserve_existing_backlog
                            && (is_initial_snapshot_table
                                || resync_tables.contains(table_id)
                                || entry.snapshot_preserve_backlog);
                        entry.snapshot_chunks =
                            snapshot_chunk_checkpoints_from_ranges(&snapshot_plan.chunk_ranges);
                        if write_plan.truncate_before_copy {
                            dest.truncate_table(&info.dest_name).await?;
                        }
                    } else if !had_progress {
                        let snapshot_plan = self
                            .plan_snapshot_chunk_ranges(&table, batch_size, snapshot_concurrency)
                            .await?;
                        if !snapshot_plan.chunk_ranges.is_empty() {
                            let first_chunk = snapshot_plan.chunk_ranges.first().copied();
                            let last_chunk = snapshot_plan.chunk_ranges.last().copied();
                            info!(
                                table = %info.source_name,
                                row_count = snapshot_plan.row_count,
                                chunk_count = snapshot_plan.chunk_ranges.len(),
                                chunk_start = first_chunk.map(|chunk| chunk.start_pk),
                                chunk_end = last_chunk.map(|chunk| chunk.end_pk),
                                "planning remaining snapshot work for resumed CDC snapshot"
                            );
                        }
                        entry.snapshot_start_lsn = Some(snapshot_start_lsn.clone());
                        entry.snapshot_preserve_backlog = preserve_existing_backlog
                            && (is_initial_snapshot_table || resync_tables.contains(table_id));
                        entry.snapshot_chunks =
                            snapshot_chunk_checkpoints_from_ranges(&snapshot_plan.chunk_ranges);
                        if write_plan.truncate_before_copy {
                            dest.truncate_table(&info.dest_name).await?;
                        }
                    }
                    if let Some(state_handle) = &state_handle {
                        state_handle
                            .save_postgres_checkpoint(&table.name, entry)
                            .await?;
                    }
                    (
                        write_plan.write_mode,
                        snapshot_resume_tasks_from_checkpoint(entry)?,
                    )
                };
                info!(
                    table = %info.source_name,
                    snapshot_task_count = task_specs.len(),
                    resume_snapshot_run,
                    resumed_from_saved_progress,
                    is_initial_snapshot_table = initial_snapshot_tables.contains(table_id),
                    is_resync_table = resync_tables.contains(table_id),
                    write_mode = match write_mode {
                        WriteMode::Append => "append",
                        WriteMode::Upsert => "upsert",
                    },
                    "planned CDC snapshot work for table"
                );
                let checkpoint_state = Arc::new(Mutex::new(
                    state
                        .postgres
                        .get(&table.name)
                        .cloned()
                        .context("missing snapshot checkpoint state after planning")?,
                ));
                snapshot_checkpoint_states
                    .insert(table.name.clone(), Arc::clone(&checkpoint_state));
                let task_count = task_specs.len();
                for (chunk, resume_from_primary_key) in task_specs {
                    snapshot_tasks_to_run.push(CdcSnapshotTask {
                        table: table.clone(),
                        info: info.clone(),
                        checkpoint_state: Arc::clone(&checkpoint_state),
                        resume_from_primary_key,
                        chunk,
                        write_mode,
                        state_handle: state_handle.clone(),
                    });
                }
                if resume_snapshot_run && task_count == 0 {
                    info!(
                        table = %info.source_name,
                        "snapshot already complete for table; keeping saved start LSN for CDC catch-up"
                    );
                } else if resumed_from_saved_progress {
                    info!(
                        table = %info.source_name,
                        write_mode = "upsert",
                        "resuming snapshot table from saved checkpoint progress"
                    );
                }
            }

            let has_snapshot_work = !snapshot_tasks_to_run.is_empty();
            let run_mixed_mode =
                should_run_mixed_mode(follow, has_snapshot_work, cdc_dest.supports_mixed_mode());
            let preferred_start_lsn = if preserve_existing_backlog {
                None
            } else {
                Some(snapshot_start_lsn.parse().map_err(|_| {
                    anyhow::anyhow!("invalid snapshot start LSN '{}'", snapshot_start_lsn)
                })?)
            };
            if run_mixed_mode {
                if !preserve_existing_backlog {
                    let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
                    cdc_state.last_lsn = Some(snapshot_start_lsn.clone());
                    if let Some(state_handle) = &state_handle {
                        state_handle.save_postgres_cdc_state(cdc_state).await?;
                    }
                    start_lsn = preferred_start_lsn;
                    last_lsn = Some(snapshot_start_lsn.clone());
                }

                let source = self.clone();
                let snapshot_connection_label = connection_label.to_string();
                let snapshot_connection_label_for_task = snapshot_connection_label.clone();
                let snapshot_manual_resync_table_names = manual_resync_table_names.clone();
                let snapshot_dest = dest.clone();
                let snapshot_stats = stats.clone();
                let snapshot_state_handle = state_handle.clone();
                let snapshot_shutdown = shutdown.clone();
                let snapshot_slot_name_for_task = snapshot_slot_name.clone();
                let snapshot_cleanup_slot_name =
                    (snapshot_slot_name != slot_name).then_some(snapshot_slot_name.clone());
                let snapshot_checkpoint_state_count = snapshot_checkpoint_states.len();
                let snapshot_task = tokio::spawn(async move {
                    run_cdc_snapshot_phase(
                        &source,
                        &snapshot_connection_label_for_task,
                        snapshot_tasks_to_run,
                        snapshot_checkpoint_states,
                        &snapshot_manual_resync_table_names,
                        snapshot_name,
                        slot_guard,
                        snapshot_slot_name_for_task,
                        preserve_existing_backlog,
                        &snapshot_dest,
                        batch_size,
                        snapshot_concurrency,
                        retry_backoff_ms,
                        snapshot_stats,
                        snapshot_state_handle,
                        snapshot_shutdown,
                        follow,
                    )
                    .await
                });
                info!(
                    connection = %snapshot_connection_label,
                    snapshot_table_count = snapshot_checkpoint_state_count,
                    "starting mixed-mode CDC while snapshot tasks continue in the background"
                );
                let start_lsn = match (start_lsn, last_lsn.as_deref()) {
                    (Some(lsn), _) => lsn,
                    (None, Some(lsn_str)) => lsn_str
                        .parse()
                        .map_err(|_| anyhow::anyhow!("invalid LSN '{}'", lsn_str))?,
                    (None, None) => {
                        let slot = replication_client.get_or_create_slot(&slot_name).await?;
                        slot.get_start_lsn()
                    }
                };

                let stream_result = self
                    .stream_cdc_changes(
                        replication_client.clone(),
                        CdcStreamConfig {
                            publication,
                            slot_name: &slot_name,
                            start_lsn,
                            pipeline_id: stream_pipeline_id,
                            idle_timeout,
                            max_pending_events,
                            apply_batch_size: cdc_apply_batch_size,
                            apply_max_fill: cdc_apply_max_fill,
                            apply_concurrency: cdc_apply_concurrency,
                            follow,
                            shutdown: shutdown.clone(),
                        },
                        CdcStreamRuntime {
                            include_tables: &include_tables,
                            table_configs: &table_ids,
                            store: &store,
                            dest: &cdc_dest,
                            table_info_map: &mut table_info_map,
                            etl_schemas: &mut etl_schemas,
                            table_hashes: &mut table_hashes,
                            table_snapshots: &mut table_snapshots,
                            state,
                            schema_policy: policy.clone(),
                            schema_diff_enabled,
                            stats: stats.clone(),
                            state_handle: state_handle.clone(),
                        },
                    )
                    .await;

                match stream_result {
                    Ok(last_flushed) => {
                        let snapshot_outcome = match snapshot_task.await {
                            Ok(result) => result?,
                            Err(err) => {
                                return Err(anyhow::anyhow!(
                                    "mixed-mode snapshot task join failed: {}",
                                    err
                                ));
                            }
                        };
                        if let Some(state_handle) = &state_handle {
                            state.postgres = state_handle.load_all_postgres_checkpoints().await?;
                        }
                        if !snapshot_outcome.completed {
                            info!(
                                connection = %snapshot_connection_label,
                                "shutdown requested during mixed-mode CDC snapshot; preserving resumable snapshot progress"
                            );
                            return Ok(());
                        }
                        if let Ok(slot) = replication_client.get_slot(&slot_name).await {
                            last_lsn = Some(slot.confirmed_flush_lsn.to_string());
                        } else {
                            last_lsn = Some(last_flushed.to_string());
                        }
                        stream_already_ran = true;
                    }
                    Err(err) => {
                        snapshot_task.abort();
                        let _ = snapshot_task.await;
                        if let Some(cleanup_slot_name) = snapshot_cleanup_slot_name
                            && let Err(cleanup_err) =
                                replication_client.delete_slot(&cleanup_slot_name).await
                            && cleanup_err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
                        {
                            warn!(
                                connection = %snapshot_connection_label,
                                slot_name = %cleanup_slot_name,
                                error = %cleanup_err,
                                "failed to clean up aborted mixed-mode snapshot slot"
                            );
                        }
                        if let Some(state_handle) = &state_handle {
                            state.postgres = state_handle.load_all_postgres_checkpoints().await?;
                        }
                        return Err(err);
                    }
                }
            } else {
                let snapshot_outcome = run_cdc_snapshot_phase(
                    self,
                    connection_label,
                    snapshot_tasks_to_run,
                    snapshot_checkpoint_states.clone(),
                    &manual_resync_table_names,
                    snapshot_name,
                    slot_guard,
                    snapshot_slot_name,
                    preserve_existing_backlog,
                    dest,
                    batch_size,
                    snapshot_concurrency,
                    retry_backoff_ms,
                    stats.clone(),
                    state_handle.clone(),
                    shutdown.clone(),
                    follow,
                )
                .await?;
                if !snapshot_outcome.completed {
                    for (table_name, checkpoint_state) in &snapshot_checkpoint_states {
                        let checkpoint = checkpoint_state.lock().await.clone();
                        state.postgres.insert(table_name.clone(), checkpoint);
                    }
                    info!(
                        connection = %connection_label,
                        snapshot_table_count = snapshot_checkpoint_states.len(),
                        "shutdown requested during CDC snapshot; preserving resumable snapshot progress"
                    );
                    return Ok(());
                }
                if !snapshot_outcome.blocked_tables.is_empty() {
                    let blocked_table_ids: HashSet<TableId> = table_ids
                        .iter()
                        .filter_map(|(table_id, table_cfg)| {
                            snapshot_outcome
                                .blocked_tables
                                .contains(&table_cfg.name)
                                .then_some(*table_id)
                        })
                        .collect();
                    if blocked_table_ids.len() == include_tables.len() {
                        return Err(CdcSyncPolicyError::SnapshotBlocked {
                            tables: snapshot_outcome.blocked_tables.iter().cloned().collect(),
                        }
                        .into());
                    }
                    for blocked_table_id in blocked_table_ids {
                        include_tables.remove(&blocked_table_id);
                    }
                    warn!(
                        connection = %connection_label,
                        blocked_tables = ?snapshot_outcome.blocked_tables,
                        remaining_cdc_tables = include_tables.len(),
                        "starting CDC while excluding blocked snapshot tables until manual resync"
                    );
                }
                if !preserve_existing_backlog {
                    let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
                    cdc_state.last_lsn = Some(snapshot_start_lsn.clone());
                    if let Some(state_handle) = &state_handle {
                        state_handle.save_postgres_cdc_state(cdc_state).await?;
                    }
                    start_lsn = preferred_start_lsn;
                    last_lsn = Some(snapshot_start_lsn.clone());
                }
                for (table_name, checkpoint_state) in snapshot_checkpoint_states {
                    let checkpoint = checkpoint_state.lock().await.clone();
                    state.postgres.insert(table_name, checkpoint);
                }
            }
        } else {
            info!(
                connection = %connection_label,
                last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
                "skipping CDC snapshot planning because snapshot is not needed"
            );
        }

        if !stream_already_ran {
            let start_lsn = match (start_lsn, last_lsn.as_deref()) {
                (Some(lsn), _) => lsn,
                (None, Some(lsn_str)) => lsn_str
                    .parse()
                    .map_err(|_| anyhow::anyhow!("invalid LSN '{}'", lsn_str))?,
                (None, None) => {
                    let slot = replication_client.get_or_create_slot(&slot_name).await?;
                    slot.get_start_lsn()
                }
            };

            let last_flushed = self
                .stream_cdc_changes(
                    replication_client.clone(),
                    CdcStreamConfig {
                        publication,
                        slot_name: &slot_name,
                        start_lsn,
                        pipeline_id: stream_pipeline_id,
                        idle_timeout,
                        max_pending_events,
                        apply_batch_size: cdc_apply_batch_size,
                        apply_max_fill: cdc_apply_max_fill,
                        apply_concurrency: cdc_apply_concurrency,
                        follow,
                        shutdown: shutdown.clone(),
                    },
                    CdcStreamRuntime {
                        include_tables: &include_tables,
                        table_configs: &table_ids,
                        store: &store,
                        dest: &cdc_dest,
                        table_info_map: &mut table_info_map,
                        etl_schemas: &mut etl_schemas,
                        table_hashes: &mut table_hashes,
                        table_snapshots: &mut table_snapshots,
                        state,
                        schema_policy: policy.clone(),
                        schema_diff_enabled,
                        stats: stats.clone(),
                        state_handle: state_handle.clone(),
                    },
                )
                .await?;

            if let Ok(slot) = replication_client.get_slot(&slot_name).await {
                last_lsn = Some(slot.confirmed_flush_lsn.to_string());
            } else {
                last_lsn = Some(last_flushed.to_string());
            }
        }

        if let Some(cdc_state) = state.postgres_cdc.as_mut() {
            cdc_state.last_lsn = last_lsn;
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }

        for (table_id, hash) in table_hashes {
            if let Some(info) = table_info_map.get(&table_id) {
                let entry = state.postgres.entry(info.source_name.clone()).or_default();
                entry.schema_hash = Some(hash);
                entry.schema_snapshot = table_snapshots.get(&table_id).cloned();
                entry.schema_primary_key = info.schema.primary_key.clone();
                if let Some(state_handle) = &state_handle {
                    state_handle
                        .save_postgres_checkpoint(&info.source_name, entry)
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub async fn validate_cdc_publication(
        &self,
        tables: &[ResolvedPostgresTable],
        verbose: bool,
    ) -> Result<()> {
        if !self.cdc_enabled() {
            return Err(CdcSyncPolicyError::CdcDisabled.into());
        }
        if tables.is_empty() {
            return Err(CdcSyncPolicyError::NoTablesConfigured.into());
        }

        let publication = self
            .config
            .publication
            .as_deref()
            .ok_or(CdcSyncPolicyError::PublicationRequired)?;
        let publication_mode = self.config.publication_mode();

        self.validate_wal_level().await?;

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;

        match publication_mode {
            crate::config::PostgresPublicationMode::Validate => {
                if !replication_client.publication_exists(publication).await? {
                    return Err(CdcSyncPolicyError::PublicationMissing {
                        publication: publication.to_string(),
                    }
                    .into());
                }
            }
            crate::config::PostgresPublicationMode::Manage => {
                self.reconcile_publication(publication, tables).await?;
            }
        }

        self.validate_publication_tables(&replication_client, publication, tables)
            .await?;
        self.validate_publication_filters(publication, tables, verbose)
            .await?;
        Ok(())
    }

    pub(super) async fn resolve_table_ids(
        &self,
        tables: &[ResolvedPostgresTable],
    ) -> Result<HashMap<TableId, ResolvedPostgresTable>> {
        let mut table_ids = HashMap::new();
        for table in tables {
            let (schema_name, table_name) = split_table_name(&table.name);
            let oid: Option<i32> = sqlx::query_scalar(
                r#"
                select c.oid::int
                from pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                where n.nspname = $1 and c.relname = $2
                "#,
            )
            .bind(&schema_name)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await?;
            let oid = oid.context(format!("table {} not found", table.name))?;
            table_ids.insert(TableId::new(oid.cast_unsigned()), table.clone());
        }
        Ok(table_ids)
    }

    async fn validate_wal_level(&self) -> Result<()> {
        let wal_level: String = sqlx::query_scalar("show wal_level")
            .fetch_one(&self.pool)
            .await
            .context("checking wal_level")?;
        if wal_level != "logical" {
            return Err(CdcSyncPolicyError::WalLevelNotLogical { wal_level }.into());
        }
        Ok(())
    }

    async fn validate_publication_tables(
        &self,
        replication_client: &PgReplicationClient,
        publication: &str,
        tables: &[ResolvedPostgresTable],
    ) -> Result<()> {
        let publication_tables = replication_client
            .get_publication_table_names(publication)
            .await?;
        let publication_set: HashSet<String> = publication_tables
            .into_iter()
            .map(|t| format!("{}.{}", t.schema, t.name))
            .collect();

        for table in tables {
            if !publication_set.contains(&table.name) {
                return Err(CdcSyncPolicyError::TableNotInPublication {
                    table: table.name.clone(),
                    publication: publication.to_string(),
                }
                .into());
            }
        }
        Ok(())
    }

    async fn validate_publication_filters(
        &self,
        publication: &str,
        tables: &[ResolvedPostgresTable],
        verbose: bool,
    ) -> Result<()> {
        let filters = self.load_publication_filters(publication).await?;
        for table in tables {
            if let Some(where_clause) = &table.where_clause {
                let actual = filters.get(&table.name).and_then(|v| v.as_ref());
                let actual =
                    actual.ok_or_else(|| CdcSyncPolicyError::PublicationFilterMissing {
                        publication: publication.to_string(),
                        table: table.name.clone(),
                    })?;
                let expected_norm = normalize_filter(where_clause);
                let actual_norm = normalize_filter(actual);
                if expected_norm != actual_norm {
                    if verbose {
                        warn!(
                            table = %table.name,
                            expected = %where_clause,
                            actual = %actual,
                            "publication row filter mismatch"
                        );
                    }
                    return Err(CdcSyncPolicyError::PublicationFilterMismatch {
                        publication: publication.to_string(),
                        table: table.name.clone(),
                        expected: where_clause.clone(),
                        actual: actual.clone(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    async fn reconcile_publication(
        &self,
        publication: &str,
        tables: &[ResolvedPostgresTable],
    ) -> Result<()> {
        let publication_sql = quote_pg_identifier(publication);
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
                .bind(publication)
                .fetch_one(&self.pool)
                .await?;
        if !exists {
            sqlx::query(&format!("create publication {};", publication_sql))
                .execute(&self.pool)
                .await?;
        }

        let table_list = Self::publication_table_spec_list(tables);

        let statement = if table_list.is_empty() {
            format!("alter publication {} set table;", publication_sql)
        } else {
            format!(
                "alter publication {} set table {};",
                publication_sql, table_list
            )
        };
        sqlx::query(&statement).execute(&self.pool).await?;
        Ok(())
    }

    pub(super) fn publication_table_spec(table: &ResolvedPostgresTable) -> String {
        let (schema_name, table_name) = split_table_name(&table.name);
        let qualified = format!(
            "{}.{}",
            quote_pg_identifier(&schema_name),
            quote_pg_identifier(&table_name)
        );
        match &table.where_clause {
            Some(where_clause) => format!("{qualified} WHERE ({where_clause})"),
            None => qualified,
        }
    }

    pub(super) fn publication_table_spec_list(tables: &[ResolvedPostgresTable]) -> String {
        tables
            .iter()
            .map(Self::publication_table_spec)
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn load_publication_filters(
        &self,
        publication: &str,
    ) -> Result<HashMap<String, Option<String>>> {
        let rows = sqlx::query(
            r#"
            select n.nspname as schema_name,
                   c.relname as table_name,
                   pg_get_expr(pr.prqual, pr.prrelid) as row_filter
            from pg_publication_rel pr
            join pg_publication p on pr.prpubid = p.oid
            join pg_class c on pr.prrelid = c.oid
            join pg_namespace n on c.relnamespace = n.oid
            where p.pubname = $1
            "#,
        )
        .bind(publication)
        .fetch_all(&self.pool)
        .await?;

        let mut filters = HashMap::new();
        for row in rows {
            let schema: String = row.try_get("schema_name")?;
            let table: String = row.try_get("table_name")?;
            let filter: Option<String> = row.try_get("row_filter")?;
            filters.insert(format!("{}.{}", schema, table), filter);
        }
        Ok(filters)
    }
}
