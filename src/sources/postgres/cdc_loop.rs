use super::*;

pub(super) struct CdcIdleState {
    pub(super) follow: bool,
    pub(super) in_tx: bool,
    pub(super) pending_events_empty: bool,
    pub(super) queued_batches_empty: bool,
    pub(super) pending_table_batches_empty: bool,
    pub(super) inflight_apply_empty: bool,
}

pub(super) fn next_cdc_wait_timeout(
    idle_timeout: Duration,
    apply_max_fill: Duration,
    pending_table_batches: &HashMap<TableId, PendingTableApplyBatch>,
    inflight_apply_count: usize,
    max_active_applies: usize,
) -> Duration {
    if inflight_apply_count >= max_active_applies.max(1) {
        return idle_timeout;
    }

    let now = Instant::now();
    pending_table_batches
        .values()
        .map(|pending| {
            pending
                .first_buffered_at
                .checked_add(apply_max_fill)
                .map_or(Duration::ZERO, |deadline| {
                    deadline.saturating_duration_since(now)
                })
        })
        .min()
        .map_or(idle_timeout, |pending_timeout| {
            pending_timeout.min(idle_timeout)
        })
}

pub(super) fn cdc_fill_deadline_reached(
    apply_max_fill: Duration,
    pending_table_batches: &HashMap<TableId, PendingTableApplyBatch>,
) -> bool {
    let now = Instant::now();
    pending_table_batches
        .values()
        .any(|pending| now.duration_since(pending.first_buffered_at) >= apply_max_fill)
}

pub(super) fn cdc_should_stop_after_idle(
    state: &CdcIdleState,
    last_xlog_activity: Instant,
    idle_timeout: Duration,
) -> bool {
    !state.follow
        && !state.in_tx
        && state.pending_events_empty
        && state.queued_batches_empty
        && state.pending_table_batches_empty
        && state.inflight_apply_empty
        && last_xlog_activity.elapsed() >= idle_timeout
}

pub(super) fn coordinator_inflight_commits(
    state_rx: &watch::Receiver<CdcCoordinatorRuntimeState>,
) -> usize {
    state_rx.borrow().inflight_commits
}

pub(super) fn submit_cdc_apply_acks(
    coordinator_tx: &mpsc::UnboundedSender<CdcCoordinatorCommand>,
    acks: Vec<CdcApplyFragmentAck>,
) -> Result<()> {
    for ack in acks {
        coordinator_tx
            .send(CdcCoordinatorCommand::CompleteFragments {
                sequences: ack.sequences,
            })
            .map_err(|_| anyhow::anyhow!("CDC coordinator task stopped"))?;
    }
    Ok(())
}

pub(super) async fn drain_ready_cdc_coordinator_advances(
    coordinator_advances_rx: &mut mpsc::UnboundedReceiver<CdcWatermarkAdvance>,
    stats: &Option<StatsHandle>,
    table_configs: &HashMap<TableId, ResolvedPostgresTable>,
    state: &mut ConnectionState,
    state_handle: Option<&StateHandle>,
    mut stream: Pin<&mut EventsStream>,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: &mut etl::types::PgLsn,
) -> Result<()> {
    while let Ok(advance) = coordinator_advances_rx.try_recv() {
        apply_cdc_watermark_advance(
            advance,
            &mut CdcWatermarkRuntime {
                stats,
                table_configs,
                state,
                state_handle,
            },
            stream.as_mut(),
            last_received_lsn,
            last_flushed_lsn,
        )
        .await?;
    }
    Ok(())
}

pub(super) fn dispatch_cdc_batches_and_record(
    slot_name: &str,
    pending_events_len: usize,
    queued_batches: &mut VecDeque<CommittedCdcBatch>,
    pending_table_batches: &mut HashMap<TableId, PendingTableApplyBatch>,
    inflight_dispatch: &mut FuturesUnordered<CdcDispatchFuture>,
    coordinator_tx: &mpsc::UnboundedSender<CdcCoordinatorCommand>,
    coordinator_state_rx: &watch::Receiver<CdcCoordinatorRuntimeState>,
    dest: &EtlBigQueryDestination,
    coordination: &mut CdcApplyCoordination<'_>,
    config: CdcDispatchConfig,
) -> Result<()> {
    dispatch_cdc_batches(
        queued_batches,
        pending_table_batches,
        inflight_dispatch,
        coordinator_tx,
        dest,
        coordination,
        config,
    )?;
    crate::telemetry::record_cdc_pipeline_depths(
        slot_name,
        pending_events_len as u64,
        (queued_batches.len() + pending_cdc_commit_count(pending_table_batches)) as u64,
        coordinator_inflight_commits(coordinator_state_rx) as u64,
    );
    Ok(())
}

pub(super) fn handle_cdc_connection_update(
    changed: Result<(), tokio::sync::watch::error::RecvError>,
    connection_updates_rx: &mut tokio::sync::watch::Receiver<
        etl::replication::client::PostgresConnectionUpdate,
    >,
    slot_name: &str,
) -> Result<()> {
    if changed.is_err() {
        anyhow::bail!(
            "postgres replication connection updates ended unexpectedly for slot {}",
            slot_name
        );
    }

    let update = connection_updates_rx.borrow_and_update().clone();
    match update {
        etl::replication::client::PostgresConnectionUpdate::Running => {
            info!(
                slot_name = slot_name,
                "postgres replication connection running"
            );
            Ok(())
        }
        etl::replication::client::PostgresConnectionUpdate::Terminated => {
            anyhow::bail!(
                "postgres replication connection terminated for slot {}",
                slot_name
            )
        }
        etl::replication::client::PostgresConnectionUpdate::Errored { error } => {
            anyhow::bail!(
                "postgres replication connection errored for slot {}: {}",
                slot_name,
                error
            )
        }
    }
}

pub(super) fn maybe_log_cdc_wait_timeout(
    slot_name: &str,
    last_heartbeat_log: &mut Instant,
    last_replication_message: Instant,
    last_xlog_activity: Instant,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: etl::types::PgLsn,
    pending_events: usize,
    queued_batches: usize,
    pending_table_batches: usize,
    inflight_apply: usize,
    active_table_applies: usize,
    inflight_commits: usize,
    in_tx: bool,
    follow: bool,
    reason: &'static str,
) {
    if last_heartbeat_log.elapsed() < Duration::from_secs(30) {
        return;
    }
    info!(
        slot_name = slot_name,
        last_received_lsn = %last_received_lsn,
        last_flushed_lsn = %last_flushed_lsn,
        pending_events,
        queued_batches,
        pending_table_batches,
        inflight_apply,
        active_table_applies,
        inflight_commits,
        in_tx,
        follow,
        last_replication_message_secs = last_replication_message.elapsed().as_secs(),
        last_xlog_activity_secs = last_xlog_activity.elapsed().as_secs(),
        reason,
        "waiting for replication message"
    );
    *last_heartbeat_log = Instant::now();
}

pub(super) fn log_replication_message_after_idle(
    slot_name: &str,
    replication_idle_for: Duration,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: etl::types::PgLsn,
) {
    if replication_idle_for < Duration::from_secs(30) {
        return;
    }
    info!(
        slot_name = slot_name,
        idle_secs = replication_idle_for.as_secs(),
        last_received_lsn = %last_received_lsn,
        last_flushed_lsn = %last_flushed_lsn,
        "received replication message after idle period"
    );
}

pub(super) async fn handle_primary_keepalive_reply(
    slot_name: &str,
    wal_end: etl::types::PgLsn,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: etl::types::PgLsn,
    mut stream: Pin<&mut EventsStream>,
    state_handle: Option<&StateHandle>,
) -> Result<()> {
    info!(
        event = "cdc_keepalive_reply_requested",
        component = "coordinator",
        slot_name = slot_name,
        wal_end = %wal_end,
        last_received_lsn = %last_received_lsn,
        last_flushed_lsn = %last_flushed_lsn,
        "postgres requested logical replication keepalive reply"
    );
    await_cdc_timeout(
        format!("sending CDC keepalive status update for slot {slot_name}"),
        CDC_STATUS_UPDATE_TIMEOUT,
        stream.as_mut().send_status_update(
            last_received_lsn,
            last_flushed_lsn,
            true,
            StatusUpdateType::KeepAlive,
        ),
    )
    .await?;
    if let Some(state_handle) = state_handle {
        let mut watermark_state = state_handle
            .load_cdc_watermark_state()
            .await?
            .unwrap_or_default();
        let now = chrono::Utc::now();
        watermark_state.last_status_update_sent_at = Some(now);
        watermark_state.last_keepalive_reply_at = Some(now);
        watermark_state.last_slot_feedback_lsn = Some(last_flushed_lsn.to_string());
        watermark_state.updated_at = Some(now);
        state_handle
            .save_cdc_watermark_state(&watermark_state)
            .await?;
    }
    Ok(())
}

pub(super) fn maybe_log_cdc_loop_heartbeat(
    slot_name: &str,
    last_heartbeat_log: &mut Instant,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: etl::types::PgLsn,
    pending_events: usize,
    queued_batches: usize,
    pending_table_batches: usize,
    inflight_apply: usize,
    active_table_applies: usize,
    coordinator_state: &CdcCoordinatorRuntimeState,
    in_tx: bool,
    expected_commit_lsn: Option<etl::types::PgLsn>,
    last_xlog_activity: Instant,
) {
    if last_heartbeat_log.elapsed() < Duration::from_secs(30) {
        return;
    }
    info!(
        slot_name = slot_name,
        last_received_lsn = %last_received_lsn,
        last_flushed_lsn = %last_flushed_lsn,
        pending_events,
        queued_batches,
        pending_table_batches,
        inflight_apply,
        active_table_applies,
        inflight_commits = coordinator_state.inflight_commits,
        next_sequence_to_ack = coordinator_state.next_sequence_to_ack,
        in_tx,
        expected_commit_lsn = ?expected_commit_lsn,
        last_xlog_activity_secs = last_xlog_activity.elapsed().as_secs(),
        "cdc loop heartbeat"
    );
    *last_heartbeat_log = Instant::now();
}

pub(super) async fn finalize_cdc_runtime(
    slot_name: &str,
    pending_events_len: usize,
    queued_batches: &mut VecDeque<CommittedCdcBatch>,
    pending_table_batches: &mut HashMap<TableId, PendingTableApplyBatch>,
    inflight_dispatch: &mut FuturesUnordered<CdcDispatchFuture>,
    inflight_apply: &mut FuturesUnordered<CdcApplyFuture>,
    coordinator_tx: mpsc::UnboundedSender<CdcCoordinatorCommand>,
    coordinator_advances_rx: &mut mpsc::UnboundedReceiver<CdcWatermarkAdvance>,
    coordinator_state_rx: &watch::Receiver<CdcCoordinatorRuntimeState>,
    coordinator_task: tokio::task::JoinHandle<Result<()>>,
    dest: &EtlBigQueryDestination,
    coordination: &mut CdcApplyCoordination<'_>,
    dispatch_config: CdcDispatchConfig,
    stats: &Option<StatsHandle>,
    table_configs: &HashMap<TableId, ResolvedPostgresTable>,
    state: &mut ConnectionState,
    state_handle: Option<&StateHandle>,
    mut stream: Pin<&mut EventsStream>,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: &mut etl::types::PgLsn,
) -> Result<etl::types::PgLsn> {
    dispatch_cdc_batches_and_record(
        slot_name,
        pending_events_len,
        queued_batches,
        pending_table_batches,
        inflight_dispatch,
        &coordinator_tx,
        coordinator_state_rx,
        dest,
        coordination,
        CdcDispatchConfig {
            force_flush: true,
            ..dispatch_config
        },
    )?;
    while !inflight_dispatch.is_empty()
        || !inflight_apply.is_empty()
        || !pending_table_batches.is_empty()
    {
        while let Some(Some(result)) = inflight_dispatch.next().now_or_never() {
            let acks = handle_cdc_dispatch_result(
                Some(result),
                inflight_apply,
                coordination.active_table_applies,
            )?;
            submit_cdc_apply_acks(&coordinator_tx, acks)?;
        }
        drain_ready_cdc_coordinator_advances(
            coordinator_advances_rx,
            stats,
            table_configs,
            state,
            state_handle,
            stream.as_mut(),
            last_received_lsn,
            last_flushed_lsn,
        )
        .await?;
        let acks = drain_one_cdc_work(
            inflight_dispatch,
            inflight_apply,
            coordination.active_table_applies,
        )
        .await?;
        submit_cdc_apply_acks(&coordinator_tx, acks)?;
        drain_ready_cdc_coordinator_advances(
            coordinator_advances_rx,
            stats,
            table_configs,
            state,
            state_handle,
            stream.as_mut(),
            last_received_lsn,
            last_flushed_lsn,
        )
        .await?;
        dispatch_cdc_batches_and_record(
            slot_name,
            pending_events_len,
            queued_batches,
            pending_table_batches,
            inflight_dispatch,
            &coordinator_tx,
            coordinator_state_rx,
            dest,
            coordination,
            CdcDispatchConfig {
                force_flush: true,
                ..dispatch_config
            },
        )?;
    }

    drop(coordinator_tx);
    while let Some(advance) = coordinator_advances_rx.recv().await {
        apply_cdc_watermark_advance(
            advance,
            &mut CdcWatermarkRuntime {
                stats,
                table_configs,
                state,
                state_handle,
            },
            stream.as_mut(),
            last_received_lsn,
            last_flushed_lsn,
        )
        .await?;
    }
    coordinator_task.await.map_err(anyhow::Error::new)??;

    Ok(*last_flushed_lsn)
}
