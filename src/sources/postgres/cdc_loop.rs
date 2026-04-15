use super::*;

const CDC_DURABLE_FRONTIER_SCAN_LIMIT: usize = 4096;

pub(super) enum CdcRelationLockWaitProgress<'a> {
    Acquired(tokio::sync::MutexGuard<'a, ()>),
    ApplyAcks(Vec<CdcApplyFragmentAck>),
    WatermarkAdvance(CdcWatermarkAdvance),
}

pub(super) struct CdcIdleState {
    pub(super) follow: bool,
    pub(super) in_tx: bool,
    pub(super) pending_events_empty: bool,
    pub(super) queued_batches_empty: bool,
    pub(super) pending_table_batches_empty: bool,
    pub(super) inflight_apply_empty: bool,
}

pub(super) struct CdcKeepaliveFeedbackState {
    pub(super) in_tx: bool,
    pub(super) pending_events_empty: bool,
    pub(super) queued_batches_empty: bool,
    pub(super) pending_table_batches_empty: bool,
    pub(super) inflight_dispatch_empty: bool,
    pub(super) inflight_apply_empty: bool,
    pub(super) active_table_applies_empty: bool,
    pub(super) coordinator_inflight_commits: usize,
    pub(super) durable_backlog_empty: bool,
}

impl CdcKeepaliveFeedbackState {
    fn can_ack_idle_wal(&self) -> bool {
        !self.in_tx
            && self.pending_events_empty
            && self.queued_batches_empty
            && self.pending_table_batches_empty
            && self.inflight_dispatch_empty
            && self.inflight_apply_empty
            && self.active_table_applies_empty
            && self.coordinator_inflight_commits == 0
            && self.durable_backlog_empty
    }
}

pub(super) fn cdc_keepalive_feedback_lsn(
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: etl::types::PgLsn,
    state: &CdcKeepaliveFeedbackState,
) -> etl::types::PgLsn {
    if state.can_ack_idle_wal() && last_received_lsn > last_flushed_lsn {
        last_received_lsn
    } else {
        last_flushed_lsn
    }
}

pub(super) async fn cdc_durable_idle_backlog_empty(
    slot_name: &str,
    state_handle: Option<&StateHandle>,
) -> bool {
    let Some(state_handle) = state_handle else {
        return true;
    };
    match state_handle.load_cdc_coordinator_summary(None).await {
        Ok(summary) => summary.pending_fragments == 0 && summary.failed_fragments == 0,
        Err(err) => {
            warn!(
                slot_name = slot_name,
                error = %err,
                "failed to inspect durable CDC backlog before idle keepalive feedback"
            );
            false
        }
    }
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

pub(super) async fn wait_for_table_apply_lock_or_cdc_progress<'a>(
    table_id: TableId,
    table_lock: &'a Arc<Mutex<()>>,
    inflight_dispatch: &mut FuturesUnordered<CdcDispatchFuture>,
    inflight_apply: &mut FuturesUnordered<CdcApplyFuture>,
    active_table_applies: &mut HashSet<TableId>,
    coordinator_advances_rx: &mut mpsc::UnboundedReceiver<CdcWatermarkAdvance>,
    wait_timeout: Duration,
) -> Result<CdcRelationLockWaitProgress<'a>> {
    tokio::select! {
        guard = table_lock.lock(), if !active_table_applies.contains(&table_id) => {
            Ok(CdcRelationLockWaitProgress::Acquired(guard))
        }
        result = inflight_dispatch.next(), if !inflight_dispatch.is_empty() => {
            Ok(CdcRelationLockWaitProgress::ApplyAcks(
                handle_cdc_dispatch_result(result, inflight_apply, active_table_applies)?
            ))
        }
        result = inflight_apply.next(), if !inflight_apply.is_empty() => {
            Ok(CdcRelationLockWaitProgress::ApplyAcks(
                handle_cdc_apply_result(result, active_table_applies)?
            ))
        }
        advance = coordinator_advances_rx.recv() => {
            Ok(CdcRelationLockWaitProgress::WatermarkAdvance(
                advance.context("CDC coordinator task stopped")?
            ))
        }
        () = tokio::time::sleep(wait_timeout) => {
            anyhow::bail!(
                "timed out after {}s waiting for CDC table apply lock while draining in-flight work",
                wait_timeout.as_secs()
            )
        }
    }
}

pub(super) async fn submit_cdc_durable_frontier_acks(
    coordinator_tx: &mpsc::UnboundedSender<CdcCoordinatorCommand>,
    coordinator_state_rx: &watch::Receiver<CdcCoordinatorRuntimeState>,
    state_handle: Option<&StateHandle>,
) -> Result<()> {
    let Some(state_handle) = state_handle else {
        return Ok(());
    };
    let from_sequence = coordinator_state_rx.borrow().next_sequence_to_ack;
    let Some(frontier) = state_handle
        .load_cdc_durable_apply_frontier(from_sequence, CDC_DURABLE_FRONTIER_SCAN_LIMIT)
        .await?
    else {
        return Ok(());
    };
    if frontier.next_sequence_to_ack <= from_sequence {
        return Ok(());
    }
    let sequences = (from_sequence..frontier.next_sequence_to_ack).collect::<Vec<_>>();
    info!(
        component = "coordinator",
        event = "cdc_coordinator_durable_frontier_seen",
        connection_id = state_handle.connection_id(),
        from_sequence,
        next_sequence_to_ack = frontier.next_sequence_to_ack,
        commit_lsn = %frontier.commit_lsn,
        sequence_count = sequences.len(),
        "submitting durable CDC frontier completions"
    );
    coordinator_tx
        .send(CdcCoordinatorCommand::CompleteCommits { sequences })
        .map_err(|_| anyhow::anyhow!("CDC coordinator task stopped"))?;
    Ok(())
}

pub(super) async fn cdc_durable_backlog_backpressure_exceeded(
    slot_name: &str,
    state_handle: Option<&StateHandle>,
    max_pending_fragments: Option<usize>,
    max_oldest_pending: Option<Duration>,
) -> Result<bool> {
    if max_pending_fragments.is_none() && max_oldest_pending.is_none() {
        return Ok(false);
    }
    let Some(state_handle) = state_handle else {
        return Ok(false);
    };
    let summary = state_handle.load_cdc_coordinator_summary(None).await?;
    if let Some(max_pending_fragments) = max_pending_fragments
        && summary.pending_fragments >= i64::try_from(max_pending_fragments).unwrap_or(i64::MAX)
    {
        crate::telemetry::record_cdc_backpressure_wait(
            slot_name,
            u64::try_from(summary.pending_fragments).unwrap_or_default(),
            max_pending_fragments as u64,
        );
        return Ok(true);
    }
    if let (Some(oldest_pending_age_seconds), Some(max_oldest_pending)) =
        (summary.oldest_pending_age_seconds, max_oldest_pending)
        && oldest_pending_age_seconds
            >= i64::try_from(max_oldest_pending.as_secs()).unwrap_or(i64::MAX)
    {
        crate::telemetry::record_cdc_backpressure_wait(
            slot_name,
            u64::try_from(oldest_pending_age_seconds).unwrap_or_default(),
            max_oldest_pending.as_secs(),
        );
        return Ok(true);
    }
    Ok(false)
}

pub(super) async fn drain_ready_cdc_coordinator_advances(
    coordinator_tx: &mpsc::UnboundedSender<CdcCoordinatorCommand>,
    coordinator_advances_rx: &mut mpsc::UnboundedReceiver<CdcWatermarkAdvance>,
    coordinator_state_rx: &watch::Receiver<CdcCoordinatorRuntimeState>,
    stats: &Option<StatsHandle>,
    table_configs: &HashMap<TableId, ResolvedPostgresTable>,
    state: &mut ConnectionState,
    state_handle: Option<&StateHandle>,
    mut stream: Pin<&mut EventsStream>,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: &mut etl::types::PgLsn,
) -> Result<()> {
    submit_cdc_durable_frontier_acks(coordinator_tx, coordinator_state_rx, state_handle).await?;
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
    last_feedback_lsn: &mut etl::types::PgLsn,
    feedback_lsn: etl::types::PgLsn,
    force: bool,
    mut stream: Pin<&mut EventsStream>,
    state_handle: Option<&StateHandle>,
) -> Result<()> {
    info!(
        event = "cdc_keepalive_feedback",
        component = "coordinator",
        slot_name = slot_name,
        wal_end = %wal_end,
        last_received_lsn = %last_received_lsn,
        last_flushed_lsn = %last_flushed_lsn,
        last_feedback_lsn = %*last_feedback_lsn,
        feedback_lsn = %feedback_lsn,
        force,
        "sending logical replication keepalive feedback"
    );
    await_cdc_timeout(
        format!("sending CDC keepalive status update for slot {slot_name}"),
        CDC_STATUS_UPDATE_TIMEOUT,
        stream.as_mut().send_status_update(
            last_received_lsn,
            feedback_lsn,
            force,
            StatusUpdateType::KeepAlive,
        ),
    )
    .await?;
    if feedback_lsn > *last_feedback_lsn {
        *last_feedback_lsn = feedback_lsn;
    }
    if let Some(state_handle) = state_handle {
        let mut feedback_state = state_handle
            .load_cdc_feedback_state()
            .await?
            .unwrap_or_default();
        let now = chrono::Utc::now();
        feedback_state.last_received_lsn = Some(last_received_lsn.to_string());
        feedback_state.last_flushed_lsn = Some(last_flushed_lsn.to_string());
        feedback_state.last_persisted_lsn = Some(last_flushed_lsn.to_string());
        feedback_state.last_status_update_sent_at = Some(now);
        feedback_state.last_keepalive_reply_at = Some(now);
        feedback_state.last_slot_feedback_lsn = Some(feedback_lsn.to_string());
        feedback_state.updated_at = Some(now);
        state_handle
            .save_cdc_feedback_state(&feedback_state)
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
            &coordinator_tx,
            coordinator_advances_rx,
            coordinator_state_rx,
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
            &coordinator_tx,
            coordinator_advances_rx,
            coordinator_state_rx,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_feedback_advances_idle_flush_to_received_lsn() {
        let state = CdcKeepaliveFeedbackState {
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: true,
            pending_table_batches_empty: true,
            inflight_dispatch_empty: true,
            inflight_apply_empty: true,
            active_table_applies_empty: true,
            coordinator_inflight_commits: 0,
            durable_backlog_empty: true,
        };

        assert_eq!(
            cdc_keepalive_feedback_lsn(
                etl::types::PgLsn::from(0x20_u64),
                etl::types::PgLsn::from(0x10_u64),
                &state,
            ),
            etl::types::PgLsn::from(0x20_u64)
        );
    }

    #[test]
    fn keepalive_feedback_keeps_durable_frontier_when_work_is_inflight() {
        let state = CdcKeepaliveFeedbackState {
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: true,
            pending_table_batches_empty: true,
            inflight_dispatch_empty: true,
            inflight_apply_empty: false,
            active_table_applies_empty: false,
            coordinator_inflight_commits: 1,
            durable_backlog_empty: true,
        };

        assert_eq!(
            cdc_keepalive_feedback_lsn(
                etl::types::PgLsn::from(0x20_u64),
                etl::types::PgLsn::from(0x10_u64),
                &state,
            ),
            etl::types::PgLsn::from(0x10_u64)
        );
    }

    #[test]
    fn keepalive_feedback_keeps_durable_frontier_when_durable_backlog_exists() {
        let state = CdcKeepaliveFeedbackState {
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: true,
            pending_table_batches_empty: true,
            inflight_dispatch_empty: true,
            inflight_apply_empty: true,
            active_table_applies_empty: true,
            coordinator_inflight_commits: 0,
            durable_backlog_empty: false,
        };

        assert_eq!(
            cdc_keepalive_feedback_lsn(
                etl::types::PgLsn::from(0x20_u64),
                etl::types::PgLsn::from(0x10_u64),
                &state,
            ),
            etl::types::PgLsn::from(0x10_u64)
        );
    }

    #[tokio::test]
    async fn relation_lock_wait_drains_dispatch_ack_while_lock_is_held() -> anyhow::Result<()> {
        let table_id = TableId::new(7);
        let table_lock = Arc::new(Mutex::new(()));
        let guard = table_lock.lock().await;
        let mut inflight_dispatch: FuturesUnordered<CdcDispatchFuture> = FuturesUnordered::new();
        let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
        let mut active_table_applies = HashSet::new();
        let (_advance_tx, mut advance_rx) = mpsc::unbounded_channel();
        let dispatch: CdcDispatchFuture = Box::pin(async {
            Ok(CdcDispatchResult::Immediate(CdcApplyFragmentAck {
                sequences: vec![7],
                released_table: None,
            }))
        });
        inflight_dispatch.push(dispatch);

        let progress = wait_for_table_apply_lock_or_cdc_progress(
            table_id,
            &table_lock,
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
            &mut advance_rx,
            Duration::from_secs(1),
        )
        .await?;

        match progress {
            CdcRelationLockWaitProgress::ApplyAcks(acks) => {
                assert_eq!(acks.len(), 1);
                assert_eq!(acks[0].sequences, vec![7]);
            }
            CdcRelationLockWaitProgress::Acquired(_)
            | CdcRelationLockWaitProgress::WatermarkAdvance(_) => {
                anyhow::bail!("expected relation wait to drain dispatch progress first")
            }
        }

        drop(guard);
        let progress = wait_for_table_apply_lock_or_cdc_progress(
            table_id,
            &table_lock,
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
            &mut advance_rx,
            Duration::from_secs(1),
        )
        .await?;
        match progress {
            CdcRelationLockWaitProgress::Acquired(_guard) => {}
            CdcRelationLockWaitProgress::ApplyAcks(_)
            | CdcRelationLockWaitProgress::WatermarkAdvance(_) => {
                anyhow::bail!("expected relation wait to acquire the released table lock")
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn relation_lock_wait_drains_active_same_table_dispatch_before_acquiring_free_lock()
    -> anyhow::Result<()> {
        let table_id = TableId::new(7);
        let table_lock = Arc::new(Mutex::new(()));
        let mut inflight_dispatch: FuturesUnordered<CdcDispatchFuture> = FuturesUnordered::new();
        let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
        let mut active_table_applies = HashSet::from([table_id]);
        let (_advance_tx, mut advance_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = oneshot::channel();
        let dispatch: CdcDispatchFuture = Box::pin(async move {
            release_rx.await?;
            Ok(CdcDispatchResult::Immediate(CdcApplyFragmentAck {
                sequences: vec![7],
                released_table: Some(table_id),
            }))
        });
        inflight_dispatch.push(dispatch);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = release_tx.send(());
        });

        let progress = wait_for_table_apply_lock_or_cdc_progress(
            table_id,
            &table_lock,
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
            &mut advance_rx,
            Duration::from_secs(1),
        )
        .await?;

        match progress {
            CdcRelationLockWaitProgress::ApplyAcks(acks) => {
                assert_eq!(acks.len(), 1);
                assert_eq!(acks[0].sequences, vec![7]);
                assert!(!active_table_applies.contains(&table_id));
            }
            CdcRelationLockWaitProgress::Acquired(_)
            | CdcRelationLockWaitProgress::WatermarkAdvance(_) => {
                anyhow::bail!("expected active same-table dispatch to drain before lock acquire")
            }
        }

        let progress = wait_for_table_apply_lock_or_cdc_progress(
            table_id,
            &table_lock,
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
            &mut advance_rx,
            Duration::from_secs(1),
        )
        .await?;
        match progress {
            CdcRelationLockWaitProgress::Acquired(_guard) => {}
            CdcRelationLockWaitProgress::ApplyAcks(_)
            | CdcRelationLockWaitProgress::WatermarkAdvance(_) => {
                anyhow::bail!("expected relation wait to acquire after same-table work drained")
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn relation_lock_wait_times_out_without_progress() {
        let table_id = TableId::new(7);
        let table_lock = Arc::new(Mutex::new(()));
        let _guard = table_lock.lock().await;
        let mut inflight_dispatch: FuturesUnordered<CdcDispatchFuture> = FuturesUnordered::new();
        let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
        let mut active_table_applies = HashSet::new();
        let (_advance_tx, mut advance_rx) = mpsc::unbounded_channel();

        let Err(err) = wait_for_table_apply_lock_or_cdc_progress(
            table_id,
            &table_lock,
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
            &mut advance_rx,
            Duration::from_millis(10),
        )
        .await
        else {
            panic!("relation lock wait should time out");
        };

        assert!(
            err.to_string().contains("waiting for CDC table apply lock"),
            "{err}"
        );
    }

    fn test_state_config() -> Option<crate::config::StateConfig> {
        let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
        Some(crate::config::StateConfig {
            url,
            schema: Some(format!("cdsync_state_it_{}", Uuid::new_v4().simple())),
        })
    }

    #[tokio::test]
    async fn durable_frontier_scan_advances_replayed_registered_commit() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        crate::state::SyncStateStore::migrate_with_config(&config, 16).await?;
        let store = crate::state::SyncStateStore::open_with_config(&config, 16).await?;
        let state_handle = store.handle("app");
        let now = chrono::Utc::now().timestamp_millis();
        let job = crate::state::CdcBatchLoadJobRecord {
            job_id: "job-replayed-after-feedback-gap".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 10,
            status: crate::state::CdcBatchLoadJobStatus::Succeeded,
            stage: crate::state::CdcLedgerStage::Applied,
            payload_json: "{}".to_string(),
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        let fragment = crate::state::CdcCommitFragmentRecord {
            fragment_id: "job-replayed-after-feedback-gap:10".to_string(),
            job_id: job.job_id.clone(),
            sequence: 10,
            commit_lsn: "0/A".to_string(),
            table_key: job.table_key.clone(),
            status: crate::state::CdcCommitFragmentStatus::Succeeded,
            stage: crate::state::CdcLedgerStage::Applied,
            row_count: 1,
            upserted_count: 1,
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        state_handle
            .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
            .await?;

        let (coordinator_tx, mut advances_rx, state_rx, task) = spawn_cdc_coordinator(10);
        coordinator_tx.send(CdcCoordinatorCommand::RegisterCommit {
            sequence: 10,
            commit_lsn: etl::types::PgLsn::from(0x0A_u64),
            stats: HashMap::new(),
            extract_ms: 0,
            fragment_count: 1,
        })?;

        submit_cdc_durable_frontier_acks(&coordinator_tx, &state_rx, Some(&state_handle)).await?;
        let advance = timeout(Duration::from_secs(1), advances_rx.recv())
            .await?
            .context("durable frontier should advance replayed commit")?;
        assert_eq!(advance.next_sequence_to_ack, 11);
        assert_eq!(advance.commit_lsn, etl::types::PgLsn::from(0x0A_u64));

        drop(coordinator_tx);
        task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn durable_backlog_backpressure_uses_pending_fragment_count() -> anyhow::Result<()> {
        let Some(config) = test_state_config() else {
            return Ok(());
        };
        crate::state::SyncStateStore::migrate_with_config(&config, 16).await?;
        let store = crate::state::SyncStateStore::open_with_config(&config, 16).await?;
        let state_handle = store.handle("app");
        let now = chrono::Utc::now().timestamp_millis();
        let job = crate::state::CdcBatchLoadJobRecord {
            job_id: "job-pending-backpressure".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 10,
            status: crate::state::CdcBatchLoadJobStatus::Pending,
            stage: crate::state::CdcLedgerStage::Loaded,
            payload_json: "{}".to_string(),
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        let fragment = crate::state::CdcCommitFragmentRecord {
            fragment_id: "job-pending-backpressure:10".to_string(),
            job_id: job.job_id.clone(),
            sequence: 10,
            commit_lsn: "0/A".to_string(),
            table_key: job.table_key.clone(),
            status: crate::state::CdcCommitFragmentStatus::Pending,
            stage: crate::state::CdcLedgerStage::Loaded,
            row_count: 1,
            upserted_count: 1,
            created_at: now,
            updated_at: now,
            ..Default::default()
        };
        state_handle
            .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
            .await?;

        assert!(
            cdc_durable_backlog_backpressure_exceeded("slot", Some(&state_handle), Some(1), None)
                .await?
        );
        assert!(
            !cdc_durable_backlog_backpressure_exceeded("slot", Some(&state_handle), Some(2), None)
                .await?
        );

        Ok(())
    }
}
