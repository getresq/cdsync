pub(super) use super::cdc_loop::{
    CdcIdleState, cdc_fill_deadline_reached, cdc_should_stop_after_idle, next_cdc_wait_timeout,
};
#[cfg(test)]
pub(super) use super::cdc_relation::relation_change_requires_destination_ensure;
use super::*;

const CDC_RELATION_PENDING_APPLY_TIMEOUT: Duration =
    crate::destinations::bigquery::BATCH_LOAD_JOB_HARD_TIMEOUT;
const CDC_RELATION_LOCK_TIMEOUT: Duration = Duration::from_secs(120);
const CDC_RELATION_CHANGE_TIMEOUT: Duration = Duration::from_secs(120);
const CDC_IDLE_KEEPALIVE_FEEDBACK_INTERVAL: Duration = Duration::from_secs(10);

fn next_cdc_commit_sequence_from_watermark(
    watermark: Option<&crate::state::CdcWatermarkState>,
) -> u64 {
    watermark.map_or(0, |state| state.next_sequence_to_ack)
}

async fn load_next_cdc_commit_sequence(state_handle: Option<&StateHandle>) -> Result<u64> {
    let watermark = match state_handle {
        Some(state_handle) => state_handle.load_cdc_watermark_state().await?,
        None => None,
    };
    Ok(next_cdc_commit_sequence_from_watermark(watermark.as_ref()))
}

async fn cached_cdc_table_schema(
    store: &MemoryStore,
    cache: &mut HashMap<TableId, Arc<EtlTableSchema>>,
    table_id: TableId,
) -> Result<Option<Arc<EtlTableSchema>>> {
    if let Some(schema) = cache.get(&table_id) {
        return Ok(Some(schema.clone()));
    }

    let schema = store.get_table_schema(&table_id).await?;
    if let Some(schema) = &schema {
        cache.insert(table_id, schema.clone());
    }
    Ok(schema)
}

impl PostgresSource {
    pub(super) async fn stream_cdc_changes(
        &self,
        replication_client: PgReplicationClient,
        config: CdcStreamConfig<'_>,
        runtime: CdcStreamRuntime<'_>,
    ) -> Result<etl::types::PgLsn> {
        let CdcStreamConfig {
            publication,
            slot_name,
            start_lsn,
            pipeline_id,
            idle_timeout,
            max_pending_events,
            apply_batch_size,
            apply_max_fill,
            apply_concurrency,
            max_inflight_commits,
            backlog_max_pending_fragments,
            backlog_max_oldest_pending,
            follow,
            shutdown,
        } = config;
        let include_tables = runtime.include_tables;
        let table_configs = runtime.table_configs;
        let store = runtime.store;
        let dest = runtime.dest;
        let table_info_map = runtime.table_info_map;
        let etl_schemas = runtime.etl_schemas;
        let table_hashes = runtime.table_hashes;
        let table_snapshots = runtime.table_snapshots;
        let state = runtime.state;
        let schema_policy = runtime.schema_policy;
        let schema_diff_enabled = runtime.schema_diff_enabled;
        let stats = runtime.stats;
        let state_handle = runtime.state_handle;
        let logical_stream = replication_client
            .start_logical_replication(publication, slot_name, start_lsn)
            .await?;
        let stream = EventsStream::wrap(logical_stream, pipeline_id);
        tokio::pin!(stream);
        let mut connection_updates_rx = replication_client.connection_updates_rx();

        let mut pending_events: Vec<Event> = Vec::with_capacity(max_pending_events.min(1024));
        let mut pending_stats: HashMap<TableId, usize> = HashMap::new();
        let mut tx_schema_cache: HashMap<TableId, Arc<EtlTableSchema>> = HashMap::new();
        let mut last_received_lsn = start_lsn;
        let mut last_flushed_lsn = start_lsn;
        let mut last_feedback_lsn = start_lsn;
        let mut last_idle_keepalive_feedback = Instant::now();
        let mut last_xlog_activity = Instant::now();
        let mut last_replication_message = Instant::now();
        let mut last_heartbeat_log = Instant::now();
        let mut tx_extract_started_at: Option<Instant> = None;
        let mut next_tx_ordinal = 0u64;
        let mut in_tx = false;
        let mut expected_commit_lsn: Option<etl::types::PgLsn> = None;
        let mut shutdown = shutdown;
        let mut shutdown_requested = false;
        let initial_commit_sequence = load_next_cdc_commit_sequence(state_handle.as_ref()).await?;
        let mut next_commit_sequence = initial_commit_sequence;
        let mut queued_batches: VecDeque<CommittedCdcBatch> = VecDeque::new();
        let mut pending_table_batches: HashMap<TableId, PendingTableApplyBatch> = HashMap::new();
        let mut inflight_dispatch: FuturesUnordered<CdcDispatchFuture> = FuturesUnordered::new();
        let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
        let (coordinator_tx, mut coordinator_advances_rx, coordinator_state_rx, coordinator_task) =
            spawn_cdc_coordinator(initial_commit_sequence);
        let mut table_apply_locks: HashMap<TableId, Arc<Mutex<()>>> = HashMap::new();
        let mut active_table_applies: HashSet<TableId> = HashSet::new();
        let max_active_applies = apply_concurrency.max(1);
        let max_commit_queue_depth = max_inflight_commits.max(1);

        loop {
            if shutdown_requested && !in_tx {
                break;
            }

            let wait_timeout = next_cdc_wait_timeout(
                idle_timeout,
                apply_max_fill,
                &pending_table_batches,
                inflight_dispatch.len(),
                max_active_applies,
            );
            let message = if let Some(shutdown) = shutdown.as_mut() {
                tokio::select! {
                    changed = shutdown.changed(), if !shutdown_requested => {
                        if changed {
                            shutdown_requested = true;
                            if in_tx {
                                continue;
                            }
                            break;
                        }
                        continue;
                    }
                    changed = connection_updates_rx.changed() => {
                        handle_cdc_connection_update(changed, &mut connection_updates_rx, slot_name)?;
                        continue;
                    }
                    result = inflight_dispatch.next(), if !inflight_dispatch.is_empty() => {
                        let acks = handle_cdc_dispatch_result(
                            result,
                            &mut inflight_apply,
                            &mut active_table_applies,
                        )?;
                        submit_cdc_apply_acks(&coordinator_tx, acks)?;
                        drain_ready_cdc_coordinator_advances(
                            &coordinator_tx,
                            &mut coordinator_advances_rx,
                            &coordinator_state_rx,
                            &stats,
                            table_configs,
                            state,
                            state_handle.as_ref(),
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        dispatch_cdc_batches_and_record(
                            slot_name,
                            pending_events.len(),
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_dispatch,
                            &coordinator_tx,
                            &coordinator_state_rx,
                            dest,
                            &mut CdcApplyCoordination {
                                table_apply_locks: &mut table_apply_locks,
                                active_table_applies: &mut active_table_applies,
                            },
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        )?;
                        continue;
                    }
                    result = inflight_apply.next(), if !inflight_apply.is_empty() => {
                        let acks = handle_cdc_apply_result(
                            result,
                            &mut active_table_applies,
                        )?;
                        submit_cdc_apply_acks(&coordinator_tx, acks)?;
                        drain_ready_cdc_coordinator_advances(
                            &coordinator_tx,
                            &mut coordinator_advances_rx,
                            &coordinator_state_rx,
                            &stats,
                            table_configs,
                            state,
                            state_handle.as_ref(),
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        dispatch_cdc_batches_and_record(
                            slot_name,
                            pending_events.len(),
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_dispatch,
                            &coordinator_tx,
                            &coordinator_state_rx,
                            dest,
                            &mut CdcApplyCoordination {
                                table_apply_locks: &mut table_apply_locks,
                                active_table_applies: &mut active_table_applies,
                            },
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        )?;
                        continue;
                    }
                    advance = coordinator_advances_rx.recv() => {
                        let advance = advance.context("CDC coordinator task stopped")?;
                        apply_cdc_watermark_advance(
                            advance,
                            &mut CdcWatermarkRuntime {
                                stats: &stats,
                                table_configs,
                                state,
                                state_handle: state_handle.as_ref(),
                            },
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        continue;
                    }
                    result = timeout(wait_timeout, stream.next()) => {
                        match result {
                            Ok(Some(msg)) => msg?,
                            Ok(None) => break,
                            Err(_) => {
                                if cdc_fill_deadline_reached(apply_max_fill, &pending_table_batches) {
                                    dispatch_cdc_batches_and_record(
                                        slot_name,
                                        pending_events.len(),
                                        &mut queued_batches,
                                        &mut pending_table_batches,
                                        &mut inflight_dispatch,
                                        &coordinator_tx,
                                        &coordinator_state_rx,
                                        dest,
                                        &mut CdcApplyCoordination {
                                            table_apply_locks: &mut table_apply_locks,
                                            active_table_applies: &mut active_table_applies,
                                        },
                                        CdcDispatchConfig {
                                            max_active_applies,
                                            apply_batch_size,
                                            max_fill: apply_max_fill,
                                            force_flush: true,
                                        },
                                    )?;
                                    continue;
                                }
                                if in_tx {
                                    maybe_log_cdc_wait_timeout(
                                        slot_name,
                                        &mut last_heartbeat_log,
                                        last_replication_message,
                                        last_xlog_activity,
                                        last_received_lsn,
                                        last_flushed_lsn,
                                        pending_events.len(),
                                        queued_batches.len(),
                                        pending_table_batches.len(),
                                        inflight_dispatch.len() + inflight_apply.len(),
                                        active_table_applies.len(),
                                        coordinator_inflight_commits(&coordinator_state_rx),
                                        in_tx,
                                        follow,
                                        "open_transaction",
                                    );
                                    continue;
                                }
                                if follow {
                                    maybe_log_cdc_wait_timeout(
                                        slot_name,
                                        &mut last_heartbeat_log,
                                        last_replication_message,
                                        last_xlog_activity,
                                        last_received_lsn,
                                        last_flushed_lsn,
                                        pending_events.len(),
                                        queued_batches.len(),
                                        pending_table_batches.len(),
                                        inflight_dispatch.len() + inflight_apply.len(),
                                        active_table_applies.len(),
                                        coordinator_inflight_commits(&coordinator_state_rx),
                                        in_tx,
                                        follow,
                                        "follow_idle",
                                    );
                                    continue;
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                tokio::select! {
                    result = inflight_dispatch.next(), if !inflight_dispatch.is_empty() => {
                        let acks = handle_cdc_dispatch_result(
                            result,
                            &mut inflight_apply,
                            &mut active_table_applies,
                        )?;
                        submit_cdc_apply_acks(&coordinator_tx, acks)?;
                        drain_ready_cdc_coordinator_advances(
                            &coordinator_tx,
                            &mut coordinator_advances_rx,
                            &coordinator_state_rx,
                            &stats,
                            table_configs,
                            state,
                            state_handle.as_ref(),
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        dispatch_cdc_batches_and_record(
                            slot_name,
                            pending_events.len(),
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_dispatch,
                            &coordinator_tx,
                            &coordinator_state_rx,
                            dest,
                            &mut CdcApplyCoordination {
                                table_apply_locks: &mut table_apply_locks,
                                active_table_applies: &mut active_table_applies,
                            },
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        )?;
                        continue;
                    }
                    result = inflight_apply.next(), if !inflight_apply.is_empty() => {
                        let acks = handle_cdc_apply_result(
                            result,
                            &mut active_table_applies,
                        )?;
                        submit_cdc_apply_acks(&coordinator_tx, acks)?;
                        drain_ready_cdc_coordinator_advances(
                            &coordinator_tx,
                            &mut coordinator_advances_rx,
                            &coordinator_state_rx,
                            &stats,
                            table_configs,
                            state,
                            state_handle.as_ref(),
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        dispatch_cdc_batches_and_record(
                            slot_name,
                            pending_events.len(),
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_dispatch,
                            &coordinator_tx,
                            &coordinator_state_rx,
                            dest,
                            &mut CdcApplyCoordination {
                                table_apply_locks: &mut table_apply_locks,
                                active_table_applies: &mut active_table_applies,
                            },
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        )?;
                        continue;
                    }
                    advance = coordinator_advances_rx.recv() => {
                        let advance = advance.context("CDC coordinator task stopped")?;
                        apply_cdc_watermark_advance(
                            advance,
                            &mut CdcWatermarkRuntime {
                                stats: &stats,
                                table_configs,
                                state,
                                state_handle: state_handle.as_ref(),
                            },
                            stream.as_mut(),
                            last_received_lsn,
                            &mut last_flushed_lsn,
                        )
                        .await?;
                        continue;
                    }
                    changed = connection_updates_rx.changed() => {
                        handle_cdc_connection_update(changed, &mut connection_updates_rx, slot_name)?;
                        continue;
                    }
                    result = timeout(wait_timeout, stream.next()) => match result {
                    Ok(Some(msg)) => msg?,
                    Ok(None) => break,
                    Err(_) => {
                        if cdc_fill_deadline_reached(apply_max_fill, &pending_table_batches) {
                            dispatch_cdc_batches_and_record(
                                slot_name,
                                pending_events.len(),
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_dispatch,
                                &coordinator_tx,
                                &coordinator_state_rx,
                                dest,
                                &mut CdcApplyCoordination {
                                    table_apply_locks: &mut table_apply_locks,
                                    active_table_applies: &mut active_table_applies,
                                },
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: true,
                                },
                            )?;
                            continue;
                        }
                        if in_tx {
                            maybe_log_cdc_wait_timeout(
                                slot_name,
                                &mut last_heartbeat_log,
                                last_replication_message,
                                last_xlog_activity,
                                last_received_lsn,
                                last_flushed_lsn,
                                pending_events.len(),
                                queued_batches.len(),
                                pending_table_batches.len(),
                                inflight_dispatch.len() + inflight_apply.len(),
                                active_table_applies.len(),
                                coordinator_inflight_commits(&coordinator_state_rx),
                                in_tx,
                                follow,
                                "open_transaction",
                            );
                            continue;
                        }
                        if follow {
                            maybe_log_cdc_wait_timeout(
                                slot_name,
                                &mut last_heartbeat_log,
                                last_replication_message,
                                last_xlog_activity,
                                last_received_lsn,
                                last_flushed_lsn,
                                pending_events.len(),
                                queued_batches.len(),
                                pending_table_batches.len(),
                                inflight_dispatch.len() + inflight_apply.len(),
                                active_table_applies.len(),
                                coordinator_inflight_commits(&coordinator_state_rx),
                                in_tx,
                                follow,
                                "follow_idle",
                            );
                            continue;
                        }
                        break;
                    }
                }}
            };

            let replication_idle_for = last_replication_message.elapsed();
            last_replication_message = Instant::now();
            log_replication_message_after_idle(
                slot_name,
                replication_idle_for,
                last_received_lsn,
                last_flushed_lsn,
            );

            match message {
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    let wal_end = etl::types::PgLsn::from(keepalive.wal_end());
                    if wal_end > last_received_lsn {
                        last_received_lsn = wal_end;
                    }
                    let coordinator_inflight_commits =
                        coordinator_inflight_commits(&coordinator_state_rx);
                    let in_memory_idle = !in_tx
                        && pending_events.is_empty()
                        && queued_batches.is_empty()
                        && pending_table_batches.is_empty()
                        && inflight_dispatch.is_empty()
                        && inflight_apply.is_empty()
                        && active_table_applies.is_empty()
                        && coordinator_inflight_commits == 0;
                    let durable_backlog_empty = if in_memory_idle {
                        cdc_durable_idle_backlog_empty(slot_name, state_handle.as_ref()).await
                    } else {
                        false
                    };
                    let feedback_lsn = cdc_keepalive_feedback_lsn(
                        last_received_lsn,
                        last_flushed_lsn,
                        &CdcKeepaliveFeedbackState {
                            in_tx,
                            pending_events_empty: pending_events.is_empty(),
                            queued_batches_empty: queued_batches.is_empty(),
                            pending_table_batches_empty: pending_table_batches.is_empty(),
                            inflight_dispatch_empty: inflight_dispatch.is_empty(),
                            inflight_apply_empty: inflight_apply.is_empty(),
                            active_table_applies_empty: active_table_applies.is_empty(),
                            coordinator_inflight_commits,
                            durable_backlog_empty,
                        },
                    );
                    let force_keepalive_reply = keepalive.reply() == 1;
                    let idle_feedback_due = feedback_lsn > last_feedback_lsn
                        && last_idle_keepalive_feedback.elapsed()
                            >= CDC_IDLE_KEEPALIVE_FEEDBACK_INTERVAL;
                    if force_keepalive_reply || idle_feedback_due {
                        handle_primary_keepalive_reply(
                            slot_name,
                            wal_end,
                            last_received_lsn,
                            last_flushed_lsn,
                            &mut last_feedback_lsn,
                            feedback_lsn,
                            force_keepalive_reply,
                            stream.as_mut(),
                            state_handle.as_ref(),
                        )
                        .await?;
                        last_idle_keepalive_feedback = Instant::now();
                    }
                }
                ReplicationMessage::XLogData(xlog) => {
                    last_xlog_activity = Instant::now();
                    let start = etl::types::PgLsn::from(xlog.wal_start());
                    let end = etl::types::PgLsn::from(xlog.wal_end());
                    if end > last_received_lsn {
                        last_received_lsn = end;
                    }

                    match xlog.data() {
                        LogicalReplicationMessage::Begin(begin) => {
                            in_tx = true;
                            tx_extract_started_at = Some(Instant::now());
                            next_tx_ordinal = 0;
                            expected_commit_lsn = Some(etl::types::PgLsn::from(begin.final_lsn()));
                            pending_events.clear();
                            pending_stats.clear();
                            tx_schema_cache.clear();
                        }
                        LogicalReplicationMessage::Commit(commit) => {
                            let commit_lsn = etl::types::PgLsn::from(commit.commit_lsn());
                            if let Some(expected) = expected_commit_lsn.take()
                                && expected != commit_lsn
                            {
                                warn!(
                                    expected = %expected,
                                    actual = %commit_lsn,
                                    "commit lsn mismatch"
                                );
                            }

                            let table_batches =
                                split_commit_events_by_table(std::mem::take(&mut pending_events));
                            let stats_by_table = std::mem::take(&mut pending_stats);
                            tx_schema_cache.clear();
                            let extract_ms = tx_extract_started_at
                                .take()
                                .map(|started_at| started_at.elapsed().as_millis() as u64)
                                .unwrap_or_default();
                            queued_batches.push_back(CommittedCdcBatch {
                                sequence: next_commit_sequence,
                                commit_lsn,
                                table_batches,
                                stats: stats_by_table,
                                extract_ms,
                            });
                            next_commit_sequence += 1;

                            dispatch_cdc_batches_and_record(
                                slot_name,
                                pending_events.len(),
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_dispatch,
                                &coordinator_tx,
                                &coordinator_state_rx,
                                dest,
                                &mut CdcApplyCoordination {
                                    table_apply_locks: &mut table_apply_locks,
                                    active_table_applies: &mut active_table_applies,
                                },
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: false,
                                },
                            )?;

                            while coordinator_inflight_commits(&coordinator_state_rx)
                                >= max_commit_queue_depth
                            {
                                crate::telemetry::record_cdc_backpressure_wait(
                                    slot_name,
                                    coordinator_inflight_commits(&coordinator_state_rx) as u64,
                                    max_commit_queue_depth as u64,
                                );
                                dispatch_cdc_batches_and_record(
                                    slot_name,
                                    pending_events.len(),
                                    &mut queued_batches,
                                    &mut pending_table_batches,
                                    &mut inflight_dispatch,
                                    &coordinator_tx,
                                    &coordinator_state_rx,
                                    dest,
                                    &mut CdcApplyCoordination {
                                        table_apply_locks: &mut table_apply_locks,
                                        active_table_applies: &mut active_table_applies,
                                    },
                                    CdcDispatchConfig {
                                        max_active_applies,
                                        apply_batch_size,
                                        max_fill: apply_max_fill,
                                        force_flush: true,
                                    },
                                )?;
                                let acks = drain_one_cdc_work(
                                    &mut inflight_dispatch,
                                    &mut inflight_apply,
                                    &mut active_table_applies,
                                )
                                .await?;
                                submit_cdc_apply_acks(&coordinator_tx, acks)?;
                                drain_ready_cdc_coordinator_advances(
                                    &coordinator_tx,
                                    &mut coordinator_advances_rx,
                                    &coordinator_state_rx,
                                    &stats,
                                    table_configs,
                                    state,
                                    state_handle.as_ref(),
                                    stream.as_mut(),
                                    last_received_lsn,
                                    &mut last_flushed_lsn,
                                )
                                .await?;
                            }
                            while cdc_backlog_backpressure_exceeded(
                                slot_name,
                                state_handle.as_ref(),
                                backlog_max_pending_fragments,
                                backlog_max_oldest_pending,
                            )
                            .await?
                            {
                                dispatch_cdc_batches_and_record(
                                    slot_name,
                                    pending_events.len(),
                                    &mut queued_batches,
                                    &mut pending_table_batches,
                                    &mut inflight_dispatch,
                                    &coordinator_tx,
                                    &coordinator_state_rx,
                                    dest,
                                    &mut CdcApplyCoordination {
                                        table_apply_locks: &mut table_apply_locks,
                                        active_table_applies: &mut active_table_applies,
                                    },
                                    CdcDispatchConfig {
                                        max_active_applies,
                                        apply_batch_size,
                                        max_fill: apply_max_fill,
                                        force_flush: true,
                                    },
                                )?;
                                if inflight_dispatch.is_empty() && inflight_apply.is_empty() {
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                } else {
                                    let acks = drain_one_cdc_work(
                                        &mut inflight_dispatch,
                                        &mut inflight_apply,
                                        &mut active_table_applies,
                                    )
                                    .await?;
                                    submit_cdc_apply_acks(&coordinator_tx, acks)?;
                                }
                                drain_ready_cdc_coordinator_advances(
                                    &coordinator_tx,
                                    &mut coordinator_advances_rx,
                                    &coordinator_state_rx,
                                    &stats,
                                    table_configs,
                                    state,
                                    state_handle.as_ref(),
                                    stream.as_mut(),
                                    last_received_lsn,
                                    &mut last_flushed_lsn,
                                )
                                .await?;
                            }
                            in_tx = false;
                        }
                        LogicalReplicationMessage::Relation(relation) => {
                            let table_id = TableId::new(relation.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            info!(
                                table_id = table_id.into_inner(),
                                "processing cdc relation change"
                            );
                            dispatch_cdc_batches_and_record(
                                slot_name,
                                pending_events.len(),
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_dispatch,
                                &coordinator_tx,
                                &coordinator_state_rx,
                                dest,
                                &mut CdcApplyCoordination {
                                    table_apply_locks: &mut table_apply_locks,
                                    active_table_applies: &mut active_table_applies,
                                },
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: false,
                                },
                            )?;
                            let table_lock = table_apply_locks
                                .entry(table_id)
                                .or_insert_with(|| Arc::new(Mutex::new(())))
                                .clone();
                            let relation_table_active = active_table_applies.contains(&table_id);
                            let relation_lock_busy = table_lock.try_lock().is_err();
                            if relation_table_active || relation_lock_busy {
                                info!(
                                    component = "coordinator",
                                    event = "cdc_relation_waiting_table_apply_lock",
                                    slot_name = slot_name,
                                    table_id = table_id.into_inner(),
                                    relation_table_active,
                                    relation_lock_busy,
                                    timeout_secs = CDC_RELATION_LOCK_TIMEOUT.as_secs(),
                                    "waiting for CDC table apply lock before relation change"
                                );
                            }
                            let relation_lock_started_at = Instant::now();
                            let _guard = loop {
                                drain_ready_cdc_coordinator_advances(
                                    &coordinator_tx,
                                    &mut coordinator_advances_rx,
                                    &coordinator_state_rx,
                                    &stats,
                                    table_configs,
                                    state,
                                    state_handle.as_ref(),
                                    stream.as_mut(),
                                    last_received_lsn,
                                    &mut last_flushed_lsn,
                                )
                                .await?;
                                let elapsed = relation_lock_started_at.elapsed();
                                if elapsed >= CDC_RELATION_LOCK_TIMEOUT {
                                    anyhow::bail!(
                                        "waiting for CDC table apply lock before relation change for {} timed out after {}s",
                                        table_id.into_inner(),
                                        CDC_RELATION_LOCK_TIMEOUT.as_secs()
                                    );
                                }
                                match wait_for_table_apply_lock_or_cdc_progress(
                                    table_id,
                                    &table_lock,
                                    &mut inflight_dispatch,
                                    &mut inflight_apply,
                                    &mut active_table_applies,
                                    &mut coordinator_advances_rx,
                                    CDC_RELATION_LOCK_TIMEOUT.saturating_sub(elapsed),
                                )
                                .await
                                .with_context(|| {
                                    format!(
                                        "waiting for CDC table apply lock before relation change for {}",
                                        table_id.into_inner()
                                    )
                                })? {
                                    CdcRelationLockWaitProgress::Acquired(guard) => break guard,
                                    CdcRelationLockWaitProgress::ApplyAcks(acks) => {
                                        submit_cdc_apply_acks(&coordinator_tx, acks)?;
                                    }
                                    CdcRelationLockWaitProgress::WatermarkAdvance(advance) => {
                                        apply_cdc_watermark_advance(
                                            advance,
                                            &mut CdcWatermarkRuntime {
                                                stats: &stats,
                                                table_configs,
                                                state,
                                                state_handle: state_handle.as_ref(),
                                            },
                                            stream.as_mut(),
                                            last_received_lsn,
                                            &mut last_flushed_lsn,
                                        )
                                        .await?;
                                    }
                                }
                            };
                            if let Some(pending_batch) = pending_table_batches.remove(&table_id) {
                                let buffered_event_count = pending_batch.events.len();
                                await_cdc_timeout(
                                    format!(
                                        "applying buffered CDC table batch for {} before schema update",
                                        table_id.into_inner()
                                    ),
                                    CDC_RELATION_PENDING_APPLY_TIMEOUT,
                                    dest.write_table_events(table_id, pending_batch.events),
                                )
                                .await
                                .map_err(|err| {
                                    anyhow::anyhow!(
                                        "applying buffered CDC table batch for {} before schema update failed after {} buffered events: {}",
                                        table_id.into_inner(),
                                        buffered_event_count,
                                        err
                                    )
                                })?;
                                submit_cdc_apply_acks(
                                    &coordinator_tx,
                                    vec![CdcApplyFragmentAck {
                                        sequences: pending_batch
                                            .fragments
                                            .into_iter()
                                            .map(|fragment| fragment.sequence)
                                            .collect(),
                                        released_table: None,
                                    }],
                                )?;
                                drain_ready_cdc_coordinator_advances(
                                    &coordinator_tx,
                                    &mut coordinator_advances_rx,
                                    &coordinator_state_rx,
                                    &stats,
                                    table_configs,
                                    state,
                                    state_handle.as_ref(),
                                    stream.as_mut(),
                                    last_received_lsn,
                                    &mut last_flushed_lsn,
                                )
                                .await?;
                            }
                            let mut relation_runtime = CdcRelationRuntime {
                                table_configs,
                                store,
                                dest,
                                table_info_map: &mut *table_info_map,
                                etl_schemas: &mut *etl_schemas,
                                table_hashes: &mut *table_hashes,
                                table_snapshots: &mut *table_snapshots,
                                state: &mut *state,
                                schema_policy: schema_policy.clone(),
                                schema_diff_enabled,
                                state_handle: state_handle.clone(),
                            };
                            await_cdc_timeout(
                                format!("handling relation change for {}", table_id.into_inner()),
                                CDC_RELATION_CHANGE_TIMEOUT,
                                self.handle_relation_change(table_id, &mut relation_runtime),
                            )
                            .await?;
                            info!(
                                table_id = table_id.into_inner(),
                                "cdc relation change applied"
                            );
                        }
                        LogicalReplicationMessage::Insert(insert) => {
                            let table_id = TableId::new(insert.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let tx_ordinal = next_tx_ordinal;
                            next_tx_ordinal = next_tx_ordinal.saturating_add(1);
                            let schema =
                                cached_cdc_table_schema(store, &mut tx_schema_cache, table_id)
                                    .await?
                                    .context("missing schema for insert")?;
                            let table_row = tuple_to_row(
                                &schema.column_schemas,
                                insert.tuple().tuple_data(),
                                None,
                            )?;
                            let event = InsertEvent {
                                start_lsn: start,
                                commit_lsn,
                                tx_ordinal,
                                table_id,
                                table_row,
                            };
                            pending_events.push(Event::Insert(event));
                        }
                        LogicalReplicationMessage::Update(update) => {
                            let table_id = TableId::new(update.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let tx_ordinal = next_tx_ordinal;
                            next_tx_ordinal = next_tx_ordinal.saturating_add(1);
                            let schema =
                                cached_cdc_table_schema(store, &mut tx_schema_cache, table_id)
                                    .await?
                                    .context("missing schema for update")?;
                            let is_key = update.old_tuple().is_none();
                            let old_tuple = update.old_tuple().or(update.key_tuple());
                            let old_table_row = old_tuple
                                .map(|identity| {
                                    tuple_to_row(
                                        &schema.column_schemas,
                                        identity.tuple_data(),
                                        None,
                                    )
                                    .map(|row| (is_key, row))
                                })
                                .transpose()?;
                            let table_row = tuple_to_row(
                                &schema.column_schemas,
                                update.new_tuple().tuple_data(),
                                old_table_row.as_ref().map(|(_, row)| row),
                            )?;
                            let event = UpdateEvent {
                                start_lsn: start,
                                commit_lsn,
                                tx_ordinal,
                                table_id,
                                table_row,
                                old_table_row,
                            };
                            pending_events.push(Event::Update(event));
                        }
                        LogicalReplicationMessage::Delete(delete) => {
                            let table_id = TableId::new(delete.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            if pending_events.len() >= max_pending_events {
                                anyhow::bail!(
                                    "CDC transaction exceeds {} events; reduce transaction size or increase postgres.cdc_max_pending_events",
                                    max_pending_events
                                );
                            }
                            *pending_stats.entry(table_id).or_insert(0) += 1;
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let tx_ordinal = next_tx_ordinal;
                            next_tx_ordinal = next_tx_ordinal.saturating_add(1);
                            let schema =
                                cached_cdc_table_schema(store, &mut tx_schema_cache, table_id)
                                    .await?
                                    .context("missing schema for delete")?;
                            let is_key = delete.old_tuple().is_none();
                            let old_tuple = delete.old_tuple().or(delete.key_tuple());
                            let old_table_row = old_tuple
                                .map(|identity| {
                                    tuple_to_row(
                                        &schema.column_schemas,
                                        identity.tuple_data(),
                                        None,
                                    )
                                    .map(|row| (is_key, row))
                                })
                                .transpose()?;
                            let event = DeleteEvent {
                                start_lsn: start,
                                commit_lsn,
                                tx_ordinal,
                                table_id,
                                old_table_row,
                            };
                            pending_events.push(Event::Delete(event));
                        }
                        LogicalReplicationMessage::Truncate(truncate) => {
                            let rel_ids = truncate.rel_ids().to_vec();
                            if !rel_ids
                                .iter()
                                .any(|id| include_tables.contains(&TableId::new(*id)))
                            {
                                continue;
                            }
                            let commit_lsn = expected_commit_lsn.unwrap_or(start);
                            let tx_ordinal = next_tx_ordinal;
                            next_tx_ordinal = next_tx_ordinal.saturating_add(1);
                            let event = TruncateEvent {
                                start_lsn: start,
                                commit_lsn,
                                tx_ordinal,
                                options: truncate.options(),
                                rel_ids,
                            };
                            pending_events.push(Event::Truncate(event));
                        }
                        LogicalReplicationMessage::Origin(_)
                        | LogicalReplicationMessage::Type(_) => {}
                        _ => {}
                    }
                }
                _ => {}
            }

            let coordinator_state = coordinator_state_rx.borrow().clone();
            maybe_log_cdc_loop_heartbeat(
                slot_name,
                &mut last_heartbeat_log,
                last_received_lsn,
                last_flushed_lsn,
                pending_events.len(),
                &pending_stats,
                table_configs,
                queued_batches.len(),
                pending_table_batches.len(),
                inflight_dispatch.len() + inflight_apply.len(),
                active_table_applies.len(),
                &coordinator_state,
                in_tx,
                expected_commit_lsn,
                last_xlog_activity,
            );

            while let Some(Some(result)) = inflight_dispatch.next().now_or_never() {
                let acks = handle_cdc_dispatch_result(
                    Some(result),
                    &mut inflight_apply,
                    &mut active_table_applies,
                )?;
                submit_cdc_apply_acks(&coordinator_tx, acks)?;
            }
            drain_ready_cdc_coordinator_advances(
                &coordinator_tx,
                &mut coordinator_advances_rx,
                &coordinator_state_rx,
                &stats,
                table_configs,
                state,
                state_handle.as_ref(),
                stream.as_mut(),
                last_received_lsn,
                &mut last_flushed_lsn,
            )
            .await?;

            while let Some(Some(result)) = inflight_apply.next().now_or_never() {
                let acks = handle_cdc_apply_result(Some(result), &mut active_table_applies)?;
                submit_cdc_apply_acks(&coordinator_tx, acks)?;
            }
            drain_ready_cdc_coordinator_advances(
                &coordinator_tx,
                &mut coordinator_advances_rx,
                &coordinator_state_rx,
                &stats,
                table_configs,
                state,
                state_handle.as_ref(),
                stream.as_mut(),
                last_received_lsn,
                &mut last_flushed_lsn,
            )
            .await?;

            dispatch_cdc_batches_and_record(
                slot_name,
                pending_events.len(),
                &mut queued_batches,
                &mut pending_table_batches,
                &mut inflight_dispatch,
                &coordinator_tx,
                &coordinator_state_rx,
                dest,
                &mut CdcApplyCoordination {
                    table_apply_locks: &mut table_apply_locks,
                    active_table_applies: &mut active_table_applies,
                },
                CdcDispatchConfig {
                    max_active_applies,
                    apply_batch_size,
                    max_fill: apply_max_fill,
                    force_flush: false,
                },
            )?;

            let idle_state = CdcIdleState {
                follow,
                in_tx,
                pending_events_empty: pending_events.is_empty(),
                queued_batches_empty: queued_batches.is_empty(),
                pending_table_batches_empty: pending_table_batches.is_empty(),
                inflight_apply_empty: inflight_dispatch.is_empty() && inflight_apply.is_empty(),
            };
            if cdc_should_stop_after_idle(&idle_state, last_xlog_activity, idle_timeout) {
                break;
            }
        }

        finalize_cdc_runtime(
            slot_name,
            pending_events.len(),
            &mut queued_batches,
            &mut pending_table_batches,
            &mut inflight_dispatch,
            &mut inflight_apply,
            coordinator_tx,
            &mut coordinator_advances_rx,
            &coordinator_state_rx,
            coordinator_task,
            dest,
            &mut CdcApplyCoordination {
                table_apply_locks: &mut table_apply_locks,
                active_table_applies: &mut active_table_applies,
            },
            CdcDispatchConfig {
                max_active_applies,
                apply_batch_size,
                max_fill: apply_max_fill,
                force_flush: false,
            },
            &stats,
            table_configs,
            state,
            state_handle.as_ref(),
            stream.as_mut(),
            last_received_lsn,
            &mut last_flushed_lsn,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_cdc_commit_sequence_uses_next_sequence_to_ack() {
        let watermark = crate::state::CdcWatermarkState {
            next_sequence_to_ack: 41,
            last_enqueued_sequence: Some(99),
            ..Default::default()
        };
        assert_eq!(
            next_cdc_commit_sequence_from_watermark(Some(&watermark)),
            41
        );
        assert_eq!(next_cdc_commit_sequence_from_watermark(None), 0);
    }
}
