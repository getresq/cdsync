use super::*;

pub(super) const CDC_STATUS_UPDATE_TIMEOUT: Duration = Duration::from_secs(30);
const CDC_STATE_SAVE_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) struct CommittedCdcBatch {
    pub(super) sequence: u64,
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) table_batches: Vec<CdcTableApplyBatch>,
    pub(super) stats: HashMap<TableId, usize>,
    pub(super) extract_ms: u64,
}

pub(super) struct CdcTableApplyBatch {
    pub(super) table_id: TableId,
    pub(super) events: Vec<Event>,
}

pub(super) struct CdcApplyFragmentAck {
    pub(super) sequences: Vec<u64>,
    pub(super) released_table: Option<TableId>,
}

pub(super) struct CommitTrackerEntry {
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) remaining_fragments: usize,
    pub(super) stats: HashMap<TableId, usize>,
    pub(super) extract_ms: u64,
}

pub(super) struct CdcWatermarkAdvance {
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) stats: HashMap<TableId, usize>,
    pub(super) extract_ms: u64,
}

#[derive(Default)]
pub(super) struct CdcWatermarkTracker {
    pub(super) next_sequence: u64,
    pub(super) commits: BTreeMap<u64, CommitTrackerEntry>,
}

pub(super) struct CdcWatermarkRuntime<'a> {
    pub(super) stats: &'a Option<StatsHandle>,
    pub(super) table_configs: &'a HashMap<TableId, ResolvedPostgresTable>,
    pub(super) state: &'a mut ConnectionState,
    pub(super) state_handle: Option<&'a StateHandle>,
}

pub(super) struct PendingTableApplyBatch {
    pub(super) sequences: Vec<u64>,
    pub(super) events: Vec<Event>,
    pub(super) event_count: usize,
    pub(super) first_buffered_at: Instant,
}

pub(super) struct CdcDispatchConfig {
    pub(super) max_active_applies: usize,
    pub(super) apply_batch_size: usize,
    pub(super) max_fill: Duration,
    pub(super) force_flush: bool,
}

pub(super) struct CdcApplyCoordination<'a> {
    pub(super) table_apply_locks: &'a mut HashMap<TableId, Arc<Mutex<()>>>,
    pub(super) active_table_applies: &'a mut HashSet<TableId>,
}

impl CdcWatermarkTracker {
    pub(super) fn inflight_commits(&self) -> usize {
        self.commits.len()
    }

    pub(super) fn register_commit(
        &mut self,
        sequence: u64,
        commit_lsn: etl::types::PgLsn,
        stats: HashMap<TableId, usize>,
        extract_ms: u64,
        fragment_count: usize,
    ) {
        self.commits.insert(
            sequence,
            CommitTrackerEntry {
                commit_lsn,
                remaining_fragments: fragment_count,
                stats,
                extract_ms,
            },
        );
    }

    pub(super) fn complete_fragment(
        &mut self,
        sequence: u64,
    ) -> Result<Option<CdcWatermarkAdvance>> {
        let entry = self
            .commits
            .get_mut(&sequence)
            .context("missing CDC commit tracker entry")?;
        if entry.remaining_fragments == 0 {
            anyhow::bail!("duplicate CDC apply ack for sequence {}", sequence);
        }
        entry.remaining_fragments -= 1;
        Ok(self.advance_ready())
    }

    fn advance_ready(&mut self) -> Option<CdcWatermarkAdvance> {
        let mut last_lsn = None;
        let mut stats = HashMap::new();
        let mut extract_ms = 0u64;

        while let Some(entry) = self.commits.get(&self.next_sequence) {
            if entry.remaining_fragments != 0 {
                break;
            }
            let entry = self.commits.remove(&self.next_sequence)?;
            last_lsn = Some(entry.commit_lsn);
            for (table_id, count) in entry.stats {
                *stats.entry(table_id).or_insert(0) += count;
            }
            extract_ms = extract_ms.saturating_add(entry.extract_ms);
            self.next_sequence += 1;
        }

        last_lsn.map(|commit_lsn| CdcWatermarkAdvance {
            commit_lsn,
            stats,
            extract_ms,
        })
    }
}

pub(super) fn split_commit_events_by_table(events: Vec<Event>) -> Vec<CdcTableApplyBatch> {
    let mut order = Vec::new();
    let mut table_batches: HashMap<TableId, Vec<Event>> = HashMap::new();

    let mut push_event = |table_id: TableId, event: Event| {
        let entry = table_batches.entry(table_id).or_insert_with(|| {
            order.push(table_id);
            Vec::new()
        });
        entry.push(event);
    };

    for event in events {
        match event {
            Event::Insert(insert) => push_event(insert.table_id, Event::Insert(insert)),
            Event::Update(update) => push_event(update.table_id, Event::Update(update)),
            Event::Delete(delete) => push_event(delete.table_id, Event::Delete(delete)),
            Event::Truncate(truncate) => {
                for rel_id in truncate.rel_ids {
                    let table_id = TableId::new(rel_id);
                    push_event(
                        table_id,
                        Event::Truncate(TruncateEvent {
                            start_lsn: truncate.start_lsn,
                            commit_lsn: truncate.commit_lsn,
                            tx_ordinal: truncate.tx_ordinal,
                            options: truncate.options,
                            rel_ids: vec![rel_id],
                        }),
                    );
                }
            }
            Event::Begin(_) | Event::Commit(_) | Event::Relation(_) | Event::Unsupported => {}
        }
    }

    order
        .into_iter()
        .filter_map(|table_id| {
            table_batches
                .remove(&table_id)
                .map(|events| CdcTableApplyBatch { table_id, events })
        })
        .collect()
}

pub(super) fn dispatch_cdc_batches(
    queued_batches: &mut VecDeque<CommittedCdcBatch>,
    pending_table_batches: &mut HashMap<TableId, PendingTableApplyBatch>,
    inflight_dispatch: &mut FuturesUnordered<CdcDispatchFuture>,
    watermark_tracker: &mut CdcWatermarkTracker,
    dest: &EtlBigQueryDestination,
    coordination: &mut CdcApplyCoordination<'_>,
    config: CdcDispatchConfig,
) {
    while let Some(batch) = queued_batches.pop_front() {
        let table_count = batch.table_batches.len();
        let event_count: usize = batch
            .table_batches
            .iter()
            .map(|table_batch| table_batch.events.len())
            .sum();
        let fragment_count = batch.table_batches.len().max(1);
        info!(
            sequence = batch.sequence,
            commit_lsn = %batch.commit_lsn,
            table_count,
            event_count,
            fragment_count,
            "cdc commit queued for dispatch"
        );
        watermark_tracker.register_commit(
            batch.sequence,
            batch.commit_lsn,
            batch.stats,
            batch.extract_ms,
            fragment_count,
        );

        if batch.table_batches.is_empty() {
            let sequences = vec![batch.sequence];
            inflight_dispatch.push(Box::pin(async move {
                Ok(CdcDispatchResult::Immediate(CdcApplyFragmentAck {
                    sequences,
                    released_table: None,
                }))
            }));
            continue;
        }

        for table_batch in batch.table_batches {
            let event_count = table_batch.events.len().max(1);
            let pending = pending_table_batches
                .entry(table_batch.table_id)
                .or_insert_with(|| PendingTableApplyBatch {
                    sequences: Vec::new(),
                    events: Vec::new(),
                    event_count: 0,
                    first_buffered_at: Instant::now(),
                });
            pending.sequences.push(batch.sequence);
            pending.event_count += event_count;
            pending.events.extend(table_batch.events);
        }
    }

    let now = Instant::now();
    let mut ready_tables: Vec<(u64, TableId)> = pending_table_batches
        .iter()
        .filter_map(|(table_id, pending)| {
            let should_flush = config.force_flush
                || pending.event_count >= config.apply_batch_size
                || now.duration_since(pending.first_buffered_at) >= config.max_fill;
            should_flush.then_some((
                pending.sequences.first().copied().unwrap_or_default(),
                *table_id,
            ))
        })
        .collect();
    ready_tables.sort_by_key(|(sequence, _)| *sequence);

    for (_, table_id) in ready_tables {
        if inflight_dispatch.len() >= config.max_active_applies {
            break;
        }
        if coordination.active_table_applies.contains(&table_id) {
            continue;
        }
        let pending = pending_table_batches
            .remove(&table_id)
            .expect("pending batch missing for ready table");
        let table_lock = coordination
            .table_apply_locks
            .entry(table_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let apply_dest = dest.clone();
        let sequences = pending.sequences;
        let events = pending.events;
        let sequence_count = sequences.len();
        let event_count = events.len();
        coordination.active_table_applies.insert(table_id);
        info!(
            table_id = table_id.into_inner(),
            sequence_count,
            event_count,
            inflight_dispatch = inflight_dispatch.len(),
            force_flush = config.force_flush,
            "cdc table batch dispatched"
        );
        inflight_dispatch.push(Box::pin(async move {
            info!(
                table_id = table_id.into_inner(),
                sequence_count, event_count, "waiting to acquire cdc table apply lock"
            );
            let _guard = table_lock.lock().await;
            info!(
                table_id = table_id.into_inner(),
                sequence_count, event_count, "acquired cdc table apply lock"
            );
            match apply_dest
                .dispatch_table_events(table_id, events, sequences[0])
                .await
                .map_err(|err| {
                    anyhow::anyhow!(
                        "dispatching CDC table batch for {} failed: {}",
                        table_id.into_inner(),
                        err
                    )
                })? {
                CdcTableApplyExecution::Immediate => {
                    info!(
                        table_id = table_id.into_inner(),
                        sequence_count, event_count, "cdc table batch apply completed"
                    );
                    Ok(CdcDispatchResult::Immediate(CdcApplyFragmentAck {
                        sequences,
                        released_table: Some(table_id),
                    }))
                }
                CdcTableApplyExecution::Deferred(receiver) => {
                    let completion = Box::pin(async move {
                        receiver.await.map_err(|_| {
                            anyhow::anyhow!(
                                "CDC batch-load completion channel closed for {}",
                                table_id.into_inner()
                            )
                        })??;
                        info!(
                            table_id = table_id.into_inner(),
                            sequence_count, event_count, "cdc table batch apply completed"
                        );
                        Ok(CdcApplyFragmentAck {
                            sequences,
                            released_table: None,
                        })
                    });
                    Ok(CdcDispatchResult::Deferred(CdcDeferredApplyAck {
                        released_table: Some(table_id),
                        completion,
                    }))
                }
            }
        }));
    }
}

pub(super) fn handle_cdc_dispatch_result(
    result: Option<Result<CdcDispatchResult>>,
    watermark_tracker: &mut CdcWatermarkTracker,
    inflight_completion: &mut FuturesUnordered<CdcApplyFuture>,
    active_table_applies: &mut HashSet<TableId>,
) -> Result<Vec<CdcWatermarkAdvance>> {
    let Some(result) = result else {
        return Ok(Vec::new());
    };
    match result? {
        CdcDispatchResult::Immediate(ack) => {
            handle_cdc_apply_result(Some(Ok(ack)), watermark_tracker, active_table_applies)
        }
        CdcDispatchResult::Deferred(deferred) => {
            if let Some(table_id) = deferred.released_table {
                active_table_applies.remove(&table_id);
            }
            inflight_completion.push(deferred.completion);
            Ok(Vec::new())
        }
    }
}

pub(super) fn handle_cdc_apply_result(
    result: Option<Result<CdcApplyFragmentAck>>,
    watermark_tracker: &mut CdcWatermarkTracker,
    active_table_applies: &mut HashSet<TableId>,
) -> Result<Vec<CdcWatermarkAdvance>> {
    let Some(result) = result else {
        return Ok(Vec::new());
    };
    let ack = result?;
    info!(
        sequence_count = ack.sequences.len(),
        released_table = ?ack.released_table.map(|table_id| table_id.into_inner()),
        "cdc apply result received"
    );
    if let Some(table_id) = ack.released_table {
        active_table_applies.remove(&table_id);
    }
    let mut advances = Vec::new();
    for sequence in ack.sequences {
        if let Some(advance) = watermark_tracker.complete_fragment(sequence)? {
            advances.push(advance);
        }
    }
    Ok(advances)
}

pub(super) async fn drain_one_cdc_apply(
    inflight_apply: &mut FuturesUnordered<CdcApplyFuture>,
    watermark_tracker: &mut CdcWatermarkTracker,
    active_table_applies: &mut HashSet<TableId>,
) -> Result<Vec<CdcWatermarkAdvance>> {
    let result = inflight_apply.next().await;
    handle_cdc_apply_result(result, watermark_tracker, active_table_applies)
}

pub(super) fn pending_cdc_commit_count(
    pending_table_batches: &HashMap<TableId, PendingTableApplyBatch>,
) -> usize {
    pending_table_batches
        .values()
        .map(|pending| pending.sequences.len())
        .sum()
}

pub(super) async fn apply_cdc_watermark_advance(
    advance: CdcWatermarkAdvance,
    runtime: &mut CdcWatermarkRuntime<'_>,
    stream: Pin<&mut EventsStream>,
    last_received_lsn: etl::types::PgLsn,
    last_flushed_lsn: &mut etl::types::PgLsn,
) -> Result<()> {
    if let Some(stats) = runtime.stats {
        let total_rows = advance.stats.values().copied().sum::<usize>().max(1) as u64;
        let mut remaining_ms = advance.extract_ms;
        for (index, (table_id, count)) in advance.stats.iter().enumerate() {
            if let Some(cfg) = runtime.table_configs.get(table_id) {
                let count_u64 = (*count).max(1) as u64;
                let extract_ms = if index + 1 == advance.stats.len() {
                    remaining_ms
                } else {
                    (advance.extract_ms.saturating_mul(count_u64)) / total_rows
                };
                remaining_ms = remaining_ms.saturating_sub(extract_ms);
                stats.record_extract(&cfg.name, *count, extract_ms).await;
            }
        }
    }

    *last_flushed_lsn = advance.commit_lsn;
    info!(
        commit_lsn = %advance.commit_lsn,
        stats_tables = advance.stats.len(),
        extract_ms = advance.extract_ms,
        last_received_lsn = %last_received_lsn,
        "advancing cdc watermark"
    );
    await_cdc_timeout(
        format!("sending CDC status update for {}", advance.commit_lsn),
        CDC_STATUS_UPDATE_TIMEOUT,
        stream.send_status_update(
            last_received_lsn,
            *last_flushed_lsn,
            true,
            StatusUpdateType::KeepAlive,
        ),
    )
    .await?;
    let cdc_state = runtime
        .state
        .postgres_cdc
        .get_or_insert_with(Default::default);
    cdc_state.last_lsn = Some(last_flushed_lsn.to_string());
    if let Some(state_handle) = runtime.state_handle {
        match timeout(
            CDC_STATE_SAVE_TIMEOUT,
            state_handle.save_postgres_cdc_state(cdc_state),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(
                    lsn = %advance.commit_lsn,
                    error = %err,
                    "failed to persist postgres CDC state after advancing watermark"
                );
            }
            Err(_) => {
                warn!(
                    lsn = %advance.commit_lsn,
                    timeout_secs = CDC_STATE_SAVE_TIMEOUT.as_secs(),
                    "persisting postgres CDC state timed out after advancing watermark"
                );
            }
        }
    }
    Ok(())
}

pub(super) async fn await_cdc_timeout<T, E, F>(
    label: impl Into<String>,
    duration: Duration,
    fut: F,
) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<anyhow::Error>,
{
    let label = label.into();
    match timeout(duration, fut).await {
        Ok(result) => result.map_err(Into::into).with_context(|| label.clone()),
        Err(_) => anyhow::bail!("{} timed out after {}s", label, duration.as_secs()),
    }
}
