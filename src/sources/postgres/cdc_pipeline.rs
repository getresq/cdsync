use super::*;

pub(super) struct CommittedCdcBatch {
    pub(super) sequence: u64,
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) table_batches: Vec<CdcTableApplyBatch>,
    pub(super) stats: HashMap<TableId, usize>,
}

pub(super) struct CdcTableApplyBatch {
    pub(super) table_id: TableId,
    pub(super) events: Vec<Event>,
}

pub(super) struct CdcApplyFragmentAck {
    pub(super) sequences: Vec<u64>,
}

pub(super) struct CommitTrackerEntry {
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) remaining_fragments: usize,
    pub(super) stats: HashMap<TableId, usize>,
}

pub(super) struct CdcWatermarkAdvance {
    pub(super) commit_lsn: etl::types::PgLsn,
    pub(super) stats: HashMap<TableId, usize>,
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

impl CdcWatermarkTracker {
    pub(super) fn inflight_commits(&self) -> usize {
        self.commits.len()
    }

    pub(super) fn register_commit(
        &mut self,
        sequence: u64,
        commit_lsn: etl::types::PgLsn,
        stats: HashMap<TableId, usize>,
        fragment_count: usize,
    ) {
        self.commits.insert(
            sequence,
            CommitTrackerEntry {
                commit_lsn,
                remaining_fragments: fragment_count,
                stats,
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

        while let Some(entry) = self.commits.get(&self.next_sequence) {
            if entry.remaining_fragments != 0 {
                break;
            }
            let entry = self.commits.remove(&self.next_sequence)?;
            last_lsn = Some(entry.commit_lsn);
            for (table_id, count) in entry.stats {
                *stats.entry(table_id).or_insert(0) += count;
            }
            self.next_sequence += 1;
        }

        last_lsn.map(|commit_lsn| CdcWatermarkAdvance { commit_lsn, stats })
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
    inflight_apply: &mut FuturesUnordered<CdcApplyFuture>,
    watermark_tracker: &mut CdcWatermarkTracker,
    dest: &EtlBigQueryDestination,
    table_apply_locks: &mut HashMap<TableId, Arc<Mutex<()>>>,
    config: CdcDispatchConfig,
) {
    while let Some(batch) = queued_batches.pop_front() {
        let fragment_count = batch.table_batches.len().max(1);
        watermark_tracker.register_commit(
            batch.sequence,
            batch.commit_lsn,
            batch.stats,
            fragment_count,
        );

        if batch.table_batches.is_empty() {
            let sequences = vec![batch.sequence];
            inflight_apply.push(Box::pin(
                async move { Ok(CdcApplyFragmentAck { sequences }) },
            ));
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
        if inflight_apply.len() >= config.max_active_applies {
            break;
        }
        let pending = pending_table_batches
            .remove(&table_id)
            .expect("pending batch missing for ready table");
        let table_lock = table_apply_locks
            .entry(table_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let apply_dest = dest.clone();
        let sequences = pending.sequences;
        let events = pending.events;
        inflight_apply.push(Box::pin(async move {
            let _guard = table_lock.lock().await;
            apply_dest
                .write_table_events(table_id, events)
                .await
                .map_err(|err| {
                    anyhow::anyhow!(
                        "applying CDC table batch for {} failed: {}",
                        table_id.into_inner(),
                        err
                    )
                })?;
            Ok(CdcApplyFragmentAck { sequences })
        }));
    }
}

pub(super) fn handle_cdc_apply_result(
    result: Option<Result<CdcApplyFragmentAck>>,
    watermark_tracker: &mut CdcWatermarkTracker,
) -> Result<Vec<CdcWatermarkAdvance>> {
    let Some(result) = result else {
        return Ok(Vec::new());
    };
    let ack = result?;
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
) -> Result<Vec<CdcWatermarkAdvance>> {
    let result = inflight_apply.next().await;
    handle_cdc_apply_result(result, watermark_tracker)
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
        for (table_id, count) in &advance.stats {
            if let Some(cfg) = runtime.table_configs.get(table_id) {
                stats.record_extract(&cfg.name, *count, 0).await;
            }
        }
    }

    *last_flushed_lsn = advance.commit_lsn;
    stream
        .send_status_update(
            last_received_lsn,
            *last_flushed_lsn,
            true,
            StatusUpdateType::KeepAlive,
        )
        .await?;
    let cdc_state = runtime
        .state
        .postgres_cdc
        .get_or_insert_with(Default::default);
    cdc_state.last_lsn = Some(last_flushed_lsn.to_string());
    if let Some(state_handle) = runtime.state_handle {
        state_handle.save_postgres_cdc_state(cdc_state).await?;
    }
    Ok(())
}
