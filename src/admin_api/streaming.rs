use super::*;

pub(super) async fn stream(
    State(state): State<AdminApiState>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, AdminApiError> {
    state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == query.connection)
        .context("connection not found")?;

    let mut interval = tokio::time::interval(STREAM_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let stream = stream::unfold(
        StreamCursor {
            state,
            connection_id: query.connection,
            seq: 0,
            interval,
            pending_events: VecDeque::new(),
            previous_run_snapshot: None,
        },
        |mut cursor| async move {
            loop {
                if let Some(event) = cursor.pending_events.pop_front() {
                    return Some((Ok(event), cursor));
                }

                cursor.interval.tick().await;
                cursor.pending_events = match build_stream_events(&mut cursor).await {
                    Ok(events) => events,
                    Err(err) => {
                        let mut events = VecDeque::new();
                        if let Ok(event) = next_stream_event(
                            &mut cursor.seq,
                            &cursor.connection_id,
                            "stream.error",
                            StreamErrorData {
                                message: err.to_string(),
                            },
                        ) {
                            events.push_back(event);
                        }
                        events
                    }
                };
            }
        },
    );

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(STREAM_KEEP_ALIVE_INTERVAL)
            .text("keep-alive"),
    ))
}

async fn build_stream_events(cursor: &mut StreamCursor) -> anyhow::Result<VecDeque<SseEvent>> {
    let now = Utc::now();
    let connection = cursor
        .state
        .cfg
        .connections
        .iter()
        .find(|connection| connection.id == cursor.connection_id)
        .context("connection not found")?;
    let sync_state = cursor.state.state_store.load_state().await?;
    let connection_state = sync_state.connections.get(&cursor.connection_id).cloned();
    let (current_run, run_tables, live_snapshot) =
        load_current_run_view(&cursor.state, &cursor.connection_id).await?;
    let cached_cdc_state = cached_postgres_cdc_slot_state(&cursor.state, connection);
    let cdc_probe = match cached_cdc_state.as_ref() {
        Some(slot_state) if slot_state.sampler_status == "ok" => Ok(slot_state.snapshot.clone()),
        Some(_) => Err(()),
        None => Ok(None),
    };
    let cdc_slot_snapshot = match &cdc_probe {
        Ok(snapshot) => snapshot.clone(),
        Err(()) => None,
    };
    let cdc_runtime_state = match &cdc_probe {
        Ok(snapshot) => postgres_cdc_runtime_state_from_snapshot(snapshot.as_ref()),
        Err(()) => Some(PostgresCdcRuntimeState::Unknown),
    };
    let runtime = derive_connection_runtime(
        connection,
        connection_state.as_ref(),
        current_run.as_ref(),
        cdc_runtime_state,
        now,
        RuntimeMetadata {
            config_hash: &cursor.state.config_hash,
            deploy_revision: cursor.state.deploy_revision.as_deref(),
            last_restart_reason: &cursor.state.last_restart_reason,
        },
    );
    let tables = build_table_progress(
        connection,
        connection_state.as_ref(),
        &run_tables,
        now,
        runtime.reason_code,
        cdc_runtime_state,
    );

    let mut events = VecDeque::new();
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "service.heartbeat",
        ServiceHeartbeatData {
            service: "cdsync",
            version: env!("CARGO_PKG_VERSION"),
            started_at: cursor.state.started_at,
            uptime_seconds: (now - cursor.state.started_at).num_seconds().max(0),
            mode: cursor.state.mode.clone(),
            deploy_revision: cursor.state.deploy_revision.clone(),
            config_hash: cursor.state.config_hash.clone(),
            last_restart_reason: cursor.state.last_restart_reason.clone(),
        },
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.runtime",
        &runtime,
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.throughput",
        build_connection_throughput_event(
            current_run.as_ref(),
            live_snapshot.as_ref(),
            &mut cursor.previous_run_snapshot,
            now,
        ),
    )?);
    events.push_back(next_stream_event(
        &mut cursor.seq,
        &cursor.connection_id,
        "connection.cdc",
        ConnectionCdcData {
            sampler_status: cached_cdc_state
                .as_ref()
                .map(|state| state.sampler_status)
                .unwrap_or("disabled"),
            sampled_at: cached_cdc_state.as_ref().and_then(|state| state.sampled_at),
            slot_name: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.slot_name.clone()),
            slot_active: cdc_slot_snapshot.as_ref().map(|snapshot| snapshot.active),
            current_wal_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.current_wal_lsn.clone()),
            restart_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.restart_lsn.clone()),
            confirmed_flush_lsn: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.confirmed_flush_lsn.clone()),
            wal_bytes_retained_by_slot: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.wal_bytes_retained_by_slot),
            wal_bytes_behind_confirmed: cdc_slot_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.wal_bytes_behind_confirmed),
        },
    )?);

    let active_tables = select_active_tables(&tables);
    if !active_tables.is_empty() {
        events.push_back(next_stream_event(
            &mut cursor.seq,
            &cursor.connection_id,
            "table.progress",
            &active_tables,
        )?);
    }

    let snapshot_tables = select_snapshot_tables(&tables);
    if !snapshot_tables.is_empty() {
        events.push_back(next_stream_event(
            &mut cursor.seq,
            &cursor.connection_id,
            "snapshot.progress",
            &snapshot_tables,
        )?);
    }

    Ok(events)
}

pub(super) async fn load_current_run_view(
    state: &AdminApiState,
    connection_id: &str,
) -> anyhow::Result<(
    Option<RunSummary>,
    Vec<TableStatsSnapshot>,
    Option<RunStatsSnapshot>,
)> {
    if let Some(snapshot) = live_run_snapshot(connection_id) {
        return Ok((
            Some(summarize_run(&snapshot)),
            snapshot.tables.clone(),
            Some(snapshot),
        ));
    }

    if let Some(stats_db) = &state.stats_db {
        let runs = stats_db.recent_runs(Some(connection_id), 1).await?;
        if let Some(run) = runs.into_iter().next() {
            let tables = stats_db.run_tables(&run.run_id).await?;
            return Ok((Some(run), tables, None));
        }
    }

    Ok((None, Vec::new(), None))
}

fn build_connection_throughput_event(
    current_run: Option<&RunSummary>,
    live_snapshot: Option<&RunStatsSnapshot>,
    previous_run_snapshot: &mut Option<(DateTime<Utc>, RunStatsSnapshot)>,
    now: DateTime<Utc>,
) -> ConnectionThroughputData {
    let mut event = ConnectionThroughputData {
        run_id: current_run.map(|run| run.run_id.clone()),
        status: current_run.and_then(|run| run.status.clone()),
        rows_read_total: current_run.map(|run| run.rows_read).unwrap_or_default(),
        rows_written_total: current_run.map(|run| run.rows_written).unwrap_or_default(),
        rows_deleted_total: current_run.map(|run| run.rows_deleted).unwrap_or_default(),
        rows_upserted_total: current_run.map(|run| run.rows_upserted).unwrap_or_default(),
        extract_ms_total: current_run.map(|run| run.extract_ms).unwrap_or_default(),
        load_ms_total: current_run.map(|run| run.load_ms).unwrap_or_default(),
        rows_read_per_sec: None,
        rows_written_per_sec: None,
        rows_deleted_per_sec: None,
        rows_upserted_per_sec: None,
    };

    let Some(live_snapshot) = live_snapshot else {
        return event;
    };

    if let Some((previous_at, previous_snapshot)) = previous_run_snapshot.as_ref()
        && previous_snapshot.run_id == live_snapshot.run_id
    {
        let elapsed_ms = (now - *previous_at).num_milliseconds().max(1) as f64;
        let elapsed_seconds = elapsed_ms / 1000.0;
        event.rows_read_per_sec = Some(
            ((live_snapshot.rows_read - previous_snapshot.rows_read).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_written_per_sec = Some(
            ((live_snapshot.rows_written - previous_snapshot.rows_written).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_deleted_per_sec = Some(
            ((live_snapshot.rows_deleted - previous_snapshot.rows_deleted).max(0) as f64)
                / elapsed_seconds,
        );
        event.rows_upserted_per_sec = Some(
            ((live_snapshot.rows_upserted - previous_snapshot.rows_upserted).max(0) as f64)
                / elapsed_seconds,
        );
    }

    *previous_run_snapshot = Some((now, live_snapshot.clone()));
    event
}

pub(super) fn select_active_tables(tables: &[TableProgress]) -> Vec<TableProgress> {
    tables
        .iter()
        .filter(|table| {
            matches!(table.phase, "error" | "blocked")
                || table.snapshot_chunks_total > 0
                || table.stats.as_ref().is_some_and(|stats| {
                    stats.rows_read > 0
                        || stats.rows_written > 0
                        || stats.rows_deleted > 0
                        || stats.rows_upserted > 0
                })
        })
        .take(STREAM_ACTIVE_TABLE_LIMIT)
        .cloned()
        .collect()
}

fn select_snapshot_tables(tables: &[TableProgress]) -> Vec<TableProgress> {
    tables
        .iter()
        .filter(|table| {
            table.snapshot_chunks_total > 0
                && table.snapshot_chunks_complete < table.snapshot_chunks_total
        })
        .take(STREAM_ACTIVE_TABLE_LIMIT)
        .cloned()
        .collect()
}

fn next_stream_event<T: Serialize>(
    seq: &mut u64,
    connection_id: &str,
    event_type: &'static str,
    data: T,
) -> anyhow::Result<SseEvent> {
    *seq += 1;
    Ok(SseEvent::default()
        .event(event_type)
        .json_data(StreamEnvelope {
            event_type,
            connection_id: connection_id.to_string(),
            seq: *seq,
            at: Utc::now(),
            data,
        })?)
}
