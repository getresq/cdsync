use super::*;

pub(super) struct CdcIdleState {
    pub(super) follow: bool,
    pub(super) in_tx: bool,
    pub(super) pending_events_empty: bool,
    pub(super) queued_batches_empty: bool,
    pub(super) pending_table_batches_empty: bool,
    pub(super) inflight_apply_empty: bool,
}

impl PostgresSource {
    pub(super) async fn load_etl_table_schema(&self, table_id: TableId) -> Result<EtlTableSchema> {
        let row = sqlx::query(
            r#"
            select n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = $1
            "#,
        )
        .bind(table_id.into_inner() as i32)
        .fetch_one(&self.pool)
        .await?;
        let schema_name: String = row.try_get("schema_name")?;
        let table_name: String = row.try_get("table_name")?;

        let columns = sqlx::query(
            r#"
            select a.attname as column_name,
                   a.atttypid::int as type_oid,
                   a.atttypmod as type_modifier,
                   a.attnotnull as not_null,
                   coalesce(i.indisprimary, false) as is_primary
            from pg_attribute a
            left join pg_index i
              on i.indrelid = a.attrelid
             and a.attnum = any(i.indkey)
             and i.indisprimary
            where a.attrelid = $1
              and a.attnum > 0
              and not a.attisdropped
            order by a.attnum
            "#,
        )
        .bind(table_id.into_inner() as i32)
        .fetch_all(&self.pool)
        .await?;

        let mut column_schemas = Vec::with_capacity(columns.len());
        for row in columns {
            let name: String = row.try_get("column_name")?;
            let type_oid: i32 = row.try_get("type_oid")?;
            let type_modifier: i32 = row.try_get("type_modifier")?;
            let not_null: bool = row.try_get("not_null")?;
            let is_primary: bool = row.try_get("is_primary")?;
            let typ = etl_postgres::types::convert_type_oid_to_type(type_oid as u32);
            column_schemas.push(etl_postgres::types::ColumnSchema::new(
                name,
                typ,
                type_modifier,
                !not_null,
                is_primary,
            ));
        }

        let table_name = etl_postgres::types::TableName::new(schema_name, table_name);
        Ok(EtlTableSchema::new(table_id, table_name, column_schemas))
    }

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

        let mut pending_events: Vec<Event> = Vec::with_capacity(max_pending_events.min(1024));
        let mut pending_stats: HashMap<TableId, usize> = HashMap::new();
        let mut last_received_lsn = start_lsn;
        let mut last_flushed_lsn = start_lsn;
        let mut last_xlog_activity = Instant::now();
        let mut in_tx = false;
        let mut expected_commit_lsn: Option<etl::types::PgLsn> = None;
        let mut shutdown = shutdown;
        let mut shutdown_requested = false;
        let mut next_commit_sequence = 0u64;
        let mut queued_batches: VecDeque<CommittedCdcBatch> = VecDeque::new();
        let mut pending_table_batches: HashMap<TableId, PendingTableApplyBatch> = HashMap::new();
        let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
        let mut watermark_tracker = CdcWatermarkTracker::default();
        let mut table_apply_locks: HashMap<TableId, Arc<Mutex<()>>> = HashMap::new();
        let max_active_applies = apply_concurrency.max(1);
        let max_commit_queue_depth = max_active_applies.saturating_mul(4).max(1);

        loop {
            if shutdown_requested && !in_tx {
                break;
            }

            let wait_timeout = next_cdc_wait_timeout(
                idle_timeout,
                apply_max_fill,
                &pending_table_batches,
                inflight_apply.len(),
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
                    result = inflight_apply.next(), if !inflight_apply.is_empty() => {
                        for advance in handle_cdc_apply_result(result, &mut watermark_tracker)? {
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
                        dispatch_cdc_batches(
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_apply,
                            &mut watermark_tracker,
                            dest,
                            &mut table_apply_locks,
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        );
                        crate::telemetry::record_cdc_pipeline_depths(
                            slot_name,
                            pending_events.len() as u64,
                            (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches))
                                as u64,
                            watermark_tracker.inflight_commits() as u64,
                        );
                        continue;
                    }
                    result = timeout(wait_timeout, stream.next()) => {
                        match result {
                            Ok(Some(msg)) => msg?,
                            Ok(None) => break,
                            Err(_) => {
                                if cdc_fill_deadline_reached(apply_max_fill, &pending_table_batches) {
                                    dispatch_cdc_batches(
                                        &mut queued_batches,
                                        &mut pending_table_batches,
                                        &mut inflight_apply,
                                        &mut watermark_tracker,
                                        dest,
                                        &mut table_apply_locks,
                                        CdcDispatchConfig {
                                            max_active_applies,
                                            apply_batch_size,
                                            max_fill: apply_max_fill,
                                            force_flush: true,
                                        },
                                    );
                                    crate::telemetry::record_cdc_pipeline_depths(
                                        slot_name,
                                        pending_events.len() as u64,
                                        (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches))
                                            as u64,
                                        watermark_tracker.inflight_commits() as u64,
                                    );
                                    continue;
                                }
                                if in_tx {
                                    continue;
                                }
                                if follow {
                                    continue;
                                }
                                break;
                            }
                        }
                    }
                }
            } else {
                tokio::select! {
                    result = inflight_apply.next(), if !inflight_apply.is_empty() => {
                        for advance in handle_cdc_apply_result(result, &mut watermark_tracker)? {
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
                        dispatch_cdc_batches(
                            &mut queued_batches,
                            &mut pending_table_batches,
                            &mut inflight_apply,
                            &mut watermark_tracker,
                            dest,
                            &mut table_apply_locks,
                            CdcDispatchConfig {
                                max_active_applies,
                                apply_batch_size,
                                max_fill: apply_max_fill,
                                force_flush: false,
                            },
                        );
                        crate::telemetry::record_cdc_pipeline_depths(
                            slot_name,
                            pending_events.len() as u64,
                            (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches))
                                as u64,
                            watermark_tracker.inflight_commits() as u64,
                        );
                        continue;
                    }
                    result = timeout(wait_timeout, stream.next()) => match result {
                    Ok(Some(msg)) => msg?,
                    Ok(None) => break,
                    Err(_) => {
                        if cdc_fill_deadline_reached(apply_max_fill, &pending_table_batches) {
                            dispatch_cdc_batches(
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_apply,
                                &mut watermark_tracker,
                                dest,
                                &mut table_apply_locks,
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: true,
                                },
                            );
                            crate::telemetry::record_cdc_pipeline_depths(
                                slot_name,
                                pending_events.len() as u64,
                                (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches))
                                    as u64,
                                watermark_tracker.inflight_commits() as u64,
                            );
                            continue;
                        }
                        if in_tx {
                            continue;
                        }
                        if follow {
                            continue;
                        }
                        break;
                    }
                }}
            };

            match message {
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    let wal_end = etl::types::PgLsn::from(keepalive.wal_end());
                    if wal_end > last_received_lsn {
                        last_received_lsn = wal_end;
                    }
                    if keepalive.reply() == 1 {
                        stream
                            .as_mut()
                            .send_status_update(
                                last_received_lsn,
                                last_flushed_lsn,
                                true,
                                StatusUpdateType::KeepAlive,
                            )
                            .await?;
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
                            expected_commit_lsn = Some(etl::types::PgLsn::from(begin.final_lsn()));
                            pending_events.clear();
                            pending_stats.clear();
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
                            queued_batches.push_back(CommittedCdcBatch {
                                sequence: next_commit_sequence,
                                commit_lsn,
                                table_batches,
                                stats: stats_by_table,
                            });
                            next_commit_sequence += 1;

                            dispatch_cdc_batches(
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_apply,
                                &mut watermark_tracker,
                                dest,
                                &mut table_apply_locks,
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: false,
                                },
                            );
                            crate::telemetry::record_cdc_pipeline_depths(
                                slot_name,
                                pending_events.len() as u64,
                                (queued_batches.len()
                                    + pending_cdc_commit_count(&pending_table_batches))
                                    as u64,
                                watermark_tracker.inflight_commits() as u64,
                            );

                            while watermark_tracker.inflight_commits() >= max_commit_queue_depth {
                                crate::telemetry::record_cdc_backpressure_wait(
                                    slot_name,
                                    watermark_tracker.inflight_commits() as u64,
                                    max_commit_queue_depth as u64,
                                );
                                dispatch_cdc_batches(
                                    &mut queued_batches,
                                    &mut pending_table_batches,
                                    &mut inflight_apply,
                                    &mut watermark_tracker,
                                    dest,
                                    &mut table_apply_locks,
                                    CdcDispatchConfig {
                                        max_active_applies,
                                        apply_batch_size,
                                        max_fill: apply_max_fill,
                                        force_flush: true,
                                    },
                                );
                                for advance in
                                    drain_one_cdc_apply(&mut inflight_apply, &mut watermark_tracker)
                                        .await?
                                {
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
                            in_tx = false;
                        }
                        LogicalReplicationMessage::Relation(relation) => {
                            let table_id = TableId::new(relation.rel_id());
                            if !include_tables.contains(&table_id) {
                                continue;
                            }
                            dispatch_cdc_batches(
                                &mut queued_batches,
                                &mut pending_table_batches,
                                &mut inflight_apply,
                                &mut watermark_tracker,
                                dest,
                                &mut table_apply_locks,
                                CdcDispatchConfig {
                                    max_active_applies,
                                    apply_batch_size,
                                    max_fill: apply_max_fill,
                                    force_flush: false,
                                },
                            );
                            let table_lock = table_apply_locks
                                .entry(table_id)
                                .or_insert_with(|| Arc::new(Mutex::new(())))
                                .clone();
                            let _guard = table_lock.lock().await;
                            if let Some(pending_batch) = pending_table_batches.remove(&table_id) {
                                dest.write_table_events(table_id, pending_batch.events)
                                    .await
                                    .map_err(|err| {
                                        anyhow::anyhow!(
                                            "applying buffered CDC table batch for {} before schema update failed: {}",
                                            table_id.into_inner(),
                                            err
                                        )
                                    })?;
                                for sequence in pending_batch.sequences {
                                    if let Some(advance) =
                                        watermark_tracker.complete_fragment(sequence)?
                                    {
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
                            self.handle_relation_change(table_id, &mut relation_runtime)
                                .await?;
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
                            let schema = store
                                .get_table_schema(&table_id)
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
                            let schema = store
                                .get_table_schema(&table_id)
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
                            let schema = store
                                .get_table_schema(&table_id)
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
                            let event = TruncateEvent {
                                start_lsn: start,
                                commit_lsn,
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

            while let Some(Some(result)) = inflight_apply.next().now_or_never() {
                for advance in handle_cdc_apply_result(Some(result), &mut watermark_tracker)? {
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

            dispatch_cdc_batches(
                &mut queued_batches,
                &mut pending_table_batches,
                &mut inflight_apply,
                &mut watermark_tracker,
                dest,
                &mut table_apply_locks,
                CdcDispatchConfig {
                    max_active_applies,
                    apply_batch_size,
                    max_fill: apply_max_fill,
                    force_flush: false,
                },
            );
            crate::telemetry::record_cdc_pipeline_depths(
                slot_name,
                pending_events.len() as u64,
                (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches)) as u64,
                watermark_tracker.inflight_commits() as u64,
            );

            if cdc_should_stop_after_idle(
                &CdcIdleState {
                    follow,
                    in_tx,
                    pending_events_empty: pending_events.is_empty(),
                    queued_batches_empty: queued_batches.is_empty(),
                    pending_table_batches_empty: pending_table_batches.is_empty(),
                    inflight_apply_empty: inflight_apply.is_empty(),
                },
                last_xlog_activity,
                idle_timeout,
            ) {
                break;
            }
        }

        dispatch_cdc_batches(
            &mut queued_batches,
            &mut pending_table_batches,
            &mut inflight_apply,
            &mut watermark_tracker,
            dest,
            &mut table_apply_locks,
            CdcDispatchConfig {
                max_active_applies,
                apply_batch_size,
                max_fill: apply_max_fill,
                force_flush: true,
            },
        );
        crate::telemetry::record_cdc_pipeline_depths(
            slot_name,
            pending_events.len() as u64,
            (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches)) as u64,
            watermark_tracker.inflight_commits() as u64,
        );
        while !inflight_apply.is_empty() || !pending_table_batches.is_empty() {
            for advance in drain_one_cdc_apply(&mut inflight_apply, &mut watermark_tracker).await? {
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
            dispatch_cdc_batches(
                &mut queued_batches,
                &mut pending_table_batches,
                &mut inflight_apply,
                &mut watermark_tracker,
                dest,
                &mut table_apply_locks,
                CdcDispatchConfig {
                    max_active_applies,
                    apply_batch_size,
                    max_fill: apply_max_fill,
                    force_flush: true,
                },
            );
            crate::telemetry::record_cdc_pipeline_depths(
                slot_name,
                pending_events.len() as u64,
                (queued_batches.len() + pending_cdc_commit_count(&pending_table_batches)) as u64,
                watermark_tracker.inflight_commits() as u64,
            );
        }

        Ok(last_flushed_lsn)
    }

    async fn handle_relation_change(
        &self,
        table_id: TableId,
        runtime: &mut CdcRelationRuntime<'_>,
    ) -> Result<()> {
        let table_cfg = runtime
            .table_configs
            .get(&table_id)
            .context("relation for unknown table")?;

        let etl_schema = self.load_etl_table_schema(table_id).await?;
        runtime.store.store_table_schema(etl_schema.clone()).await?;
        let info = cdc_table_info_from_schema(table_cfg, &etl_schema, &self.metadata)?;
        let new_hash = schema_fingerprint(&info.schema);
        let current_primary_key = info.schema.primary_key.clone();
        let prev_snapshot = runtime.table_snapshots.get(&table_id).cloned();
        let entry = runtime
            .state
            .postgres
            .entry(table_cfg.name.clone())
            .or_default();
        let diff = schema_diff(prev_snapshot.as_deref(), &info.schema);
        let default_diff = SchemaDiff::default();
        let primary_key_changed_detected = primary_key_changed(
            entry.schema_primary_key.as_deref(),
            entry.schema_hash.as_deref(),
            &info.schema,
            &new_hash,
            diff.as_ref().unwrap_or(&default_diff),
        );
        if let Some(diff) = diff
            && !diff.is_empty()
        {
            if runtime.schema_diff_enabled {
                log_schema_diff(&table_cfg.name, &diff);
            }
            if primary_key_changed_detected {
                warn!(
                    table = %table_cfg.name,
                    previous_primary_key = entry.schema_primary_key.as_deref().unwrap_or("unknown"),
                    current_primary_key = info.schema.primary_key.as_deref().unwrap_or("none"),
                    "schema change: primary key changed"
                );
            }
            match runtime.schema_policy.clone() {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table_cfg.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    warn!(
                        table = %table_cfg.name,
                        "schema change detected; marking CDC for resync"
                    );
                    if let Some(cdc_state) = runtime.state.postgres_cdc.as_mut() {
                        cdc_state.last_lsn = None;
                    }
                    return Err(anyhow::anyhow!(
                        "schema change detected for {}; resync required",
                        table_cfg.name
                    ));
                }
                SchemaChangePolicy::Auto => {
                    if diff.has_incompatible() || primary_key_changed_detected {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; set schema_changes=resync or fail",
                            table_cfg.name
                        );
                    }
                    info!(
                        table = %table_cfg.name,
                        "schema change detected; updating destination schema"
                    );
                }
            }
        } else if primary_key_changed_detected {
            warn!(
                table = %table_cfg.name,
                previous_primary_key = entry.schema_primary_key.as_deref().unwrap_or("unknown"),
                current_primary_key = info.schema.primary_key.as_deref().unwrap_or("none"),
                "schema change: primary key changed"
            );
            match runtime.schema_policy.clone() {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table_cfg.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    warn!(
                        table = %table_cfg.name,
                        "schema change detected; marking CDC for resync"
                    );
                    if let Some(cdc_state) = runtime.state.postgres_cdc.as_mut() {
                        cdc_state.last_lsn = None;
                    }
                    return Err(anyhow::anyhow!(
                        "schema change detected for {}; resync required",
                        table_cfg.name
                    ));
                }
                SchemaChangePolicy::Auto => {
                    anyhow::bail!(
                        "incompatible schema change detected for {}; set schema_changes=resync or fail",
                        table_cfg.name
                    );
                }
            }
        }

        let snapshot = schema_snapshot_from_schema(&info.schema);
        runtime.dest.ensure_table_schema(&info.schema).await?;
        runtime.dest.update_table_info(info.clone()).await?;
        runtime.table_info_map.insert(table_id, info);
        runtime.etl_schemas.insert(table_id, etl_schema);
        runtime.table_hashes.insert(table_id, new_hash.clone());
        runtime.table_snapshots.insert(table_id, snapshot.clone());

        entry.schema_hash = Some(new_hash);
        entry.schema_snapshot = Some(snapshot);
        entry.schema_primary_key = current_primary_key;
        if let Some(state_handle) = &runtime.state_handle {
            state_handle
                .save_postgres_checkpoint(&table_cfg.name, entry)
                .await?;
        }

        Ok(())
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
                .map(|deadline| deadline.saturating_duration_since(now))
                .unwrap_or(Duration::ZERO)
        })
        .min()
        .map(|pending_timeout| pending_timeout.min(idle_timeout))
        .unwrap_or(idle_timeout)
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
