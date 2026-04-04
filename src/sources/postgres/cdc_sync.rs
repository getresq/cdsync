use super::snapshot_sync::{release_exported_snapshot_slot, snapshot_shutdown_requested};
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct SnapshotTableWritePlan {
    pub write_mode: WriteMode,
    pub truncate_before_copy: bool,
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
    resync_tables: &HashSet<TableId>,
    initial_snapshot_tables: &HashSet<TableId>,
    mode: crate::types::SyncMode,
    last_lsn: Option<&str>,
) -> bool {
    if mode == crate::types::SyncMode::Full || last_lsn.is_none() || !resync_tables.is_empty() {
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

    !initial_snapshot_tables.is_empty()
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
                .union(initial_snapshot_tables)
                .copied()
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
    if !resume_snapshot_run {
        SnapshotTableWritePlan {
            write_mode: WriteMode::Append,
            truncate_before_copy: mode == crate::types::SyncMode::Full
                || is_resync_table
                || is_initial_snapshot_table
                || (force_snapshot_restart && had_progress),
        }
    } else if is_initial_snapshot_table && !had_progress {
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

        if !self.cdc_enabled() {
            anyhow::bail!("CDC is disabled for Postgres source");
        }
        if tables.is_empty() {
            anyhow::bail!("no postgres tables configured for CDC");
        }
        let policy = self.config.schema_policy();

        let publication = self
            .config
            .publication
            .as_deref()
            .context("postgres.publication is required when CDC is enabled")?;
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
                    anyhow::bail!("publication '{}' does not exist", publication);
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

        for (table_id, table_cfg) in table_ids.iter() {
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
                        anyhow::bail!(
                            "schema change detected for {}; set schema_changes=auto or resync",
                            table_cfg.name
                        );
                    }
                    SchemaChangePolicy::Resync => {
                        resync_tables.insert(*table_id);
                    }
                    SchemaChangePolicy::Auto => {
                        if diff.has_incompatible() || primary_key_changed_detected {
                            anyhow::bail!(
                                "incompatible schema change detected for {}; set schema_changes=resync or fail",
                                table_cfg.name
                            );
                        }
                    }
                }
            } else if primary_key_changed_detected {
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
                        anyhow::bail!(
                            "schema change detected for {}; set schema_changes=auto or resync",
                            table_cfg.name
                        );
                    }
                    SchemaChangePolicy::Resync => {
                        resync_tables.insert(*table_id);
                    }
                    SchemaChangePolicy::Auto => {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; set schema_changes=resync or fail",
                            table_cfg.name
                        );
                    }
                }
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
            state_handle.clone(),
        )
        .await?;

        let slot_name = cdc_slot_name(
            state_handle
                .as_ref()
                .map(|handle| handle.connection_id())
                .unwrap_or("postgres"),
            pipeline_id,
        )?;

        {
            let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
            cdc_state.slot_name = Some(slot_name.clone());
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }
        let mut last_lsn = state.postgres_cdc.as_ref().and_then(|s| s.last_lsn.clone());
        let initial_snapshot_tables =
            initial_snapshot_table_ids(&table_ids, state, last_lsn.as_deref());
        let snapshot_progress_tables = snapshot_progress_table_ids(&table_ids, state);
        info!(
            connection = %state_handle
                .as_ref()
                .map(|handle| handle.connection_id())
                .unwrap_or("postgres"),
            include_table_count = include_tables.len(),
            resync_table_count = resync_tables.len(),
            initial_snapshot_table_count = initial_snapshot_tables.len(),
            snapshot_progress_table_count = snapshot_progress_tables.len(),
            last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
            slot_name = %slot_name,
            "resolved CDC snapshot inputs"
        );

        let has_snapshot_progress = !snapshot_progress_tables.is_empty();
        let mut snapshot_progress_missing_lsn = false;
        let snapshot_resume_lsn = tables.iter().try_fold(None, |current, table| {
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
                    anyhow::bail!("snapshot progress checkpoints have mismatched start LSNs")
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
            connection = %state_handle
                .as_ref()
                .map(|handle| handle.connection_id())
                .unwrap_or("postgres"),
            mode = ?mode,
            needs_snapshot,
            has_snapshot_progress,
            force_snapshot_restart,
            preserve_existing_backlog,
            last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
            "computed CDC snapshot decision"
        );

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
                connection = %state_handle
                    .as_ref()
                    .map(|handle| handle.connection_id())
                    .unwrap_or("postgres"),
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
                    if let Some(schema_hash) = table_hashes.get(table_id) {
                        entry.schema_hash = Some(schema_hash.clone());
                    }
                    if let Some(snapshot) = table_snapshots.get(table_id) {
                        entry.schema_snapshot = Some(snapshot.clone());
                    }
                    entry.schema_primary_key = info.schema.primary_key.clone();
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
                            && (is_initial_snapshot_table || entry.snapshot_preserve_backlog);
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
                        entry.snapshot_preserve_backlog =
                            preserve_existing_backlog && is_initial_snapshot_table;
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

            let semaphore = Arc::new(tokio::sync::Semaphore::new(snapshot_concurrency));
            let mut snapshot_tasks = FuturesUnordered::new();
            for snapshot_task in snapshot_tasks_to_run {
                let permit_pool = Arc::clone(&semaphore);
                let snapshot_name = snapshot_name.clone();
                let task_shutdown = shutdown.clone();
                let source = self;
                let dest = dest.clone();
                let stats = stats.clone();
                snapshot_tasks.push(async move {
                    let permit = permit_pool
                        .acquire_owned()
                        .await
                        .map_err(|_| anyhow::anyhow!("failed to acquire CDC snapshot permit"))?;
                    let _permit = permit;
                    info!(
                        table = %snapshot_task.info.source_name,
                        snapshot_name = snapshot_name.as_deref().unwrap_or("resume"),
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
                    source
                        .run_snapshot_copy_with_exported_snapshot(
                            &snapshot_task.table,
                            &snapshot_task.info.schema,
                            snapshot_task.chunk,
                            SnapshotCopyContext {
                                dest: &dest,
                                snapshot_name: snapshot_name.as_deref(),
                                batch_size,
                                stats,
                                checkpoint_state: snapshot_task.checkpoint_state,
                                resume_from_primary_key: snapshot_task.resume_from_primary_key,
                                write_mode: snapshot_task.write_mode,
                                state_handle: snapshot_task.state_handle,
                                shutdown: task_shutdown,
                            },
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "copying exported snapshot for {}",
                                snapshot_task.info.source_name
                            )
                        })?;
                    Ok::<(), anyhow::Error>(())
                });
            }

            let snapshot_result: Result<()> = async {
                while let Some(result) = snapshot_tasks.next().await {
                    result?;
                }
                Ok(())
            }
            .await;
            let snapshot_completed =
                cdc_snapshot_completed(snapshot_result.is_ok(), shutdown.as_ref());
            if let Some(slot_guard) = slot_guard {
                release_exported_snapshot_slot(slot_guard, snapshot_completed).await?;
            }
            if preserve_existing_backlog
                && let Err(err) = replication_client.delete_slot(&snapshot_slot_name).await
                && err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
            {
                return Err(err.into());
            }
            snapshot_result?;
            if !snapshot_completed {
                for (table_name, checkpoint_state) in &snapshot_checkpoint_states {
                    let checkpoint = checkpoint_state.lock().await.clone();
                    state.postgres.insert(table_name.clone(), checkpoint);
                }
                info!(
                    connection = %state_handle
                        .as_ref()
                        .map(|handle| handle.connection_id())
                        .unwrap_or("postgres"),
                    snapshot_table_count = snapshot_checkpoint_states.len(),
                    "shutdown requested during CDC snapshot; preserving resumable snapshot progress"
                );
                return Ok(());
            }
            if !preserve_existing_backlog {
                let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
                cdc_state.last_lsn = Some(snapshot_start_lsn.clone());
                if let Some(state_handle) = &state_handle {
                    state_handle.save_postgres_cdc_state(cdc_state).await?;
                }
            }
            let completed_at = Utc::now();
            for (table_name, checkpoint_state) in snapshot_checkpoint_states {
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
                }
                state.postgres.insert(table_name, checkpoint);
            }
            if !preserve_existing_backlog {
                let parsed_start_lsn = snapshot_start_lsn.parse().map_err(|_| {
                    anyhow::anyhow!("invalid snapshot start LSN '{}'", snapshot_start_lsn)
                })?;
                start_lsn = Some(parsed_start_lsn);
                last_lsn = Some(snapshot_start_lsn);
            }
        } else {
            info!(
                connection = %state_handle
                    .as_ref()
                    .map(|handle| handle.connection_id())
                    .unwrap_or("postgres"),
                last_lsn = last_lsn.as_deref().unwrap_or("<none>"),
                "skipping CDC snapshot planning because snapshot is not needed"
            );
        }

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
                    schema_policy: policy,
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
            anyhow::bail!("CDC is disabled for Postgres source");
        }
        if tables.is_empty() {
            anyhow::bail!("no postgres tables configured for CDC");
        }

        let publication = self
            .config
            .publication
            .as_deref()
            .context("postgres.publication is required when CDC is enabled")?;
        let publication_mode = self.config.publication_mode();

        self.validate_wal_level().await?;

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;

        match publication_mode {
            crate::config::PostgresPublicationMode::Validate => {
                if !replication_client.publication_exists(publication).await? {
                    anyhow::bail!("publication '{}' does not exist", publication);
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
            table_ids.insert(TableId::new(oid as u32), table.clone());
        }
        Ok(table_ids)
    }

    async fn validate_wal_level(&self) -> Result<()> {
        let wal_level: String = sqlx::query_scalar("show wal_level")
            .fetch_one(&self.pool)
            .await
            .context("checking wal_level")?;
        if wal_level != "logical" {
            anyhow::bail!("wal_level must be logical for CDC (found '{}')", wal_level);
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
                anyhow::bail!(
                    "table {} not found in publication {}",
                    table.name,
                    publication
                );
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
                let actual = actual.context(format!(
                    "publication {} missing row filter for table {}",
                    publication, table.name
                ))?;
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
                    anyhow::bail!(
                        "publication {} row filter mismatch for {} (expected `{}`, got `{}`)",
                        publication,
                        table.name,
                        where_clause,
                        actual
                    );
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
