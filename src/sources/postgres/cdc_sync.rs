use super::*;
use super::snapshot_sync::release_exported_snapshot_slot;

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
        let pipeline_id = self.config.cdc_pipeline_id.unwrap_or(1);
        let batch_size = self
            .config
            .cdc_batch_size
            .or(self.config.batch_size)
            .unwrap_or(default_batch_size);
        let snapshot_concurrency = snapshot_concurrency.max(1);
        let max_pending_events = self.config.cdc_max_pending_events.unwrap_or(100_000);
        let idle_timeout = Duration::from_secs(self.config.cdc_idle_timeout_seconds.unwrap_or(10));

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;

        self.validate_wal_level().await?;

        if !replication_client.publication_exists(publication).await? {
            anyhow::bail!("publication '{}' does not exist", publication);
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
            let prev_snapshot = state
                .postgres
                .get(&table_cfg.name)
                .and_then(|checkpoint| checkpoint.schema_snapshot.clone());
            if let Some(diff) = schema_diff(prev_snapshot.as_deref(), &info.schema)
                && !diff.is_empty()
            {
                if schema_diff_enabled {
                    log_schema_diff(&table_cfg.name, &diff);
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
                        if diff.has_incompatible() {
                            anyhow::bail!(
                                "incompatible schema change detected for {}; set schema_changes=resync or fail",
                                table_cfg.name
                            );
                        }
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
            snapshot_concurrency,
        );

        let slot_name: String = EtlReplicationSlot::for_apply_worker(pipeline_id)
            .try_into()
            .context("building CDC replication slot name")?;

        {
            let cdc_state = state.postgres_cdc.get_or_insert_with(Default::default);
            cdc_state.slot_name = Some(slot_name.clone());
            if let Some(state_handle) = &state_handle {
                state_handle.save_postgres_cdc_state(cdc_state).await?;
            }
        }
        let mut last_lsn = state.postgres_cdc.as_ref().and_then(|s| s.last_lsn.clone());

        let has_snapshot_progress = tables.iter().any(|table| {
            state
                .postgres
                .get(&table.name)
                .is_some_and(table_has_snapshot_progress)
        });
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

        let mut start_lsn = None;
        let mut needs_snapshot =
            mode == crate::types::SyncMode::Full || last_lsn.is_none() || has_snapshot_progress;
        if !resync_tables.is_empty() {
            needs_snapshot = true;
        }

        if needs_snapshot {
            let snapshot_table_ids: HashSet<TableId> = if mode == crate::types::SyncMode::Full
                || last_lsn.is_none()
                || has_snapshot_progress
            {
                include_tables.clone()
            } else {
                resync_tables.clone()
            };

            let resume_snapshot_run = snapshot_resume_lsn.is_some() && !force_snapshot_restart;
            let mut slot_guard = None;
            let mut snapshot_name = None;
            let snapshot_start_lsn = if resume_snapshot_run {
                snapshot_resume_lsn
                    .clone()
                    .context("missing snapshot start LSN for snapshot resume")?
            } else {
                if let Err(err) = replication_client.delete_slot(&slot_name).await
                    && err.kind() != etl::error::ErrorKind::ReplicationSlotNotFound
                {
                    return Err(err.into());
                }
                let (guard, slot) = self.create_exported_snapshot_slot(&slot_name).await?;
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
                let write_mode;
                let resumed_from_saved_progress;
                let task_specs = {
                    let entry = state.postgres.entry(table.name.clone()).or_default();
                    let had_progress = table_has_snapshot_progress(entry);
                    resumed_from_saved_progress = resume_snapshot_run && had_progress;
                    if let Some(schema_hash) = table_hashes.get(table_id) {
                        entry.schema_hash = Some(schema_hash.clone());
                    }
                    if let Some(snapshot) = table_snapshots.get(table_id) {
                        entry.schema_snapshot = Some(snapshot.clone());
                    }
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
                        entry.snapshot_chunks =
                            snapshot_chunk_checkpoints_from_ranges(&snapshot_plan.chunk_ranges);
                        if mode == crate::types::SyncMode::Full
                            || resync_tables.contains(table_id)
                            || (force_snapshot_restart && had_progress)
                        {
                            dest.truncate_table(&info.dest_name).await?;
                        }
                        write_mode = WriteMode::Append;
                    } else {
                        if !had_progress {
                            let snapshot_plan = self
                                .plan_snapshot_chunk_ranges(
                                    &table,
                                    batch_size,
                                    snapshot_concurrency,
                                )
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
                            entry.snapshot_chunks =
                                snapshot_chunk_checkpoints_from_ranges(&snapshot_plan.chunk_ranges);
                        }
                        write_mode = WriteMode::Upsert;
                    }
                    if let Some(state_handle) = &state_handle {
                        state_handle
                            .save_postgres_checkpoint(&table.name, entry)
                            .await?;
                    }
                    snapshot_resume_tasks_from_checkpoint(entry)?
                };
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
            if let Some(slot_guard) = slot_guard {
                release_exported_snapshot_slot(slot_guard, snapshot_result.is_ok()).await?;
            }
            snapshot_result?;
            {
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
            let parsed_start_lsn = snapshot_start_lsn.parse().map_err(|_| {
                anyhow::anyhow!("invalid snapshot start LSN '{}'", snapshot_start_lsn)
            })?;
            start_lsn = Some(parsed_start_lsn);
            last_lsn = Some(snapshot_start_lsn);
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
                    pipeline_id,
                    idle_timeout,
                    max_pending_events,
                    apply_concurrency: snapshot_concurrency,
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

        self.validate_wal_level().await?;

        let pg_config = self.build_pg_connection_config().await?;
        let replication_client = PgReplicationClient::connect(pg_config.clone()).await?;
        if !replication_client.publication_exists(publication).await? {
            anyhow::bail!("publication '{}' does not exist", publication);
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
