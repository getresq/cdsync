use super::*;
use rustls::pki_types::pem::PemObject;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnapshotCopyExit {
    Completed,
    Interrupted,
}

pub(super) async fn write_snapshot_batch(
    dest: &dyn Destination,
    schema: &TableSchema,
    batch: &DataFrame,
    write_mode: WriteMode,
    primary_key: &str,
) -> Result<()> {
    dest.write_batch(&schema.name, schema, batch, write_mode, Some(primary_key))
        .await
}

pub(super) async fn finalize_snapshot_copy_progress(
    checkpoint_state: &Arc<Mutex<TableCheckpoint>>,
    table_name: &str,
    chunk: Option<SnapshotChunkRange>,
    last_primary_key: Option<String>,
    exit: SnapshotCopyExit,
    state_handle: Option<&StateHandle>,
) -> Result<bool> {
    if !matches!(exit, SnapshotCopyExit::Completed) {
        return Ok(false);
    }

    save_snapshot_progress(
        checkpoint_state,
        table_name,
        chunk,
        last_primary_key,
        true,
        state_handle,
    )
    .await?;
    Ok(true)
}

impl PostgresSource {
    pub(super) async fn resolve_primary_key_type_info(
        &self,
        schema_name: &str,
        table_name: &str,
        column: &str,
    ) -> Result<PrimaryKeyTypeInfo> {
        let row = sqlx::query(
            r#"
            select udt_name, udt_schema
            from information_schema.columns
            where table_schema = $1 and table_name = $2 and column_name = $3
            "#,
        )
        .bind(schema_name)
        .bind(table_name)
        .bind(column)
        .fetch_one(&self.pool)
        .await?;
        let udt_name: String = row.try_get("udt_name")?;
        let udt_schema: String = row.try_get("udt_schema")?;
        Ok(PrimaryKeyTypeInfo {
            cast_type: format_cast_type(&udt_name, &udt_schema),
            udt_name,
        })
    }

    pub(super) async fn resolve_primary_key_cast(
        &self,
        schema_name: &str,
        table_name: &str,
        column: &str,
    ) -> Result<String> {
        Ok(self
            .resolve_primary_key_type_info(schema_name, table_name, column)
            .await?
            .cast_type)
    }

    pub(super) async fn plan_snapshot_chunk_ranges(
        &self,
        table: &ResolvedPostgresTable,
        batch_size: usize,
        max_chunks: usize,
    ) -> Result<SnapshotChunkPlan> {
        if batch_size == 0 || max_chunks <= 1 {
            return Ok(SnapshotChunkPlan {
                row_count: 0,
                chunk_ranges: Vec::new(),
            });
        }

        let (schema_name, table_name) = split_table_name(&table.name);
        let pk_info = self
            .resolve_primary_key_type_info(&schema_name, &table_name, &table.primary_key)
            .await?;
        if !is_chunkable_snapshot_primary_key(&pk_info.udt_name) {
            return Ok(SnapshotChunkPlan {
                row_count: 0,
                chunk_ranges: Vec::new(),
            });
        }

        let pk = quote_pg_identifier(&table.primary_key);
        let table_ref = format!(
            "{}.{}",
            quote_pg_identifier(&schema_name),
            quote_pg_identifier(&table_name)
        );
        let sql = if let Some(where_clause) = &table.where_clause {
            format!(
                "SELECT COUNT(*)::bigint AS row_count, \
                 MIN({pk})::bigint AS min_pk, \
                 MAX({pk})::bigint AS max_pk \
                 FROM {table_ref} WHERE ({where_clause})",
                pk = pk,
                table_ref = table_ref,
                where_clause = where_clause
            )
        } else {
            format!(
                "SELECT COUNT(*)::bigint AS row_count, \
                 MIN({pk})::bigint AS min_pk, \
                 MAX({pk})::bigint AS max_pk \
                 FROM {table_ref}",
                pk = pk,
                table_ref = table_ref
            )
        };
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        let row_count: i64 = row.try_get("row_count")?;
        let min_pk: Option<i64> = row.try_get("min_pk")?;
        let max_pk: Option<i64> = row.try_get("max_pk")?;
        let chunk_ranges = match (min_pk, max_pk) {
            (Some(min_pk), Some(max_pk)) => plan_snapshot_chunk_ranges_from_bounds(
                row_count, min_pk, max_pk, batch_size, max_chunks,
            ),
            _ => Vec::new(),
        };

        Ok(SnapshotChunkPlan {
            row_count,
            chunk_ranges,
        })
    }

    pub async fn sync_table(&self, request: TableSyncRequest<'_>) -> Result<TableCheckpoint> {
        let TableSyncRequest {
            table,
            dest,
            checkpoint,
            state_handle,
            mode,
            dry_run,
            default_batch_size,
            schema_diff_enabled,
            stats,
        } = request;
        let schema = self.discover_schema(table).await?;
        dest.ensure_table(&schema).await?;

        let schema_hash = schema_fingerprint(&schema);
        let policy = self.config.schema_policy();
        let mut entry = checkpoint;
        let run_options = TableRunOptions {
            dry_run,
            batch_size: self.config.batch_size.unwrap_or(default_batch_size),
            stats: stats.as_ref(),
            state_handle,
        };
        if let Some(diff) = schema_diff(entry.schema_snapshot.as_deref(), &schema)
            && !diff.is_empty()
        {
            if schema_diff_enabled {
                log_schema_diff(&table.name, &diff);
            }
            let primary_key_changed_detected = primary_key_changed(
                entry.schema_primary_key.as_deref(),
                entry.schema_hash.as_deref(),
                &schema,
                &schema_hash,
                &diff,
            );
            if primary_key_changed_detected {
                warn!(
                    table = %table.name,
                    previous_primary_key = entry.schema_primary_key.as_deref().unwrap_or("unknown"),
                    current_primary_key = schema.primary_key.as_deref().unwrap_or("none"),
                    "schema change: primary key changed"
                );
            }
            match policy {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    info!(table = %table.name, "schema change detected; resyncing table");
                    if !dry_run {
                        dest.truncate_table(&schema.name).await?;
                    }
                    self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                    entry.schema_hash = Some(schema_hash);
                    entry.schema_snapshot = Some(schema_snapshot_from_schema(&schema));
                    return Ok(entry);
                }
                SchemaChangePolicy::Auto => {
                    if diff.has_incompatible() || primary_key_changed_detected {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; set schema_changes=resync or fail",
                            table.name
                        );
                    }
                    info!(table = %table.name, "schema change detected; auto-altering destination");
                }
            }
        } else if primary_key_changed(
            entry.schema_primary_key.as_deref(),
            entry.schema_hash.as_deref(),
            &schema,
            &schema_hash,
            &SchemaDiff::default(),
        ) {
            warn!(
                table = %table.name,
                previous_primary_key = entry.schema_primary_key.as_deref().unwrap_or("unknown"),
                current_primary_key = schema.primary_key.as_deref().unwrap_or("none"),
                "schema change: primary key changed"
            );
            match policy {
                SchemaChangePolicy::Fail => {
                    anyhow::bail!(
                        "schema change detected for {}; set schema_changes=auto or resync",
                        table.name
                    );
                }
                SchemaChangePolicy::Resync => {
                    info!(table = %table.name, "schema change detected; resyncing table");
                    if !dry_run {
                        dest.truncate_table(&schema.name).await?;
                    }
                    self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                    entry.schema_hash = Some(schema_hash.clone());
                    entry.schema_snapshot = Some(schema_snapshot_from_schema(&schema));
                    entry.schema_primary_key = schema.primary_key.clone();
                    return Ok(entry);
                }
                SchemaChangePolicy::Auto => {
                    anyhow::bail!(
                        "incompatible schema change detected for {}; set schema_changes=resync or fail",
                        table.name
                    );
                }
            }
        }

        match mode {
            crate::types::SyncMode::Full => {
                if !dry_run {
                    dest.truncate_table(&schema.name).await?;
                }
                self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                    .await?;
            }
            crate::types::SyncMode::Incremental => {
                if table.updated_at_column.is_none() {
                    warn!(
                        "table {} missing updated_at_column; falling back to full refresh",
                        table.name
                    );
                    if !dry_run {
                        dest.truncate_table(&schema.name).await?;
                    }
                    self.run_full_refresh(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                } else {
                    self.run_incremental(table, &schema, dest, &mut entry, &run_options)
                        .await?;
                }
            }
        }

        entry.schema_hash = Some(schema_hash);
        entry.schema_snapshot = Some(schema_snapshot_from_schema(&schema));
        entry.schema_primary_key = schema.primary_key.clone();
        Ok(entry)
    }

    async fn run_full_refresh(
        &self,
        table: &ResolvedPostgresTable,
        schema: &TableSchema,
        dest: &dyn Destination,
        checkpoint: &mut TableCheckpoint,
        options: &TableRunOptions<'_>,
    ) -> Result<()> {
        let (schema_name, table_name) = split_table_name(&table.name);

        if !options.dry_run {
            dest.ensure_table(schema).await?;
        }

        let pk_alias = "__cdsync_pk";
        let pk_cast = self
            .resolve_primary_key_cast(&schema_name, &table_name, &table.primary_key)
            .await?;
        let select_columns = build_select_columns(schema);
        let select_expr = format!(
            "{}, {}::text as {}",
            select_columns,
            quote_pg_identifier(&table.primary_key),
            quote_pg_identifier(pk_alias)
        );
        let mut last_pk: Option<String> = None;
        let has_where = table.where_clause.is_some();
        let base_sql = if let Some(where_clause) = &table.where_clause {
            format!(
                "SELECT {select} FROM {schema}.{table} WHERE ({where_clause})",
                select = select_expr,
                schema = schema_name,
                table = table_name,
                where_clause = where_clause
            )
        } else {
            format!(
                "SELECT {select} FROM {schema}.{table}",
                select = select_expr,
                schema = schema_name,
                table = table_name
            )
        };
        let sql_without_pk = format!("{} ORDER BY {} LIMIT $1", base_sql, table.primary_key);
        let sql_with_pk = if has_where {
            format!(
                "{} AND {} > $1::{} ORDER BY {} LIMIT $2",
                base_sql, table.primary_key, pk_cast, table.primary_key
            )
        } else {
            format!(
                "{} WHERE {} > $1::{} ORDER BY {} LIMIT $2",
                base_sql, table.primary_key, pk_cast, table.primary_key
            )
        };
        loop {
            let extract_start = Instant::now();
            let rows = if let Some(last_pk_value) = last_pk.as_ref() {
                sqlx::query(&sql_with_pk)
                    .bind(last_pk_value)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                sqlx::query(&sql_without_pk)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            };
            let extract_ms = extract_start.elapsed().as_millis() as u64;

            if rows.is_empty() {
                break;
            }

            let batch = rows_to_batch(schema, &rows, table, Utc::now(), &self.metadata)?;
            if let Some(stats) = options.stats {
                stats
                    .record_extract(&table.name, rows.len(), extract_ms)
                    .await;
            }
            if !options.dry_run {
                let load_start = Instant::now();
                dest.write_batch(
                    &schema.name,
                    schema,
                    &batch,
                    WriteMode::Append,
                    Some(&table.primary_key),
                )
                .await?;
                if let Some(stats) = options.stats {
                    let load_ms = load_start.elapsed().as_millis() as u64;
                    stats
                        .record_load(&table.name, rows.len(), 0, 0, load_ms)
                        .await;
                }
            }

            let last_value: String = rows
                .last()
                .and_then(|row| row.try_get(pk_alias).ok())
                .context("missing primary key value for keyset pagination")?;
            last_pk = Some(last_value);
            checkpoint.last_synced_at = Some(Utc::now());
            if let Some(state_handle) = &options.state_handle {
                state_handle
                    .save_postgres_checkpoint(&table.name, checkpoint)
                    .await?;
            }
        }
        Ok(())
    }

    async fn run_incremental(
        &self,
        table: &ResolvedPostgresTable,
        schema: &TableSchema,
        dest: &dyn Destination,
        checkpoint: &mut TableCheckpoint,
        options: &TableRunOptions<'_>,
    ) -> Result<()> {
        let updated_at = table
            .updated_at_column
            .as_ref()
            .context("updated_at_column required for incremental sync")?;
        let (schema_name, table_name) = split_table_name(&table.name);
        let pk_cast = self
            .resolve_primary_key_cast(&schema_name, &table_name, &table.primary_key)
            .await?;

        if checkpoint.last_synced_at.is_some() && checkpoint.last_primary_key.is_none() {
            warn!(
                table = %table.name,
                "missing last_primary_key in checkpoint; falling back to updated_at-only paging"
            );
        }

        let mut last_seen = checkpoint
            .last_synced_at
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_else(Utc::now));
        let mut last_pk = checkpoint.last_primary_key.clone();

        let select_columns = build_select_columns(schema);

        loop {
            let sql = build_incremental_sql(&IncrementalSqlParts {
                schema: &schema_name,
                table: &table_name,
                columns: &select_columns,
                updated_at,
                primary_key: &table.primary_key,
                pk_cast: &pk_cast,
                where_clause: table.where_clause.as_deref(),
                has_last_pk: last_pk.is_some(),
            });

            let extract_start = Instant::now();
            let rows = if let Some(last_pk_value) = last_pk.as_ref() {
                sqlx::query(&sql)
                    .bind(last_seen)
                    .bind(last_pk_value)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            } else {
                sqlx::query(&sql)
                    .bind(last_seen)
                    .bind(options.batch_size as i64)
                    .fetch_all(&self.pool)
                    .await?
            };
            let extract_ms = extract_start.elapsed().as_millis() as u64;

            if rows.is_empty() {
                break;
            }

            let batch_synced_at = Utc::now();
            let batch = rows_to_batch(schema, &rows, table, batch_synced_at, &self.metadata)?;
            if let Some(stats) = options.stats {
                stats
                    .record_extract(&table.name, rows.len(), extract_ms)
                    .await;
            }
            if !options.dry_run {
                let load_start = Instant::now();
                dest.write_batch(
                    &schema.name,
                    schema,
                    &batch,
                    WriteMode::Upsert,
                    Some(&table.primary_key),
                )
                .await?;
                if let Some(stats) = options.stats {
                    let load_ms = load_start.elapsed().as_millis() as u64;
                    stats
                        .record_load(&table.name, rows.len(), rows.len(), 0, load_ms)
                        .await;
                }
            }

            let last_row = rows
                .last()
                .context("incremental batch empty after non-empty check")?;
            let next_seen = read_updated_at(last_row, updated_at)
                .context("missing updated_at value for incremental paging")?;
            let next_pk = read_primary_key(last_row, &table.primary_key)?;
            last_seen = next_seen;
            last_pk = Some(next_pk.clone());
            checkpoint.last_synced_at = Some(last_seen);
            checkpoint.last_primary_key = Some(next_pk);
            if let Some(state_handle) = &options.state_handle {
                state_handle
                    .save_postgres_checkpoint(&table.name, checkpoint)
                    .await?;
            }
        }
        Ok(())
    }

    pub(super) async fn build_pg_connection_config(&self) -> Result<PgConnectionConfig> {
        let url = Url::parse(&self.config.url).context("invalid postgres url")?;
        let host = url.host_str().context("postgres url missing host")?;
        let username = url.username();
        if username.is_empty() {
            anyhow::bail!("postgres url missing username");
        }
        let db_name = url.path().trim_start_matches('/');
        if db_name.is_empty() {
            anyhow::bail!("postgres url missing database name");
        }
        let port = url.port().unwrap_or(5432);
        let password = url
            .password()
            .map(|pw| SecretString::new(pw.to_string().into()));

        let tls = if self.config.cdc_tls.unwrap_or(false) {
            let pem = if let Some(raw) = &self.config.cdc_tls_ca {
                raw.clone()
            } else if let Some(path) = &self.config.cdc_tls_ca_path {
                tokio::fs::read_to_string(path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?
            } else {
                String::new()
            };
            TlsConfig {
                trusted_root_certs: pem,
                enabled: true,
            }
        } else {
            TlsConfig::disabled()
        };

        Ok(PgConnectionConfig {
            host: host.to_string(),
            port,
            name: db_name.to_string(),
            username: username.to_string(),
            password,
            tls,
            keepalive: None,
        })
    }

    pub(super) async fn create_exported_snapshot_slot(
        &self,
        slot_name: &str,
    ) -> Result<(ExportedSnapshotSlot, ExportedSnapshotSlotInfo)> {
        let pg_config = self.build_pg_connection_config().await?;
        let client = connect_replication_control_client(&pg_config).await?;
        let info = create_exported_snapshot_slot_info(&client, slot_name).await?;
        Ok((
            ExportedSnapshotSlot {
                client: Some(client),
            },
            info,
        ))
    }

    pub(super) async fn run_snapshot_copy_with_exported_snapshot(
        &self,
        table: &ResolvedPostgresTable,
        schema: &TableSchema,
        chunk: Option<SnapshotChunkRange>,
        ctx: SnapshotCopyContext<'_>,
    ) -> Result<()> {
        let SnapshotCopyContext {
            dest,
            snapshot_name,
            batch_size,
            stats,
            checkpoint_state,
            resume_from_primary_key,
            write_mode,
            state_handle,
            shutdown,
        } = ctx;
        let (schema_name, table_name) = split_table_name(&table.name);
        let table_ref = format!(
            "{}.{}",
            quote_pg_identifier(&schema_name),
            quote_pg_identifier(&table_name)
        );

        if !schema.columns.is_empty() {
            dest.ensure_table(schema).await?;
        }

        let pk_cast = self
            .resolve_primary_key_cast(&schema_name, &table_name, &table.primary_key)
            .await?;
        let pk_alias = "__cdsync_pk";
        let pk_ident = quote_pg_identifier(&table.primary_key);
        let select_columns = build_select_columns(schema);
        let select_expr = format!(
            "{}, {}::text as {}",
            select_columns,
            pk_ident,
            quote_pg_identifier(pk_alias)
        );
        let mut filters = Vec::new();
        if let Some(where_clause) = &table.where_clause {
            filters.push(format!("({where_clause})"));
        }
        if let Some(chunk) = chunk {
            filters.push(snapshot_chunk_filter_sql(&pk_ident, &pk_cast, chunk));
        }
        let has_filters = !filters.is_empty();
        let base_sql = if has_filters {
            format!(
                "SELECT {select} FROM {table_ref} WHERE {filters}",
                select = select_expr,
                table_ref = table_ref,
                filters = filters.join(" AND ")
            )
        } else {
            format!(
                "SELECT {select} FROM {table_ref}",
                select = select_expr,
                table_ref = table_ref
            )
        };
        let pg_config = self.build_pg_connection_config().await?;
        let connection = acquire_snapshot_reader(&pg_config, snapshot_name).await?;
        let mut last_pk = resume_from_primary_key;
        let mut extracted_rows = 0usize;
        let mut loaded_batches = 0usize;
        let mut completed = false;
        let mut copy_exit = SnapshotCopyExit::Completed;
        let chunk_start = chunk.map(|range| range.start_pk);
        let chunk_end = chunk.map(|range| range.end_pk);
        let snapshot_label = snapshot_name.unwrap_or("resume");

        let run_result: Result<()> = async {
            loop {
                if snapshot_shutdown_requested(shutdown.as_ref()) {
                    copy_exit = SnapshotCopyExit::Interrupted;
                    info!(
                        table = %table.name,
                        snapshot_name = snapshot_label,
                        chunk_start,
                        chunk_end,
                        "shutdown requested before starting next exported snapshot batch"
                    );
                    break;
                }
                let extract_start = Instant::now();
                let rows = if let Some(last_pk_value) = last_pk.as_ref() {
                    let query = if has_filters {
                        format!(
                            "{base_sql} AND {pk} > {last_pk}::{pk_cast} ORDER BY {pk} LIMIT {limit}",
                            base_sql = base_sql,
                            pk = pk_ident,
                            last_pk = quote_pg_literal(last_pk_value),
                            pk_cast = pk_cast,
                            limit = batch_size,
                        )
                    } else {
                        format!(
                            "{base_sql} WHERE {pk} > {last_pk}::{pk_cast} ORDER BY {pk} LIMIT {limit}",
                            base_sql = base_sql,
                            pk = pk_ident,
                            last_pk = quote_pg_literal(last_pk_value),
                            pk_cast = pk_cast,
                            limit = batch_size,
                        )
                    };
                    connection.query(&query, &[]).await?
                } else {
                    let query = format!("{} ORDER BY {} LIMIT {}", base_sql, pk_ident, batch_size);
                    connection.query(&query, &[]).await?
                };
                let extract_ms = extract_start.elapsed().as_millis() as u64;

                if rows.is_empty() {
                    break;
                }

                extracted_rows += rows.len();
                let batch =
                    tokio_rows_to_batch(schema, &rows, table, Utc::now(), &self.metadata)?;
                if let Some(stats) = &stats {
                    stats
                        .record_extract(&table.name, rows.len(), extract_ms)
                        .await;
                }

                let load_start = Instant::now();
                write_snapshot_batch(dest, schema, &batch, write_mode, &table.primary_key).await?;
                if let Some(stats) = &stats {
                    let upserted = if matches!(write_mode, WriteMode::Upsert) {
                        rows.len()
                    } else {
                        0
                    };
                    stats
                        .record_load(
                            &table.name,
                            rows.len(),
                            upserted,
                            0,
                            load_start.elapsed().as_millis() as u64,
                        )
                        .await;
                }

                loaded_batches += 1;
                if should_log_snapshot_progress(loaded_batches) {
                    info!(
                        table = %table.name,
                        extracted_rows,
                        loaded_batches,
                        snapshot_name = snapshot_label,
                        chunk_start,
                        chunk_end,
                        "exported snapshot copy progress"
                    );
                }

                last_pk = Some(
                    rows.last()
                        .and_then(|row| row.try_get::<_, String>(pk_alias).ok())
                        .context("missing primary key value for snapshot pagination")?,
                );
                save_snapshot_progress(
                    &checkpoint_state,
                    &table.name,
                    chunk,
                    last_pk.clone(),
                    false,
                    state_handle.as_ref(),
                )
                .await?;

                if snapshot_shutdown_requested(shutdown.as_ref()) {
                    copy_exit = SnapshotCopyExit::Interrupted;
                    info!(
                        table = %table.name,
                        extracted_rows,
                        loaded_batches,
                        snapshot_name = snapshot_label,
                        chunk_start,
                        chunk_end,
                        "shutdown requested after exported snapshot batch checkpoint"
                    );
                    break;
                }
            }

            completed = matches!(copy_exit, SnapshotCopyExit::Completed);
            Ok(())
        }
        .await;

        release_exported_snapshot_reader(&connection, completed).await?;
        run_result?;

        let snapshot_completed = finalize_snapshot_copy_progress(
            &checkpoint_state,
            &table.name,
            chunk,
            last_pk.clone(),
            copy_exit,
            state_handle.as_ref(),
        )
        .await?;
        if !snapshot_completed {
            info!(
                table = %table.name,
                extracted_rows,
                loaded_batches,
                snapshot_name = snapshot_label,
                chunk_start,
                chunk_end,
                "shutdown requested during exported snapshot copy; preserving checkpoint for resume"
            );
            return Ok(());
        }

        info!(
            table = %table.name,
            extracted_rows,
            loaded_batches,
            snapshot_name = snapshot_label,
            chunk_start,
            chunk_end,
            "completed exported snapshot copy"
        );
        Ok(())
    }
}

async fn connect_replication_control_client(
    pg_config: &PgConnectionConfig,
) -> Result<TokioPgClient> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(pg_config.host.clone())
        .port(pg_config.port)
        .dbname(pg_config.name.clone())
        .user(pg_config.username.clone())
        .replication_mode(ReplicationMode::Logical);

    if let Some(password) = &pg_config.password {
        config.password(password.expose_secret());
    }

    if pg_config.tls.enabled {
        let mut builder = native_tls::TlsConnector::builder();
        add_native_tls_root_certs(&mut builder, &pg_config.tls.trusted_root_certs)?;
        let connector = MakeNativeTlsConnect::new(builder.build()?);
        let (client, connection) = config.connect(connector).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "exported snapshot replication connection error");
            }
        });
        Ok(client)
    } else {
        config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "exported snapshot replication connection error");
            }
        });
        Ok(client)
    }
}

async fn connect_snapshot_reader_client(pg_config: &PgConnectionConfig) -> Result<TokioPgClient> {
    let mut config = tokio_postgres::Config::new();
    config
        .host(pg_config.host.clone())
        .port(pg_config.port)
        .dbname(pg_config.name.clone())
        .user(pg_config.username.clone());

    if let Some(password) = &pg_config.password {
        config.password(password.expose_secret());
    }

    if pg_config.tls.enabled {
        let mut builder = native_tls::TlsConnector::builder();
        add_native_tls_root_certs(&mut builder, &pg_config.tls.trusted_root_certs)?;
        let connector = MakeNativeTlsConnect::new(builder.build()?);
        let (client, connection) = config.connect(connector).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "exported snapshot reader connection error");
            }
        });
        Ok(client)
    } else {
        config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
        let (client, connection) = config.connect(NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "exported snapshot reader connection error");
            }
        });
        Ok(client)
    }
}

fn add_native_tls_root_certs(
    builder: &mut native_tls::TlsConnectorBuilder,
    pem_bundle: &str,
) -> Result<()> {
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(pem_bundle.as_bytes()) {
        let cert = cert?;
        builder.add_root_certificate(Certificate::from_der(cert.as_ref())?);
    }
    Ok(())
}

async fn create_exported_snapshot_slot_info(
    client: &TokioPgClient,
    slot_name: &str,
) -> Result<ExportedSnapshotSlotInfo> {
    let query = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput EXPORT_SNAPSHOT",
        quote_pg_identifier(slot_name)
    );

    for message in client.simple_query(&query).await? {
        if let SimpleQueryMessage::Row(row) = message {
            let consistent_point = row
                .get("consistent_point")
                .context("replication slot response missing consistent_point")?
                .to_string();
            let snapshot_name = row
                .get("snapshot_name")
                .context("replication slot response missing snapshot_name")?
                .to_string();
            if snapshot_name.is_empty() {
                anyhow::bail!("replication slot response returned empty snapshot_name");
            }
            return Ok(ExportedSnapshotSlotInfo {
                consistent_point,
                snapshot_name,
            });
        }
    }

    anyhow::bail!("replication slot creation returned no row")
}

async fn acquire_snapshot_reader(
    pg_config: &PgConnectionConfig,
    snapshot_name: Option<&str>,
) -> Result<TokioPgClient> {
    let connection = connect_snapshot_reader_client(pg_config).await?;
    connection
        .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
        .await
        .with_context(|| "starting exported snapshot reader transaction")?;
    if let Some(snapshot_name) = snapshot_name
        && let Err(err) = connection
            .simple_query(&format!(
                "SET TRANSACTION SNAPSHOT {}",
                quote_pg_literal(snapshot_name)
            ))
            .await
    {
        let _ = connection.simple_query("ROLLBACK;").await;
        return Err(err).with_context(|| {
            format!(
                "importing exported snapshot {} into snapshot reader transaction",
                snapshot_name
            )
        });
    }
    Ok(connection)
}

async fn release_exported_snapshot_reader(connection: &TokioPgClient, commit: bool) -> Result<()> {
    let statement = if commit { "COMMIT;" } else { "ROLLBACK;" };
    connection.simple_query(statement).await?;
    Ok(())
}

pub(super) async fn release_exported_snapshot_slot(
    mut slot: ExportedSnapshotSlot,
    _commit: bool,
) -> Result<()> {
    let _ = slot.client.take();
    Ok(())
}

pub(super) async fn save_snapshot_progress(
    checkpoint_state: &Arc<Mutex<TableCheckpoint>>,
    table_name: &str,
    chunk: Option<SnapshotChunkRange>,
    last_primary_key: Option<String>,
    complete: bool,
    state_handle: Option<&StateHandle>,
) -> Result<()> {
    let checkpoint = {
        let mut checkpoint = checkpoint_state.lock().await;
        checkpoint.last_synced_at = Some(Utc::now());
        update_snapshot_chunk_checkpoint(&mut checkpoint, chunk, last_primary_key, complete)?;
        checkpoint.clone()
    };
    if let Some(state_handle) = state_handle {
        state_handle
            .save_postgres_checkpoint(table_name, &checkpoint)
            .await?;
    }
    Ok(())
}

pub(super) fn snapshot_shutdown_requested(shutdown: Option<&ShutdownSignal>) -> bool {
    shutdown.is_some_and(ShutdownSignal::is_shutdown)
}
