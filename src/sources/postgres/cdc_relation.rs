use super::*;

pub(super) fn relation_change_requires_destination_ensure(
    prev_snapshot_exists: bool,
    table_known: bool,
    diff: Option<&SchemaDiff>,
) -> bool {
    !prev_snapshot_exists || !table_known || diff.is_some_and(|value| !value.is_empty())
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
        .bind(table_id.into_inner().cast_signed())
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
        .bind(table_id.into_inner().cast_signed())
        .fetch_all(&self.pool)
        .await?;

        let mut column_schemas = Vec::with_capacity(columns.len());
        for row in columns {
            let name: String = row.try_get("column_name")?;
            let type_oid: i32 = row.try_get("type_oid")?;
            let type_modifier: i32 = row.try_get("type_modifier")?;
            let not_null: bool = row.try_get("not_null")?;
            let is_primary: bool = row.try_get("is_primary")?;
            let typ = etl_postgres::types::convert_type_oid_to_type(type_oid.cast_unsigned());
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

    pub(super) async fn handle_relation_change(
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
        if let Some(state_handle) = &runtime.state_handle
            && let Some(latest_checkpoint) = state_handle
                .load_postgres_checkpoint(&table_cfg.name)
                .await?
        {
            runtime
                .state
                .postgres
                .insert(table_cfg.name.clone(), latest_checkpoint);
        }
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
        if let Some(ref diff) = diff
            && !diff.is_empty()
        {
            if runtime.schema_diff_enabled {
                log_schema_diff(&table_cfg.name, diff);
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
                        "schema change detected for {}; set schema_changes=auto for additive changes or trigger a manual table resync",
                        table_cfg.name
                    );
                }
                SchemaChangePolicy::Auto => {
                    if diff.has_incompatible() || primary_key_changed_detected {
                        anyhow::bail!(
                            "incompatible schema change detected for {}; trigger a manual table resync",
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
                        "schema change detected for {}; set schema_changes=auto for additive changes or trigger a manual table resync",
                        table_cfg.name
                    );
                }
                SchemaChangePolicy::Auto => {
                    anyhow::bail!(
                        "incompatible schema change detected for {}; trigger a manual table resync",
                        table_cfg.name
                    );
                }
            }
        }

        let snapshot = schema_snapshot_from_schema(&info.schema);
        if relation_change_requires_destination_ensure(
            prev_snapshot.is_some(),
            runtime.table_info_map.contains_key(&table_id),
            diff.as_ref(),
        ) {
            runtime.dest.ensure_table_schema(&info.schema).await?;
        }
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
