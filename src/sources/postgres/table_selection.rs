use super::*;

pub(super) fn build_globset(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = globset::GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

pub(super) fn collect_table_configs(
    explicit_tables: Option<&[PostgresTableConfig]>,
    selection: Option<&TableSelectionConfig>,
    discovered_names: &[String],
) -> Result<Vec<PostgresTableConfig>> {
    let mut table_map: HashMap<String, PostgresTableConfig> = HashMap::new();

    if let Some(tables) = explicit_tables {
        for table in tables {
            table_map.insert(table.name.clone(), table.clone());
        }
    }

    if let Some(selection) = selection {
        if !selection.include.is_empty() || !selection.exclude.is_empty() {
            let include_set = build_globset(&selection.include)?;
            let exclude_set = build_globset(&selection.exclude)?;

            for name in discovered_names {
                if !selection.include.is_empty() && !include_set.is_match(name) {
                    continue;
                }
                if !selection.exclude.is_empty() && exclude_set.is_match(name) {
                    continue;
                }
                table_map
                    .entry(name.clone())
                    .or_insert(PostgresTableConfig {
                        name: name.clone(),
                        primary_key: None,
                        updated_at_column: None,
                        soft_delete: None,
                        soft_delete_column: None,
                        where_clause: None,
                        columns: None,
                    });
            }
        }

        if !selection.exclude.is_empty() {
            let exclude_set = build_globset(&selection.exclude)?;
            table_map.retain(|name, _| !exclude_set.is_match(name));
        }
    }

    if table_map.is_empty() {
        anyhow::bail!("no postgres tables resolved from config");
    }

    let mut table_configs: Vec<PostgresTableConfig> = table_map.into_values().collect();
    table_configs.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(table_configs)
}

pub(super) fn required_columns(table: &ResolvedPostgresTable) -> HashSet<String> {
    let mut required = HashSet::new();
    required.insert(table.primary_key.clone());
    if let Some(updated_at) = &table.updated_at_column {
        required.insert(updated_at.clone());
    }
    if table.soft_delete
        && let Some(column) = &table.soft_delete_column
    {
        required.insert(column.clone());
    }
    required
}

pub(super) fn ensure_required_columns(
    all_columns: &[ColumnSchema],
    required: &HashSet<String>,
) -> Result<()> {
    for name in required {
        if !all_columns.iter().any(|c| &c.name == name) {
            anyhow::bail!("required column {} not found in source table", name);
        }
    }
    Ok(())
}

pub(super) fn filter_columns(
    all_columns: &[ColumnSchema],
    selection: &ColumnSelection,
    required: &HashSet<String>,
) -> Vec<ColumnSchema> {
    let include_set: HashSet<&str> = selection.include.iter().map(String::as_str).collect();
    let exclude_set: HashSet<&str> = selection.exclude.iter().map(String::as_str).collect();
    let include_all = include_set.is_empty();

    let mut filtered = Vec::new();
    for column in all_columns {
        let mut include = include_all || include_set.contains(column.name.as_str());
        if exclude_set.contains(column.name.as_str()) {
            include = false;
        }
        if required.contains(&column.name) {
            include = true;
        }
        if include {
            filtered.push(column.clone());
        }
    }
    filtered
}
