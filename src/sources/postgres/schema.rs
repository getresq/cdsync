use super::*;

#[derive(Default)]
pub(super) struct SchemaDiff {
    pub(super) added: Vec<String>,
    pub(super) removed: Vec<String>,
    pub(super) type_changed: Vec<(String, DataType, DataType)>,
    pub(super) nullable_changed: Vec<(String, bool, bool)>,
}

impl SchemaDiff {
    pub(super) fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.type_changed.is_empty()
            && self.nullable_changed.is_empty()
    }

    pub(super) fn has_incompatible(&self) -> bool {
        !self.type_changed.is_empty()
    }
}

pub(super) fn normalize_filter(value: &str) -> String {
    #[derive(Clone, Copy)]
    enum Mode {
        Normal,
        SingleQuoted,
        DoubleQuoted,
    }

    let mut normalized = String::with_capacity(value.len());
    let mut mode = Mode::Normal;
    let mut chars = value.chars().peekable();

    while let Some(ch) = chars.next() {
        match mode {
            Mode::Normal => {
                if ch.is_whitespace() {
                    continue;
                }
                match ch {
                    '\'' => {
                        normalized.push(ch);
                        mode = Mode::SingleQuoted;
                    }
                    '"' => {
                        normalized.push(ch);
                        mode = Mode::DoubleQuoted;
                    }
                    _ => normalized.extend(ch.to_lowercase()),
                }
            }
            Mode::SingleQuoted => {
                normalized.push(ch);
                if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        normalized.push(chars.next().expect("peeked single quote"));
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
            Mode::DoubleQuoted => {
                normalized.push(ch);
                if ch == '"' {
                    if chars.peek() == Some(&'"') {
                        normalized.push(chars.next().expect("peeked double quote"));
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
        }
    }

    strip_wrapping_parentheses(&normalized)
}

fn strip_wrapping_parentheses(value: &str) -> String {
    let mut current = value;
    while let Some(stripped) = strip_one_wrapping_parentheses(current) {
        current = stripped;
    }
    current.to_string()
}

fn strip_one_wrapping_parentheses(value: &str) -> Option<&str> {
    if !value.starts_with('(') || !value.ends_with(')') {
        return None;
    }

    #[derive(Clone, Copy)]
    enum Mode {
        Normal,
        SingleQuoted,
        DoubleQuoted,
    }

    let mut mode = Mode::Normal;
    let mut depth = 0usize;
    let bytes = value.as_bytes();
    let last_index = bytes.len().checked_sub(1)?;
    let mut idx = 0usize;

    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        match mode {
            Mode::Normal => match ch {
                '\'' => mode = Mode::SingleQuoted,
                '"' => mode = Mode::DoubleQuoted,
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 && idx != last_index {
                        return None;
                    }
                }
                _ => {}
            },
            Mode::SingleQuoted => {
                if ch == '\'' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] as char == '\'' {
                        idx += 1;
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
            Mode::DoubleQuoted => {
                if ch == '"' {
                    if idx + 1 < bytes.len() && bytes[idx + 1] as char == '"' {
                        idx += 1;
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
        }
        idx += 1;
    }

    (depth == 0).then_some(&value[1..last_index])
}

pub(super) fn schema_fingerprint(schema: &TableSchema) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schema.name.hash(&mut hasher);
    schema.primary_key.hash(&mut hasher);
    for column in &schema.columns {
        column.name.hash(&mut hasher);
        std::mem::discriminant(&column.data_type).hash(&mut hasher);
        column.nullable.hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}

pub(super) fn schema_snapshot_from_schema(schema: &TableSchema) -> Vec<SchemaFieldSnapshot> {
    schema
        .columns
        .iter()
        .map(|col| SchemaFieldSnapshot {
            name: col.name.clone(),
            data_type: col.data_type.clone(),
            nullable: col.nullable,
        })
        .collect()
}

pub(super) fn should_log_snapshot_progress(completed_batches: usize) -> bool {
    completed_batches != 0 && completed_batches.is_multiple_of(10)
}

pub(super) fn schema_diff(
    previous: Option<&[SchemaFieldSnapshot]>,
    current: &TableSchema,
) -> Option<SchemaDiff> {
    let previous = previous?;
    let mut diff = SchemaDiff::default();
    let mut prev_map: HashMap<&str, &SchemaFieldSnapshot> = HashMap::new();
    for col in previous {
        prev_map.insert(col.name.as_str(), col);
    }

    let mut current_names = HashSet::new();
    for col in &current.columns {
        current_names.insert(col.name.as_str());
        if let Some(prev) = prev_map.get(col.name.as_str()) {
            if prev.data_type != col.data_type {
                diff.type_changed.push((
                    col.name.clone(),
                    prev.data_type.clone(),
                    col.data_type.clone(),
                ));
            } else if prev.nullable != col.nullable {
                diff.nullable_changed
                    .push((col.name.clone(), prev.nullable, col.nullable));
            }
        } else {
            diff.added.push(col.name.clone());
        }
    }

    for prev in previous {
        if !current_names.contains(prev.name.as_str()) {
            diff.removed.push(prev.name.clone());
        }
    }

    Some(diff)
}

pub(super) fn log_schema_diff(table: &str, diff: &SchemaDiff) {
    if !diff.added.is_empty() {
        info!(table = %table, added = ?diff.added, "schema change: added columns");
    }
    if !diff.removed.is_empty() {
        warn!(table = %table, removed = ?diff.removed, "schema change: removed columns");
    }
    if !diff.type_changed.is_empty() {
        warn!(table = %table, type_changed = ?diff.type_changed, "schema change: column type changes");
    }
    if !diff.nullable_changed.is_empty() {
        warn!(
            table = %table,
            nullable_changed = ?diff.nullable_changed,
            "schema change: nullable changes"
        );
    }
}

pub(super) fn primary_key_changed(
    previous_primary_key: Option<&str>,
    previous_schema_hash: Option<&str>,
    current: &TableSchema,
    current_schema_hash: &str,
    diff: &SchemaDiff,
) -> bool {
    if let Some(previous_primary_key) = previous_primary_key {
        return current.primary_key.as_deref() != Some(previous_primary_key);
    }

    diff.is_empty()
        && previous_schema_hash
            .is_some_and(|previous_schema_hash| previous_schema_hash != current_schema_hash)
}

#[cfg(test)]
mod tests {
    use super::normalize_filter;

    #[test]
    fn normalize_filter_ignores_whitespace_and_keyword_case() {
        let lhs = "tenant_id = 1 AND deleted_at IS NULL";
        let rhs = "tenant_id=1 and   deleted_at   is null";
        assert_eq!(normalize_filter(lhs), normalize_filter(rhs));
    }

    #[test]
    fn normalize_filter_ignores_wrapping_parentheses() {
        let lhs = "tenant_id = 1";
        let rhs = "(tenant_id = 1)";
        let nested = "(((tenant_id = 1)))";
        assert_eq!(normalize_filter(lhs), normalize_filter(rhs));
        assert_eq!(normalize_filter(lhs), normalize_filter(nested));
    }

    #[test]
    fn normalize_filter_preserves_string_literal_case() {
        let lhs = "name = 'Foo'";
        let rhs = "name = 'foo'";
        assert_ne!(normalize_filter(lhs), normalize_filter(rhs));
    }

    #[test]
    fn normalize_filter_preserves_quoted_identifiers() {
        let lhs = "\"TenantId\" = 1";
        let rhs = "\"tenantid\" = 1";
        assert_ne!(normalize_filter(lhs), normalize_filter(rhs));
    }
}
