use super::*;

pub(super) fn validate_schema_name(schema: &str) -> anyhow::Result<()> {
    if schema.is_empty() {
        anyhow::bail!("postgres state schema must not be empty");
    }
    if schema
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Ok(());
    }
    anyhow::bail!("invalid postgres state schema `{schema}`");
}

pub(super) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn cdc_batch_load_job_record_from_row(
    row: PgRow,
) -> anyhow::Result<CdcBatchLoadJobRecord> {
    Ok(CdcBatchLoadJobRecord {
        job_id: row.try_get("job_id")?,
        table_key: row.try_get("table_key")?,
        first_sequence: row.try_get::<i64, _>("first_sequence")? as u64,
        status: CdcBatchLoadJobStatus::from_str(&row.try_get::<String, _>("status")?)?,
        payload_json: row.try_get("payload_json")?,
        attempt_count: row.try_get("attempt_count")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(super) async fn ensure_schema_exists(
    conn: &mut sqlx::PgConnection,
    schema: &str,
) -> anyhow::Result<()> {
    let exists: bool = sqlx::query_scalar(
        "select exists(select 1 from information_schema.schemata where schema_name = $1)",
    )
    .bind(schema)
    .fetch_one(&mut *conn)
    .await?;
    if !exists {
        anyhow::bail!("required schema {schema} does not exist");
    }
    Ok(())
}

pub(super) async fn create_schema_if_missing(
    conn: &mut sqlx::PgConnection,
    schema: &str,
) -> anyhow::Result<()> {
    sqlx::query(&format!("create schema if not exists {}", quote_ident(schema)))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub(super) async fn ensure_table_exists(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    table: &str,
) -> anyhow::Result<()> {
    let exists: bool = sqlx::query_scalar(
        r#"
        select exists(
            select 1
            from information_schema.tables
            where table_schema = $1 and table_name = $2
        )
        "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_one(&mut *conn)
    .await?;
    if !exists {
        anyhow::bail!(
            "required table {}.{} does not exist; run `cdsync migrate --config ...` first",
            schema,
            table
        );
    }
    Ok(())
}

pub(super) fn load_cdc_state_from_row(row: &PgRow) -> anyhow::Result<Option<PostgresCdcState>> {
    let last_lsn: Option<String> = row.try_get("postgres_cdc_last_lsn")?;
    let slot_name: Option<String> = row.try_get("postgres_cdc_slot_name")?;
    if last_lsn.is_none() && slot_name.is_none() {
        return Ok(None);
    }
    Ok(Some(PostgresCdcState {
        last_lsn,
        slot_name,
    }))
}

pub(super) fn parse_optional_rfc3339(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

pub(super) fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

pub(super) fn datetime_from_millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value)
}

pub(super) fn max_updated_at(current: Option<i64>, next: i64) -> Option<i64> {
    match current {
        Some(current) => Some(current.max(next)),
        None => Some(next),
    }
}
