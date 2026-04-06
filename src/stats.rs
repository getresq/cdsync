use crate::config::StatsConfig;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::{Mutex as AsyncMutex, watch};
use uuid::Uuid;
static STATS_MIGRATOR: Migrator = sqlx::migrate!("./migrations/stats");
static LIVE_RUN_REGISTRY: OnceLock<Mutex<HashMap<String, watch::Sender<RunStatsSnapshot>>>> =
    OnceLock::new();

#[derive(Clone)]
pub struct StatsHandle {
    inner: Arc<AsyncMutex<RunStatsState>>,
    live_tx: watch::Sender<RunStatsSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStatsSnapshot {
    pub run_id: String,
    pub connection_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub error: Option<String>,
    pub rows_read: i64,
    pub rows_written: i64,
    pub rows_deleted: i64,
    pub rows_upserted: i64,
    pub extract_ms: i64,
    pub load_ms: i64,
    pub api_calls: i64,
    pub rate_limit_hits: i64,
    pub tables: Vec<TableStatsSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummary {
    pub run_id: String,
    pub connection_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: Option<String>,
    pub error: Option<String>,
    pub rows_read: i64,
    pub rows_written: i64,
    pub rows_deleted: i64,
    pub rows_upserted: i64,
    pub extract_ms: i64,
    pub load_ms: i64,
    pub api_calls: i64,
    pub rate_limit_hits: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatsSnapshot {
    pub run_id: String,
    pub connection_id: String,
    pub table_name: String,
    pub rows_read: i64,
    pub rows_written: i64,
    pub rows_deleted: i64,
    pub rows_upserted: i64,
    pub extract_ms: i64,
    pub load_ms: i64,
}

#[derive(Debug, Default, Clone)]
struct TableStatsState {
    rows_read: i64,
    rows_written: i64,
    rows_deleted: i64,
    rows_upserted: i64,
    extract_ms: i64,
    load_ms: i64,
}

#[derive(Debug, Clone)]
struct RunStatsState {
    run_id: String,
    connection_id: String,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    status: Option<String>,
    error: Option<String>,
    rows_read: i64,
    rows_written: i64,
    rows_deleted: i64,
    rows_upserted: i64,
    extract_ms: i64,
    load_ms: i64,
    api_calls: i64,
    rate_limit_hits: i64,
    tables: HashMap<String, TableStatsState>,
}

impl StatsHandle {
    pub fn new(connection_id: &str) -> Self {
        let run_id = Uuid::new_v4().to_string();
        let state = RunStatsState {
            run_id,
            connection_id: connection_id.to_string(),
            started_at: Utc::now(),
            finished_at: None,
            status: Some("running".to_string()),
            error: None,
            rows_read: 0,
            rows_written: 0,
            rows_deleted: 0,
            rows_upserted: 0,
            extract_ms: 0,
            load_ms: 0,
            api_calls: 0,
            rate_limit_hits: 0,
            tables: HashMap::new(),
        };
        let snapshot = snapshot_from_state(&state);
        let (live_tx, _live_rx) = watch::channel(snapshot.clone());
        live_run_registry()
            .lock()
            .expect("live run registry lock poisoned")
            .insert(connection_id.to_string(), live_tx.clone());

        Self {
            inner: Arc::new(AsyncMutex::new(state)),
            live_tx,
        }
    }

    pub async fn finish(&self, status: &str, error: Option<String>) {
        let mut guard = self.inner.lock().await;
        guard.finished_at = Some(Utc::now());
        guard.status = Some(status.to_string());
        guard.error = error;
        self.live_tx.send_replace(snapshot_from_state(&guard));
    }

    pub async fn run_id(&self) -> String {
        let guard = self.inner.lock().await;
        guard.run_id.clone()
    }

    pub async fn record_extract(&self, table: &str, rows: usize, elapsed_ms: u64) {
        let mut guard = self.inner.lock().await;
        let rows = rows as i64;
        guard.rows_read += rows;
        guard.extract_ms += elapsed_ms as i64;
        let table_stats = guard.tables.entry(table.to_string()).or_default();
        table_stats.rows_read += rows;
        table_stats.extract_ms += elapsed_ms as i64;
        crate::telemetry::record_rows_read(table, rows as u64);
        self.live_tx.send_replace(snapshot_from_state(&guard));
    }

    pub async fn record_load(
        &self,
        table: &str,
        rows: usize,
        upserted: usize,
        deleted: usize,
        elapsed_ms: u64,
    ) {
        let mut guard = self.inner.lock().await;
        let rows = rows as i64;
        let upserted = upserted as i64;
        let deleted = deleted as i64;
        guard.rows_written += rows;
        guard.rows_upserted += upserted;
        guard.rows_deleted += deleted;
        guard.load_ms += elapsed_ms as i64;
        let table_stats = guard.tables.entry(table.to_string()).or_default();
        table_stats.rows_written += rows;
        table_stats.rows_upserted += upserted;
        table_stats.rows_deleted += deleted;
        table_stats.load_ms += elapsed_ms as i64;
        crate::telemetry::record_rows_written(table, rows as u64);
        self.live_tx.send_replace(snapshot_from_state(&guard));
    }

    pub async fn snapshot(&self) -> RunStatsSnapshot {
        let guard = self.inner.lock().await;
        snapshot_from_state(&guard)
    }
}

pub fn live_run_snapshot(connection_id: &str) -> Option<RunStatsSnapshot> {
    live_run_registry()
        .lock()
        .expect("live run registry lock poisoned")
        .get(connection_id)
        .map(|sender| sender.borrow().clone())
}

pub fn summarize_run(snapshot: &RunStatsSnapshot) -> RunSummary {
    RunSummary {
        run_id: snapshot.run_id.clone(),
        connection_id: snapshot.connection_id.clone(),
        started_at: snapshot.started_at,
        finished_at: snapshot.finished_at,
        status: snapshot.status.clone(),
        error: snapshot.error.clone(),
        rows_read: snapshot.rows_read,
        rows_written: snapshot.rows_written,
        rows_deleted: snapshot.rows_deleted,
        rows_upserted: snapshot.rows_upserted,
        extract_ms: snapshot.extract_ms,
        load_ms: snapshot.load_ms,
        api_calls: snapshot.api_calls,
        rate_limit_hits: snapshot.rate_limit_hits,
    }
}

fn live_run_registry() -> &'static Mutex<HashMap<String, watch::Sender<RunStatsSnapshot>>> {
    LIVE_RUN_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn snapshot_from_state(state: &RunStatsState) -> RunStatsSnapshot {
    let tables = state
        .tables
        .iter()
        .map(|(name, stats)| TableStatsSnapshot {
            run_id: state.run_id.clone(),
            connection_id: state.connection_id.clone(),
            table_name: name.clone(),
            rows_read: stats.rows_read,
            rows_written: stats.rows_written,
            rows_deleted: stats.rows_deleted,
            rows_upserted: stats.rows_upserted,
            extract_ms: stats.extract_ms,
            load_ms: stats.load_ms,
        })
        .collect();
    RunStatsSnapshot {
        run_id: state.run_id.clone(),
        connection_id: state.connection_id.clone(),
        started_at: state.started_at,
        finished_at: state.finished_at,
        status: state.status.clone(),
        error: state.error.clone(),
        rows_read: state.rows_read,
        rows_written: state.rows_written,
        rows_deleted: state.rows_deleted,
        rows_upserted: state.rows_upserted,
        extract_ms: state.extract_ms,
        load_ms: state.load_ms,
        api_calls: state.api_calls,
        rate_limit_hits: state.rate_limit_hits,
        tables,
    }
}

#[derive(Clone)]
pub struct StatsDb {
    pool: PgPool,
    schema: String,
}

impl StatsDb {
    pub async fn migrate_with_config(
        config: &StatsConfig,
        default_url: &str,
    ) -> anyhow::Result<()> {
        let url = config.url.as_deref().unwrap_or(default_url);
        let schema = config.schema_name().to_string();
        validate_schema_name(&schema)?;
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        let db = Self { pool, schema };
        db.migrate().await
    }

    pub async fn new(config: &StatsConfig, default_url: &str) -> anyhow::Result<Self> {
        let url = config.url.as_deref().unwrap_or(default_url);
        let schema = config.schema_name().to_string();
        validate_schema_name(&schema)?;
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        let db = Self { pool, schema };
        db.init().await?;
        Ok(db)
    }

    async fn init(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        ensure_schema_exists(&mut conn, &self.schema).await?;
        ensure_table_exists(&mut conn, &self.schema, "_sqlx_migrations").await?;
        ensure_table_exists(&mut conn, &self.schema, "runs").await?;
        ensure_table_exists(&mut conn, &self.schema, "run_tables").await?;
        Ok(())
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        create_schema_if_missing(&mut conn, &self.schema).await?;
        sqlx::query(&format!("set search_path to {}", quote_ident(&self.schema)))
            .execute(&mut *conn)
            .await?;
        STATS_MIGRATOR.run_direct(&mut *conn).await?;
        Ok(())
    }

    pub async fn persist_run(&self, handle: &StatsHandle) -> anyhow::Result<()> {
        let snapshot = handle.snapshot().await;
        sqlx::query(&format!(
            "delete from {} where run_id = $1",
            self.table("run_tables")
        ))
        .bind(&snapshot.run_id)
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            r#"
            insert into {} (
                run_id,
                connection_id,
                started_at,
                finished_at,
                status,
                error,
                rows_read,
                rows_written,
                rows_deleted,
                rows_upserted,
                extract_ms,
                load_ms,
                api_calls,
                rate_limit_hits
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            on conflict(run_id) do update set
                connection_id = excluded.connection_id,
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                status = excluded.status,
                error = excluded.error,
                rows_read = excluded.rows_read,
                rows_written = excluded.rows_written,
                rows_deleted = excluded.rows_deleted,
                rows_upserted = excluded.rows_upserted,
                extract_ms = excluded.extract_ms,
                load_ms = excluded.load_ms,
                api_calls = excluded.api_calls,
                rate_limit_hits = excluded.rate_limit_hits
            "#,
            self.table("runs")
        ))
        .bind(&snapshot.run_id)
        .bind(&snapshot.connection_id)
        .bind(snapshot.started_at.to_rfc3339())
        .bind(snapshot.finished_at.map(|t| t.to_rfc3339()))
        .bind(snapshot.status)
        .bind(snapshot.error)
        .bind(snapshot.rows_read)
        .bind(snapshot.rows_written)
        .bind(snapshot.rows_deleted)
        .bind(snapshot.rows_upserted)
        .bind(snapshot.extract_ms)
        .bind(snapshot.load_ms)
        .bind(snapshot.api_calls)
        .bind(snapshot.rate_limit_hits)
        .execute(&self.pool)
        .await?;

        for table in snapshot.tables {
            sqlx::query(&format!(
                r#"
                insert into {} (
                    run_id,
                    connection_id,
                    table_name,
                    rows_read,
                    rows_written,
                    rows_deleted,
                    rows_upserted,
                    extract_ms,
                    load_ms
                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
                self.table("run_tables")
            ))
            .bind(&table.run_id)
            .bind(&table.connection_id)
            .bind(&table.table_name)
            .bind(table.rows_read)
            .bind(table.rows_written)
            .bind(table.rows_deleted)
            .bind(table.rows_upserted)
            .bind(table.extract_ms)
            .bind(table.load_ms)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub async fn recent_runs(
        &self,
        connection_id: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<RunSummary>> {
        let limit = limit.max(1) as i64;
        let rows = if let Some(connection_id) = connection_id {
            sqlx::query(&format!(
                r#"
                select
                    run_id,
                    connection_id,
                    started_at,
                    finished_at,
                    status,
                    error,
                    rows_read,
                    rows_written,
                    rows_deleted,
                    rows_upserted,
                    extract_ms,
                    load_ms,
                    api_calls,
                    rate_limit_hits
                from {}
                where connection_id = $1
                order by started_at desc
                limit $2
                "#,
                self.table("runs")
            ))
            .bind(connection_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(&format!(
                r#"
                select
                    run_id,
                    connection_id,
                    started_at,
                    finished_at,
                    status,
                    error,
                    rows_read,
                    rows_written,
                    rows_deleted,
                    rows_upserted,
                    extract_ms,
                    load_ms,
                    api_calls,
                    rate_limit_hits
                from {}
                order by started_at desc
                limit $1
                "#,
                self.table("runs")
            ))
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        rows.into_iter()
            .map(|row| {
                Ok(RunSummary {
                    run_id: row.try_get("run_id")?,
                    connection_id: row.try_get("connection_id")?,
                    started_at: parse_rfc3339(row.try_get("started_at")?)?,
                    finished_at: row
                        .try_get::<Option<String>, _>("finished_at")?
                        .map(parse_rfc3339)
                        .transpose()?,
                    status: row.try_get("status")?,
                    error: row.try_get("error")?,
                    rows_read: row.try_get("rows_read")?,
                    rows_written: row.try_get("rows_written")?,
                    rows_deleted: row.try_get("rows_deleted")?,
                    rows_upserted: row.try_get("rows_upserted")?,
                    extract_ms: row.try_get("extract_ms")?,
                    load_ms: row.try_get("load_ms")?,
                    api_calls: row.try_get("api_calls")?,
                    rate_limit_hits: row.try_get("rate_limit_hits")?,
                })
            })
            .collect()
    }

    pub async fn run_tables(&self, run_id: &str) -> anyhow::Result<Vec<TableStatsSnapshot>> {
        let rows = sqlx::query(&format!(
            r#"
            select
                run_id,
                connection_id,
                table_name,
                rows_read,
                rows_written,
                rows_deleted,
                rows_upserted,
                extract_ms,
                load_ms
            from {}
            where run_id = $1
            order by rows_read desc, table_name asc
            "#,
            self.table("run_tables")
        ))
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(TableStatsSnapshot {
                    run_id: row.try_get("run_id")?,
                    connection_id: row.try_get("connection_id")?,
                    table_name: row.try_get("table_name")?,
                    rows_read: row.try_get("rows_read")?,
                    rows_written: row.try_get("rows_written")?,
                    rows_deleted: row.try_get("rows_deleted")?,
                    rows_upserted: row.try_get("rows_upserted")?,
                    extract_ms: row.try_get("extract_ms")?,
                    load_ms: row.try_get("load_ms")?,
                })
            })
            .collect()
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let _: i32 = sqlx::query_scalar("select 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    fn table(&self, table_name: &str) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(table_name))
    }
}

fn validate_schema_name(schema: &str) -> anyhow::Result<()> {
    let mut chars = schema.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => anyhow::bail!("invalid postgres stats schema `{}`", schema),
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        anyhow::bail!("invalid postgres stats schema `{}`", schema);
    }
    Ok(())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn ensure_schema_exists(conn: &mut sqlx::PgConnection, schema: &str) -> anyhow::Result<()> {
    let exists: bool =
        sqlx::query_scalar("select exists (select 1 from pg_namespace where nspname = $1)")
            .bind(schema)
            .fetch_one(&mut *conn)
            .await?;
    if !exists {
        anyhow::bail!("required schema {} does not exist", schema);
    }
    Ok(())
}

async fn create_schema_if_missing(
    conn: &mut sqlx::PgConnection,
    schema: &str,
) -> anyhow::Result<()> {
    sqlx::query(&format!(
        "create schema if not exists {}",
        quote_ident(schema)
    ))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn ensure_table_exists(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    table: &str,
) -> anyhow::Result<()> {
    let exists: bool = sqlx::query_scalar(
        r#"
        select exists (
            select 1
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = $1
              and c.relname = $2
              and c.relkind in ('r', 'p')
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

fn parse_rfc3339(value: String) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}
