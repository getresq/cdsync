use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct StatsHandle {
    inner: Arc<Mutex<RunStatsState>>,
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
            status: None,
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

        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    pub async fn finish(&self, status: &str, error: Option<String>) {
        let mut guard = self.inner.lock().await;
        guard.finished_at = Some(Utc::now());
        guard.status = Some(status.to_string());
        guard.error = error;
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
    }

    pub async fn record_api_call(&self, rate_limited: bool) {
        let mut guard = self.inner.lock().await;
        guard.api_calls += 1;
        if rate_limited {
            guard.rate_limit_hits += 1;
        }
    }

    pub async fn snapshot(&self) -> RunStatsSnapshot {
        let guard = self.inner.lock().await;
        let tables = guard
            .tables
            .iter()
            .map(|(name, stats)| TableStatsSnapshot {
                run_id: guard.run_id.clone(),
                connection_id: guard.connection_id.clone(),
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
            run_id: guard.run_id.clone(),
            connection_id: guard.connection_id.clone(),
            started_at: guard.started_at,
            finished_at: guard.finished_at,
            status: guard.status.clone(),
            error: guard.error.clone(),
            rows_read: guard.rows_read,
            rows_written: guard.rows_written,
            rows_deleted: guard.rows_deleted,
            rows_upserted: guard.rows_upserted,
            extract_ms: guard.extract_ms,
            load_ms: guard.load_ms,
            api_calls: guard.api_calls,
            rate_limit_hits: guard.rate_limit_hits,
            tables,
        }
    }
}

pub struct StatsDb {
    pool: SqlitePool,
}

impl StatsDb {
    pub async fn new(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let db = Self { pool };
        db.init().await?;
        Ok(db)
    }

    async fn init(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            create table if not exists runs (
                run_id text primary key,
                connection_id text not null,
                started_at text not null,
                finished_at text,
                status text,
                error text,
                rows_read integer not null,
                rows_written integer not null,
                rows_deleted integer not null,
                rows_upserted integer not null,
                extract_ms integer not null,
                load_ms integer not null,
                api_calls integer not null,
                rate_limit_hits integer not null
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            create table if not exists run_tables (
                id integer primary key autoincrement,
                run_id text not null,
                connection_id text not null,
                table_name text not null,
                rows_read integer not null,
                rows_written integer not null,
                rows_deleted integer not null,
                rows_upserted integer not null,
                extract_ms integer not null,
                load_ms integer not null
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query("create index if not exists idx_runs_connection on runs(connection_id);")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "create index if not exists idx_run_tables_connection on run_tables(connection_id);",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("create index if not exists idx_run_tables_table on run_tables(table_name);")
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn persist_run(&self, handle: &StatsHandle) -> anyhow::Result<()> {
        let snapshot = handle.snapshot().await;
        sqlx::query("delete from run_tables where run_id = ?")
            .bind(&snapshot.run_id)
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            insert or replace into runs (
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
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
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
            sqlx::query(
                r#"
                insert into run_tables (
                    run_id,
                    connection_id,
                    table_name,
                    rows_read,
                    rows_written,
                    rows_deleted,
                    rows_upserted,
                    extract_ms,
                    load_ms
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
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
            sqlx::query(
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
                from runs
                where connection_id = ?
                order by started_at desc
                limit ?
                "#,
            )
            .bind(connection_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(
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
                from runs
                order by started_at desc
                limit ?
                "#,
            )
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
}

fn parse_rfc3339(value: String) -> anyhow::Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&value)?.with_timezone(&Utc))
}
