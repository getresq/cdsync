use crate::types::TableCheckpoint;
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

const LOCK_TTL_SECONDS: u64 = 60;
const LOCK_HEARTBEAT_SECONDS: u64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncState {
    #[serde(default)]
    pub connections: HashMap<String, ConnectionState>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PostgresCdcState {
    pub last_lsn: Option<String>,
    pub slot_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConnectionState {
    #[serde(default)]
    pub postgres: HashMap<String, TableCheckpoint>,
    pub postgres_cdc: Option<PostgresCdcState>,
    #[serde(default)]
    pub salesforce: HashMap<String, TableCheckpoint>,
    pub last_sync_started_at: Option<DateTime<Utc>>,
    pub last_sync_finished_at: Option<DateTime<Utc>>,
    pub last_sync_status: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct SyncStateStore {
    pool: SqlitePool,
    lock_ttl: Duration,
}

#[derive(Clone)]
pub struct StateHandle {
    store: SyncStateStore,
    connection_id: String,
}

pub struct ConnectionLease {
    store: SyncStateStore,
    connection_id: String,
    owner_id: String,
    stop_tx: Option<oneshot::Sender<()>>,
    heartbeat_task: Option<JoinHandle<()>>,
}

impl SyncState {
    pub async fn load(path: &Path) -> anyhow::Result<Self> {
        let store = SyncStateStore::open(path).await?;
        store.load_state().await
    }
}

impl SyncStateStore {
    pub async fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let store = Self {
            pool,
            lock_ttl: Duration::from_secs(LOCK_TTL_SECONDS),
        };
        store.init().await?;
        Ok(store)
    }

    pub fn handle(&self, connection_id: &str) -> StateHandle {
        StateHandle {
            store: self.clone(),
            connection_id: connection_id.to_string(),
        }
    }

    pub async fn load_state(&self) -> anyhow::Result<SyncState> {
        let rows = sqlx::query(
            r#"
            select
                connection_id,
                last_sync_started_at,
                last_sync_finished_at,
                last_sync_status,
                last_error,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            from connection_state
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let checkpoint_rows = sqlx::query(
            r#"
            select connection_id, source_kind, entity_name, checkpoint_json
            from table_checkpoints
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut connections = HashMap::new();
        let mut latest_updated_at = None;

        for row in rows {
            let connection_id: String = row.try_get("connection_id")?;
            let updated_at_ms: i64 = row.try_get("updated_at")?;
            let connection_state = ConnectionState {
                postgres: HashMap::new(),
                postgres_cdc: load_cdc_state_from_row(&row)?,
                salesforce: HashMap::new(),
                last_sync_started_at: parse_optional_rfc3339(row.try_get("last_sync_started_at")?),
                last_sync_finished_at: parse_optional_rfc3339(
                    row.try_get("last_sync_finished_at")?,
                ),
                last_sync_status: row.try_get("last_sync_status")?,
                last_error: row.try_get("last_error")?,
            };
            latest_updated_at = max_updated_at(latest_updated_at, updated_at_ms);
            connections.insert(connection_id, connection_state);
        }

        for row in checkpoint_rows {
            let connection_id: String = row.try_get("connection_id")?;
            let source_kind: String = row.try_get("source_kind")?;
            let entity_name: String = row.try_get("entity_name")?;
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            let checkpoint: TableCheckpoint =
                serde_json::from_str(&checkpoint_json).with_context(|| {
                    format!("parsing checkpoint for {}:{}", connection_id, entity_name)
                })?;
            let connection = connections.entry(connection_id).or_default();
            match source_kind.as_str() {
                "postgres" => {
                    connection.postgres.insert(entity_name, checkpoint);
                }
                "salesforce" => {
                    connection.salesforce.insert(entity_name, checkpoint);
                }
                _ => {}
            }
        }

        Ok(SyncState {
            connections,
            updated_at: latest_updated_at.and_then(datetime_from_millis),
        })
    }

    pub async fn save_connection_state(
        &self,
        connection_id: &str,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.save_connection_meta(connection_id, connection_state)
            .await?;
        for (table_name, checkpoint) in &connection_state.postgres {
            self.save_table_checkpoint(connection_id, "postgres", table_name, checkpoint)
                .await?;
        }
        for (object_name, checkpoint) in &connection_state.salesforce {
            self.save_table_checkpoint(connection_id, "salesforce", object_name, checkpoint)
                .await?;
        }
        Ok(())
    }

    pub async fn save_connection_meta(
        &self,
        connection_id: &str,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        let cdc_state = connection_state.postgres_cdc.as_ref();
        sqlx::query(
            r#"
            insert into connection_state (
                connection_id,
                last_sync_started_at,
                last_sync_finished_at,
                last_sync_status,
                last_error,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
            on conflict(connection_id) do update set
                last_sync_started_at = excluded.last_sync_started_at,
                last_sync_finished_at = excluded.last_sync_finished_at,
                last_sync_status = excluded.last_sync_status,
                last_error = excluded.last_error,
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(connection_id)
        .bind(
            connection_state
                .last_sync_started_at
                .map(|dt| dt.to_rfc3339()),
        )
        .bind(
            connection_state
                .last_sync_finished_at
                .map(|dt| dt.to_rfc3339()),
        )
        .bind(connection_state.last_sync_status.clone())
        .bind(connection_state.last_error.clone())
        .bind(cdc_state.and_then(|state| state.last_lsn.clone()))
        .bind(cdc_state.and_then(|state| state.slot_name.clone()))
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, "connection_meta");
        Ok(())
    }

    pub async fn save_table_checkpoint(
        &self,
        connection_id: &str,
        source_kind: &str,
        entity_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        let checkpoint_json = serde_json::to_string(checkpoint)?;
        sqlx::query(
            r#"
            insert into table_checkpoints (
                connection_id,
                source_kind,
                entity_name,
                checkpoint_json,
                updated_at
            ) values (?, ?, ?, ?, ?)
            on conflict(connection_id, source_kind, entity_name) do update set
                checkpoint_json = excluded.checkpoint_json,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(connection_id)
        .bind(source_kind)
        .bind(entity_name)
        .bind(checkpoint_json)
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, source_kind);
        Ok(())
    }

    pub async fn save_postgres_cdc_state(
        &self,
        connection_id: &str,
        cdc_state: &PostgresCdcState,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        sqlx::query(
            r#"
            insert into connection_state (
                connection_id,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values (?, ?, ?, ?)
            on conflict(connection_id) do update set
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(connection_id)
        .bind(cdc_state.last_lsn.clone())
        .bind(cdc_state.slot_name.clone())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, "postgres_cdc");
        Ok(())
    }

    pub async fn acquire_connection_lock(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<ConnectionLease> {
        let owner_id = Uuid::new_v4().to_string();
        self.try_acquire_lock(connection_id, &owner_id).await?;

        let (stop_tx, mut stop_rx) = oneshot::channel();
        let store = self.clone();
        let connection = connection_id.to_string();
        let owner = owner_id.clone();
        let heartbeat_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(LOCK_HEARTBEAT_SECONDS)) => {
                        let _ = store.heartbeat_lock(&connection, &owner).await;
                    }
                    _ = &mut stop_rx => {
                        break;
                    }
                }
            }
        });

        Ok(ConnectionLease {
            store: self.clone(),
            connection_id: connection_id.to_string(),
            owner_id,
            stop_tx: Some(stop_tx),
            heartbeat_task: Some(heartbeat_task),
        })
    }

    async fn init(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            create table if not exists connection_state (
                connection_id text primary key,
                last_sync_started_at text,
                last_sync_finished_at text,
                last_sync_status text,
                last_error text,
                postgres_cdc_last_lsn text,
                postgres_cdc_slot_name text,
                updated_at integer not null
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            create table if not exists table_checkpoints (
                connection_id text not null,
                source_kind text not null,
                entity_name text not null,
                checkpoint_json text not null,
                updated_at integer not null,
                primary key (connection_id, source_kind, entity_name)
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            create table if not exists connection_locks (
                connection_id text primary key,
                owner_id text not null,
                acquired_at integer not null,
                heartbeat_at integer not null
            );
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn try_acquire_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        sqlx::query("begin immediate")
            .execute(&mut *conn)
            .await
            .context("acquiring sqlite write lock")?;

        let outcome = async {
            let existing = sqlx::query(
                r#"
                select owner_id, heartbeat_at
                from connection_locks
                where connection_id = ?
                "#,
            )
            .bind(connection_id)
            .fetch_optional(&mut *conn)
            .await?;

            let now = now_millis();
            let lock_ttl_ms = self.lock_ttl.as_millis() as i64;
            if let Some(row) = existing {
                let current_owner: String = row.try_get("owner_id")?;
                let heartbeat_at: i64 = row.try_get("heartbeat_at")?;
                let is_stale = now - heartbeat_at > lock_ttl_ms;
                if !is_stale && current_owner != owner_id {
                    anyhow::bail!("connection {} is already locked", connection_id);
                }
            }

            sqlx::query(
                r#"
                insert into connection_locks (
                    connection_id,
                    owner_id,
                    acquired_at,
                    heartbeat_at
                ) values (?, ?, ?, ?)
                on conflict(connection_id) do update set
                    owner_id = excluded.owner_id,
                    acquired_at = excluded.acquired_at,
                    heartbeat_at = excluded.heartbeat_at
                "#,
            )
            .bind(connection_id)
            .bind(owner_id)
            .bind(now)
            .bind(now)
            .execute(&mut *conn)
            .await?;

            anyhow::Ok(())
        }
        .await;

        match outcome {
            Ok(()) => {
                sqlx::query("commit").execute(&mut *conn).await?;
                Ok(())
            }
            Err(err) => {
                let _ = sqlx::query("rollback").execute(&mut *conn).await;
                Err(err)
            }
        }
    }

    async fn heartbeat_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            update connection_locks
            set heartbeat_at = ?
            where connection_id = ? and owner_id = ?
            "#,
        )
        .bind(now_millis())
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            delete from connection_locks
            where connection_id = ? and owner_id = ?
            "#,
        )
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl StateHandle {
    pub async fn save_connection_state(
        &self,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.store
            .save_connection_state(&self.connection_id, connection_state)
            .await
    }

    pub async fn save_connection_meta(
        &self,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        self.store
            .save_connection_meta(&self.connection_id, connection_state)
            .await
    }

    pub async fn save_postgres_checkpoint(
        &self,
        table_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        self.store
            .save_table_checkpoint(&self.connection_id, "postgres", table_name, checkpoint)
            .await
    }

    pub async fn save_salesforce_checkpoint(
        &self,
        object_name: &str,
        checkpoint: &TableCheckpoint,
    ) -> anyhow::Result<()> {
        self.store
            .save_table_checkpoint(&self.connection_id, "salesforce", object_name, checkpoint)
            .await
    }

    pub async fn save_postgres_cdc_state(
        &self,
        cdc_state: &PostgresCdcState,
    ) -> anyhow::Result<()> {
        self.store
            .save_postgres_cdc_state(&self.connection_id, cdc_state)
            .await
    }
}

impl ConnectionLease {
    pub async fn release(mut self) -> anyhow::Result<()> {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(task) = self.heartbeat_task.take() {
            let _ = task.await;
        }
        self.store
            .release_lock(&self.connection_id, &self.owner_id)
            .await
    }
}

fn load_cdc_state_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> anyhow::Result<Option<PostgresCdcState>> {
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

fn parse_optional_rfc3339(value: Option<String>) -> Option<DateTime<Utc>> {
    value
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

fn datetime_from_millis(value: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(value)
}

fn max_updated_at(current: Option<i64>, next: i64) -> Option<i64> {
    match current {
        Some(current) => Some(current.max(next)),
        None => Some(next),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn state_store_round_trips_connection_state() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.db");
        let store = SyncStateStore::open(&path).await?;
        let handle = store.handle("app");

        let mut state = ConnectionState {
            last_sync_status: Some("running".to_string()),
            ..Default::default()
        };
        state.postgres.insert(
            "public.accounts".to_string(),
            TableCheckpoint {
                last_primary_key: Some("42".to_string()),
                ..Default::default()
            },
        );
        state.postgres_cdc = Some(PostgresCdcState {
            last_lsn: Some("0/16B6C50".to_string()),
            slot_name: Some("slot".to_string()),
        });

        handle.save_connection_state(&state).await?;

        let loaded = store.load_state().await?;
        let connection = loaded
            .connections
            .get("app")
            .context("missing connection")?;
        assert_eq!(connection.last_sync_status.as_deref(), Some("running"));
        assert_eq!(
            connection
                .postgres
                .get("public.accounts")
                .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
            Some("42")
        );
        assert_eq!(
            connection
                .postgres_cdc
                .as_ref()
                .and_then(|cdc| cdc.last_lsn.as_deref()),
            Some("0/16B6C50")
        );
        Ok(())
    }

    #[tokio::test]
    async fn connection_locks_block_second_owner() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.db");
        let store = SyncStateStore::open(&path).await?;

        let lease = store.acquire_connection_lock("app").await?;
        let second = store.acquire_connection_lock("app").await;
        assert!(second.is_err());
        lease.release().await?;
        Ok(())
    }
}
