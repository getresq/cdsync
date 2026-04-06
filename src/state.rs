mod cdc;
mod handle;
mod helpers;
mod models;
#[cfg(test)]
mod tests;

use crate::config::StateConfig;
use crate::types::TableCheckpoint;
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::Row;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPoolOptions, PgRow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

use self::helpers::*;
pub use self::models::*;

const LOCK_TTL_SECONDS: u64 = 60;
const LOCK_HEARTBEAT_SECONDS: u64 = 15;
static STATE_MIGRATOR: Migrator = sqlx::migrate!("./migrations/state");

impl SyncState {
    pub async fn load_with_config(config: &StateConfig) -> anyhow::Result<Self> {
        let store = SyncStateStore::open_with_config(config).await?;
        store.load_state().await
    }
}

impl SyncStateStore {
    pub async fn migrate_with_config(config: &StateConfig) -> anyhow::Result<()> {
        validate_schema_name(config.schema_name())?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&config.url)
            .await?;
        let store = Self {
            pool,
            schema: config.schema_name().to_string(),
            lock_ttl: Duration::from_secs(LOCK_TTL_SECONDS),
        };
        store.migrate().await
    }

    pub async fn open_with_config(config: &StateConfig) -> anyhow::Result<Self> {
        Self::open(&config.url, config.schema_name()).await
    }

    pub async fn open(url: &str, schema: &str) -> anyhow::Result<Self> {
        validate_schema_name(schema)?;
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        let store = Self {
            pool,
            schema: schema.to_string(),
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
        let rows = sqlx::query(&format!(
            "select connection_id, last_sync_started_at, last_sync_finished_at, last_sync_status, last_error, postgres_cdc_last_lsn, postgres_cdc_slot_name, updated_at from {}",
            self.table("connection_state")
        ))
        .fetch_all(&self.pool)
        .await?;

        let checkpoint_rows = sqlx::query(&format!(
            "select connection_id, source_kind, entity_name, checkpoint_json from {}",
            self.table("table_checkpoints")
        ))
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
                last_sync_started_at: parse_optional_rfc3339(row.try_get("last_sync_started_at")?),
                last_sync_finished_at: parse_optional_rfc3339(
                    row.try_get("last_sync_finished_at")?,
                ),
                last_sync_status: row.try_get("last_sync_status")?,
                last_error: row.try_get("last_error")?,
            };
            latest_updated_at = Some(max_updated_at(latest_updated_at, updated_at_ms));
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
            if source_kind.as_str() == "postgres" {
                connection.postgres.insert(entity_name, checkpoint);
            }
        }

        Ok(SyncState {
            connections,
            updated_at: latest_updated_at.and_then(datetime_from_millis),
        })
    }

    pub async fn load_connection_state(
        &self,
        connection_id: &str,
    ) -> anyhow::Result<Option<ConnectionState>> {
        let row = sqlx::query(&format!(
            "select last_sync_started_at, last_sync_finished_at, last_sync_status, last_error, postgres_cdc_last_lsn, postgres_cdc_slot_name from {} where connection_id = $1",
            self.table("connection_state")
        ))
        .bind(connection_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(ConnectionState {
            postgres: HashMap::new(),
            postgres_cdc: load_cdc_state_from_row(&row)?,
            last_sync_started_at: parse_optional_rfc3339(row.try_get("last_sync_started_at")?),
            last_sync_finished_at: parse_optional_rfc3339(row.try_get("last_sync_finished_at")?),
            last_sync_status: row.try_get("last_sync_status")?,
            last_error: row.try_get("last_error")?,
        }))
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let _: i32 = sqlx::query_scalar("select 1").fetch_one(&self.pool).await?;
        Ok(())
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
        Ok(())
    }

    pub async fn save_connection_meta(
        &self,
        connection_id: &str,
        connection_state: &ConnectionState,
    ) -> anyhow::Result<()> {
        let updated_at = now_millis();
        let cdc_state = connection_state.postgres_cdc.as_ref();
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                last_sync_started_at,
                last_sync_finished_at,
                last_sync_status,
                last_error,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values ($1, $2, $3, $4, $5, $6, $7, $8)
            on conflict(connection_id) do update set
                last_sync_started_at = excluded.last_sync_started_at,
                last_sync_finished_at = excluded.last_sync_finished_at,
                last_sync_status = excluded.last_sync_status,
                last_error = excluded.last_error,
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
            self.table("connection_state")
        ))
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
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                source_kind,
                entity_name,
                checkpoint_json,
                updated_at
            ) values ($1, $2, $3, $4, $5)
            on conflict(connection_id, source_kind, entity_name) do update set
                checkpoint_json = excluded.checkpoint_json,
                updated_at = excluded.updated_at
            "#,
            self.table("table_checkpoints")
        ))
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
        sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                postgres_cdc_last_lsn,
                postgres_cdc_slot_name,
                updated_at
            ) values ($1, $2, $3, $4)
            on conflict(connection_id) do update set
                postgres_cdc_last_lsn = excluded.postgres_cdc_last_lsn,
                postgres_cdc_slot_name = excluded.postgres_cdc_slot_name,
                updated_at = excluded.updated_at
            "#,
            self.table("connection_state")
        ))
        .bind(connection_id)
        .bind(cdc_state.last_lsn.clone())
        .bind(cdc_state.slot_name.clone())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        crate::telemetry::record_checkpoint_save(connection_id, "postgres_cdc");
        Ok(())
    }

    pub async fn load_table_checkpoint(
        &self,
        connection_id: &str,
        source_kind: &str,
        entity_name: &str,
    ) -> anyhow::Result<Option<TableCheckpoint>> {
        let row = sqlx::query(&format!(
            "select checkpoint_json from {} where connection_id = $1 and source_kind = $2 and entity_name = $3",
            self.table("table_checkpoints")
        ))
        .bind(connection_id)
        .bind(source_kind)
        .bind(entity_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            serde_json::from_str(&checkpoint_json).with_context(|| {
                format!(
                    "parsing checkpoint for {}:{}:{}",
                    connection_id, source_kind, entity_name
                )
            })
        })
        .transpose()
    }

    pub async fn load_all_table_checkpoints(
        &self,
        connection_id: &str,
        source_kind: &str,
    ) -> anyhow::Result<HashMap<String, TableCheckpoint>> {
        let rows = sqlx::query(&format!(
            r#"
            select entity_name, checkpoint_json
            from {}
            where connection_id = $1 and source_kind = $2
            "#,
            self.table("table_checkpoints")
        ))
        .bind(connection_id)
        .bind(source_kind)
        .fetch_all(&self.pool)
        .await?;

        let mut checkpoints = HashMap::with_capacity(rows.len());
        for row in rows {
            let entity_name: String = row.try_get("entity_name")?;
            let checkpoint_json: String = row.try_get("checkpoint_json")?;
            let checkpoint: TableCheckpoint =
                serde_json::from_str(&checkpoint_json).with_context(|| {
                    format!(
                        "parsing checkpoint for {}:{}:{}",
                        connection_id, source_kind, entity_name
                    )
                })?;
            checkpoints.insert(entity_name, checkpoint);
        }
        Ok(checkpoints)
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
                    () = tokio::time::sleep(Duration::from_secs(LOCK_HEARTBEAT_SECONDS)) => {
                        let _ = store.heartbeat_lock(&connection, &owner).await;
                    }
                    _ = &mut stop_rx => {
                        break;
                    }
                }
            }
        });

        Ok(ConnectionLease {
            cleanup: Some(Arc::new(LeaseCleanup {
                store: self.clone(),
                connection_id: connection_id.to_string(),
                owner_id,
            })),
            stop_tx: Some(stop_tx),
            heartbeat_task: Some(heartbeat_task),
        })
    }

    async fn init(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        ensure_schema_exists(&mut conn, &self.schema).await?;
        ensure_table_exists(&mut conn, &self.schema, "_sqlx_migrations").await?;
        ensure_table_exists(&mut conn, &self.schema, "connection_state").await?;
        ensure_table_exists(&mut conn, &self.schema, "table_checkpoints").await?;
        ensure_table_exists(&mut conn, &self.schema, "connection_locks").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_batch_load_jobs").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_commit_fragments").await?;
        ensure_table_exists(&mut conn, &self.schema, "cdc_watermark_state").await?;
        ensure_table_exists(&mut conn, &self.schema, "postgres_table_resync_requests").await?;
        Ok(())
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        create_schema_if_missing(&mut conn, &self.schema).await?;
        sqlx::query(&format!("set search_path to {}", quote_ident(&self.schema)))
            .execute(&mut *conn)
            .await?;
        STATE_MIGRATOR.run_direct(&mut *conn).await?;
        Ok(())
    }

    async fn try_acquire_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        let now = now_millis();
        let stale_before = now - saturating_u128_to_i64(self.lock_ttl.as_millis());
        let row = sqlx::query(&format!(
            r#"
            insert into {} (
                connection_id,
                owner_id,
                acquired_at,
                heartbeat_at
            ) values ($1, $2, $3, $3)
            on conflict(connection_id) do update set
                owner_id = excluded.owner_id,
                acquired_at = excluded.acquired_at,
                heartbeat_at = excluded.heartbeat_at
            where {}.owner_id = excluded.owner_id
               or {}.heartbeat_at < $4
            returning owner_id
            "#,
            self.table("connection_locks"),
            self.table("connection_locks"),
            self.table("connection_locks"),
        ))
        .bind(connection_id)
        .bind(owner_id)
        .bind(now)
        .bind(stale_before)
        .fetch_optional(&self.pool)
        .await?;

        if row.is_none() {
            anyhow::bail!("connection {} is already locked", connection_id);
        }
        Ok(())
    }

    async fn heartbeat_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "update {} set heartbeat_at = $1 where connection_id = $2 and owner_id = $3",
            self.table("connection_locks")
        ))
        .bind(now_millis())
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release_lock(&self, connection_id: &str, owner_id: &str) -> anyhow::Result<()> {
        sqlx::query(&format!(
            "delete from {} where connection_id = $1 and owner_id = $2",
            self.table("connection_locks")
        ))
        .bind(connection_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn table(&self, table_name: &str) -> String {
        format!("{}.{}", quote_ident(&self.schema), quote_ident(table_name))
    }
}

impl StateHandle {
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }
}
