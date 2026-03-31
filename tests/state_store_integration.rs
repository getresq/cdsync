use cdsync::config::StateConfig;
use cdsync::state::{ConnectionState, PostgresCdcState, SyncStateStore};
use cdsync::types::{DataType, SchemaFieldSnapshot, SnapshotChunkCheckpoint, TableCheckpoint};
use uuid::Uuid;

fn test_state_config() -> Option<StateConfig> {
    let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
    Some(StateConfig {
        url,
        schema: Some(format!("cdsync_state_it_{}", Uuid::new_v4().simple())),
    })
}

#[tokio::test]
async fn postgres_state_store_persists_public_api_state() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");

    let mut connection = ConnectionState {
        last_sync_status: Some("success".to_string()),
        ..Default::default()
    };
    connection.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            last_primary_key: Some("99".to_string()),
            ..Default::default()
        },
    );
    connection.postgres_cdc = Some(PostgresCdcState {
        last_lsn: Some("0/16B6C50".to_string()),
        slot_name: Some("slot".to_string()),
    });

    handle.save_connection_state(&connection).await?;

    let loaded = store.load_state().await?;
    let app = loaded
        .connections
        .get("app")
        .expect("connection should be persisted");
    assert_eq!(app.last_sync_status.as_deref(), Some("success"));
    assert_eq!(
        app.postgres
            .get("public.accounts")
            .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
        Some("99")
    );
    assert_eq!(
        app.postgres_cdc
            .as_ref()
            .and_then(|cdc| cdc.slot_name.as_deref()),
        Some("slot")
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_releases_lock_for_next_owner() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;

    let lease = store.acquire_connection_lock("app").await?;
    assert!(store.acquire_connection_lock("app").await.is_err());
    lease.release().await?;
    assert!(store.acquire_connection_lock("app").await.is_ok());

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_round_trips_snapshot_resume_checkpoint_metadata() -> anyhow::Result<()>
{
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");

    let mut connection = ConnectionState::default();
    connection.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            schema_hash: Some("hash-1".to_string()),
            schema_snapshot: Some(vec![
                SchemaFieldSnapshot {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                SchemaFieldSnapshot {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ]),
            schema_primary_key: Some("id".to_string()),
            snapshot_start_lsn: Some("0/ABC".to_string()),
            snapshot_chunks: vec![
                SnapshotChunkCheckpoint {
                    start_primary_key: Some("1".to_string()),
                    end_primary_key: Some("10".to_string()),
                    last_primary_key: Some("6".to_string()),
                    complete: false,
                },
                SnapshotChunkCheckpoint {
                    start_primary_key: Some("11".to_string()),
                    end_primary_key: Some("20".to_string()),
                    last_primary_key: Some("20".to_string()),
                    complete: true,
                },
            ],
            ..Default::default()
        },
    );

    handle.save_connection_state(&connection).await?;

    let loaded = store.load_state().await?;
    let checkpoint = loaded
        .connections
        .get("app")
        .and_then(|connection| connection.postgres.get("public.accounts"))
        .expect("checkpoint should be persisted");

    assert_eq!(checkpoint.schema_hash.as_deref(), Some("hash-1"));
    assert_eq!(checkpoint.schema_primary_key.as_deref(), Some("id"));
    assert_eq!(checkpoint.snapshot_start_lsn.as_deref(), Some("0/ABC"));
    assert_eq!(checkpoint.snapshot_chunks.len(), 2);
    assert_eq!(
        checkpoint.snapshot_chunks[0].last_primary_key.as_deref(),
        Some("6")
    );
    assert!(!checkpoint.snapshot_chunks[0].complete);
    assert_eq!(
        checkpoint.snapshot_chunks[1].end_primary_key.as_deref(),
        Some("20")
    );
    assert!(checkpoint.snapshot_chunks[1].complete);

    let schema_snapshot = checkpoint
        .schema_snapshot
        .as_ref()
        .expect("schema snapshot should be persisted");
    assert_eq!(schema_snapshot.len(), 2);
    assert_eq!(schema_snapshot[0].name, "id");
    assert_eq!(schema_snapshot[0].data_type, DataType::Int64);
    assert!(!schema_snapshot[0].nullable);

    Ok(())
}
