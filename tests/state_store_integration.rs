use cdsync::state::{ConnectionState, PostgresCdcState, SyncStateStore};
use cdsync::types::TableCheckpoint;

#[tokio::test]
async fn sqlite_state_store_persists_public_api_state() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("state.db");
    let store = SyncStateStore::open(&path).await?;
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
async fn sqlite_state_store_releases_lock_for_next_owner() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path().join("state.db");
    let store = SyncStateStore::open(&path).await?;

    let lease = store.acquire_connection_lock("app").await?;
    assert!(store.acquire_connection_lock("app").await.is_err());
    lease.release().await?;
    assert!(store.acquire_connection_lock("app").await.is_ok());

    Ok(())
}
