use super::*;
use crate::retry::ErrorReasonCode;
use crate::types::{TableRuntimeState, TableRuntimeStatus};

fn test_state_config() -> Option<StateConfig> {
    let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
    Some(StateConfig {
        url,
        schema: Some(format!("cdsync_state_test_{}", Uuid::new_v4().simple())),
    })
}

#[test]
fn cdc_ledger_stage_defaults_from_legacy_statuses() {
    assert_eq!(
        CdcLedgerStage::from_job_status(CdcBatchLoadJobStatus::Pending),
        CdcLedgerStage::Received
    );
    assert_eq!(
        CdcLedgerStage::from_job_status(CdcBatchLoadJobStatus::Running),
        CdcLedgerStage::Applying
    );
    assert_eq!(
        CdcLedgerStage::from_job_status(CdcBatchLoadJobStatus::Succeeded),
        CdcLedgerStage::Applied
    );
    assert_eq!(
        CdcLedgerStage::from_fragment_status(CdcCommitFragmentStatus::Succeeded),
        CdcLedgerStage::Applied
    );
}

#[test]
fn cdc_ledger_stage_normalization_preserves_explicit_pipeline_stages() {
    assert_eq!(
        CdcLedgerStage::normalize_for_job_status(
            CdcLedgerStage::Staged,
            CdcBatchLoadJobStatus::Pending,
        ),
        CdcLedgerStage::Staged
    );
    assert_eq!(
        CdcLedgerStage::normalize_for_job_status(
            CdcLedgerStage::Received,
            CdcBatchLoadJobStatus::Running,
        ),
        CdcLedgerStage::Applying
    );
    assert_eq!(
        CdcLedgerStage::normalize_for_fragment_status(
            CdcLedgerStage::Loaded,
            CdcCommitFragmentStatus::Pending,
        ),
        CdcLedgerStage::Loaded
    );
    assert_eq!(
        CdcLedgerStage::normalize_for_fragment_status(
            CdcLedgerStage::Received,
            CdcCommitFragmentStatus::Failed,
        ),
        CdcLedgerStage::Failed
    );
}

#[tokio::test]
async fn state_store_round_trips_connection_state() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");

    let mut state = ConnectionState {
        last_sync_status: Some("running".to_string()),
        last_error_reason: Some(ErrorReasonCode::LastRunFailed),
        ..Default::default()
    };
    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            last_primary_key: Some("42".to_string()),
            runtime: Some(TableRuntimeState {
                status: TableRuntimeStatus::Retrying,
                attempts: 3,
                reason: Some(ErrorReasonCode::BigqueryDmlQuota),
                last_error: Some("quota exceeded".to_string()),
                next_retry_at: Some(Utc::now()),
                updated_at: Some(Utc::now()),
            }),
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
        connection.last_error_reason,
        Some(ErrorReasonCode::LastRunFailed)
    );
    assert_eq!(
        connection
            .postgres
            .get("public.accounts")
            .and_then(|checkpoint| checkpoint.last_primary_key.as_deref()),
        Some("42")
    );
    assert!(matches!(
        connection
            .postgres
            .get("public.accounts")
            .and_then(|checkpoint| checkpoint.runtime.as_ref())
            .map(|runtime| (&runtime.status, runtime.attempts)),
        Some((TableRuntimeStatus::Retrying, 3))
    ));
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
async fn state_store_load_connection_state_reads_single_connection_meta() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");

    let mut state = ConnectionState {
        last_sync_status: Some("running".to_string()),
        last_error_reason: Some(ErrorReasonCode::LastRunFailed),
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

    let loaded = store
        .load_connection_state("app")
        .await?
        .context("missing connection")?;
    assert_eq!(loaded.last_sync_status.as_deref(), Some("running"));
    assert_eq!(
        loaded.last_error_reason,
        Some(ErrorReasonCode::LastRunFailed)
    );
    assert!(loaded.postgres.is_empty());
    assert_eq!(
        loaded
            .postgres_cdc
            .as_ref()
            .and_then(|cdc| cdc.last_lsn.as_deref()),
        Some("0/16B6C50")
    );
    Ok(())
}

#[tokio::test]
async fn connection_locks_block_second_owner() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;

    let lease = store.acquire_connection_lock("app").await?;
    let second = store.acquire_connection_lock("app").await;
    assert!(second.is_err());
    lease.release().await?;
    Ok(())
}

#[tokio::test]
async fn dropping_connection_lease_releases_lock() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;

    {
        let _lease = store.acquire_connection_lock("app").await?;
    }

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match store.acquire_connection_lock("app").await {
                Ok(lease) => {
                    lease.release().await?;
                    return Ok::<(), anyhow::Error>(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    })
    .await
    .context("lock was not released after dropping lease")??;

    Ok(())
}
