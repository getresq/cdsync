use cdsync::config::StateConfig;
use cdsync::retry::SyncRetryClass;
use cdsync::state::{
    CdcBatchLoadJobRecord, CdcBatchLoadJobStatus, CdcCommitFragmentRecord, CdcCommitFragmentStatus,
    CdcLedgerStage, CdcWatermarkState, ConnectionState, PostgresCdcState, SyncStateStore,
};
use cdsync::types::{DataType, SchemaFieldSnapshot, SnapshotChunkCheckpoint, TableCheckpoint};
use chrono::Utc;
use uuid::Uuid;

fn test_state_config() -> Option<StateConfig> {
    let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
    Some(StateConfig {
        url,
        schema: Some(format!("cdsync_state_it_{}", Uuid::new_v4().simple())),
    })
}

fn reducer_payload_json(job_id: &str, table: &str) -> String {
    serde_json::json!({
        "job_id": job_id,
        "source_table": format!("public.{table}"),
        "target_table": table,
        "schema": {
            "name": table,
            "columns": [],
            "primary_key": "id"
        },
        "staging_schema": {
            "name": table,
            "columns": [],
            "primary_key": "id"
        },
        "primary_key": "id",
        "truncate": false,
        "steps": []
    })
    .to_string()
}

#[tokio::test]
async fn postgres_state_store_persists_public_api_state() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
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
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;

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
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
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

#[tokio::test]
async fn postgres_state_store_persists_cdc_fragments_and_watermark_state() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");

    handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                job_id: "job-1".to_string(),
                table_key: "public__items".to_string(),
                first_sequence: 41,
                status: CdcBatchLoadJobStatus::Pending,
                payload_json: "{}".to_string(),
                attempt_count: 0,
                retry_class: None,
                last_error: None,
                created_at: 1000,
                updated_at: 1000,
                ..Default::default()
            },
            &[
                CdcCommitFragmentRecord {
                    fragment_id: "fragment-1".to_string(),
                    job_id: "job-1".to_string(),
                    sequence: 41,
                    commit_lsn: "0/AAA".to_string(),
                    table_key: "public__items".to_string(),
                    status: CdcCommitFragmentStatus::Pending,
                    row_count: 10,
                    upserted_count: 8,
                    deleted_count: 2,
                    last_error: None,
                    created_at: 1000,
                    updated_at: 1000,
                    ..Default::default()
                },
                CdcCommitFragmentRecord {
                    fragment_id: "fragment-2".to_string(),
                    job_id: "job-1".to_string(),
                    sequence: 42,
                    commit_lsn: "0/AAB".to_string(),
                    table_key: "public__items".to_string(),
                    status: CdcCommitFragmentStatus::Pending,
                    row_count: 0,
                    upserted_count: 0,
                    deleted_count: 0,
                    last_error: None,
                    created_at: 1001,
                    updated_at: 1001,
                    ..Default::default()
                },
            ],
        )
        .await?;
    handle.mark_cdc_batch_load_bundle_succeeded("job-1").await?;

    handle
        .save_cdc_feedback_state(&CdcWatermarkState {
            next_sequence_to_ack: 41,
            last_enqueued_sequence: Some(42),
            last_received_lsn: Some("0/AAB".to_string()),
            last_flushed_lsn: Some("0/AAA".to_string()),
            last_persisted_lsn: Some("0/AAA".to_string()),
            last_relevant_change_seen_at: None,
            last_status_update_sent_at: None,
            last_keepalive_reply_at: None,
            last_slot_feedback_lsn: None,
            updated_at: None,
        })
        .await?;

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Succeeded])
        .await?;
    assert_eq!(fragments.len(), 2);
    assert!(fragments.iter().all(|fragment| fragment.job_id == "job-1"));
    assert_eq!(fragments[0].sequence, 41);
    assert_eq!(fragments[1].sequence, 42);
    assert!(
        fragments
            .iter()
            .all(|fragment| fragment.stage == CdcLedgerStage::Applied)
    );

    let watermark = store
        .load_cdc_watermark_state("app")
        .await?
        .expect("watermark state should be persisted");
    assert_eq!(watermark.next_sequence_to_ack, 41);
    assert_eq!(watermark.last_enqueued_sequence, Some(42));
    assert_eq!(watermark.last_received_lsn.as_deref(), Some("0/AAB"));
    assert_eq!(watermark.last_flushed_lsn.as_deref(), Some("0/AAA"));

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_claims_oldest_loaded_apply_job_per_table() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for job in [
        CdcBatchLoadJobRecord {
            job_id: "job-a-10".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 10,
            status: CdcBatchLoadJobStatus::Pending,
            stage: CdcLedgerStage::Loaded,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now,
            updated_at: now,
            ..Default::default()
        },
        CdcBatchLoadJobRecord {
            job_id: "job-a-11".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 11,
            status: CdcBatchLoadJobStatus::Pending,
            stage: CdcLedgerStage::Loaded,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now + 1,
            updated_at: now + 1,
            ..Default::default()
        },
        CdcBatchLoadJobRecord {
            job_id: "job-b-20".to_string(),
            table_key: "table_b".to_string(),
            first_sequence: 20,
            status: CdcBatchLoadJobStatus::Pending,
            stage: CdcLedgerStage::Loaded,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now + 2,
            updated_at: now + 2,
            ..Default::default()
        },
    ] {
        handle.enqueue_cdc_batch_load_bundle(&job, &[]).await?;
    }

    let first = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 1)
        .await?
        .into_iter()
        .next()
        .expect("first claim");
    assert_eq!(first.job_id, "job-a-10");
    assert_eq!(first.status, CdcBatchLoadJobStatus::Running);
    assert_eq!(first.stage, CdcLedgerStage::Applying);

    let second = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 1)
        .await?
        .into_iter()
        .next()
        .expect("second claim");
    assert_eq!(second.job_id, "job-b-20");
    assert_eq!(second.status, CdcBatchLoadJobStatus::Running);
    assert_eq!(second.stage, CdcLedgerStage::Applying);

    let third = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 1)
        .await?;
    assert!(third.is_empty());

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_claims_loaded_apply_window_for_one_table() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (job_id, table_key, sequence) in [
        ("job-a-10", "table_a", 10),
        ("job-a-11", "table_a", 11),
        ("job-b-20", "table_b", 20),
    ] {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: table_key.to_string(),
                    first_sequence: sequence,
                    status: CdcBatchLoadJobStatus::Pending,
                    stage: CdcLedgerStage::Loaded,
                    payload_json: reducer_payload_json(job_id, table_key),
                    created_at: now + sequence as i64,
                    updated_at: now + sequence as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let window = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 10)
        .await?;
    assert_eq!(
        window
            .iter()
            .map(|job| job.job_id.as_str())
            .collect::<Vec<_>>(),
        vec!["job-a-10", "job-a-11"]
    );
    assert!(
        window
            .iter()
            .all(|job| job.status == CdcBatchLoadJobStatus::Running)
    );
    assert!(
        window
            .iter()
            .all(|job| job.stage == CdcLedgerStage::Applying)
    );

    let next = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 10)
        .await?;
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].job_id, "job-b-20");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_loaded_apply_window_stops_at_barrier() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (job_id, sequence, barrier_kind) in [
        ("job-a-10", 10, None),
        ("job-a-11", 11, Some("truncate".to_string())),
        ("job-a-12", 12, None),
    ] {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: "table_a".to_string(),
                    first_sequence: sequence,
                    status: CdcBatchLoadJobStatus::Pending,
                    stage: CdcLedgerStage::Loaded,
                    payload_json: reducer_payload_json(job_id, "table_a"),
                    barrier_kind,
                    created_at: now + sequence as i64,
                    updated_at: now + sequence as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let window = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 10)
        .await?;
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].job_id, "job-a-10");

    handle
        .mark_cdc_batch_load_window_succeeded(&[window[0].job_id.clone()])
        .await?;

    let barrier = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 10)
        .await?;
    assert_eq!(barrier.len(), 1);
    assert_eq!(barrier[0].job_id, "job-a-11");
    assert_eq!(barrier[0].barrier_kind.as_deref(), Some("truncate"));

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_loaded_apply_window_reclaims_stale_barrier_alone()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (job_id, sequence, status, barrier_kind) in [
        (
            "job-a-10",
            10,
            CdcBatchLoadJobStatus::Running,
            Some("truncate".to_string()),
        ),
        ("job-a-11", 11, CdcBatchLoadJobStatus::Pending, None),
    ] {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: "table_a".to_string(),
                    first_sequence: sequence,
                    status,
                    stage: if status == CdcBatchLoadJobStatus::Running {
                        CdcLedgerStage::Applying
                    } else {
                        CdcLedgerStage::Loaded
                    },
                    payload_json: reducer_payload_json(job_id, "table_a"),
                    barrier_kind,
                    created_at: now + sequence as i64,
                    updated_at: now + sequence as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let window = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now + 20, 10)
        .await?;
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].job_id, "job-a-10");
    assert_eq!(window[0].barrier_kind.as_deref(), Some("truncate"));

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_loaded_apply_window_does_not_cross_succeeded_barrier()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (job_id, sequence, status, stage, barrier_kind) in [
        (
            "job-a-10",
            10,
            CdcBatchLoadJobStatus::Pending,
            CdcLedgerStage::Loaded,
            None,
        ),
        (
            "job-a-11",
            11,
            CdcBatchLoadJobStatus::Succeeded,
            CdcLedgerStage::Applied,
            Some("truncate".to_string()),
        ),
        (
            "job-a-12",
            12,
            CdcBatchLoadJobStatus::Pending,
            CdcLedgerStage::Loaded,
            None,
        ),
    ] {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: "table_a".to_string(),
                    first_sequence: sequence,
                    status,
                    stage,
                    payload_json: reducer_payload_json(job_id, "table_a"),
                    barrier_kind,
                    created_at: now + sequence as i64,
                    updated_at: now + sequence as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let window = handle
        .claim_next_loaded_cdc_batch_load_job_window_for_apply(now - 1, 10)
        .await?;
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].job_id, "job-a-10");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_marks_batch_load_window_succeeded_atomically() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for sequence in [10_u64, 11] {
        let job_id = format!("job-window-{sequence}");
        let job = CdcBatchLoadJobRecord {
            job_id: job_id.clone(),
            table_key: "table_a".to_string(),
            first_sequence: sequence,
            status: CdcBatchLoadJobStatus::Pending,
            stage: CdcLedgerStage::Loaded,
            payload_json: reducer_payload_json(&job_id, "table_a"),
            created_at: now + sequence as i64,
            updated_at: now + sequence as i64,
            ..Default::default()
        };
        let fragment = CdcCommitFragmentRecord {
            fragment_id: format!("{job_id}:{sequence}"),
            job_id: job.job_id.clone(),
            sequence,
            commit_lsn: format!("0/{sequence:X}"),
            table_key: job.table_key.clone(),
            status: CdcCommitFragmentStatus::Pending,
            stage: CdcLedgerStage::Loaded,
            row_count: 1,
            upserted_count: 1,
            created_at: now + sequence as i64,
            updated_at: now + sequence as i64,
            ..Default::default()
        };
        handle
            .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
            .await?;
    }

    handle
        .mark_cdc_batch_load_window_succeeded(&[
            "job-window-10".to_string(),
            "job-window-11".to_string(),
        ])
        .await?;

    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Succeeded])
        .await?;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| job.stage == CdcLedgerStage::Applied));

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Succeeded])
        .await?;
    assert_eq!(fragments.len(), 2);
    assert!(
        fragments
            .iter()
            .all(|fragment| fragment.stage == CdcLedgerStage::Applied)
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_round_trips_manual_table_resync_requests() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;

    store
        .request_postgres_table_resync("app", "public.accounts")
        .await?;
    store
        .request_postgres_table_resync("app", "public.orders")
        .await?;

    let requests = store.load_postgres_table_resync_requests("app").await?;
    let requested_tables: Vec<&str> = requests
        .iter()
        .map(|request| request.source_table.as_str())
        .collect();
    assert_eq!(requested_tables, vec!["public.accounts", "public.orders"]);

    store
        .clear_postgres_table_resync_request("app", "public.accounts")
        .await?;
    let requests = store.load_postgres_table_resync_requests("app").await?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].source_table, "public.orders");
    Ok(())
}

#[tokio::test]
async fn postgres_state_store_discards_inflight_jobs_for_replay() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let cases = [
        (
            CdcBatchLoadJobRecord {
                job_id: "job-pending".to_string(),
                table_key: "table_a".to_string(),
                first_sequence: 10,
                status: CdcBatchLoadJobStatus::Pending,
                payload_json: "{}".to_string(),
                attempt_count: 0,
                retry_class: None,
                last_error: None,
                created_at: now,
                updated_at: now,
                ..Default::default()
            },
            CdcCommitFragmentStatus::Pending,
        ),
        (
            CdcBatchLoadJobRecord {
                job_id: "job-running".to_string(),
                table_key: "table_b".to_string(),
                first_sequence: 11,
                status: CdcBatchLoadJobStatus::Running,
                payload_json: "{}".to_string(),
                attempt_count: 1,
                retry_class: None,
                last_error: None,
                created_at: now + 1,
                updated_at: now + 1,
                ..Default::default()
            },
            CdcCommitFragmentStatus::Pending,
        ),
        (
            CdcBatchLoadJobRecord {
                job_id: "job-retryable-failed".to_string(),
                table_key: "table_c".to_string(),
                first_sequence: 12,
                status: CdcBatchLoadJobStatus::Failed,
                payload_json: "{}".to_string(),
                attempt_count: 2,
                retry_class: Some(SyncRetryClass::Transient),
                last_error: Some("retry me".to_string()),
                created_at: now + 2,
                updated_at: now + 2,
                ..Default::default()
            },
            CdcCommitFragmentStatus::Failed,
        ),
        (
            CdcBatchLoadJobRecord {
                job_id: "job-permanent-failed".to_string(),
                table_key: "table_d".to_string(),
                first_sequence: 13,
                status: CdcBatchLoadJobStatus::Failed,
                payload_json: "{}".to_string(),
                attempt_count: 2,
                retry_class: Some(SyncRetryClass::Permanent),
                last_error: Some("keep me".to_string()),
                created_at: now + 3,
                updated_at: now + 3,
                ..Default::default()
            },
            CdcCommitFragmentStatus::Failed,
        ),
        (
            CdcBatchLoadJobRecord {
                job_id: "job-succeeded".to_string(),
                table_key: "table_e".to_string(),
                first_sequence: 14,
                status: CdcBatchLoadJobStatus::Succeeded,
                payload_json: "{}".to_string(),
                attempt_count: 1,
                retry_class: None,
                last_error: None,
                created_at: now + 4,
                updated_at: now + 4,
                ..Default::default()
            },
            CdcCommitFragmentStatus::Succeeded,
        ),
    ];

    for (job, fragment_status) in cases {
        let fragment = CdcCommitFragmentRecord {
            fragment_id: format!("{}:{}", job.job_id, job.first_sequence),
            job_id: job.job_id.clone(),
            sequence: job.first_sequence,
            commit_lsn: format!("0/{:X}", job.first_sequence),
            table_key: job.table_key.clone(),
            status: fragment_status,
            row_count: 1,
            upserted_count: 1,
            deleted_count: 0,
            last_error: job.last_error.clone(),
            created_at: job.created_at,
            updated_at: job.updated_at,
            ..Default::default()
        };
        handle
            .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
            .await?;
    }

    let cleanup = handle
        .discard_inflight_cdc_batch_load_state_for_replay()
        .await?;
    assert_eq!(cleanup.discarded_jobs, 2);
    assert_eq!(cleanup.discarded_fragments, 2);
    assert_eq!(cleanup.repaired_terminal_fragments, 0);

    let pending = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
        .await?;
    let running = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Running])
        .await?;
    let failed = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Failed])
        .await?;
    let succeeded = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Succeeded])
        .await?;

    assert!(pending.is_empty());
    assert!(running.is_empty());
    let failed_job_ids: Vec<&str> = failed.iter().map(|job| job.job_id.as_str()).collect();
    assert_eq!(
        failed_job_ids,
        vec!["job-retryable-failed", "job-permanent-failed"]
    );
    assert_eq!(succeeded.len(), 1);
    assert_eq!(succeeded[0].job_id, "job-succeeded");

    let pending_fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
        .await?;
    let failed_fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Failed])
        .await?;
    let succeeded_fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Succeeded])
        .await?;

    assert!(pending_fragments.is_empty());
    let failed_fragment_job_ids: Vec<&str> = failed_fragments
        .iter()
        .map(|fragment| fragment.job_id.as_str())
        .collect();
    assert_eq!(
        failed_fragment_job_ids,
        vec!["job-retryable-failed", "job-permanent-failed"]
    );
    assert_eq!(succeeded_fragments.len(), 1);
    assert_eq!(succeeded_fragments[0].job_id, "job-succeeded");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_replay_cleanup_repairs_succeeded_jobs_with_pending_fragments()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let job = CdcBatchLoadJobRecord {
        job_id: "job-succeeded-fragment-pending".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Succeeded,
        payload_json: "{}".to_string(),
        attempt_count: 1,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    let fragment = CdcCommitFragmentRecord {
        fragment_id: "job-succeeded-fragment-pending:10".to_string(),
        job_id: job.job_id.clone(),
        sequence: 10,
        commit_lsn: "0/AAA".to_string(),
        table_key: job.table_key.clone(),
        status: CdcCommitFragmentStatus::Pending,
        row_count: 1,
        upserted_count: 1,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;

    let cleanup = handle
        .discard_inflight_cdc_batch_load_state_for_replay()
        .await?;
    assert_eq!(cleanup.discarded_jobs, 0);
    assert_eq!(cleanup.discarded_fragments, 0);
    assert_eq!(cleanup.repaired_terminal_fragments, 1);

    let pending_fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
        .await?;
    assert!(pending_fragments.is_empty());

    let succeeded_fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Succeeded])
        .await?;
    assert_eq!(succeeded_fragments.len(), 1);
    assert_eq!(succeeded_fragments[0].stage, CdcLedgerStage::Applied);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_marks_batch_load_bundle_succeeded_atomically() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let job = CdcBatchLoadJobRecord {
        job_id: "job-atomic-success".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{}".to_string(),
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    let fragment = CdcCommitFragmentRecord {
        fragment_id: "job-atomic-success:10".to_string(),
        job_id: job.job_id.clone(),
        sequence: 10,
        commit_lsn: "0/AAA".to_string(),
        table_key: job.table_key.clone(),
        status: CdcCommitFragmentStatus::Pending,
        row_count: 1,
        upserted_count: 1,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;
    handle
        .mark_cdc_batch_load_bundle_succeeded(&job.job_id)
        .await?;

    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Succeeded])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].stage, CdcLedgerStage::Applied);

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Succeeded])
        .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].stage, CdcLedgerStage::Applied);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_marks_batch_load_bundle_failed_atomically() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let job = CdcBatchLoadJobRecord {
        job_id: "job-atomic-failure".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{}".to_string(),
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    let fragment = CdcCommitFragmentRecord {
        fragment_id: "job-atomic-failure:10".to_string(),
        job_id: job.job_id.clone(),
        sequence: 10,
        commit_lsn: "0/AAA".to_string(),
        table_key: job.table_key.clone(),
        status: CdcCommitFragmentStatus::Pending,
        row_count: 1,
        upserted_count: 1,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;
    handle
        .mark_cdc_batch_load_bundle_failed(
            &job.job_id,
            "permanent failure",
            SyncRetryClass::Permanent,
            true,
        )
        .await?;

    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Failed])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].stage, CdcLedgerStage::Failed);
    assert_eq!(jobs[0].last_error.as_deref(), Some("permanent failure"));

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Failed])
        .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].stage, CdcLedgerStage::Failed);
    assert_eq!(
        fragments[0].last_error.as_deref(),
        Some("permanent failure")
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_dedup_preserves_succeeded_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let base = CdcBatchLoadJobRecord {
        job_id: "job-dedup".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{\"v\":1}".to_string(),
        attempt_count: 0,
        retry_class: None,
        last_error: None,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };

    let first = handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    assert_eq!(first.status, CdcBatchLoadJobStatus::Pending);

    handle
        .mark_cdc_batch_load_bundle_succeeded(&base.job_id)
        .await?;

    let replayed = handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                payload_json: "{\"v\":2}".to_string(),
                updated_at: now + 100,
                ..base.clone()
            },
            &[],
        )
        .await?;
    assert_eq!(replayed.status, CdcBatchLoadJobStatus::Succeeded);
    assert_eq!(replayed.stage, CdcLedgerStage::Applied);
    assert_eq!(replayed.payload_json, "{\"v\":1}");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_dedup_revives_failed_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let base = CdcBatchLoadJobRecord {
        job_id: "job-failed".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{\"v\":1}".to_string(),
        attempt_count: 0,
        retry_class: None,
        last_error: None,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };

    handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    handle
        .mark_cdc_batch_load_bundle_failed(&base.job_id, "boom", SyncRetryClass::Permanent, true)
        .await?;

    let replayed = handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                payload_json: "{\"v\":2}".to_string(),
                updated_at: now + 100,
                ..base
            },
            &[],
        )
        .await?;
    assert_eq!(replayed.status, CdcBatchLoadJobStatus::Pending);
    assert_eq!(replayed.stage, CdcLedgerStage::Received);
    assert_eq!(replayed.payload_json, "{\"v\":2}");
    assert_eq!(replayed.last_error, None);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_requeues_retryable_batch_load_job_by_id() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let base = CdcBatchLoadJobRecord {
        job_id: "job-retry".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 10,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{\"v\":1}".to_string(),
        attempt_count: 0,
        retry_class: None,
        last_error: None,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };

    handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    handle
        .mark_cdc_batch_load_bundle_failed(
            &base.job_id,
            "quota exceeded",
            SyncRetryClass::Backpressure,
            false,
        )
        .await?;

    assert!(handle.requeue_cdc_batch_load_job(&base.job_id).await?);

    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, base.job_id);
    assert_eq!(jobs[0].stage, CdcLedgerStage::Received);
    assert_eq!(jobs[0].retry_class, Some(SyncRetryClass::Backpressure));
    assert_eq!(jobs[0].last_error, None);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_loads_retryable_failed_jobs_for_durable_retry() -> anyhow::Result<()>
{
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (index, (job_id, retry_class)) in [
        ("job-transient", Some(SyncRetryClass::Transient)),
        ("job-backpressure", Some(SyncRetryClass::Backpressure)),
        ("job-permanent", Some(SyncRetryClass::Permanent)),
        ("job-unclassified", None),
    ]
    .into_iter()
    .enumerate()
    {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: format!("public__table_{index}"),
                    first_sequence: 10 + index as u64,
                    status: CdcBatchLoadJobStatus::Failed,
                    stage: CdcLedgerStage::Failed,
                    payload_json: "{}".to_string(),
                    attempt_count: 2,
                    retry_class,
                    last_error: Some("failed".to_string()),
                    created_at: now + index as i64,
                    updated_at: now + index as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let jobs = store
        .load_retryable_failed_cdc_batch_load_jobs("app")
        .await?;
    let job_ids = jobs
        .iter()
        .map(|job| job.job_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(job_ids, vec!["job-transient", "job-backpressure"]);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_batch_load_queue_summary_classifies_failed_jobs() -> anyhow::Result<()>
{
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let cases = [
        (
            "job-snapshot-handoff",
            Some(SyncRetryClass::Transient),
            "CDC batch-load waiting for snapshot handoff for public.workorders_workorder",
        ),
        (
            "job-generic-retryable",
            Some(SyncRetryClass::Backpressure),
            "quota exceeded",
        ),
        (
            "job-permanent",
            Some(SyncRetryClass::Permanent),
            "invalid field mapping",
        ),
        (
            "job-legacy-null",
            None,
            "legacy failed row without retry class",
        ),
    ];

    for (index, (job_id, retry_class, error)) in cases.into_iter().enumerate() {
        handle
            .enqueue_cdc_batch_load_bundle(
                &CdcBatchLoadJobRecord {
                    job_id: job_id.to_string(),
                    table_key: format!("public__table_{index}"),
                    first_sequence: 10 + index as u64,
                    status: CdcBatchLoadJobStatus::Failed,
                    stage: CdcLedgerStage::Failed,
                    payload_json: "{}".to_string(),
                    attempt_count: 1,
                    retry_class,
                    last_error: Some(error.to_string()),
                    created_at: now + index as i64,
                    updated_at: now + index as i64,
                    ..Default::default()
                },
                &[],
            )
            .await?;
    }

    let summary = store.load_cdc_batch_load_queue_summary("app").await?;
    assert_eq!(summary.failed_jobs, 4);
    assert_eq!(summary.failed_retryable_jobs, 2);
    assert_eq!(summary.failed_snapshot_handoff_jobs, 1);
    assert_eq!(summary.failed_permanent_jobs, 1);
    assert_eq!(summary.failed_unclassified_jobs, 1);
    assert_eq!(summary.latest_failed_retry_class, None);
    assert_eq!(
        summary.latest_failed_error.as_deref(),
        Some("legacy failed row without retry class")
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_releases_snapshot_handoff_waiting_jobs_when_snapshot_completes()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();
    let job_id = "job-snapshot-wait".to_string();
    let reason = "CDC batch-load waiting for snapshot handoff for public.accounts";

    handle
        .save_postgres_checkpoint(
            "public.accounts",
            &TableCheckpoint {
                snapshot_chunks: vec![SnapshotChunkCheckpoint {
                    start_primary_key: Some("1".to_string()),
                    end_primary_key: Some("10".to_string()),
                    last_primary_key: Some("5".to_string()),
                    complete: false,
                }],
                ..Default::default()
            },
        )
        .await?;
    handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                job_id: job_id.clone(),
                table_key: "public__accounts".to_string(),
                first_sequence: 10,
                status: CdcBatchLoadJobStatus::Running,
                stage: CdcLedgerStage::Applying,
                payload_json: reducer_payload_json(&job_id, "accounts"),
                attempt_count: 1,
                created_at: now,
                updated_at: now,
                ..Default::default()
            },
            &[CdcCommitFragmentRecord {
                fragment_id: format!("{job_id}:10"),
                job_id: job_id.clone(),
                sequence: 10,
                commit_lsn: "0/AAA".to_string(),
                table_key: "public__accounts".to_string(),
                status: CdcCommitFragmentStatus::Pending,
                row_count: 1,
                upserted_count: 1,
                created_at: now,
                updated_at: now,
                ..Default::default()
            }],
        )
        .await?;
    handle
        .mark_cdc_batch_load_window_blocked_for_snapshot_handoff(
            std::slice::from_ref(&job_id),
            reason,
        )
        .await?;

    let summary = store.load_cdc_batch_load_queue_summary("app").await?;
    assert_eq!(summary.failed_jobs, 0);
    assert_eq!(summary.blocked_jobs, 1);
    assert_eq!(summary.snapshot_handoff_waiting_jobs, 1);
    assert_eq!(
        store
            .release_snapshot_handoff_blocked_cdc_batch_load_jobs("app")
            .await?,
        0
    );

    handle
        .save_postgres_checkpoint(
            "public.accounts",
            &TableCheckpoint {
                snapshot_chunks: vec![SnapshotChunkCheckpoint {
                    start_primary_key: Some("1".to_string()),
                    end_primary_key: Some("10".to_string()),
                    last_primary_key: Some("10".to_string()),
                    complete: true,
                }],
                ..Default::default()
            },
        )
        .await?;

    assert_eq!(
        store
            .release_snapshot_handoff_blocked_cdc_batch_load_jobs("app")
            .await?,
        1
    );
    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, job_id);
    assert_eq!(jobs[0].stage, CdcLedgerStage::Loaded);
    assert_eq!(jobs[0].last_error, None);

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
        .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].stage, CdcLedgerStage::Received);
    assert_eq!(fragments[0].last_error, None);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_bundle_persists_job_fragments_and_watermark()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let job = CdcBatchLoadJobRecord {
        job_id: "job-bundle".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 42,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{\"v\":1}".to_string(),
        attempt_count: 0,
        retry_class: None,
        last_error: None,
        staging_table: Some("public__table_a_staging_upsert_job_bundle".to_string()),
        artifact_uri: Some("gs://bucket/table_a.parquet".to_string()),
        load_job_id: Some("load-job-1".to_string()),
        merge_job_id: Some("merge-job-1".to_string()),
        primary_key_lane: Some("lane-0".to_string()),
        barrier_kind: Some("truncate".to_string()),
        ledger_metadata_json: Some("{\"source\":\"test\"}".to_string()),
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    let fragment = CdcCommitFragmentRecord {
        fragment_id: "job-bundle:42".to_string(),
        job_id: job.job_id.clone(),
        sequence: 42,
        commit_lsn: "0/ABC".to_string(),
        table_key: job.table_key.clone(),
        status: CdcCommitFragmentStatus::Pending,
        row_count: 10,
        upserted_count: 10,
        deleted_count: 0,
        last_error: None,
        artifact_uri: Some("gs://bucket/table_a.parquet".to_string()),
        staging_table: Some("public__table_a_staging_upsert_job_bundle".to_string()),
        load_job_id: Some("load-job-1".to_string()),
        merge_job_id: Some("merge-job-1".to_string()),
        primary_key_lane: Some("lane-0".to_string()),
        barrier_kind: Some("truncate".to_string()),
        ledger_metadata_json: Some("{\"source\":\"test\"}".to_string()),
        created_at: now,
        updated_at: now,
        ..Default::default()
    };

    let persisted = handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;
    assert_eq!(persisted.job_id, job.job_id);

    let jobs = store
        .load_cdc_batch_load_jobs("app", &[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, job.job_id);
    assert_eq!(jobs[0].stage, CdcLedgerStage::Received);
    assert_eq!(
        jobs[0].staging_table.as_deref(),
        Some("public__table_a_staging_upsert_job_bundle")
    );
    assert_eq!(
        jobs[0].artifact_uri.as_deref(),
        Some("gs://bucket/table_a.parquet")
    );
    assert_eq!(jobs[0].load_job_id.as_deref(), Some("load-job-1"));
    assert_eq!(jobs[0].merge_job_id.as_deref(), Some("merge-job-1"));
    assert_eq!(jobs[0].primary_key_lane.as_deref(), Some("lane-0"));
    assert_eq!(jobs[0].barrier_kind.as_deref(), Some("truncate"));
    assert_eq!(
        jobs[0].ledger_metadata_json.as_deref(),
        Some("{\"source\":\"test\"}")
    );

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
        .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].fragment_id, fragment.fragment_id);
    assert_eq!(fragments[0].stage, CdcLedgerStage::Received);
    assert_eq!(
        fragments[0].staging_table.as_deref(),
        Some("public__table_a_staging_upsert_job_bundle")
    );
    assert_eq!(
        fragments[0].artifact_uri.as_deref(),
        Some("gs://bucket/table_a.parquet")
    );
    assert_eq!(fragments[0].load_job_id.as_deref(), Some("load-job-1"));
    assert_eq!(fragments[0].merge_job_id.as_deref(), Some("merge-job-1"));
    assert_eq!(fragments[0].primary_key_lane.as_deref(), Some("lane-0"));
    assert_eq!(fragments[0].barrier_kind.as_deref(), Some("truncate"));

    let watermark = store.load_cdc_watermark_state("app").await?;
    assert_eq!(
        watermark.and_then(|state| state.last_enqueued_sequence),
        Some(42)
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_loads_contiguous_durable_apply_frontier() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for (sequence, status) in [
        (10_u64, CdcCommitFragmentStatus::Succeeded),
        (11, CdcCommitFragmentStatus::Succeeded),
        (12, CdcCommitFragmentStatus::Pending),
    ] {
        let job_id = format!("job-frontier-{sequence}");
        let job = CdcBatchLoadJobRecord {
            job_id: job_id.clone(),
            table_key: "table_a".to_string(),
            first_sequence: sequence,
            status: if status == CdcCommitFragmentStatus::Succeeded {
                CdcBatchLoadJobStatus::Succeeded
            } else {
                CdcBatchLoadJobStatus::Pending
            },
            stage: if status == CdcCommitFragmentStatus::Succeeded {
                CdcLedgerStage::Applied
            } else {
                CdcLedgerStage::Loaded
            },
            payload_json: reducer_payload_json(&job_id, "table_a"),
            created_at: now + sequence as i64,
            updated_at: now + sequence as i64,
            ..Default::default()
        };
        let fragment = CdcCommitFragmentRecord {
            fragment_id: format!("{job_id}:{sequence}"),
            job_id: job.job_id.clone(),
            sequence,
            commit_lsn: format!("0/{sequence:X}"),
            table_key: job.table_key.clone(),
            status,
            stage: if status == CdcCommitFragmentStatus::Succeeded {
                CdcLedgerStage::Applied
            } else {
                CdcLedgerStage::Loaded
            },
            row_count: 1,
            upserted_count: 1,
            created_at: now + sequence as i64,
            updated_at: now + sequence as i64,
            ..Default::default()
        };
        handle
            .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
            .await?;
    }

    let frontier = handle
        .load_cdc_durable_apply_frontier(10, 10)
        .await?
        .expect("durable frontier");
    assert_eq!(frontier.next_sequence_to_ack, 12);
    assert_eq!(frontier.commit_lsn, "0/B");

    assert!(
        handle
            .load_cdc_durable_apply_frontier(12, 10)
            .await?
            .is_none()
    );

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_composes_enqueue_and_feedback_watermark_state() -> anyhow::Result<()>
{
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config, 16).await?;
    let store = SyncStateStore::open_with_config(&config, 16).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    let job = CdcBatchLoadJobRecord {
        job_id: "job-compose".to_string(),
        table_key: "table_a".to_string(),
        first_sequence: 77,
        status: CdcBatchLoadJobStatus::Pending,
        payload_json: "{\"v\":1}".to_string(),
        attempt_count: 0,
        retry_class: None,
        last_error: None,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    let fragment = CdcCommitFragmentRecord {
        fragment_id: "job-compose:77".to_string(),
        job_id: job.job_id.clone(),
        sequence: 77,
        commit_lsn: "0/BEE".to_string(),
        table_key: job.table_key.clone(),
        status: CdcCommitFragmentStatus::Pending,
        row_count: 10,
        upserted_count: 10,
        deleted_count: 0,
        last_error: None,
        created_at: now,
        updated_at: now,
        ..Default::default()
    };
    handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;

    handle
        .save_cdc_feedback_state(&CdcWatermarkState {
            next_sequence_to_ack: 77,
            last_enqueued_sequence: None,
            last_received_lsn: None,
            last_flushed_lsn: Some("0/BEF".to_string()),
            last_persisted_lsn: Some("0/BEF".to_string()),
            last_relevant_change_seen_at: None,
            last_status_update_sent_at: Some(Utc::now()),
            last_keepalive_reply_at: Some(Utc::now()),
            last_slot_feedback_lsn: Some("0/BEF".to_string()),
            updated_at: Some(Utc::now()),
        })
        .await?;

    let watermark = store
        .load_cdc_watermark_state("app")
        .await?
        .expect("watermark state should be composed");
    let feedback = handle
        .load_cdc_feedback_state()
        .await?
        .expect("feedback state should be persisted");
    assert_eq!(watermark.next_sequence_to_ack, 77);
    assert_eq!(watermark.last_enqueued_sequence, Some(77));
    assert_eq!(watermark.last_received_lsn.as_deref(), Some("0/BEE"));
    assert_eq!(watermark.last_flushed_lsn.as_deref(), Some("0/BEF"));
    assert_eq!(watermark.last_persisted_lsn.as_deref(), Some("0/BEF"));
    assert!(watermark.last_status_update_sent_at.is_some());
    assert!(watermark.last_keepalive_reply_at.is_some());
    assert_eq!(watermark.last_slot_feedback_lsn.as_deref(), Some("0/BEF"));
    assert_eq!(feedback.next_sequence_to_ack, 77);
    assert_eq!(feedback.last_enqueued_sequence, None);
    assert_eq!(feedback.last_received_lsn, None);

    Ok(())
}
