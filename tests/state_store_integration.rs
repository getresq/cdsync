use cdsync::config::StateConfig;
use cdsync::retry::SyncRetryClass;
use cdsync::state::{
    CdcBatchLoadJobRecord, CdcBatchLoadJobStatus, CdcCommitFragmentRecord, CdcCommitFragmentStatus,
    CdcWatermarkState, ConnectionState, PostgresCdcState, SyncStateStore,
};
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

#[tokio::test]
async fn postgres_state_store_persists_cdc_fragments_and_watermark_state() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
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
                },
            ],
        )
        .await?;
    handle
        .mark_cdc_commit_fragments_succeeded_for_job("job-1")
        .await?;

    handle
        .save_cdc_watermark_state(&CdcWatermarkState {
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

    let watermark = handle
        .load_cdc_watermark_state()
        .await?
        .expect("watermark state should be persisted");
    assert_eq!(watermark.next_sequence_to_ack, 41);
    assert_eq!(watermark.last_enqueued_sequence, Some(42));
    assert_eq!(watermark.last_received_lsn.as_deref(), Some("0/AAB"));
    assert_eq!(watermark.last_flushed_lsn.as_deref(), Some("0/AAA"));

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_claims_oldest_eligible_job_per_table() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    for job in [
        CdcBatchLoadJobRecord {
            job_id: "job-a-10".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 10,
            status: CdcBatchLoadJobStatus::Pending,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        },
        CdcBatchLoadJobRecord {
            job_id: "job-a-11".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 11,
            status: CdcBatchLoadJobStatus::Pending,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now + 1,
            updated_at: now + 1,
        },
        CdcBatchLoadJobRecord {
            job_id: "job-b-20".to_string(),
            table_key: "table_b".to_string(),
            first_sequence: 20,
            status: CdcBatchLoadJobStatus::Pending,
            payload_json: "{}".to_string(),
            attempt_count: 0,
            retry_class: None,
            last_error: None,
            created_at: now + 2,
            updated_at: now + 2,
        },
    ] {
        handle.enqueue_cdc_batch_load_bundle(&job, &[]).await?;
    }

    let first = handle
        .claim_next_cdc_batch_load_job(now - 1)
        .await?
        .expect("first claim");
    assert_eq!(first.job_id, "job-a-10");
    assert_eq!(first.status, CdcBatchLoadJobStatus::Running);

    let second = handle
        .claim_next_cdc_batch_load_job(now - 1)
        .await?
        .expect("second claim");
    assert_eq!(second.job_id, "job-b-20");
    assert_eq!(second.status, CdcBatchLoadJobStatus::Running);

    let third = handle.claim_next_cdc_batch_load_job(now - 1).await?;
    assert!(third.is_none());

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_round_trips_manual_table_resync_requests() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;

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
async fn postgres_state_store_requeues_running_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                job_id: "job-running".to_string(),
                table_key: "table_a".to_string(),
                first_sequence: 10,
                status: CdcBatchLoadJobStatus::Running,
                payload_json: "{}".to_string(),
                attempt_count: 1,
                retry_class: None,
                last_error: None,
                created_at: now,
                updated_at: now,
            },
            &[],
        )
        .await?;

    let requeued = handle.requeue_cdc_batch_load_running_jobs().await?;
    assert_eq!(requeued, 1);

    let jobs = handle
        .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "job-running");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_requeues_retryable_failed_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    handle
        .enqueue_cdc_batch_load_bundle(&CdcBatchLoadJobRecord {
            job_id: "job-failed-merge".to_string(),
            table_key: "table_a".to_string(),
            first_sequence: 10,
            status: CdcBatchLoadJobStatus::Failed,
            payload_json: "{}".to_string(),
            attempt_count: 2,
            retry_class: Some(SyncRetryClass::Transient),
            last_error: Some(
                "[DestinationError] failed to process CDC batch-load job: merging staging BigQuery table foo into bar".to_string(),
            ),
            created_at: now,
            updated_at: now,
        }, &[])
        .await?;

    let requeued = handle
        .requeue_retryable_failed_cdc_batch_load_jobs()
        .await?;
    assert_eq!(requeued, 1);

    let jobs = handle
        .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "job-failed-merge");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_does_not_requeue_permanent_failed_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");
    let now = chrono::Utc::now().timestamp_millis();

    handle
        .enqueue_cdc_batch_load_bundle(
            &CdcBatchLoadJobRecord {
                job_id: "job-failed-perm".to_string(),
                table_key: "table_a".to_string(),
                first_sequence: 10,
                status: CdcBatchLoadJobStatus::Failed,
                payload_json: "{}".to_string(),
                attempt_count: 2,
                retry_class: Some(SyncRetryClass::Permanent),
                last_error: Some("permanent failure".to_string()),
                created_at: now,
                updated_at: now,
            },
            &[],
        )
        .await?;

    let requeued = handle
        .requeue_retryable_failed_cdc_batch_load_jobs()
        .await?;
    assert_eq!(requeued, 0);

    let jobs = handle
        .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Failed])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, "job-failed-perm");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_dedup_preserves_succeeded_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
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
    };

    let first = handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    assert_eq!(first.status, CdcBatchLoadJobStatus::Pending);

    handle
        .mark_cdc_batch_load_job_succeeded(&base.job_id)
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
    assert_eq!(replayed.payload_json, "{\"v\":1}");

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_dedup_revives_failed_jobs() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
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
    };

    handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    handle
        .mark_cdc_batch_load_job_failed(&base.job_id, "boom", SyncRetryClass::Permanent)
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
    assert_eq!(replayed.payload_json, "{\"v\":2}");
    assert_eq!(replayed.last_error, None);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_requeues_retryable_batch_load_job_by_id() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
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
    };

    handle.enqueue_cdc_batch_load_bundle(&base, &[]).await?;
    handle
        .mark_cdc_batch_load_job_failed(
            &base.job_id,
            "quota exceeded",
            SyncRetryClass::Backpressure,
        )
        .await?;

    assert!(handle.requeue_cdc_batch_load_job(&base.job_id).await?);

    let jobs = handle
        .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, base.job_id);
    assert_eq!(jobs[0].retry_class, Some(SyncRetryClass::Backpressure));
    assert_eq!(jobs[0].last_error, None);

    Ok(())
}

#[tokio::test]
async fn postgres_state_store_enqueue_bundle_persists_job_fragments_and_watermark()
-> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    SyncStateStore::migrate_with_config(&config).await?;
    let store = SyncStateStore::open_with_config(&config).await?;
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
        created_at: now,
        updated_at: now,
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
        created_at: now,
        updated_at: now,
    };

    let persisted = handle
        .enqueue_cdc_batch_load_bundle(&job, std::slice::from_ref(&fragment))
        .await?;
    assert_eq!(persisted.job_id, job.job_id);

    let jobs = handle
        .load_cdc_batch_load_jobs(&[CdcBatchLoadJobStatus::Pending])
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].job_id, job.job_id);

    let fragments = store
        .load_cdc_commit_fragments("app", &[CdcCommitFragmentStatus::Pending])
        .await?;
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].fragment_id, fragment.fragment_id);

    let watermark = handle.load_cdc_watermark_state().await?;
    assert_eq!(
        watermark.and_then(|state| state.last_enqueued_sequence),
        Some(42)
    );

    Ok(())
}
