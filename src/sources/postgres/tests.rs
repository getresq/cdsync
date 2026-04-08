use super::snapshot_sync::save_snapshot_progress;
use super::*;
use crate::destinations::Destination;
use async_trait::async_trait;
use polars::prelude::{NamedFrom, Series};
use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

#[test]
fn maps_pg_types_to_internal_types() {
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INT4).expect("int4"),
        DataType::Int64
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::FLOAT8).expect("float8"),
        DataType::Float64
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::BOOL).expect("bool"),
        DataType::Bool
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::TIMESTAMPTZ).expect("timestamptz"),
        DataType::Timestamp
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INTERVAL).expect("interval"),
        DataType::Interval
    );
}

#[test]
fn maps_array_and_range_types_to_json() {
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INT4_ARRAY).expect("int4 array"),
        DataType::Json
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INT4_RANGE).expect("int4 range"),
        DataType::Json
    );
}

#[test]
fn snapshot_shutdown_requested_tracks_signal_state() {
    let (controller, signal) = crate::runner::ShutdownController::new();
    assert!(!super::snapshot_sync::snapshot_shutdown_requested(Some(
        &signal
    )));

    controller.shutdown();

    assert!(super::snapshot_sync::snapshot_shutdown_requested(Some(
        &signal
    )));
    assert!(!super::snapshot_sync::snapshot_shutdown_requested(None));
}

#[test]
fn cdc_snapshot_completion_requires_success_without_shutdown() {
    let (controller, signal) = crate::runner::ShutdownController::new();
    assert!(super::cdc_sync::cdc_snapshot_completed(true, Some(&signal)));

    controller.shutdown();

    assert!(!super::cdc_sync::cdc_snapshot_completed(
        true,
        Some(&signal)
    ));
    assert!(!super::cdc_sync::cdc_snapshot_completed(false, None));
}

#[test]
fn next_cdc_wait_timeout_uses_pending_fill_deadline_when_sooner() {
    let mut pending = HashMap::new();
    pending.insert(
        TableId::new(1),
        super::cdc_pipeline::PendingTableApplyBatch {
            fragments: vec![crate::destinations::etl_bigquery::CdcCommitFragmentMeta {
                sequence: 1,
                commit_lsn: "0/1".to_string(),
            }],
            events: Vec::new(),
            event_count: 1,
            first_buffered_at: Instant::now() - Duration::from_millis(1_500),
        },
    );

    let timeout = super::cdc_runtime::next_cdc_wait_timeout(
        Duration::from_secs(10),
        Duration::from_secs(2),
        &pending,
        0,
        1,
    );
    assert!(timeout <= Duration::from_millis(550));
}

#[test]
fn next_cdc_wait_timeout_falls_back_to_idle_when_apply_slots_are_full() {
    let mut pending = HashMap::new();
    pending.insert(
        TableId::new(1),
        super::cdc_pipeline::PendingTableApplyBatch {
            fragments: vec![crate::destinations::etl_bigquery::CdcCommitFragmentMeta {
                sequence: 1,
                commit_lsn: "0/1".to_string(),
            }],
            events: Vec::new(),
            event_count: 1,
            first_buffered_at: Instant::now() - Duration::from_secs(3),
        },
    );

    let timeout = super::cdc_runtime::next_cdc_wait_timeout(
        Duration::from_secs(10),
        Duration::from_secs(2),
        &pending,
        1,
        1,
    );
    assert_eq!(timeout, Duration::from_secs(10));
}

#[test]
fn cdc_fill_deadline_reached_tracks_pending_batch_age() {
    let mut pending = HashMap::new();
    pending.insert(
        TableId::new(1),
        super::cdc_pipeline::PendingTableApplyBatch {
            fragments: vec![crate::destinations::etl_bigquery::CdcCommitFragmentMeta {
                sequence: 1,
                commit_lsn: "0/1".to_string(),
            }],
            events: Vec::new(),
            event_count: 1,
            first_buffered_at: Instant::now() - Duration::from_secs(3),
        },
    );

    assert!(super::cdc_runtime::cdc_fill_deadline_reached(
        Duration::from_secs(2),
        &pending,
    ));
    assert!(!super::cdc_runtime::cdc_fill_deadline_reached(
        Duration::from_secs(5),
        &pending,
    ));
}

#[tokio::test]
async fn cdc_timeout_helper_errors_on_elapsed_deadline() {
    let result = super::cdc_pipeline::await_cdc_timeout(
        "pending watermark operation",
        Duration::from_millis(10),
        pending::<Result<(), anyhow::Error>>(),
    )
    .await;

    let err = result.expect_err("expected timeout");
    assert!(
        err.to_string()
            .contains("pending watermark operation timed out"),
        "{err}"
    );
}

#[test]
fn cdc_should_stop_after_idle_ignores_keepalive_only_idle_time() {
    assert!(super::cdc_runtime::cdc_should_stop_after_idle(
        &super::cdc_runtime::CdcIdleState {
            follow: false,
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: true,
            pending_table_batches_empty: true,
            inflight_apply_empty: true,
        },
        Instant::now() - Duration::from_secs(2),
        Duration::from_secs(1),
    ));
    assert!(!super::cdc_runtime::cdc_should_stop_after_idle(
        &super::cdc_runtime::CdcIdleState {
            follow: true,
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: true,
            pending_table_batches_empty: true,
            inflight_apply_empty: true,
        },
        Instant::now() - Duration::from_secs(2),
        Duration::from_secs(1),
    ));
    assert!(!super::cdc_runtime::cdc_should_stop_after_idle(
        &super::cdc_runtime::CdcIdleState {
            follow: false,
            in_tx: false,
            pending_events_empty: true,
            queued_batches_empty: false,
            pending_table_batches_empty: true,
            inflight_apply_empty: true,
        },
        Instant::now() - Duration::from_secs(2),
        Duration::from_secs(1),
    ));
}

#[test]
fn rejects_multirange_and_builtin_unsupported_types() {
    assert!(pg_type_to_data_type_from_type(&etl::types::Type::INT4MULTI_RANGE).is_err());
    assert!(pg_type_to_data_type_from_type(&etl::types::Type::XID8).is_err());
}

#[test]
fn pg_catalog_data_type_handles_arrays_json_and_unsupported_types() {
    assert_eq!(
        pg_type_to_data_type("ARRAY", "_int4", "b").expect("array"),
        DataType::Json
    );
    assert_eq!(
        pg_type_to_data_type("USER-DEFINED", "hstore", "b").expect("hstore"),
        DataType::Json
    );
    assert_eq!(
        pg_type_to_data_type("USER-DEFINED", "status_enum", "e").expect("enum"),
        DataType::String
    );
    assert!(pg_type_to_data_type("USER-DEFINED", "custom_composite", "c").is_err());
    assert!(pg_type_to_data_type("USER-DEFINED", "int4multirange", "m").is_err());
}

#[test]
fn incremental_sql_uses_strict_paging_without_last_pk() {
    let sql = build_incremental_sql(&IncrementalSqlParts {
        schema: "public",
        table: "accounts",
        columns: "id, updated_at",
        updated_at: "updated_at",
        primary_key: "id",
        pk_cast: "bigint",
        where_clause: None,
        has_last_pk: false,
    });
    assert!(sql.contains("updated_at > $1"));
    assert!(!sql.contains(">="));
    assert!(sql.contains("ORDER BY updated_at ASC, id ASC LIMIT $2"));
}

#[test]
fn incremental_sql_uses_tie_breaker_with_last_pk_and_filters() {
    let sql = build_incremental_sql(&IncrementalSqlParts {
        schema: "public",
        table: "accounts",
        columns: "id, updated_at",
        updated_at: "updated_at",
        primary_key: "id",
        pk_cast: "bigint",
        where_clause: Some("tenant_id = 42"),
        has_last_pk: true,
    });
    assert!(sql.contains("(tenant_id = 42)"));
    assert!(sql.contains("(updated_at > $1 OR (updated_at = $1 AND id > $2::bigint))"));
    assert!(sql.contains("ORDER BY updated_at ASC, id ASC LIMIT $3"));
}

#[test]
fn read_primary_key_prefers_string_then_numeric() {
    let value =
        read_primary_key_from_value(Value::String("abc123".to_string())).expect("string pk");
    assert_eq!(value, "abc123");

    let value = read_primary_key_from_value(Value::Number(serde_json::Number::from(42)))
        .expect("numeric pk");
    assert_eq!(value, "42");
}

#[test]
fn read_primary_key_supports_bool_and_null() {
    let value = read_primary_key_from_value(Value::Bool(true)).expect("bool pk");
    assert_eq!(value, "true");

    assert!(read_primary_key_from_value(Value::Null).is_err());
}

#[test]
fn read_primary_key_supports_uuid_and_decimal() {
    let uuid = Uuid::parse_str("2e4b7f22-5a7f-4f94-9a9f-6b1f1c2e0a5b").expect("valid uuid");
    let value = read_primary_key_from_value(Value::String(uuid.to_string())).expect("uuid pk");
    assert_eq!(value, uuid.to_string());

    let decimal = BigDecimal::from(12345i64);
    let value =
        read_primary_key_from_value(Value::String(decimal.to_string())).expect("decimal pk");
    assert_eq!(value, "12345");
}

#[test]
fn primary_key_cast_qualifies_non_pg_schema() {
    assert_eq!(format_cast_type("citext", "public"), "public.citext");
    assert_eq!(format_cast_type("uuid", "pg_catalog"), "uuid");
}

#[test]
fn filter_columns_keeps_required_fields() {
    let all_columns = vec![
        ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        ColumnSchema {
            name: "name".to_string(),
            data_type: DataType::String,
            nullable: true,
        },
        ColumnSchema {
            name: "updated_at".to_string(),
            data_type: DataType::Timestamp,
            nullable: false,
        },
    ];
    let selection = ColumnSelection {
        include: vec!["name".to_string()],
        exclude: vec!["updated_at".to_string()],
    };
    let required: HashSet<String> = ["id".to_string(), "updated_at".to_string()]
        .into_iter()
        .collect();

    let filtered = filter_columns(&all_columns, &selection, &required);
    let names: Vec<&str> = filtered.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "updated_at"]);
}

#[test]
fn collect_table_configs_applies_include_and_exclude_patterns() {
    let selection = TableSelectionConfig {
        include: vec!["public.*".to_string()],
        exclude: vec!["public.audit_*".to_string()],
        defaults: None,
    };

    let configs = collect_table_configs(
        None,
        Some(&selection),
        &[
            "public.accounts".to_string(),
            "public.audit_log".to_string(),
            "analytics.events".to_string(),
        ],
    )
    .expect("table configs");

    let names: Vec<&str> = configs.iter().map(|config| config.name.as_str()).collect();
    assert_eq!(names, vec!["public.accounts"]);
}

#[test]
fn collect_table_configs_preserves_explicit_table_settings() {
    let selection = TableSelectionConfig {
        include: vec!["public.*".to_string()],
        exclude: Vec::new(),
        defaults: None,
    };
    let explicit_table = PostgresTableConfig {
        name: "public.accounts".to_string(),
        primary_key: Some("account_id".to_string()),
        updated_at_column: Some("modified_at".to_string()),
        soft_delete: Some(true),
        soft_delete_column: Some("deleted_at".to_string()),
        where_clause: Some("tenant_id = 42".to_string()),
        columns: Some(ColumnSelection {
            include: vec!["account_id".to_string(), "modified_at".to_string()],
            exclude: Vec::new(),
        }),
    };

    let configs = collect_table_configs(
        Some(std::slice::from_ref(&explicit_table)),
        Some(&selection),
        &["public.accounts".to_string(), "public.orders".to_string()],
    )
    .expect("table configs");

    let accounts = configs
        .iter()
        .find(|config| config.name == "public.accounts")
        .expect("accounts config");
    assert_eq!(accounts.primary_key.as_deref(), Some("account_id"));
    assert_eq!(accounts.updated_at_column.as_deref(), Some("modified_at"));
    assert_eq!(accounts.where_clause.as_deref(), Some("tenant_id = 42"));

    let names: Vec<&str> = configs.iter().map(|config| config.name.as_str()).collect();
    assert_eq!(names, vec!["public.accounts", "public.orders"]);
}

#[test]
fn schema_diff_detects_incompatible_changes() {
    let previous = vec![
        SchemaFieldSnapshot {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        SchemaFieldSnapshot {
            name: "name".to_string(),
            data_type: DataType::String,
            nullable: false,
        },
    ];
    let current = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![
            ColumnSchema {
                name: "name".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "extra".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ],
        primary_key: Some("id".to_string()),
    };

    let diff = schema_diff(Some(&previous), &current).expect("diff");
    assert!(!diff.is_empty());
    assert!(diff.has_incompatible());
    assert!(diff.added.contains(&"extra".to_string()));
    assert!(diff.removed.contains(&"id".to_string()));
    assert!(diff.type_changed.iter().any(|(name, _, _)| name == "name"));
}

#[test]
fn schema_diff_treats_removed_and_nullable_only_changes_as_compatible() {
    let previous = vec![
        SchemaFieldSnapshot {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        SchemaFieldSnapshot {
            name: "legacy_name".to_string(),
            data_type: DataType::String,
            nullable: false,
        },
    ];
    let current = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: true,
        }],
        primary_key: Some("id".to_string()),
    };

    let diff = schema_diff(Some(&previous), &current).expect("diff");
    assert_eq!(diff.removed, vec!["legacy_name".to_string()]);
    assert_eq!(diff.nullable_changed, vec![("id".to_string(), false, true)]);
    assert!(!diff.has_incompatible());
}

#[test]
fn primary_key_changed_detects_explicit_primary_key_change() {
    let diff = SchemaDiff::default();
    let schema = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![ColumnSchema {
            name: "account_id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        primary_key: Some("account_id".to_string()),
    };

    assert!(primary_key_changed(
        Some("id"),
        Some("hash-a"),
        &schema,
        "hash-b",
        &diff,
    ));
}

#[test]
fn primary_key_changed_uses_hash_fallback_for_legacy_checkpoints() {
    let diff = SchemaDiff::default();
    let schema = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        primary_key: Some("account_id".to_string()),
    };

    assert!(primary_key_changed(
        None,
        Some("old-hash"),
        &schema,
        "new-hash",
        &diff
    ));
    assert!(!primary_key_changed(
        None,
        Some("same-hash"),
        &schema,
        "same-hash",
        &diff
    ));
}

#[test]
fn primary_key_changed_ignores_schema_hash_when_explicit_primary_key_matches() {
    let diff = SchemaDiff {
        added: vec!["extra".to_string()],
        removed: vec!["legacy_name".to_string()],
        nullable_changed: vec![("id".to_string(), false, true)],
        ..Default::default()
    };
    let schema = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            ColumnSchema {
                name: "extra".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ],
        primary_key: Some("id".to_string()),
    };

    assert!(!primary_key_changed(
        Some("id"),
        Some("old-hash"),
        &schema,
        "new-hash",
        &diff,
    ));
}

#[test]
fn relation_change_requires_destination_ensure_for_new_or_changed_tables() {
    assert!(super::cdc_runtime::relation_change_requires_destination_ensure(false, true, None));
    assert!(super::cdc_runtime::relation_change_requires_destination_ensure(true, false, None));

    let diff = SchemaDiff {
        added: vec!["extra".to_string()],
        ..Default::default()
    };
    assert!(
        super::cdc_runtime::relation_change_requires_destination_ensure(true, true, Some(&diff))
    );
}

#[test]
fn relation_change_skips_destination_ensure_when_schema_is_unchanged() {
    assert!(!super::cdc_runtime::relation_change_requires_destination_ensure(true, true, None));

    let empty = SchemaDiff::default();
    assert!(
        !super::cdc_runtime::relation_change_requires_destination_ensure(true, true, Some(&empty))
    );
}

#[tokio::test]
async fn drain_one_cdc_work_advances_immediate_dispatch_without_completion_future() {
    let mut inflight_dispatch: FuturesUnordered<CdcDispatchFuture> = FuturesUnordered::new();
    let mut inflight_apply: FuturesUnordered<CdcApplyFuture> = FuturesUnordered::new();
    let mut active_table_applies = HashSet::new();
    let table_id = TableId::new(42);
    active_table_applies.insert(table_id);
    inflight_dispatch.push(Box::pin(async move {
        Ok(CdcDispatchResult::Immediate(CdcApplyFragmentAck {
            sequences: vec![0],
            released_table: Some(table_id),
        }))
    }));

    let advances = timeout(
        Duration::from_secs(1),
        super::drain_one_cdc_work(
            &mut inflight_dispatch,
            &mut inflight_apply,
            &mut active_table_applies,
        ),
    )
    .await
    .expect("drain_one_cdc_work timed out")
    .expect("drain_one_cdc_work failed");

    assert_eq!(advances.len(), 1);
    assert_eq!(advances[0].sequences, vec![0]);
    assert!(active_table_applies.is_empty());
}

#[test]
fn snapshot_progress_logging_uses_ten_batch_interval() {
    assert!(!should_log_snapshot_progress(0));
    assert!(!should_log_snapshot_progress(9));
    assert!(should_log_snapshot_progress(10));
    assert!(!should_log_snapshot_progress(11));
    assert!(should_log_snapshot_progress(20));
}

#[test]
fn plan_snapshot_chunk_ranges_keeps_small_tables_on_whole_table_path() {
    let ranges = plan_snapshot_chunk_ranges_from_bounds(31_999, 1, 31_999, 1_000, 4);
    assert!(ranges.is_empty());
}

#[test]
fn snapshot_chunk_checkpoints_use_unbounded_entry_for_single_table_resume() {
    let checkpoints = snapshot_chunk_checkpoints_from_ranges(&[]);
    assert_eq!(
        checkpoints,
        vec![SnapshotChunkCheckpoint {
            start_primary_key: None,
            end_primary_key: None,
            last_primary_key: None,
            complete: false,
        }]
    );
}

#[test]
fn plan_snapshot_chunk_ranges_splits_large_tables_evenly() {
    let ranges = plan_snapshot_chunk_ranges_from_bounds(40_000, 1, 40_000, 1_000, 4);
    assert_eq!(
        ranges,
        vec![
            SnapshotChunkRange {
                start_pk: 1,
                end_pk: 10_000,
            },
            SnapshotChunkRange {
                start_pk: 10_001,
                end_pk: 20_000,
            },
            SnapshotChunkRange {
                start_pk: 20_001,
                end_pk: 30_000,
            },
            SnapshotChunkRange {
                start_pk: 30_001,
                end_pk: 40_000,
            },
        ]
    );
}

#[test]
fn snapshot_chunk_filter_sql_uses_inclusive_upper_bound() {
    assert_eq!(
        snapshot_chunk_filter_sql(
            "\"id\"",
            "int8",
            SnapshotChunkRange {
                start_pk: 10,
                end_pk: 20,
            }
        ),
        "\"id\" >= 10::int8 AND \"id\" <= 20::int8"
    );
}

#[test]
fn snapshot_resume_tasks_skip_completed_chunks_and_keep_saved_progress() {
    let checkpoint = TableCheckpoint {
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
    };

    let tasks = snapshot_resume_tasks_from_checkpoint(&checkpoint).expect("resume tasks");
    assert_eq!(
        tasks,
        vec![(
            Some(SnapshotChunkRange {
                start_pk: 1,
                end_pk: 10,
            }),
            Some("6".to_string()),
        )]
    );
}

#[test]
fn checkpoint_has_material_sync_progress_ignores_schema_only_checkpoints() {
    let schema_only = TableCheckpoint {
        schema_hash: Some("hash-a".to_string()),
        schema_snapshot: Some(vec![SchemaFieldSnapshot {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }]),
        schema_primary_key: Some("id".to_string()),
        ..Default::default()
    };
    assert!(!super::cdc_sync::checkpoint_has_material_sync_progress(
        &schema_only
    ));

    let completed = TableCheckpoint {
        last_synced_at: Some(Utc::now()),
        ..Default::default()
    };
    assert!(super::cdc_sync::checkpoint_has_material_sync_progress(
        &completed
    ));
}

fn test_resolved_table(name: &str) -> ResolvedPostgresTable {
    ResolvedPostgresTable {
        name: name.to_string(),
        primary_key: "id".to_string(),
        updated_at_column: Some("updated_at".to_string()),
        soft_delete: false,
        soft_delete_column: None,
        where_clause: None,
        columns: ColumnSelection {
            include: Vec::new(),
            exclude: Vec::new(),
        },
    }
}

#[test]
fn publication_table_spec_quotes_table_name() {
    let table = test_resolved_table("public.accounts");
    assert_eq!(
        super::PostgresSource::publication_table_spec(&table),
        "\"public\".\"accounts\""
    );
}

#[test]
fn publication_table_spec_includes_row_filter() {
    let mut table = test_resolved_table("public.accounts");
    table.where_clause = Some("tenant_id = 42".to_string());
    assert_eq!(
        super::PostgresSource::publication_table_spec(&table),
        "\"public\".\"accounts\" WHERE (tenant_id = 42)"
    );
}

#[test]
fn publication_table_spec_list_joins_tables() {
    let first = test_resolved_table("public.accounts");
    let mut second = test_resolved_table("public.users");
    second.where_clause = Some("deleted_at is null".to_string());

    assert_eq!(
        super::PostgresSource::publication_table_spec_list(&[first, second]),
        "\"public\".\"accounts\", \"public\".\"users\" WHERE (deleted_at is null)"
    );
}

#[test]
fn initial_snapshot_table_ids_select_new_tables_under_existing_cdc_lsn() {
    let table_accounts = TableId::new(1);
    let table_messages = TableId::new(2);
    let table_users = TableId::new(3);
    let table_ids = HashMap::from([
        (table_accounts, test_resolved_table("public.accounts")),
        (table_messages, test_resolved_table("public.messages")),
        (table_users, test_resolved_table("public.users")),
    ]);
    let mut state = ConnectionState::default();
    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            last_synced_at: Some(Utc::now()),
            ..Default::default()
        },
    );
    state.postgres.insert(
        "public.messages".to_string(),
        TableCheckpoint {
            schema_hash: Some("hash-a".to_string()),
            schema_snapshot: Some(vec![SchemaFieldSnapshot {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            }]),
            schema_primary_key: Some("id".to_string()),
            ..Default::default()
        },
    );

    let initial_tables =
        super::cdc_sync::initial_snapshot_table_ids(&table_ids, &state, Some("0/16B6C50"));
    assert_eq!(initial_tables, HashSet::from([table_messages, table_users]));

    let no_lsn_tables = super::cdc_sync::initial_snapshot_table_ids(&table_ids, &state, None);
    assert!(no_lsn_tables.is_empty());
}

#[test]
fn should_preserve_existing_backlog_for_bootstrap_and_resync_snapshots() {
    let table_accounts = TableId::new(1);
    let table_messages = TableId::new(2);
    let table_ids = HashMap::from([
        (table_accounts, test_resolved_table("public.accounts")),
        (table_messages, test_resolved_table("public.messages")),
    ]);
    let mut state = ConnectionState::default();
    let snapshot_progress_tables = HashSet::new();
    let initial_snapshot_tables = HashSet::from([table_messages]);
    let resync_tables = HashSet::new();

    assert!(super::cdc_sync::should_preserve_existing_backlog(
        &table_ids,
        &state,
        &snapshot_progress_tables,
        &resync_tables,
        &initial_snapshot_tables,
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
    ));

    state.postgres.insert(
        "public.accounts".to_string(),
        TableCheckpoint {
            snapshot_start_lsn: Some("0/200".to_string()),
            snapshot_chunks: vec![SnapshotChunkCheckpoint {
                start_primary_key: None,
                end_primary_key: None,
                last_primary_key: Some("10".to_string()),
                complete: false,
            }],
            ..Default::default()
        },
    );
    let snapshot_progress_tables = super::cdc_sync::snapshot_progress_table_ids(&table_ids, &state);
    assert!(!super::cdc_sync::should_preserve_existing_backlog(
        &table_ids,
        &state,
        &snapshot_progress_tables,
        &resync_tables,
        &initial_snapshot_tables,
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
    ));

    state
        .postgres
        .get_mut("public.accounts")
        .expect("accounts checkpoint")
        .snapshot_preserve_backlog = true;
    assert!(super::cdc_sync::should_preserve_existing_backlog(
        &table_ids,
        &state,
        &snapshot_progress_tables,
        &resync_tables,
        &initial_snapshot_tables,
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
    ));

    let resync_tables = HashSet::from([table_messages]);
    assert!(super::cdc_sync::should_preserve_existing_backlog(
        &table_ids,
        &ConnectionState::default(),
        &HashSet::new(),
        &resync_tables,
        &HashSet::new(),
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
    ));
}

#[test]
fn snapshot_table_ids_keep_resume_bootstrap_scope_narrow() {
    let include_tables = HashSet::from([TableId::new(1), TableId::new(2), TableId::new(3)]);
    let snapshot_progress_tables = HashSet::from([TableId::new(1)]);
    let resync_tables = HashSet::from([TableId::new(2)]);
    let initial_snapshot_tables = HashSet::from([TableId::new(3)]);

    let selected = super::cdc_sync::snapshot_table_ids(
        &include_tables,
        &HashSet::new(),
        &resync_tables,
        &initial_snapshot_tables,
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
        false,
    );
    assert_eq!(selected, HashSet::from([TableId::new(2), TableId::new(3)]));

    let resume_selected = super::cdc_sync::snapshot_table_ids(
        &include_tables,
        &snapshot_progress_tables,
        &resync_tables,
        &initial_snapshot_tables,
        crate::types::SyncMode::Incremental,
        Some("0/16B6C50"),
        true,
    );
    assert_eq!(
        resume_selected,
        HashSet::from([TableId::new(1), TableId::new(2), TableId::new(3)])
    );
}

#[test]
fn snapshot_table_write_plan_truncates_new_bootstrap_tables_on_resume() {
    let plan = super::cdc_sync::snapshot_table_write_plan(
        crate::types::SyncMode::Incremental,
        true,
        false,
        true,
        false,
        false,
    );
    assert!(matches!(plan.write_mode, WriteMode::Append));
    assert!(plan.truncate_before_copy);

    let resumed_progress = super::cdc_sync::snapshot_table_write_plan(
        crate::types::SyncMode::Incremental,
        true,
        true,
        true,
        false,
        false,
    );
    assert!(matches!(resumed_progress.write_mode, WriteMode::Upsert));
    assert!(!resumed_progress.truncate_before_copy);

    let resumed_resync = super::cdc_sync::snapshot_table_write_plan(
        crate::types::SyncMode::Incremental,
        true,
        false,
        false,
        true,
        false,
    );
    assert!(matches!(resumed_resync.write_mode, WriteMode::Append));
    assert!(resumed_resync.truncate_before_copy);
}

#[test]
fn snapshot_task_failure_disposition_retries_bigquery_dml_quota_errors() {
    let err = anyhow::anyhow!(
        "merging staging BigQuery table public__foo into public__bar: Quota exceeded: Your table exceeded quota for total number of dml jobs writing to a table, pending + running"
    );
    assert_eq!(
        super::cdc_sync::snapshot_task_failure_disposition(&err),
        super::cdc_sync::SnapshotTaskFailureDisposition::Retry
    );
}

#[test]
fn snapshot_task_failure_disposition_blocks_non_quota_errors() {
    let err = anyhow::anyhow!("invalid field mapping for BigQuery merge");
    assert_eq!(
        super::cdc_sync::snapshot_task_failure_disposition(&err),
        super::cdc_sync::SnapshotTaskFailureDisposition::Block
    );
}

#[test]
fn snapshot_task_retry_backoff_caps_at_fifteen_minutes() {
    assert_eq!(
        super::cdc_sync::snapshot_task_retry_backoff(1, 1_000),
        Duration::from_secs(1)
    );
    assert_eq!(
        super::cdc_sync::snapshot_task_retry_backoff(6, 1_000),
        Duration::from_secs(32)
    );
    assert_eq!(
        super::cdc_sync::snapshot_task_retry_backoff(20, 1_000),
        Duration::from_secs(15 * 60)
    );
}

#[test]
fn snapshot_task_queue_seed_preserves_blocked_tables_until_manual_resync() {
    let runtime = TableRuntimeState {
        status: TableRuntimeStatus::Blocked,
        attempts: 2,
        last_error: Some("permanent schema mismatch".to_string()),
        next_retry_at: None,
        updated_at: None,
    };

    assert_eq!(
        super::cdc_sync::snapshot_task_queue_seed(Some(&runtime), false),
        super::cdc_sync::SnapshotTaskQueueSeed::Blocked {
            last_error: Some("permanent schema mismatch".to_string())
        }
    );
    assert_eq!(
        super::cdc_sync::snapshot_task_queue_seed(Some(&runtime), true),
        super::cdc_sync::SnapshotTaskQueueSeed::Runnable {
            next_retry_at: None
        }
    );
}

#[test]
fn snapshot_task_queue_seed_keeps_retry_schedule_for_retrying_tables() {
    let retry_at = Utc::now();
    let runtime = TableRuntimeState {
        status: TableRuntimeStatus::Retrying,
        attempts: 4,
        last_error: Some("quota exceeded".to_string()),
        next_retry_at: Some(retry_at),
        updated_at: Some(retry_at),
    };

    assert_eq!(
        super::cdc_sync::snapshot_task_queue_seed(Some(&runtime), false),
        super::cdc_sync::SnapshotTaskQueueSeed::Runnable {
            next_retry_at: Some(retry_at)
        }
    );
}

#[test]
fn snapshot_task_requires_table_serialization_only_for_upserts() {
    assert!(!super::cdc_sync::snapshot_task_requires_table_serialization(WriteMode::Append));
    assert!(super::cdc_sync::snapshot_task_requires_table_serialization(
        WriteMode::Upsert
    ));
}

#[test]
fn should_reset_snapshot_runtime_for_fresh_or_manual_snapshots() {
    assert!(super::cdc_sync::should_reset_snapshot_runtime(false, false));
    assert!(super::cdc_sync::should_reset_snapshot_runtime(true, true));
    assert!(!super::cdc_sync::should_reset_snapshot_runtime(true, false));
}

#[test]
fn should_clear_snapshot_runtime_only_after_table_fully_drains() {
    assert!(!super::cdc_sync::should_clear_snapshot_runtime_on_success(
        true, 0, false
    ));
    assert!(!super::cdc_sync::should_clear_snapshot_runtime_on_success(
        false, 1, false
    ));
    assert!(!super::cdc_sync::should_clear_snapshot_runtime_on_success(
        false, 0, true
    ));
    assert!(super::cdc_sync::should_clear_snapshot_runtime_on_success(
        false, 0, false
    ));
}

#[test]
fn manual_resync_request_overrides_incompatible_startup_schema_checks() {
    let mut diff = SchemaDiff::default();
    diff.type_changed
        .push(("name".to_string(), DataType::String, DataType::Int64));
    assert!(super::cdc_sync::cdc_startup_requires_manual_resync(
        true,
        Some(&diff),
        false
    ));
    assert!(super::cdc_sync::cdc_startup_requires_manual_resync(
        true, None, true
    ));
    assert!(!super::cdc_sync::cdc_startup_requires_manual_resync(
        false,
        Some(&diff),
        false
    ));
}

#[test]
fn cdc_slot_name_defaults_to_connection_scoped_name() {
    let slot_name = super::cdc_sync::cdc_slot_name("app_staging", None).expect("slot name");
    assert_eq!(slot_name, "cdsync_app_staging_cdc");
}

#[test]
fn cdc_slot_name_sanitizes_connection_id() {
    let slot_name = super::cdc_sync::cdc_slot_name("App/Staging Public", None).expect("slot name");
    assert_eq!(slot_name, "cdsync_app_staging_public_cdc");
}

#[test]
fn cdc_slot_name_honors_explicit_pipeline_id_override() {
    let slot_name = super::cdc_sync::cdc_slot_name("ignored", Some(1101)).expect("slot name");
    assert_eq!(slot_name, "supabase_etl_apply_1101");
}

#[test]
fn build_select_columns_casts_string_columns_to_text() {
    let schema = TableSchema {
        name: "public.example".to_string(),
        columns: vec![
            ColumnSchema {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnSchema {
                name: "search_vector".to_string(),
                data_type: DataType::String,
                nullable: true,
            },
        ],
        primary_key: Some("id".to_string()),
    };

    assert_eq!(
        build_select_columns(&schema),
        "\"id\", \"search_vector\"::text as \"search_vector\""
    );
}

#[test]
fn build_select_columns_projects_interval_as_epoch_seconds() {
    let schema = TableSchema {
        name: "public.example".to_string(),
        columns: vec![ColumnSchema {
            name: "elapsed".to_string(),
            data_type: DataType::Interval,
            nullable: true,
        }],
        primary_key: Some("id".to_string()),
    };

    assert_eq!(
        build_select_columns(&schema),
        "extract(epoch from \"elapsed\") as \"elapsed\""
    );
}

#[test]
fn build_select_columns_projects_json_columns_via_to_json() {
    let schema = TableSchema {
        name: "public.example".to_string(),
        columns: vec![ColumnSchema {
            name: "tags".to_string(),
            data_type: DataType::Json,
            nullable: true,
        }],
        primary_key: Some("id".to_string()),
    };

    assert_eq!(
        build_select_columns(&schema),
        "to_json(\"tags\")::text as \"tags\""
    );
}

#[test]
fn parse_postgres_interval_to_seconds_handles_postgres_style_values() {
    assert_eq!(
        parse_postgres_interval_to_seconds("3 days 04:05:06.5").expect("interval"),
        273_906.5
    );
    assert_eq!(
        parse_postgres_interval_to_seconds("1 mon").expect("interval"),
        2_592_000.0
    );
    assert_eq!(
        parse_postgres_interval_to_seconds("-01:30:00").expect("interval"),
        -5_400.0
    );
}

#[test]
fn parse_text_cell_parses_supported_arrays_and_jsonish_ranges() {
    match parse_text_cell(&etl::types::Type::INT4_ARRAY, "{1,NULL,3}") {
        Cell::Array(etl::types::ArrayCell::I32(values)) => {
            assert_eq!(values, vec![Some(1), None, Some(3)]);
        }
        other => panic!("unexpected array cell: {:?}", other),
    }

    match parse_text_cell(&etl::types::Type::INT4_RANGE, "[1,4)") {
        Cell::Json(Value::String(value)) => assert_eq!(value, "[1,4)"),
        other => panic!("unexpected range cell: {:?}", other),
    }
}

fn test_state_config() -> Option<crate::config::StateConfig> {
    let url = std::env::var("CDSYNC_E2E_PG_URL").ok()?;
    Some(crate::config::StateConfig {
        url,
        schema: Some(format!(
            "cdsync_state_snapshot_test_{}",
            Uuid::new_v4().simple()
        )),
    })
}

#[tokio::test]
async fn save_snapshot_progress_persists_checkpoint_updates() -> anyhow::Result<()> {
    let Some(config) = test_state_config() else {
        return Ok(());
    };
    crate::state::SyncStateStore::migrate_with_config(&config).await?;
    let store = crate::state::SyncStateStore::open_with_config(&config).await?;
    let handle = store.handle("app");

    let checkpoint_state = Arc::new(Mutex::new(TableCheckpoint {
        snapshot_start_lsn: Some("0/ABC".to_string()),
        snapshot_chunks: vec![SnapshotChunkCheckpoint {
            start_primary_key: Some("1".to_string()),
            end_primary_key: Some("10".to_string()),
            last_primary_key: None,
            complete: false,
        }],
        ..Default::default()
    }));

    save_snapshot_progress(
        &checkpoint_state,
        "public.accounts",
        Some(SnapshotChunkRange {
            start_pk: 1,
            end_pk: 10,
        }),
        Some("6".to_string()),
        false,
        Some(&handle),
    )
    .await?;

    let checkpoint = handle
        .load_postgres_checkpoint("public.accounts")
        .await?
        .expect("checkpoint persisted");
    assert_eq!(checkpoint.snapshot_start_lsn.as_deref(), Some("0/ABC"));
    assert_eq!(checkpoint.snapshot_chunks.len(), 1);
    assert_eq!(
        checkpoint.snapshot_chunks[0].last_primary_key.as_deref(),
        Some("6")
    );
    assert!(!checkpoint.snapshot_chunks[0].complete);
    assert!(checkpoint.last_synced_at.is_some());

    save_snapshot_progress(
        &checkpoint_state,
        "public.accounts",
        Some(SnapshotChunkRange {
            start_pk: 1,
            end_pk: 10,
        }),
        Some("10".to_string()),
        true,
        Some(&handle),
    )
    .await?;

    let checkpoint = handle
        .load_postgres_checkpoint("public.accounts")
        .await?
        .expect("checkpoint persisted");
    assert_eq!(
        checkpoint.snapshot_chunks[0].last_primary_key.as_deref(),
        Some("10")
    );
    assert!(checkpoint.snapshot_chunks[0].complete);
    Ok(())
}

#[tokio::test]
async fn finalize_snapshot_copy_progress_skips_completion_when_interrupted() -> anyhow::Result<()> {
    let checkpoint_state = Arc::new(Mutex::new(TableCheckpoint {
        snapshot_start_lsn: Some("0/ABC".to_string()),
        snapshot_chunks: vec![SnapshotChunkCheckpoint {
            start_primary_key: Some("1".to_string()),
            end_primary_key: Some("10".to_string()),
            last_primary_key: Some("6".to_string()),
            complete: false,
        }],
        ..Default::default()
    }));

    let completed = super::snapshot_sync::finalize_snapshot_copy_progress(
        &checkpoint_state,
        "public.accounts",
        Some(SnapshotChunkRange {
            start_pk: 1,
            end_pk: 10,
        }),
        Some("10".to_string()),
        super::snapshot_sync::SnapshotCopyExit::Interrupted,
        None,
    )
    .await?;

    assert!(!completed);
    let checkpoint = checkpoint_state.lock().await.clone();
    assert_eq!(
        checkpoint.snapshot_chunks[0].last_primary_key.as_deref(),
        Some("6")
    );
    assert!(!checkpoint.snapshot_chunks[0].complete);
    Ok(())
}

#[derive(Default)]
struct RecordingDestination {
    write_modes: Mutex<Vec<WriteMode>>,
}

#[async_trait]
impl Destination for RecordingDestination {
    async fn ensure_table(&self, _schema: &TableSchema) -> anyhow::Result<()> {
        Ok(())
    }

    async fn truncate_table(&self, _table: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_batch(
        &self,
        _table: &str,
        _schema: &TableSchema,
        _frame: &DataFrame,
        mode: WriteMode,
        _primary_key: Option<&str>,
    ) -> anyhow::Result<()> {
        self.write_modes.lock().await.push(mode);
        Ok(())
    }
}

#[tokio::test]
async fn write_snapshot_batch_uses_requested_write_mode() -> anyhow::Result<()> {
    let dest = RecordingDestination::default();
    let schema = TableSchema {
        name: "public__accounts".to_string(),
        columns: vec![ColumnSchema {
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        primary_key: Some("id".to_string()),
    };
    let batch = DataFrame::new(vec![Series::new("id".into(), &[1_i64]).into()])?;

    super::snapshot_sync::write_snapshot_batch(&dest, &schema, &batch, WriteMode::Upsert, "id")
        .await?;

    assert!(matches!(
        dest.write_modes.lock().await.as_slice(),
        [WriteMode::Upsert]
    ));
    Ok(())
}
