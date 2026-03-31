use super::snapshot_sync::save_snapshot_progress;
use super::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[test]
fn maps_pg_types_to_internal_types() {
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INT4),
        DataType::Int64
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::FLOAT8),
        DataType::Float64
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::BOOL),
        DataType::Bool
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::TIMESTAMPTZ),
        DataType::Timestamp
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::TIME),
        DataType::Time
    );
    assert_eq!(
        pg_type_to_data_type_from_type(&etl::types::Type::INTERVAL),
        DataType::Interval
    );
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
