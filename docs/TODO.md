# CDSync v1 TODO

## 0. Project Setup
- [x] Define crate dependencies and feature flags (tokio, clap, serde, yaml, tracing, sqlx, reqwest, retry, secrecy).
- [x] Enable overflow checks in release builds in `Cargo.toml`.
- [x] Add CI linting and test workflow.

## 1. Core Architecture
- [x] Define core traits: `Destination`.
- [x] Define shared data model: `TableSchema`, `RowBatch`, `SyncMode`.
- [x] Implement type mapping utilities (Postgres -> BigQuery).
- [x] Implement pipeline orchestration: discover → extract → transform → load.

## 2. Config + State
- [x] Implement YAML config schema with connections, include/exclude selection, and sync options.
- [x] Implement durable SQLite-backed state storage with connection locking and per-table checkpoints.
- [x] Implement config validation and `cdsync init` (connection check + schema preview).

## 3. Sources
### Postgres (AWS RDS Read Replica)
- [x] Connect via URL with TLS; discover schema via `information_schema`.
- [x] Full refresh: batched `SELECT *`.
- [x] Incremental polling fallback: `updated_at > last_synced` (table-configurable).
- [x] Handle soft deletes if delete detection is available; otherwise skip.
- [x] Row filtering support (optional WHERE clause).
- [x] CDC streaming as default (logical replication).

## 4. Destination
### BigQuery
- [x] Auth via service account JSON or env.
- [x] Auto-create/alter tables based on source schema.
- [x] Add metadata columns `_cdsync_synced_at`, `_cdsync_deleted_at`.
- [x] Make metadata column names configurable.
- [x] Upsert load path (MERGE on primary key); streaming insert for incremental batches.
- [x] Optional partitioning by `_cdsync_synced_at` (configurable).

## 5. CLI
- [x] `cdsync init --config=...` (validate + generate template).
- [x] `cdsync sync --config=... --full|--incremental [--dry-run]`.
- [x] `cdsync status --config=...` (read state file).
- [x] `cdsync validate --config=... [--connection ...]` (publication row filters).
- [x] Logging (JSON/console) and verbosity flags.

## 6. Reliability
- [x] Retry policy with exponential backoff.
- [x] Checkpoint after each batch/commit.
- [x] Idempotent load with primary keys.
- [x] Long-running CDC follow mode with retry supervision.
- [x] Fail syncs on BigQuery row-level insert errors.

## 7. Tests
- [x] Unit tests for type mapping and config parsing.
- [x] Integration tests with dockerized Postgres and BigQuery emulator.
- [x] Real BigQuery heavy polling/incremental integration test.
- [x] Real BigQuery heavy CDC integration test.

## 8. Reporting + Observability
- [x] Add `report` command for recent run history.
- [x] Add `reconcile` command for source vs destination row-count checks.
- [x] Add OTLP metrics/tracing support.
