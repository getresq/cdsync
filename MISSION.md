# Mission

Mission: Replace Fivetran with CDSync by building a production-grade PostgreSQL to BigQuery replicator that is fast, observable, and safe under real staging and production workloads.

## Done Criteria

1. Phase 1 is complete:
   - bulk/initial-load BigQuery writes use a batch-native path instead of `insertAll` or committed Storage Write as the primary mechanism
   - staging validates and runs through the new batch path successfully
2. Phase 2 is complete:
   - initial snapshot architecture supports meaningful parallelism across tables or chunks
   - concurrency is bounded and observable
3. Phase 3 is complete:
   - extract, transform, stage, and load are pipelined with backpressure instead of largely serialized execution
4. Phase 4 is complete:
   - steady-state CDC is tuned and instrumented
   - retry, lag, throttling, and load behavior are measurable and explainable
5. Staging runbooks and generated config artifacts exist for both app and nora sources.

## Guardrails

- Do not regress correctness or resumability for checkpoints and CDC state.
- Keep clippy and tests passing.
- Prefer hard breaks over carrying dead fallback paths unless there is a clear transitional reason.
- Use staging to prove behavior before broadening to full production scope.
- Keep secrets out of git; only document stable references and parameter names/ARNs.

## Phases

### Phase 1: Batch-Load Destination Path

Goal:
- replace bulk append/full-refresh destination writes with a GCS-backed BigQuery load-job path

Primary work:
- add GCS batch-load config
- upload NDJSON/Parquet batch files to GCS
- run BigQuery load jobs and poll to completion
- validate on staging one-off ECS task

### Phase 2: Parallel Snapshot Architecture

Goal:
- make initial snapshots materially faster by parallelizing across tables and then, if needed, across chunks

Primary work:
- split snapshot scheduling from live CDC follow
- run multiple snapshot workers with bounded concurrency
- separate small-table and large-table strategies

### Phase 3: Pipelined Extract / Transform / Load

Goal:
- turn the current mostly serialized bulk path into a bounded pipeline

Primary work:
- producer/consumer queues between extraction, transformation, staging, and load
- explicit backpressure and queue metrics
- overlapping extract and load instead of strict stepwise execution

### Phase 4: Steady-State CDC Tuning And Backpressure

Goal:
- optimize the always-on CDC path for throughput, recovery, and operator visibility

Primary work:
- tune batch sizes and fill timers
- add lag, retry, throttle, and merge/load-job metrics
- define destination-side throttling behavior and safe fallbacks

## Critical Learnings

- Decision: Use GCS-backed BigQuery load jobs as the long-term bulk-write direction; do not treat `insertAll` as the real production path.
- Decision: Keep primary-key auto-discovery as the default, with explicit per-table overrides only where needed.
- Decision: Parallel CDC snapshots will use a dedicated exported-snapshot slot connection plus multiple imported-snapshot readers, while WAL follow remains a single ordered stream.
- Decision: Runtime state and reporting storage are now Postgres-backed; SQLite is removed from the compiled runtime path.
- Decision: Control-plane DDL now belongs to an explicit `cdsync migrate` flow; runtime startup should not issue Postgres migrations in environments with DDL audit triggers.
- Constraint: The local tunnel path is awkward for logical replication TLS because hostname validation wants the real RDS hostname; staging validation is cleaner from inside ECS/VPC.
- Constraint: The current GCP service account can write to `nora-461013` and now has enough access for the staging GCS bucket, but dataset creation had to be handled separately.
- Constraint: WAL apply parallelism must preserve commit/LSN ordering; do not use unordered multi-reader CDC.
- Constraint: The staging source database has an `awsdms_intercept_ddl` event trigger that writes to `public.awsdms_ddl_audit`; runtime users without DDL-audit privileges cannot safely run migrations on startup.
- Constraint: Exported Postgres snapshots are only importable while the creating transaction stays open; the slot-creation connection must hold that transaction open until snapshot readers have finished importing it.
- Validation: Staging primary PostgreSQL is CDC-ready and `cdsync_staging_pub` plus the `cdsync_staging` role are in place.
- Validation: One-off ECS runs in `cluster-staging` can connect to the staging writer with TLS and begin CDC snapshot work.
- Validation: The Postgres-backed control-plane schemas were precreated on staging, with `_sqlx_migrations`, `cdsync_state.*`, and `cdsync_stats.*` tables created and granted to `cdsync_staging`.
- Blocker: The first real staging destination bottleneck was BigQuery Storage Write throttling on large snapshot batches.
- Blocker: BigQuery load jobs can fail if we model source `NOT NULL` columns as destination `REQUIRED`, because CDC/upsert batches can still surface `NULL` values for those fields.
- Blocker: The exported-snapshot snapshot path was aborting because runtime logs hid the root cause; richer error chains now show the real failure source.
- Blocker: Snapshot row decoding still assumed `INT4` columns could be read directly as Rust `i64`; the current local fix adds `i32`/`i16` fallback casting, but it still needs to be staged.
- Blocker: Snapshot reads also hit Postgres string-like types such as `tsvector`; selecting all `DataType::String` columns as `::text` is the current hardening approach for snapshot/polling SQL.
- Validation: The scrubbed admin API config must redact `observability.otlp_headers`; `/v1/config` is not safe if it clones those headers verbatim.
- Decision: Use the existing `cdsync_e2e_real` dataset temporarily for staging one-off runs until a dedicated staging shadow dataset is provisioned.
- Validation: The exported-snapshot parallel snapshot implementation compiles cleanly and passes `cargo clippy --all-targets --all-features -- -D warnings` plus `cargo test`.
