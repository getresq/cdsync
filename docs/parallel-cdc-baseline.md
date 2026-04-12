# Parallel CDC Baseline

This is a baseline of the current CDC-to-BigQuery data plane before the durable
fanout/fanin redesign. It is intentionally descriptive, not aspirational.

## Current Hot Path

1. `stream_cdc_changes` reads one PostgreSQL logical replication stream and
   assigns a monotonically increasing commit sequence from durable
   `next_sequence_to_ack`.
   - Code: `src/sources/postgres/cdc_runtime.rs`
   - Key state: `next_commit_sequence`, `queued_batches`, `pending_table_batches`,
     `inflight_dispatch`, `inflight_apply`

2. On each commit, the WAL reader splits the commit into table batches, queues a
   `CommittedCdcBatch`, and immediately tries to dispatch work.
   - Code: `src/sources/postgres/cdc_runtime.rs`
   - Dispatch call: `dispatch_cdc_batches_and_record(...)`

3. `dispatch_cdc_batches` registers each commit with the in-memory coordinator
   and buffers rows per table. A table becomes ready when forced, when row count
   reaches `cdc_batch_size`, or when `cdc_max_fill_ms` elapses.
   - Code: `src/sources/postgres/cdc_pipeline.rs`
   - Current behavior: batching is table-local, but commit frontier tracking is
     in-memory while the run is alive.

4. The dispatcher starts table work only while `inflight_dispatch.len() <
   cdc_apply_concurrency`. It also skips a table already present in
   `active_table_applies`.
   - Code: `src/sources/postgres/cdc_pipeline.rs`
   - Production config: `cdc_apply_concurrency = 4`

5. The dispatch future takes a per-table async lock before calling
   `dispatch_table_events`. For queued BigQuery batch-load work, this lock covers
   dispatch/enqueue, not the full downstream BigQuery application after the job is
   deferred.
   - Code: `src/sources/postgres/cdc_pipeline.rs`

6. `dispatch_table_events` compacts events for a single table and either writes
   them directly or creates a durable CDC batch-load job and returns an in-memory
   oneshot receiver that resolves when the job finishes.
   - Code: `src/destinations/etl_bigquery.rs`
   - Current durable state: `cdc_batch_load_jobs`, `cdc_commit_fragments`

7. `CdcBatchLoadManager` starts a fixed worker pool. Workers claim durable jobs
   from the state DB with `FOR UPDATE SKIP LOCKED`, then process BigQuery work.
   Same-table job claiming already blocks newer jobs behind older pending,
   failed, or non-stale running jobs for that `table_key`. Stale running jobs are
   intentionally reclaimable.
   - Code: `src/destinations/etl_bigquery/queue.rs`
   - Claim SQL: `src/state/cdc.rs`
   - Production config: `cdc_batch_load_worker_count = 8`

8. BigQuery batch-load processing currently does per-job staging:
   - ensure target table
   - optionally truncate target table
   - ensure a per-job staging table
   - submit and poll a BigQuery load job into staging
   - run a BigQuery `MERGE` into the target table
   - spawn async best-effort staging-table cleanup
   - Code: `src/destinations/bigquery.rs`,
     `src/destinations/bigquery/batch_load.rs`

9. `merge_staging` serializes MERGEs with an in-memory per-target-table semaphore.
   This is separate from both SQL job-claim ordering and the table dispatch lock.
   - Code: `src/destinations/bigquery.rs`

10. The in-memory coordinator advances the WAL feedback frontier only when all
    fragments for contiguous commit sequences have acknowledged.
    - Code: `src/sources/postgres/cdc_pipeline.rs`
    - Feedback persistence: `cdc_feedback_state`

Production uses the queued BigQuery batch-load path because the production
destination config sets `batch_load_bucket`. When the queue is disabled, CDC
falls back to the direct path in `flush_pending`/`write_batch`, where
`apply_concurrency` bounds concurrent table writes and the destination writes
straight through `write_batch` rather than durable queue workers.

## Current Production Config

From `resq-fullstack/deploy/cdsync/config/cdsync-app.production.template.toml`:

- `cdc_batch_size = 5000`
- `cdc_apply_concurrency = 4`
- `cdc_batch_load_worker_count = 8`
- `cdc_max_fill_ms = 2000`
- `cdc_max_pending_events = 100000`
- `cdc_idle_timeout_seconds = 10`

With `cdc_apply_concurrency = 4`, current `max_commit_queue_depth` is
`4 * cdc_apply_concurrency = 16`. The WAL reader starts draining one unit of CDC
work synchronously once coordinator in-flight commits reaches that cap.

## Serialization And Backpressure Points

- Single logical replication stream.
  - Essential: one replication slot gives ordered WAL input.
  - Not necessarily essential: tying WAL read-ahead to downstream BigQuery
    completion via a small in-memory commit queue.

- `max_commit_queue_depth = cdc_apply_concurrency * 4`.
  - Current accidental brake: production starts backpressuring around 16 in-flight
    commits.
  - Better shape: durable backlog budget by bytes/rows/fragments/oldest age.

- `cdc_apply_concurrency` dispatch cap.
  - Current brake: limits table dispatch/enqueue work.
  - Not the only BigQuery cap; batch-load workers and per-table merge locks are
    separate.

- `active_table_applies` and table apply locks.
  - Current purpose: avoid overlapping dispatch/enqueue for a table.
  - In queued mode, this does not cover the full BigQuery job lifetime.

- SQL same-table job blocker.
  - Current durable correctness guard: newer jobs for a `table_key` wait behind
    older pending, non-stale running, or failed jobs.
  - Future lane-aware design must keep table-wide barriers global across lanes.

- Per-target BigQuery merge semaphore.
  - Current reducer guard: only one MERGE per destination table in-process.
  - Essential idea: BigQuery per-table mutating DML concurrency must be bounded.
  - Not essential shape: one tiny MERGE per CDC job.

- BigQuery `run_query` completion semantics.
  - Current bug/architecture smell: `jobs.query` responses are checked for
    `errors`, but `job_complete` is not checked or polled.
  - Fix before increasing parallelism.

## Metrics Inventory

Existing useful metrics:

- Generic sync signals:
  - `cdsync_rows_read_total`
  - `cdsync_rows_written_total`
  - `cdsync_checkpoint_saves_total`
  - `cdsync_retry_attempts_total`
  - `cdsync_bigquery_row_errors_total`
  - `cdsync_connection_checkpoint_age_seconds`

- WAL/CDC loop depth:
  - `cdsync_cdc_pending_events`
  - `cdsync_cdc_commit_queue_depth`
  - `cdsync_cdc_inflight_commits`
  - `cdsync_cdc_backpressure_waits_total`

- Batch-load job outcomes and stage latency:
  - `cdsync_cdc_batch_load_jobs_total`
  - `cdsync_cdc_batch_load_job_duration_ms`
  - `cdsync_cdc_batch_load_stage_duration_ms`

- Batch-load queue summary:
  - `cdsync_cdc_batch_load_pending_jobs`
  - `cdsync_cdc_batch_load_running_jobs`
  - `cdsync_cdc_batch_load_failed_jobs`
  - `cdsync_cdc_batch_load_oldest_pending_age_seconds`
  - `cdsync_cdc_batch_load_oldest_running_age_seconds`
  - `cdsync_cdc_batch_load_jobs_per_minute`
  - `cdsync_cdc_batch_load_rows_per_minute`

- Coordinator/frontier summary:
  - `cdsync_cdc_coordinator_pending_fragments`
  - `cdsync_cdc_coordinator_failed_fragments`
  - `cdsync_cdc_coordinator_oldest_pending_fragment_age_seconds`
  - `cdsync_cdc_coordinator_next_sequence_to_ack`
  - `cdsync_cdc_coordinator_last_enqueued_sequence`
  - `cdsync_cdc_unattributed_wal_gap_bytes`

Gaps to close before or during the redesign:

- WAL read rate by rows/transactions/bytes per second.
- Durable enqueue rate and enqueue latency.
- Artifact bytes, frame-build rows/bytes, and GCS upload latency.
- BigQuery load-job counts and latency as durable job lifecycle metrics.
- Reducer window size, reducer rows/fragments per MERGE, and reducer lag.
- Per-table queue age and backlog distribution beyond top-N summaries.
- Frontier lag in both sequences and WAL bytes.
- BigQuery quota/backpressure classification by operation type.
- Job claim/reclaim churn: claim attempts, claim failures, stale running reclaims,
  retry requeues, and retry classes by table.

## Baseline Conclusion

The current code has some parallel work, but it is not an embarrassingly parallel
data plane. The highest-value redesign is not simply raising worker counts. It
is decoupling WAL ingestion from BigQuery completion through a durable ledger,
parallelizing staging/load work, choosing a staging-table strategy that avoids
runaway table metadata churn, and reducing each table through controlled, larger
MERGE windows that advance a durable contiguous frontier.
