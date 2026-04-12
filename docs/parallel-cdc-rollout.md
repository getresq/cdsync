# Parallel CDC Rollout

The parallel CDC work is controlled per connection through conservative knobs.

Primary rollout gate:

- `cdc_batch_load_reducer_enabled: false` forces queued CDC apply back to
  single-job windows. This disables reducer coalescing while keeping the
  additive durable ledger and staging/load fixes in place.

Capacity knobs:

- `cdc_batch_load_staging_worker_count`: staging/load worker pool size
- `cdc_batch_load_reducer_worker_count`: apply/reducer worker pool size
- `cdc_batch_load_reducer_max_jobs`: max loaded jobs per reducer window
- `cdc_max_inflight_commits`: current in-memory WAL read-ahead cap
- `cdc_backlog_max_pending_fragments`: optional durable backlog safety cap
- `cdc_backlog_max_oldest_pending_seconds`: optional durable backlog age cap

Recommended deployment sequence:

1. Run migrations before starting the new binary.
2. Deploy staging with `cdc_batch_load_reducer_enabled: false` and the old
   effective worker counts. Set `cdc_batch_load_staging_worker_count` and
   `cdc_batch_load_reducer_worker_count` explicitly; if they are omitted, both
   pools fall back to `cdc_batch_load_worker_count`, which can roughly double
   queued CDC worker tasks on upgrade.
3. Verify CDC health with actual movement: admin progress, pending fragments,
   next sequence to ack, WAL gap, Datadog logs, and BigQuery job completion.
4. Enable reducer coalescing in staging with a small
   `cdc_batch_load_reducer_max_jobs` such as `4`.
5. Increase staging and reducer worker counts separately while watching queue
   age, load-job latency, MERGE latency, and BigQuery quota errors.
6. Roll production with the same sequence: gate off first, enable small
   windows, then raise capacity only when frontier movement is healthy.

Rollback levers:

- Set `cdc_batch_load_reducer_enabled: false` to stop coalesced reducer MERGEs.
- Lower `cdc_batch_load_staging_worker_count` if load jobs or GCS upload are
  saturating.
- Lower `cdc_batch_load_reducer_worker_count` or
  `cdc_batch_load_reducer_max_jobs` if BigQuery DML quota/backpressure appears.
- Lower `cdc_max_inflight_commits` or set durable backlog caps if source WAL
  read-ahead is outrunning the destination.

Do not judge a rollout only by ECS/task health. Confirm that the CDC frontier is
advancing and that the source slot confirmed-flush LSN is moving.
