# Parallel CDC Benchmark Harness

Use `scripts/run-cdc-benchmark.sh` to generate a repeatable CDC workload against
Postgres while a CDSync follow runner is active. The harness creates many
published tables, seeds rows, optionally starts a runner command, applies mixed
CDC changes, and writes workload timing JSON.

Example:

```bash
scripts/run-cdc-benchmark.sh \
  --pg-url "$CDSYNC_E2E_PG_URL" \
  --publication cdsync_bench_pub \
  --table-prefix cdsync_bench \
  --tables 32 \
  --rows 5000 \
  --rounds 20 \
  --hot-keys 100 \
  --insert-batch 500 \
  --schema-every 5 \
  --truncate-every 10 \
  --runner-cmd 'cargo run -- run --config /tmp/cdsync-bench.yaml --connection bench --follow' \
  --summary-path /tmp/cdsync-cdc-benchmark-summary.json
```

Workload coverage:

- many tables, to validate cross-table staging and reducer parallelism
- hot keys on every table, to exercise reducer deduplication
- repeated inserts and soft deletes, to exercise mixed mutation shape
- optional schema changes, to exercise table-local barriers
- optional truncates on table 0, to exercise hard barrier handling
- separate SQL statements in autocommit mode, so CDC sees many commits rather
  than one synthetic transaction

Record these measurements per run:

- workload-side duration and estimated event counts from the summary JSON
- CDSync admin CDC stats: pending fragments, queue age, jobs/min, rows/min,
  next sequence to ack, and WAL gap
- Datadog/log spans for staging load-job latency and reducer MERGE latency
- BigQuery job counts and DML quota/backpressure responses
- source slot confirmed-flush movement before, during, and after the workload

Compare runs by keeping table count, rows per table, round count, runner config,
BigQuery dataset, and source database class constant. Then vary one setting at a
time, such as staging workers, reducer workers, reducer window size, or durable
backlog caps.
