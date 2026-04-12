# Parallel CDC Staging Strategy

This document records the staging-table decision for the parallel CDC redesign.

## Decision

Keep the current runtime on `per_job` staging tables until the durable
staging/reducer pipeline is implemented.

For the new parallel pipeline, target persistent per-table staging tables with an
optional shard suffix when a single hot table needs more staging/load fanout.

## Why Not Switch Current Runtime Immediately

The current CDC batch-load path creates one staging table per queued job step,
loads that job step into the staging table, then merges the whole staging table
into the target table.

That shape is safe with per-job staging because the MERGE source contains only
the rows for that job step.

Switching the current runtime to a shared persistent staging table would be
incorrect without reducer metadata and reducer filtering. A current MERGE would
read unrelated staged rows and could apply rows from the wrong sequence window.

## Current Compatible Strategy

Current runtime strategy:

- `per_job`
- one staging table per job step
- best-effort async staging-table cleanup after MERGE
- scalar ledger columns such as `staging_table` and `artifact_uri` are first-step
  hints
- complete step metadata is recorded in `ledger_metadata_json.steps`

This preserves behavior while making the staging strategy explicit in durable
metadata.

## Target Parallel Strategy

Target runtime strategy:

- persistent per-table staging table by default
- optional persistent per-table shard, for example by primary-key hash lane, only
  after table-level reducer correctness is proven
- reducer metadata columns in staging rows, including sequence, commit LSN,
  transaction ordinal, operation kind, and delete metadata
- reducer MERGE reads a bounded table/window/lane from staging and deduplicates by
  primary key using the newest sequence and transaction ordinal
- cleanup deletes or expires applied staging rows by connection/table/window

## Requirements

- Every staged row must be attributable to a connection, table, sequence, and
  operation.
- `TRUNCATE`, relation/schema changes, snapshot handoff, blocked table state, and
  manual resync remain table-wide barriers.
- Primary-key hash lanes are optional and must not allow table-wide barriers to
  be bypassed.
- BigQuery table metadata churn must be bounded; creating one staging table per
  small job is not the desired high-throughput shape.
- Reducer queries must stay inside BigQuery mutating DML limits and should prefer
  fewer larger MERGE windows over many tiny MERGEs.
