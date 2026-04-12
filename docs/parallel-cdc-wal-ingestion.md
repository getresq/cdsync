# Parallel CDC WAL Ingestion

This note records the compatible WAL-ingestion step before the full durable
frontier redesign.

## Current Constraint

Today the WAL reader is coupled to downstream apply completion through the
in-memory coordinator. The reader assigns commit sequences, dispatches table
batches, and starts draining completed work once in-flight commits reaches the
read-ahead cap.

Historically that cap was hardcoded as:

```text
max_commit_queue_depth = cdc_apply_concurrency * 4
```

With the current production `cdc_apply_concurrency = 4`, the WAL reader starts
backpressuring around 16 in-flight commits.

## Compatible Change

The current read-ahead cap is now explicit:

```toml
cdc_max_inflight_commits = 32
```

If omitted, it keeps the previous behavior:

```text
cdc_max_inflight_commits = cdc_apply_concurrency * 4
```

This is not the final durable backlog budget. It is a compatibility-preserving
control for the existing in-memory coordinator path.

## Why Not Fully Split Yet

The WAL loop cannot safely read arbitrarily far ahead until WAL feedback is
driven by a durable applied-fragment frontier. Otherwise the in-memory
coordinator could accumulate unbounded commit state, and process death could
still lose completion knowledge.

The full split requires later checklist work:

- durable staging/load/reducer states
- durable contiguous frontier calculation
- state-based backpressure by rows, bytes, fragments, and oldest backlog age
- restart-safe status update to PostgreSQL based on durable frontier state

## Future Target

The future WAL ingestor should:

- parse logical replication changes from the single ordered WAL stream
- assign sequence and commit LSN metadata
- persist received fragments quickly
- stop waiting for BigQuery worker completion directly
- enforce durable backlog budgets instead of a small in-memory commit-count cap
