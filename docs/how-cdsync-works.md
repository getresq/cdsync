# How CDSync Works

This is the current runtime model, not a future plan.

## Core Model

CDSync keeps a BigQuery dataset aligned with selected PostgreSQL tables. It does that in two stages:

1. take a snapshot of the current table contents
2. keep applying later changes

For polling connections, later changes come from repeated `SELECT` queries. For CDC connections, later changes come from PostgreSQL logical replication.

## Runtime Modes

`cdsync run --once ...` executes a single sync pass and exits.

`cdsync run ...` is the long-lived worker mode:

- polling connections run on their configured `schedule.every`
- CDC connections stay attached to the WAL and keep following changes

## First CDC Run

For a CDC connection with no saved LSN yet, CDSync:

1. loads config and validates the publication
2. resolves the configured table set
3. snapshots the selected tables into BigQuery
4. preserves the matching WAL backlog
5. drains queued CDC work
6. persists the last safe LSN

That gives you a consistent initial copy plus all changes that happened while the snapshot was running.

## Resume Behavior

Once a CDC connection has a saved LSN, CDSync resumes from that position instead of starting over.

The important state lives in the configured `state` schema, including:

- per-table checkpoints
- CDC slot metadata
- queued batch-load/coordinator state for the BigQuery batch-load path

If that state is durable, restart and task replacement should resume cleanly.

## Polling Versus CDC

Polling mode:

- uses `updated_at_column` plus primary-key paging
- is simpler to operate
- cannot see hard deletes unless they are modeled as soft deletes

CDC mode:

- reads inserts, updates, and deletes from PostgreSQL logical replication
- has lower latency
- needs a publication, replication slot state, and durable checkpoints

## Config Changes

CDSync does not hot-reload config files. Apply config changes by restarting `cdsync run` or replacing the running service/task.

What happens after restart depends on the change:

- adding a polling table causes a fresh backfill for that table
- adding a CDC table bootstraps that table on the next run if the table is also present in the publication
- removing a table stops syncing it but does not delete the destination table

For CDC publication management:

- `publication_mode: manage` lets CDSync reconcile publication membership at startup
- `publication_mode: validate` requires the operator to update the publication first

## Schema Changes

With `schema_changes: auto`:

- additive columns are applied conservatively
- historical rows stay null for newly added columns unless you resync
- non-destructive changes are kept non-destructive on the BigQuery side

Incompatible changes, especially primary-key changes, are treated as blocking and may require an explicit resync.

## Failure Semantics

CDSync persists state as it goes. In practice that means:

- a crash should resume from the last durable checkpoint
- CDC acknowledgement only advances after the relevant downstream work is durably safe

The main operational requirement is simple: keep `state` and `stats` on durable storage, not ephemeral task-local files.
