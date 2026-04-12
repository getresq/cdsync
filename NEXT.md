# Next

## Staging BigQuery `totalRows` Decode Backlog

- [x] Confirm `jobs.getQueryResults` decode fails before local query-response normalization when BigQuery omits `totalRows`.
- [x] Add a lenient `getQueryResults` fetch path for completed DML/MERGE query jobs.
- [x] Add regression coverage for missing `totalRows` in completed query results.
- [x] Run focused CDSync checks.

## Milestone 3: Partial Snapshot + CDC Overlap

Goal:
- let already-snapshotted tables consume CDC while other tables are still snapshotting
- avoid one hot snapshot table freezing the whole connection
- preserve correct WAL acknowledgement semantics

Why this is separate:
- Milestones 1 and 2 are safe hardening work
  - per-table merge serialization
  - table-local retry / blocked state during snapshot
- Milestone 3 changes the correctness boundary between snapshot completion and CDC follow
- that means more risk around duplicates, gaps, and watermark advancement

Required runtime model:
- snapshot state becomes table-local instead of connection-global
- each table needs an explicit lifecycle:
  - `never_started`
  - `snapshotting`
  - `snapshot_retrying`
  - `snapshot_blocked`
  - `cdc_ready`
  - `cdc_following`
- CDC producer may read WAL for the connection before every table is `cdc_ready`
- coordinator must only advance WAL when every fragment up to the head-of-line sequence is safe

Design constraints:
- a table may not receive CDC apply before its snapshot start LSN and snapshot completeness rules are satisfied
- table promotion from snapshot to CDC must be explicit and durable
- WAL acknowledgement cannot depend on vague "connection is mostly healthy" logic
- backpressure must remain table-aware, not only connection-aware

State required:
- durable per-table runtime state
  - phase
  - attempts
  - last error
  - next retry time
  - snapshot start LSN
  - snapshot completion marker
  - CDC-ready marker
- durable per-fragment / coordinator state remains the source of truth for WAL advancement

Implementation sketch:
1. Keep the current exported snapshot planning, but mark tables `cdc_ready` as soon as their snapshot chunks complete.
2. Start the WAL producer once the exported snapshot slot is established.
3. For tables not yet `cdc_ready`, producer persists CDC fragments but apply is deferred.
4. For tables that are `cdc_ready`, consumer may claim and apply fragments immediately.
5. Coordinator advances only when contiguous fragments are complete, regardless of how many tables are still snapshoting.
6. Dashboard/admin API must show mixed-mode reality:
   - some tables snapshotting
   - some tables retrying
   - some tables following CDC
   - coordinator pending / blocked sequences

Main risks:
- duplicate apply if a table is promoted to CDC too early
- gaps if snapshot completion and first CDC LSN are not stitched together exactly
- coordinator head-of-line blocking if one non-ready table owns early sequences
- WAL retention growth if blocked tables accumulate fragments faster than operators resolve them

Suggested rollout path:
1. Prove Milestones 1 and 2 in staging.
2. Add Milestone 3 behind an explicit feature flag.
3. Run restart/resume tests with mixed table states.
4. Force one table into retry/block while others keep moving.
5. Verify:
   - CDC continues for `cdc_ready` tables
   - blocked table does not duplicate or lose rows after recovery
   - coordinator never advances beyond unsafe contiguous state
