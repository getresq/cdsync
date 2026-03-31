# Mission

Mission: Build the first production-ready very-large-table snapshot strategy by chunking large PostgreSQL CDC snapshot tables by primary-key range while keeping small tables on the existing whole-table path.

## Done Criteria

1. Large CDC snapshot tables are detected automatically at runtime.
2. Eligible tables are split into bounded PK-range chunks and copied in parallel under the existing snapshot concurrency limit.
3. Small tables and unsupported PK types keep the current whole-table snapshot behavior.
4. The change is covered by focused tests and passes formatting, tests, and clippy.

## Guardrails

- Do not regress correctness of initial snapshot output.
- Do not make snapshot checkpoint state misleading or unsafe for resume semantics.
- Keep the change bounded to the large-table strategy; do not mix in Parquet or resume-state redesign.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: Limit the first chunking pass to CDC snapshot tables with single-column integer PKs (`int2`/`int4`/`int8`).
- Decision: Detect “large” tables from runtime row-count and PK-range inspection instead of introducing new config knobs first.
- Constraint: Current snapshot checkpoints are not rich enough to represent parallel per-chunk resume safely, so chunked snapshot tasks must avoid persisting ambiguous `last_primary_key` progress.
