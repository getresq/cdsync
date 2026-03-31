# Mission

Mission: Make PostgreSQL CDC snapshot resume semantics safe enough for interrupted runs by persisting per-table snapshot progress, resuming incomplete work without duplicate snapshot appends, and preserving the snapshot start LSN needed for CDC catch-up.

## Done Criteria

1. In-progress CDC snapshots persist resumable per-table snapshot state instead of only a single ambiguous `last_primary_key`.
2. Restarted snapshot runs resume incomplete tables or chunks without truncating already-loaded work.
3. Resumed snapshot work writes through an idempotent path so rereads from a later source view do not create duplicate destination rows.
4. The snapshot start LSN survives interruption and is reused for the CDC catch-up boundary.
5. The change is covered by focused tests and passes formatting, tests, and clippy.

## Guardrails

- Keep the change bounded to CDC snapshot resume semantics; do not redesign the WAL pipeline.
- Do not regress the normal fresh snapshot path or polling sync behavior.
- Be explicit when old checkpoint state is too incomplete to resume safely.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: Persist snapshot progress as explicit per-table chunk checkpoints plus a saved snapshot start LSN, rather than overloading `last_primary_key`.
- Decision: Resume snapshot batches via `Upsert` so rereading from a later source view still converges correctly when WAL catch-up starts from the original snapshot LSN.
- Constraint: Exported Postgres snapshots do not survive process restarts, so a resumed run cannot depend on re-importing the original exported snapshot name.
