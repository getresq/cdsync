# Mission

Mission: Implement a safer raw-replication schema-evolution policy that auto-accepts additive and non-destructive source changes while still blocking the truly incompatible ones.

## Done Criteria

1. Column additions continue automatically.
2. Column removals and renames-as-add+remove no longer force resync/failure in `auto` mode.
3. Nullability-only changes no longer force resync/failure in `auto` mode.
4. Real incompatible changes still stop or resync cleanly, especially destination-type changes and primary-key changes.
5. Tests cover the new compatibility matrix and the code stays fmt/test/clippy clean.

## Guardrails

- Keep the raw replicated layer non-destructive by default.
- Do not infer true renames heuristically.
- Preserve explicit `fail` and `resync` behavior for incompatible cases.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: In the raw destination, column removal should mean “leave the old column in place and stop updating it,” not “drop it automatically.”
- Decision: A rename should naturally appear as add+remove unless the source emits a dedicated semantic rename event and we choose to model it later.
- Constraint: CDSync currently stores only field snapshots plus a schema fingerprint, so primary-key change detection needs to piggyback on the fingerprint when the field diff alone is empty.
- Validation: The relaxed compatibility matrix and checkpoint changes passed `cargo test -q` and `cargo clippy --all-targets --all-features -- -D warnings`.
