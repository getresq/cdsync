# Mission

Mission: Increase `cdsync` test coverage with the highest-value remaining unit and integration cases, and keep the full relevant Rust test/lint surface passing.

## Done Criteria

1. Add at least one meaningful new unit-test slice that locks down tricky engine behavior.
2. Add at least one meaningful new integration-style test that exercises real persistence/runtime seams.
3. The new tests run in the normal suite and pass.
4. `cargo clippy --all-targets --all-features -- -D warnings` stays clean.
5. Leave a clear recommendation for the next staging one-off task once coverage work is merged.

## Guardrails

- Prefer production-significant scenarios over broad but shallow test churn.
- Reuse existing test harnesses instead of inventing parallel infrastructure.
- Keep the changes localized and maintainable.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: The next highest-value uncovered path is the CDC snapshot/resume flow against Postgres-backed state, because that is still the test gap explicitly called out in `NEXT.md`.
- Decision: The quickest durable coverage gain is to prove persisted checkpoint metadata round-trips through the real state store, instead of only testing helper functions in isolation.
- Validation: Added one new unit test around explicit-PK compatibility behavior and one new integration test around snapshot-resume checkpoint persistence.
- Validation: `cargo test -q` and `cargo clippy --all-targets --all-features -- -D warnings` both passed after the new tests landed.
