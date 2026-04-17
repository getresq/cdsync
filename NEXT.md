# Next

## Current Mission: DynamoDB Source Release Readiness

Mission:
- Harden DynamoDB and Umami PostgreSQL source support for the `resq-sites` CDSync release while preserving PostgreSQL, BigQuery, admin API, dashboard, and infra behavior across `cdsync`, `resq-sites`, and `resq-fullstack`.

Checklist:
- [x] Bump CDSync release version to `0.5.0`.
- [x] Record active mission baseline and guardrails.
- [ ] Audit existing `cdsync` source/destination/admin implementation for PostgreSQL, DynamoDB, and BigQuery correctness risks.
- [ ] Verify PostgreSQL parallelism, data correctness, WAL CDC handling, and initial snapshot behavior remain intact.
- [ ] Verify or fix DynamoDB S3 snapshot ingestion, Kinesis follow, parallelism, and data type mapping.
- [ ] Verify or fix BigQuery compatibility for both PostgreSQL and DynamoDB sources.
- [ ] Verify or fix admin API DynamoDB fidelity without regressing PostgreSQL progress/state reporting.
- [x] Update `resq-fullstack` CDSync dashboard for DynamoDB.
- [x] Add CDSync infra to `resq-sites` comparable to `resq-fullstack` and `resq-agent`.
- [x] Add `resq-sites` Umami Postgres as a CDSync PostgreSQL CDC source.
- [x] Add `resq-sites` VPC/CDSync instance to the `resq-fullstack` production monitored CDSync instances list.
- [ ] Run focused and broad validation across touched repos.
- [ ] Run subagent review after each meaningful code chunk and fix confirmed findings.
- [ ] Add or run an emulated/local integration confidence layer for DynamoDB/Kinesis/S3, Postgres CDC, and BigQuery behavior where practical.

Guardrails:
- PostgreSQL CDC/snapshot correctness is release-blocking.
- Do not build Linux release artifacts locally.
- Do not revert unrelated changes in any repository.
- Ask before AWS actions that require SSO or mutate deployed infrastructure.
- Use `bun`/`bunx` for TypeScript and JavaScript, `uv`/`uvx` for Python.
- Do not preserve legacy/dead code or add backwards-compatible branches without explicit approval.
- Umami Postgres is in scope as a required `resq-sites` CDSync source, not an optional follow-up.

Progress:
- Mission baseline written to `MISSION.md`.
- Completed first `cdsync` hardening chunk:
  - DynamoDB bootstrap now chooses PITR `latest_restorable_date_time` and refuses to bootstrap until the configured Kinesis stream covers that cutover.
  - DynamoDB snapshot S3 reads are scoped to the current export ID, preventing stale export files from being appended after a retry.
  - DynamoDB snapshot batch loads append after the target truncate instead of doing per-batch BigQuery merges.
  - Admin progress now keeps PostgreSQL CDC queue/coordinator data PostgreSQL-only and adds explicit DynamoDB follow-state summary.
- Review:
  - First subagent review found two high-severity DynamoDB correctness issues; both were fixed.
  - Second subagent review found no issues.
- Validation:
  - `cargo fmt --check` passed.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::dynamodb -- --nocapture` passed with 12 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::tests -- --nocapture` passed with 21 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::route_tests::admin_api_in_process_stateful_routes_work -- --nocapture` passed with 1 test.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::postgres::tests::snapshot -- --nocapture` passed with 19 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::postgres::cdc_loop -- --nocapture` passed with 8 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib destinations::bigquery::tests -- --nocapture` passed with 15 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- Umami scope update:
  - Subagent confirmed `resq-sites` deploys Umami Postgres for staging and production via `infra/umami.ts` and `infra/internal-analytics-config.ts`.
  - Umami is not currently wired as a CDSync source; adding it requires PostgreSQL CDC infra/config in addition to the existing DynamoDB/Kinesis targets.
- Completed dashboard/admin stream chunk:
  - CDSync streams a nullable `connection.dynamodb_follow` event and filters DynamoDB follow summaries to DynamoDB connections.
  - DynamoDB follow state records observed shard count separately from checkpointed shards.
  - DynamoDB full resyncs stay in `snapshot`/`snapshot_in_progress` while `snapshot_in_progress=true`, even with an existing prior checkpoint.
  - `resq-fullstack` dashboard has a DynamoDB Follow card and a `kinesis` mode that keeps PostgreSQL CDC/WAL details separate.
- Dashboard/admin stream validation:
  - `uvx ruff check common/admin/cdsync.py common/tests/test_cdsync_admin_view.py` passed.
  - `bunx prettier --check templates/admin/cdsync_dashboard.html` passed.
  - `./resq test common/tests/test_cdsync_admin_view.py` passed with 13 tests after starting the backend container.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::tests -- --nocapture` passed with 23 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::dynamodb -- --nocapture` passed with 12 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::route_tests::admin_api_in_process_stateful_routes_work -- --nocapture` passed with 1 test.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- Dashboard/admin stream review:
  - First review found three issues around full-resync labeling, stale follow state, and shard count denominator; all were fixed.
  - Final review found no issues.
- Completed `resq-sites` infra and fullstack registry chunk:
  - Added `resq-sites` CDSync Docker/runtime wrapper and a combined config template for Build, DomainMapping, Revision DynamoDB sources plus Umami Postgres CDC.
  - Added `resq-sites` SST CDSync service scaffold in the Umami VPC, with internal NLB, PrivateLink endpoint service discovery, DynamoDB export bucket, BigQuery/GCP/admin JWT secrets, and task permissions for DynamoDB export/Kinesis/S3.
  - Added `resq-fullstack` production consumer wiring for `/com/getresq/production/env/generated/SITES_CDSYNC_VPCE_SERVICE_NAME`, adding `resq-sites production` to the production CDSync admin instance registry.
- Infra validation:
  - `bunx oxfmt --check infra/cdsync.ts infra/secret.ts sst.config.ts` passed in `resq-sites`.
  - `bunx oxlint infra/cdsync.ts infra/secret.ts sst.config.ts` passed in `resq-sites`.
  - `bunx tsc -p tsconfig.json --noEmit` passed in `resq-sites`.
  - `bash -n deploy/cdsync/cdsync-service.sh` passed in `resq-sites`.
  - `bunx prettier --check infra/apps/platform/sst.config.ts` passed in `resq-fullstack`.
  - Direct `bunx tsc --noEmit --skipLibCheck infra/apps/platform/sst.config.ts` in `resq-fullstack` is not usable as a focused check because it surfaces existing repo-wide SST/type errors unrelated to this patch.
- Infra review:
  - First review found missing DynamoDB export/S3 write permissions, an unmanaged Umami publication assumption, and ambiguous admin JWT key source. All were fixed.
  - Second review found `DescribeExport` needed export ARNs; fixed by scoping it to `${tableArn}/export/*`.
  - Final monitored-instance review found no issues.
- Validation strategy note:
  - LocalStack should be used for fast AWS SDK/API wiring tests around DynamoDB, Kinesis, and S3, but not as the final release proof for DynamoDB PITR export, Kinesis streaming destinations, IAM, or BigQuery behavior.
  - Release confidence still needs a small real-AWS staging run with PITR export, Kinesis follow, Umami Postgres CDC, BigQuery destination writes, and admin/dashboard checks.
- Started confidence-plan implementation:
  - Added optional LocalStack service to `docker-compose.e2e.yml`.
  - Added `CDSYNC_AWS_ENDPOINT_URL` / `CDSYNC_E2E_LOCALSTACK_ENDPOINT` support for the DynamoDB source AWS SDK clients.
  - Added `e2e_dynamodb_localstack` integration test for DynamoDB/Kinesis follow wiring and shard checkpoint state.
- Confidence-plan validation:
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_dynamodb_localstack -- --nocapture` passed with 1 test against the local LocalStack endpoint.
  - `CDSYNC_E2E_LOCALSTACK_ENDPOINT=http://localhost:4566 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_dynamodb_localstack -- --nocapture` passed with 1 test against the running local LocalStack container.
  - `CDSYNC_E2E_PG_URL=postgres://cdsync:cdsync@localhost:5433/cdsync CDSYNC_E2E_BQ_HTTP=http://localhost:9050 CDSYNC_E2E_BQ_GRPC=localhost:9051 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_postgres_bigquery -- --nocapture` passed with 2 tests against local Postgres + BigQuery emulator.
  - `CDSYNC_E2E_PG_URL=postgres://cdsync:cdsync@localhost:5433/cdsync CDSYNC_E2E_BQ_HTTP=http://localhost:9050 CDSYNC_E2E_BQ_GRPC=localhost:9051 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_postgres_cdc -- --nocapture` passed with 1 test.
  - `CDSYNC_E2E_PG_URL=postgres://cdsync:cdsync@localhost:5433/cdsync CDSYNC_E2E_BQ_HTTP=http://localhost:9050 CDSYNC_E2E_BQ_GRPC=localhost:9051 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_postgres_schema_changes -- --nocapture` passed with 4 tests.
  - `CDSYNC_E2E_PG_URL=postgres://cdsync:cdsync@localhost:5433/cdsync CDSYNC_E2E_BQ_HTTP=http://localhost:9050 CDSYNC_E2E_BQ_GRPC=localhost:9051 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_postgres_soft_deletes -- --nocapture` passed with 2 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::dynamodb -- --nocapture` passed with 12 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- Confidence-plan review:
  - First review found ambient AWS credentials dependency, overbroad test naming, and cleanup gaps; all were fixed.
  - A real LocalStack run exposed stream readiness and missing precision quirks; fixed with readiness polling and strict-production/local-emulator precision handling.
  - Final review found no issues.
- Release prep:
  - Bumped CDSync crate/package version to `0.5.0`.
  - `cargo fmt --check` passed.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::dynamodb -- --nocapture` passed with 12 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::tests -- --nocapture` passed with 23 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib destinations::bigquery::tests -- --nocapture` passed with 15 tests.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::postgres::cdc_loop -- --nocapture` passed with 8 tests.
  - `CDSYNC_E2E_LOCALSTACK_ENDPOINT=http://localhost:4566 CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --features integration-tests --test e2e_dynamodb_localstack -- --nocapture` passed with 1 test.
  - `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- Production `resq-sites` deploy monitoring:
  - After enabling desired count 1, ECS task definition `:7` failed before container startup because the Datadog sidecar/FireLens config referenced a hardcoded SSM parameter ARN for the Datadog API key and the Umami private subnets have no SSM egress.
  - Set the production SST `DatadogApiKey` secret from the existing Datadog parameter and patched local `resq-sites` infra to remove the hardcoded SSM dependency from the CDSync task.
  - User chose private subnets with managed NAT. Patched `resq-sites` to add `nat: "managed"` to the Umami VPC and to feed Datadog ECS secret retrieval from a managed SecureString parameter sourced from the SST `DatadogApiKey` secret.
  - Review found and fixed an intermediate secret exposure regression: the Datadog key must not be placed in normal ECS environment/log options or linked as `SST_RESOURCE_DatadogApiKey`; it now stays behind ECS `ssm`/FireLens `secretOptions`.
  - SST deploy created the managed NAT gateways and private route-table default routes, then CDSync started and exited with code 1.
  - Root cause of the app exit was in CDSync `v0.5.0`: `DynamoDbConfig::validate` rejected valid configs where `key_attributes` also appeared in declared `attributes`, even though that projection is required. Added a regression test and bumped CDSync to `0.5.1`.
  - Validation: focused config regression test passed, DynamoDB unit tests passed, `cargo check --locked --lib` passed, and the rendered `resq-sites` config now passes config validation and reaches AWS API validation.
  - Released CDSync `v0.5.1`; GitHub release workflow passed and published `cdsync-x86_64-unknown-linux-gnu.tar.gz`.
  - Deployed `resq-sites` production with managed NAT, v0.5.1, top-level task role permissions, and Datadog FireLens sidecars.
  - Production validation: ECS desired/running `1/1`, target group healthy, Datadog app logs present for the current task, and admin API reports `sites_build`, `sites_domain_mapping`, and `sites_revision` as `kinesis_following` and `sites_umami` as `cdc_following`.
  - Merged `resq-sites` PR #305 (`Stabilize production CDSync networking`) into `master`.

## Prior Work History

## Release/Deploy CDSync `v0.4.9`

Mission:
- Release CDSync `v0.4.9` for the BigQuery MERGE retry pinning fix and roll it through `resq-fullstack` staging/production, then Nora/resq-agent production.

Checklist:
- [x] Bump CDSync crate/package version to `0.4.9`.
- [x] Run release-grade local checks without building Linux artifacts locally.
- [x] Review release-prep diff.
- [ ] Commit and tag `v0.4.9`.
- [ ] Push commit/tag and wait for GitHub release artifact.
- [ ] Update `resq-fullstack` CDSync pins to `v0.4.9`.
- [ ] Deploy `resq-fullstack` staging CDSync.
- [ ] Deploy `resq-fullstack` production CDSync.
- [ ] Update `resq-agent` Nora CDSync pin to `v0.4.9`.
- [ ] Deploy `resq-agent` production Nora CDSync.
- [ ] Verify ECS/admin/progress/log behavior after each deploy.

Guardrails:
- Do not build Linux release artifacts locally.
- Accept normal ECS drain-then-start zero-target windows.
- Do not force a replacement deployment while SST/ECS is still in the normal 1-3 minute handoff.
- Report actual CDC progress, not only ECS stabilization.

Validation:
- `cargo fmt --check` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo clippy --locked --all-targets --all-features -- -D warnings` passed.

Review:
- Subagent release-prep review found no issues and confirmed no Linux release artifacts were added.

## Deploy CDSync Relation Lock Fix

Mission:
- Release and deploy the CDC relation-lock fix to `resq-fullstack` production.

Checklist:
- [x] Bump CDSync version for a patch release.
- [x] Run release-grade local checks without building Linux artifacts locally.
- [x] Commit and tag CDSync release.
- [ ] Push commit and tag; wait for GitHub release artifact.
- [ ] Update `resq-fullstack` CDSync release pin.
- [ ] Deploy production CDSync ECS service.
- [ ] Wait about 3 minutes, then verify ECS health, admin progress, Datadog logs, and source slot feedback.

Guardrails:
- Do not build Linux release artifacts locally.
- Do not revert unrelated work.
- Report actual CDC movement, not only ECS stabilization.

Validation:
- `cargo fmt --check` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib relation_lock_wait_ -- --nocapture` passed with 3 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib sources::postgres::cdc_loop -- --nocapture` passed with 8 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo clippy --locked --all-targets -- -D warnings` passed after a test-only `let...else` cleanup.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo clippy --locked --all-targets --all-features -- -D warnings` passed; this matches the pre-push clippy hook with a reliable target dir.

## Production Retryable BigQuery Backlog Investigation

Mission:
- Explain the current `resq-fullstack` production CDC backlog and determine whether it needs intervention or should drain by retry.

Checklist:
- [x] Interpret the dashboard screenshot against CDSync admin progress semantics.
- [x] Verify the deployed CDSync version and task identity from Datadog startup logs.
- [ ] Inspect admin progress twice for real CDC movement.
- [x] Inspect Datadog (`pup`) logs around the latest failed BigQuery merge.
- [x] Identify the failing table/error class and next operator action.

Current findings:
- The dashboard shows actual CDC backlog, not only idle/unattributed WAL drift.
- Head-of-line acknowledgement is stuck at sequence `25013` while the coordinator has enqueued through `25028`, so the sequence lag is `15`.
- One CDC batch-load job is pending/queued, one retryable failed job is visible, and no jobs or rows completed in the last minute.
- Production `production-cdsync-app` started CDSync `v0.4.8` at `2026-04-15 20:16:45 UTC` on task version `22`.
- The failing job is `cdc_job_ec1085312949c50047595e97` for `public__core_workordernote`, first sequence `25013`, one CDC row.
- Datadog shows repeated failure/retry scheduling for that same job from `2026-04-16 08:05:33 UTC` through at least `2026-04-16 18:09:27 UTC`.
- Staging/load succeeds; the job fails at the reduced BigQuery MERGE into target `public__core_workordernote`.
- The latest observed retry at `2026-04-16 18:09:27 UTC` was classified `transient` and scheduled the next retry after `972800 ms` (about `16m 13s`).
- WAL is accumulating behind confirmed flush because the coordinator cannot acknowledge sequence `25013` until the pending/retryable job succeeds.
- Direct BigQuery job-history lookup works after granting the service account additional permissions.
- BigQuery job `job_XyFzZQsBdnjTh9Se3i7wJQinBRbE` failed at `2026-04-16 08:05:33 UTC` with `backendError`: `Error encountered during execution. Retrying may solve the problem.`
- The staging table has exactly one row: `id=588391`, `_cdsync_sequence=25013`, `_cdsync_operation=upsert`; the target table currently has zero rows for that id.
- Inference: CDSync's deterministic BigQuery query request id is likely causing repeated retries to observe/reuse the same failed BigQuery job instead of forcing a fresh MERGE attempt.
- Next operator action: with explicit approval, run the same MERGE manually without the deterministic request id, then verify the durable job drains and coordinator advances beyond sequence `25013`.

## Fix BigQuery MERGE Retry Pinning

Mission:
- Prevent retryable BigQuery MERGE backend errors from pinning a queued CDC job to the original failed BigQuery query job forever.

Checklist:
- [x] Confirm the defect from production evidence.
- [x] Make queued CDC MERGE request ids include the durable apply attempt.
- [x] Add focused regression coverage for retry request id rotation.
- [x] Run focused Rust checks.
- [x] Review the diff and address findings.

Guardrails:
- Preserve same-attempt idempotency for stale/running claim recovery.
- Rotate the BigQuery request id only after CDSync has durably failed and requeued retryable work.
- Do not build Linux release artifacts locally.

Validation:
- `cargo fmt` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib merge_query_request_id_rotates_by_apply_attempt_key -- --nocapture` passed with 1 test.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib apply_attempt_key -- --nocapture` passed with 3 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --locked --lib` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib destinations::bigquery::tests -- --nocapture` passed with 15 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib destinations::etl_bigquery::queue::tests -- --nocapture` passed with 15 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo clippy --locked --lib -- -D warnings` passed.

Review:
- First subagent review found the initial tests were too helper-level and did not cover queue object wiring.
- Addressed by moving attempt-key generation onto queued job/window types and testing those types directly.
- Second subagent review still noted the destination apply API could be called without the attempt key.
- Addressed by making `BigQueryDestination::apply_cdc_batch_load_job` require an apply attempt key.
- Third subagent review found no issues.

## Production Unattributed WAL Gap Investigation

Mission:
- Explain why the `resq-fullstack` production dashboard showed a very large Unattributed / Idle WAL Gap.

Checklist:
- [x] Trace the dashboard metric to the CDSync admin/state calculation.
- [x] Verify the deployed production CDSync version and task health.
- [x] Inspect the production Postgres replication slot, replication sender state, and CDSync durable state.
- [x] Inspect production CDSync logs around the last observed feedback movement.
- [x] Summarize the cause and a follow-up fix path.

Findings:
- Production is running CDSync `0.4.7` on task definition revision `21`; ECS reports one healthy running task started on 2026-04-14 20:48:05 UTC.
- At 2026-04-15 19:20:11 UTC, source slot `supabase_etl_apply_1101` was active, but `confirmed_flush_lsn` was still `1DFB/98383AF0` while `pg_current_wal_lsn()` was `1DFF/700F76A0`, producing about 16.5 GB behind confirmed.
- CDSync durable feedback for `app_production` last updated at 2026-04-15 09:57:17 UTC with `last_slot_feedback_lsn = 1DFB/98383AF0`.
- Durable CDC fragments and batch-load jobs had no pending or failed rows; all rows were `succeeded`, with latest job completions around 2026-04-15 09:57:41 UTC.
- The latest enqueued sequence was `9428`, but durable feedback still had `next_sequence_to_ack = 9418`, so the final small CDC frontier did not get flushed; the much larger WAL gap accumulated afterward while the source kept generating WAL.
- Datadog returned no CDSync app logs after 2026-04-15 09:57:41 UTC in the inspected window, even though the task and replication slot remained active.
- Deeper cause: the WAL loop advanced to sequence `9418` at 2026-04-15 09:57:17 UTC, then dispatched table batches for OIDs `984824`, `19260`, `17739`, and `18317`. OID `18317` is `public.core_recordofworklineitem`.
- At 2026-04-15 09:57:18 UTC the WAL loop logged `processing cdc relation change` for OID `18317`, but never logged `cdc relation change applied`. The relation path waits on that table's apply lock before its 120s relation timeout starts, so a held table lock can block the WAL loop indefinitely.
- Inference from logs and code: the table dispatch for OID `18317` held the table apply lock and did not reach a durable `cdc_producer_enqueued_job` / `cdc table batch apply completed` log for that final batch. While the WAL loop waited on the lock, it stopped draining completed waiter futures and coordinator advances, so background worker successes through sequence `9428` never translated into replication feedback.

Parked follow-up:
- Fix the CDC relation/table-lock stall. Starting points: `src/sources/postgres/cdc_runtime.rs`, `src/sources/postgres/cdc_loop.rs`, `src/sources/postgres/cdc_pipeline.rs`, and `src/destinations/etl_bigquery/queue.rs`. Validate by reproducing a relation message arriving while a same-table deferred dispatch is in progress; the WAL loop must keep draining completed jobs/frontier and the relation wait must be timeout-bounded/observable.

## Fix CDC Relation Lock Stall

Mission:
- Prevent a relation/schema-change message from freezing CDC feedback when same-table CDC dispatch work is already in flight.

Checklist:
- [x] Patch relation handling so it keeps draining in-flight dispatch/apply work while waiting for the table apply lock.
- [x] Make relation lock waits timeout-bounded and observable.
- [x] Add focused regression coverage for a relation wait that must poll an in-flight dispatch future to release the lock.
- [x] Run focused Rust tests.
- [x] Review the diff and address findings.

Current context:
- Production v0.4.7 most likely stalled after `processing cdc relation change` for OID `18317` / `public.core_recordofworklineitem`.
- The relation path currently awaits `table_lock.lock().await` directly; the timeout only wraps the later buffered-apply and relation-update operations.
- Dispatch futures are stored in `FuturesUnordered`, not spawned, so the main loop must poll them for same-table locks to be released.

Validation:
- `cargo fmt` passed.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --lib relation_lock_wait_ -- --nocapture` passed with 3 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --lib sources::postgres::cdc_loop -- --nocapture` passed with 8 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --lib` passed.
- After the observability tweak, `cargo fmt && CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --lib relation_lock_wait_ -- --nocapture && CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo check --lib` passed.

Review:
- Subagent found the first patch still allowed relation handling to acquire a free mutex while same-table dispatch was active but not yet polled. The invariant must check `active_table_applies`, not only mutex availability.
- Addressed by passing the relation table id into the wait helper and disabling lock acquisition while that table remains active. Added a regression for the unlocked-but-active same-table dispatch case.
- Second subagent review found no correctness issues. It noted the active-but-unlocked wait should be observable; the relation wait log now includes both `relation_table_active` and `relation_lock_busy`.

## CDC Dashboard WAL Gap Signal

Mission:
- Keep idle/unattributed WAL drift from surfacing as a scary blocking signal unless the admin data shows actual blocking/backlogged work.

Checklist:
- [x] Reclassify idle WAL gap in CDSync admin progress insight.
- [x] Adjust the ResQ dashboard label for non-blocking progress states.
- [x] Add focused regression coverage.
- [x] Run focused ResQ checks and Rust formatting.
- [x] Review the diff.
- [x] Re-run focused Rust checks in an isolated target dir.
- [x] Bump CDSync version and release pin.
- [x] Commit and tag the release.

Validation:
- `cargo fmt` passed in `/Users/mazdak/Code/cdsync`.
- `uvx ruff format common/admin/cdsync.py common/tests/test_cdsync_admin_view.py && uvx ruff check common/admin/cdsync.py common/tests/test_cdsync_admin_view.py` passed in `/Users/mazdak/Code/resq-fullstack`.
- `./resq test common/tests/test_cdsync_admin_view.py` passed in `/Users/mazdak/Code/resq-fullstack` with 12 tests.
- `cargo test --lib admin_api::tests::cdc_progress_insight` in the default target dir and `cargo check --lib --tests` both stalled with idle rustc children at 0% CPU and were stopped.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --lib admin_api::tests::cdc_progress_insight` passed with 8 tests.
- `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --lib admin_api::route_tests::admin_api_in_process_stateful_routes_work` passed with 1 test.
- After bumping CDSync to `0.4.6`, `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::tests::cdc_progress_insight` passed with 8 tests.
- After bumping CDSync to `0.4.6`, `CARGO_TARGET_DIR=target/codex-check CARGO_INCREMENTAL=0 cargo test --locked --lib admin_api::route_tests::admin_api_in_process_stateful_routes_work` passed with 1 test.
- `bunx prettier --check infra/shared/platform-cdsync-ecs-service.ts templates/admin/cdsync_dashboard.html` passed in `/Users/mazdak/Code/resq-fullstack`.

Review:
- Subagent review found no issues.

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
