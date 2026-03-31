# Mission

Mission: Start hardening the WAL pipeline/backpressure path by making the CDC commit queue and backpressure behavior explicitly observable instead of only implicitly bounded in code.

## Done Criteria

1. CDC runtime emits explicit metrics for pending events and commit queue/in-flight depth.
2. Backpressure waits are recorded so operators can tell when BigQuery/apply throughput is throttling WAL consumption.
3. The change stays bounded to observability/runtime signaling and does not redesign the whole CDC engine in one pass.

## Guardrails

- Preserve commit ordering and existing correctness guarantees.
- Do not change the existing bounded-queue behavior unless there is a clear correctness reason.
- No compiler warnings, no clippy warnings, no broken tests.

## Critical Learnings

- Decision: The extracted `cdc_runtime.rs` is now the cleanest seam for queue/backpressure instrumentation.
- Constraint: We already have bounded queues and watermark-based advancement; the immediate gap is visibility, not the absence of any queueing at all.
