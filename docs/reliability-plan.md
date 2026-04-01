# Reliability Plan

This document tracks the three workstreams needed to move CDSync from
"functionally correct" toward "world-class reliable":

1. destructive/fault-injection testing
2. metrics and alerting
3. admin API / control-plane improvements

## Workstream 1: Destructive Testing

### Goal

Prove that CDSync converges correctly across ugly interruption cases, not just
happy-path sync runs.

### Target failure modes

- snapshot interrupted before checkpoint save
- snapshot interrupted after checkpoint save
- CDC interrupted mid-transaction
- task replacement during CDC follow
- lock owner death / stale lock takeover
- duplicate worker startup for the same connection
- schema-blocked tables across restart/resume
- destination rewrite / resync interruption

### Plan

1. Add test-only failpoints in snapshot copy, CDC apply, checkpoint save, and
   lock lifecycle.
2. Add binary-level destructive tests that kill/restart real `cdsync` child
   processes against local Postgres/BigQuery emulator state.
3. Add recurring staging chaos runs for task replacement and resume behavior.

## Workstream 2: Metrics And Alerting

### Goal

Make it obvious whether a connection is alive, advancing, blocked, or silently
failing.

### Near-term metrics

- connection worker lifecycle events
- connection checkpoint age
- retry attempts
- WAL/backpressure depths
- sync run duration / status

### Alert candidates

- no forward checkpoint progress for N minutes
- repeated worker failures on one connection
- checkpoint age above threshold
- schema-blocked connection
- resync/snapshot stuck too long

## Workstream 3: Admin / Control Plane

### Goal

Give operators an explicit runtime model, not just raw state dumps.

### Near-term surface

- config hash / deploy revision / restart metadata
- per-connection runtime phase + reason code
- per-table phase + reason code
- per-table checkpoint age / lag diagnostics

### Later control-plane actions

- pause / resume connection
- trigger targeted table resync / backfill
- acknowledge or clear blocked schema state
- command history / durable operator intent

## First Slice Landed Here

This mission slice focuses on:

- a durable plan for the three workstreams
- admin API runtime/lag diagnostics
- worker/checkpoint reliability metrics
- targeted regression coverage for the new runtime surface
