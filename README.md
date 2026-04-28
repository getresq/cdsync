# CDSync

CDSync is an open-source PostgreSQL-to-BigQuery sync tool. It supports one-shot table syncs, scheduled polling, and logical-replication CDC with durable checkpoints stored in Postgres.

## Current Scope

- Source: PostgreSQL
- Destination: BigQuery
- Modes: full snapshot, incremental polling, CDC follow mode
- State: Postgres-backed state and stats schemas
- Ops: validation, reconcile/report commands, admin API, OTLP telemetry

## Quick Start

Generate a starter config:

```bash
cargo run -- init --config config.yaml
```

After editing the config, apply migrations and validate it:

```bash
cargo run -- migrate --config config.yaml
cargo run -- validate --config config.yaml --connection app --verbose
```

Run one incremental sync:

```bash
cargo run -- run --config config.yaml --connection app --once --incremental
```

Run a long-lived worker:

```bash
cargo run -- run --config config.yaml --connection app
```

Notes:

- Config files can be YAML or TOML.
- CDC connections require `publication` and `cdc_pipeline_id`.
- Copy `.env.example` to `.env` for local integration-test defaults.
- Deployment manifests and environment-specific infra are intentionally out of tree.

## Docs

- [Configuration Reference](docs/configuration.md)
- [How CDSync Works](docs/how-cdsync-works.md)
- [Operations](docs/operations.md)
- [End-to-End Tests](docs/e2e.md)
- [CDSync Sunset: Datastream First, Airbyte Second](docs/datastream-airbyte-migration.md)
