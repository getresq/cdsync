# End-to-End Tests

`cdsync` now separates tests into three tiers:

- normal tests: `cargo test`
- emulator-backed integration tests: gated behind `--features integration-tests`
- real BigQuery integration tests: gated behind `--features real-bq-tests`

Normal `cargo test` does not build or run the heavy `e2e_*` targets. The local suite uses Postgres plus the BigQuery emulator. The live suite uses a real BigQuery dataset and a writable GCS batch-load bucket.

## Local Emulator Suite

Start the BigQuery emulator:

```bash
docker compose -f docker-compose.e2e.yml up -d bigquery-emulator
```

Optionally start the repo's Postgres container:

```bash
docker compose -f docker-compose.e2e.yml --profile itest up -d postgres
```

Set the local test environment:

```bash
export CDSYNC_E2E_PG_URL="postgres://cdsync:cdsync@localhost:5433/cdsync"
export CDSYNC_E2E_BQ_HTTP="http://localhost:9050"
export CDSYNC_E2E_BQ_GRPC="localhost:9051"
export CDSYNC_E2E_BQ_PROJECT="cdsync"
export CDSYNC_E2E_BQ_DATASET="cdsync_e2e"
```

Run the emulator-backed e2e suite on request:

```bash
scripts/run-e2e-suite.sh emulator
```

Or run one emulator-backed target directly:

```bash
cargo test --features integration-tests --test e2e_postgres_cdc -- --nocapture
```

Notes:

- the BigQuery emulator does not support `MERGE`, so update-heavy tests validate behavior accordingly
- the emulator deletes tables on truncate, so CDSync recreates them during full refresh
- metadata columns are configurable and covered by the emulator suite

## Real BigQuery Suite

Copy `.env.example` to `.env` or export the equivalent variables manually, then enable live tests:

```bash
export CDSYNC_RUN_REAL_BQ_TESTS=1
export CDSYNC_E2E_PG_URL="postgres://cdsync:cdsync@localhost:5433/cdsync"
export CDSYNC_REAL_BQ_PROJECT="your-gcp-project"
export CDSYNC_REAL_BQ_DATASET="cdsync_e2e_real"
export CDSYNC_REAL_BQ_LOCATION="US"
export CDSYNC_REAL_BQ_BATCH_LOAD_BUCKET="your-cdsync-load-bucket"
export CDSYNC_REAL_BQ_KEY_PATH="/absolute/path/to/service-account.json"
```

Run the live suites on request:

```bash
scripts/run-e2e-suite.sh real-bq
```

The runner fails fast if the required live-test environment is not present. A successful `real-bq` run means the live suite actually executed; it no longer silently returns green because the environment was missing.

Or run one real-BQ target directly:

```bash
cargo test --features real-bq-tests --test e2e_postgres_follow_config_changes_real -- --nocapture
```

The live tests cover:

- full polling syncs
- CDC snapshot plus follow-up change capture
- schema additions
- soft deletes and hard deletes
- adding and removing tables from an existing CDC config

You can also run every explicit target suite directly:

```bash
scripts/run-e2e-suite.sh e2e_postgres_cdc
scripts/run-e2e-suite.sh e2e_postgres_follow_config_changes_real
```
