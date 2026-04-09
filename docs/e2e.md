# End-to-End Tests

Most integration tests are ignored by default. The local suite uses Postgres plus the BigQuery emulator. The live suite uses a real BigQuery dataset and a writable GCS batch-load bucket.

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

Representative emulator-backed tests:

```bash
cargo test --test e2e_postgres_bigquery -- --ignored --nocapture
cargo test --test e2e_postgres_cdc -- --ignored --nocapture
cargo test --test e2e_postgres_schema_changes -- --ignored --nocapture
cargo test --test e2e_postgres_soft_deletes -- --ignored --nocapture
cargo test --test e2e_runner_graceful_shutdown -- --ignored --nocapture
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

Run the live suites:

```bash
cargo test --test e2e_postgres_bigquery_real -- --ignored --nocapture
cargo test --test e2e_postgres_bigquery_real_cdc -- --ignored --nocapture
cargo test --test e2e_postgres_follow_config_changes_real -- --nocapture
```

The live tests cover:

- full polling syncs
- CDC snapshot plus follow-up change capture
- schema additions
- soft deletes and hard deletes
- adding and removing tables from an existing CDC config
