# End-to-end tests (docker)

These tests are ignored by default and require a Postgres instance plus a BigQuery emulator.

## Start services

Start only the BigQuery emulator:

```
docker compose -f docker-compose.e2e.yml up -d bigquery-emulator
```

Optionally start the Postgres container (disabled by default to avoid clashing with other local Postgres):

```
docker compose -f docker-compose.e2e.yml --profile itest up -d postgres
```

## Environment variables

Set the connection info before running the tests:

```
export CDSYNC_E2E_PG_URL="postgres://cdsync:cdsync@localhost:5433/cdsync"
export CDSYNC_E2E_BQ_HTTP="http://localhost:9050"
export CDSYNC_E2E_BQ_GRPC="localhost:9051"
export CDSYNC_E2E_BQ_PROJECT="cdsync"
export CDSYNC_E2E_BQ_DATASET="cdsync_e2e"
```

If you already run Postgres elsewhere, point `CDSYNC_E2E_PG_URL` at that instance and skip the dockerized Postgres service.

## Run the test

```
cargo test --test e2e_postgres_bigquery -- --ignored --nocapture
```

## CDC test

The CDC test validates publication row filters with logical replication enabled.

```
cargo test --test e2e_postgres_cdc -- --ignored --nocapture
```

## Schema/soft-delete tests

```
cargo test --test e2e_postgres_schema_changes -- --ignored --nocapture
cargo test --test e2e_postgres_soft_deletes -- --ignored --nocapture
```

## Notes

- The BigQuery emulator does not support MERGE, so upserts append rows. Tests that validate soft deletes
  check for at least one row with `_cdsync_deleted_at` set rather than assuming a single-row table.
- The emulator deletes tables on truncate; CDSync recreates tables during full refresh to avoid 404s.

## Quick cloud smoke test (AWS RDS Postgres + BigQuery)

Use this when you want a real cloud-backed test instead of the emulator.

### 1) Configure Postgres (AWS RDS)

1. Enable logical replication on the writer instance (not a read replica). In the DB parameter group:
   - `rds.logical_replication = 1`
   - `wal_level = logical`
   - `max_replication_slots` and `max_wal_senders` sized for your workload (e.g., 5+)
   - Reboot the instance after applying the parameter group.
2. Create a replication user and grant privileges:

```
-- Run as a superuser / rds_superuser
CREATE ROLE cdsync_rep WITH LOGIN PASSWORD '***' REPLICATION;
GRANT rds_replication TO cdsync_rep;
GRANT USAGE ON SCHEMA public TO cdsync_rep;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdsync_rep;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdsync_rep;
```

3. Create a publication for the tables you want to sync:

```
CREATE PUBLICATION cdsync_pub FOR TABLE public.accounts, public.orders;
```

If you use row filters, Postgres 15+ is required:

```
CREATE PUBLICATION cdsync_pub
  FOR TABLE public.accounts WHERE (tenant_id = 1);
```

### 2) Configure BigQuery

1. Enable the BigQuery API.
2. Create a dataset (e.g., `cdsync_test`) in a location (e.g., `US`).
3. Create a service account and grant:
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.jobUser`
4. Download a JSON key for the service account.

### 3) Example config (YAML)

```
connections:
  - id: rds_to_bq
    enabled: true
    source:
      type: postgres
      url: "postgres://cdsync_rep:***@your-rds-host:5432/your_db?sslmode=require"
      cdc: true
      publication: cdsync_pub
      # CDC TLS (replication stream) uses these fields, not the URL sslmode
      cdc_tls: true
      cdc_tls_ca_path: /path/to/rds-ca-bundle.pem
      table_selection:
        include: ["public.*"]
        exclude: ["public.audit_*"]
        defaults:
          primary_key: id
          updated_at_column: updated_at
          soft_delete: true
          soft_delete_column: deleted_at
          where_clause: "tenant_id = 1"   # must match publication row filter
          columns:
            include: ["id", "tenant_id", "name", "updated_at", "deleted_at"]
    destination:
      type: bigquery
      project_id: your-project
      dataset: cdsync_test
      location: US
      service_account_key_path: /path/to/bq-sa.json

state:
  path: ./state.json
```

### 4) Validate + run

```
cdsync validate --connection rds_to_bq --verbose
cdsync sync --connection rds_to_bq --incremental --schema-diff
```

Notes:
- CDC must connect to the writer. If you only have a read replica, set `cdc: false` and rely on polling via `updated_at_column`.
- For row-filtered CDC, the publication `WHERE (...)` and the config `where_clause` must match exactly (validate will show diffs).
