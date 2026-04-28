# Configuration Reference

CDSync loads configuration from a file passed with `--config`.

Parsing is extension-based:

- `.toml` files are parsed as TOML
- everything else is parsed as YAML

Examples below use YAML, but the same keys exist in TOML.

## Minimal Shape

```yaml
state:
  url: "postgres://user:pass@host:5432/db"

connections:
  - id: "app"
    source:
      type: postgres
      url: "postgres://user:pass@host:5432/source_db"
      cdc: true
      publication: "cdsync_pub"
      cdc_pipeline_id: 1
      table_selection:
        include: ["public.*"]
    destination:
      type: bigquery
      project_id: "your-project"
      dataset: "cdsync"
      batch_load_bucket: "your-cdsync-loads"
```

## Top-Level Keys

### `state`

Required.

```yaml
state:
  url: "postgres://user:pass@host:5432/db"
  schema: "cdsync_state"
```

- `url`: required Postgres connection string for CDSync state
- `schema`: optional, defaults to `cdsync_state`

Notes:

- schema names must contain only lowercase letters, digits, and `_`
- `cdsync migrate` creates the required state tables in this schema

### `metadata`

Optional destination metadata column names.

```yaml
metadata:
  synced_at_column: "_cdsync_synced_at"
  deleted_at_column: "_cdsync_deleted_at"
```

Defaults:

- `synced_at_column`: `_cdsync_synced_at`
- `deleted_at_column`: `_cdsync_deleted_at`

### `logging`

Optional logging settings.

```yaml
logging:
  level: "info"
  json: false
```

Defaults:

- `level`: `info`
- `json`: `false`

Notes:

- `RUST_LOG` overrides `logging.level` through `tracing_subscriber::EnvFilter`

### `admin_api`

Optional authenticated admin API.

```yaml
admin_api:
  enabled: true
  bind: "127.0.0.1:8080"
  auth:
    service_jwt_public_keys:
      my-kid: |
        -----BEGIN PUBLIC KEY-----
        ...
        -----END PUBLIC KEY-----
    service_jwt_allowed_issuers: ["https://issuer.example"]
    service_jwt_allowed_audiences: ["cdsync"]
    required_scopes: ["cdsync:admin"]
```

Fields:

- `enabled`: optional, defaults to `false`
- `bind`: optional, defaults to `127.0.0.1:8080`
- `auth`: required when `enabled: true`

`auth` fields:

- `service_jwt_public_keys`: optional inline `kid -> PEM` map
- `service_jwt_public_keys_json`: optional JSON string form of the same map
- `service_jwt_allowed_issuers`: optional list, but must resolve non-empty when enabled
- `service_jwt_allowed_audiences`: optional list, defaults to `["cdsync"]`
- `required_scopes`: optional list, defaults to `["cdsync:admin"]`

Environment fallbacks:

- `CDSYNC_SERVICE_JWT_PUBLIC_KEYS_JSON`
- `CDSYNC_SERVICE_JWT_ALLOWED_ISSUERS`
- `CDSYNC_SERVICE_JWT_ALLOWED_AUDIENCES`

### `observability`

Optional OTLP and service metadata.

```yaml
observability:
  service_name: "cdsync"
  otlp_traces_endpoint: "http://localhost:4318/v1/traces"
  otlp_metrics_endpoint: "http://localhost:4318/v1/metrics"
  otlp_headers:
    Authorization: "Bearer ..."
  metrics_interval_seconds: 30
```

Defaults:

- `service_name`: `cdsync`
- `metrics_interval_seconds`: `30`

### `sync`

Optional global execution defaults.

```yaml
sync:
  default_batch_size: 10000
  max_retries: 3
  retry_backoff_ms: 1000
  max_concurrency: 1
```

Defaults:

- `default_batch_size`: `10000`
- `max_retries`: `3`
- `retry_backoff_ms`: `1000`
- `max_concurrency`: `1`

Notes:

- polling uses `max_concurrency` for per-table parallelism
- CDC uses `max_concurrency` to bound snapshot/resync work
- a source-level `batch_size` overrides `sync.default_batch_size`

### `stats`

Optional run-history storage used by `report` and admin API run summaries.

```yaml
stats:
  url: "postgres://user:pass@host:5432/db"
  schema: "cdsync_stats"
```

Fields:

- `url`: optional, defaults to `state.url`
- `schema`: optional, defaults to `cdsync_stats`

Notes:

- if the `stats` block is omitted, runtime reporting is disabled
- schema name rules match `state.schema`
- `cdsync migrate` still applies stats migrations using default values even if `stats` is omitted

## `connections`

Required non-empty list.

```yaml
connections:
  - id: "app"
    enabled: true
    schedule:
      every: "30m"
    source: ...
    destination: ...
```

Fields:

- `id`: required connection identifier
- `enabled`: optional, defaults to `true`
- `schedule`: optional

### `schedule.every`

Only required for `cdsync run` on non-CDC connections.

Supported formats:

- `30s`
- `5m`
- `1h`
- `1d`
- `15` which means 15 seconds

Constraints:

- must be greater than zero
- supported units are `s`, `m`, `h`, `d`

## Source: PostgreSQL

Only `type: postgres` is currently supported.

```yaml
source:
  type: postgres
  url: "postgres://user:pass@host:5432/source_db"
  cdc: true
  publication: "cdsync_pub"
  publication_mode: validate
  schema_changes: auto
  cdc_pipeline_id: 1
  cdc_batch_size: 10000
  cdc_apply_concurrency: 8
  cdc_batch_load_staging_worker_count: 8
  cdc_batch_load_reducer_worker_count: 8
  cdc_max_inflight_commits: 32
  cdc_batch_load_reducer_max_jobs: 16
  cdc_batch_load_reducer_max_fill_ms: 5000
  cdc_batch_load_reducer_enabled: true
  cdc_backlog_max_pending_fragments: 10000
  cdc_backlog_max_oldest_pending_seconds: 300
  cdc_max_fill_ms: 2000
  cdc_max_pending_events: 100000
  cdc_idle_timeout_seconds: 10
  cdc_tls: false
  table_selection:
    include: ["public.*"]
    exclude: ["public.audit_*"]
    defaults:
      primary_key: "id"
      updated_at_column: "updated_at"
      soft_delete: true
      soft_delete_column: "deleted_at"
  tables:
    - name: "public.accounts"
      primary_key: "id"
```

Core fields:

- `url`: required Postgres connection string
- `tables`: optional explicit table list
- `table_selection`: optional include/exclude discovery config
- at least one of `tables` or `table_selection.include/exclude` must be present
- `batch_size`: optional polling batch size override

CDC fields:

- `cdc`: optional, defaults to `true`
- `publication`: required when `cdc: true`
- `publication_mode`: optional, defaults to `validate`
- `schema_changes`: optional, defaults to `auto`
- `cdc_pipeline_id`: required when `cdc: true`
- `cdc_batch_size`: optional, defaults to `batch_size` then `sync.default_batch_size`
- `cdc_apply_concurrency`: optional, defaults to `sync.max_concurrency`
- `cdc_batch_load_worker_count`: optional legacy batch-load worker count, defaults to `cdc_apply_concurrency`; used as the fallback for staging and reducer worker counts. Do not use this in production configs; set the split staging/reducer worker counts explicitly so deploy reviews can see actual downstream concurrency.
- `cdc_batch_load_staging_worker_count`: optional queued CDC staging/load worker count, defaults to `cdc_batch_load_worker_count`
- `cdc_batch_load_reducer_worker_count`: optional queued CDC apply/reducer worker count, defaults to `cdc_batch_load_worker_count`
- `cdc_max_inflight_commits`: optional current in-memory CDC read-ahead cap, defaults to `cdc_apply_concurrency * 4`. This is not the future durable backlog budget.
- `cdc_batch_load_reducer_max_jobs`: optional max loaded CDC jobs a table-local reducer claims at once, defaults to `16`
- `cdc_batch_load_reducer_max_fill_ms`: optional max time to wait for a table-local reducer window to fill before claiming a smaller window, defaults to `0`
- `cdc_batch_load_reducer_enabled`: optional reducer coalescing rollout gate, defaults to `true`; set `false` to force single-job apply windows
- `cdc_backlog_max_pending_fragments`: optional CDC backlog cap for WAL backpressure; it applies to durable pending fragments and, for queued batch-load CDC, pending target materialization jobs. `durable_enqueue` requires this or `cdc_backlog_max_oldest_pending_seconds`.
- `cdc_backlog_max_oldest_pending_seconds`: optional oldest-pending CDC backlog age cap for WAL backpressure; it applies to durable pending fragments and, for queued batch-load CDC, pending target materialization jobs. `durable_enqueue` requires this or `cdc_backlog_max_pending_fragments`.
- `cdc_ack_boundary: durable_enqueue`: requires the configured `state.url` to be on a different non-loopback Postgres host/port from the source Postgres URL, because the durable queue must not live on the source writer.
- Queued CDC staging and reducer claim SQL has a hard 5s timeout; repeated timeout logs mean the state database or queue indexes are unhealthy and CDSync will back off instead of keeping long-running claim sessions open.
- `cdc_max_fill_ms`: optional, defaults to `2000`
- `cdc_max_pending_events`: optional, defaults to `100000`
- `cdc_idle_timeout_seconds`: optional, defaults to `10`
- `cdc_tls`: optional, defaults to `false`
- `cdc_tls_ca_path`: optional path to CA bundle, required if `cdc_tls: true` and inline CA is absent
- `cdc_tls_ca`: optional inline CA PEM, required if `cdc_tls: true` and CA path is absent

`publication_mode` values:

- `validate`: require the publication to already match the config
- `manage`: let CDSync reconcile publication membership at startup

`schema_changes` values:

- `auto`: allow conservative additive handling, block incompatible changes
- `fail`: fail when schema drift is detected

### `table_selection`

Pattern-based table discovery.

```yaml
table_selection:
  include: ["public.*"]
  exclude: ["public.audit_*"]
  defaults:
    primary_key: "id"
    updated_at_column: "updated_at"
    soft_delete: true
    soft_delete_column: "deleted_at"
    where_clause: "tenant_id = 42"
    columns:
      include: ["id", "tenant_id", "name"]
      exclude: ["debug_blob"]
```

Fields:

- `include`: glob patterns to include discovered tables
- `exclude`: glob patterns to exclude discovered tables
- `defaults`: optional defaults applied when a resolved table omits a field

Notes:

- explicit `tables` entries are merged with pattern-resolved tables
- explicit per-table values win over `defaults`
- `exclude` patterns are still applied after merge

### Per-table fields

```yaml
tables:
  - name: "public.accounts"
    primary_key: "id"
    updated_at_column: "updated_at"
    soft_delete: true
    soft_delete_column: "deleted_at"
    where_clause: "tenant_id = 42"
    columns:
      include: ["id", "tenant_id", "name", "updated_at", "deleted_at"]
      exclude: ["secret_value"]
```

Fields:

- `name`: required fully qualified table name, for example `public.accounts`
- `primary_key`: optional; if omitted, CDSync tries to discover it
- `updated_at_column`: optional; used for polling incremental syncs
- `soft_delete`: optional, defaults to `false`
- `soft_delete_column`: optional soft-delete timestamp/tombstone column
- `where_clause`: optional source-side filter
- `row_filter`: accepted as an alias for `where_clause`
- `columns.include`: optional allowlist
- `columns.exclude`: optional denylist

Notes:

- required columns are always retained even if excluded explicitly
- required columns include the primary key and any configured `updated_at_column` or `soft_delete_column`

## Destination: BigQuery

Only `type: bigquery` is currently supported.

```yaml
destination:
  type: bigquery
  project_id: "your-project"
  dataset: "cdsync"
  location: "US"
  service_account_key_path: "/path/to/service-account.json"
  partition_by_synced_at: true
  batch_load_bucket: "your-cdsync-loads"
  batch_load_prefix: "staging/app"
```

Fields:

- `project_id`: required
- `dataset`: required
- `location`: optional
- `service_account_key_path`: optional path to a service-account JSON file
- `service_account_key`: optional inline service-account JSON
- `partition_by_synced_at`: optional, defaults to `false`
- `batch_load_bucket`: required unless `emulator_http` is set
- `batch_load_prefix`: optional object prefix inside the batch-load bucket
- `emulator_http`: optional BigQuery emulator base URL
- `emulator_grpc`: optional emulator gRPC address; requires `emulator_http`

Auth behavior:

- if `service_account_key_path` is set, CDSync uses that key
- otherwise if `service_account_key` is set, CDSync uses the inline JSON
- otherwise CDSync falls back to ambient Google auth

Batch-load behavior:

- real BigQuery CDC batch loading uses GCS via `batch_load_bucket`
- emulator mode skips that requirement

## Runtime Environment Variables

These are not config-file keys, but they affect runtime behavior:

- `RUST_LOG`: overrides `logging.level`
- `CDSYNC_SERVICE_JWT_PUBLIC_KEYS_JSON`: admin API auth fallback
- `CDSYNC_SERVICE_JWT_ALLOWED_ISSUERS`: admin API auth fallback
- `CDSYNC_SERVICE_JWT_ALLOWED_AUDIENCES`: admin API auth fallback
- `CDSYNC_DEPLOY_REVISION`: surfaced by the admin API as deploy metadata
- `CDSYNC_LAST_RESTART_REASON`: surfaced by the admin API as restart metadata
