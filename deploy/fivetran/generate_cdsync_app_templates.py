#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


STAGING_HOST = "staging-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com"
PRODUCTION_HOST = "production-postgres-private.c0qssstf2cvw.us-east-1.rds.amazonaws.com"
PUBLICATION = "cdsync_app_pub"
NORA_PRODUCTION_HOST = "nora-production-postgres-read.c0qssstf2cvw.us-east-1.rds.amazonaws.com"
NORA_PUBLICATION = "cdsync_nora_pub"


def load_schema_export(path: Path) -> dict:
    return json.loads(path.read_text())


def enabled_tables(schema_export: dict) -> list[dict]:
    tables: list[dict] = []
    for schema_name, schema in sorted(schema_export["data"]["schemas"].items()):
        if not schema.get("enabled", False):
            continue
        for table_name, table in sorted(schema.get("tables", {}).items()):
            if not table.get("enabled", False):
                continue
            columns = table.get("columns", {})
            enabled_columns = [
                name for name, cfg in sorted(columns.items()) if cfg.get("enabled", False)
            ]
            has_disabled_columns = any(not cfg.get("enabled", False) for cfg in columns.values())
            tables.append(
                {
                    "name": f"{schema_name}.{table_name}",
                    "soft_delete": table.get("sync_mode") == "SOFT_DELETE",
                    "enabled_columns": enabled_columns,
                    "has_disabled_columns": has_disabled_columns,
                }
            )
    return tables


def render_tables(tables: list[dict]) -> str:
    parts: list[str] = []
    for table in tables:
        parts.append("[[connections.source.tables]]")
        parts.append(f'name = "{table["name"]}"')
        parts.append(f'soft_delete = {"true" if table["soft_delete"] else "false"}')
        if table["has_disabled_columns"]:
            include = ", ".join(json.dumps(column) for column in table["enabled_columns"])
            parts.append("[connections.source.tables.columns]")
            parts.append(f"include = [{include}]")
            parts.append("exclude = []")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def render_template(
    *,
    source_connection_id: str,
    source_connection_dir: str,
    host: str,
    connection_id: str,
    service_name: str,
    dataset: str,
    tables: list[dict],
    publication: str,
) -> str:
    header = f"""# Generated from Fivetran connection `{source_connection_id}`.
# Source of truth:
#   .secrets/fivetran/{source_connection_dir}/connection.json
#   .secrets/fivetran/{source_connection_dir}/schemas.json
#
# Notes:
# - Primary keys are auto-discovered from Postgres by default.
# - For critical tables or nonstandard keys, you can add `primary_key = "..."` overrides per table.
# - This template represents the full app connector table set, not the current staging pilot publication.
# - `schema_changes = "auto"` matches the current intended CDSync default for additive changes.

[state]
url = "postgres://__STATE_USER__:__STATE_PASSWORD__@{host}:5432/resq?sslmode=require"
schema = "cdsync_state"

[logging]
level = "info"
json = true

[observability]
service_name = "{service_name}"
metrics_interval_seconds = 30

[sync]
default_batch_size = 5000
max_retries = 5
retry_backoff_ms = 1000
max_concurrency = 4

[stats]
url = "postgres://__STATE_USER__:__STATE_PASSWORD__@{host}:5432/resq?sslmode=require"
schema = "cdsync_stats"

[[connections]]
id = "{connection_id}"
enabled = true

[connections.source]
type = "postgres"
url = "postgres://__POSTGRES_USER__:__POSTGRES_PASSWORD__@{host}:5432/resq?sslmode=require"
cdc = true
publication = "{publication}"
schema_changes = "auto"
cdc_pipeline_id = 1101
cdc_batch_size = 5000
cdc_max_fill_ms = 2000
cdc_max_pending_events = 100000
cdc_idle_timeout_seconds = 10
cdc_tls = true
cdc_tls_ca_path = "/etc/ssl/certs/ca-certificates.crt"

"""
    destination = f"""
[connections.destination]
type = "bigquery"
project_id = "__BIGQUERY_PROJECT_ID__"
dataset = "{dataset}"
location = "US"
service_account_key_path = "/etc/cdsync/gcp-key.json"
partition_by_synced_at = true
storage_write_enabled = true
"""
    return header + render_tables(tables) + destination.lstrip()


def main() -> None:
    schema_export = load_schema_export(Path(".secrets/fivetran/prove_gentle/schemas.json"))
    tables = enabled_tables(schema_export)
    nora_schema_export = load_schema_export(Path(".secrets/fivetran/languor_withdrawn/schemas.json"))
    nora_tables = enabled_tables(nora_schema_export)
    output_dir = Path("deploy/config")
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "cdsync-app.tables.generated.toml").write_text(render_tables(tables))
    (output_dir / "cdsync-app.staging.template.toml").write_text(
        render_template(
            source_connection_id="prove_gentle",
            source_connection_dir="prove_gentle",
            host=STAGING_HOST,
            connection_id="app_staging",
            service_name="staging-cdsync-app",
            dataset="cdsync_app_staging",
            tables=tables,
            publication=PUBLICATION,
        )
    )
    (output_dir / "cdsync-app.production.template.toml").write_text(
        render_template(
            source_connection_id="prove_gentle",
            source_connection_dir="prove_gentle",
            host=PRODUCTION_HOST,
            connection_id="app_production",
            service_name="production-cdsync-app",
            dataset="cdsync_app_production",
            tables=tables,
            publication=PUBLICATION,
        )
    )
    (output_dir / "cdsync-nora.tables.generated.toml").write_text(render_tables(nora_tables))
    (output_dir / "cdsync-nora.production.template.toml").write_text(
        render_template(
            source_connection_id="languor_withdrawn",
            source_connection_dir="languor_withdrawn",
            host=NORA_PRODUCTION_HOST,
            connection_id="nora_production",
            service_name="cdsync-nora-production",
            dataset="cdsync_nora_production",
            tables=nora_tables,
            publication=NORA_PUBLICATION,
        )
    )


if __name__ == "__main__":
    main()
