# Operations

This document is the short operator-facing command reference for the current CLI.

## Config

Use `cdsync init` to generate a starter config:

```bash
cdsync init --config config.yaml
```

CDSync accepts YAML and TOML configs. Destination metadata columns can be customized at the top level:

```yaml
metadata:
  synced_at_column: "_cdsync_synced_at"
  deleted_at_column: "_cdsync_deleted_at"
```

For the full field-by-field config reference, see [configuration.md](configuration.md).

## Core Commands

Apply state and stats migrations:

```bash
cdsync migrate --config config.yaml
```

Validate source and destination connectivity:

```bash
cdsync validate --config config.yaml --connection app --verbose
```

Run a one-shot sync:

```bash
cdsync run --config config.yaml --connection app --once --incremental
```

Force a full one-shot refresh:

```bash
cdsync run --config config.yaml --connection app --once --full
```

Inspect state and recent runs:

```bash
cdsync status --config config.yaml --connection app
cdsync report --config config.yaml --connection app --limit 10
cdsync reconcile --config config.yaml --connection app
```

## Long-Lived Runner Mode

Use `cdsync run` without `--once` when the process should stay alive under a supervisor.

CDC connections:

```bash
cdsync run --config config.yaml --connection app
```

Polling connections:

- require `schedule.every`
- run incremental syncs on that interval

Supported interval examples:

- `30s`
- `5m`
- `1h`
- `1d`
- `15` for 15 seconds

`SIGINT` and `SIGTERM` are handled cleanly:

- polling runners finish the current sync cycle, then exit
- CDC runners stop at a safe boundary and persist the last checkpoint

## Deployment Boundary

This repo does not ship service units, ECS manifests, or environment-specific deployment templates.

The intended split is:

- this repo owns the binary, config schema, and generic runner image
- downstream infra repos own process supervision, secrets injection, and scheduler-specific rollout config

## Container

Build the runner image:

```bash
docker build -f docker/Dockerfile.runner -t cdsync-runner .
```

Run it:

```bash
docker run --rm \
  -v "$(pwd)/config.yaml:/etc/cdsync/config.yaml:ro" \
  cdsync-runner --config /etc/cdsync/config.yaml --connection app
```

The image entrypoint is already `cdsync run`.

## Releases

GitHub releases publish Linux tarballs and matching checksum files.

Current workflow behavior:

- tagged releases build `x86_64` automatically
- optional `aarch64` builds are available through the manual release workflow

## Real BigQuery

Live BigQuery tests use the environment variables in `.env.example`. See [e2e.md](e2e.md) for the current test commands.
