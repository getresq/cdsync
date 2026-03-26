# Operations

## Runner Mode

Use `run` when you want CDSync to stay alive under a process manager instead of invoking one-shot `sync`.

### PostgreSQL CDC

For PostgreSQL connections with `cdc: true`, `run` uses long-lived CDC follow mode and exits cleanly on `SIGINT` or `SIGTERM`.

```bash
cdsync run --config ./config.yaml --connection app
```

### Polling Sources

For polling-based connections, `run` requires `schedule.every` on the connection and performs incremental syncs on that interval.

Supported `schedule.every` values:

- `30s`
- `5m`
- `1h`
- `1d`
- `15` (defaults to seconds)

## systemd

Install the binary and config, then use the instance unit:

```bash
sudo cp deploy/systemd/cdsync@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cdsync@app
```

This runs:

```bash
cdsync run --config /etc/cdsync/config.yaml --connection app
```

## Container

Build the runner image:

```bash
docker build -f docker/Dockerfile.runner -t cdsync-runner .
```

Run it:

```bash
docker run --rm \
  -v $(pwd)/config.yaml:/etc/cdsync/config.yaml:ro \
  -v $(pwd)/state:/var/lib/cdsync \
  cdsync-runner --config /etc/cdsync/config.yaml --connection app
```

## Graceful Shutdown

- `SIGINT` and `SIGTERM` are handled.
- Polling runners finish the current sync cycle, then exit.
- CDC runners stop following the stream after the current transaction boundary and persist the last known checkpoint.
