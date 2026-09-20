# Unattended capture

Run from the **repository root**. Prefer mainnet public configs under `examples/`.

## Long-running process

Set `runtime.capture_seconds = 0` (until SIGTERM / Ctrl+C).

```bash
make build-release-capture

./scripts/run-mainnet-capture.sh examples/capture.multi-venue-mainnet.toml

# or
./scripts/run-capture-service.sh \
  --config examples/capture.multi-venue-mainnet.toml \
  --release
```

For a single product binary, build only the venues used by the config:

```bash
make build-release-capture \
  CAPTURE_FEATURES=venue-binance,venue-lighter
```

The feature list above is a **build-time** option. It must not be appended to
`catalog-capture-cli run`.

## Background with nohup

Run from the repository root. The direct binary command makes the PID file
refer to the actual recorder process, so `SIGTERM` reaches Nautilus directly.

```bash
mkdir -p logs

LOG_FILE="logs/binance-lighter-$(date -u +%Y%m%dT%H%M%SZ).log"
PID_FILE="logs/binance-lighter.pid"

nohup env NAUTILUS_LOG='stdout=Info;is_colored=false' \
  ./bin/catalog-capture-cli run \
  --config examples/capture.binance-lighter-btc-sol-perp-books.toml \
  > "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "PID: $(cat "$PID_FILE")"
echo "LOG: $LOG_FILE"
```

The current CLI uses `run`, not `capture`. It does not accept
`--log-level`, `--metrics-port`, `--no-default-features`, or `--features` as
runtime flags. Logging is configured with the `NAUTILUS_LOG` environment
variable; the default level is already `Info`.

Monitor the process and log:

```bash
tail -f "$(ls -t logs/binance-lighter-*.log | head -1)"
ps -p "$(cat logs/binance-lighter.pid)" -o pid=,etime=,command=
```

Stop it gracefully and flush/seal the active parquet parts:

```bash
kill -TERM "$(cat logs/binance-lighter.pid)"
```

Wait for `Received SIGTERM` and `Capture completed` in the log. Do not use
`kill -9`, which can leave `.parquet.part` files or lose buffered data.

For the generic wrapper, run it in the foreground or use systemd/launchd so
the service manager owns signal delivery:

```bash
./scripts/run-capture-service.sh \
  --config examples/capture.multi-venue-mainnet.toml \
  --release
```

## Metrics

Metrics are configured in TOML, not with a CLI port flag. Add this to the
configuration when monitoring is needed:

```toml
[runtime.metrics]
enabled = true
bind_addr = "127.0.0.1"
port = 9108
refresh_interval_secs = 5
```

Then query:

```bash
curl -s http://127.0.0.1:9108/metrics
```

## Health checks

If `runtime.metrics.enabled = true` (multi-venue example):

```bash
curl -s http://127.0.0.1:9108/metrics | egrep 'rss|dropped|active_partitions'
```

Option-universe lineage check (configs that write option resolutions):

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

## Optional user service

Still runs **this clone** (not `/opt`):

```bash
./scripts/optional-user-service.sh --help
```

See [cloud capture](cloud_capture.md).
