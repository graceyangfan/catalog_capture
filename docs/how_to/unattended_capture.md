# Unattended capture

Run from the **repository root**. Prefer mainnet public configs under `examples/`.

## Build the product binary

Set `runtime.capture_seconds = 0` so the recorder runs until `SIGTERM` or
`Ctrl+C`. Build once, validate once, then run the binary directly.

```bash
cargo build --release -p catalog-capture-cli \
  --no-default-features \
  --features venue-binance,venue-lighter,venue-extended

mkdir -p bin
install -m 755 target/release/catalog-capture-cli bin/catalog-capture-cli

./bin/catalog-capture-cli validate \
  --config examples/capture.binance-lighter-extended-btc-sol-perp-books.toml
```

For another configuration, set the feature list to exactly the venues it uses.
Cargo feature flags belong to the build command, never to
`catalog-capture-cli run`. `make build-release-capture` is also valid for the
standard profile; override `CAPTURE_FEATURES` when the profile includes
Extended:

```bash
CAPTURE_FEATURES=venue-binance,venue-lighter,venue-extended \
  make build-release-capture
```

## Background with nohup

Run from the repository root. Start the product binary itself, not
`run-capture-service.sh`: the wrapper is a foreground build/logging helper and
its `tee` pipeline is not the recorder PID. `nohup`, detached stdin, and a
direct log redirect keep the recorder alive after the SSH shell closes while
leaving the PID file pointing at the actual Nautilus process.

```bash
mkdir -p logs

CONFIG="examples/capture.binance-lighter-extended-btc-sol-perp-books.toml"
NAME="binance-lighter-extended"
LOG_FILE="logs/${NAME}-$(date -u +%Y%m%dT%H%M%SZ).log"
PID_FILE="logs/${NAME}.pid"

if [[ -s "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Recorder already running with PID $(cat "$PID_FILE")" >&2
  exit 1
fi
rm -f "$PID_FILE"

nohup env NAUTILUS_LOG='stdout=Info;is_colored=false' \
  ./bin/catalog-capture-cli run \
  --config "$CONFIG" \
  >"$LOG_FILE" 2>&1 </dev/null &

PID=$!
echo "$PID" > "$PID_FILE"
echo "PID: $(cat "$PID_FILE")"
echo "LOG: $LOG_FILE"
```

The current CLI uses `run`, not `capture`. It does not accept
`--log-level`, `--metrics-port`, `--no-default-features`, or `--features` as
runtime flags. Logging is configured with the `NAUTILUS_LOG` environment
variable; metrics are configured in TOML.

Monitor the process and log:

```bash
PID_FILE="logs/binance-lighter-extended.pid"
ps -p "$(cat "$PID_FILE")" -o pid=,ppid=,sid=,pgid=,etime=,stat=,command=
tail -f "$(ls -t logs/binance-lighter-extended-*.log | head -1)"
```

Stop it gracefully and flush/seal the active parquet parts:

```bash
PID_FILE="logs/binance-lighter-extended.pid"
PID="$(cat "$PID_FILE")"
kill -TERM "$PID"

for _ in $(seq 1 60); do
  kill -0 "$PID" 2>/dev/null || break
  sleep 1
done

if kill -0 "$PID" 2>/dev/null; then
  echo "Recorder did not stop within 60 seconds; keep $PID_FILE for diagnosis" >&2
  exit 1
fi

rm -f "$PID_FILE"
```

Confirm `Received SIGTERM` and `Capture completed` in the log. Do not use
`kill -9` during normal operation: it can leave `.parquet.part` files or lose
buffered data.

This method protects against SSH disconnects, but it does not restart a process
after OOM, machine reboot, or an external `SIGKILL`. Restart it manually if
`ps` shows that the PID is gone. The generic wrapper remains useful as a
foreground build/run helper:

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

See [cloud capture](cloud_capture.md) for catalog inspection and cleanup.
