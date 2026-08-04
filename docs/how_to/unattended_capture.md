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

Background:

```bash
mkdir -p logs
nohup ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml \
  > logs/nohup.out 2>&1 &
echo $! > logs/capture.pid

kill -TERM "$(cat logs/capture.pid)"
```

## Health / metrics

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
