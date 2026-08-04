# Unattended capture

Set `runtime.capture_seconds = 0` to run until `SIGTERM` / `Ctrl+C`.

Operator configs:

- `examples/operator/capture.deribit-btc-universe-unattended.toml`
- `examples/operator/capture.okx-btc-universe-unattended.toml`
- `examples/operator/capture.bybit-btc-universe-unattended.toml`

```bash
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Health check:

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

Deploy templates: `deploy/launchd/`, `deploy/systemd/`.  
Details: [examples/operator/README.md](../../examples/operator/README.md).
