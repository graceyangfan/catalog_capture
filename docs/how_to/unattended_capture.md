# Unattended capture

## Daemon mode

Set `runtime.capture_seconds = 0` in your TOML. The process runs until `SIGTERM` or
`Ctrl+C`, suitable for launchd/systemd supervision.

Production-shaped configs:

- `examples/operator/capture.deribit-btc-universe-unattended.toml`
- `examples/operator/capture.okx-btc-universe-unattended.toml`
- `examples/operator/capture.bybit-btc-universe-unattended.toml`

## Run with logging

```bash
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Add `--validate` to run `validate-option-universe` after graceful shutdown.

## Health check

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

## Deployment templates

- macOS: `deploy/launchd/`
- Linux: `deploy/systemd/`

See `examples/operator/README.md` for install steps.
