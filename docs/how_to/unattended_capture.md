# Unattended capture

Run from the **repository root**. Set `runtime.capture_seconds = 0` to run until
`SIGTERM` / `Ctrl+C`. Catalogs land under `./data/…` (see TOML).

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

Optional user-level service (still uses this clone, not `/opt`):  
`./scripts/optional-user-service.sh --help` — see [deploy/README.md](../../deploy/README.md).
