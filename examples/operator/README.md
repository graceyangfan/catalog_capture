# Unattended operator configs

These configs target long-running, production-shaped option-universe capture.

## Key settings

- `runtime.capture_seconds = 0` runs until `SIGTERM` or `Ctrl+C`.
- `catalog_uri` points to a persistent directory (not `/tmp`).
- Flush thresholds are more conservative than smoke examples; see `docs/flush-rotation-policy.md`.

Before deploying, create the catalog root and ensure the service user can write to it:

```bash
sudo mkdir -p /var/lib/nautilus-catalog-capture/{deribit,okx,bybit}-btc-universe
sudo chown -R "$USER" /var/lib/nautilus-catalog-capture
```

## Run manually

```bash
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Add `--validate` to run `validate-option-universe` after a graceful shutdown.

## Health checks

While a capture job is running (or after restart), metadata lineage can be checked without stopping the process:

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

## macOS launchd

1. Edit `deploy/launchd/com.nautilus.catalog-capture.deribit.plist` paths if needed.
2. Install:

```bash
cp deploy/launchd/com.nautilus.catalog-capture.deribit.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.nautilus.catalog-capture.deribit.plist
```

3. Stop:

```bash
launchctl unload ~/Library/LaunchAgents/com.nautilus.catalog-capture.deribit.plist
```

## Linux systemd

1. Edit `deploy/systemd/catalog-capture@.service` paths if needed.
2. Install:

```bash
sudo cp deploy/systemd/catalog-capture@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now catalog-capture@deribit-btc-universe
```

The instance name is informational; point `Environment=CATALOG_CAPTURE_CONFIG` at the desired TOML file.
