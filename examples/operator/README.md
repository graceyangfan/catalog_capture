# Unattended operator configs

Long-running option-universe capture profiles.

## Key settings

- `runtime.capture_seconds = 0` runs until `SIGTERM` or `Ctrl+C`.
- `catalog_uri` points to a persistent directory (not `/tmp`).
- `layout_compatibility = "rust_canonical_only"`.
- Flush profile **B** (unattended mixed universe): `flush_rows = 5000`,
  `flush_interval_ms = 5000`, `max_buffer_bytes = 64MiB` — see
  `docs/concepts/flush_and_rotation.md`.

```bash
sudo mkdir -p /var/lib/catalog-capture/{deribit,okx,bybit}-btc-universe
sudo chown -R "$USER" /var/lib/catalog-capture
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

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

## macOS launchd

1. Edit `deploy/launchd/com.github.catalog-capture.deribit.plist` paths if needed.
2. Install:

```bash
cp deploy/launchd/com.github.catalog-capture.deribit.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.github.catalog-capture.deribit.plist
```

Stop:

```bash
launchctl unload ~/Library/LaunchAgents/com.github.catalog-capture.deribit.plist
```

## Linux systemd

1. Edit `deploy/systemd/catalog-capture@.service` paths and service user if needed.
2. Install:

```bash
sudo cp deploy/systemd/catalog-capture@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now catalog-capture@deribit-btc-universe
```

Point `Environment=CATALOG_CAPTURE_CONFIG` at the desired TOML file when the
instance name does not match an examples path.
