# Unattended configs

Long-running option-universe profiles. Run from the **repository root** after build.

## Settings

- `runtime.capture_seconds = 0` — until SIGTERM / Ctrl+C
- `catalog_uri` — under `./data/…` (created on first run)
- Flush profile B — see `docs/concepts/flush_and_rotation.md`

```bash
mkdir -p data
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Optional post-run check: add `--validate`.

## Health check

```bash
./scripts/healthcheck-option-universe.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

## Optional user service

Not required. If you want launchd/systemd for *this clone*:

```bash
./scripts/optional-user-service.sh --platform launchd \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

See [deploy/README.md](../../deploy/README.md).
