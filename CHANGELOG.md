# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- `runtime.capture_seconds = 0` daemon mode: run until `SIGTERM` or `Ctrl+C`.
- Operator configs under `examples/operator/` for Deribit, OKX, and Bybit unattended capture.
- Deployment templates: `deploy/launchd/` and `deploy/systemd/`.
- Ops scripts: `scripts/cleanup-tmp-captures.sh`, `scripts/run-capture-service.sh`, `scripts/healthcheck-option-universe.sh`.
- `Makefile` with build, test, soak, and cleanup targets.
- GitHub Actions CI workflow for workspace unit tests.
- Apache-2.0 `LICENSE`.

### Changed

- `nautilus_trader` path dependencies now use `../nautilus_trader` for sibling-repo layouts.

### Fixed

- Option-universe readback Tokio nested-runtime panic (readback runs on a dedicated thread).
- Soak validation no longer requires contract state unless explicitly requested.
- `rolling-autorefresh` preset defers refresh-change checks to `--require-refresh-change`.

## 0.1.0

- Initial direct runtime-to-catalog capture CLI and option-universe validation suite.