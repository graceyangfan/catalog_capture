# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Changed

- Rewrote concept docs (`architecture`, `custom-data-contract`, `production-architecture`,
  `flush-rotation-policy`, `pyo3-surface`, `live-validation`, `implementation-plan`) with
  project-owned voice and repository-relative paths.
- Rewrote `docs/rfc.md`, `docs/native-custom-data-targets.md`, and option-universe design
  docs with project-owned architecture voice.
- Renamed `docs/upstream-strategy.md` to `docs/integration-strategy.md`.

### Added

- LGPL-3.0-or-later license, `NOTICE`, and `TRADEMARK.md`.
- LGPL copyright headers on all Rust sources under `crates/`.
- `deny.toml`, `.cargo/config.toml`, and slim `.pre-commit-config.yaml`.
- Documentation map: `docs/index.md`, getting started, developer guide, how-to, reference.
- Crate-level `README.md` files.
- CI jobs for pre-commit and `cargo deny check licenses`.

### Changed

- Project license from Apache-2.0 to LGPL-3.0-or-later.

### Added (prior)

- `runtime.capture_seconds = 0` daemon mode: run until `SIGTERM` or `Ctrl+C`.
- Operator configs under `examples/operator/` for Deribit, OKX, and Bybit unattended capture.
- Deployment templates: `deploy/launchd/` and `deploy/systemd/`.
- Ops scripts: `scripts/cleanup-tmp-captures.sh`, `scripts/run-capture-service.sh`, `scripts/healthcheck-option-universe.sh`.
- `Makefile` with build, test, soak, and cleanup targets.
- GitHub Actions CI workflow for workspace unit tests.

### Changed (prior)

- Path dependencies now use `../nautilus_trader` for sibling-repo layouts.

### Fixed

- Option-universe readback Tokio nested-runtime panic (readback runs on a dedicated thread).
- Soak validation no longer requires contract state unless explicitly requested.
- `rolling-autorefresh` preset defers refresh-change checks to `--require-refresh-change`.

## 0.1.0

- Initial direct runtime-to-catalog capture CLI and option-universe validation suite.
