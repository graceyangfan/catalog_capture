# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Fixed

- **Segment durability tick** no longer calls `ArrowWriter::flush` (that sealed a
  parquet row group every sync interval and could hit the 32 767 RG/file limit on
  Deribit BookSummary-style custom streams). Tick only fsyncs the open `.part`.
- **Custom/BookSummary** memory flush stays ~1 000 rows/poll while parquet row groups
  target **50 000** rows so a day part stays far under the RG limit.
- **Capacity roll:** open parts seal and reopen near ~30 000 flushed row groups
  (same path as day seal; mid-day multi-file is valid).

### Changed

- **Bootstrap default:** `make bootstrap-deps` now runs with `--pin-ci` so first-time
  builds match CI’s Nautilus revision. Use `make bootstrap-deps-local` for an
  editable sibling tree without forcing the pin.
- **README Status:** document 0.1.x early-open-source expectations and multi-day
  mainnet soak validation.
- **Repository:** GitHub path is `graceyangfan/catalog_capture` (was
  `nautilus_catalog_capture`).
- **Local-first paths:** examples and defaults use `file://./data/…` under the repo.
  Removed system install roots (`/opt`, `/var/lib`). Optional user service helper only:
  `scripts/optional-user-service.sh`.
- **Product branding:** public name is **Catalog Capture** (independent, unofficial).
  CLI clap name is `catalog-capture-cli` (was `nautilus-capture`).
- Credentials simplified to two modes: public (`None`) or complete env key+secret pair.
- **Documentation** restructured like Nautilus Trader (getting_started / concepts /
  how_to / developer_guide / reference). Removed historical plan docs, design dumps,
  `dev/legacy-examples/`, and `research/`.
- **Tooling:** pre-commit uses stable `cargo fmt` (empty-config pattern) + product-crate
  clippy; toolchain 1.97.1. Clippy `doc-valid-idents` limited to supported venues only.

### Added

- Offline catalog layout proofs and capture → `ParquetDataCatalog` readback tests.
- Examples policy: TOML configs only for the single product binary.
- Community hygiene: `SECURITY.md`, `CODE_OF_CONDUCT.md`, issue/PR templates,
  `tests/README.md`.


## 0.1.0

First public-shaped baseline: single CLI, Rust-canonical catalog only, multi-venue
features, bootstrap script, custom subscribe/request registry, request-path metrics,
optional env credentials, and `metadata/capture_run.json`.

### Breaking

- Removed Python legacy catalog path mirroring.
  Only Nautilus Trader **Rust** `ParquetDataCatalog` layout is written
  (`layout_compatibility = "rust_canonical_only"`, the sole accepted value).
  Configs using `rust_canonical_with_python_legacy_mirror` fail validation with a migration message.
  Use the same `catalog_uri` for Rust backtest without conversion.
- Removed cargo `[[example]]` binaries from `catalog-capture-runtime-adapter`.
  The **only product binary** is `catalog-capture-cli`. Former examples live under
  `dev/legacy-examples/` and are not built. Use TOML configs + the CLI instead.

### Changed

- Workspace Cargo profiles aligned with Nautilus Trader lean-dev practice
  (`debug = false`, `strip = "debuginfo"`, third-party `opt-level = 1` in dev).
- Dropped Hyperliquid `python` feature from workspace deps (Rust capture only).
- Makefile: toolchain 1.97.1; `build` targets only `catalog-capture-cli`; added `clean` / `clean-debug`.
- Unified getting-started and live-validation docs on plain `cargo` commands
  (`rust-toolchain.toml` pins 1.97.1).
- CI: toolchain **1.97.1**; pin `nautilus_trader` to fixed rev
  (`NAUTILUS_TRADER_REF`, not `develop`); slim `venue-deribit` compile check.
- `catalog-capture-cli` optional venue cargo features (`venue-*` / `all-venues`,
  default `all-venues`) so operators can slim the adapter graph.
- Split `catalog-capture-cli` `config.rs` into `config/` modules (Track C1):
  `runtime`, `output`, `capture`, `plan`, `custom`, `option_universe`, `hip4`, `venues`.
- Custom-data **registry** (Track C2): `custom_data/` owns subscribe vs request
  type names; config parse, runtime validate, and adapter register share one source.
- `scripts/bootstrap-deps.sh` + `make bootstrap-deps` (Track O3): prefer local
  `nautilus_trader`, else clone `nautechsystems/nautilus_trader` **develop**; optional `--pin-ci`.
- README first-screen rewrite (Track O4): positioning, boundaries, multi-venue,
  Rust catalog, three happy paths (bootstrap / validate / live).
- Removed empty `catalog-capture-plugin-adapter` directory (Track O5).
- Strengthened unofficial / LGPL / trademark language in README, NOTICE, TRADEMARK (O7).
- C3 tests: bidirectional reject of subscribe vs request custom-data channel misuse.

### Added

- Request-path metrics (R1) on `/metrics` and `/metrics.json`:
  `catalog_capture_custom_data_request_{polls,rows,skipped_inflight,timeouts}_total`
  and `…_in_flight` (per-job labels + aggregates). Soak table updated (R2).
- Optional venue credentials from environment (O8); see `docs/how_to/credentials.md`
  and `.env.example`. Default remains public data.
- Startup `metadata/capture_run.json` (L7): node, venues, plan summary, CLI features,
  optional `NAUTILUS_TRADER_REF`.
- CLI feature flags: `venue-binance`, `venue-bybit`, `venue-deribit`, `venue-okx`,
  `venue-hyperliquid`, `all-venues` (default).
- LGPL-3.0-or-later license, `NOTICE`, and `TRADEMARK.md`.
- LGPL copyright headers on all Rust sources under `crates/`.
- `deny.toml`, `.cargo/config.toml`, and slim `.pre-commit-config.yaml`.
- Documentation map: `docs/index.md`, getting started, developer guide, how-to, reference.
- Crate-level `README.md` files.
- CI jobs for pre-commit and `cargo deny check licenses`.

### Docs / packaging

- Rewrote root `README.md`, crate READMEs, and deployment templates with
  project-owned voice and repository-relative paths.
- Smoke tests and catalog probes resolve `../nautilus_trader` via `tests/nautilus_import.py`
  instead of machine-specific absolute paths.
- Rewrote `examples/README.md` and `ROADMAP.md` with project-owned voice and
  repository-relative paths.
- Rewrote concept docs (`architecture`, `custom-data-contract`, `production-architecture`,
  `flush-rotation-policy`, `pyo3-surface`, `live-validation`, `implementation-plan`) with
  project-owned voice and repository-relative paths.
- Rewrote `docs/rfc.md`, `docs/native-custom-data-targets.md`, and option-universe design
  docs with project-owned architecture voice.
- Renamed `docs/upstream-strategy.md` to `docs/integration-strategy.md`.
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
