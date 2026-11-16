# Roadmap

Catalog Capture records live market data into Rust-native Nautilus Parquet
catalogs. Current behavior and supported venues are documented in the
[operator guides](docs/index.md); implementation history remains in Git.

## Planned work

- Scheduled live smoke checks against the pinned Nautilus revision.
- A separate HIP-4 feature for slimmer deployment graphs.
- Per-family flush overrides where operational measurements justify them.
- An explicit configuration schema version for the next incompatible TOML change.

## Dependency updates

Use the sibling `../nautilus_trader_quantity_commission` fork. Keep the immutable
revision in `.github/workflows/ci.yml`, `scripts/bootstrap-deps.sh` and
[installation](docs/getting_started/installation.md) synchronized. Push the fork
revision before using it in CI, and validate affected Rust feature combinations.
