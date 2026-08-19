# Roadmap

## Direction

Write-focused capture for research-grade derivatives data:

- sibling `../nautilus_trader` adapters
- Rust-canonical Parquet (`rust_canonical_only`)
- consumers: Rust backtest and research loaders

Independent product (**Catalog Capture**). Nautilus names are compatibility only —
see [TRADEMARK.md](TRADEMARK.md).

## Status (0.1.x)

| Area | Status |
|------|--------|
| Single CLI + TOML | done |
| Venue features | done |
| Bootstrap + CI pin | done |
| Custom subscribe / request | done |
| Metrics + capture_run metadata | done |
| Segment lifecycle / unattended | done |
| Catalog write + ParquetDataCatalog readback | done |
| Independent branding / docs | done |
| Row-group capacity (BookSummary long parts) | done |
| Multi-day mainnet unattended soak | done (operator-validated) |

## Next

1. Minimal BacktestNode (or load) smoke beyond catalog query
2. Nightly live smoke against pinned Nautilus rev (when CI minutes available)
3. Optional: HIP-4 as a true optional feature
4. Optional: per-family flush overrides in TOML
5. Config `schema_version` when TOML next breaks

When changing `NAUTILUS_TRADER_REF`, update `.github/workflows/ci.yml` and
[installation](docs/getting_started/installation.md).
