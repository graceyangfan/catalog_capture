# Roadmap

## Product direction

Standalone **write-focused** capture service for research-grade derivatives data:

- ingress via sibling `../nautilus_trader` venue adapters  
- write **Nautilus Trader Rust-canonical** Parquet (`rust_canonical_only`)  
- consumers: **Rust backtest**, research, optional PyO3 readback  

**Active plan:** [docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md)

## Current status (0.1.0 baseline)

| Area | Status |
|------|--------|
| Single product CLI + TOML | done |
| Multi-venue features (`venue-*` / `all-venues`) | done |
| Bootstrap sibling nautilus_trader | done (`make bootstrap-deps`) |
| Custom subscribe vs request registry | done |
| Request-path `/metrics` | done |
| Optional API keys from env | done |
| `metadata/capture_run.json` | done |
| Unattended / segment lifecycle | done (see segment-lifecycle docs) |

## Next (post 0.1.0)

1. Optional: physical examples subdirs; Docker/GHCR; HIP-4 optional feature  
2. Optional: per-family flush **code** overrides (docs already have R3 table)  
3. Nightly live smoke against pinned nautilus rev  
4. Optional: full `BacktestNode` smoke (catalog query already proves load)  

Done recently:

- L5/L6 offline write + **Rust `ParquetDataCatalog` readback**  
- Examples = **configs only** + one product CLI  
- **R3** flush guidance table · **C5** core stable API README · **R4** operator canonical  

## Historical phases

Older “Phase 1–3 / Step N” lists are **historical**. Treat
[docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md) as the
source of truth for open work. See also:

- [docs/implementation-plan.md](docs/implementation-plan.md) (historical)  
- [docs/stepwise-capture-roadmap.md](docs/stepwise-capture-roadmap.md) (historical)  
- [docs/segment-lifecycle.md](docs/segment-lifecycle.md) (Track S — largely done)  
