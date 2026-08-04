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

1. **C4** — examples directory tiers (`minimal` / `research` / `operator`)  
2. **C5** — core public API surface docs  
3. **R3** — per-family flush guidance tables  
4. Optional: Docker/GHCR (B3), HIP-4 optional feature to slim residual HL link  
5. Nightly live smoke against pinned nautilus rev  

Done recently: L5/L6 offline layout write proofs + custom path audit (`catalog_layout`).  

## Historical phases

Older “Phase 1–3 / Step N” lists are **historical**. Treat
[docs/refactor-optimization-plan.md](docs/refactor-optimization-plan.md) as the
source of truth for open work. See also:

- [docs/implementation-plan.md](docs/implementation-plan.md) (historical)  
- [docs/stepwise-capture-roadmap.md](docs/stepwise-capture-roadmap.md) (historical)  
- [docs/segment-lifecycle.md](docs/segment-lifecycle.md) (Track S — largely done)  
