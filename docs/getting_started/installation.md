# Installation

## Layout

Path dependencies expect a sibling Nautilus Trader tree:

```text
~/nautilus_trader      # adapters + persistence
~/catalog_capture      # this repo (any clone name is fine)
```

## Bootstrap

```bash
make bootstrap-deps
```

1. Uses `NAUTILUS_TRADER_PATH` if set, else existing `../nautilus_trader`.
2. If missing, clones `nautechsystems/nautilus_trader` **develop** into `../nautilus_trader`.
3. Runs a workspace check.

Match CI pin after bootstrap:

```bash
./scripts/bootstrap-deps.sh --pin-ci
```

### Pinned Nautilus Trader revision (CI)

| Catalog Capture | Nautilus Trader ref |
|-----------------|---------------------|
| 0.1.x / main | `a7159b484e816a8b73388ff58db71de454253222` |

Source of truth: `NAUTILUS_TRADER_REF` in `.github/workflows/ci.yml`. Update this table when the pin changes.

## Toolchain

Rust **1.97.1** (`rust-toolchain.toml`):

```bash
rustup toolchain install 1.97.1
rustup component add rustfmt clippy --toolchain 1.97.1
```

## Build

```bash
make build                 # catalog-capture-cli, all venues
make build-release
make build-slim            # default FEATURES=venue-deribit
```

Venue features: `venue-binance`, `venue-bybit`, `venue-deribit`, `venue-okx`,
`venue-hyperliquid`, `all-venues`.

`examples/*.toml` are CLI configs, not cargo examples.

## Python (optional)

Live probes under `tests/` may need a PyO3 environment from the same Nautilus
revision. Not required for the Rust product path.
