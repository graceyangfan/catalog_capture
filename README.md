# Catalog Capture

Independent, community-maintained capture tool that writes live market data into
[Nautilus Trader](https://github.com/nautechsystems/nautilus_trader) **Rust
`ParquetDataCatalog`** layouts for direct backtest use.

Not affiliated with or endorsed by Nautech Systems Pty Ltd.
See [TRADEMARK.md](TRADEMARK.md) and [NOTICE](NOTICE).

```text
venues → catalog-capture-cli + TOML
      → file://…/data/…   (rust_canonical_only)
      → ParquetDataCatalog / Rust backtest
```

## Features

- **Single product binary** — `catalog-capture-cli` (configs are TOML only)
- **Rust-canonical catalog** — no Feather convert, no Python legacy path mirror
- **Multi-venue** — Binance Futures, Deribit, Bybit, OKX, Hyperliquid (cargo features)
- **Custom data** — subscribe vs request channels stay separate
- **Ops-ready** — unattended run, segment seal, optional metrics HTTP

## Quick start

Requires Rust **1.97.1** and a sibling `../nautilus_trader` checkout.
Run everything from the **repository root** (no system install).

```bash
make bootstrap-deps
make build

cargo run -p catalog-capture-cli -- validate --config examples/capture.toml
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
# writes under ./data/… (gitignored)

# Offline: write path is catalog-readable
cargo test -p catalog-capture-core --lib catalog_layout
```

Slim build (one venue):

```bash
cargo build -p catalog-capture-cli --no-default-features --features venue-deribit
```

## Documentation

Aligned with the [Divio](https://docs.divio.com/documentation-system/) layout used by Nautilus Trader:

| Section | Path |
|---------|------|
| Getting started | [docs/getting_started/](docs/getting_started/) |
| Concepts | [docs/concepts/](docs/concepts/) |
| How-to | [docs/how_to/](docs/how_to/) |
| Developer guide | [docs/developer_guide/](docs/developer_guide/) |
| CLI reference | [docs/reference/cli.md](docs/reference/cli.md) |
| Examples | [examples/README.md](examples/README.md) |
| Doc map | [docs/index.md](docs/index.md) |

## Credentials

Public capture is the default (leave API env vars unset). For authenticated venues, set a complete key+secret pair in the environment — never in TOML. See [credentials](docs/how_to/credentials.md).

## Development

```bash
make bootstrap-deps
make install-tools
pip install pre-commit && pre-commit install
make test && make clippy && make cargo-deny
```

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [ROADMAP.md](ROADMAP.md).

## License

[LGPL-3.0-or-later](LICENSE). Links against Nautilus Trader (LGPL). Third-party notices: [NOTICE](NOTICE).
