# Quickstart

After [installation](installation.md):

```bash
make bootstrap-deps
make build

cargo run -p catalog-capture-cli -- validate --config examples/capture.toml
cargo test -p catalog-capture-core --lib catalog_layout
```

## First live run

Examples write under `./data/…` (gitignored). From the repo root:

```bash
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

More profiles: [examples/README.md](../../examples/README.md).

## Next

- [Rust backtest from catalog](../how_to/rust_backtest_from_catalog.md)
- [Credentials](../how_to/credentials.md)
- [Unattended capture](../how_to/unattended_capture.md)
