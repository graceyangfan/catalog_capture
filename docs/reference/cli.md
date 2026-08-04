# CLI reference

Binary: `catalog-capture-cli`.

## Capture

```bash
cargo run -p catalog-capture-cli -- run --config <path.toml>
```

## Config

```bash
cargo run -p catalog-capture-cli -- validate --config <path.toml>
cargo run -p catalog-capture-cli -- print-effective-config --config <path.toml>
```

## Option universe

| Subcommand | Purpose |
|------------|---------|
| `inspect-option-universe` | Summarize resolution lineage |
| `validate-option-universe-metadata` | Check resolution metadata |
| `validate-option-universe-readback` | Catalog readback |
| `validate-option-universe-catalog` | On-disk parquet checks |
| `validate-option-universe` | Full suite |

```bash
cargo run -p catalog-capture-cli -- validate-option-universe \
  --config examples/capture.deribit-btc-universe-autorefresh.toml \
  --option-universe-format text
```

Use `--help` on any subcommand for flags.
