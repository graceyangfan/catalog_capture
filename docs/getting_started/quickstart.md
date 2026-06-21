# Quickstart

Run a 30-second Deribit option-universe smoke capture and validate the catalog:

```bash
cd nautilus_catalog_capture
python3 tests/probe_option_universe_smoke.py \
  --venue deribit-autorefresh \
  --seconds 30 \
  --cleanup
```

The probe writes a temporary catalog under `/tmp`, runs `validate-option-universe`, then
removes artifacts when `--cleanup` is set.

## CLI-only validation

After a manual capture:

```bash
cargo run -p catalog-capture-cli -- run \
  --config examples/capture.deribit-btc-universe-autorefresh.toml

cargo run -p catalog-capture-cli -- validate-option-universe \
  --config examples/capture.deribit-btc-universe-autorefresh.toml \
  --catalog-uri file:///tmp/nautilus-catalog-capture-deribit-btc-universe-autorefresh \
  --option-universe-format text
```

## Next steps

- [Smoke and soak](../how_to/smoke_and_soak.md)
- [Unattended capture](../how_to/unattended_capture.md)
- [CLI reference](../reference/cli.md)