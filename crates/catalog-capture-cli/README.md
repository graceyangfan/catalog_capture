# catalog-capture-cli

Product binary: TOML-driven capture, validation, and option-universe reporting.

Run these commands from the repository root:

```bash
./scripts/cargo.sh run -p catalog-capture-cli -- validate --config examples/capture.toml
./scripts/cargo.sh run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

See [docs/reference/cli.md](../../docs/reference/cli.md).
