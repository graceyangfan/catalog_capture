# Legacy cargo examples (not part of the product build)

These files were formerly registered as `[[example]]` binaries under
`catalog-capture-runtime-adapter`. They are **not** built by default and are
**not** the supported product path.

## Product path (use this)

```bash
# Single product binary (same idea as nautilus_trader's `nautilus` CLI)
cargo build -p catalog-capture-cli
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-btc-book-summary.toml
```

Configuration samples live in `/examples/*.toml` (TOML only — not cargo binaries).

## Why these were demoted

- Nautilus Trader ships **one** product binary (`nautilus`) and gates demos behind
  optional features; libraries stay `rlib`.
- Multiple cargo examples inflate mental load and encourage `cargo build --examples`.
- Capture validation belongs in **unit tests** and **CLI + TOML**, not extra bins.

## Restoring a manual one-off (maintainers only)

If you need to compile one of these files temporarily, copy it into a scratch
crate or run it as a standalone `rustc` experiment — do not re-add `[[example]]`
entries to the product crates without an explicit design review.
