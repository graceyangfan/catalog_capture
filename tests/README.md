# Optional live / readback probes

These Python scripts are **operator and maintainer tools**, not the product surface.

| Kind | Role |
|------|------|
| `probe_*.py` | Short live venue smokes / soaks |
| `python_*_smoke.py` / `*_probe.py` | Optional PyO3 catalog readback |
| `nautilus_import.py`, `*_common.py` | Shared helpers |

Product path is Rust:

```bash
cargo test --workspace --lib --bins
cargo test -p catalog-capture-core --lib catalog_layout
```

Live smokes need network and a matching Nautilus Python environment — see
[docs/how_to/smoke_and_soak.md](../docs/how_to/smoke_and_soak.md).

Do not add new product binaries here; extend `catalog-capture-cli` or unit tests instead.
