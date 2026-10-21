# Test and calibration tools

These scripts support development and supervised calibration. The product remains
the Rust `catalog-capture-cli`; test strategies and collectors are not adapter APIs.

| Tools | Purpose |
| --- | --- |
| `probe_*.py` | Explicit venue smokes and soaks |
| `python_*_probe.py` | Optional PyO3 readback of an existing catalog |
| `nautilus_import.py`, `catalog_probe_common.py`, `live_smoke_common.py` | Shared import, catalog assertion and capture-probe helpers |
| `test_predict*.py` | Offline SDK, collector, recorded-evidence and safety checks |
| `capture_predict*.py`, `predict_private_http_recorder.py` | Explicit credential-redacted protocol collection |
| `run_predict*_readonly.py` | Explicit read-only venue checks through Rust strategy/LiveNode |
| `run_predict*_live.py` | Separately authorized, supervised funded execution |
| `predict_evidence.py` | Immutable capture manifest verification |

Shared helpers are source modules, not generated artifacts. Catalog helpers read
an existing catalog; live-smoke helpers assemble/run an explicitly requested capture.
They do not expose adapter APIs.

## Offline checks

```bash
cargo test --workspace --lib --bins
uv run --python 3.12 --with predict-sdk==0.0.22 --with websocket-client --with python-dotenv --with requests --with websockets python -m unittest discover -s tests -p 'test_predict*.py'
uv run --python 3.12 python tests/predict_evidence.py
```

Predict Rust test layers, feature combinations, fixture provenance and regression
commands are maintained in the [crate test guide](../crates/nautilus-predict/tests/README.md).

## Network tools

Use the [smoke/soak guide](../docs/how_to/smoke_and_soak.md) for generic probes and
the [Predict private API guide](../docs/how_to/predict-private-api-tests.md) for
explicit read-only checks. Funded tools require an agreed execution scope and
budget; normal offline tests do not invoke their entry points.

Retain raw redacted JSON/JSONL, Native journals and collector logs together.
Do not overwrite a real capture with synthetic output. Follow the crate guide's
manifest and evidence rules before committing new recordings.
