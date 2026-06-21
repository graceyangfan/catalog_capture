#!/usr/bin/env python3
"""Fixture-based smoke for selective option book_deltas readback (Step 6d)."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402

OPTION_ID = "BTC-13JAN23-16000-P.DERIBIT"


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-book-deltas-readback-"))
    try:
        cmd = [
            "cargo",
            "run",
            "-p",
            "catalog-capture-runtime-adapter",
            "--example",
            "write_book_deltas_readback_fixture",
            "--",
            str(catalog_dir),
        ]
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

        catalog = ParquetDataCatalog(str(catalog_dir))
        deltas = catalog.query_order_book_deltas([OPTION_ID])
        assert len(deltas) >= 2, (
            f"expected at least 2 order book deltas for {OPTION_ID}, got {len(deltas)}"
        )
        assert all(str(item.instrument_id) == OPTION_ID for item in deltas)
        assert_monotonic_ts_init(deltas, f"order_book_deltas[{OPTION_ID}]")

        delta_dir = catalog_dir / "data" / "order_book_deltas" / OPTION_ID
        assert delta_dir.exists(), (
            f"expected order_book_deltas parquet under data/order_book_deltas/{OPTION_ID}"
        )

        print("Book deltas readback fixture smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Option id: {OPTION_ID}")
        print(f"Order book deltas loaded: {len(deltas)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
