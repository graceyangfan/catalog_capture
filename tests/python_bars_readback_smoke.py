#!/usr/bin/env python3
"""Fixture-based smoke for perp 1m bar capture readback (Step 6c)."""

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

BAR_TYPE = "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-bars-readback-"))
    try:
        cmd = [
            "cargo",
            "run",
            "-p",
            "catalog-capture-runtime-adapter",
            "--example",
            "write_python_readback_fixture",
            "--",
            str(catalog_dir),
        ]
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

        catalog = ParquetDataCatalog(str(catalog_dir))
        bars = catalog.query_bars([BAR_TYPE])
        assert len(bars) >= 2, f"expected at least 2 bars for {BAR_TYPE}, got {len(bars)}"
        assert_monotonic_ts_init(bars, f"bars[{BAR_TYPE}]")

        bar_dir = catalog_dir / "data" / "bar" / BAR_TYPE
        bars_alias_dir = catalog_dir / "data" / "bars" / BAR_TYPE
        assert bar_dir.exists() or bars_alias_dir.exists(), (
            f"expected bar parquet under data/bar or data/bars for {BAR_TYPE}"
        )

        print("Bars readback fixture smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Bar type: {BAR_TYPE}")
        print(f"Bars loaded: {len(bars)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())