#!/usr/bin/env python3
"""Fixture-based smoke for Binance perp trade tick capture (Step 6a)."""

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

DERIVATIVES_PROBE = PROJECT_ROOT / "tests" / "python_catalog_derivatives_probe.py"


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-binance-trades-fixture-"))
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

        probe_cmd = [
            sys.executable,
            str(DERIVATIVES_PROBE),
            str(catalog_dir),
            "ETHUSDT-PERP.BINANCE",
            "1",
            "--min-trade-rows",
            "3",
            "--require-contract-state",
        ]
        subprocess.run(probe_cmd, cwd=PROJECT_ROOT, check=True)

        print("Binance perp trades fixture smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
