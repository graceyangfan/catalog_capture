#!/usr/bin/env python3
"""Fixture-based smoke for Binance perp trade tick capture (Step 6a)."""

from __future__ import annotations

import subprocess
import sys

from python_smoke_common import PROJECT_ROOT  # noqa: E402
from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402

DERIVATIVES_PROBE = PROJECT_ROOT / "tests" / "python_catalog_derivatives_probe.py"


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-binance-trades-fixture-")
    try:
        run_fixture_example("write_python_readback_fixture", catalog_dir)

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
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
