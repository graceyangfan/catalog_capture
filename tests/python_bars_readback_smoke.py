#!/usr/bin/env python3
"""Fixture-based smoke for perp 1m bar capture readback (Step 6c)."""

from __future__ import annotations

import sys

from catalog_probe_common import assert_monotonic_ts_init  # noqa: E402
from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402

BAR_TYPE = "ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-bars-readback-")
    try:
        run_fixture_example("write_python_readback_fixture", catalog_dir)

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
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
