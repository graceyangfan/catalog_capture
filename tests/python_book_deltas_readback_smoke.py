#!/usr/bin/env python3
"""Fixture-based smoke for selective option book_deltas readback (Step 6d)."""

from __future__ import annotations

import sys

from catalog_probe_common import assert_monotonic_ts_init  # noqa: E402
from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402

OPTION_ID = "BTC-13JAN23-16000-P.DERIBIT"


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-book-deltas-readback-")
    try:
        run_fixture_example("write_book_deltas_readback_fixture", catalog_dir)

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
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
