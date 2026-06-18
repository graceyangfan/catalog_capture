"""Probe a multi-venue catalog: instruments partitioned by venue suffix."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path("/Users/yfclark/nautilus_trader")
sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe multi-venue capture catalog.")
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("binance_id", type=str, nargs="?", default="ETHUSDT-PERP.BINANCE")
    parser.add_argument("deribit_id", type=str, nargs="?", default="BTC-PERPETUAL.DERIBIT")
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    return parser.parse_args()


def assert_venue_instrument(catalog: ParquetDataCatalog, instrument_id: str, min_rows: int) -> None:
    assert instrument_id.rsplit(".", maxsplit=1)[-1] in {"BINANCE", "DERIBIT", "BYBIT", "OKX"}
    instruments = catalog.instruments(instrument_ids=[instrument_id])
    assert instruments, f"expected instrument metadata for {instrument_id}"
    assert str(instruments[0].id) == instrument_id

    quotes = catalog.query_quote_ticks([instrument_id])
    assert len(quotes) >= min_rows, f"expected quotes for {instrument_id}"
    assert all(str(q.instrument_id) == instrument_id for q in quotes)


def main() -> int:
    args = parse_args()
    catalog = ParquetDataCatalog(str(args.catalog_dir))

    assert_venue_instrument(catalog, args.binance_id, args.min_rows)
    assert_venue_instrument(catalog, args.deribit_id, args.min_rows)

    mark = catalog.query_mark_price_updates([args.deribit_id])
    assert len(mark) >= args.min_rows

    print("Python multi-venue catalog probe succeeded (Step 4)")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Binance instrument: {args.binance_id}")
    print(f"Deribit instrument: {args.deribit_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())