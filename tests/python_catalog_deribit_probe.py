"""Probe a Deribit Step 3 catalog for perp + option greeks families."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path("/Users/yfclark/nautilus_trader")
sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Deribit perp and option greeks written by nautilus_catalog_capture.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("perp_id", type=str, default="BTC-PERPETUAL.DERIBIT", nargs="?")
    parser.add_argument("call_id", type=str, default="BTC-19JUN26-64500-C.DERIBIT", nargs="?")
    parser.add_argument("put_id", type=str, default="BTC-19JUN26-64500-P.DERIBIT", nargs="?")
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    return parser.parse_args()


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def assert_instrument(catalog: ParquetDataCatalog, instrument_id: str) -> None:
    instruments = catalog.instruments(instrument_ids=[instrument_id])
    assert instruments, f"expected instrument metadata for {instrument_id}"
    assert str(instruments[0].id) == instrument_id


def assert_quotes(catalog: ParquetDataCatalog, instrument_id: str, min_rows: int) -> int:
    quotes = catalog.query_quote_ticks([instrument_id])
    assert len(quotes) >= min_rows, (
        f"expected at least {min_rows} quotes for {instrument_id}, got {len(quotes)}"
    )
    assert all(str(q.instrument_id) == instrument_id for q in quotes)
    assert_monotonic_ts_init(quotes, f"quotes[{instrument_id}]")
    return len(quotes)


def assert_option_greeks(catalog: ParquetDataCatalog, instrument_id: str, min_rows: int) -> int:
    greeks = catalog.query_option_greeks([instrument_id])
    assert len(greeks) >= min_rows, (
        f"expected at least {min_rows} option greeks for {instrument_id}, got {len(greeks)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in greeks)
    assert_monotonic_ts_init(greeks, f"option_greeks[{instrument_id}]")

    sample = greeks[-1]
    assert sample.delta is not None
    assert sample.gamma is not None
    assert sample.vega is not None
    assert sample.theta is not None
    assert sample.mark_iv is not None
    return len(greeks)


def main() -> int:
    args = parse_args()
    perp_id = args.perp_id
    call_id = args.call_id
    put_id = args.put_id
    min_rows = args.min_rows

    catalog = ParquetDataCatalog(str(args.catalog_dir))

    for instrument_id in (perp_id, call_id, put_id):
        assert_instrument(catalog, instrument_id)

    perp_quotes = assert_quotes(catalog, perp_id, min_rows)
    call_quotes = assert_quotes(catalog, call_id, min_rows)
    put_quotes = assert_quotes(catalog, put_id, min_rows)

    mark_prices = catalog.query_mark_price_updates([perp_id])
    index_prices = catalog.query_index_price_updates([perp_id])
    assert len(mark_prices) >= min_rows, f"expected mark prices for {perp_id}"
    assert len(index_prices) >= min_rows, f"expected index prices for {perp_id}"

    call_greeks = assert_option_greeks(catalog, call_id, min_rows)
    put_greeks = assert_option_greeks(catalog, put_id, min_rows)

    print("Python Deribit catalog probe succeeded (Step 3)")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Perp quotes: {perp_quotes}")
    print(f"Call quotes: {call_quotes}, greeks: {call_greeks}")
    print(f"Put quotes: {put_quotes}, greeks: {put_greeks}")
    print(f"Perp mark prices: {len(mark_prices)}, index prices: {len(index_prices)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())