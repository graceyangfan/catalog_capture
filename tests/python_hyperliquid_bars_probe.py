"""Probe a live or fixture Hyperliquid catalog for perp quotes + 1m bars."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402

DEFAULT_INSTRUMENT_ID = "ETH-USD-PERP.HYPERLIQUID"
DEFAULT_BAR_TYPE = "ETH-USD-PERP.HYPERLIQUID-1-MINUTE-LAST-EXTERNAL"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Hyperliquid perp quote + bar capture.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument(
        "instrument_id",
        type=str,
        nargs="?",
        default=DEFAULT_INSTRUMENT_ID,
    )
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    parser.add_argument(
        "--bar-type",
        default=DEFAULT_BAR_TYPE,
        help="Bar type to validate through ParquetDataCatalog.query_bars().",
    )
    parser.add_argument(
        "--min-bar-rows",
        type=int,
        default=1,
        help="Minimum bar rows required.",
    )
    parser.add_argument(
        "--min-quotes",
        type=int,
        default=0,
        help="Minimum quote rows required for the instrument (default: 0 for fixture compatibility).",
    )
    return parser.parse_args()


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def main() -> int:
    args = parse_args()
    assert args.catalog_dir.exists(), f"catalog_dir does not exist: {args.catalog_dir}"

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    data_types = catalog.list_data_types()

    instruments = catalog.instruments(instrument_ids=[args.instrument_id])
    assert instruments, f"expected instrument metadata for {args.instrument_id}"
    assert str(instruments[0].id) == args.instrument_id

    quotes = catalog.query_quote_ticks([args.instrument_id])
    assert len(quotes) >= args.min_quotes, (
        f"expected at least {args.min_quotes} quotes for {args.instrument_id}, got {len(quotes)}"
    )
    assert all(str(item.instrument_id) == args.instrument_id for item in quotes)
    if quotes:
        assert_monotonic_ts_init(quotes, f"quotes[{args.instrument_id}]")

    bars = catalog.query_bars([args.bar_type])
    assert len(bars) >= args.min_bar_rows, (
        f"expected at least {args.min_bar_rows} bars for {args.bar_type}, got {len(bars)}"
    )
    assert_monotonic_ts_init(bars, f"bars[{args.bar_type}]")

    bar_dir = args.catalog_dir / "data" / "bar" / args.bar_type
    bars_alias_dir = args.catalog_dir / "data" / "bars" / args.bar_type
    assert bar_dir.exists() or bars_alias_dir.exists(), (
        f"expected bar parquet under data/bar or data/bars for {args.bar_type}"
    )

    print("Python Hyperliquid bars catalog probe succeeded (Step 6c)")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Instrument id: {args.instrument_id}")
    print(f"Bar type: {args.bar_type}")
    print(f"Data types: {data_types}")
    print(f"Instruments loaded: {len(instruments)}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"Bars loaded: {len(bars)}")
    if quotes:
        print(f"Quote ts_init range: {quotes[0].ts_init} .. {quotes[-1].ts_init}")
    if bars:
        print(f"Bar ts_init range: {bars[0].ts_init} .. {bars[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
