"""Probe a live or fixture Hyperliquid catalog for perp quotes + open interest."""

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

from nautilus_trader.core.nautilus_pyo3 import HyperliquidOpenInterest  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402
from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Hyperliquid perp quote + open-interest capture.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("instrument_id", type=str, nargs="?", default="ETH-USD-PERP.HYPERLIQUID")
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
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
    register_custom_data_class(HyperliquidOpenInterest)

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

    custom = catalog.query(
        "HyperliquidOpenInterest",
        [args.instrument_id],
        None,
        None,
        None,
        None,
        True,
    )
    assert len(custom) >= args.min_rows, (
        f"expected at least {args.min_rows} HyperliquidOpenInterest rows, got {len(custom)}"
    )

    for item in custom:
        assert item.data_type.type_name == "HyperliquidOpenInterest"
        assert item.data_type.identifier == args.instrument_id
        assert item.data_type.metadata["instrument_id"] == args.instrument_id
        assert isinstance(item.data, HyperliquidOpenInterest)
        assert str(item.data.instrument_id) == args.instrument_id

    assert_monotonic_ts_init(custom, f"HyperliquidOpenInterest[{args.instrument_id}]")

    print("Python Hyperliquid open-interest catalog probe succeeded (Step 5)")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Instrument id: {args.instrument_id}")
    print(f"Data types: {data_types}")
    print(f"Instruments loaded: {len(instruments)}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"Open-interest rows loaded: {len(custom)}")
    if quotes:
        print(f"Quote ts_init range: {quotes[0].ts_init} .. {quotes[-1].ts_init}")
    if custom:
        print(f"OI ts_init range: {custom[0].ts_init} .. {custom[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
