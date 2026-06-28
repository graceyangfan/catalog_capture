"""Probe a live or fixture Binance Futures catalog for native custom data."""

from __future__ import annotations

import argparse
from pathlib import Path

from catalog_probe_common import assert_monotonic_ts_init  # noqa: E402
from catalog_probe_common import load_catalog  # noqa: E402
from catalog_probe_common import print_probe_header  # noqa: E402
from catalog_probe_common import require_instrument  # noqa: E402
from nautilus_trader.adapters.binance import BinanceFuturesLiquidation  # noqa: E402
from nautilus_trader.adapters.binance import BinanceFuturesTicker  # noqa: E402
from nautilus_trader.model.data import QuoteTick  # noqa: E402


SUPPORTED_TYPES = {
    "BinanceFuturesTicker": BinanceFuturesTicker,
    "BinanceFuturesLiquidation": BinanceFuturesLiquidation,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Binance Futures custom-data capture written by nautilus_catalog_capture.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("type_name", choices=sorted(SUPPORTED_TYPES))
    parser.add_argument("instrument_id", type=str, nargs="?")
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    parser.add_argument(
        "--min-quotes",
        type=int,
        default=0,
        help="Minimum quote rows required for the instrument (default: 0).",
    )
    parser.add_argument(
        "--all-market",
        action="store_true",
        help="Query custom rows without an identifier filter (for all-market liquidation capture).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    assert args.catalog_dir.exists(), f"catalog_dir does not exist: {args.catalog_dir}"

    custom_cls = SUPPORTED_TYPES[args.type_name]
    all_market = args.all_market
    instrument_id = args.instrument_id
    min_rows = args.min_rows
    if all_market and instrument_id and instrument_id.isdigit():
        min_rows = int(instrument_id)
        instrument_id = None
    instrument_id = instrument_id or "ETHUSDT-PERP.BINANCE"
    if args.type_name == "BinanceFuturesTicker" and all_market:
        raise SystemExit("--all-market is not supported for BinanceFuturesTicker")

    catalog = load_catalog(args.catalog_dir)
    data_types = catalog.list_data_types()

    instruments = require_instrument(catalog, instrument_id)

    quotes = catalog.query(QuoteTick, identifiers=[instrument_id])
    assert len(quotes) >= args.min_quotes, (
        f"expected at least {args.min_quotes} quotes for {instrument_id}, got {len(quotes)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in quotes)
    if quotes:
        assert_monotonic_ts_init(quotes, f"quotes[{instrument_id}]")

    identifiers = None if all_market else [instrument_id]
    custom = catalog.query(custom_cls, identifiers=identifiers)
    assert len(custom) >= min_rows, (
        f"expected at least {min_rows} {args.type_name} rows, got {len(custom)}"
    )

    for item in custom:
        assert isinstance(item, custom_cls)
        if not all_market:
            assert str(item.instrument_id) == instrument_id

    if custom:
        label = "all-market" if all_market else instrument_id
        assert_monotonic_ts_init(custom, f"{args.type_name}[{label}]")

    print_probe_header(
        "Python Binance custom-data catalog probe succeeded (Step 5)",
        args.catalog_dir,
        [
            ("Type name", args.type_name),
            ("Instrument id", "all-market" if all_market else instrument_id),
        ],
    )
    print(f"Data types: {data_types}")
    print(f"Instruments loaded: {len(instruments)}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"Custom rows loaded: {len(custom)}")
    if quotes:
        print(f"Quote ts_init range: {quotes[0].ts_init} .. {quotes[-1].ts_init}")
    if custom:
        print(f"Custom ts_init range: {custom[0].ts_init} .. {custom[-1].ts_init}")
        print(f"Custom instruments seen: {len({str(item.instrument_id) for item in custom})}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
