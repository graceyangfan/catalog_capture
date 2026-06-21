"""Probe a Bybit Step 4 catalog for perp + option greeks families."""

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe Bybit perp and option greeks capture.")
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("perp_id", type=str, nargs="?", default="BTCUSDT-LINEAR.BYBIT")
    parser.add_argument("call_id", type=str, nargs="?", default="BTC-25SEP26-64000-C-USDT-OPTION.BYBIT")
    parser.add_argument("put_id", type=str, nargs="?", default="BTC-25SEP26-64000-P-USDT-OPTION.BYBIT")
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    return parser.parse_args()


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def main() -> int:
    args = parse_args()
    catalog = ParquetDataCatalog(str(args.catalog_dir))

    for instrument_id in (args.perp_id, args.call_id, args.put_id):
        instruments = catalog.instruments(instrument_ids=[instrument_id])
        assert instruments, f"expected instrument metadata for {instrument_id}"
        assert str(instruments[0].id) == instrument_id

    for instrument_id in (args.perp_id, args.call_id, args.put_id):
        quotes = catalog.query_quote_ticks([instrument_id])
        assert len(quotes) >= args.min_rows, f"expected quotes for {instrument_id}"
        assert_monotonic_ts_init(quotes, f"quotes[{instrument_id}]")

    mark = catalog.query_mark_price_updates([args.perp_id])
    index = catalog.query_index_price_updates([args.perp_id])
    assert len(mark) >= args.min_rows
    assert len(index) >= args.min_rows

    for option_id in (args.call_id, args.put_id):
        greeks = catalog.query_option_greeks([option_id])
        assert len(greeks) >= args.min_rows, f"expected option greeks for {option_id}"
        sample = greeks[-1]
        assert sample.delta is not None
        assert sample.mark_iv is not None

    print("Python Bybit catalog probe succeeded (Step 4b)")
    print(f"Catalog dir: {args.catalog_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
