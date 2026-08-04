"""Probe a live or fixture catalog for Binance perp WS families (Step 1–2, Step 6a trades)."""

from __future__ import annotations

import argparse
from pathlib import Path

from catalog_probe_common import assert_monotonic_ts_init  # noqa: E402
from catalog_probe_common import print_probe_header  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe quotes, mark/index/funding, instruments, and optional contract-state "
            "families written by Catalog Capture (Binance perp WS profile)."
        ),
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("instrument_id", type=str)
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    parser.add_argument(
        "--require-contract-state",
        action="store_true",
        help="Require instrument_status and instrument_closes rows (fixture/smoke mode).",
    )
    parser.add_argument(
        "--min-trade-rows",
        type=int,
        default=0,
        help="Minimum trade ticks required (0 skips trade readback).",
    )
    parser.add_argument(
        "--bar-type",
        action="append",
        default=[],
        help="Bar type to validate through ParquetDataCatalog.query_bars(). Repeat for multiple bars.",
    )
    parser.add_argument(
        "--min-bar-rows",
        type=int,
        default=1,
        help="Minimum bar rows required per --bar-type (ignored when no bar types set).",
    )
    return parser.parse_args()


def probe_contract_state(
    catalog: ParquetDataCatalog,
    catalog_dir: Path,
    instrument_id: str,
    *,
    require: bool,
) -> tuple[int, int]:
    status_dir = catalog_dir / "data" / "instrument_status" / instrument_id
    close_dir = catalog_dir / "data" / "instrument_closes" / instrument_id

    statuses = []
    closes = []
    if status_dir.exists() and any(status_dir.glob("*.parquet")):
        statuses = catalog.query(
            "instrument_status",
            [instrument_id],
            None,
            None,
            None,
            None,
            True,
        )
    if close_dir.exists() and any(close_dir.glob("*.parquet")):
        closes = catalog.query(
            "instrument_closes",
            [instrument_id],
            None,
            None,
            None,
            None,
            True,
        )

    if require:
        assert statuses, f"expected instrument_status rows for {instrument_id}"
        assert closes, f"expected instrument_closes rows for {instrument_id}"
    elif not statuses and not closes:
        print(
            "NOTE: no instrument_status/instrument_closes rows yet "
            "(Binance status poll ~3600s; closes are rare on short live runs)"
        )

    if statuses:
        assert all(str(item.instrument_id) == instrument_id for item in statuses)
        assert_monotonic_ts_init(statuses, "instrument_status")
    if closes:
        assert all(str(item.instrument_id) == instrument_id for item in closes)
        assert_monotonic_ts_init(closes, "instrument_closes")

    return len(statuses), len(closes)


def main() -> int:
    args = parse_args()
    instrument_id = args.instrument_id
    min_rows = args.min_rows

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    data_types = catalog.list_data_types()

    quotes = catalog.query_quote_ticks([instrument_id])
    mark_prices = catalog.query_mark_price_updates([instrument_id])
    index_prices = catalog.query_index_price_updates([instrument_id])

    instruments = catalog.instruments(instrument_ids=[instrument_id])
    assert instruments, f"expected instrument metadata in catalog for {instrument_id}"
    assert str(instruments[0].id) == instrument_id

    assert len(quotes) >= min_rows, (
        f"expected at least {min_rows} quotes for {instrument_id}, got {len(quotes)}"
    )
    assert all(str(q.instrument_id) == instrument_id for q in quotes)
    assert_monotonic_ts_init(quotes, "quotes")

    assert len(mark_prices) >= min_rows, (
        f"expected at least {min_rows} mark prices for {instrument_id}, got {len(mark_prices)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in mark_prices)
    assert_monotonic_ts_init(mark_prices, "mark_prices")

    assert len(index_prices) >= min_rows, (
        f"expected at least {min_rows} index prices for {instrument_id}, got {len(index_prices)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in index_prices)
    assert_monotonic_ts_init(index_prices, "index_prices")

    assert "funding_rate_update" in data_types, (
        "expected funding_rate_update in list_data_types; "
        f"got {data_types}"
    )
    funding_dir = args.catalog_dir / "data" / "funding_rate_update" / instrument_id
    assert funding_dir.exists(), f"expected funding parquet directory at {funding_dir}"
    funding_files = list(funding_dir.glob("*.parquet"))
    assert funding_files, f"expected funding parquet files under {funding_dir}"

    status_count, close_count = probe_contract_state(
        catalog,
        args.catalog_dir,
        instrument_id,
        require=args.require_contract_state,
    )

    trade_count = 0
    if args.min_trade_rows > 0:
        trades = catalog.query_trade_ticks([instrument_id])
        assert len(trades) >= args.min_trade_rows, (
            f"expected at least {args.min_trade_rows} trade ticks for {instrument_id}, "
            f"got {len(trades)}"
        )
        assert all(str(item.instrument_id) == instrument_id for item in trades)
        assert_monotonic_ts_init(trades, "trade_ticks")
        trade_count = len(trades)

    bar_counts: list[tuple[str, int]] = []
    if args.bar_type:
        for bar_type in args.bar_type:
            bars = catalog.query_bars([bar_type])
            assert len(bars) >= args.min_bar_rows, (
                f"expected at least {args.min_bar_rows} bars for {bar_type}, got {len(bars)}"
            )
            assert_monotonic_ts_init(bars, f"bars[{bar_type}]")
            bar_counts.append((bar_type, len(bars)))

    step_label = "Step 1–2"
    if args.min_trade_rows > 0:
        step_label = "Step 1–2 + Step 6a trades"
    if args.bar_type:
        step_label = f"{step_label} + Step 6c bars"

    print_probe_header(
        f"Python derivatives catalog probe succeeded ({step_label})",
        args.catalog_dir,
        [("Instrument id", instrument_id)],
    )
    print(f"Data types: {data_types}")
    print(f"Instruments loaded: {len(instruments)}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"Mark prices loaded: {len(mark_prices)}")
    print(f"Index prices loaded: {len(index_prices)}")
    print(f"Funding parquet files: {len(funding_files)}")
    print(f"Instrument statuses loaded: {status_count}")
    print(f"Instrument closes loaded: {close_count}")
    if args.min_trade_rows > 0:
        print(f"Trade ticks loaded: {trade_count}")
    for bar_type, count in bar_counts:
        print(f"Bars loaded ({bar_type}): {count}")
    if quotes:
        print(f"Quote ts_init range: {quotes[0].ts_init} .. {quotes[-1].ts_init}")
    if mark_prices:
        print(f"Mark ts_init range: {mark_prices[0].ts_init} .. {mark_prices[-1].ts_init}")
    if index_prices:
        print(f"Index ts_init range: {index_prices[0].ts_init} .. {index_prices[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
