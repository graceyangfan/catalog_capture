"""Probe an option-universe catalog for per-option and hedge-perp data quality."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
from typing import Sequence

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional row-count validation.
    pq = None

_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Probe option-universe parquet readback through Nautilus ParquetDataCatalog."
        ),
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("--perp-id", required=True)
    parser.add_argument(
        "--option-id",
        action="append",
        default=[],
        help="Option instrument id to validate. Repeat for multiple options.",
    )
    parser.add_argument("--min-rows", type=int, default=1)
    parser.add_argument(
        "--min-trade-rows",
        type=int,
        default=1,
        help="Minimum perp trade ticks required (0 skips perp trade readback).",
    )
    parser.add_argument(
        "--min-option-trade-rows",
        type=int,
        default=0,
        help="Minimum trade ticks per option (0 skips option trade readback).",
    )
    parser.add_argument(
        "--require-contract-state",
        action="store_true",
        help="Require instrument_status and instrument_closes rows for the hedge perp and options.",
    )
    parser.add_argument(
        "--bar-type",
        action="append",
        default=[],
        help="Bar type to validate through ParquetDataCatalog.query_bars(). Repeat for multiple bars.",
    )
    parser.add_argument(
        "--bars-only",
        action="store_true",
        help="Validate hedge perp + bars only; skip sampled option quote/greek readback.",
    )
    parser.add_argument(
        "--book-deltas-only",
        action="store_true",
        help="Validate sampled option order_book_deltas only; skip quote/greek readback.",
    )
    parser.add_argument(
        "--min-option-book-delta-rows",
        type=int,
        default=1,
        help="Minimum order book deltas per option when --book-deltas-only is set.",
    )
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


def assert_quotes(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    min_rows: int,
) -> int:
    quotes = catalog.query_quote_ticks([instrument_id])
    assert len(quotes) >= min_rows, (
        f"expected at least {min_rows} quotes for {instrument_id}, got {len(quotes)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in quotes)
    assert_monotonic_ts_init(quotes, f"quotes[{instrument_id}]")
    return len(quotes)


def assert_mark_prices(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    min_rows: int,
) -> int:
    mark_prices = catalog.query_mark_price_updates([instrument_id])
    assert len(mark_prices) >= min_rows, (
        f"expected at least {min_rows} mark prices for {instrument_id}, "
        f"got {len(mark_prices)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in mark_prices)
    assert_monotonic_ts_init(mark_prices, f"mark_prices[{instrument_id}]")
    return len(mark_prices)


def assert_index_prices(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    min_rows: int,
) -> int:
    index_prices = catalog.query_index_price_updates([instrument_id])
    assert len(index_prices) >= min_rows, (
        f"expected at least {min_rows} index prices for {instrument_id}, "
        f"got {len(index_prices)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in index_prices)
    assert_monotonic_ts_init(index_prices, f"index_prices[{instrument_id}]")
    return len(index_prices)


def assert_trade_ticks(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    min_rows: int,
) -> int:
    trades = catalog.query_trade_ticks([instrument_id])
    assert len(trades) >= min_rows, (
        f"expected at least {min_rows} trade ticks for {instrument_id}, "
        f"got {len(trades)}"
    )
    assert all(str(item.instrument_id) == instrument_id for item in trades)
    assert_monotonic_ts_init(trades, f"trade_ticks[{instrument_id}]")
    return len(trades)


def assert_option_greeks(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    min_rows: int,
) -> int:
    greeks = catalog.query_option_greeks([instrument_id])
    assert len(greeks) >= min_rows, (
        f"expected at least {min_rows} option greeks for {instrument_id}, "
        f"got {len(greeks)}"
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


def assert_funding_files(catalog_dir: Path, instrument_id: str) -> tuple[int, int | None]:
    funding_dir = catalog_dir / "data" / "funding_rate_update" / instrument_id
    assert funding_dir.exists(), f"expected funding parquet directory at {funding_dir}"
    files = sorted(funding_dir.glob("*.parquet"))
    assert files, f"expected funding parquet files under {funding_dir}"

    if pq is None:
        return len(files), None
    rows = sum(pq.ParquetFile(path).metadata.num_rows for path in files)
    assert rows > 0, f"expected funding parquet rows under {funding_dir}"
    return len(files), rows


def assert_bars(
    catalog: ParquetDataCatalog,
    bar_types: Sequence[str],
    min_rows: int,
) -> list[tuple[str, int]]:
    counts: list[tuple[str, int]] = []
    for bar_type in bar_types:
        bars = catalog.query_bars([bar_type])
        assert len(bars) >= min_rows, (
            f"expected at least {min_rows} bars for {bar_type}, got {len(bars)}"
        )
        assert_monotonic_ts_init(bars, f"bars[{bar_type}]")
        counts.append((bar_type, len(bars)))
    return counts


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
            f"NOTE: no instrument_status/instrument_closes rows yet for {instrument_id}",
        )

    if statuses:
        assert all(str(item.instrument_id) == instrument_id for item in statuses)
        assert_monotonic_ts_init(statuses, f"instrument_status[{instrument_id}]")
    if closes:
        assert all(str(item.instrument_id) == instrument_id for item in closes)
        assert_monotonic_ts_init(closes, f"instrument_closes[{instrument_id}]")

    return len(statuses), len(closes)


def main() -> int:
    args = parse_args()
    if args.min_rows <= 0:
        raise ValueError("--min-rows must be positive")
    if not args.bars_only and not args.book_deltas_only and not args.option_id:
        raise ValueError(
            "at least one --option-id is required unless --bars-only or "
            "--book-deltas-only is set"
        )
    if args.bars_only and not args.bar_type:
        raise ValueError("--bars-only requires at least one --bar-type")
    if args.book_deltas_only and not args.option_id:
        raise ValueError("--book-deltas-only requires at least one --option-id")

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    assert_instrument(catalog, args.perp_id)

    if args.book_deltas_only:
        options_with_book_deltas = 0
        option_counts = []
        for option_id in args.option_id:
            assert_instrument(catalog, option_id)
            deltas = catalog.query_order_book_deltas([option_id])
            if len(deltas) >= args.min_option_book_delta_rows:
                assert all(str(item.instrument_id) == option_id for item in deltas)
                assert_monotonic_ts_init(deltas, f"order_book_deltas[{option_id}]")
                options_with_book_deltas += 1
            option_counts.append((option_id, len(deltas)))
        if options_with_book_deltas == 0:
            raise AssertionError(
                f"expected at least one option with >= {args.min_option_book_delta_rows} "
                f"order book deltas; validated {len(args.option_id)} options"
            )
        print("Python option-universe catalog probe succeeded (book-deltas-only)")
        print(f"Catalog dir: {args.catalog_dir}")
        print(f"Perp: {args.perp_id}")
        for option_id, count in option_counts:
            print(f"Option: {option_id} order_book_deltas={count}")
        return 0

    if not args.bars_only:
        for instrument_id in args.option_id:
            assert_instrument(catalog, instrument_id)

    perp_quotes = assert_quotes(catalog, args.perp_id, args.min_rows)
    perp_trades = (
        assert_trade_ticks(catalog, args.perp_id, args.min_trade_rows)
        if args.min_trade_rows > 0
        else 0
    )
    perp_marks = assert_mark_prices(catalog, args.perp_id, args.min_rows)
    perp_index = assert_index_prices(catalog, args.perp_id, args.min_rows)
    funding_files, funding_rows = assert_funding_files(args.catalog_dir, args.perp_id)
    bar_counts = assert_bars(catalog, args.bar_type, args.min_rows) if args.bar_type else []
    perp_statuses, perp_closes = probe_contract_state(
        catalog,
        args.catalog_dir,
        args.perp_id,
        require=args.require_contract_state,
    )

    option_counts = []
    options_with_trades = 0
    if args.bars_only:
        print("Python option-universe catalog probe succeeded (bars-only)")
        print(f"Catalog dir: {args.catalog_dir}")
        print(f"Perp: {args.perp_id}")
        print(
            f"Perp quotes={perp_quotes} trade_ticks={perp_trades} "
            f"mark_prices={perp_marks} index_prices={perp_index} "
            f"instrument_statuses={perp_statuses} instrument_closes={perp_closes}"
        )
        funding_row_text = "unavailable" if funding_rows is None else str(funding_rows)
        print(f"Perp funding_files={funding_files} funding_rows={funding_row_text}")
        for bar_type, count in bar_counts:
            print(f"Bars: {bar_type} rows={count}")
        return 0

    for option_id in args.option_id:
        status_count, close_count = probe_contract_state(
            catalog,
            args.catalog_dir,
            option_id,
            require=args.require_contract_state,
        )
        option_trades = 0
        if args.min_option_trade_rows > 0:
            trades = catalog.query_trade_ticks([option_id])
            if len(trades) >= args.min_option_trade_rows:
                assert all(str(item.instrument_id) == option_id for item in trades)
                assert_monotonic_ts_init(trades, f"trade_ticks[{option_id}]")
                option_trades = len(trades)
                options_with_trades += 1
        option_counts.append(
            (
                option_id,
                assert_quotes(catalog, option_id, args.min_rows),
                option_trades,
                assert_mark_prices(catalog, option_id, args.min_rows),
                assert_option_greeks(catalog, option_id, args.min_rows),
                status_count,
                close_count,
            )
        )
    if args.min_option_trade_rows > 0 and options_with_trades == 0:
        raise AssertionError(
            f"expected at least one option with >= {args.min_option_trade_rows} "
            f"trade ticks; validated {len(args.option_id)} options"
        )

    print("Python option-universe catalog probe succeeded")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Perp: {args.perp_id}")
    print(
        f"Perp quotes={perp_quotes} trade_ticks={perp_trades} "
        f"mark_prices={perp_marks} index_prices={perp_index} "
        f"instrument_statuses={perp_statuses} instrument_closes={perp_closes}"
    )
    funding_row_text = "unavailable" if funding_rows is None else str(funding_rows)
    print(f"Perp funding_files={funding_files} funding_rows={funding_row_text}")
    for bar_type, count in bar_counts:
        print(f"Bars: {bar_type} rows={count}")
    for option_id, quotes, trades, marks, greeks, statuses, closes in option_counts:
        print(
            f"Option: {option_id} quotes={quotes} trade_ticks={trades} "
            f"mark_prices={marks} option_greeks={greeks} "
            f"instrument_statuses={statuses} instrument_closes={closes}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
