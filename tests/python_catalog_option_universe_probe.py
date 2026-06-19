"""Probe an option-universe catalog for per-option and hedge-perp data quality."""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

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


def main() -> int:
    args = parse_args()
    if args.min_rows <= 0:
        raise ValueError("--min-rows must be positive")
    if not args.option_id:
        raise ValueError("at least one --option-id is required")

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    all_instrument_ids = [args.perp_id, *args.option_id]

    for instrument_id in all_instrument_ids:
        assert_instrument(catalog, instrument_id)

    perp_quotes = assert_quotes(catalog, args.perp_id, args.min_rows)
    perp_marks = assert_mark_prices(catalog, args.perp_id, args.min_rows)
    perp_index = assert_index_prices(catalog, args.perp_id, args.min_rows)
    funding_files, funding_rows = assert_funding_files(args.catalog_dir, args.perp_id)

    option_counts = []
    for option_id in args.option_id:
        option_counts.append(
            (
                option_id,
                assert_quotes(catalog, option_id, args.min_rows),
                assert_mark_prices(catalog, option_id, args.min_rows),
                assert_option_greeks(catalog, option_id, args.min_rows),
            )
        )

    print("Python option-universe catalog probe succeeded")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Perp: {args.perp_id}")
    print(f"Perp quotes={perp_quotes} mark_prices={perp_marks} index_prices={perp_index}")
    funding_row_text = "unavailable" if funding_rows is None else str(funding_rows)
    print(f"Perp funding_files={funding_files} funding_rows={funding_row_text}")
    for option_id, quotes, marks, greeks in option_counts:
        print(
            f"Option: {option_id} quotes={quotes} mark_prices={marks} "
            f"option_greeks={greeks}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
