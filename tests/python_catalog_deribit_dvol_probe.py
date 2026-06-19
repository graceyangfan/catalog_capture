"""Probe a live or fixture Deribit catalog for DVOL custom data."""

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

from nautilus_trader.core.nautilus_pyo3 import DeribitVolatilityIndex  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402
from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Deribit DVOL capture written by nautilus_catalog_capture.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("min_rows", type=int, nargs="?", default=1)
    parser.add_argument("--index-name", type=str, default="btc_usd")
    return parser.parse_args()


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def main() -> int:
    args = parse_args()
    assert args.catalog_dir.exists(), f"catalog_dir does not exist: {args.catalog_dir}"
    register_custom_data_class(DeribitVolatilityIndex)

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    data_types = catalog.list_data_types()
    custom = catalog.query("DeribitVolatilityIndex", None, None, None, None, None, True)

    assert len(custom) >= args.min_rows, (
        f"expected at least {args.min_rows} DeribitVolatilityIndex rows, got {len(custom)}"
    )

    for item in custom:
        assert item.data_type.type_name == "DeribitVolatilityIndex"
        assert item.data_type.metadata["index_name"] == args.index_name
        assert isinstance(item.data, DeribitVolatilityIndex)
        assert item.data.index_name == args.index_name

    assert_monotonic_ts_init(custom, f"DeribitVolatilityIndex[{args.index_name}]")

    print("Python Deribit DVOL catalog probe succeeded (Step 5)")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Data types: {data_types}")
    print(f"Index name: {args.index_name}")
    print(f"DVOL rows loaded: {len(custom)}")
    if custom:
        print(f"DVOL ts_init range: {custom[0].ts_init} .. {custom[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
