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
    parser = argparse.ArgumentParser(
        description=(
            "Validate segment-sealed catalog output: no active .part files and "
            "PyO3 ParquetDataCatalog readback succeeds."
        ),
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("instrument_id", type=str)
    parser.add_argument(
        "--min-quotes",
        type=int,
        default=1,
        help="Minimum quote ticks expected from readback.",
    )
    return parser.parse_args()


def find_part_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.parquet")
        if ".part." in path.name or path.name.endswith(".part.parquet")
    )


def count_sealed_parquet(root: Path) -> int:
    return sum(
        1
        for path in root.rglob("*.parquet")
        if ".part." not in path.name
    )


def main() -> int:
    args = parse_args()
    catalog_dir = args.catalog_dir

    part_files = find_part_files(catalog_dir)
    assert not part_files, (
        "active .part files block catalog readback; seal or recover before probing: "
        f"{part_files}"
    )

    sealed_count = count_sealed_parquet(catalog_dir)
    assert sealed_count > 0, f"expected sealed parquet under {catalog_dir}"

    catalog = ParquetDataCatalog(str(catalog_dir))
    quotes = catalog.query_quote_ticks([args.instrument_id])
    assert len(quotes) >= args.min_quotes, (
        f"expected at least {args.min_quotes} quotes for {args.instrument_id}, "
        f"got {len(quotes)}"
    )
    assert all(str(q.instrument_id) == args.instrument_id for q in quotes)

    print("Segment seal readback probe succeeded")
    print(f"Catalog dir: {catalog_dir}")
    print(f"Instrument id: {args.instrument_id}")
    print(f"Sealed parquet files: {sealed_count}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"First ts_init: {quotes[0].ts_init}")
    print(f"Last ts_init: {quotes[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())