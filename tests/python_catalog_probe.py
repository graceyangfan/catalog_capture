from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path("/Users/yfclark/nautilus_trader")
sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.model.data import QuoteTick  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe a catalog written by nautilus_catalog_capture using Nautilus Python readback.",
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("instrument_id", type=str)
    parser.add_argument("min_quotes", type=int, nargs="?", default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    catalog = ParquetDataCatalog(str(args.catalog_dir))
    instruments = catalog.instruments(instrument_ids=[args.instrument_id])
    quotes = catalog.query(QuoteTick, identifiers=[args.instrument_id])

    assert instruments, f"expected at least one instrument for {args.instrument_id}"
    assert len(quotes) >= args.min_quotes, (
        f"expected at least {args.min_quotes} quotes for {args.instrument_id}, got {len(quotes)}"
    )
    assert all(str(q.instrument_id) == args.instrument_id for q in quotes)

    print("Python catalog probe succeeded")
    print(f"Catalog dir: {args.catalog_dir}")
    print(f"Instrument id: {args.instrument_id}")
    print(f"Instruments loaded: {len(instruments)}")
    print(f"Quotes loaded: {len(quotes)}")
    print(f"First ts_init: {quotes[0].ts_init}")
    print(f"Last ts_init: {quotes[-1].ts_init}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
