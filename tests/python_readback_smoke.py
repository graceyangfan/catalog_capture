from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path("/Users/yfclark/nautilus_trader")
PROJECT_ROOT = Path("/Users/yfclark/nautilus_catalog_capture")

sys.path.insert(0, str(REPO_ROOT))

from nautilus_trader.model.data import QuoteTick  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-python-readback-"))
    try:
        cmd = [
            "cargo",
            "+1.96.0",
            "run",
            "-p",
            "catalog-capture-runtime-adapter",
            "--example",
            "write_python_readback_fixture",
            "--",
            str(catalog_dir),
        ]
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

        catalog = ParquetDataCatalog(str(catalog_dir))

        instruments = catalog.instruments(instrument_ids=["ETHUSDT-PERP.BINANCE"])
        quotes = catalog.query(QuoteTick, identifiers=["ETHUSDT-PERP.BINANCE"])

        assert len(instruments) == 1, f"expected 1 instrument, got {len(instruments)}"
        assert str(instruments[0].id) == "ETHUSDT-PERP.BINANCE"

        assert len(quotes) == 5, f"expected 5 quotes, got {len(quotes)}"
        assert all(str(q.instrument_id) == "ETHUSDT-PERP.BINANCE" for q in quotes)
        assert quotes[0].ts_init == 1_000_000
        assert quotes[-1].ts_init == 1_004_000

        print("Python readback smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Instruments loaded: {len(instruments)}")
        print(f"Quotes loaded: {len(quotes)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
