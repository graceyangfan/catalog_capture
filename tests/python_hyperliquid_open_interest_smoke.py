from __future__ import annotations

import importlib.util
import shutil
import subprocess
import tempfile
from pathlib import Path

PROJECT_ROOT = Path("/Users/yfclark/nautilus_catalog_capture")
_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog as PyO3ParquetDataCatalog  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-hyperliquid-open-interest-"))
    try:
        cmd = [
            "cargo",
            "+1.96.0",
            "run",
            "-p",
            "catalog-capture-runtime-adapter",
            "--example",
            "write_hyperliquid_open_interest_fixture",
            "--",
            str(catalog_dir),
        ]
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

        catalog = ParquetDataCatalog(str(catalog_dir))
        pyo3_catalog = PyO3ParquetDataCatalog(str(catalog_dir))

        instrument_id = "ETH-USD-PERP.HYPERLIQUID"
        instruments = catalog.instruments(instrument_ids=[instrument_id])
        custom = pyo3_catalog.query(
            "HyperliquidOpenInterest",
            [instrument_id],
            None,
            None,
            None,
            None,
            True,
        )

        assert len(instruments) == 1, f"expected 1 instrument, got {len(instruments)}"
        assert str(instruments[0].id) == instrument_id

        assert len(custom) == 2, f"expected 2 custom items, got {len(custom)}"

        first = custom[0]
        second = custom[1]
        assert first.data_type.type_name == "HyperliquidOpenInterest"
        assert first.data_type.identifier == instrument_id
        assert str(first.data.instrument_id) == instrument_id
        assert str(first.data.open_interest) == "12345.6789"
        assert first.data.ts_init == 9_000_000
        assert str(second.data.open_interest) == "12388.5000"
        assert second.data.ts_init == 9_001_000

        print("Hyperliquid open-interest smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Instruments loaded: {len(instruments)}")
        print(f"Custom items loaded: {len(custom)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
