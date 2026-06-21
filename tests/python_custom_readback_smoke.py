from __future__ import annotations

import importlib.util
import shutil
import subprocess
import tempfile
from pathlib import Path

_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()
PROJECT_ROOT = _mod.project_root()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog as PyO3ParquetDataCatalog  # noqa: E402
from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class  # noqa: E402
from nautilus_trader.core.nautilus_pyo3.persistence import RustTestCustomData  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-python-custom-readback-"))
    try:
        cmd = [
            "cargo",
            "+1.96.0",
            "run",
            "-p",
            "catalog-capture-runtime-adapter",
            "--example",
            "write_python_custom_readback_fixture",
            "--",
            str(catalog_dir),
        ]
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)

        register_custom_data_class(RustTestCustomData)
        catalog = ParquetDataCatalog(str(catalog_dir))
        pyo3_catalog = PyO3ParquetDataCatalog(str(catalog_dir))

        instruments = catalog.instruments(instrument_ids=["ETHUSDT-PERP.BINANCE"])
        custom = pyo3_catalog.query(
            "RustTestCustomData",
            ["ETHUSDT-PERP.BINANCE"],
            None,
            None,
            None,
            None,
            True,
        )

        assert len(instruments) == 1, f"expected 1 instrument, got {len(instruments)}"
        assert str(instruments[0].id) == "ETHUSDT-PERP.BINANCE"

        assert len(custom) == 2, f"expected 2 custom items, got {len(custom)}"

        first = custom[0]
        second = custom[1]
        assert first.data_type.type_name == "RustTestCustomData"
        assert first.data_type.identifier == "ETHUSDT-PERP.BINANCE"
        assert isinstance(first.data, RustTestCustomData)
        assert isinstance(second.data, RustTestCustomData)
        assert str(first.data.instrument_id) == "ETHUSDT-PERP.BINANCE"
        assert first.data.value == 1.23
        assert first.data.flag is True
        assert first.data.ts_init == 1_000_000
        assert second.data.value == 4.56
        assert second.data.flag is False
        assert second.data.ts_init == 1_001_000

        print("Python custom readback smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Instruments loaded: {len(instruments)}")
        print(f"Custom items loaded: {len(custom)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
