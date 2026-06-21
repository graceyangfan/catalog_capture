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

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = Path(tempfile.mkdtemp(prefix="nautilus-pyo3-market-readback-"))
    try:
        cmd = [
            "cargo",
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
        data_types = catalog.list_data_types()
        instruments = catalog.instruments(instrument_ids=["ETHUSDT-PERP.BINANCE"])
        quotes = catalog.query_quote_ticks(["ETHUSDT-PERP.BINANCE"])
        mark_prices = catalog.query_mark_price_updates(["ETHUSDT-PERP.BINANCE"])
        index_prices = catalog.query_index_price_updates(["ETHUSDT-PERP.BINANCE"])
        statuses = catalog.query("instrument_status", ["ETHUSDT-PERP.BINANCE"], None, None, None, None, True)
        closes = catalog.query("instrument_closes", ["ETHUSDT-PERP.BINANCE"], None, None, None, None, True)
        greeks = catalog.query_option_greeks(["ETHUSDT-PERP.BINANCE"])

        assert len(instruments) == 1, f"expected 1 instrument, got {len(instruments)}"
        assert str(instruments[0].id) == "ETHUSDT-PERP.BINANCE"

        assert len(quotes) == 5, f"expected 5 quotes, got {len(quotes)}"
        assert all(str(q.instrument_id) == "ETHUSDT-PERP.BINANCE" for q in quotes)
        assert quotes[0].ts_init == 1_000_000
        assert quotes[-1].ts_init == 1_004_000

        assert len(mark_prices) == 2, f"expected 2 mark prices, got {len(mark_prices)}"
        assert all(str(item.instrument_id) == "ETHUSDT-PERP.BINANCE" for item in mark_prices)
        assert mark_prices[0].ts_init == 2_000_000
        assert mark_prices[-1].ts_init == 2_001_000

        assert len(index_prices) == 2, f"expected 2 index prices, got {len(index_prices)}"
        assert all(str(item.instrument_id) == "ETHUSDT-PERP.BINANCE" for item in index_prices)
        assert index_prices[0].ts_init == 3_000_000
        assert index_prices[-1].ts_init == 3_001_000

        assert "funding_rate_update" in data_types, (
            "expected funding_rate_update in list_data_types; "
            f"got {data_types}"
        )
        funding_dir = catalog_dir / "data" / "funding_rate_update" / "ETHUSDT-PERP.BINANCE"
        assert funding_dir.exists(), f"expected funding parquet directory at {funding_dir}"
        assert any(funding_dir.glob("*.parquet")), f"expected funding parquet files under {funding_dir}"

        assert len(statuses) == 2, f"expected 2 instrument statuses, got {len(statuses)}"
        assert all(str(item.instrument_id) == "ETHUSDT-PERP.BINANCE" for item in statuses)
        assert statuses[0].ts_init == 5_000_000
        assert statuses[-1].ts_init == 5_001_000

        assert len(closes) == 2, f"expected 2 instrument closes, got {len(closes)}"
        assert all(str(item.instrument_id) == "ETHUSDT-PERP.BINANCE" for item in closes)
        assert closes[0].ts_init == 6_000_000
        assert closes[-1].ts_init == 6_001_000

        assert len(greeks) == 2, f"expected 2 option greeks, got {len(greeks)}"
        assert all(str(item.instrument_id) == "ETHUSDT-PERP.BINANCE" for item in greeks)
        assert greeks[0].ts_init == 7_000_000
        assert greeks[-1].ts_init == 7_001_000

        print("PyO3 market-data readback smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Instruments loaded: {len(instruments)}")
        print(f"Quotes loaded: {len(quotes)}")
        print(f"Mark prices loaded: {len(mark_prices)}")
        print(f"Index prices loaded: {len(index_prices)}")
        print("Funding rates written and discoverable via list_data_types")
        print(f"Instrument statuses loaded: {len(statuses)}")
        print(f"Instrument closes loaded: {len(closes)}")
        print(f"Option greeks loaded: {len(greeks)}")
        return 0
    finally:
        shutil.rmtree(catalog_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
