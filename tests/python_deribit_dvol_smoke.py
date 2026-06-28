from __future__ import annotations

from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import DeribitVolatilityIndex  # noqa: E402
from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog as PyO3ParquetDataCatalog  # noqa: E402
from nautilus_trader.core.nautilus_pyo3.model import register_custom_data_class  # noqa: E402


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-deribit-dvol-")
    try:
        run_fixture_example("write_deribit_dvol_fixture", catalog_dir)

        register_custom_data_class(DeribitVolatilityIndex)
        catalog = PyO3ParquetDataCatalog(str(catalog_dir))
        custom = catalog.query("DeribitVolatilityIndex", None, None, None, None, None, True)

        assert len(custom) == 2, f"expected 2 custom items, got {len(custom)}"

        first = custom[0]
        second = custom[1]
        assert first.data_type.type_name == "DeribitVolatilityIndex"
        assert first.data_type.metadata["index_name"] == "btc_usd"
        assert isinstance(first.data, DeribitVolatilityIndex)
        assert first.data.index_name == "btc_usd"
        assert first.data.volatility == 63.25
        assert first.data.ts_init == 10_000_000
        assert second.data.volatility == 64.5
        assert second.data.ts_init == 10_500_000

        print("Deribit DVOL smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Custom items loaded: {len(custom)}")
        return 0
    finally:
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
