from __future__ import annotations

from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402
from nautilus_trader.adapters.binance import BinanceFuturesLiquidation  # noqa: E402
from nautilus_trader.adapters.binance import BinanceFuturesTicker  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-binance-custom-")
    try:
        run_fixture_example("write_binance_custom_fixture", catalog_dir)

        catalog = ParquetDataCatalog(str(catalog_dir))
        instrument_id = "ETHUSDT-PERP.BINANCE"

        instruments = catalog.instruments(instrument_ids=[instrument_id])
        assert len(instruments) == 1, f"expected 1 instrument, got {len(instruments)}"
        assert str(instruments[0].id) == instrument_id

        data_types = catalog.list_data_types()
        assert "custom_binance_futures_ticker" in data_types
        assert "custom_binance_futures_liquidation" in data_types

        ticker = catalog.query(BinanceFuturesTicker, identifiers=[instrument_id])
        liquidation = catalog.query(BinanceFuturesLiquidation, identifiers=[instrument_id])

        assert len(ticker) == 1, f"expected 1 ticker row, got {len(ticker)}"
        assert len(liquidation) == 1, f"expected 1 liquidation row, got {len(liquidation)}"

        ticker_row = ticker[0]
        assert isinstance(ticker_row, BinanceFuturesTicker)
        assert str(ticker_row.instrument_id) == instrument_id
        assert str(ticker_row.last_price) == "2640.000001"
        assert ticker_row.num_trades == 300
        assert ticker_row.ts_init == 13

        liquidation_row = liquidation[0]
        assert isinstance(liquidation_row, BinanceFuturesLiquidation)
        assert str(liquidation_row.instrument_id) == instrument_id
        assert str(liquidation_row.price) == "2641.10"
        assert str(liquidation_row.accumulated_qty) == "1.500"
        assert liquidation_row.ts_init == 21

        print("Binance custom-data smoke test succeeded")
        print(f"Catalog dir: {catalog_dir}")
        print(f"Instruments loaded: {len(instruments)}")
        print(f"Data types: {data_types}")
        print(f"Ticker rows loaded: {len(ticker)}")
        print(f"Liquidation rows loaded: {len(liquidation)}")
        return 0
    finally:
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
