from __future__ import annotations

from python_smoke_common import cleanup_catalog_dir  # noqa: E402
from python_smoke_common import make_temp_catalog_dir  # noqa: E402
from python_smoke_common import run_fixture_example  # noqa: E402
from nautilus_trader.model.data import QuoteTick  # noqa: E402
from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def main() -> int:
    catalog_dir = make_temp_catalog_dir("nautilus-python-readback-")
    try:
        run_fixture_example("write_python_readback_fixture", catalog_dir)

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
        cleanup_catalog_dir(catalog_dir)


if __name__ == "__main__":
    raise SystemExit(main())
