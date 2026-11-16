"""Offline Native catalog/probe integration; run explicitly with the fork's Python."""

from __future__ import annotations

from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


@unittest.skipUnless(
    os.environ.get("NAUTILUS_PYTHON_READBACK_TESTS") == "1",
    "requires an explicitly rebuilt quantity-commission Python extension",
)
class NativeCatalogReadbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from nautilus_import import ensure_nautilus_trader_path

        ensure_nautilus_trader_path()
        import nautilus_trader.model as model
        from nautilus_trader.persistence import ParquetDataCatalog

        cls.model = model
        cls.catalog_class = ParquetDataCatalog

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.catalog = self.catalog_class(str(self.root))
        self.instrument_id = self.write_market(
            "ETHUSDT-PERP.BINANCE", "ETHUSDT", "USDT"
        )

    def write_market(self, instrument_id, raw_symbol, quote_currency):
        m = self.model
        instrument_id = m.InstrumentId.from_str(instrument_id)
        instrument = m.CryptoPerpetual(
            instrument_id,
            m.Symbol(raw_symbol),
            m.Currency.from_str("ETH"),
            m.Currency.from_str(quote_currency),
            m.Currency.from_str(quote_currency),
            False,
            2,
            3,
            m.Price.from_str("0.01"),
            m.Quantity.from_str("0.001"),
            1,
            1,
        )
        self.catalog.write_instruments([instrument])
        self.catalog.write_quote_ticks(
            [
                m.QuoteTick(
                    instrument_id,
                    m.Price.from_str("3000.00"),
                    m.Price.from_str("3000.01"),
                    m.Quantity.from_str("1.000"),
                    m.Quantity.from_str("2.000"),
                    ts,
                    ts,
                )
                for ts in (1000, 2000)
            ]
        )
        return instrument_id

    def run_probe(self, name, *args):
        result = subprocess.run(
            [sys.executable, str(Path(__file__).with_name(name)), *map(str, args)],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def write_custom(self, name, item, metadata, identifier):
        m = self.model
        m.register_custom_data_class(type(item))
        self.catalog.write_custom_data(
            [m.CustomData(m.DataType(name, metadata, identifier), item)]
        )

    def test_all_readback_entrypoints_import_with_current_public_api(self):
        for path in sorted(Path(__file__).parent.glob("python*.py")):
            with self.subTest(probe=path.name):
                self.run_probe(path.name, "--help")
        self.run_probe("probe_segment_seal_readback.py", "--help")

    def test_quote_and_segment_probes_read_populated_native_catalog(self):
        output = self.run_probe(
            "python_catalog_probe.py", self.root, self.instrument_id, 2
        )
        self.assertIn("Quotes loaded: 2", output)
        output = self.run_probe(
            "probe_segment_seal_readback.py",
            self.root,
            self.instrument_id,
            "--min-quotes",
            2,
        )
        self.assertIn("Quotes loaded: 2", output)

    def test_deribit_custom_rows_keep_native_envelope(self):
        from nautilus_trader.adapters.deribit import DeribitVolatilityIndex

        self.write_custom(
            "DeribitVolatilityIndex",
            DeribitVolatilityIndex("btc_usd", 50.5, 1000, 1000),
            {"index_name": "btc_usd"},
            "btc_usd",
        )
        self.write_custom(
            "DeribitVolatilityIndex",
            DeribitVolatilityIndex("eth_usd", 60.5, 1000, 1000),
            {"index_name": "eth_usd"},
            "eth_usd",
        )
        output = self.run_probe("python_catalog_deribit_dvol_probe.py", self.root, 1)
        self.assertIn("DVOL rows loaded: 1", output)

    def test_hyperliquid_custom_rows_keep_native_identity(self):
        from nautilus_trader.adapters.hyperliquid import HyperliquidOpenInterest

        instrument_id = self.write_market("ETH-USD-PERP.HYPERLIQUID", "ETH-USD", "USD")
        self.write_custom(
            "HyperliquidOpenInterest",
            HyperliquidOpenInterest(instrument_id, Decimal("123.45"), 1000, 1000),
            {"instrument_id": str(instrument_id)},
            str(instrument_id),
        )
        output = self.run_probe(
            "python_catalog_hyperliquid_open_interest_probe.py",
            self.root,
            instrument_id,
            1,
            "--min-quotes",
            2,
        )
        self.assertIn("Open-interest rows loaded: 1", output)

    def test_binance_custom_rows_use_typed_catalog_query(self):
        from nautilus_trader.adapters.binance import BinanceFuturesLiquidation

        m = self.model
        item = BinanceFuturesLiquidation(
            self.instrument_id,
            m.OrderSide.SELL,
            m.Price.from_str("3000.00"),
            m.Price.from_str("3000.00"),
            m.Quantity.from_str("0.100"),
            m.Quantity.from_str("0.100"),
            1000,
            1000,
        )
        self.write_custom(
            "BinanceFuturesLiquidation",
            item,
            {"instrument_id": str(self.instrument_id)},
            str(self.instrument_id),
        )
        for extra in ([], ["--all-market"]):
            with self.subTest(extra=extra):
                output = self.run_probe(
                    "python_catalog_binance_custom_probe.py",
                    self.root,
                    "BinanceFuturesLiquidation",
                    self.instrument_id,
                    1,
                    "--min-quotes",
                    2,
                    *extra,
                )
                self.assertIn("Custom rows loaded: 1", output)

    def test_hyperliquid_bars_use_native_file_inventory(self):
        m = self.model
        instrument_id = self.write_market("ETH-USD-PERP.HYPERLIQUID", "ETH-USD", "USD")
        bar_type = f"{instrument_id}-1-MINUTE-LAST-EXTERNAL"
        self.catalog.write_bars(
            [
                m.Bar(
                    m.BarType.from_str(bar_type),
                    m.Price.from_str("3000.00"),
                    m.Price.from_str("3002.00"),
                    m.Price.from_str("2999.00"),
                    m.Price.from_str("3001.00"),
                    m.Quantity.from_str("1.000"),
                    ts,
                    ts,
                )
                for ts in (60_000_000_000, 120_000_000_000)
            ]
        )
        output = self.run_probe(
            "python_hyperliquid_bars_probe.py",
            self.root,
            instrument_id,
            1,
            "--bar-type",
            bar_type,
            "--min-bar-rows",
            2,
            "--min-quotes",
            2,
        )
        self.assertIn("Bars loaded: 2", output)

    def test_derivatives_probe_uses_native_data_type_inventory(self):
        m = self.model
        self.catalog.write_mark_price_updates(
            [
                m.MarkPriceUpdate(
                    self.instrument_id, m.Price.from_str("3000.00"), 1000, 1000
                ),
            ]
        )
        self.catalog.write_index_price_updates(
            [
                m.IndexPriceUpdate(
                    self.instrument_id, m.Price.from_str("2999.00"), 1000, 1000
                ),
            ]
        )
        self.catalog.write_funding_rate_updates(
            [
                m.FundingRateUpdate(self.instrument_id, Decimal("0.0001"), 1000, 1000),
            ]
        )
        self.catalog.write_instrument_statuses(
            [
                m.InstrumentStatus(
                    self.instrument_id, m.MarketStatusAction.TRADING, 1000, 1000
                ),
            ]
        )
        self.catalog.write_instrument_closes(
            [
                m.InstrumentClose(
                    self.instrument_id,
                    m.Price.from_str("3000.00"),
                    m.InstrumentCloseType.END_OF_SESSION,
                    1000,
                    1000,
                ),
            ]
        )
        output = self.run_probe(
            "python_catalog_derivatives_probe.py",
            self.root,
            self.instrument_id,
            1,
            "--require-contract-state",
        )
        self.assertIn("Funding parquet files: 1", output)
        self.assertIn("Instrument statuses loaded: 1", output)
        self.assertIn("Instrument closes loaded: 1", output)
        script = f"""
import sys
sys.path.insert(0, {str(Path(__file__).parent.resolve())!r})
import python_catalog_option_universe_probe as probe
from pathlib import Path
from nautilus_trader.persistence import ParquetDataCatalog
root = Path({str(self.root)!r})
assert probe.assert_funding_files(root, {str(self.instrument_id)!r}) == (1, 1)
assert probe.probe_contract_state(ParquetDataCatalog(str(root)), root, {str(self.instrument_id)!r}, require=True) == (1, 1)
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
