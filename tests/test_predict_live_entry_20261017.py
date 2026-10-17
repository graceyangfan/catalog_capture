"""Replay actual entry evidence only; never connect or submit. LGPL-3.0-or-later."""
import json
import unittest
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN = ROOT / "crates/nautilus-predict/tests/fixtures/private_api/live_fill_20261017T051620Z"


class ActualEntryTests(unittest.TestCase):
    def test_wallet_gross_fee_and_native_strategy_net_agree(self):
        wallet = json.loads((RUN / "entry_wallet_observation.json").read_text())
        native = json.loads((RUN / "entry_native_observation.json").read_text())
        filled = next(e for e in wallet if e["type"] == "orderTransactionSuccess")
        gross = D(filled["fill"]["executedSizeWei"]) / 10**18
        fee = D(filled["fee"]["amountWei"]) / 10**18
        cost = D(filled["fill"]["executedValueWei"]) / 10**18
        self.assertEqual(filled["fee"]["type"], "SHARES")
        self.assertEqual((gross, fee, cost), (D(32), D(".64"), D("1.28")))
        journal = native["nativeFeedback"]
        entry = [e for e in journal if e["type"] == "Filled"]
        self.assertEqual(len(entry), 1)
        self.assertEqual(D(entry[0]["fill"]["quantity"]), gross)
        self.assertEqual(D(entry[0]["fill"]["quantityCommission"]), fee)
        self.assertEqual(D(native["nativeNetQuantity"]), gross - fee)
        callback = next(e for e in journal if e.get("callback") == "on_order_filled")
        self.assertEqual(callback["cachedStatus"], "FILLED")
        self.assertEqual(D(callback["leavesQuantity"]), 0)
        self.assertEqual(D(callback["cachedNetQuantity"]), gross - fee)
        self.assertTrue(any(e.get("callback") == "on_position_opened" for e in journal))
        self.assertEqual(D(native["cashFlowUSDT"]), -cost)
        self.assertEqual(native["balances"][0]["total"], "46.93630840 USDT")
        self.assertTrue(native["cashCurrent"])

    def test_accepted_profit_order_covers_entry_fee_but_exit_is_still_pending(self):
        audit = json.loads((RUN / "entry_validation.json").read_text())
        wallet = json.loads((RUN / "entry_wallet_observation.json").read_text())
        tp = next(e for e in wallet if e["type"] == "orderAccepted" and e["details"]["quoteType"] == "ASK")
        self.assertEqual(tp["orderHash"], audit["takeProfitOrderHash"])
        self.assertEqual(D(tp["details"]["quantity"]), D("31.36"))
        self.assertEqual(D(tp["details"]["price"]), D(".043"))
        profit = D("31.36") * D(".043") - D("1.28")
        self.assertEqual(profit, D(".06848"))
        self.assertFalse(any(e["type"] == "orderTransactionSuccess" and e["details"]["quoteType"] == "ASK" for e in wallet))
        self.assertIn("exit is pending", audit["kind"])


if __name__ == "__main__":
    unittest.main()
