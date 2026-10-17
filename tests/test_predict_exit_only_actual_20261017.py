"""Audit recorded mainnet restart/exit evidence; no network or credentials."""
import json
import unittest
from collections import Counter
from decimal import Decimal as D
from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "crates/nautilus-predict/tests/fixtures/private_api"
HOLD = BASE / "exit_only_restart_20261017T052850Z"
EXIT = BASE / "exit_only_restart_20261017T053156Z"


def read(path):
    return json.loads(path.read_text())


class ActualExitTests(unittest.TestCase):
    def test_cold_start_claim_no_history_callbacks_or_duplicate_exit(self):
        r = read(HOLD / "native_checkpoint.json")
        events = r["nativeFeedback"]
        adoption = next(e for e in events if e["type"] == "ExitOnlyAdopted")
        self.assertEqual(D(adoption["entryQuantity"]), D(32))
        self.assertEqual(D(adoption["entrySharesFee"]), D("0.64"))
        self.assertEqual(D(adoption["entryCost"]), D("1.28"))
        self.assertEqual(D(r["nativeNetQuantity"]), D("31.36"))
        self.assertEqual(r["nativeOpenOrders"], 1)
        self.assertFalse(any(e["type"] == "StrategyCallback" for e in events))
        self.assertFalse(any(e["type"] in {"NativeEntrySubmitted", "MakerTakeProfitSubmitted"} for e in events))
        writes = [e for e in read(HOLD / "http_responses.json")["events"] if e["method"] == "POST" and e["path"] != "/v1/auth"]
        self.assertEqual(writes, [])

    def test_cancel_once_then_market_fill_once_then_closed(self):
        r = read(EXIT / "native_roundtrip.json")
        callbacks = [e for e in r["nativeFeedback"] if e["type"] == "StrategyCallback"]
        names = [e["callback"] for e in callbacks]
        self.assertEqual(names, ["on_order_pending_cancel", "on_order_canceled", "on_order_submitted", "on_order_accepted", "on_order_filled", "on_position_closed"])
        self.assertEqual(Counter(names)["on_order_canceled"], 1)
        self.assertEqual(Counter(names)["on_order_filled"], 1)
        self.assertEqual(D(callbacks[-2]["cachedNetQuantity"]), 0)
        self.assertEqual(callbacks[-2]["cachedStatus"], "FILLED")
        self.assertEqual(D(callbacks[-1]["cachedNetQuantity"]), 0)
        self.assertTrue(r["sessionComplete"] and r["cashCurrent"])
        self.assertFalse(r["nodeFailed"])

    def test_raw_wallet_matches_chain_and_native_sell_fee(self):
        native = read(EXIT / "native_roundtrip.json")
        independent = BASE / (EXIT.name + "_independent")
        wallet = [json.loads(line) for line in (independent / "wallet_events.jsonl").read_text().splitlines()]
        success = [e for e in wallet if e["type"] == "orderTransactionSuccess"]
        self.assertEqual(len(success), 1)
        sale = success[0]
        self.assertEqual(sale["fee"]["type"], "COLLATERAL")
        fee = D(sale["fee"]["amountWei"]) / D(10**18)
        value = D(sale["fill"]["executedValueWei"]) / D(10**18)
        size = D(sale["fill"]["executedSizeWei"]) / D(10**18)
        self.assertEqual(size, D("31.36"))
        self.assertEqual(fee, D("0.0244608"))
        fills = [e["fill"] for e in native["nativeFeedback"] if e["type"] == "Filled"]
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]["side"], "SELL")
        self.assertIsNone(fills[0]["quantityCommission"])
        self.assertEqual(D(fills[0]["commission"].split()[0]), fee)
        calibration = read(independent / "calibration.json")
        match = next(e for e in calibration["comparisons"] if e["orderHash"] == sale["orderHash"])
        self.assertEqual(match["feeWei"], sale["fee"]["amountWei"])
        self.assertEqual(D(match["netTransferWei"]), -size * D(10**18))
        delta = (D(calibration["after"]["cashWei"]) - D(calibration["before"]["cashWei"])) / D(10**18)
        self.assertEqual(delta, value - fee)
        self.assertEqual(D(native["cashFlowUSDT"]), delta)

    def test_strategy_observed_reported_account_cache_after_settlement(self):
        r = read(EXIT / "native_roundtrip.json")
        snapshots = [e for e in r["nativeFeedback"] if e["type"] == "NativeAccountSnapshot"]
        self.assertEqual(D(snapshots[0]["balances"][0]["total"]), D("46.93630840"))
        self.assertEqual(D(snapshots[-1]["balances"][0]["total"]), D("48.13488760"))
        self.assertEqual(D(snapshots[-1]["balances"][0]["locked"]), 0)
        after = read(EXIT / "after.json")
        self.assertEqual(after["orders"], [])
        self.assertEqual(after["positions"], [])
        self.assertFalse(after["buyAllowed"])

    def test_cash_loss_and_native_quote_pnl_are_explicitly_distinct(self):
        r = read(EXIT / "native_roundtrip.json")
        proceeds = D(r["cashFlowUSDT"])
        self.assertEqual(proceeds - D("1.28"), D("-0.0814208"))
        position = next(e for e in r["nativeFeedback"] if e["type"] == "NativePositionApplied")
        native_pnl = D(position["realizedPnl"].split()[0])
        self.assertEqual(native_pnl, D("-0.0558208"))
        self.assertEqual(native_pnl - D("0.64") * D("0.04"), proceeds - D("1.28"))


if __name__ == "__main__":
    unittest.main()
