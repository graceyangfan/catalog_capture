"""Bounded-entry and SELL evidence regressions. LGPL-3.0-or-later."""

import copy
import json
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path

from predict_fill_plan import plan_round_trip
from predict_read_only_calibration import settlement_flow
from run_predict_fill_live import (
    FillPolicy,
    history_settled_before_window,
    native_account_matches,
    native_pnl_matches,
)


class RoundTripTests(unittest.TestCase):
    def test_fee_net_floor_depth_and_budget(self):
        plan = plan_round_trip([[".039", "10000"]], [[".04", "10000"]], 200)
        self.assertEqual(plan["grossQuantity"], "32")
        self.assertEqual(D(plan["entryCost"]), D("1.28"))
        self.assertEqual(D(plan["conservativeNetQuantity"]), D("31.36"))
        self.assertGreater(D(plan["signedExitFloor"]), D("1.05"))
        self.assertGreater(D(plan["netExitFloor"]), D(1))
        for bids, asks, fee, cap in [
            ([[".009", "10000"]], [[".01", "10000"]], 200, D("1.5")),
            ([[".039", "10"]], [[".04", "10000"]], 200, D("1.5")),
            ([[".039", "10000"]], [[".04", "10"]], 200, D("1.5")),
            ([[".039", "10000"]], [[".04", "10000"]], 200, D("1.2")),
        ]:
            self.assertIsNone(plan_round_trip(bids, asks, fee, max_cost=cap))

    def test_integer_fee_share_and_collateral_sell_deltas(self):
        self.assertEqual(
            settlement_flow(True, 32 * 10**18, 640000000000000000, "SHARES"),
            31360000000000000000,
        )
        self.assertEqual(
            settlement_flow(False, 31360000000000000000, 100, "COLLATERAL"),
            -31360000000000000000,
        )
        self.assertEqual(settlement_flow(True, 100, 0, "COLLATERAL"), 100)
        for args in [
            (False, 100, 1, "SHARES"),
            (True, 100, 101, "SHARES"),
            (True, 100, 0, "UNKNOWN"),
        ]:
            with self.assertRaises(ValueError):
                settlement_flow(*args)

    def test_relay_allows_one_exact_entry_and_inventory_bounded_exits(self):
        identity = "0x" + "22" * 20
        plan = plan_round_trip([[".039", "10000"]], [[".04", "10000"]], 200)
        policy = FillPolicy(
            identity, {"tokenId": "11", "feeRateBps": 200, "plan": plan}
        )
        data = {
            "strategy": "LIMIT",
            "isPostOnly": False,
            "order": {
                "maker": identity,
                "signer": identity,
                "hash": "0x" + "ab" * 32,
                "tokenId": "11",
                "side": 0,
                "feeRateBps": "200",
                "makerAmount": "1280000000000000000",
                "takerAmount": str(32 * 10**18),
            },
        }
        self.assertTrue(policy(data, 0))
        self.assertFalse(policy(data, 1))
        sell = copy.deepcopy(data)
        sell["order"].update(
            side=1,
            makerAmount="31360000000000000000",
            takerAmount="1348480000000000000",
        )
        sell["isPostOnly"] = True
        self.assertTrue(policy(sell, 1))
        self.assertFalse(policy(sell, 4))
        sell["order"]["makerAmount"] = str(32 * 10**18)
        self.assertFalse(policy(sell, 1))
        sell["order"]["makerAmount"] = "31360000000000000000"
        sell["strategy"] = "MARKET"
        sell.pop("isPostOnly")
        sell["slippageBps"] = 100
        self.assertTrue(policy(sell, 2))
        sell["slippageBps"] = 200
        self.assertFalse(policy(sell, 2))

    def test_actual_first_fill_is_not_a_complete_account_cycle(self):
        root = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        native = json.loads(
            (root / "live_fill_20261016T122233Z/native_roundtrip.json").read_text()
        )
        independent = json.loads(
            (
                root / "live_fill_20261016T122233Z_independent/calibration.json"
            ).read_text()
        )
        self.assertEqual(native["nativeNetQuantity"], "0")
        self.assertFalse(native_account_matches(native, independent))
        fixed = copy.deepcopy(native)
        fixed["balances"][0]["total"] = "48.29772920 USDT"
        self.assertTrue(native_account_matches(fixed, independent))
        sell = [r for r in independent["comparisons"] if r.get("side") == "SELL"]
        self.assertEqual(len(sell), 1)
        self.assertEqual(sell[0]["feeType"], "COLLATERAL")
        self.assertEqual(sell[0]["netTransferWei"], "-31360000000000000000")

    def test_actual_second_cycle_account_matches_but_historical_pnl_is_rejected(self):
        root = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        native = json.loads(
            (root / "live_fill_20261016T123240Z/native_roundtrip.json").read_text()
        )
        independent = json.loads(
            (
                root / "live_fill_20261016T123240Z_independent/calibration.json"
            ).read_text()
        )
        self.assertTrue(native_account_matches(native, independent))
        self.assertFalse(native_pnl_matches(native, independent))
        fixed = copy.deepcopy(native)
        for row in fixed["nativeFeedback"]:
            if (
                row.get("type") == "NativePositionApplied"
                and row.get("quantity") == "0"
            ):
                row["realizedPnl"] = "-0.08142080 USDT"
        self.assertTrue(native_pnl_matches(fixed, independent))

    def test_native_lookback_requires_no_recent_prior_settlements(self):
        now = datetime(2026, 10, 16, tzinfo=UTC)
        for seconds, allowed in [(61, True), (60, False), (59, False), (-1, False)]:
            rows = [{"executedAt": (now - timedelta(seconds=seconds)).isoformat()}]
            self.assertEqual(history_settled_before_window(rows, now), allowed)

    def test_saved_native_scope_replay_preserves_current_round_trip_only(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api/live_fill_scope_synthetic_20261016.json"
        )
        data = json.loads(path.read_text())
        session = data["session"]
        self.assertIn("synthetic", data["source"])
        self.assertEqual(data["nativePositionLookbackMins"], 1)
        self.assertEqual(session["remainingPosition"], 0)
        for byte in ("cc", "dd"):
            self.assertFalse(
                any(
                    r.get("clientOrderId") == "0x" + byte * 32
                    for r in session["nativeFeedback"]
                )
            )
        final = [
            r
            for r in session["nativeFeedback"]
            if r["type"] == "NativePositionApplied" and r["quantity"] == "0"
        ][-1]
        self.assertEqual(
            D(final["realizedPnl"].split()[0]), D(session["finalCash"]) - 1000
        )
        self.assertEqual(
            [r["data"]["strategy"] for r in session["requests"]],
            ["LIMIT", "LIMIT", "MARKET"],
        )

    def test_saved_actual_scan_is_replayable_without_network(self):
        data = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "crates/nautilus-predict/tests/fixtures/private_api/fill_roundtrip_readonly_scan_20261016.json"
            ).read_text()
        )
        candidates = []
        for row in data["markets"]:
            if row.get("book"):
                plan = plan_round_trip(
                    row["book"]["bids"],
                    row["book"]["asks"],
                    row["market"]["feeRateBps"],
                )
                if plan:
                    candidates.append(plan)
        self.assertTrue(candidates)
        self.assertTrue(
            all(
                D(p["entryCost"]) <= D("1.5") and D(p["netExitFloor"]) >= 1
                for p in candidates
            )
        )
