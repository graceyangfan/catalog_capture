"""Preflight fee/tick boundaries; no network or credentials. LGPL-3.0-or-later."""

import unittest
import json
from pathlib import Path
from datetime import UTC, datetime
from decimal import Decimal as D

from capture_predict_private_fixtures import CaptureError
from predict_fill_preflight import native_market_book_gate, take_profit
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.order_builder import ADDRESSES_BY_CHAIN_ID, make_contracts
from web3 import Web3


class PreflightTests(unittest.TestCase):
    def plan(self):
        return {
            "entryPrice": ".04",
            "entryCost": "1.28",
            "conservativeNetQuantity": "31.36",
            "bid": ".039",
        }

    def test_cash_floor_covers_shares_fee_even_when_delta_is_small(self):
        profit = take_profit(self.plan(), 3, D(".0001"))
        self.assertEqual(D(profit["price"]), D(".041"))
        self.assertEqual(D(profit["expectedCashProfitUSDT"]), D(".00576"))
        self.assertEqual(D(profit["expectedMakerFeeUSDT"]), 0)
        self.assertLessEqual(
            (D(profit["price"]) - D(profit["tick"])) * D("31.36"), D("1.28")
        )

    def test_standard_delta_uses_net_inventory_without_double_charging_shares(self):
        profit = take_profit(self.plan(), 3)
        self.assertEqual(D(profit["price"]), D(".043"))
        self.assertEqual(D(profit["expectedCashProfitUSDT"]), D(".06848"))

    def test_exact_break_even_moves_one_tick_to_positive_cash(self):
        plan = self.plan()
        plan["entryCost"] = str(D("31.36") * D(".041"))
        profit = take_profit(plan, 3, D(".001"))
        self.assertEqual(D(profit["price"]), D(".042"))
        self.assertGreater(D(profit["expectedCashProfitUSDT"]), 0)

    def test_preflight_matches_current_adapter_market_exit_guard(self):
        for age, allowed in [
            (4999, True),
            (5000, True),
            (5001, True),
            (-1, False),
            (103712, True),
        ]:
            gate = native_market_book_gate(
                {"marketId": 42, "updateTimestampMs": 1000000}, 42, 1000000 + age
            )
            self.assertEqual(gate["allowedByCurrentAdapter"], allowed)
        self.assertFalse(
            native_market_book_gate({"marketId": 43, "updateTimestampMs": 1}, 42, 1)[
                "allowedByCurrentAdapter"
            ]
        )

    def test_sdk_contracts_are_created_without_signer_or_network(self):
        self.assertIsNone(OrderBuilder.make(ChainId.BNB_MAINNET).contracts)
        contracts = make_contracts(Web3(), ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET])
        owner = "0x0000000000000000000000000000000000000011"
        self.assertIsNotNone(contracts.usdt.functions.balanceOf(owner))
        self.assertIsNotNone(contracts.usdt.functions.allowance(owner, owner))
        self.assertIsNotNone(
            contracts.conditional_tokens.functions.isApprovedForAll(owner, owner)
        )

    def test_recorded_quiet_book_passes_current_guard_and_preserves_historical_review(self):
        fixtures = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        raw = json.loads(
            (
                fixtures / "fill_preflight_readonly_20261017T041816Z_65566.json"
            ).read_text()
        )
        review = json.loads(
            (fixtures / "fill_preflight_review_20261017.json").read_text()
        )
        self.assertEqual((raw["currentOrders"], raw["currentPositions"]), ([], []))
        self.assertEqual(D(raw["chain"]["cashUSDT"]), D("48.216308400000000001"))
        candidate = raw["candidate"]
        event = next(
            e
            for e in raw["responses"]
            if e["path"] == f"/v1/markets/{candidate['marketId']}/orderbook"
        )
        elapsed = datetime.fromisoformat(event["completedAtUtc"]) - datetime(
            1970, 1, 1, tzinfo=UTC
        )
        gate = native_market_book_gate(
            candidate["book"],
            candidate["marketId"],
            elapsed.days * 86400000
            + elapsed.seconds * 1000
            + elapsed.microseconds // 1000,
        )
        self.assertEqual(gate["venueBookAgeMs"], 103712)
        self.assertTrue(gate["allowedByCurrentAdapter"])
        self.assertFalse(review["eligibleForLiveProposal"])
        self.assertTrue(review["supersedesInitialEligibility"]["rawFileFlag"])
        self.assertEqual(
            candidate["takeProfit"],
            take_profit(candidate["plan"], candidate["market"]["decimalPrecision"]),
        )
        lines = (
            (
                fixtures
                / "fill_preflight_readonly_20261017T041816Z_65566.responses.jsonl"
            )
            .read_text()
            .splitlines()
        )
        self.assertEqual([json.loads(line) for line in lines], raw["responses"])

    def test_real_strategy_queries_refresh_account_and_dedupe_historical_cancel(self):
        fixtures = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        raw = json.loads(
            (
                fixtures / "query_commands_readonly_20261017T041540Z_65186.json"
            ).read_text()
        )
        self.assertTrue(raw["passed"])
        self.assertEqual(raw["orderWrites"], 0)
        self.assertEqual(raw["chainWrites"], 0)
        self.assertTrue(
            all(
                e["method"] == "GET" or e["path"] in ("/v1/auth", "/rpc")
                for e in raw["events"]
            )
        )
        feedback = raw["rustFeedback"]
        self.assertFalse(feedback["submissionSignerInstalled"])
        self.assertFalse(feedback["orderbookRemovalEnabled"])
        journal = feedback["strategyJournal"]
        before = next(e for e in journal if e["type"] == "BeforeQueryAccount")
        after = next(e for e in journal if e["type"] == "AfterQueryAccount")
        self.assertNotEqual(before["eventId"], after["eventId"])
        self.assertGreater(after["info"]["blockNumber"], before["info"]["blockNumber"])
        self.assertEqual(after["balances"][0]["total"], "48.21630840 USDT")
        self.assertEqual(sum(e["type"] == "StrategyQueryOrder" for e in journal), 2)
        self.assertEqual(sum(e["type"] == "on_order_canceled" for e in journal), 1)
        self.assertFalse(any(e.get("kind") == "Filled" for e in journal))

    def test_invalid_inventory_or_unreachable_price_is_rejected(self):
        for net, cost in [("0", "1.28"), ("1", "2")]:
            plan = self.plan()
            plan.update(conservativeNetQuantity=net, entryCost=cost)
            with self.assertRaises(CaptureError):
                take_profit(plan, 3)


if __name__ == "__main__":
    unittest.main()
