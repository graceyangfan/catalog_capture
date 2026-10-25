"""LGPL-3.0-or-later. Offline financial and cancellation-provenance controls."""

import unittest
from decimal import Decimal as D

from predict_partial_market_plan import (
    owned_cash_at_risk_wei,
    owned_exit_is_safe,
    owned_inventory_wei,
    partial_plan,
    remainder_provenance,
)
from predict_private_http_recorder import receipt_evidence


class PartialMarketPlan(unittest.TestCase):
    def test_cleanup_inventory_uses_only_unique_owned_confirmed_settlements(self):
        buy = {
            "type": "orderTransactionSuccess",
            "orderHash": "buy",
            "settlementId": "a",
            "fill": {"executedSizeWei": "100", "executedValueWei": "50"},
            "details": {"quoteType": "BID"},
            "fee": {"type": "SHARES", "amountWei": "2"},
        }
        sell = {
            "type": "orderTransactionSuccess",
            "orderHash": "sell",
            "settlementId": "b",
            "fill": {"executedSizeWei": "40", "executedValueWei": "25"},
            "details": {"quoteType": "ASK"},
            "fee": {"type": "COLLATERAL", "amountWei": "5"},
        }
        foreign = {**buy, "orderHash": "other"}
        planned = {**buy, "type": "orderMarketMatchPlan", "settlementId": "c"}
        self.assertEqual(
            owned_inventory_wei([buy, buy, sell, foreign, planned], {"buy", "sell"}), 58
        )
        self.assertEqual(owned_inventory_wei([foreign, planned], {"buy", "sell"}), 0)
        self.assertEqual(
            owned_cash_at_risk_wei([buy, buy, sell, foreign, planned], {"buy", "sell"}),
            30,
        )

    def test_exit_requires_remote_terminal_and_confirmed_owned_inventory(self):
        self.assertTrue(owned_exit_is_safe(1, [{"status": "FILLED"}], 100, 100))
        self.assertTrue(
            owned_exit_is_safe(
                2, [{"status": "FILLED"}, {"status": "CANCELLED"}], 100, 99
            )
        )
        for count, prior, inventory, sale in [
            (0, [], 100, 1),
            (1, [], 100, 1),
            (1, [{"status": "OPEN"}], 100, 1),
            (2, [{"status": "FILLED"}, {"status": "OPEN"}], 100, 1),
            (1, [{"status": "FILLED"}], 0, 1),
            (1, [{"status": "FILLED"}], 100, 101),
            (1, [{"status": "FILLED"}], 100, 0),
            (4, [{"status": "FILLED"}] * 4, 100, 1),
        ]:
            self.assertFalse(owned_exit_is_safe(count, prior, inventory, sale))

    def test_micro_remainder_requires_explicit_mode_and_stays_below_ten_cents(self):
        bids, asks = [[".49", "100"]], [[".50", ".1"], [".52", "100"]]
        self.assertIsNone(partial_plan(bids, asks, 200))
        p = partial_plan(bids, asks, 200, allow_micro=True)
        self.assertIsNotNone(p)
        self.assertEqual(p["visiblePrincipal"], "0.050")
        self.assertEqual(p["microRemainderAllowed"], "True")
        self.assertIsNone(partial_plan(bids, [[".50", ".3"]], 200, allow_micro=True))

    def test_partial_has_bounded_entry_and_liquidatable_visible_fill(self):
        p = partial_plan([[".49", "100"]], [[".50", "2.3"], [".51", "100"]], 200)
        self.assertIsNotNone(p)
        self.assertEqual(p["signedPrincipal"], "1.4000")
        self.assertEqual(p["visibleShares"], "2.3")
        for bids, asks in [
            ([[".49", "100"]], [[".50", "10"]]),
            ([[".49", "100"]], [[".50", ".2"]]),
            ([[".30", "100"]], [[".50", "2.3"]]),
            ([[".49", "1"]], [[".50", "2.3"]]),
        ]:
            self.assertIsNone(partial_plan(bids, asks, 200))

    def test_two_usdt_requires_explicit_cap_and_still_preserves_one_order_amount_bound(
        self,
    ):
        bids, asks = [[".90", "100"]], [[".91", "2"]]
        self.assertIsNone(partial_plan(bids, asks, 200, target_principal=D("1.99")))
        p = partial_plan(
            bids, asks, 200, target_principal=D("1.99"), max_principal=D(2)
        )
        self.assertIsNotNone(p)
        self.assertLessEqual(D(p["signedPrincipal"]), D("1.99"))
        self.assertGreaterEqual(D(p["quantity"]), D(p["visibleShares"]) * D("1.05"))
        self.assertIsNone(
            partial_plan(
                bids, asks, 200, target_principal=D("2.01"), max_principal=D(2)
            )
        )
        self.assertIsNone(
            partial_plan(
                bids, asks, 200, target_principal=D("2.01"), max_principal=D("2.1")
            )
        )

    def test_149_target_admits_observed_near_140_level_without_exceeding_cap(self):
        bids, asks = [[".84", "36.3114"]], [[".86", "1.6018"]]
        self.assertIsNone(partial_plan(bids, asks, 200))
        p = partial_plan(bids, asks, 200, target_principal=D("1.49"))
        self.assertIsNotNone(p)
        self.assertLessEqual(D(p["signedPrincipal"]), D("1.49"))
        self.assertGreaterEqual(D(p["quantity"]), D(p["visibleShares"]) * D("1.05"))
        self.assertIsNone(partial_plan(bids, asks, 200, target_principal=D("1.51")))

    def test_upper_visible_principal_still_requires_at_least_five_percent_remainder(
        self,
    ):
        p = partial_plan([[".54", "100"]], [[".55", "2.4"]], 200)
        self.assertIsNotNone(p)
        self.assertEqual(p["visiblePrincipal"], "1.320")
        self.assertGreaterEqual(D(p["quantity"]), D(p["visibleShares"]) * D("1.05"))
        self.assertIsNone(partial_plan([[".54", "100"]], [[".55", "2.5"]], 200))

    def test_multiple_levels_stay_within_three_percent_and_budget(self):
        p = partial_plan(
            [[".49", "100"]], [[".50", "1.3"], [".51", "1.1"], [".52", "100"]], 200
        )
        self.assertIsNotNone(p)
        self.assertEqual(p["referencePrice"], "0.51")
        self.assertEqual(p["visibleShares"], "2.4")
        self.assertEqual(p["signedPrincipal"], "1.3974")
        self.assertIsNone(
            partial_plan([[".49", "100"]], [[".50", "1.3"], [".52", "1.1"]], 200)
        )

    def test_terminal_after_cancel_is_not_automatically_attributed_to_us(self):
        event = {"type": "orderCancelled", "observedMonotonicNs": 100}
        self.assertEqual(
            remainder_provenance([event], 101),
            "venue_terminal_observed_before_our_cancel_request",
        )
        self.assertEqual(
            remainder_provenance([event], None),
            "venue_terminal_observed_before_our_cancel_request",
        )
        self.assertEqual(
            remainder_provenance([event], 100),
            "terminal_after_our_cancel_request_causality_not_proven",
        )
        self.assertEqual(remainder_provenance([], 100), "no_wallet_terminal_observed")

    def test_removal_rejection_remains_in_evidence(self):
        payload = {"success": True, "removed": [], "noop": [], "rejected": ["123"]}
        self.assertEqual(receipt_evidence(payload, []), payload)

    def test_rest_terminal_can_precede_cancel_without_wallet_terminal(self):
        row = {
            "completedMonotonicNs": 99,
            "response": {"data": {"status": "CANCELLED"}},
        }
        self.assertEqual(
            remainder_provenance([], 100, [row]),
            "venue_terminal_observed_before_our_cancel_request",
        )
        self.assertEqual(
            remainder_provenance([], 99, [row]),
            "terminal_after_our_cancel_request_causality_not_proven",
        )

    def test_match_plan_alone_is_not_an_automatic_terminal(self):
        event = {
            "type": "orderMarketMatchPlan",
            "remainderCancelled": "83000000000000000000",
            "observedMonotonicNs": 99,
        }
        self.assertEqual(
            remainder_provenance([event], None), "no_wallet_terminal_observed"
        )


if __name__ == "__main__":
    unittest.main()
