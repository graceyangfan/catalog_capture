"""Actual mainnet evidence precedes derived replay and synthetic boundaries. LGPL-3.0-or-later."""

import hashlib
import json
import unittest
from decimal import Decimal
from pathlib import Path

import test_predict_exit_protocol as protocol
from review_predict_fill_flow import review as review_fill_flow
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.types import Book, LimitHelperInput, MarketHelperInput, Side

FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


def read(name):
    return json.loads((FIXTURES / name).read_text())


class RecordedEvidenceTests(unittest.TestCase):
    def test_current_inventory_scope_does_not_adopt_older_closed_cycle(self):
        scope = read("current_inventory_history_scope_native_derived_20261017.json")
        self.assertTrue(scope["olderSyntheticClosedCycle"])
        self.assertEqual(
            scope["nativeFeedback"][0]["netPositions"], ["31.3600000000000000"]
        )
        self.assertEqual(scope["nativeFeedback"][0]["openOrders"], 1)
        mass = scope["repeated"]["massStatus"]
        self.assertEqual(
            set(mass["order_reports"]),
            {
                scope["buy"]["order"]["hash"],
                scope["takeProfit"]["order"]["hash"],
            },
        )
        self.assertEqual(scope["repeated"]["repeatedEvents"], 0)
        self.assertEqual(scope["orderWrites"], 0)

    def test_actual_economic_replay_is_independent_of_one_minute_history_window(self):
        replay = read("actual_trade_flow_native_replay_20261017_v3.json")
        self.assertIsNone(replay["reconciliationLookbackMins"])
        self.assertEqual(replay["positionCheckLookbackMins"], 60)
        self.assertEqual(replay["cycleRealizedPnl"], "-0.08142080 USDT")
        self.assertEqual(replay["finalOpenOrders"], 0)
        self.assertEqual(replay["finalPositions"], 0)

    def test_interleaved_cost_gap_is_not_misreported_as_successful_exit(self):
        replay = read(
            "different_price_partial_share_fees_native_synthetic_20261017_v2.json"
        )
        self.assertEqual(len(replay["sessions"]), 5)
        for session in replay["sessions"]:
            probe = session["positionOnlyExitProbe"]
            self.assertEqual(
                probe["pnlMatchesCash"], not session["interleavedFeeCorrection"]
            )
            self.assertIn("not a strategy or venue SELL", probe["source"])
        failure = replay["sessions"][-1]["positionOnlyExitProbe"]
        self.assertEqual(Decimal(failure["expectedCashPnL"]), Decimal("0.0106"))
        self.assertEqual(Decimal(failure["nativeRealizedPnL"]), Decimal("0.01044214"))

    def test_fill_callback_provenance_and_account_cache_are_not_conflated(self):
        result = review_fill_flow()
        self.assertFalse(result["actualCyclePnLAccepted"])
        self.assertTrue(result["actualFinalNativeAccountMatchesChain"])
        self.assertFalse(result["actualFinalCashObservedOnAccountTopic"])
        self.assertEqual(result["actualHistoricalFillsAdopted"], 2)
        self.assertEqual(
            result["correctedReplay"]["netPositionSequence"],
            ["32.0000000000000000", "31.3600000000000000", "0"],
        )

    def test_primary_live_sources_are_preserved_and_reviews_override_early_success(
        self,
    ):
        manifest = read("filled_live_validation_20261016.json")
        for entry in manifest["files"]:
            self.assertEqual(
                hashlib.sha256((FIXTURES / entry["path"]).read_bytes()).hexdigest(),
                entry["sha256"],
                entry["path"],
            )
        self.assertFalse(manifest["cycles"][0]["nativeAccountMatch"])
        self.assertFalse(manifest["cycles"][1]["nativePnlScopeCorrect"])
        self.assertFalse(any(c["complete"] for c in manifest["cycles"]))

    def test_recorded_live_fees_match_wallet_matches_chain_and_sdk_amounts(self):
        primary = read("live_fill_20261016T123240Z_independent/calibration.json")
        events = primary["wallet"]["events"]
        receipts = {r["orderHash"]: r for r in primary["comparisons"]}
        terms = [
            e["requestTerms"]
            for e in read("live_fill_20261016T123240Z/http_responses.json")["events"]
            if "requestTerms" in e
        ]
        self.assertEqual(len(terms), 3)
        builder = OrderBuilder.make(ChainId.BNB_MAINNET)
        raw_book = read("live_fill_20261016T123240Z/admission.json")["candidate"][
            "book"
        ]
        book = Book(
            market_id=raw_book["marketId"],
            update_timestamp_ms=raw_book["updateTimestampMs"],
            asks=raw_book["asks"],
            bids=raw_book["bids"],
        )
        expected = [
            builder.get_limit_order_amounts(
                LimitHelperInput(Side.BUY, 4 * 10**16, 32 * 10**18)
            ),
            builder.get_limit_order_amounts(
                LimitHelperInput(Side.SELL, 43 * 10**15, 3136 * 10**16)
            ),
            builder.get_market_order_amounts(
                MarketHelperInput(Side.SELL, 3136 * 10**16, slippage_bps=100), book
            ),
        ]
        for actual, sdk in zip(terms, expected, strict=True):
            self.assertEqual(int(actual["makerAmount"]), sdk.maker_amount)
            self.assertEqual(int(actual["takerAmount"]), sdk.taker_amount)
            self.assertEqual(actual["nonce"], "0")
            self.assertEqual(actual["feeRateBps"], "200")
        for event in (events[2], events[8]):
            receipt = receipts[event["orderHash"]]
            match = next(
                row
                for row in primary["after"]["matches"]
                if row["taker"]["hash"] == event["orderHash"]
            )
            leg = match["taker"]
            self.assertEqual(int(event["fill"]["executedSizeWei"]), int(leg["amount"]))
            self.assertEqual(int(event["fee"]["amountWei"]), int(leg["fee"]["amount"]))
            self.assertEqual(event["fee"]["type"], leg["fee"]["type"])
            self.assertEqual(int(receipt["grossWei"]), int(leg["amount"]))
            self.assertEqual(int(receipt["feeWei"]), int(leg["fee"]["amount"]))
        buy, sell = receipts[events[2]["orderHash"]], receipts[events[8]["orderHash"]]
        self.assertEqual(int(buy["netTransferWei"]), 3136 * 10**16)
        self.assertEqual(int(sell["netTransferWei"]), -3136 * 10**16)
        cash_delta = int(primary["after"]["cashWei"]) - int(
            primary["before"]["cashWei"]
        )
        self.assertEqual(
            cash_delta,
            -int(buy["collateralWei"])
            + int(sell["collateralWei"])
            - int(sell["feeWei"]),
        )
        self.assertEqual(Decimal(cash_delta) / 10**18, Decimal("-0.0814208"))
        plan = events[6]
        self.assertEqual(plan["type"], "orderMarketMatchPlan")
        self.assertEqual(int(plan["amountFilled"]), 0)

    def test_actual_economic_replay_signatures_and_native_callbacks_match_sdk(self):
        replay = read("actual_trade_flow_native_replay_20261016_v2.json")
        raw_book = read("live_fill_20261016T123240Z/admission.json")["candidate"][
            "book"
        ]
        session = {
            **replay,
            "entryPostOnly": False,
            "orderbooks": [{**raw_book, "marketId": 42}],
        }
        protocol.PredictExitProtocolTests().assert_sdk_requests(
            {"sessions": [session]}, 3
        )
        self.assertTrue(replay["marketFlags"]["isYieldBearing"])
        feedback = replay["nativeFeedback"]
        self.assertEqual(
            sum(r.get("callback") == "on_order_filled" for r in feedback), 3
        )
        # One BUY, one native SHARES adjustment, one SELL despite duplicate wallet packets.
        for callback in (
            "on_position_opened",
            "on_position_changed",
            "on_position_closed",
        ):
            self.assertEqual(sum(r.get("callback") == callback for r in feedback), 1)
        self.assertEqual(replay["finalOpenOrders"], 0)
        self.assertEqual(replay["finalPositions"], 0)
        self.assertEqual(replay["cycleRealizedPnl"], "-0.08142080 USDT")
        self.assertEqual(Decimal(replay["finalCash"]), Decimal("48.2163084"))

    def test_actual_read_only_startup_agrees_with_independent_sdk_chain_snapshot(self):
        independent = read("startup_readonly_20261016_v1/calibration.json")
        self.assertTrue(independent["summary"]["cashUnchanged"])
        self.assertEqual(independent["summary"]["chainVerifiedOwnFills"], 6)
        self.assertEqual(independent["after"]["orders"], [])
        self.assertEqual(independent["after"]["positions"], [])
        native = read("native_startup_readonly_20261016T130830Z_31130.json")
        self.assertTrue(native["nativeStartupReconciliation"])
        self.assertFalse(native["submissionSignerInstalled"])
        self.assertEqual(native["strategySubmissions"], 0)
        self.assertEqual(native["nativeCachedOpenOrders"], 0)
        self.assertEqual(native["nativeCachedOpenPositions"], 0)
        self.assertGreater(native["accountReportsObserved"], 0)
        self.assertGreater(len(native["snapshots"]), 0)
        chain_cash = Decimal(independent["after"]["cashWei"]) / 10**18
        self.assertEqual(
            Decimal(native["cashTotal"]), chain_cash.quantize(Decimal("0.00000001"))
        )
        offline = read("actual_startup_native_replay_20261016_v2.json")
        self.assertTrue(offline["reconciliation"])
        self.assertEqual(offline["historicalMatches"], 6)
        self.assertEqual(offline["submitted"], 0)
