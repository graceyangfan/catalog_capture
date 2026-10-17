"""Offline SDK comparison of actual loopback strategy requests. LGPL-3.0-or-later."""

import json
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId, Side
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import Book, BuildOrderInput, LimitHelperInput, MarketHelperInput

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api/exit_strategy_synthetic_20261015.json"
)
PUBLIC_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"


class PredictExitProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads(FIXTURE.read_text())

    def test_native_strategy_sell_requests_match_official_sdk_amounts_hash_and_signature(
        self,
    ):
        self.assert_sdk_requests(self.fixture, 6)

    def test_strategy_command_buy_and_sell_requests_match_official_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name("strategy_commands_synthetic_20261015.json").read_text()
        )
        self.assert_sdk_requests(fixture, 16)

    def test_native_reconciliation_uses_unique_matches_and_official_sdk_requests(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_reconciliation_live_node_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for stage, session in enumerate(fixture["sessions"], 1):
            self.assertEqual(session["nativeReconciliationStage"], stage)
            self.assertFalse(session["explicitStrategyOrderQueries"])
            self.assertFalse(session["customRecoveryTimerEnabled"])
            self.assertEqual(session["primaryFillCallbacks"], 2)
            fills = [
                r
                for r in session["nativeFeedback"]
                if r.get("callback") == "on_order_filled"
            ]
            self.assertEqual(len({r["tradeId"] for r in fills}), 2)
            self.assertTrue(all(r["tradeId"].startswith("PF-") for r in fills))
            self.assertEqual(sum(Decimal(r["lastQty"]) for r in fills), 100)
            self.assertEqual(Decimal(session["netPosition"]), 100)
            self.assertEqual(Decimal(session["cashTotal"]), 938)
            self.assertEqual(Decimal(session["cashLocked"]), 0)
            self.assertEqual(session["orderPosts"], 1)
        self.assert_sdk_requests(
            {"sessions": [{"requests": [s["request"]]} for s in fixture["sessions"]]}, 2
        )

    def test_bulk_mass_report_failures_and_signed_requests_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name("bulk_mass_reports_synthetic_20261016.json").read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 11)
        for session in fixture["sessions"]:
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["nativeStatusBefore"], "SUBMITTED")
            if session["case"] == "valid":
                report = session["massStatus"]
                self.assertTrue(report["reports_complete"])
                self.assertEqual(len(report["order_reports"]), 1)
                self.assertEqual(
                    sum(len(v) for v in report["fill_reports"].values()), 2
                )
                self.assertEqual(len(report["position_reports"]), 1)
            else:
                self.assertTrue(session["error"])
                self.assertTrue(session["repairedMassStatus"]["reports_complete"])
                self.assertEqual(
                    sum(
                        len(v)
                        for v in session["repairedMassStatus"]["fill_reports"].values()
                    ),
                    2,
                )
        self.assert_sdk_requests(
            {"sessions": [{"requests": [s["request"]]} for s in fixture["sessions"]]},
            11,
        )

    def test_native_account_verification_resumes_one_maker_without_replaying_fills(
        self,
    ):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_account_resume_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 1)
        for session in fixture["sessions"]:
            self.assertEqual(session["nativeReconciliationStage"], 2)
            self.assertFalse(session["explicitStrategyOrderQueries"])
            self.assertTrue(session["customRecoveryTimerEnabled"])
            self.assertFalse(session["newSubmissionsBlocked"])
            self.assertEqual(session["orderPosts"], 2)
            self.assertEqual(session["primaryFillCallbacks"], 2)
            self.assertEqual(Decimal(session["netPosition"]), 100)
            self.assertEqual(Decimal(session["cashLocked"]), 0)
            self.assertTrue(
                any(
                    r.get("type") == "ExecutionConnectivityVerified"
                    and r["afterNativeReconciliation"]
                    for r in session["nativeFeedback"]
                )
            )
        self.assert_sdk_requests(
            {
                "sessions": [
                    {"requests": [s["request"], s["resumedRequest"]]}
                    for s in fixture["sessions"]
                ]
            },
            2,
        )

    def test_native_inflight_and_account_recovery_preserve_cancel_stop_barrier(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_account_exits_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for session in fixture["sessions"]:
            self.assertTrue(session["nativeInflightChecks"])
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["remainingPosition"], 0)
            self.assertEqual(Decimal(session["finalCash"]), Decimal("999.80536"))
            self.assertEqual(len(session["removals"]), 2)
            self.assertEqual(len(session["requests"]), 3)
            self.assertEqual(session["requests"][1]["data"]["strategy"], "LIMIT")
            self.assertTrue(session["requests"][1]["data"]["isPostOnly"])
            self.assertEqual(session["requests"][2]["data"]["strategy"], "MARKET")
            self.assertEqual(
                session["requests"][2]["data"]["order"]["makerAmount"],
                "154000000000000000000",
            )
            fills = [
                r
                for r in session["nativeFeedback"]
                if r.get("callback") == "on_order_filled"
            ]
            self.assertEqual(len(fills), 5)
            states = [
                r["connected"]
                for r in session["nativeFeedback"]
                if r.get("type") == "ExecutionConnectionObserved"
            ]
            self.assertEqual(states, [True, False, True])
        self.assert_sdk_requests(fixture, 6)

    def test_native_restart_reuses_exit_intent_and_sdk_orders_without_duplicate_tp(
        self,
    ):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_strategy_restart_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 5)
        for session in fixture["sessions"]:
            self.assertTrue(session["nativeCacheInitiallyEmpty"])
            self.assertTrue(session["cachePointersDifferent"])
            self.assertEqual(session["mainnetReads"], 0)
            self.assertEqual(session["mainnetWrites"], 0)
            events = session["nativeFeedback"]
            kinds = [r.get("type") for r in events]
            self.assertNotIn("MakerTakeProfitSubmitted", kinds)
            self.assertNotIn("NativeEntrySubmitted", kinds)
            if session["case"] in {"MissingIntent", "WrongRole"}:
                self.assertIn("RestartRefused", kinds)
                self.assertNotIn("RestartAdopted", kinds)
                self.assertTrue(session["startupError"])
                self.assertEqual(len(session["requests"]), 2)
                self.assertEqual(session["finalOpenOrders"], 1)
                self.assertEqual(Decimal(session["finalNetPosition"]), 200)
                continue
            self.assertIsNone(session["startupError"])
            self.assertEqual(session["finalOpenOrders"], 0)
            self.assertEqual(Decimal(session["finalNetPosition"]), 0)
            self.assertEqual(Decimal(session["finalLockedCash"]), 0)
            adopted = next(r for r in events if r.get("type") == "RestartAdopted")
            self.assertEqual(Decimal(adopted["entryCost"]), 2)
            if session["case"] == "FilledOffline":
                self.assertIsNone(adopted["maker"])
                self.assertEqual(len(session["requests"]), 2)
                self.assertNotIn("TakerStopSubmitted", kinds)
            else:
                quantity = 154 if session["case"] == "PartialStop" else 200
                self.assertEqual(adopted["maker"], session["makerClientOrderId"])
                self.assertEqual(Decimal(adopted["netPosition"]), quantity)
                self.assertEqual(len(session["requests"]), 3)
                self.assertLess(
                    kinds.index("Canceled"), kinds.index("TakerStopSubmitted")
                )
                self.assertEqual(
                    session["requests"][2]["data"]["order"]["makerAmount"],
                    str(quantity * 10**18),
                )
            orders = session["finalNativeOrders"]
            self.assertEqual(len({r["clientOrderId"] for r in orders}), len(orders))
            trade_ids = [t for r in orders for t in r["tradeIds"]]
            self.assertEqual(len(set(trade_ids)), len(trade_ids))
            self.assertTrue(all(t.startswith("PF-") for t in trade_ids))
        self.assert_sdk_requests(fixture, 12)

    def test_existing_native_submission_retention_survives_query_exhaustion(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_submission_retention_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 1)
        session = fixture["sessions"][0]
        self.assertEqual(session["orderPosts"], 1)
        self.assertEqual(session["primaryFillCallbacks"], 2)
        diagnostics = session["submissionRecoveryDiagnostics"]
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["source"], "Inflight")
        self.assertEqual(diagnostics[0]["retryCount"], 2)
        retained = next(
            r
            for r in session["nativeFeedback"]
            if r.get("type") == "NativeRecoveryRetained"
        )
        self.assertEqual(retained["status"], "SUBMITTED")
        self.assertEqual(Decimal(session["netPosition"]), 100)
        self.assert_sdk_requests({"sessions": [{"requests": [session["request"]]}]}, 1)

    def test_missing_ack_recovery_requests_match_official_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name("query_recovery_synthetic_20261015.json").read_text()
        )
        for session in fixture["sessions"]:
            session["requests"] = session.get("requests") or [session["request"]]
        self.assert_sdk_requests(fixture, 10)

    def test_resumed_maker_orders_match_sdk_without_duplicate_entry(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "reconciliation_ready_live_node_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for session in fixture["sessions"]:
            self.assertEqual(session["primaryFillCallbacks"], 2)
            self.assertEqual(session["orderPosts"], 1 if session["sharesFee"] else 2)
            self.assertFalse(session["newSubmissionsBlocked"])
            self.assertEqual(Decimal(session["cashLocked"]), 0)
        self.assert_sdk_requests(
            {
                "sessions": [
                    {"requests": [s["resumedRequest"]]} for s in fixture["sessions"]
                ]
            },
            2,
        )

    def test_automatic_reconnect_preserves_take_profit_and_sdk_contract(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "automatic_reconnect_exit_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for session in fixture["sessions"]:
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["remainingPosition"], 0)
            self.assertEqual(len(session["removals"]), 2)
            self.assertEqual(len(session["requests"]), 3)
            fills = [
                r
                for r in session["nativeFeedback"]
                if r.get("callback") == "on_order_filled"
            ]
            self.assertEqual(len(fills), 5)
            self.assertEqual(
                session["requests"][2]["data"]["order"]["makerAmount"],
                "154000000000000000000",
            )
            self.assertTrue(
                any(
                    r.get("reconciliationReady") and r.get("connectionEpoch") == 1
                    for r in session["nativeFeedback"]
                )
            )
        self.assert_sdk_requests(fixture, 6)

    def test_gap_history_contract_hashes_and_kernel_signatures_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "matches_gap_native_feedback_synthetic_20261015.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        self.assert_history_contracts(fixture)

    def test_native_command_rejections_and_batch_results_match_saved_contract(self):
        fixture = json.loads(
            FIXTURE.with_name("command_alignment_synthetic_20261016.json").read_text()
        )
        gap = json.loads(
            FIXTURE.with_name(
                "command_gap_rejections_synthetic_20261016.json"
            ).read_text()
        )
        fixture["sessions"].extend(gap["sessions"])
        self.assertEqual(len(fixture["sessions"]), 5)
        for session in fixture["sessions"]:
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["finalOpenOrders"], 0)
            self.assertEqual(session["finalPositions"], 0)
            self.assertEqual(Decimal(session["finalCash"]), 1000)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            feedback = session["feedback"]
            rejects = [r for r in feedback if r["type"] == "CancelRejected"]
            if session["case"] in ("PreflightRejected", "DisconnectedPreflight"):
                self.assertEqual(session["removals"], [])
                self.assertEqual(len(rejects), 1)
                modify = next(r for r in feedback if r["type"] == "ModifyRejected")
                self.assertEqual(modify["status"], "ACCEPTED")
                self.assertEqual(modify["price"], "0.010")
                self.assertEqual(rejects[0]["status"], "ACCEPTED")
                if session["case"] == "DisconnectedPreflight":
                    self.assertIn("not connected", modify["reason"])
                    self.assertIn("not connected", rejects[0]["reason"])
            elif session["case"] == "BatchMixed":
                self.assertEqual(len(rejects), 1)
                self.assertEqual(len(session["removals"]), 3)
            elif session["case"] == "BatchUnknown":
                self.assertEqual(rejects, [])
                self.assertEqual(len(session["removals"]), 2)
                self.assertEqual(
                    sum(r["status"] == 500 for r in session["removalResponses"]), 1
                )
                self.assertEqual(session["wallet"][-1], session["wallet"][-2])
            else:
                self.assertEqual(rejects, [])
                self.assertEqual(len(session["removals"]), 2)
        self.assert_sdk_requests(fixture, 2)

    def test_read_only_share_reports_and_native_restart_match_sdk(self):
        shares = json.loads(
            FIXTURE.with_name(
                "readonly_share_recovery_synthetic_20261016.json"
            ).read_text()
        )
        session = shares["sessions"][0]
        self.assertTrue(session["readOnly"])
        self.assertEqual(session["orderPosts"], 0)
        fills = [
            row
            for row in session["nativeFeedback"]
            if row.get("callback") == "on_order_filled"
            and row["clientOrderId"] == "IMPORTED-TAKER-001"
        ]
        self.assertEqual(len(fills), 2)
        self.assertEqual(len({row["tradeId"] for row in fills}), 2)
        self.assertEqual(sum(Decimal(row["lastQty"]) for row in fills), 100)
        self.assertEqual(Decimal(session["netPosition"]), 98)
        self.assert_history_contracts(shares)
        restart = json.loads(
            FIXTURE.with_name(
                "readonly_native_restart_synthetic_20261016.json"
            ).read_text()
        )
        self.assertFalse(restart["restartSubmissionEnabled"])
        session = restart["session"]
        self.assertEqual(Decimal(session["finalNetPosition"]), 0)
        self.assertEqual(session["finalOpenOrders"], 0)
        self.assertEqual(Decimal(session["finalLockedCash"]), 0)
        self.assert_sdk_requests({"sessions": [session]}, 2)

    def assert_history_contracts(self, fixture):
        builder = OrderBuilder(
            ChainId.BNB_MAINNET,
            18,
            ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
            lambda: "1",
            Logger("ERROR"),
            Account.from_key(PUBLIC_KEY),
            "0x1111111111111111111111111111111111111111",
        )
        for session in fixture["sessions"]:
            data = session["request"]["data"]
            wire = data["order"]
            with patch(
                "predict_sdk.order_builder.time.time",
                return_value=int(wire["expiration"]) - 300,
            ):
                order = builder.build_order(
                    data["strategy"],
                    BuildOrderInput(
                        side=Side(wire["side"]),
                        token_id=wire["tokenId"],
                        maker_amount=int(wire["makerAmount"]),
                        taker_amount=int(wire["takerAmount"]),
                        fee_rate_bps=int(wire["feeRateBps"]),
                        nonce=int(wire["nonce"]),
                        salt=wire["salt"],
                        expires_at=datetime.fromtimestamp(int(wire["expiration"]), UTC),
                    ),
                )
            typed = builder.build_typed_data(
                order, is_neg_risk=False, is_yield_bearing=False
            )
            self.assertEqual(builder.build_typed_data_hash(typed), wire["hash"])
            signed = builder.sign_typed_data_order(typed)
            self.assertEqual(
                "0x" + signed.signature.removeprefix("0x").lower(), wire["signature"]
            )
            self.assertEqual(session["primaryFillCallbacks"], 2)
            self.assertEqual(
                Decimal(session["netPosition"]), 98 if session["sharesFee"] else 100
            )
            self.assertEqual(Decimal(session["cashTotal"]), 938)

    def test_multi_submit_live_node_uses_native_risk_and_preserves_unknown_order(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "multi_submission_native_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 3)
        for session in fixture["sessions"]:
            self.assertEqual(session["initialCachedOrders"], [])
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["finalOpenOrders"], 0)
            self.assertEqual(session["finalPositions"], 0)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            if session["case"] == "LiveRiskDenied":
                self.assertEqual(len(session["requests"]), 1)
                denied = [r for r in session["feedback"] if r["type"] == "Denied"]
                self.assertEqual(len(denied), 1)
                self.assertTrue(
                    denied[0]["reason"].startswith("NOTIONAL_EXCEEDS_FREE_BALANCE")
                )
                self.assertEqual(Decimal(session["finalCash"]), Decimal("1.5"))
            else:
                self.assertEqual(len(session["requests"]), 2)
                self.assertEqual(len(session["removals"]), 2)
                if session["case"] == "LiveMultiple":
                    self.assertEqual(Decimal(session["finalCash"]), 2)
                else:
                    retained = next(
                        r
                        for r in session["feedback"]
                        if r["type"] == "UnknownRetainedWhileSecondPosted"
                    )
                    self.assertEqual(retained["status"], "SUBMITTED")
                    self.assertEqual(retained["posts"], 2)
        self.assert_sdk_requests(fixture, 5)

    def test_multi_admission_deduplicates_open_and_native_resource_claims(self):
        fixture = json.loads(
            FIXTURE.with_name("multi_admission_synthetic_20261016.json").read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 10)
        for session in fixture["sessions"]:
            case = session["case"]
            self.assertEqual(session["orderPosts"], session["expectedPosts"])
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(session["finalOpenOrders"], 0)
            self.assertEqual(session["finalInflightOrders"], 0)
            if case in (
                "queued_buy_funds",
                "unknown_buy_funds",
                "queued_sell_inventory",
            ):
                self.assertEqual(session["orderPosts"], 1)
            else:
                self.assertEqual(session["orderPosts"], 2)
            if case == "queued_lifetime_cap":
                self.assertEqual(
                    sum(
                        row["status"] == "REJECTED" for row in session["beforeCleanup"]
                    ),
                    1,
                )
            if case in (
                "unknown_buy_funds",
                "unknown_buy_then_sell",
                "pending_buy_then_sell",
            ):
                self.assertEqual(session["beforeCleanup"][0]["status"], "SUBMITTED")
            hashes = [r["data"]["order"]["hash"] for r in session["requests"]]
            self.assertEqual(len(hashes), len(set(hashes)))
        self.assert_sdk_requests(fixture, 17)

    def test_multi_admission_change_preserves_strategy_exit_linkage(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "multi_admission_exit_replay_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 3)
        for session in fixture["sessions"]:
            self.assertEqual(session["remainingPosition"], 0)
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(len(session["requests"]), 3)
            self.assertEqual(Decimal(session["finalCash"]), Decimal("999.80536"))
            self.assertEqual(session["requests"][-1]["data"]["strategy"], "MARKET")
        self.assert_sdk_requests(fixture, 9)

    def test_two_native_strategies_keep_order_ownership_through_exit(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "concurrent_strategy_exit_synthetic_20261016.json"
            ).read_text()
        )
        (session,) = fixture["sessions"]
        self.assertEqual(session["case"], "concurrent_strategy_stop")
        self.assertEqual(session["mainnetWrites"], 0)
        self.assertEqual(len(session["requests"]), 4)
        self.assertEqual(len(session["removals"]), 3)
        self.assertEqual(session["removals"][-1]["data"]["ids"], ["77777"])
        self.assertEqual(session["remainingPosition"], 0)
        self.assertEqual(Decimal(session["finalCash"]), Decimal("999.80536"))
        both, partial, cleanup = session["concurrentObservations"]
        self.assertEqual(both["openOrders"], 2)
        self.assertEqual(Decimal(both["cashLocked"]), 1)
        self.assertEqual(partial["passiveStatus"], "ACCEPTED")
        self.assertEqual(partial["stopFilled"], 54)
        self.assertEqual(partial["remainingPosition"], 100)
        self.assertEqual(cleanup["passiveStatus"], "CANCELED")
        passive = session["passiveFeedback"]
        self.assertEqual(sum(r["type"] == "Accepted" for r in passive), 1)
        self.assertEqual(sum(r["type"] == "Canceled" for r in passive), 1)
        self.assertEqual(len({r["id"] for r in passive}), 1)
        passive_id = passive[0]["id"]
        self.assertTrue(all(r["strategyId"] == "PREDICT-PASSIVE-002" for r in passive))
        self.assertFalse(
            any(r.get("clientOrderId") == passive_id for r in session["nativeFeedback"])
        )
        fills = [
            r
            for r in session["nativeFeedback"]
            if r.get("callback") == "on_order_filled"
        ]
        self.assertEqual(len(fills), 5)
        stop = session["requests"][-1]["data"]
        self.assertEqual(stop["strategy"], "MARKET")
        self.assertEqual(stop["order"]["makerAmount"], "154000000000000000000")
        self.assert_sdk_requests(fixture, 4)

    def test_native_query_and_bulk_recover_two_strategy_orders_after_wallet_gap(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "concurrent_native_reconciliation_synthetic_20261016.json"
            ).read_text()
        )
        (session,) = fixture["sessions"]
        self.assertTrue(session["nativeInflightChecks"])
        self.assertFalse(session["customTimerQueriesOrders"])
        self.assertEqual(session["mainnetWrites"], 0)
        self.assertEqual(len(session["requests"]), 4)
        entry, maker, passive, stop = [r["data"] for r in session["requests"]]
        passive_hash = passive["order"]["hash"]
        maker_hash = maker["order"]["hash"]
        self.assertFalse(
            any(
                r["type"] == "orderAccepted" and r["orderHash"] == passive_hash
                for r in session["walletEvents"]
            )
        )
        self.assertTrue(
            any(
                r["body"].get("data", {}).get("order", {}).get("hash") == passive_hash
                for r in session["orderResponses"]
            )
        )
        self.assertTrue(
            any(
                "status" not in r["query"]
                and {passive_hash, maker_hash}.issubset(
                    {row["order"]["hash"] for row in r["response"]["data"]}
                )
                for r in session["orderLists"]
            )
        )
        self.assertEqual(
            session["concurrentObservations"][1]["phase"], "native_acceptance_and_bulk"
        )
        self.assertTrue(session["concurrentObservations"][1]["bothOwnersPreserved"])
        self.assertEqual(
            session["concurrentObservations"][-2]["passiveStatus"], "ACCEPTED"
        )
        self.assertEqual(session["removals"][-1]["data"]["ids"], ["77777"])
        self.assertEqual(len(session["removals"]), 3)
        fills = [
            r
            for r in session["nativeFeedback"]
            if r.get("callback") == "on_order_filled"
        ]
        self.assertEqual(len(fills), 5)
        self.assertEqual(
            len({(r["clientOrderId"], r["filledQuantity"]) for r in fills}), 5
        )
        self.assertEqual(
            sum(r["type"] == "Accepted" for r in session["passiveFeedback"]), 1
        )
        self.assertFalse(
            any(
                r["type"] in ("Filled", "Denied", "Rejected")
                for r in session["passiveFeedback"]
            )
        )
        self.assertEqual(entry["order"]["takerAmount"], "250000000000000000000")
        self.assertEqual(stop["strategy"], "MARKET")
        self.assertEqual(stop["order"]["makerAmount"], "154000000000000000000")
        self.assertEqual(session["remainingPosition"], 0)
        self.assertEqual(Decimal(session["finalCash"]), Decimal("999.80536"))
        self.assert_sdk_requests(fixture, 4)

    def test_partial_buy_bulk_recovery_preserves_other_strategy_position(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "concurrent_partial_buy_synthetic_20261016.json"
            ).read_text()
        )
        (session,) = fixture["sessions"]
        self.assertEqual(session["case"], "concurrent_partial_buy")
        self.assertTrue(session["nativeInflightChecks"])
        self.assertFalse(session["customTimerQueriesOrders"])
        self.assertEqual(session["mainnetWrites"], 0)
        self.assertEqual(len(session["requests"]), 4)
        self.assertEqual(len(session["removals"]), 3)
        self.assertEqual(session["removals"][-1]["data"]["ids"], ["77777"])
        partials = [
            r
            for r in session["concurrentObservations"]
            if r["phase"] == "passive_partial_buy"
        ]
        self.assertEqual(
            [(r["filled"], r["leaves"]) for r in partials], [(17, 83), (40, 60)]
        )
        self.assertEqual(
            [Decimal(r["cashLocked"]) for r in partials],
            [Decimal("0.83"), Decimal("0.60")],
        )
        self.assertTrue(partials[-1]["restOnlyIncrement"])
        passive = session["passiveFeedback"]
        fills = [r for r in passive if r["type"] == "Filled"]
        self.assertEqual(len(fills), 2)
        self.assertEqual([Decimal(r["filled"]) for r in fills], [17, 40])
        self.assertEqual([Decimal(r["leaves"]) for r in fills], [83, 60])
        self.assertEqual(sum(r["type"] == "Accepted" for r in passive), 1)
        (opened,) = [r for r in passive if r["type"] == "PositionOpened"]
        (changed,) = [r for r in passive if r["type"] == "PositionChanged"]
        self.assertEqual(Decimal(opened["quantity"]), 17)
        self.assertEqual(Decimal(changed["quantity"]), 40)
        self.assertFalse(any(r["type"] == "PositionClosed" for r in passive))
        self.assertTrue(all(r["strategyId"] == "PREDICT-PASSIVE-002" for r in passive))
        first = [
            r
            for r in session["nativeFeedback"]
            if r.get("callback") == "on_order_filled"
        ]
        self.assertEqual(len(first), 5)
        self.assertTrue(
            all(
                r.get("clientOrderId") != fills[0]["id"]
                for r in session["nativeFeedback"]
            )
        )
        self.assertEqual(
            sum(
                r.get("callback") == "on_position_closed"
                for r in session["nativeFeedback"]
            ),
            1,
        )
        stop = session["requests"][-1]["data"]
        self.assertEqual(stop["strategy"], "MARKET")
        self.assertEqual(stop["order"]["makerAmount"], "154000000000000000000")
        self.assertEqual(session["remainingPosition"], 40)
        self.assertEqual(Decimal(session["finalCash"]), Decimal("999.40536"))
        locks = {
            Decimal(b["locked"])
            for r in session["nativeFeedback"]
            if r["type"] == "NativeAccountReported"
            for b in r["balances"]
        }
        self.assertTrue({Decimal("0.83"), Decimal("0.60"), Decimal(0)}.issubset(locks))
        self.assert_sdk_requests(fixture, 4)

    def test_native_close_with_maker_and_share_fee_entry_matches_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_close_fee_cycle_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for s in fixture["sessions"]:
            share = s["case"] == "native_close_taker_share_fee_entry"
            self.assertEqual(s["mainnetWrites"], 0)
            self.assertEqual(len(s["requests"]), 3)
            self.assertEqual(len(s["removals"]), 2)
            entry, maker, stop = [r["data"] for r in s["requests"]]
            self.assertEqual(entry["isPostOnly"], not share)
            self.assertEqual(entry["order"]["feeRateBps"], "200")
            self.assertEqual(
                maker["order"]["makerAmount"], str((196 if share else 200) * 10**18)
            )
            self.assertEqual(stop["strategy"], "MARKET")
            self.assertEqual(
                stop["order"]["makerAmount"], str((150 if share else 154) * 10**18)
            )
            (requested,) = [
                r
                for r in s["nativeFeedback"]
                if r["type"] == "NativeClosePositionRequested"
            ]
            self.assertFalse(requested["reduceOnly"])
            self.assertEqual(Decimal(requested["quantity"]), 150 if share else 154)
            self.assertEqual(
                sum(
                    r.get("callback") == "on_position_closed"
                    for r in s["nativeFeedback"]
                ),
                1,
            )
            self.assertEqual(s["remainingPosition"], 0)
            self.assertEqual(
                Decimal(s["finalCash"]), Decimal("999.774" if share else "999.80536")
            )
            fills = [
                r
                for r in s["walletEvents"]
                if r["type"] == "orderTransactionSuccess"
                and r["orderHash"] == entry["order"]["hash"]
            ]
            self.assertEqual(
                sum(Decimal(r["fill"]["executedSizeWei"]) for r in fills), 200 * 10**18
            )
            if share:
                self.assertTrue(all(r["fee"]["type"] == "SHARES" for r in fills))
                self.assertEqual(
                    sum(Decimal(r["fee"]["amountWei"]) for r in fills), 4 * 10**18
                )
        self.assert_sdk_requests(fixture, 6)

    def test_native_close_default_and_share_residual_boundaries_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_close_boundaries_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 3)
        expected = {
            "native_default_reduce_only_denied": (2, 150, "998.598", "Denied"),
            "native_share_residual_below_minimum": (3, 126, "998.78616", "Rejected"),
            "native_share_residual_above_minimum": (4, 0, "999.774", None),
        }
        for session in fixture["sessions"]:
            count, remaining, cash, failure_type = expected[session["case"]]
            self.assertEqual(session["mainnetWrites"], 0)
            self.assertEqual(len(session["requests"]), count)
            self.assertEqual(len(session["removals"]), 2)
            self.assertEqual(session["remainingPosition"], remaining)
            self.assertEqual(Decimal(session["finalCash"]), Decimal(cash))
            entry, tp = [r["data"] for r in session["requests"][:2]]
            self.assertFalse(entry["isPostOnly"])
            self.assertTrue(tp["isPostOnly"])
            self.assertEqual(tp["order"]["makerAmount"], str(196 * 10**18))
            fills = [
                r
                for r in session["walletEvents"]
                if r["type"] == "orderTransactionSuccess"
                and r["orderHash"] == entry["order"]["hash"]
            ]
            self.assertEqual(
                sum(Decimal(r["fill"]["executedSizeWei"]) for r in fills), 200 * 10**18
            )
            self.assertTrue(all(r["fee"]["type"] == "SHARES" for r in fills))
            self.assertEqual(
                sum(Decimal(r["fee"]["amountWei"]) for r in fills), 4 * 10**18
            )
            feedback = session["nativeFeedback"]
            failures = [r for r in feedback if r.get("type") in {"Denied", "Rejected"}]
            self.assertEqual(len(failures), int(failure_type is not None))
            if failure_type:
                (failure,) = failures
                self.assertEqual(failure["type"], failure_type)
                order_id = failure["clientOrderId"]
                callback = (
                    "on_order_denied"
                    if failure_type == "Denied"
                    else "on_order_rejected"
                )
                self.assertEqual(
                    sum(
                        r.get("callback") == callback
                        and r.get("clientOrderId") == order_id
                        for r in feedback
                    ),
                    1,
                )
                self.assertFalse(
                    any(
                        r.get("callback") == "on_order_accepted"
                        and r.get("clientOrderId") == order_id
                        for r in feedback
                    )
                )
                self.assertEqual(
                    sum(
                        r.get("callback") == "on_order_submitted"
                        and r.get("clientOrderId") == order_id
                        for r in feedback
                    ),
                    int(failure_type == "Rejected"),
                )
                self.assertIn(
                    "reduce-only" if failure_type == "Denied" else "slippage floor",
                    failure["reason"],
                )
            self.assertEqual(
                sum(r.get("callback") == "on_position_closed" for r in feedback),
                int(remaining == 0),
            )
            close = [
                r for r in feedback if r.get("type") == "NativeClosePositionRequested"
            ]
            self.assertEqual(len(close), 1 if failure_type == "Denied" else 2)
            self.assertEqual(Decimal(close[0]["quantity"]), 150)
            if failure_type == "Denied":
                self.assertIsNone(close[0]["reduceOnly"])
            else:
                self.assertTrue(all(r["reduceOnly"] is False for r in close))
                residual = 126 if remaining else 127
                self.assertEqual(Decimal(close[1]["quantity"]), residual)
                if not remaining:
                    self.assertEqual(
                        session["requests"][-1]["data"]["order"]["makerAmount"],
                        str(127 * 10**18),
                    )
            self.assertTrue(
                any(
                    r.get("type") in {"NativeAccountReported", "NativeAccountSnapshot"}
                    and any(
                        b["currency"] == "USDT"
                        and Decimal(b["total"]) == Decimal(cash)
                        and Decimal(b["locked"]) == 0
                        for b in r.get("balances", [])
                    )
                    for r in feedback
                )
            )
        self.assert_sdk_requests(fixture, 9)

    def test_sdk_sell_slippage_straddles_existing_one_usdt_admission_floor(self):
        builder = OrderBuilder(
            ChainId.BNB_MAINNET,
            18,
            ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
            lambda: "1",
            Logger("ERROR"),
            Account.from_key(PUBLIC_KEY),
            "0x1111111111111111111111111111111111111111",
        )
        book = Book(
            market_id=42,
            update_timestamp_ms=1,
            asks=[["0.014", "1000"]],
            bids=[["0.008", "1000"]],
        )
        for quantity, floor in [(126, "0.99792"), (127, "1.00584")]:
            with self.subTest(quantity=quantity):
                amounts = builder.get_market_order_amounts(
                    MarketHelperInput(
                        Side.SELL, quantity_wei=quantity * 10**18, slippage_bps=100
                    ),
                    book,
                )
                self.assertEqual(amounts.maker_amount, quantity * 10**18)
                self.assertEqual(Decimal(amounts.taker_amount) / 10**18, Decimal(floor))
                self.assertGreater(Decimal(quantity) * Decimal("0.008"), 1)
                self.assertEqual(amounts.taker_amount >= 10**18, quantity == 127)
                if quantity == 127:
                    # Minimum signed proceeds and fee-net cash are different boundaries.
                    self.assertLess(
                        Decimal(quantity) * Decimal("0.008") * Decimal("0.98"), 1
                    )

    def test_adapter_native_quotes_drive_exit_and_sdk_requests(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_quotes_shared_exit_synthetic_20261016.json"
            ).read_text()
        )
        (session,) = fixture["sessions"]
        self.assertEqual(session["case"], "adapter_native_quotes_share_fee_exit")
        self.assertEqual(session["mainnetWrites"], 0)
        self.assertEqual(session["remainingPosition"], 0)
        self.assertEqual(Decimal(session["finalCash"]), Decimal("999.774"))
        quotes = [
            r
            for r in session["nativeFeedback"]
            if r.get("type") == "NativeQuoteObserved"
        ]
        self.assertTrue(quotes)
        self.assertEqual(quotes, session["passiveFeedback"])
        keys = [
            (q["tsEvent"], q["bid"], q["ask"], q["bidSize"], q["askSize"])
            for q in quotes
        ]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertTrue(any(q["bid"] == "0.008" for q in quotes))
        self.assertEqual(len(session["removals"]), 2)
        self.assert_sdk_requests(fixture, 3)

    def test_early_cancel_native_inflight_and_signed_requests_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "early_cancel_native_feedback_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 5)
        for session in fixture["sessions"]:
            mode = session["mode"]
            self.assertEqual(session["finalOpen"], 0)
            self.assertEqual(session["finalInflight"], 0)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            feedback = session["feedback"]
            self.assertTrue(any(r.get("callback") == "account_state" for r in feedback))
            self.assertFalse(any(r.get("type") == "CancelRejected" for r in feedback))
            self.assertEqual(sum(r.get("type") == "PendingCancel" for r in feedback), 1)
            partial = mode == "query_partial"
            filled = mode == "query_filled"
            self.assertEqual(
                session["filledQuantity"], 33 if partial else 100 if filled else 0
            )
            self.assertEqual(session["finalPositions"], int(partial or filled))
            self.assertEqual(
                Decimal(session["finalCash"]),
                Decimal("999.67")
                if partial
                else Decimal(999)
                if filled
                else Decimal(1000),
            )
            self.assertEqual(
                len(session["removalResponses"]),
                int(mode not in {"rejected", "query_filled"}),
            )
            for reply in session["removalResponses"]:
                self.assertEqual(reply["request"], {"data": {"ids": ["12345"]}})
                self.assertEqual(reply["body"]["removed"], ["12345"])
            if partial or filled:
                self.assertEqual(len(session["matches"]), 2)
                self.assertEqual(session["matches"][0], session["matches"][1])
                self.assertEqual(sum(r.get("type") == "Filled" for r in feedback), 1)
        self.assert_sdk_requests(fixture, 5)

    def test_unmodified_native_exec_tester_requests_and_shutdown_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "native_exec_tester_profile_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 8)
        for session in fixture["sessions"]:
            case = session["case"]
            cfg = session["testerConfig"]
            self.assertFalse(session["nativeRiskBypassed"])
            self.assertTrue(session["nativeReconciliation"])
            self.assertIsNone(session["reconciliationLookbackMins"])
            self.assertTrue(cfg["subscribe_quotes"])
            self.assertFalse(cfg["subscribe_trades"])
            self.assertFalse(cfg["enable_limit_sells"])
            self.assertTrue(cfg["cancel_orders_on_stop"])
            self.assertFalse(cfg["close_positions_on_stop"])
            self.assertEqual(session["stopGraceSecs"], 1)
            self.assertEqual(session["finalOpenOrders"], 0)
            self.assertEqual(session["finalInflightOrders"], 0)
            self.assertEqual(session["finalPositions"], 0)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            self.assertTrue(session["quotes"])
            self.assertTrue(session["finalAccountSnapshot"])
            if case not in {"dry_run", "risk_denied"}:
                self.assertTrue(session["accountFeedback"])
            events = session["feedback"]
            self.assertFalse(
                any(e["type"] in {"Filled", "CancelRejected"} for e in events)
            )
            if case in {"dry_run", "risk_denied"}:
                self.assertFalse(session["requests"])
                self.assertFalse(session["removalResponses"])
            if case == "risk_denied":
                denied = [e for e in events if e["type"] == "Denied"]
                self.assertEqual(len(denied), 1)
                self.assertTrue(
                    denied[0]["reason"].startswith("NOTIONAL_EXCEEDS_FREE_BALANCE")
                )
            elif case == "modify_rejected":
                rejected = [e for e in events if e["type"] == "ModifyRejected"]
                self.assertEqual(len(rejected), 1)
                self.assertIn(
                    "atomic order modification is unsupported", rejected[0]["reason"]
                )
            elif case == "venue_rejected":
                self.assertEqual(sum(e["type"] == "Rejected" for e in events), 1)
                self.assertFalse(session["removalResponses"])
            elif case == "cancel_replace":
                first, second = session["requests"]
                self.assertEqual(first["data"]["pricePerShare"], "10000000000000000")
                self.assertEqual(second["data"]["pricePerShare"], "11000000000000000")
                self.assertNotEqual(
                    first["data"]["order"]["hash"], second["data"]["order"]["hash"]
                )
                self.assertEqual(sum(e["type"] == "Canceled" for e in events), 2)
        self.assert_sdk_requests(fixture, 7)

    def test_thin_product_host_discovery_authentication_and_native_orders_match_sdk(
        self,
    ):
        fixture = json.loads(
            FIXTURE.with_name(
                "thin_node_native_bootstrap_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for session in fixture["sessions"]:
            writes = session["writesEnabledInFixture"]
            self.assertFalse(session["nativeRiskBypassed"])
            self.assertEqual(session["discoveredMarket"], 42)
            self.assertFalse(session["hostPreloadedInstruments"])
            self.assertTrue(session["nativeBootstrapLoadedBothInstruments"])
            self.assertEqual(session["finalOpen"], 0)
            self.assertEqual(session["finalInflight"], 0)
            self.assertEqual(session["finalPositions"], 0)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            self.assertEqual(Decimal(session["finalCash"]), 1000)
            self.assertEqual(
                {q["instrument"] for q in session["quotes"]},
                {"42-1.PREDICT", "42-2.PREDICT"},
            )
            self.assertTrue(any(r["kind"] == "category" for r in session["trace"]))
            self.assertEqual(
                sum(r["kind"] == "authenticate" for r in session["trace"]), 1
            )
            self.assertEqual(len(session["requests"]), int(writes))
            self.assertEqual(len(session["removalResponses"]), int(writes))
            self.assertEqual(
                sum(e["type"] == "Canceled" for e in session["feedback"]), int(writes)
            )
        self.assert_sdk_requests(fixture, 1)
        cli = json.loads(
            FIXTURE.with_name(
                "thin_node_native_cli_synthetic_20261016.json"
            ).read_text()
        )
        self.assertTrue(cli["environmentIsolated"])
        self.assertTrue(cli["nativeSigintShutdown"])
        self.assertTrue(cli["exitSuccess"])
        self.assertEqual(cli["orderPosts"], 0)
        self.assertEqual(cli["removalPosts"], 0)
        self.assertTrue(cli["startupOrderReports"])
        self.assertEqual(cli["quotesObserved"], ["42-1.PREDICT", "42-2.PREDICT"])

    def test_custom_snapshot_strategy_host_request_matches_sdk(self):
        session = json.loads(
            FIXTURE.with_name(
                "snapshot_strategy_account_feedback_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(session["signalInput"], "custom_snapshot")
        self.assertTrue(session["canceledBeforeNodeStop"])
        self.assertFalse(session["nativeRiskBypassed"])
        callbacks = [r["callback"] for r in session["strategyCallbacks"]]
        self.assertEqual(callbacks.count("on_data"), 2)
        self.assertEqual(callbacks.count("submit_order"), 1)
        self.assertEqual(callbacks.count("on_order_accepted"), 1)
        self.assertEqual(callbacks.count("on_order_canceled"), 1)
        self.assertLess(
            callbacks.index("on_order_accepted"), callbacks.index("on_order_canceled")
        )
        self.assertEqual(len(session["removalResponses"]), 1)
        self.assertEqual(session["finalOpen"], 0)
        self.assertEqual(session["finalInflight"], 0)
        self.assertEqual(session["finalPositions"], 0)
        self.assertEqual(Decimal(session["finalLocked"]), 0)
        self.assertEqual(Decimal(session["finalCash"]), 1000)
        self.assertTrue(
            any(
                Decimal(b["locked"]) == 1
                for e in session["accountFeedback"]
                for b in e["balances"]
                if b["currency"] == "USDT"
            )
        )
        self.assert_sdk_requests({"sessions": [session]}, 1)

    def test_custom_snapshot_modify_rejection_and_cancel_replace_match_sdk(self):
        session = json.loads(
            FIXTURE.with_name(
                "snapshot_modify_cancel_replace_synthetic_20261016.json"
            ).read_text()
        )
        rows = session["strategyCallbacks"]
        callbacks = [r["callback"] for r in rows]
        self.assertEqual(callbacks.count("on_data"), 4)
        self.assertEqual(callbacks.count("submit_order"), 2)
        self.assertEqual(callbacks.count("modify_order"), 1)
        self.assertEqual(callbacks.count("on_order_modify_rejected"), 1)
        self.assertEqual(callbacks.count("on_order_accepted"), 2)
        self.assertEqual(callbacks.count("on_order_canceled"), 2)
        self.assertEqual(callbacks.count("query_account"), 1)
        rejected = next(r for r in rows if r["callback"] == "on_order_modify_rejected")
        self.assertIn("atomic order modification is unsupported", rejected["reason"])
        self.assertEqual(Decimal(rejected["cachedPrice"]), Decimal("0.010"))
        self.assertEqual(Decimal(rejected["cachedQuantity"]), 100)
        self.assertEqual(rejected["cachedStatus"], "Accepted")
        deferred = next(r for r in rows if r["callback"] == "ReplacementDeferred")
        ready = next(r for r in rows if r["callback"] == "ReplacementAccountReady")
        self.assertEqual(Decimal(deferred["account"]["locked"]), 1)
        self.assertEqual(Decimal(ready["account"]["locked"]), 0)
        self.assertGreater(ready["account"]["block"], deferred["account"]["block"])
        self.assertLess(
            callbacks.index("on_order_modify_rejected"),
            callbacks.index("on_order_canceled"),
        )
        self.assertLess(
            callbacks.index("query_account"), callbacks.index("ReplacementDeferred")
        )
        self.assertLess(
            callbacks.index("ReplacementDeferred"),
            callbacks.index("ReplacementAccountReady"),
        )
        self.assertTrue(session["accountRefreshHeldBeforeReplacement"])
        self.assertTrue(session["canceledBeforeNodeStop"])
        self.assertFalse(session["nativeRiskBypassed"])
        self.assertEqual(len(session["removalResponses"]), 2)
        hashes = [r["data"]["order"]["hash"] for r in session["requests"]]
        self.assertEqual(len(set(hashes)), 2)
        self.assertEqual(session["wallet"][0], session["wallet"][1])
        self.assertEqual(session["wallet"][0]["orderHash"], hashes[0])
        self.assertEqual(session["wallet"][2]["orderHash"], hashes[1])
        for field in ["finalOpen", "finalInflight", "finalPositions"]:
            self.assertEqual(session[field], 0)
        self.assertEqual(Decimal(session["finalLocked"]), 0)
        self.assertEqual(Decimal(session["finalCash"]), 1000)
        self.assertEqual(
            session["nativeAccountAtEnd"]["block"], ready["account"]["block"]
        )
        self.assert_sdk_requests({"sessions": [session]}, 2)

    def test_multi_active_order_reservations_and_native_denial_match_sdk(self):
        fixture = json.loads(
            FIXTURE.with_name(
                "snapshot_multi_native_reservations_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        for session in fixture["sessions"]:
            denied = session["case"] == "native_insufficient_free_balance"
            self.assertFalse(session["nativeRiskBypassed"])
            self.assertEqual(session["signalInput"], "custom_snapshot")
            expected = 2 if denied else 3
            self.assertEqual(len(session["requests"]), expected)
            self.assertEqual(len(session["removalResponses"]), expected)
            checkpoints = {r["phase"]: r["account"] for r in session["checkpoints"]}
            self.assertEqual(Decimal(checkpoints["two_reserved"]["locked"]), 2)
            before = checkpoints["before_replacement"]
            self.assertEqual(Decimal(before["locked"]), 1)
            self.assertEqual(Decimal(before["free"]), 1 if denied else 2)
            self.assertEqual(before["open"], 1)
            rows = session["targetCallbacks"]
            callbacks = [r["callback"] for r in rows]
            self.assertEqual(callbacks.count("submit_order"), 2)
            self.assertEqual(callbacks.count("on_order_accepted"), 1 if denied else 2)
            self.assertEqual(callbacks.count("on_order_denied"), int(denied))
            self.assertEqual(callbacks.count("on_order_canceled"), 1 if denied else 2)
            self.assertTrue(
                any(
                    r["callback"] == "AccountAtAccepted" and r["account"]["open"] == 2
                    for r in rows
                )
            )
            ready = next(r for r in rows if r["callback"] == "ReplacementAccountReady")
            self.assertEqual(Decimal(ready["account"]["locked"]), 1)
            self.assertEqual(Decimal(ready["account"]["free"]), 1 if denied else 2)
            if denied:
                rejection = next(r for r in rows if r["callback"] == "on_order_denied")
                self.assertIn("FREE_BALANCE", rejection["reason"])
                self.assertEqual(
                    Decimal(checkpoints["replacement_denied"]["locked"]), 1
                )
            else:
                self.assertEqual(
                    Decimal(checkpoints["replacement_reserved"]["locked"]),
                    Decimal("2.1"),
                )
                self.assertEqual(
                    Decimal(checkpoints["replacement_reserved"]["free"]), Decimal("0.9")
                )
            self.assertEqual(Decimal(checkpoints["companion_preserved"]["locked"]), 1)
            self.assertEqual(checkpoints["companion_preserved"]["open"], 1)
            other = [r["callback"] for r in session["companionCallbacks"]]
            self.assertEqual(other.count("submit_order"), 1)
            self.assertEqual(other.count("on_order_accepted"), 1)
            self.assertEqual(other.count("on_order_canceled"), 1)
            self.assertEqual(session["wallet"][1], session["wallet"][2])
            removed = [r["request"]["data"]["ids"] for r in session["removalResponses"]]
            self.assertEqual(removed[0], [session["wallet"][1]["orderId"]])
            self.assertEqual(removed[-1], [session["wallet"][0]["orderId"]])
            self.assertTrue(session["bothStrategiesCanceledBeforeStop"])
            for field in ["finalOpen", "finalInflight", "finalPositions"]:
                self.assertEqual(session[field], 0)
            self.assertEqual(Decimal(session["finalLocked"]), 0)
            self.assertEqual(session["finalCash"], session["initialCash"])
        self.assert_sdk_requests(fixture, 5)

    def test_private_fill_evidence_and_native_livenode_preserve_fee_and_quantity(self):
        facts = json.loads(
            FIXTURE.with_name(
                "private_fill_evidence_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(facts["nativeReports"]), 2)
        self.assertEqual(len(facts["privateEvidence"]), 2)
        self.assertTrue(all("info" not in r for r in facts["nativeReports"]))
        self.assertEqual(
            sum(int(r["grossSharesWei"]) for r in facts["privateEvidence"]),
            100 * 10**18,
        )
        self.assertEqual(
            sum(Decimal(r["last_qty"]) for r in facts["nativeReports"]), 100
        )
        self.assertEqual(
            [Decimal(r["last_qty"]) for r in facts["nativeReports"]], [17, 83]
        )
        self.assertEqual(
            sum(Decimal(r["commission"].split()[0]) for r in facts["nativeReports"]),
            Decimal("0.26"),
        )
        self.assertTrue(
            all(r["feeAsset"] == "SHARES" for r in facts["privateEvidence"])
        )
        self.assertTrue(facts["duplicateReplayAllocationsUnchanged"])
        fixture = json.loads(
            FIXTURE.with_name(
                "private_fill_native_livenode_synthetic_20261016.json"
            ).read_text()
        )
        session = fixture["sessions"][0]
        self.assertEqual(Decimal(session["remainingPosition"]), 0)
        self.assertEqual(Decimal(session["finalCash"]), Decimal("999.774"))
        feedback = session["nativeFeedback"]
        take_profit = next(
            r for r in feedback if r.get("type") == "MakerTakeProfitSubmitted"
        )
        stop = next(r for r in feedback if r.get("type") == "TakerStopSubmitted")
        self.assertEqual(Decimal(take_profit["quantity"]), 196)
        self.assertEqual(Decimal(stop["quantity"]), 150)
        self.assertTrue(any(r.get("callback") == "on_order_filled" for r in feedback))
        self.assertTrue(
            any(r.get("callback") == "on_position_closed" for r in feedback)
        )
        self.assert_sdk_requests(fixture, 3)

    def assert_sdk_requests(self, fixture, expected_count):
        builder = OrderBuilder(
            ChainId.BNB_MAINNET,
            18,
            ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
            lambda: "1",
            Logger("ERROR"),
            Account.from_key(PUBLIC_KEY),
            "0x1111111111111111111111111111111111111111",
        )
        checked = 0
        for session in fixture["sessions"]:
            for request in session["requests"]:
                data = request["data"]
                wire = data["order"]
                side = Side(wire["side"])
                quantity = int(
                    wire["makerAmount"] if side == Side.SELL else wire["takerAmount"]
                )
                if data["strategy"] == "LIMIT":
                    amounts = builder.get_limit_order_amounts(
                        LimitHelperInput(side, int(data["pricePerShare"]), quantity)
                    )
                    expected_post_only = not (
                        session.get("case")
                        in {
                            "native_close_taker_share_fee_entry",
                            "adapter_native_quotes_share_fee_exit",
                            "native_default_reduce_only_denied",
                            "native_share_residual_below_minimum",
                            "native_share_residual_above_minimum",
                        }
                        and side == Side.BUY
                    )
                    if side == Side.BUY and "entryPostOnly" in session:
                        expected_post_only = session["entryPostOnly"]
                    self.assertEqual(data["isPostOnly"], expected_post_only)
                else:
                    book = session["orderbooks"][-1]
                    amounts = builder.get_market_order_amounts(
                        MarketHelperInput(
                            Side.SELL,
                            quantity_wei=quantity,
                            slippage_bps=data["slippageBps"],
                        ),
                        Book(
                            market_id=book["marketId"],
                            update_timestamp_ms=book["updateTimestampMs"],
                            asks=book["asks"],
                            bids=book["bids"],
                        ),
                    )
                    self.assertNotIn("isPostOnly", data)
                    self.assertNotIn("isMinAmountOut", data)
                self.assertEqual(str(amounts.maker_amount), wire["makerAmount"])
                self.assertEqual(str(amounts.taker_amount), wire["takerAmount"])
                self.assertEqual(str(amounts.price_per_share), data["pricePerShare"])
                with patch(
                    "predict_sdk.order_builder.time.time",
                    return_value=int(wire["expiration"]) - 300,
                ):
                    order = builder.build_order(
                        data["strategy"],
                        BuildOrderInput(
                            side=side,
                            token_id=wire["tokenId"],
                            maker_amount=amounts.maker_amount,
                            taker_amount=amounts.taker_amount,
                            fee_rate_bps=int(wire["feeRateBps"]),
                            nonce=int(wire["nonce"]),
                            salt=wire["salt"],
                            expires_at=(
                                datetime.fromtimestamp(int(wire["expiration"]), UTC)
                                if data["strategy"] == "LIMIT"
                                else None
                            ),
                        ),
                    )
                typed = builder.build_typed_data(
                    order,
                    is_neg_risk=session.get("marketFlags", {}).get("isNegRisk", False),
                    is_yield_bearing=session.get("marketFlags", {}).get(
                        "isYieldBearing", False
                    ),
                )
                signed = builder.sign_typed_data_order(typed)
                self.assertEqual(builder.build_typed_data_hash(typed), wire["hash"])
                self.assertEqual(
                    "0x" + signed.signature.removeprefix("0x").lower(),
                    wire["signature"],
                )
                self.assertEqual(int(signed.expiration), wire["expiration"])
                checked += 1
        self.assertEqual(checked, expected_count)

    def test_minimum_and_fee_adjusted_inventory_are_not_silently_resized(self):
        sessions = self.fixture["sessions"]
        self.assertEqual(len(sessions), 5)
        for session in sessions:
            self.assertEqual(
                session["requests"][0]["data"]["order"]["makerAmount"],
                str(196 * 10**18),
            )
            if session["subminimum"]:
                self.assertEqual(len(session["requests"]), 1)
                self.assertEqual(Decimal(session["finalPositions"][0]), Decimal(150))
                self.assertTrue(
                    any(
                        "venue minimum of 1 USDT" in (event.get("reason") or "")
                        for event in session["nativeFeedback"]
                    )
                )
            if len(session["requests"]) == 2:
                self.assertEqual(
                    session["requests"][1]["data"]["order"]["makerAmount"],
                    str(150 * 10**18),
                )
                self.assertEqual(Decimal(session["finalCash"]), Decimal("999.774"))
                self.assertEqual(session["finalPositions"], [])

    def test_cancellation_barrier_and_failed_cancel_remain_observable(self):
        for session in self.fixture["sessions"]:
            events = [event["type"] for event in session["nativeFeedback"]]
            self.assertIn("NativeAccountReported", events)
            self.assertIn("NativePositionApplied", events)
            if session["cancelRejected"]:
                self.assertIn("CancelRejected", events)
                self.assertNotIn("TakerStopSubmitted", events)
                self.assertEqual(len(session["requests"]), 1)
            elif not session["profit"] and not session["fullFillDuringCancel"]:
                self.assertLess(
                    events.index("Canceled"), events.index("TakerStopSubmitted")
                )
            if session["fullFillDuringCancel"]:
                self.assertNotIn("TakerStopSubmitted", events)
                self.assertEqual(session["finalPositions"], [])
