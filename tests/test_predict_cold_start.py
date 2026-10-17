"""Actual signed-order evidence and derived Native startup contracts. LGPL-3.0-or-later."""

import json
import unittest
from pathlib import Path

from capture_predict_startup_readonly import OWN, ZERO, collect, contract_hash, sanitize
from predict_sdk import ChainId, OrderBuilder

FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


def load(name):
    return json.loads((FIXTURES / name).read_text())


class ColdStartTests(unittest.TestCase):
    def test_real_read_only_order_fields_and_explicit_evidence_limits(self):
        actual = load("cold_start_readonly_20261016T133220Z.json")
        self.assertEqual(actual["sdkOrderHashesVerified"], 6)
        self.assertEqual(len(actual["ordersByHash"]), 6)
        self.assertEqual(actual["currentOrders"], [])
        self.assertEqual(actual["currentPositions"], [])
        self.assertEqual(actual["orderWrites"], 0)
        self.assertTrue(all(r["method"] == "GET" for r in actual["requests"]))
        self.assertTrue(
            all(r["order"]["taker"] == ZERO for r in actual["ordersByHash"])
        )
        self.assertEqual(
            {r["status"] for r in actual["ordersByHash"]}, {"FILLED", "CANCELLED"}
        )
        self.assertTrue(
            all(
                r["order"]["signature"] == "redacted-signature"
                for r in actual["ordersByHash"]
            )
        )

    def test_derived_order_hashes_match_official_sdk_and_native_ownership_is_idempotent(
        self,
    ):
        sdk = OrderBuilder.make(ChainId.BNB_MAINNET)
        fixture = load("cold_start_native_actual_derived_20261016.json")
        self.assertEqual(len(fixture["sessions"]), 3)
        for session in fixture["sessions"]:
            for row in (session["buy"], session["takeProfit"]):
                self.assertEqual(contract_hash(sdk, row), row["order"]["hash"])
            observed = session["nativeFeedback"][0]
            self.assertEqual(observed["netPositions"], ["31.3600000000000000"])
            self.assertEqual(observed["cash"], "47.01772920 USDT")
            expected = (
                []
                if session["holdingOnly"]
                else ["COLD-001" if session["claimed"] else "EXTERNAL"]
            )
            self.assertEqual(observed["owners"], expected)
            self.assertEqual(session["repeated"]["repeatedEvents"], 0)
            self.assertEqual(session["orderWrites"], 0)
            for key in (
                "initialCacheOrders",
                "initialCachePositions",
                "initialCacheInstruments",
            ):
                self.assertEqual(session[key], 0)

    def test_collector_has_only_get_requests_and_checks_sdk_hash_before_redaction(self):
        session = load("cold_start_native_actual_derived_20261016.json")["sessions"][0]
        buy = session["buy"]
        identity = buy["order"]["maker"]
        requests = []

        def request(method, path, **kwargs):
            requests.append((method, path))
            self.assertEqual(method, "GET")
            if path == "/v1/orders":
                return {"success": True, "data": [], "cursor": None}
            if path == "/v1/positions":
                return {"success": True, "data": [], "cursor": None}
            if path == "/v1/orders/matches":
                return {
                    "success": True,
                    "data": [session["confirmedMatch"]],
                    "cursor": None,
                }
            if path.startswith("/v1/orders/"):
                return {"success": True, "data": buy}
            if path.startswith("/v1/markets/"):
                return {"success": True, "data": session["confirmedMatch"]["market"]}
            self.fail("unexpected route")

        report = collect(request, identity)
        self.assertEqual(report["sdkOrderHashesVerified"], 1)
        cleaned = sanitize(report, identity)
        self.assertEqual(cleaned["ordersByHash"][0]["order"]["maker"], OWN)
        self.assertEqual(cleaned["ordersByHash"][0]["order"]["taker"], ZERO)
        self.assertEqual(len(requests), 5)

    def test_collector_rejects_signed_terms_changed_under_an_existing_hash(self):
        session = load("cold_start_native_actual_derived_20261016.json")["sessions"][0]
        buy = session["buy"]
        identity = buy["order"]["maker"]
        buy["order"]["nonce"] = "99"

        def request(method, path, **kwargs):
            self.assertEqual(method, "GET")
            if path == "/v1/orders":
                return {"success": True, "data": [buy], "cursor": None}
            if path in {"/v1/positions", "/v1/orders/matches"}:
                return {"success": True, "data": [], "cursor": None}
            return {"success": True, "data": buy}

        with self.assertRaisesRegex(RuntimeError, "SDK order hash mismatch"):
            collect(request, identity)
