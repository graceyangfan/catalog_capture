"""Offline guards for mainnet calibration cleanup. LGPL-3.0-or-later."""

import copy
import unittest
from unittest.mock import patch

from capture_predict_private_fixtures import CaptureError
from predict_resting_evidence import EvidenceError, validate_evidence, analyze_latency
from run_predict_resting_live import (
    archive_run,
    final_cleanup,
    is_calibration_order,
    run,
)

IDENTITY = "0x0000000000000000000000000000000000000022"


def order():
    return {
        "id": "123",
        "order": {
            "maker": IDENTITY,
            "signer": IDENTITY,
            "side": 0,
            "makerAmount": str(10**18),
            "takerAmount": str(100 * 10**18),
            "hash": "0x" + "ab" * 32,
        },
    }


class CleanupTests(unittest.TestCase):
    def test_latency_requires_gtd_before_authentication(self):
        with patch("run_predict_resting_live.authenticated_reader") as authenticate:
            with self.assertRaises(CaptureError):
                run("yes-gtc", "native-quantity-commission", True)
            authenticate.assert_not_called()

    def test_unknown_case_cannot_authenticate_or_write(self):
        with patch("run_predict_resting_live.authenticated_reader") as authenticate:
            with self.assertRaises(CaptureError):
                run("market-fok")
            authenticate.assert_not_called()

    def test_unknown_features_cannot_authenticate_or_write(self):
        with patch("run_predict_resting_live.authenticated_reader") as authenticate:
            with self.assertRaises(CaptureError):
                run("yes-gtd", "unknown-feature")
            authenticate.assert_not_called()

    def test_archive_preserves_logs_and_refuses_to_replace_an_existing_run(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            root = Path(directory)
            staging, archive = root / "staging", root / "archive"
            staging.mkdir()
            (staging / "result.json").write_bytes(b'{"fills":0}\n')
            with (
                patch("run_predict_resting_live.ARCHIVE", archive),
                patch("run_predict_resting_live.datetime") as clock,
            ):
                clock.now.return_value.strftime.return_value = "fixed"
                destination = archive_run(staging, "yes-gtd")
                self.assertEqual(
                    (destination / "result.json").read_bytes(), b'{"fills":0}\n'
                )
                self.assertFalse(staging.exists())
                staging.mkdir()
                (staging / "result.json").write_bytes(b'{"fills":1}\n')
                with self.assertRaises(CaptureError):
                    archive_run(staging, "yes-gtd")
                self.assertEqual(
                    (staging / "result.json").read_bytes(), b'{"fills":1}\n'
                )
                self.assertEqual(
                    (destination / "result.json").read_bytes(), b'{"fills":0}\n'
                )

    def test_empty_account_does_not_write(self):
        calls = []

        def request(method, path, **kwargs):
            calls.append(method)
            return {"success": True, "data": [], "cursor": None}

        self.assertEqual(
            final_cleanup(request, IDENTITY, {order()["order"]["hash"]})[
                "verifiedOpenOrders"
            ],
            0,
        )
        self.assertEqual(calls, ["GET"])

    def test_only_verified_order_is_removed_then_rechecked(self):
        calls, active = [], [order()]

        def request(method, path, **kwargs):
            calls.append((method, path))
            if method == "POST":
                self.assertEqual(kwargs["json"], {"data": {"ids": ["123"]}})
                active.clear()
                return {"success": True}
            if path.endswith(order()["order"]["hash"]):
                return {"data": order()}
            return {"success": True, "data": active.copy(), "cursor": None}

        with patch("run_predict_resting_live.time.sleep"):
            report = final_cleanup(request, IDENTITY, {order()["order"]["hash"]})
        self.assertEqual(
            report,
            {
                "verifiedOpenOrders": 0,
                "verifiedRunOpenOrders": 0,
                "unrelatedOpenOrders": 0,
                "fallbackRemovedIds": ["123"],
            },
        )
        self.assertEqual([method for method, _ in calls], ["GET", "GET", "POST", "GET"])

    def test_foreign_terms_never_removed(self):
        row = order()
        row["order"]["makerAmount"] = str(2 * 10**18)

        def request(method, path, **kwargs):
            self.assertEqual(method, "GET")
            return {"success": True, "data": [row], "cursor": None}

        with self.assertRaises(CaptureError):
            final_cleanup(request, IDENTITY, {order()["order"]["hash"]})

    def test_changed_identity_on_verification_never_removed(self):
        def request(method, path, **kwargs):
            self.assertEqual(method, "GET")
            if path == "/v1/orders":
                return {"success": True, "data": [order()], "cursor": None}
            row = order()
            row["id"] = "456"
            return {"data": row}

        with self.assertRaises(CaptureError):
            final_cleanup(request, IDENTITY, {order()["order"]["hash"]})

    def test_identical_foreign_terms_are_preserved_while_run_order_is_removed(self):
        own, foreign = order(), order()
        foreign["id"] = "999"
        foreign["order"]["hash"] = "0x" + "cd" * 32
        active = [foreign, own]
        removed = []

        def request(method, path, **kwargs):
            if method == "POST":
                ids = kwargs["json"]["data"]["ids"]
                removed.extend(ids)
                active[:] = [row for row in active if row["id"] not in ids]
                return {"success": True}
            if path == "/v1/orders":
                return {"success": True, "data": active.copy(), "cursor": None}
            self.assertEqual(path, "/v1/orders/" + own["order"]["hash"])
            return {"data": own}

        with patch("run_predict_resting_live.time.sleep"):
            result = final_cleanup(request, IDENTITY, {own["order"]["hash"]})
        self.assertEqual(removed, ["123"])
        self.assertEqual(active, [foreign])
        self.assertEqual(result["verifiedRunOpenOrders"], 0)
        self.assertEqual(result["unrelatedOpenOrders"], 1)

    def test_empty_run_journal_never_grants_removal_authority(self):
        def request(method, path, **kwargs):
            self.assertEqual(method, "GET")
            return {"success": True, "data": [order()], "cursor": None}

        result = final_cleanup(request, IDENTITY, set())
        self.assertEqual(result["unrelatedOpenOrders"], 1)
        self.assertEqual(result["fallbackRemovedIds"], [])

    def test_changed_hash_with_same_id_and_terms_never_removed(self):
        def request(method, path, **kwargs):
            self.assertEqual(method, "GET")
            if path == "/v1/orders":
                return {"success": True, "data": [order()], "cursor": None}
            row = order()
            row["order"]["hash"] = "0x" + "cd" * 32
            return {"data": row}

        with self.assertRaises(CaptureError):
            final_cleanup(request, IDENTITY, {order()["order"]["hash"]})

    def test_replace_terms_keep_one_usdt_budget_and_reject_foreign_signer(self):
        row = order()
        row["order"]["takerAmount"] = str(50 * 10**18)
        self.assertTrue(is_calibration_order(row, IDENTITY))
        foreign = copy.deepcopy(row)
        foreign["order"]["signer"] = "0x" + "33" * 20
        self.assertFalse(is_calibration_order(foreign, IDENTITY))


class RetainedLiveEvidenceTests(unittest.TestCase):
    def test_retained_four_case_mainnet_matrix_and_gtd_precision(self):
        import hashlib
        import json
        from pathlib import Path
        from tempfile import TemporaryDirectory

        base = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        matrix = json.loads((base / "maker_live_matrix_20261015.json").read_text())
        self.assertEqual(
            {row["case"] for row in matrix["cases"]},
            {"yes-gtc", "no-gtc", "yes-gtd", "no-gtd"},
        )
        for entry in matrix["files"]:
            self.assertEqual(
                hashlib.sha256((base / entry["path"]).read_bytes()).hexdigest(),
                entry["sha256"],
            )
        for case in matrix["cases"]:
            with self.subTest(case=case["case"]), TemporaryDirectory() as directory:
                output = Path(directory)
                for name in (
                    "result.json",
                    "calibration.json",
                    "http_responses.json",
                    "native_feedback.json",
                ):
                    (output / name).write_bytes(
                        (base / case["directory"] / name).read_bytes()
                    )
                self.assertEqual(validate_evidence(output)["finalOpenOrders"], 0)
                if case["case"].endswith("gtd"):
                    feedback = json.loads((output / "native_feedback.json").read_text())
                    event = next(row for row in feedback if row["type"] == "Accepted")
                    event["cachedOrder"]["expireTimeNs"] += 1
                    (output / "native_feedback.json").write_text(json.dumps(feedback))
                    with self.assertRaises(EvidenceError):
                        validate_evidence(output)

    def test_actual_expiry_http_rejection_did_not_create_order_or_consume_cash(self):
        import json
        from pathlib import Path

        base = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        rejection = json.loads(
            (base / "order_create_expiry_error_20261015.json").read_text()
        )
        output = base / rejection["evidenceDirectory"]
        self.assertEqual(rejection["response"]["status"], 400)
        self.assertEqual(
            rejection["response"]["errorBody"]["error"], "create_order_expiry_too_soon"
        )
        report = json.loads((output / "result.json").read_text())
        self.assertEqual(
            [report[key] for key in ("submitted", "accepted", "rejected", "fills")],
            [1, 0, 1, 0],
        )
        self.assertEqual(report["cashLocked"], "0.00000000")
        capture = json.loads((output / "calibration.json").read_text())
        self.assertEqual(capture["before"]["cashWei"], capture["after"]["cashWei"])
        self.assertEqual(capture["after"]["orders"], [])
        self.assertEqual(capture["after"]["positions"], capture["before"]["positions"])

    def test_independent_evidence_stops_on_missing_wallet_or_changed_money(self):
        import json
        from pathlib import Path
        from tempfile import TemporaryDirectory

        original = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api/live_resting_fixed_20261015"
        )
        with TemporaryDirectory() as directory:
            output = Path(directory)
            for name in ("result.json", "calibration.json", "http_responses.json"):
                (output / name).write_bytes((original / name).read_bytes())
            self.assertEqual(validate_evidence(output)["newMatches"], 0)
            recorded = json.loads((output / "calibration.json").read_text())
            for mutation in ("cash", "wallet", "matches", "orders"):
                with self.subTest(mutation=mutation):
                    capture = copy.deepcopy(recorded)
                    if mutation == "cash":
                        capture["after"]["cashWei"] = "0"
                    elif mutation == "wallet":
                        capture["wallet"]["events"] = []
                    elif mutation == "matches":
                        capture["after"]["matches"] = []
                    else:
                        capture["after"]["orders"] = [order()]
                    (output / "calibration.json").write_text(json.dumps(capture))
                    with self.assertRaises(EvidenceError):
                        validate_evidence(output)

    def test_retained_actual_json_matches_immutable_evidence_manifest(self):
        import hashlib
        import json
        from pathlib import Path

        base = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        manifest = json.loads(
            (base / "maker_evidence_manifest_20261015.json").read_text()
        )
        self.assertGreaterEqual(len(manifest["files"]), 10)
        for entry in manifest["files"]:
            path = (base / entry["path"]).resolve()
            self.assertTrue(path.is_relative_to(base.resolve()))
            self.assertEqual(
                hashlib.sha256(path.read_bytes()).hexdigest(),
                entry["sha256"],
                entry["path"],
            )

    def test_fixed_live_feedback_matches_http_wallet_cache_and_cash(self):
        import json
        from pathlib import Path

        output = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api/live_resting_fixed_20261015"
        )
        report = json.loads((output / "result.json").read_text())
        feedback = json.loads((output / "native_feedback.json").read_text())
        capture = json.loads((output / "calibration.json").read_text())
        http = json.loads((output / "http_responses.json").read_text())
        self.assertEqual(
            [
                report[key]
                for key in (
                    "submitted",
                    "accepted",
                    "canceled",
                    "modifyRejected",
                    "modifyTermsPreserved",
                )
            ],
            [2, 2, 2, 2, 2],
        )
        self.assertEqual(report["fills"], 0)
        self.assertFalse(report["nodeFailed"])
        self.assertEqual(report["cashLocked"], "0.00000000")
        self.assertEqual(
            report["cleanup"], {"verifiedOpenOrders": 0, "fallbackRemovedIds": []}
        )
        posts = [
            row
            for row in http["events"]
            if row["method"] == "POST" and row["path"] == "/v1/orders"
        ]
        self.assertEqual([row["status"] for row in posts], [201, 201])
        hashes = {row["requestTerms"]["hash"] for row in posts}
        self.assertEqual(len(hashes), 2)
        for row in posts:
            self.assertLess(int(row["requestTerms"]["salt"]), 2**31)
            self.assertEqual(int(row["requestTerms"]["makerAmount"]), 10**18)
        self.assertEqual(
            [row["type"] for row in capture["wallet"]["events"]],
            ["orderAccepted", "orderCancelled", "orderAccepted", "orderCancelled"],
        )
        # Nautilus invokes the specific accepted callback first; it immediately requests
        # amend, so the subsequent generic callback observes the latest PendingUpdate.
        for kind, status in [
            ("Accepted", "PENDING_UPDATE"),
            ("ModifyRejected", "ACCEPTED"),
            ("Canceled", "CANCELED"),
        ]:
            rows = [row for row in feedback if row["type"] == kind]
            self.assertEqual({row["venueOrderId"] for row in rows}, hashes)
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["cachedOrder"]["status"] == status for row in rows))
        self.assertEqual(capture["before"]["cashWei"], capture["after"]["cashWei"])
        self.assertEqual(capture["after"]["orders"], [])
        self.assertEqual(capture["after"]["positions"], [])

    def test_real_unresolved_submit_is_not_claimed_as_accepted(self):
        import json
        from pathlib import Path

        output = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api/live_resting_unresolved_20261015"
        )
        report = json.loads((output / "result.json").read_text())
        feedback = json.loads((output / "native_feedback.json").read_text())
        capture = json.loads((output / "calibration.json").read_text())
        self.assertEqual(report["submitted"], 1)
        self.assertEqual(report["accepted"], 0)
        self.assertEqual(report["fills"], 0)
        self.assertTrue(report["nodeFailed"])
        self.assertEqual(
            [
                row["type"]
                for row in feedback
                if row["type"] in {"Submitted", "Accepted", "Filled"}
            ],
            ["Submitted"],
        )
        self.assertTrue(
            any(
                row["type"] == "SubmissionDecision" and row["restConfirmed"]
                for row in feedback
            )
        )
        self.assertEqual(report["cleanup"]["verifiedOpenOrders"], 0)
        self.assertEqual(capture["wallet"]["events"], [])
        self.assertEqual(capture["before"]["cashWei"], capture["after"]["cashWei"])
        self.assertEqual(capture["after"]["orders"], [])




class LatencyAnalysisTests(unittest.TestCase):
    def test_summary_keeps_ns_units_request_counts_and_funding_boundary(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        import json

        names = (
            "decision_to_post",
            "command_to_post",
            "preparation",
            "resource_collection",
            "admission_open_collection",
            "create_request",
            "cancel_request",
            "cancel_to_strategy_callback",
            "ws_to_strategy_callback",
        )

        def distribution(low, high):
            return {
                "min_ns": low,
                "p50_ns": low,
                "p95_ns": high,
                "p99_ns": high,
                "max_ns": high,
                "completed_observations": 2,
                "retained_observations": 2,
            }

        distributions = {name: distribution(100000000, 200000000) for name in names}
        distributions["resource_collection"] = distribution(800000000, 1000000000)
        distributions["preparation"] = distribution(1000000000, 1100000000)
        snapshot = {
            "distributions": distributions,
            "counters": {
                "create_calls": 2,
                "remove_calls": 2,
                "rpc_requests": 20,
                "open_page_requests": 6,
                "metadata_hit": 1,
                "metadata_miss": 1,
                "abandoned_timings": 0,
            },
            "dropped_samples": 0,
            "pending_correlations": 2,
            "lost_correlations": 0,
            "invalid_intervals": 0,
        }
        with TemporaryDirectory() as directory:
            path = Path(directory)
            raw = path / "latency.json"
            raw.write_text(json.dumps({"snapshot": snapshot}))
            (path / "http_responses.json").write_text(
                json.dumps(
                    {
                        "events": [
                            {
                                "method": "POST",
                                "path": "/v1/orders",
                                "upstreamElapsedNs": 150000000,
                            }
                        ]
                    }
                )
            )
            result = analyze_latency(path)
            self.assertEqual(result["metadata"]["hitRate"], 0.5)
            self.assertEqual(result["segments"]["resource_collection"]["p99_ms"], 1000)
            self.assertTrue(result["nextOptimization"]["fundingDominatesPilot"])
            self.assertEqual(result["apiRequests"]["rpc"], 20)
            # Current command flow omits funding/OPEN spans; preserve old archives.
            snapshot["distributions"].pop("resource_collection")
            snapshot["distributions"].pop("admission_open_collection")
            raw.write_text(json.dumps({"commandPath": "venue_validated", "snapshot": snapshot}))
            current = analyze_latency(path, write=False)
            self.assertFalse(current["nextOptimization"]["fundingDominatesPilot"])
            self.assertIsNone(current["nextOptimization"]["minimumFundingOverMaximumPreparation"])
            self.assertNotIn("resource_collection", current["segments"])
            self.assertEqual(current["commandPath"], "venue_validated")
            # Background reads must not reduce the command cache-hit rate or disappear
            # from request totals. Old archives retain the former miss-as-GET fallback.
            self.assertEqual(current["apiRequests"]["marketMetadataCalls"], 1)
            self.assertIsNone(current["apiRequests"]["executionOrderbookCalls"])
            snapshot["counters"].update(metadata_hit=2, metadata_miss=0, metadata_requests=7, orderbook_requests=0)
            raw.write_text(json.dumps({"commandPath": "venue_validated", "snapshot": snapshot}))
            prepared = analyze_latency(path, write=False)
            self.assertEqual(prepared["metadata"]["hitRate"], 1.0)
            self.assertEqual(prepared["apiRequests"]["marketMetadataCalls"], 7)
            self.assertEqual(prepared["apiRequests"]["executionOrderbookCalls"], 0)
            snapshot["distributions"].pop("ws_to_strategy_callback")
            raw.write_text(json.dumps({"snapshot": snapshot}))
            with self.assertRaises(EvidenceError):
                analyze_latency(path, write=False)


if __name__ == "__main__":
    unittest.main()
