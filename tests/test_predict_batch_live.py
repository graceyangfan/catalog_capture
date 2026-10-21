"""Offline validation of the real batch/cold-start evidence contract. LGPL-3.0-or-later."""

import json
import tempfile
import unittest
from pathlib import Path

from predict_private_http_recorder import read_response_evidence
from predict_resting_evidence import (
    EvidenceError,
    validate_batch_feedback,
    validate_http_batch_removal,
)


class BatchEvidenceTests(unittest.TestCase):
    def test_read_capture_keeps_18_digit_fields_but_removes_credentials(self):
        source = {
            "data": [
                {
                    "maker": "0x" + "ab" * 20,
                    "signature": "sentinel",
                    "token": "secret",
                    "amountFilled": "1234567890123456789",
                    "hash": "0x" + "cd" * 32,
                }
            ]
        }
        result = read_response_evidence(source, ["secret"])
        self.assertEqual(result["data"][0]["amountFilled"], "1234567890123456789")
        self.assertEqual(result["data"][0]["hash"], source["data"][0]["hash"])
        self.assertNotIn("sentinel", json.dumps(result))
        self.assertNotIn("ab" * 20, json.dumps(result))
        self.assertNotIn("token", result["data"][0])

    def test_rest_and_ws_duplicates_require_exactly_one_strategy_callback(self):
        hashes = {"0x" + "ab" * 32, "0x" + "cd" * 32}
        rows = [
            {"phase": "place", "type": "StrategyOnStart", "existingOrders": 0},
            {"phase": "recover", "type": "StrategyOnStart", "existingOrders": 2},
            {
                "phase": "recover",
                "type": "BeforeNativeRun",
                "cachedOrders": 0,
                "submissionSignerInstalled": False,
            },
            {
                "phase": "handover",
                "type": "RealOpenOrdersBeforeColdStartup",
                "orders": [{"hash": h, "amountFilled": "0"} for h in hashes],
            },
            {"phase": "recover", "type": "StrategyBatchCancelOrders"},
            {
                "phase": "recover",
                "type": "AccountStateApplied",
                "balances": [{"locked": "2"}],
            },
        ]
        for h in hashes:
            rows += [
                {
                    "phase": "recover",
                    "type": "StrategyOrderCallback",
                    "kind": "Canceled",
                    "venueOrderId": h,
                },
                *[
                    {
                        "phase": "recover",
                        "type": "RawOrderStatusReport",
                        "report": {
                            "venue_order_id": h,
                            "order_status": "CANCELED",
                            "ts_last": 12000000,
                        },
                    }
                    for _ in range(2)
                ],
            ]
        rows.append(
            {
                "phase": "recover",
                "type": "AccountStateApplied",
                "balances": [{"locked": "0"}],
            }
        )
        capture = {
            "wallet": {
                "events": [
                    {"type": "orderCancelled", "orderHash": h, "timestamp": 12}
                    for h in hashes
                ]
            }
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            source = output / "native_input_and_callbacks.json"
            source.write_text(json.dumps(rows))
            validate_batch_feedback(output, {}, capture, hashes)
            self.assertEqual(
                json.loads((output / "batch_native_validation.json").read_text())[
                    "restoredActiveOrders"
                ],
                2,
            )
            rows.append(
                next(r for r in rows if r["type"] == "StrategyOrderCallback").copy()
            )
            source.write_text(json.dumps(rows))
            with self.assertRaises(EvidenceError):
                validate_batch_feedback(output, {}, capture, hashes)

    def test_actual_native_wallet_rest_and_funds_agree_and_intent_loss_is_explicit(
        self,
    ):
        for name in [
            "live_resting_batch-restart_20261016T153745Z",
            "live_resting_http-batch_20261016T160926Z",
        ]:
            self._verify_actual_capture(name)

    def _verify_actual_capture(self, name):
        output = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
            / name
        )
        rows = json.loads((output / "native_input_and_callbacks.json").read_text())
        capture = json.loads((output / "calibration.json").read_text())
        report = json.loads((output / "result.json").read_text())
        http = json.loads((output / "http_responses.json").read_text())["events"]
        hashes = {
            r["requestTerms"]["hash"]
            for r in http
            if r["method"] == "POST" and r["path"] == "/v1/orders"
        }
        # Validation writes only derived files; replay in an isolated directory.
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            (temporary / "native_input_and_callbacks.json").write_text(json.dumps(rows))
            (temporary / "http_responses.json").write_text(json.dumps({"events": http}))
            validate_batch_feedback(temporary, report, capture, hashes)
        self.assertEqual(report["fills"], 0)
        self.assertFalse(report["nodeFailed"])
        self.assertEqual(capture["before"]["cashWei"], capture["after"]["cashWei"])
        self.assertEqual(capture["after"]["orders"], [])
        self.assertEqual(capture["after"]["positions"], [])
        sdk = json.loads((output / "official_sdk_validation.json").read_text())
        self.assertEqual({r["hash"] for r in sdk["orders"]}, hashes)
        self.assertTrue(
            all(
                r["officialSdkHashMatches"] and r["officialSdkAmountsMatch"]
                for r in sdk["orders"]
            )
        )
        original = [
            r["report"]
            for r in rows
            if r["phase"] == "place" and r["type"] == "RawOrderStatusReport"
        ]
        restored = [
            r["report"]
            for r in rows
            if r["phase"] == "recover"
            and r["type"] == "RawOrderStatusReport"
            and r["report"]["order_status"] == "ACCEPTED"
        ]
        self.assertEqual(len(original), 2)
        self.assertEqual(len(restored), 2)
        self.assertTrue(
            all(r["time_in_force"] == "GTC" and r["post_only"] for r in original)
        )
        self.assertTrue(
            all(
                r["time_in_force"] == "GTD"
                and not r["post_only"]
                and r["expire_time"] == 4102444800000000000
                for r in restored
            )
        )

    def test_http_batch_proof_requires_one_request_and_exact_two_ids(self):
        import copy

        http = {
            "events": [
                {
                    "method": "POST",
                    "path": "/v1/orders",
                    "responseBody": {"data": {"orderId": order_id}},
                }
                for order_id in ["1", "2"]
            ]
            + [
                {
                    "method": "POST",
                    "path": "/v1/orders/remove",
                    "status": 200,
                    "requestIds": ["1", "2"],
                    "responseBody": {
                        "success": True,
                        "removed": ["2", "1"],
                        "noop": [],
                    },
                }
            ]
        }
        self.assertEqual(validate_http_batch_removal(http)["httpRequests"], 1)
        for mode in ["extra-request", "wrong-id", "noop", "duplicate-id"]:
            bad = copy.deepcopy(http)
            if mode == "extra-request":
                bad["events"].append(copy.deepcopy(bad["events"][-1]))
            elif mode == "wrong-id":
                bad["events"][-1]["requestIds"] = ["1", "3"]
            elif mode == "noop":
                bad["events"][-1]["responseBody"] = {
                    "success": True,
                    "removed": ["1"],
                    "noop": ["2"],
                }
            else:
                bad["events"][-1]["requestIds"] = ["1", "1"]
            with self.assertRaises(EvidenceError):
                validate_http_batch_removal(bad)
