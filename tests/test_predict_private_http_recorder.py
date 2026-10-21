"""Offline tests for private feedback recording. LGPL-3.0-or-later."""

import http.client
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import requests
from predict_private_http_recorder import (
    PrivateHttpRecorder,
    error_evidence,
    receipt_evidence,
)


class RecorderTests(unittest.TestCase):
    def test_latency_mode_pools_forwards_once_and_defers_completion_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "http.json"
            order_hash = "0x" + "ab" * 32
            body = {
                "data": {
                    "strategy": "LIMIT",
                    "isPostOnly": True,
                    "order": {
                        "side": 0,
                        "salt": "1",
                        "nonce": "0",
                        "expiration": 4102444800,
                        "feeRateBps": "0",
                        "makerAmount": str(10**18),
                        "takerAmount": str(100 * 10**18),
                        "hash": order_hash,
                        "signature": "secret-signature",
                    },
                }
            }

            def reply(method, url, **kwargs):
                if method == "POST":
                    pending = json.loads(output.read_text())["events"][-1]
                    self.assertEqual(pending["requestTerms"]["hash"], order_hash)
                    self.assertIsNone(pending["status"])
                payload = {
                    "success": True,
                    "data": {"code": "OK", "orderHash": order_hash},
                }
                return SimpleNamespace(
                    status_code=201 if method == "POST" else 200,
                    headers={"Content-Type": "application/json"},
                    content=json.dumps(payload).encode(),
                    json=lambda: payload,
                )

            with patch("predict_private_http_recorder.requests.Session") as session:
                session.return_value.request.side_effect = reply
                recorder = PrivateHttpRecorder(output, measure_latency=True)
                connection = http.client.HTTPConnection(
                    "127.0.0.1", recorder.server.server_port
                )
                try:
                    connection.request("GET", "/v1/account")
                    connection.getresponse().read()
                    self.assertFalse(output.exists())
                    connection.request("POST", "/v1/orders", body=json.dumps(body))
                    response = connection.getresponse()
                    self.assertEqual(response.status, 201)
                    response.read()
                    self.assertIsNone(
                        json.loads(output.read_text())["events"][-1]["status"]
                    )
                    self.assertEqual(recorder.submitted_hashes(), {order_hash})
                finally:
                    connection.close()
                    recorder.close()
                session.assert_called_once()
                self.assertEqual(session.return_value.request.call_count, 2)
                session.return_value.close.assert_called_once()
            saved = json.loads(output.read_text())["events"]
            self.assertEqual(saved[-1]["status"], 201)
            for event in saved:
                self.assertGreaterEqual(
                    event["relayBeforeResponseNs"], event["upstreamElapsedNs"]
                )
            self.assertNotIn("secret-signature", output.read_text())

    def test_successful_removal_response_is_saved_and_forwarded_unchanged(self):
        payload = {"success": True, "removed": ["5468024121"], "noop": []}
        upstream = json.dumps(payload).encode()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "http.json"
            recorder = PrivateHttpRecorder(output)
            try:
                with patch(
                    "predict_private_http_recorder.requests.request",
                    return_value=SimpleNamespace(
                        status_code=200,
                        headers={"Content-Type": "application/json"},
                        content=upstream,
                        json=lambda: payload,
                    ),
                ) as send:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", recorder.server.server_port
                    )
                    connection.request(
                        "POST",
                        "/v1/orders/remove",
                        body=json.dumps({"data": {"ids": ["5468024121"]}}),
                    )
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.read(), upstream)
                    connection.close()
                    send.assert_called_once()
                event = json.loads(output.read_text())["events"][0]
                self.assertEqual(event["responseBody"], payload)
                self.assertNotIn("errorBody", event)
            finally:
                recorder.close()

    def test_retained_journal_cannot_be_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "http.json"
            output.write_text('{"historical":true}')
            with self.assertRaises(FileExistsError):
                PrivateHttpRecorder(output)
            self.assertEqual(output.read_text(), '{"historical":true}')

    def test_unknown_submission_is_journaled_before_forward_and_never_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "http.json"
            recorder = PrivateHttpRecorder(output)
            order_hash = "0x" + "ab" * 32
            body = {
                "data": {
                    "strategy": "LIMIT",
                    "isPostOnly": True,
                    "order": {
                        "side": 0,
                        "salt": "17",
                        "nonce": "0",
                        "expiration": 4102444800,
                        "feeRateBps": "200",
                        "makerAmount": str(10**18),
                        "takerAmount": str(100 * 10**18),
                        "hash": order_hash,
                        "signature": "signature-sentinel",
                    },
                }
            }

            def upstream(*args, **kwargs):
                pending = json.loads(output.read_text())["events"]
                self.assertEqual(pending[0]["requestTerms"]["hash"], order_hash)
                self.assertIsNone(pending[0]["status"])
                self.assertNotIn("signature-sentinel", output.read_text())
                raise requests.Timeout()

            try:
                with patch(
                    "predict_private_http_recorder.requests.request",
                    side_effect=upstream,
                ) as send:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", recorder.server.server_port
                    )
                    connection.request("POST", "/v1/orders", body=json.dumps(body))
                    response = connection.getresponse()
                    self.assertEqual(response.status, 502)
                    response.read()
                    connection.close()
                    send.assert_called_once()
                self.assertEqual(recorder.submitted_hashes(), {order_hash})
                event = json.loads(output.read_text())["events"][0]
                self.assertTrue(event["transportError"])
                self.assertIsNone(event["status"])
                with (
                    patch.object(recorder, "persist", side_effect=OSError()),
                    patch("predict_private_http_recorder.requests.request") as send,
                ):
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", recorder.server.server_port
                    )
                    connection.request("POST", "/v1/orders", body=json.dumps(body))
                    response = connection.getresponse()
                    self.assertEqual(response.status, 503)
                    response.read()
                    connection.close()
                    send.assert_not_called()
            finally:
                recorder.close()

    def test_success_receipts_preserve_protocol_values_without_auth_echoes(self):
        created = {
            "success": True,
            "data": {
                "code": "opaque-code",
                "orderId": "5468024121",
                "orderHash": "0x" + "ab" * 32,
                "removalLockedUntil": None,
            },
        }
        removed = {"success": True, "removed": ["5468024121"], "noop": []}
        for payload in [created, removed]:
            echoed = {**payload, "signature": "sig-sentinel", "token": "jwt-sentinel"}
            self.assertEqual(
                receipt_evidence(echoed, ["sig-sentinel", "jwt-sentinel"]), payload
            )
        reflected = {
            "success": False,
            "data": {"code": "key-sentinel", "token": "jwt-sentinel"},
        }
        self.assertNotIn(
            "key-sentinel", json.dumps(receipt_evidence(reflected, ["key-sentinel"]))
        )

    def test_error_fields_redact_credentials_and_skip_trace(self):
        result = error_evidence(
            {
                "code": 500,
                "error": "internal_server_error",
                "success": False,
                "message": "bad key-sentinel token-sentinel sig-sentinel 0x"
                + "ab" * 32,
                "trace": "secret stack",
                "data": {"token": "secret"},
            },
            ["key-sentinel", "token-sentinel", "sig-sentinel"],
        )
        encoded = json.dumps(result)
        for value in [
            "key-sentinel",
            "token-sentinel",
            "sig-sentinel",
            "secret stack",
            "ab" * 32,
        ]:
            self.assertNotIn(value, encoded)
        self.assertEqual(result["code"], 500)
        self.assertEqual(result["error"], "internal_server_error")
        self.assertEqual(set(result), {"code", "error", "message", "success"})

    def test_post_is_forwarded_once_unchanged_and_error_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "http.json"
            recorder = PrivateHttpRecorder(output)
            body = json.dumps(
                {
                    "data": {
                        "strategy": "LIMIT",
                        "isPostOnly": True,
                        "order": {
                            "side": 0,
                            "salt": "17",
                            "nonce": "0",
                            "expiration": 4102444800,
                            "feeRateBps": "200",
                            "makerAmount": str(10**18),
                            "takerAmount": str(100 * 10**18),
                            "hash": "0x" + "ab" * 32,
                            "signature": "signature-sentinel",
                        },
                    }
                }
            ).encode()
            upstream = json.dumps(
                {
                    "code": 500,
                    "error": "internal_server_error",
                    "success": False,
                    "message": "failure signature-sentinel key-sentinel",
                }
            ).encode()
            try:
                with patch(
                    "predict_private_http_recorder.requests.request",
                    return_value=SimpleNamespace(
                        status_code=500,
                        headers={"Content-Type": "application/json"},
                        content=upstream,
                        json=lambda: json.loads(upstream),
                    ),
                ) as send:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", recorder.server.server_port
                    )
                    connection.request(
                        "POST",
                        "/v1/orders",
                        body=body,
                        headers={"x-api-key": "key-sentinel"},
                    )
                    response = connection.getresponse()
                    self.assertEqual(response.status, 500)
                    self.assertEqual(response.read(), upstream)
                    connection.close()
                    send.assert_called_once()
                    self.assertEqual(
                        send.call_args.args[:2],
                        ("POST", "https://api.predict.fun/v1/orders"),
                    )
                    self.assertEqual(send.call_args.kwargs["data"], body)
                    self.assertFalse(send.call_args.kwargs["allow_redirects"])
                recorded = json.loads(output.read_text())["events"]
                self.assertEqual(len(recorded), 1)
                self.assertEqual(recorded[0]["status"], 500)
                self.assertEqual(recorded[0]["requestTerms"]["salt"], "17")
                for secret in ["signature-sentinel", "key-sentinel"]:
                    self.assertNotIn(secret, output.read_text())
            finally:
                recorder.close()


if __name__ == "__main__":
    unittest.main()
