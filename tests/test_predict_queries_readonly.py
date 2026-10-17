"""Offline gates for the live read-only command recorder. LGPL-3.0-or-later."""

import http.client
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from run_predict_queries_readonly import ReadOnlyRelay, permitted, balance_block_hash


class QueryRecorderTests(unittest.TestCase):
    def test_rpc_and_rest_allowlist_excludes_all_writes(self):
        for path in ("/v1/orders", "/v1/orders/remove", "/v1/account/referral"):
            self.assertFalse(permitted("POST", path, {}))
        for method in (
            "eth_sendRawTransaction",
            "eth_sendTransaction",
            "personal_sign",
            "eth_sign",
            "eth_signTypedData_v4",
        ):
            self.assertFalse(permitted("POST", "/rpc", {"method": method}))
        self.assertFalse(permitted("POST", "/rpc", [{"method": "eth_call"}]))
        self.assertTrue(permitted("GET", "/v1/orders/0x123", {}))
        self.assertTrue(permitted("POST", "/v1/auth", {}))
        self.assertTrue(permitted("POST", "/rpc", {"method": "eth_call"}))

    def test_live_relay_denies_writes_before_network_and_records_exact_decimals(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            phase = work / "phase"
            phase.write_text("query-account")
            payload = {
                "success": True,
                "data": [
                    {"amount": "31360000000000000000", "fee": "640000000000000000"}
                ],
            }
            wire = json.dumps(payload).encode()
            upstream = Mock(
                return_value=SimpleNamespace(
                    status_code=200, content=wire, json=lambda: payload
                )
            )
            recorder = ReadOnlyRelay(
                "https://rpc.example", phase, work / "out.json", upstream
            )
            try:
                conn = http.client.HTTPConnection(
                    "127.0.0.1", recorder.server.server_port
                )
                conn.request("POST", "/v1/orders", body="{}")
                response = conn.getresponse()
                self.assertEqual(response.status, 403)
                response.read()
                upstream.assert_not_called()
                conn.close()
                conn = http.client.HTTPConnection(
                    "127.0.0.1", recorder.server.server_port
                )
                conn.request(
                    "GET",
                    "/v1/positions?first=100",
                    headers={
                        "Authorization": "Bearer sentinel-jwt",
                        "x-api-key": "sentinel-key",
                    },
                )
                response = conn.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read(), wire)
                conn.close()
            finally:
                recorder.close(success=True)
            saved = json.loads((work / "out.json").read_text())
            self.assertEqual(saved["events"][0]["responseBody"], payload)
            self.assertEqual(saved["events"][0]["phase"], "query-account")
            self.assertNotIn("sentinel", (work / "out.json").read_text())

    def test_account_projection_and_query_parameters_are_anonymous_on_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            phase = work / "phase"
            phase.write_text("query-account")
            address = "0x" + "12" * 20
            payload = {
                "success": True,
                "data": {"address": address, "name": "private-profile-sentinel"},
            }
            wire = json.dumps(payload).encode()
            upstream = Mock(
                return_value=SimpleNamespace(
                    status_code=200, content=wire, json=lambda: payload
                )
            )
            recorder = ReadOnlyRelay(
                "https://rpc.example", phase, work / "out.json", upstream
            )
            recorder.identity = address
            once = recorder.clean({"owner": address, "other": "0x" + "34" * 20})
            self.assertEqual(recorder.clean(once), once)
            try:
                conn = http.client.HTTPConnection(
                    "127.0.0.1", recorder.server.server_port
                )
                conn.request("GET", "/v1/account?signerAddress=" + address)
                response = conn.getresponse()
                self.assertEqual(response.read(), wire)
                conn.close()
            finally:
                recorder.close(success=True)
            text = (work / "out.json").read_text()
            self.assertNotIn(address, text)
            self.assertNotIn("private-profile-sentinel", text)
            data = json.loads(text)["events"][0]
            self.assertEqual(
                data["responseBody"]["data"]["address"],
                "0x0000000000000000000000000000000000000022",
            )
            self.assertEqual(
                data["query"]["signerAddress"],
                ["0x0000000000000000000000000000000000000022"],
            )

    def test_canonical_balance_fence_uses_block_hash_not_block_number(self):
        block = "0x" + "ab" * 32
        body = {
            "method": "eth_call",
            "params": [
                {"data": "0x70a08231" + "0" * 64},
                {"blockHash": block, "requireCanonical": True},
            ],
        }
        self.assertEqual(balance_block_hash(body), block)
        body["params"][1]["requireCanonical"] = False
        self.assertIsNone(balance_block_hash(body))
        body["params"][1] = "latest"
        self.assertIsNone(balance_block_hash(body))

    def test_existing_evidence_cannot_be_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            output = work / "out.json"
            output.write_text("retained")
            with self.assertRaises(FileExistsError):
                ReadOnlyRelay("https://rpc.example", work / "phase", output)
            self.assertEqual(output.read_text(), "retained")


class RecordedQueryEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.base = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api"
        )
        self.capture = json.loads(
            (
                self.base / "query_commands_readonly_20261016T145520Z_41000.json"
            ).read_text()
        )

    def test_account_refresh_matches_recorded_canonical_rpc_block_and_balance(self):
        from decimal import Decimal, ROUND_DOWN

        journal = self.capture["rustFeedback"]["strategyJournal"]
        idle = next(r for r in journal if r["type"] == "StartupAccountQuiescent")
        before = next(r for r in journal if r["type"] == "BeforeQueryAccount")
        after = next(r for r in journal if r["type"] == "AfterQueryAccount")
        self.assertEqual(idle["wire"]["active"], 0)
        self.assertEqual(
            before["info"]["blockHash"], idle["wire"]["lastBalanceBlockHash"]
        )
        self.assertNotEqual(before["eventId"], after["eventId"])
        rows = [e for e in self.capture["events"] if e["phase"] == "query-account"]
        self.assertTrue(
            any(e["path"] == "/v1/account" and e["status"] == 200 for e in rows)
        )
        self.assertTrue(
            any(
                e["path"] == "/v1/orders" and e["query"].get("status") == ["OPEN"]
                for e in rows
            )
        )
        calls = [e for e in rows if balance_block_hash(e.get("requestBody", {}))]
        self.assertTrue(calls)
        for e in calls:
            self.assertEqual(e["status"], 200)
            self.assertEqual(
                balance_block_hash(e["requestBody"]), after["info"]["blockHash"]
            )
            chain = Decimal(int(e["responseBody"]["result"], 16)) / Decimal(10**18)
            native = Decimal(after["balances"][0]["total"].split()[0])
            self.assertEqual(
                chain.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN), native
            )
            self.assertGreaterEqual(chain, native)
            self.assertLess(chain - native, Decimal("0.00000001"))
        blocks = [
            e["responseBody"]["result"]
            for e in rows
            if e.get("requestBody", {}).get("method") == "eth_getBlockByNumber"
        ]
        self.assertTrue(
            any(
                b["hash"] == after["info"]["blockHash"]
                and int(b["number"], 16) == after["info"]["blockNumber"]
                for b in blocks
            )
        )

    def test_actual_order_queries_apply_once_and_preserve_terminal_state(self):
        self.assertTrue(self.capture["passed"])
        self.assertTrue(self.capture["rustFeedback"]["historicalContextIsDerived"])
        self.assertEqual(self.capture["orderWrites"], 0)
        self.assertEqual(self.capture["chainWrites"], 0)
        journal = self.capture["rustFeedback"]["strategyJournal"]
        reports = [r for r in journal if r["type"] == "NativeOrderReportApplied"]
        self.assertEqual(len(reports), 2)
        self.assertEqual(sum(r["type"] == "on_order_canceled" for r in journal), 1)
        for r in reports:
            self.assertEqual(r["cachedStatus"], "CANCELED")
            self.assertEqual(r["report"]["order_status"], "CANCELED")
        for phase in ("query-order-1", "query-order-2"):
            rows = [
                e
                for e in self.capture["events"]
                if e["phase"] == phase and e["path"].startswith("/v1/orders/0x")
            ]
            self.assertEqual(len(rows), 1)
            self.assertTrue(rows[0]["officialSdkHashVerifiedBeforeRedaction"])
            self.assertEqual(rows[0]["responseBody"]["data"]["status"], "CANCELLED")
            self.assertEqual(
                rows[0]["responseBody"]["data"]["order"]["maker"],
                "0x0000000000000000000000000000000000000022",
            )
            self.assertEqual(
                rows[0]["responseBody"]["data"]["order"]["signer"],
                "0x0000000000000000000000000000000000000022",
            )

    def test_derived_rust_replay_hash_matches_official_sdk_and_has_no_duplicate_events(
        self,
    ):
        from capture_predict_startup_readonly import contract_hash
        from predict_sdk import OrderBuilder, ChainId

        replay = json.loads(
            (self.base / "query_commands_native_replay_20261016.json").read_text()
        )
        row = replay["derivedOrder"]
        self.assertEqual(
            contract_hash(OrderBuilder.make(ChainId.BNB_MAINNET), row),
            row["order"]["hash"],
        )
        self.assertEqual(replay["nativeFeedback"][0], replay["nativeFeedback"][1])
        self.assertEqual(replay["nativeFeedback"][0]["status"], "CANCELED")
