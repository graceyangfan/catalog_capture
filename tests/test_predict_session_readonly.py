"""Offline gates for the real private-session probe. LGPL-3.0-or-later."""
import json
import queue
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from websockets.sync.client import connect
from run_predict_queries_readonly import ReadOnlyRelay, permitted
from run_predict_session_readonly import WalletRelay


class Remote:
    def __init__(self):
        self.incoming = queue.Queue()
        self.sent = []

    def recv(self):
        return self.incoming.get(timeout=3)

    def send(self, wire):
        self.sent.append(wire)
        request = json.loads(wire)
        self.incoming.put(json.dumps({"type": "R", "requestId": request["requestId"], "success": True}))

    def close(self):
        self.incoming.put("")


class SessionRelayTests(unittest.TestCase):
    def test_allowlist_rejects_trading_and_chain_writes(self):
        for path in ["/v1/orders", "/v1/orders/remove", "/v1/orders/remove-by-hash"]:
            self.assertFalse(permitted("POST", path, {}))
        self.assertFalse(permitted("POST", "/rpc", {"method": "eth_sendRawTransaction"}))
        self.assertTrue(permitted("POST", "/v1/auth", {}))

    def test_actual_two_rotations_restore_native_and_strategy_account_without_trading(self):
        source = Path(__file__).resolve().parents[1] / "crates/nautilus-predict/tests/fixtures/private_api/session_readonly_20261017T045625Z_69675.json"
        data = json.loads(source.read_text())
        feedback = data["rustFeedback"]
        self.assertTrue(data["passed"])
        self.assertEqual((data["orderWrites"], data["chainWrites"]), (0, 0))
        self.assertEqual(len(data["events"]), 101)
        self.assertTrue(all(e["status"] == 200 for e in data["events"]))
        self.assertTrue(all(permitted(e["method"], e["path"], e.get("requestBody", {})) for e in data["events"]))
        self.assertTrue(feedback["nativeReconciliationEnabled"])
        self.assertFalse(feedback["adapterRecoveryTimerEnabled"])
        self.assertFalse(feedback["submissionSignerInstalled"])
        self.assertFalse(feedback["orderbookRemovalEnabled"])
        self.assertEqual(feedback["acknowledgedConnections"], [0, 1, 2])
        self.assertEqual(feedback["heartbeatEchoConnections"], [1, 2])
        frames, journal = feedback["walletFrames"], feedback["strategyJournal"]
        self.assertEqual(len(frames), 10)
        startup = next(e for e in journal if e["type"] == "NativeStartupComplete")
        self.assertEqual((startup["orders"], startup["positions"]), (0, 0))
        for cycle in [1, 2]:
            renewed = next(e for e in journal if e["type"] == "RealAuthenticationRenewed" and e["cycle"] == cycle)
            self.assertTrue(renewed["tokenChanged"])
            self.assertGreater(renewed["newExp"], renewed["previousExp"])
            ack = next(e for e in journal if e["type"] == "NewWalletAckBeforeQuery" and e["cycle"] == cycle)
            self.assertFalse(ack["nativeReady"])
            ready = next(e for e in journal if e["type"] == "NativeReadyRestored" and e["cycle"] == cycle)
            self.assertTrue(ready["nativeReady"])
            self.assertEqual((ready["orders"], ready["positions"]), (0, 0))
            info = ready["account"]["info"]
            self.assertEqual(info["predict_connection_epoch"], cycle)
            self.assertTrue(info["predict_reconciliation_ready"])
            self.assertEqual(ready["account"]["balances"], startup["account"]["balances"])
            self.assertTrue(any(e["type"] == "StrategyQueryAccount" and e["sequence"] == cycle for e in journal))
            self.assertTrue(any(e["type"] == "StrategyAccountSnapshot" and e["info"].get("predict_connection_epoch") == cycle and e["info"].get("predict_reconciliation_ready") is True for e in journal))
            own = [f["frame"] for f in frames if f["connection"] == cycle]
            subscription = next(f for f in own if f.get("method") == "subscribe")
            self.assertTrue(any(f.get("type") == "R" and f.get("requestId") == subscription["requestId"] and f["success"] is True for f in own))
            heartbeat = next(f for f in own if f.get("topic") == "heartbeat")
            self.assertTrue(any(f.get("method") == "heartbeat" and f["data"] == heartbeat["data"] for f in own))
        self.assertFalse(any(e["type"] == "UnexpectedOrderEvent" for e in journal))
        self.assertTrue(all(e["query"].get("status") == ["OPEN"] for e in data["events"] if e["path"] == "/v1/orders"))

    def test_wallet_relay_forwards_exact_frames_and_retains_only_redacted_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            phase = work / "phase"
            phase.write_text("renewal-1")
            recorder = ReadOnlyRelay("https://rpc.invalid", phase, work / "out.json", upstream=Mock())
            wallet = WalletRelay(recorder, work / "wallet-state.json")
            remote = Remote()
            request = json.dumps({"method": "subscribe", "requestId": 71, "params": ["predictWalletEvents/jwt-secret-sentinel"]})
            try:
                with patch("run_predict_session_readonly.websocket.create_connection", return_value=remote) as factory:
                    with connect(f"ws://127.0.0.1:{wallet.port}", additional_headers={"x-api-key": "key-secret-sentinel"}) as local:
                        local.send(request)
                        self.assertEqual(json.loads(local.recv(timeout=3)), {"type": "R", "requestId": 71, "success": True})
                        self.assertEqual(remote.sent, [request])
                    factory.assert_called_once()
                    self.assertEqual(wallet.acks, [0])
                    self.assertEqual(json.loads(wallet.state_file.read_text())["acknowledgedConnections"], [0])
            finally:
                wallet.close()
                recorder.close({"walletFrames": wallet.events}, success=True)
            retained = (work / "out.json").read_text()
            self.assertNotIn("jwt-secret-sentinel", retained)
            self.assertNotIn("key-secret-sentinel", retained)
            self.assertIn('"requestId": 71', retained)
            self.assertEqual(len(wallet.events), 2)


if __name__ == "__main__":
    unittest.main()
