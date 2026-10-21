"""Opt-in real JWT/WS/Native recovery probe; no trading endpoints. LGPL-3.0-or-later."""
import json
import os
import subprocess
import tempfile
import threading
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import websocket
from websockets.sync.server import serve

from run_predict_queries_readonly import ROOT, ReadOnlyRelay, probe_options


class WalletRelay:
    """Test-only transparent WS relay; preserve wire frames and redact retained copies."""

    def __init__(self, recorder, state_file):
        self.recorder, self.state_file = recorder, state_file
        self.lock = threading.Lock()
        self.events, self.acks, self.connections = [], [], 0
        self.echoes = []
        self.server = serve(self.handle, "127.0.0.1", 0)
        self.port = self.server.socket.getsockname()[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.save_state()

    def save_state(self):
        temporary = self.state_file.with_suffix(".pending")
        temporary.write_text(json.dumps({"connections": self.connections, "acknowledgedConnections": self.acks, "heartbeatEchoConnections": self.echoes, "frames": len(self.events)}))
        temporary.replace(self.state_file)

    def record(self, identity, direction, wire):
        value = json.loads(wire)
        with self.recorder.lock:
            # Never retain an opaque JWT-bearing topic, including in error envelopes.
            topic = value.get("topic", "")
            for item in [topic, *value.get("params", [])]:
                if isinstance(item, str) and item.startswith("predictWalletEvents/"):
                    self.recorder.secrets.add(item.split("/", 1)[1])
            clean = self.recorder.clean(value)
        with self.lock:
            self.events.append({"connection": identity, "direction": direction, "receivedAtUtc": datetime.now(UTC).isoformat(), "frame": clean})
            if direction == "venue-to-native" and value.get("type") == "R" and value.get("success") is True:
                if identity not in self.acks:
                    self.acks.append(identity)
            if direction == "native-to-venue" and value.get("method") == "heartbeat" and identity not in self.echoes:
                self.echoes.append(identity)
            self.save_state()

    def handle(self, local):
        with self.lock:
            identity = self.connections
            self.connections += 1
            self.save_state()
        key = local.request.headers.get("x-api-key")
        with self.recorder.lock:
            if key:
                self.recorder.secrets.add(key)
        remote = None
        try:
            remote = websocket.create_connection("wss://ws.predict.fun/ws", header={"x-api-key": key}, suppress_origin=True, timeout=25, enable_multithread=True)

            def receive():
                try:
                    while True:
                        frame = remote.recv()
                        if not frame:
                            break
                        self.record(identity, "venue-to-native", frame)
                        local.send(frame)
                except Exception:
                    pass  # Raw transport errors may include a private wire topic.
                finally:
                    local.close()

            incoming = threading.Thread(target=receive, daemon=True)
            incoming.start()
            try:
                for frame in local:
                    self.record(identity, "native-to-venue", frame)
                    remote.send(frame)
            finally:
                remote.close()
                incoming.join(timeout=3)
        except Exception:
            local.close()
        finally:
            if remote:
                remote.close()

    def close(self):
        self.server.shutdown()
        self.thread.join(timeout=5)


def main():
    options = probe_options()
    options.output_dir.mkdir(parents=True, exist_ok=True)
    settings = tomllib.loads((ROOT / "env/predict-execution.toml").read_text())
    name = "session_readonly_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}.json"
    output = options.output_dir / name
    with tempfile.TemporaryDirectory(prefix=".predict-session-", dir=ROOT) as directory:
        work = Path(directory)
        phase, feedback = work / "phase", work / "feedback.json"
        phase.write_text("authentication")
        relay = ReadOnlyRelay(settings["network"]["rpc_url"], phase, output, state_file=work / "http-state.json")
        wallet = WalletRelay(relay, work / "wallet-state.json")
        origin = f"http://127.0.0.1:{relay.server.server_port}"
        config = work / "relay.json"
        config.write_text(json.dumps({"http_origin": origin, "rpc_origin": origin + "/rpc", "wallet_origin": f"ws://127.0.0.1:{wallet.port}/ws", "phase_file": str(phase), "feedback_path": str(feedback), "wire_state_file": str(relay.state_file), "wallet_state_file": str(wallet.state_file)}))
        config.chmod(0o600)
        result = None
        try:
            result = subprocess.run(["cargo", "test", "-p", "nautilus-predict", "--features", options.features, "--locked", "--test", "private_api_live", "mainnet_jwt_wallet_recovery_readonly", "--", "--ignored", "--nocapture"], cwd=ROOT, env=dict(os.environ, PREDICT_SESSION_RELAY_CONFIG=str(config)), timeout=240, check=False)
        finally:
            wallet.close()
            journal = json.loads(feedback.read_text()) if feedback.exists() else {}
            journal.update(walletFrames=wallet.events, acknowledgedConnections=wallet.acks, heartbeatEchoConnections=wallet.echoes, renewalTrigger="explicit early renewal with real venue JWT; not natural expiration")
            relay.close(journal, result is not None and result.returncode == 0)
        print(json.dumps({"file": str(output), "passed": result is not None and result.returncode == 0, "requests": len(relay.events), "walletFrames": len(wallet.events), "orderWrites": 0, "chainWrites": 0}))
        raise SystemExit(result.returncode if result else 1)


if __name__ == "__main__":
    main()
