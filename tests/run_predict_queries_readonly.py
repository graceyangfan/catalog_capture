"""Opt-in read-only relay for Rust Strategy QueryAccount/QueryOrder. LGPL-3.0-or-later."""

import argparse
import json
import os
import re
import subprocess
import tempfile
import threading
import time
import tomllib
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import requests
from capture_predict_startup_readonly import OWN, contract_hash, sanitize
from predict_sdk import ChainId, OrderBuilder

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data/predict-validation"
READ_RPC = {"eth_chainId", "eth_blockNumber", "eth_getBlockByNumber", "eth_call"}


def permitted(method, path, body):
    if path == "/rpc":
        return (
            method == "POST"
            and isinstance(body, dict)
            and body.get("method") in READ_RPC
        )
    return (method == "GET" and path.startswith("/v1/")) or (
        method == "POST" and path == "/v1/auth"
    )


def balance_block_hash(body):
    if body.get("method") != "eth_call":
        return None
    params = body.get("params", [])
    if (
        len(params) != 2
        or not isinstance(params[0], dict)
        or not isinstance(params[1], dict)
    ):
        return None
    value = params[1].get("blockHash", "")
    if (
        str(params[0].get("data", "")).startswith("0x70a08231")
        and params[1].get("requireCanonical") is True
        and re.fullmatch(r"0x[0-9a-fA-F]{64}", value)
    ):
        return value.lower()
    return None


class ReadOnlyRelay:
    def __init__(self, rpc_url, phase_file, output, upstream=None, state_file=None):
        if output.exists():
            raise FileExistsError("retained query evidence exists")
        self.rpc_url, self.phase_file, self.output = rpc_url, phase_file, output
        self.upstream = upstream or requests.request
        self.identity = "0x" + "0" * 40
        self.secrets = set()
        self.events = []
        self.active = 0
        self.state_file = state_file or output.with_suffix(".state")
        self.last_balance_block = None
        self.query_account_block = None
        self.lock = threading.Lock()
        recorder = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def forward(self):
                path = urlsplit(self.path).path
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= 65536:
                    self.send_error(400)
                    return
                raw = self.rfile.read(length)
                try:
                    body = json.loads(raw) if raw else {}
                except ValueError:
                    self.send_error(400)
                    return
                if not permitted(self.command, path, body):
                    self.send_error(403)
                    return
                headers = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower() in {"authorization", "x-api-key", "content-type"}
                }
                with recorder.lock:
                    recorder.secrets.update(v for v in headers.values() if v)
                    auth = headers.get(
                        "Authorization", headers.get("authorization", "")
                    )
                    if auth.startswith("Bearer "):
                        recorder.secrets.add(auth[7:])
                    if path == "/v1/auth":
                        recorder.identity = body["signer"]
                        recorder.secrets.update(
                            body.get(k, "") for k in ("message", "signature")
                        )
                    event = {
                        "sequence": len(recorder.events),
                        "phase": recorder.phase_file.read_text(),
                        "startedAtUtc": datetime.now(UTC).isoformat(),
                        "method": self.command,
                        "path": path,
                        "query": parse_qs(urlsplit(self.path).query),
                        "status": None,
                    }
                    if path == "/rpc":
                        event["requestBody"] = recorder.clean(body)
                    recorder.events.append(event)
                    recorder.active += 1
                    recorder.state()
                try:
                    url = (
                        recorder.rpc_url
                        if path == "/rpc"
                        else "https://api.predict.fun" + self.path
                    )
                    response = recorder.upstream(
                        self.command,
                        url,
                        headers=headers,
                        data=raw or None,
                        timeout=20,
                        allow_redirects=False,
                    )
                    payload = response.json()
                    if (
                        re.fullmatch(r"/v1/orders/0x[0-9a-fA-F]{64}", path)
                        and response.status_code == 200
                    ):
                        row = payload["data"]
                        assert all(
                            row["order"][k].lower() == recorder.identity.lower()
                            for k in ("maker", "signer")
                        )
                        assert (
                            contract_hash(
                                OrderBuilder.make(ChainId.BNB_MAINNET), row
                            ).lower()
                            == path.rsplit("/", 1)[1].lower()
                        )
                        event["officialSdkHashVerifiedBeforeRedaction"] = True
                    if path == "/v1/auth":
                        stored = {
                            "success": payload.get("success"),
                            "authenticationBodyOmitted": True,
                        }
                    elif path == "/v1/account":
                        # Keep the identity field shape; omit profile/referral/points metadata.
                        stored = {
                            "success": payload.get("success"),
                            "data": {"address": payload.get("data", {}).get("address")},
                            "profileFieldsOmitted": True,
                        }
                    else:
                        stored = recorder.clean(payload)
                    with recorder.lock:
                        event.update(
                            status=response.status_code,
                            responseBody=stored,
                            completedAtUtc=datetime.now(UTC).isoformat(),
                        )
                        if (
                            path == "/rpc"
                            and response.status_code == 200
                            and isinstance(payload.get("result"), str)
                        ):
                            block_hash = balance_block_hash(body)
                            if block_hash:
                                recorder.last_balance_block = block_hash
                                if event["phase"] == "query-account":
                                    recorder.query_account_block = block_hash
                    wire = response.content
                    self.send_response(response.status_code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(wire)))
                    self.end_headers()
                    self.wfile.write(wire)
                except Exception:
                    with recorder.lock:
                        event["transportOrValidationFailure"] = True
                    self.send_error(502, "read-only upstream request failed")
                finally:
                    with recorder.lock:
                        recorder.active -= 1
                        recorder.state()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def state(self):
        # Test-only causal fence; no credentials or exchange state are modified.
        payload = {
            "active": self.active,
            "requests": len(self.events),
            "lastBalanceBlockHash": self.last_balance_block,
            "queryAccountBlockHash": self.query_account_block,
        }
        temporary = self.state_file.with_suffix(".pending")
        temporary.write_text(json.dumps(payload))
        temporary.replace(self.state_file)

    def clean(self, value):
        # Projection can run on an event and again on the final journal: preserve OWN alias.
        protected = re.sub(
            re.escape(OWN) + r"(?![0-9a-fA-F])",
            self.identity,
            json.dumps(value),
            flags=re.IGNORECASE,
        )
        text = json.dumps(sanitize(json.loads(protected), self.identity))
        for secret in self.secrets:
            if secret:
                text = text.replace(secret, "[redacted]")
        # ABI calldata encodes an address without its 0x prefix.
        text = re.sub(re.escape(self.identity[2:]), OWN[2:], text, flags=re.IGNORECASE)
        return json.loads(text)

    def close(self, feedback=None, success=False):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            with self.lock:
                if self.active == 0:
                    break
            time.sleep(0.05)
        with self.lock:
            out = {
                "source": "actual readonly REST/RPC response JSON relayed unchanged to Rust; credential/profile redaction only in retained copy",
                "transforms": [
                    "wallet and contract addresses anonymized, including query parameters and ABI calldata",
                    "signatures, JWT, authentication requests and account profile omitted",
                    "SDK hash verification performed before redaction",
                ],
                "events": self.events,
                "rustFeedback": self.clean(feedback),
                "passed": success,
                "orderWrites": 0,
                "chainWrites": 0,
                "nativeDependencyModified": False,
                "recordingLogicModified": False,
            }
            with self.output.open("x") as f:
                json.dump(self.clean(out), f, indent=2)
                f.write("\n")


def probe_options():
    parser = argparse.ArgumentParser(
        description="Read-only mainnet validation; order and chain writes are blocked."
    )
    parser.add_argument(
        "--features",
        choices=("execution",),
        default="execution",
    )
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    return parser.parse_args()


def main():
    options = probe_options()
    options.output_dir.mkdir(parents=True, exist_ok=True)
    settings = tomllib.loads((ROOT / "env/predict-execution.toml").read_text())
    name = (
        "query_commands_readonly_"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        + f"_{os.getpid()}.json"
    )
    output = options.output_dir / name
    with tempfile.TemporaryDirectory(prefix=".predict-query-", dir=ROOT) as directory:
        work = Path(directory)
        phase = work / "phase.txt"
        phase.write_text("preflight")
        feedback = work / "feedback.json"
        wire_state = work / "wire-state.json"
        relay = ReadOnlyRelay(
            settings["network"]["rpc_url"], phase, output, state_file=wire_state
        )
        origin = f"http://127.0.0.1:{relay.server.server_port}"
        config = work / "relay.json"
        config.write_text(
            json.dumps(
                {
                    "http_origin": origin,
                    "rpc_origin": origin + "/rpc",
                    "phase_file": str(phase),
                    "feedback_path": str(feedback),
                    "wire_state_file": str(wire_state),
                }
            )
        )
        config.chmod(0o600)
        env = dict(os.environ, PREDICT_QUERY_RELAY_CONFIG=str(config))
        result = None
        try:
            result = subprocess.run(
                [
                    "cargo",
                    "test",
                    "-p",
                    "nautilus-predict",
                    "--features",
                    options.features,
                    "--locked",
                    "--test",
                    "private_api_live",
                    "mainnet_strategy_query_commands_readonly",
                    "--",
                    "--ignored",
                    "--nocapture",
                ],
                cwd=ROOT,
                env=env,
                timeout=180,
                check=False,
            )
        finally:
            journal = json.loads(feedback.read_text()) if feedback.exists() else None
            relay.close(journal, result is not None and result.returncode == 0)
        print(
            json.dumps(
                {
                    "file": str(output),
                    "passed": result.returncode == 0,
                    "requests": len(relay.events),
                    "orderWrites": 0,
                    "chainWrites": 0,
                }
            )
        )
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
