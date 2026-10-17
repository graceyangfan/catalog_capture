"""Test-only loopback relay retaining sanitized official error JSON. LGPL-3.0-or-later."""

import json
import os
import re
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import requests


def error_evidence(payload, secrets):
    """Allow only error-envelope fields; redact credentials before bounding text."""
    if not isinstance(payload, dict):
        return {"unrecognizedBody": True}
    result = {}
    for key in ("code", "error", "message", "success"):
        value = payload.get(key)
        if isinstance(value, str):
            for secret in secrets:
                if secret:
                    value = value.replace(secret, "[redacted]")
            value = re.sub(r"0x[0-9a-fA-F]{40,}", "[redacted-hex]", value)
            value = re.sub(r"eyJ[A-Za-z0-9_.-]+", "[redacted-jwt]", value)
            value = " ".join(value.split())[:1024]
        elif value is not None and type(value) not in (int, bool):
            value = "[redacted-structured-value]"
        result[key] = value
    return result


def receipt_evidence(payload, secrets):
    """Preserve create/remove receipt fields, excluding arbitrary echoes and credentials."""
    if not isinstance(payload, dict):
        return {"unrecognizedBody": True}
    keys = {
        "success",
        "data",
        "code",
        "orderId",
        "orderHash",
        "removalLockedUntil",
        "removed",
        "noop",
    }

    def project(value):
        if isinstance(value, dict):
            return {key: project(item) for key, item in value.items() if key in keys}
        if isinstance(value, list):
            return [project(item) for item in value]
        if isinstance(value, str):
            for secret in secrets:
                if secret:
                    value = value.replace(secret, "[redacted]")
            value = re.sub(
                r"0x[0-9a-fA-F]{40}(?![0-9a-fA-F])", "[redacted-address]", value
            )
            value = re.sub(r"eyJ[A-Za-z0-9_.-]+", "[redacted-jwt]", value)
            return value[:1024]
        return value

    return project(payload)


def read_response_evidence(payload, secrets):
    """Retain numeric protocol fields, omitting identities/signatures/credentials."""

    def project(value):
        if isinstance(value, dict):
            return {
                key: (
                    "[redacted-signature]"
                    if key.lower() == "signature"
                    else project(item)
                )
                for key, item in value.items()
                if key.lower()
                not in {
                    "token",
                    "jwt",
                    "privatekey",
                    "name",
                    "imageurl",
                    "referral",
                    "points",
                }
            }
        if isinstance(value, list):
            return [project(item) for item in value]
        if isinstance(value, str):
            for secret in secrets:
                if secret:
                    value = value.replace(secret, "[redacted]")
            value = re.sub(
                r"0x[0-9a-fA-F]{40}(?![0-9a-fA-F])", "[redacted-address]", value
            )
            return re.sub(r"eyJ[A-Za-z0-9_.-]+", "[redacted-jwt]", value)
        return value

    return project(payload)


class PrivateHttpRecorder:
    """Forwards unchanged requests once to a fixed HTTPS origin; never retries POST."""

    def __init__(self, output, submission_policy=None, record_reads=False):
        if output.exists():
            raise FileExistsError("refusing to replace retained HTTP evidence")
        self.output = output
        self.events = []
        self.lock = threading.Lock()
        self.submissions = 0
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
                allowed = (self.command == "GET" and path.startswith("/v1/")) or (
                    self.command == "POST"
                    and path in {"/v1/auth", "/v1/orders", "/v1/orders/remove"}
                )
                length = int(self.headers.get("Content-Length", "0"))
                if not allowed or not 0 <= length <= 65536:
                    self.send_error(400)
                    return
                body = self.rfile.read(length)
                parsed = json.loads(body) if body else {}
                event = {
                    "observedAtUtc": datetime.now(UTC).isoformat(),
                    "method": self.command,
                    "path": path,
                    "status": None,
                }
                if self.command == "POST" and path == "/v1/orders/remove":
                    ids = parsed.get("data", {}).get("ids", [])
                    event["requestIds"] = (
                        ids
                        if isinstance(ids, list)
                        and all(isinstance(x, str) and x.isdecimal() for x in ids)
                        else []
                    )
                if self.command == "POST" and path == "/v1/orders":
                    data = parsed.get("data", {})
                    order = data.get("order", {})
                    with recorder.lock:
                        valid = (
                            recorder.submissions < 2
                            and data.get("isPostOnly") is True
                            and data.get("strategy") == "LIMIT"
                            and order.get("side") == 0
                            and order.get("makerAmount") == str(10**18)
                            and order.get("takerAmount")
                            in {str(100 * 10**18), str(50 * 10**18)}
                            and all(
                                key in order
                                for key in ("salt", "nonce", "expiration", "feeRateBps")
                            )
                            and isinstance(order.get("hash"), str)
                            and re.fullmatch(
                                r"0x[0-9a-fA-F]{64}", order.get("hash", "")
                            )
                        )
                        if submission_policy is not None:
                            valid = submission_policy(data, recorder.submissions)
                        if not valid:
                            self.send_error(400)
                            return
                        recorder.submissions += 1
                        event["requestTerms"] = {
                            key: order[key]
                            for key in (
                                "salt",
                                "nonce",
                                "expiration",
                                "feeRateBps",
                                "makerAmount",
                                "takerAmount",
                                "hash",
                            )
                        }
                        # Commit identity before forwarding, including timeout/unknown outcomes.
                        recorder.events.append(event)
                        try:
                            recorder.persist()
                        except OSError:
                            self.send_error(503)
                            return
                headers = {
                    key: value
                    for key, value in self.headers.items()
                    if key.lower()
                    not in {"host", "content-length", "connection", "accept-encoding"}
                }
                secrets = [
                    self.headers.get("x-api-key", ""),
                    self.headers.get("Authorization", "").removeprefix("Bearer "),
                ]
                secrets += [
                    str(parsed.get(key, "")) for key in ("message", "signature")
                ]
                secrets.append(
                    str(parsed.get("data", {}).get("order", {}).get("signature", ""))
                )
                try:
                    response = requests.request(
                        self.command,
                        "https://api.predict.fun" + self.path,
                        data=body or None,
                        headers=headers,
                        timeout=15,
                        allow_redirects=False,
                    )
                except requests.RequestException:
                    with recorder.lock:
                        event["transportError"] = True
                        if "requestTerms" not in event:
                            recorder.events.append(event)
                        recorder.persist()
                    self.send_error(502)
                    return
                event["status"] = response.status_code
                event["completedAtUtc"] = datetime.now(UTC).isoformat()
                try:
                    payload = response.json()
                except ValueError:
                    payload = None
                if response.status_code >= 400 or (
                    isinstance(payload, dict) and payload.get("success") is False
                ):
                    event["errorBody"] = error_evidence(payload, secrets)
                if self.command == "POST" and path in {
                    "/v1/orders",
                    "/v1/orders/remove",
                }:
                    event["responseBody"] = receipt_evidence(payload, secrets)
                if (
                    record_reads
                    and self.command == "GET"
                    and path != "/v1/auth/message"
                ):
                    event["responseBody"] = read_response_evidence(payload, secrets)
                with recorder.lock:
                    if "requestTerms" not in event:
                        recorder.events.append(event)
                    recorder.persist()
                self.send_response(response.status_code)
                self.send_header(
                    "Content-Type",
                    response.headers.get("Content-Type", "application/json"),
                )
                self.send_header("Content-Length", str(len(response.content)))
                if "Retry-After" in response.headers:
                    self.send_header("Retry-After", response.headers["Retry-After"])
                self.end_headers()
                self.wfile.write(response.content)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f"http://127.0.0.1:{self.server.server_port}"

    def persist(self):
        """Caller holds lock; readers see an entire snapshot, never a partial JSON file."""
        pending = self.output.with_suffix(".json.pending")
        owned = False
        try:
            with pending.open("x") as stream:
                owned = True
                json.dump(
                    {
                        "source": "official mainnet HTTP via test-only loopback relay; requests/responses forwarded unchanged, no POST retries",
                        "events": self.events,
                    },
                    stream,
                    indent=2,
                )
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            pending.replace(self.output)
        finally:
            if owned:
                pending.unlink(missing_ok=True)

    def submitted_hashes(self):
        with self.lock:
            return {
                event["requestTerms"]["hash"].lower()
                for event in self.events
                if "requestTerms" in event
            }

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
