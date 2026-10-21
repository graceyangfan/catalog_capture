#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Explicit minimum-size Rust mainnet calibration with independent final cleanup.

LGPL-3.0-or-later. Only Rust submits orders. Python supervises, captures read-only
feedback, and can remove verified residual calibration orders after Rust exits.
"""

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests
from capture_predict_private_fixtures import ROOT, CaptureError, private_toml
from eth_account import Account
from predict_private_http_recorder import PrivateHttpRecorder
from predict_read_only_calibration import complete_pages
from predict_resting_evidence import EvidenceError, validate_evidence
from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
from web3 import Web3

OUTPUT = ROOT / "crates/nautilus-predict/tests/fixtures/private_api/live_resting"
RUN_CONFIG = ROOT / "env/predict-resting-calibration.toml"
PROXY_MANIFEST = ROOT / "logs/predict-private-http-proxy.json"
ARCHIVE = ROOT / "data/predict-validation"


def archive_run(output, label):
    """Keep operational recordings out of immutable regression fixtures."""
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    destination = ARCHIVE / (
        f"live_resting_{label}_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    )
    if destination.exists():
        raise CaptureError("run archive already exists; refusing to replace evidence")
    output.rename(destination)
    return destination


def is_calibration_order(row, identity):
    order = row["order"]
    return (
        order["maker"].lower() == identity.lower()
        and order["signer"].lower() == identity.lower()
        and int(order["side"]) == 0
        and int(order["makerAmount"]) == 10**18
        and int(order["takerAmount"]) in (100 * 10**18, 50 * 10**18)
    )


def authenticated_reader():
    settings = private_toml(ROOT / "env/predict-execution.toml")
    if settings["network"]["environment"] != "mainnet":
        raise CaptureError("mainnet configuration required")
    key_path = Path(settings["network"]["api_key_file"])
    if not key_path.is_absolute():
        key_path = ROOT / "env" / key_path
    api_key = private_toml(key_path)["api_key"]
    private = settings["privy"]
    owner = Account.from_key(private["private_key"])
    if (
        private.get("owner_address")
        and owner.address.lower() != private["owner_address"].lower()
    ):
        raise CaptureError("owner guard mismatch")
    if private.get("mode", "predict_account") != "predict_account":
        raise CaptureError("this supervisor requires explicit Predict Account mode")
    identity = Web3.to_checksum_address(private["predict_account"])
    builder = OrderBuilder.make(
        ChainId.BNB_MAINNET,
        private["private_key"],
        OrderBuilderOptions(predict_account=identity),
    )
    http = requests.Session()
    http.headers["x-api-key"] = api_key

    def request(method, path, **kwargs):
        if method != "GET" and (method, path) not in {
            ("POST", "/v1/auth"),
            ("POST", "/v1/orders/remove"),
        }:
            raise CaptureError(
                "supervisor forbids order submission and chain transactions"
            )
        response = http.request(
            method,
            "https://api.predict.fun" + path,
            timeout=15,
            allow_redirects=False,
            **kwargs,
        )
        if response.status_code != 200:
            raise CaptureError(f"supervisor HTTP {response.status_code}")
        return response.json()

    challenge = request("GET", "/v1/auth/message")["data"]["message"]
    signature = builder.sign_predict_account_message(challenge)
    jwt = request(
        "POST",
        "/v1/auth",
        json={"signer": identity, "message": challenge, "signature": signature},
    )["data"]["token"]
    http.headers["Authorization"] = "Bearer " + jwt
    if request("GET", "/v1/account")["data"]["address"].lower() != identity.lower():
        raise CaptureError("authenticated account mismatch")
    return request, identity


def final_cleanup(request, identity, submitted_hashes, verify_terms=None):
    """Only this run's pre-forward journal grants authority to remove an order."""
    submitted_hashes = {value.lower() for value in submitted_hashes}
    verify_terms = verify_terms or (lambda row: is_calibration_order(row, identity))
    if any(not re.fullmatch(r"0x[0-9a-f]{64}", value) for value in submitted_hashes):
        raise CaptureError("invalid run order hash")
    removed = []
    for _ in range(6):
        rows = complete_pages(request, "/v1/orders", status="OPEN")
        owned = [
            row for row in rows if row["order"]["hash"].lower() in submitted_hashes
        ]
        if not owned:
            return {
                "verifiedOpenOrders": len(rows),
                "verifiedRunOpenOrders": 0,
                "unrelatedOpenOrders": len(rows),
                "fallbackRemovedIds": removed,
            }
        ids = []
        for row in owned:
            if not verify_terms(row):
                raise CaptureError(
                    "unrelated order found; cleanup refuses foreign terms"
                )
            verified = request("GET", "/v1/orders/" + row["order"]["hash"])["data"]
            if (
                verified["id"] != row["id"]
                or verified["order"]["hash"].lower() != row["order"]["hash"].lower()
                or not verify_terms(verified)
            ):
                raise CaptureError("cleanup hash/id/terms mismatch")
            ids.append(row["id"])
        receipt = request("POST", "/v1/orders/remove", json={"data": {"ids": ids}})
        if receipt.get("success") is not True:
            raise CaptureError("cleanup removal not acknowledged")
        removed.extend(ids)
        time.sleep(0.5)
    raise CaptureError("residual open orders remain")


CASES = (
    "auto",
    "yes-gtc",
    "no-gtc",
    "yes-gtd",
    "no-gtd",
    "batch-restart",
    "http-batch",
)


def run(case="auto", features="execution", measure_latency=False):
    if case not in CASES:
        raise CaptureError("unsupported resting case")
    if features not in {"execution", "native-quantity-commission"}:
        raise CaptureError("unsupported Native feature configuration")
    if measure_latency and case not in {"yes-gtd", "no-gtd"}:
        raise CaptureError("latency measurement requires one bounded GTD resting case")
    request, identity = authenticated_reader()
    if complete_pages(request, "/v1/orders", status="OPEN"):
        raise CaptureError("existing open orders; refusing calibration")
    if RUN_CONFIG.exists() or PROXY_MANIFEST.exists():
        raise CaptureError(
            "one-run TOML/relay manifest already exists; refusing to overwrite"
        )
    if OUTPUT.exists():
        archive_run(OUTPUT, "interrupted")
    OUTPUT.mkdir(parents=True)
    metadata = {
        "startedAtUtc": datetime.now(UTC).isoformat(),
        "case": case,
        "features": features,
        "measureLatency": measure_latency,
        "buildProfile": "release" if measure_latency else "debug",
        "sourceCommit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "trackedDiffSha256": hashlib.sha256(
            subprocess.check_output(["git", "diff", "HEAD", "--"], cwd=ROOT)
        ).hexdigest(),
        "fileSha256": {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in (
                "Cargo.lock",
                "tests/run_predict_resting_live.py",
                "tests/predict_resting_evidence.py",
                "crates/nautilus-predict/tests/support/resting_live.rs",
                "crates/nautilus-predict/tests/support/strategy.rs",
                "crates/nautilus-predict/src/latency.rs",
                "crates/nautilus-predict/src/execution/submitter.rs",
                "crates/nautilus-predict/src/execution/cancellations.rs",
            )
        },
    }
    (OUTPUT / "run_metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # Credentials remain local in a separate 0600 profile, removed in finally.
    source = (ROOT / "env/predict-execution.toml").read_text()
    for name, value in (
        ("allow_order_writes", "true"),
        ("max_order_usdt", '"1"'),
        ("max_total_usdt", '"2"'),
    ):
        source, count = re.subn(
            r"(?m)^" + name + r"\s*=.*$", name + " = " + value, source
        )
        if count != 1:
            raise CaptureError("one-run TOML field missing or ambiguous")
    marker = ROOT / "logs/predict-wallet-ready.json"
    marker.unlink(missing_ok=True)
    collector = node = recorder = None
    manifest_owned = False
    node_status = None
    try:
        descriptor = os.open(RUN_CONFIG, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(source)
        recorder = PrivateHttpRecorder(
            OUTPUT / "http_responses.json",
            record_reads=case in {"batch-restart", "http-batch"},
            measure_latency=measure_latency,
        )
        descriptor = os.open(
            PROXY_MANIFEST, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        manifest_owned = True
        with os.fdopen(descriptor, "w") as stream:
            json.dump(
                {
                    "origin": recorder.origin,
                    "case": case,
                    "measureLatency": measure_latency,
                },
                stream,
            )
        with (OUTPUT / "collector.log").open("w") as capture_log:
            collector = subprocess.Popen(
                [
                    sys.executable,
                    str(ROOT / "tests/capture_predict_private_fixtures.py"),
                    "--capture",
                    "--calibrate",
                    "--live-resting",
                    "--calibration-seconds",
                    "140" if case in {"batch-restart", "http-batch"} else "100",
                    "--ready-file",
                    str(marker),
                ],
                cwd=ROOT,
                stdout=capture_log,
                stderr=subprocess.DEVNULL,
            )
            deadline = time.monotonic() + 45
            while not marker.exists():
                if collector.poll() is not None or time.monotonic() >= deadline:
                    raise CaptureError(
                        "wallet collector did not acknowledge; no orders allowed"
                    )
                time.sleep(0.2)
            print(
                f"Wallet capture subscribed; {case}: Rust LiveNode, <=1 USDT/order, <=2 USDT total.",
                flush=True,
            )
            with (OUTPUT / "node.log").open("w") as node_log:
                node = subprocess.Popen(
                    [
                        "cargo",
                        "test",
                        *(["--release"] if measure_latency else []),
                        "-p",
                        "nautilus-predict",
                        "--features",
                        features,
                        "--locked",
                        "--test",
                        "private_api_live",
                        (
                            "batch_live::mainnet_rust_strategy_batch_restart"
                            if case in {"batch-restart", "http-batch"}
                            else "resting_live::mainnet_rust_strategy_quiet_resting_cancel_replace"
                        ),
                        "--",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                    ],
                    cwd=ROOT,
                    stdout=node_log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                node_status = node.wait(
                    timeout=140 if case in {"batch-restart", "http-batch"} else 100
                )
    finally:
        if node is not None and node.poll() is None:
            os.killpg(node.pid, signal.SIGINT)
            try:
                node.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(node.pid, signal.SIGKILL)
                node.wait()
        try:
            # Drain in-flight upstream calls before the independent final order query.
            try:
                if recorder is not None:
                    recorder.close()
            finally:
                cleanup = final_cleanup(
                    request,
                    identity,
                    recorder.submitted_hashes() if recorder else set(),
                )
                (OUTPUT / "supervisor_cleanup.json").write_text(
                    json.dumps(cleanup, indent=2) + "\n"
                )
                if cleanup["verifiedOpenOrders"]:
                    raise CaptureError(
                        "unrelated orders appeared; preserved, stop calibration"
                    )
                print("Independent final REST check: zero open orders.", flush=True)
        finally:
            RUN_CONFIG.unlink(missing_ok=True)
            if manifest_owned:
                PROXY_MANIFEST.unlink(missing_ok=True)
            marker.unlink(missing_ok=True)
            if collector is not None:
                try:
                    collector.wait(
                        timeout=165 if case in {"batch-restart", "http-batch"} else 125
                    )
                except subprocess.TimeoutExpired:
                    collector.terminate()
                    collector.wait(timeout=5)
    node_log = OUTPUT / "node.log"
    if node_log.exists():
        lines = [
            re.sub(r"\x1b\[[0-9;]*m", "", line)
            for line in node_log.read_text().splitlines()
            if "Predict transmitting order:" in line
            or "Predict transmitting LIMIT order:" in line
            or "Predict submission outcome is unresolved;" in line
        ]
        (OUTPUT / "submission_diagnostics.json").write_text(
            json.dumps(
                {"source": "allowlisted sanitized adapter diagnostics", "lines": lines},
                indent=2,
            )
            + "\n"
        )
    if node_status != 0:
        raise CaptureError(
            "Rust lifecycle assertions failed; inspect local feedback; cleanup ran"
        )
    if collector.returncode != 0:
        raise CaptureError("read-only collector failed; cleanup ran")
    if measure_latency and not (OUTPUT / "latency.json").exists():
        raise CaptureError("requested latency evidence is missing; cleanup ran")
    validate_evidence(OUTPUT)
    if complete_pages(request, "/v1/orders", status="OPEN"):
        raise CaptureError("fresh final open-order check failed")
    (OUTPUT / "final_open_orders_check.json").write_text(
        json.dumps(
            {
                "observedAtUtc": datetime.now(UTC).isoformat(),
                "source": "independent authenticated complete OPEN pagination after capture",
                "verifiedOpenOrders": 0,
            },
            indent=2,
        )
        + "\n"
    )
    destination = archive_run(OUTPUT, case)
    print("Live calibration completed; JSON feedback and independent cleanup retained.")
    print(f"Evidence: {destination.relative_to(ROOT)}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute-minimum-resting",
        action="store_true",
        required=True,
        help="authorize this run's two <=1 USDT post-only orders and final cleanup",
    )
    parser.add_argument("--case", choices=CASES, default="auto")
    parser.add_argument(
        "--measure-latency",
        action="store_true",
        help="enable bounded probes, deferred journals and release build for one GTD case",
    )
    parser.add_argument(
        "--features",
        choices=("execution", "native-quantity-commission"),
        default="execution",
    )
    args = parser.parse_args()
    try:
        run(args.case, args.features, args.measure_latency)
    except Exception as error:  # noqa: BLE001 - never expose remote exception text or credentials
        print(
            "Calibration failed:",
            str(error)
            if isinstance(error, (CaptureError, EvidenceError))
            else type(error).__name__,
        )
        raise SystemExit(1) from None
