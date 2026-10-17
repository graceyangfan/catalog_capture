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
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import requests
from capture_predict_private_fixtures import ROOT, CaptureError, private_toml
from eth_account import Account
from predict_private_http_recorder import PrivateHttpRecorder
from predict_read_only_calibration import complete_pages
from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
from web3 import Web3

OUTPUT = ROOT / "crates/nautilus-predict/tests/fixtures/private_api/live_resting"
RUN_CONFIG = ROOT / "env/predict-resting-calibration.toml"
PROXY_MANIFEST = ROOT / "logs/predict-private-http-proxy.json"


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


def validate_evidence(output):
    """Independent captured wallet/chain facts must agree before another run."""
    report = json.loads((output / "result.json").read_text())
    capture = json.loads((output / "calibration.json").read_text())
    http = json.loads((output / "http_responses.json").read_text())
    before, after = capture["before"], capture["after"]
    posts = [
        row
        for row in http["events"]
        if row["method"] == "POST" and row["path"] == "/v1/orders"
    ]
    hashes = {row["requestTerms"]["hash"] for row in posts}
    accepted = {
        row["orderHash"]
        for row in capture["wallet"]["events"]
        if row["type"] == "orderAccepted"
    }
    canceled = {
        row["orderHash"]
        for row in capture["wallet"]["events"]
        if row["type"] == "orderCancelled"
    }
    matches = lambda snapshot: {
        (row["transactionHash"], str(row["settlementId"]), row["amountFilled"])
        for row in snapshot["matches"]
    }
    if not (
        len(posts) == len(hashes) == 2
        and all(row["status"] == 201 for row in posts)
        and hashes == accepted == canceled
        and before["cashWei"] == after["cashWei"]
        and before["tokenBalances"] == after["tokenBalances"]
        and before["positions"] == after["positions"]
        and matches(before) == matches(after)
        and not before["orders"]
        and not after["orders"]
        and report["fills"] == 0
        and report["rejected"] == report["denied"] == 0
        and all(
            report[key] == 2
            for key in (
                "submitted",
                "accepted",
                "canceled",
                "modifyRejected",
                "modifyTermsPreserved",
            )
        )
    ):
        raise CaptureError("independent mainnet evidence disagrees; stop further cases")
    case = report.get("case", "auto")
    if case in {"batch-restart", "http-batch"}:
        validate_batch_feedback(output, report, capture, hashes)
    elif case != "auto":
        feedback = json.loads((output / "native_feedback.json").read_text())
        outcome, tif = case.split("-")
        if report["outcome"] != {"yes": 1, "no": 2}[outcome]:
            raise CaptureError("recorded outcome differs from selected case")
        if any(
            row["details"]["outcomeIndex"] != report["outcome"]
            or row["details"]["marketId"] != report["marketId"]
            for row in capture["wallet"]["events"]
            if row.get("orderHash") in hashes
        ):
            raise CaptureError(
                "wallet market/outcome differs from selected native instrument"
            )
        for row in posts:
            receipt = row["responseBody"]
            if not (
                receipt["success"] is True
                and receipt["data"]["code"] == "OK"
                and receipt["data"]["orderHash"] == row["requestTerms"]["hash"]
            ):
                raise CaptureError("create receipt disagrees with signed hash")
            expiration = row["requestTerms"]["expiration"]
            if tif == "gtc":
                if expiration != 4102444800:
                    raise CaptureError("GTC differs from SDK default expiration")
            else:
                observed = datetime.fromisoformat(row["observedAtUtc"]).timestamp()
                if not observed + 120 < expiration <= observed + 300:
                    raise CaptureError(
                        "GTD expiration differs from bounded future timestamp"
                    )
        for kind in ("Accepted", "ModifyRejected", "Canceled"):
            events = [row for row in feedback if row["type"] == kind]
            if len(events) != 2 or {row["venueOrderId"] for row in events} != hashes:
                raise CaptureError(
                    "native lifecycle disagrees with actual wallet/HTTP hashes"
                )
            for event in events:
                cached = event["cachedOrder"]
                if not cached["postOnly"] or cached["timeInForce"] != tif.upper():
                    raise CaptureError(
                        "native order lost its post-only or time-in-force"
                    )
                if tif == "gtd":
                    wire = next(
                        row
                        for row in posts
                        if row["requestTerms"]["hash"] == event["venueOrderId"]
                    )
                    if (
                        cached["expireTimeNs"]
                        != wire["requestTerms"]["expiration"] * 10**9
                    ):
                        raise CaptureError(
                            "native GTD expiration differs from signed request"
                        )
        locks = [
            Decimal(balance["locked"])
            for row in feedback
            if row["type"] == "AccountStateApplied"
            for balance in row["balances"]
        ]
        if Decimal(1) not in locks or not locks or locks[-1] != 0 or max(locks) > 1:
            raise CaptureError(
                "strategy did not observe collateral reservation then unlock"
            )
    validation = {
        "verifiedAtUtc": datetime.now(UTC).isoformat(),
        "case": report.get("case", "auto"),
        "createdOrders": 2,
        "walletAcceptedAndCanceled": 2,
        "cashWeiUnchanged": True,
        "positionsAndTokenBalancesUnchanged": True,
        "newMatches": 0,
        "finalOpenOrders": 0,
    }
    (output / "evidence_validation.json").write_text(
        json.dumps(validation, indent=2) + "\n"
    )
    return validation


def validate_http_batch_removal(http):
    creates = [
        r for r in http["events"] if r["method"] == "POST" and r["path"] == "/v1/orders"
    ]
    removes = [
        r
        for r in http["events"]
        if r["method"] == "POST" and r["path"] == "/v1/orders/remove"
    ]
    ids = {r["responseBody"]["data"]["orderId"] for r in creates}
    if len(creates) != 2 or len(ids) != 2 or len(removes) != 1:
        raise CaptureError(
            "expected exactly one HTTP batch removal for two created orders"
        )
    row = removes[0]
    requested = row.get("requestIds", [])
    receipt = row.get("responseBody", {})
    if not (
        len(requested) == 2
        and set(requested) == ids
        and row["status"] == 200
        and receipt.get("success") is True
        and len(receipt.get("removed", [])) == 2
        and set(receipt["removed"]) == ids
        and receipt.get("noop") == []
    ):
        raise CaptureError("multi-ID HTTP request and per-order receipts disagree")
    return {"httpRequests": 1, "requestIds": requested, "allRemoved": True}


def validate_batch_feedback(output, report, capture, hashes):
    batch_http = None
    if report.get("case") == "http-batch":
        batch_http = validate_http_batch_removal(
            json.loads((output / "http_responses.json").read_text())
        )
    rows = json.loads((output / "native_input_and_callbacks.json").read_text())
    starts = {r["phase"]: r for r in rows if r["type"] == "StrategyOnStart"}
    if (
        starts["place"]["existingOrders"] != 0
        or starts["recover"]["existingOrders"] != 2
    ):
        raise CaptureError("cold startup did not recover exactly two active orders")
    if not any(
        r["type"] == "BeforeNativeRun"
        and r["cachedOrders"] == 0
        and r["submissionSignerInstalled"] is False
        for r in rows
    ):
        raise CaptureError("restart is not an empty-cache, no-submit host")
    pending = next(r for r in rows if r["type"] == "RealOpenOrdersBeforeColdStartup")[
        "orders"
    ]
    if {r["hash"] for r in pending} != hashes or any(
        r["amountFilled"] != "0" for r in pending
    ):
        raise CaptureError("real handover orders differ from this run")
    if sum(r["type"] == "StrategyBatchCancelOrders" for r in rows) != 1:
        raise CaptureError("expected one native batch cancel command")
    wallet = [
        r
        for r in capture["wallet"]["events"]
        if r["type"] == "orderCancelled" and r["orderHash"] in hashes
    ]
    ordering = []
    for order_hash in sorted(hashes):
        callbacks = [
            (i, r)
            for i, r in enumerate(rows)
            if r["phase"] == "recover"
            and r["type"] == "StrategyOrderCallback"
            and r["kind"] == "Canceled"
            and r["venueOrderId"] == order_hash
        ]
        reports = [
            (i, r)
            for i, r in enumerate(rows)
            if r["phase"] == "recover"
            and r["type"] == "RawOrderStatusReport"
            and r["report"]["venue_order_id"] == order_hash
            and r["report"]["order_status"] == "CANCELED"
        ]
        messages = [r for r in wallet if r["orderHash"] == order_hash]
        if len(callbacks) != 1 or not reports or not messages:
            raise CaptureError(
                "REST/WS cancellation did not yield exactly one native callback"
            )
        matching = [
            (i, r)
            for i, r in reports
            if any(
                int(r["report"]["ts_last"]) == int(w["timestamp"]) * 1_000_000
                for w in messages
            )
        ]
        if not matching:
            raise CaptureError(
                "Native cancellation input does not match the real wallet timestamp"
            )
        ordering.append(
            {
                "hash": order_hash,
                "callbackIndex": callbacks[0][0],
                "walletReportIndices": [i for i, _ in matching],
                "duplicateCanceledCallbacks": 0,
                "first": "native-callback-before-wallet-report"
                if callbacks[0][0] < matching[0][0]
                else "wallet-report-before-native-callback",
            }
        )
    account = [
        r
        for r in rows
        if r["phase"] == "recover" and r["type"] == "AccountStateApplied"
    ]
    locks = [Decimal(b["locked"]) for r in account for b in r["balances"]]
    if not locks or Decimal(2) not in locks or locks[-1] != 0 or max(locks) > 2:
        raise CaptureError("two-order account reservation/unlock not observed")
    (output / "batch_native_validation.json").write_text(
        json.dumps(
            {
                "source": "derived checks of actual Native report subscriptions, strategy callbacks, independent wallet and REST",
                "coldCacheOrdersBeforeRun": 0,
                "restoredActiveOrders": 2,
                "nativeBatchCommands": 1,
                "httpBatchRemoval": batch_http,
                "cancelOrdering": ordering,
                "finalLocked": "0",
                "newFills": 0,
            },
            indent=2,
        )
        + "\n"
    )


def run(case="auto"):
    if case not in CASES:
        raise CaptureError("unsupported resting case")
    request, identity = authenticated_reader()
    if complete_pages(request, "/v1/orders", status="OPEN"):
        raise CaptureError("existing open orders; refusing calibration")
    if RUN_CONFIG.exists() or PROXY_MANIFEST.exists():
        raise CaptureError(
            "one-run TOML/relay manifest already exists; refusing to overwrite"
        )
    if OUTPUT.exists():
        OUTPUT.rename(
            OUTPUT.with_name(
                "live_resting_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            )
        )
    OUTPUT.mkdir(parents=True)
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
        )
        descriptor = os.open(
            PROXY_MANIFEST, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        manifest_owned = True
        with os.fdopen(descriptor, "w") as stream:
            json.dump({"origin": recorder.origin, "case": case}, stream)
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
                        "-p",
                        "nautilus-predict",
                        "--features",
                        "execution",
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
            if recorder is not None:
                recorder.close()
            cleanup = final_cleanup(
                request, identity, recorder.submitted_hashes() if recorder else set()
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
            if "Predict transmitting LIMIT order:" in line
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
    print("Live calibration completed; JSON feedback and independent cleanup retained.")
    if case != "auto":
        destination = OUTPUT.with_name(
            f"live_resting_{case}_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        )
        OUTPUT.rename(destination)
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
    args = parser.parse_args()
    try:
        run(args.case)
    except Exception as error:  # noqa: BLE001 - never expose remote exception text or credentials
        print(
            "Calibration failed:",
            str(error) if type(error) is CaptureError else type(error).__name__,
        )
        raise SystemExit(1) from None
