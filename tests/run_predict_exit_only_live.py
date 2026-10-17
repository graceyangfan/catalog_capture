#!/usr/bin/env python3
"""Explicit exit-only cold-start calibration; never BUY or cancel on shutdown.

LGPL-3.0-or-later. Rust/Native own trading. This supervisor retains actual JSON
and preserves resting orders when interrupted, for operator-controlled handoff.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal as D
from pathlib import Path

from capture_predict_private_fixtures import ROOT, CaptureError
from predict_private_http_recorder import PrivateHttpRecorder, read_response_evidence
from predict_read_only_calibration import complete_pages
from run_predict_fill_live import build_native_host
from run_predict_resting_live import authenticated_reader


class ExitOnlyPolicy:
    def __init__(self, identity, token, quantity, fee_bps):
        self.identity = identity.lower()
        self.token = str(token)
        self.quantity = int(D(quantity) * 10**18)
        self.fee_bps = int(fee_bps)

    def __call__(self, data, count):
        try:
            order = data["order"]
            return (
                count < 3
                and order["side"] == 1
                and order["maker"].lower() == self.identity
                and order["signer"].lower() == self.identity
                and str(order["tokenId"]) == self.token
                and int(order["feeRateBps"]) == self.fee_bps
                and 0 < int(order["makerAmount"]) <= self.quantity
                and (
                    (data["strategy"] == "LIMIT" and data.get("isPostOnly") is True)
                    or (data["strategy"] == "MARKET" and data.get("slippageBps") == 100)
                )
            )
        except (KeyError, ValueError, TypeError):
            return False


def run(exit_immediately=False):
    command = build_native_host()
    command[1] = "fill_live::mainnet_exit_only_restart"
    request, identity = authenticated_reader()
    positions = complete_pages(request, "/v1/positions", includeLost="true")
    orders = complete_pages(request, "/v1/orders", status="OPEN")
    holdings = [p for p in positions if D(p["amount"]) > 0]
    if len(holdings) != 1:
        raise CaptureError("exit-only test requires one existing holding; no entry allowed")
    holding = holdings[0]
    market, outcome = holding["market"], holding["outcome"]
    if market["tradingStatus"] != "OPEN":
        raise CaptureError("held market is not trading")
    token = str(outcome["onChainId"])
    quantity = D(holding["amount"]) / D(10**18)
    if len(orders) > 1 or any(
        row["marketId"] != market["id"]
        or str(row["order"]["tokenId"]) != token
        or row["order"]["side"] != 1
        or row["order"]["maker"].lower() != identity.lower()
        for row in orders
    ):
        raise CaptureError("ambiguous existing orders; no write")
    # The prior entry evidence is admission context only; Native reloads fills/cost via reports.
    manifest = ROOT / "logs/predict-fill-proxy.json"
    previous = json.loads(manifest.read_text())
    candidate = previous["candidate"]
    if candidate["marketId"] != market["id"] or str(candidate["tokenId"]) != token:
        raise CaptureError("previous live test differs from discovered holding")
    name = "exit_only_restart_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output = ROOT / "crates/nautilus-predict/tests/fixtures/private_api" / name
    output.mkdir(exist_ok=False)
    (output / "before.json").write_text(json.dumps(read_response_evidence(
        {"orders": orders, "positions": positions, "orderWrites": 0}, [identity]), indent=2) + "\n")
    (output / "previous_manifest.json").write_text(json.dumps(previous, indent=2) + "\n")
    policy = ExitOnlyPolicy(identity, token, quantity, market["feeRateBps"])
    recorder = PrivateHttpRecorder(output / "http_responses.json", policy, record_reads=True)
    marker = ROOT / "logs/predict-exit-only-ready.json"
    stop = ROOT / "logs/predict-exit-only-stop.json"
    if marker.exists() or stop.exists():
        recorder.close()
        raise CaptureError("exit-only collector controls already exist")
    collector = node = None
    try:
        manifest.write_text(json.dumps({"origin": recorder.origin, "waitForStrategyExit": True,
            "exitOnly": True, "exitImmediately": exit_immediately, "candidate": candidate, "output": str(output)}, indent=2) + "\n")
        with (output / "collector.log").open("w") as log:
            collector = subprocess.Popen([sys.executable,
                str(ROOT / "tests/capture_predict_private_fixtures.py"), "--capture", "--calibrate",
                "--calibration-name", name + "_independent", "--calibration-seconds", "180",
                "--until-stop-file", str(stop), "--ready-file", str(marker)], cwd=ROOT,
                stdout=log, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 45
        while not marker.exists():
            if collector.poll() is not None or time.monotonic() > deadline:
                raise CaptureError("readonly collector did not acknowledge")
            time.sleep(0.2)
        print(json.dumps({"evidence": str(output.relative_to(ROOT)), "marketId": market["id"],
            "netShares": str(quantity), "existingExits": len(orders), "buyAllowed": False,
            "shutdownCancels": False}), flush=True)
        with (output / "node.log").open("w") as log:
            node = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
            while node.poll() is None:
                if collector.poll() is not None:
                    raise CaptureError("readonly collector stopped; preserving existing exit")
                time.sleep(0.5)
            if node.returncode:
                raise CaptureError("exit-only Native host stopped; inspect checkpoint")
    except KeyboardInterrupt:
        print("Operator stopped host; resting exits preserved, no extra BUY", flush=True)
    finally:
        if node and node.poll() is None:
            os.killpg(node.pid, signal.SIGINT)
            try:
                node.wait(timeout=12)
            except subprocess.TimeoutExpired:
                os.killpg(node.pid, signal.SIGKILL)
                node.wait()
        recorder.close()
        if collector:
            stop.write_text('{"exitOnlyNodeStopped":true}\n')
            try:
                collector.wait(timeout=60)
            except subprocess.TimeoutExpired:
                collector.terminate()
                collector.wait(timeout=5)
        for control in (marker, stop):
            control.unlink(missing_ok=True)
        after = {"orders": complete_pages(request, "/v1/orders", status="OPEN"),
            "positions": complete_pages(request, "/v1/positions", includeLost="true"),
            "buyAllowed": False, "shutdownCancels": False}
        (output / "after.json").write_text(json.dumps(read_response_evidence(after, [identity]), indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute-exit-only", action="store_true", required=True)
    parser.add_argument("--exit-immediately", action="store_true", help="operator requests cancel TP then taker SELL")
    args = parser.parse_args()
    try:
        run(args.exit_immediately)
    except Exception as error:
        print("Exit-only calibration stopped:", str(error) if type(error) is CaptureError else type(error).__name__, flush=True)
        raise SystemExit(1) from None
