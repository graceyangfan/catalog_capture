#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Explicit Rust mainnet calibration; Native strategy owns exits. LGPL-3.0-or-later."""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import ROUND_DOWN
from decimal import Decimal as D
from pathlib import Path

from capture_predict_private_fixtures import ROOT, CaptureError
from predict_fill_plan import plan_round_trip
from predict_private_http_recorder import PrivateHttpRecorder
from predict_read_only_calibration import complete_pages
from run_predict_resting_live import authenticated_reader, final_cleanup


class FillPolicy:
    def __init__(self, identity, candidate, market_entry=False):
        self.identity = identity.lower()
        self.candidate = candidate
        self.token = str(candidate["tokenId"])
        self.plan = candidate["plan"]
        self.market_entry = market_entry

    def terms(self, row):
        fields = row["order"]
        return (
            fields["maker"].lower() == self.identity
            and fields["signer"].lower() == self.identity
            and str(fields["tokenId"]) == self.token
        )

    def __call__(self, data, count):
        try:
            order = data["order"]
            if count >= 4 or not self.terms({"order": order}):
                return False
            if not re.fullmatch(r"0x[0-9a-fA-F]{64}", order["hash"]):
                return False
            if int(order["feeRateBps"]) != self.candidate["feeRateBps"]:
                return False
            if count == 0:
                if self.market_entry:
                    reference = D(data["pricePerShare"]) / 10**18
                    shares = int(D(self.plan["grossQuantity"]) * 10**18)
                    return (
                        data["strategy"] == "MARKET"
                        and data.get("isFillOrKill") is True
                        and data.get("isMinAmountOut") is False
                        and data.get("slippageBps") == "300"
                        and order["side"] == 0
                        and int(order["takerAmount"]) == shares
                        and 0 < reference <= D(".48")
                        and int(order["makerAmount"])
                        == int(reference * shares * D("1.03"))
                        and int(order["makerAmount"]) <= 150 * 10**16
                    )
                return (
                    data["strategy"] == "LIMIT"
                    and data.get("isPostOnly") is False
                    and order["side"] == 0
                    and int(order["makerAmount"])
                    == int(D(self.plan["entryCost"]) * 10**18)
                    and int(order["takerAmount"])
                    == int(D(self.plan["grossQuantity"]) * 10**18)
                )
            return (
                order["side"] == 1
                and 0
                < int(order["makerAmount"])
                <= int(D(self.plan["conservativeNetQuantity"]) * 10**18)
                and (
                    (data["strategy"] == "LIMIT" and data.get("isPostOnly") is True)
                    or (
                        data["strategy"] == "MARKET"
                        and data.get("slippageBps")
                        == ("300" if self.market_entry else "100")
                    )
                )
            )
        except (KeyError, ValueError, TypeError):
            return False


def discover(request):
    from concurrent.futures import ThreadPoolExecutor

    rows = request("GET", "/v1/markets", params={"first": 100, "status": "OPEN"})[
        "data"
    ]
    markets = [
        m
        for m in rows
        if m.get("tradingStatus") == "OPEN"
        and len(m.get("outcomes", [])) == 2
        and m.get("decimalPrecision", 9) <= 3
    ]

    def check(m):
        try:
            book = request("GET", f"/v1/markets/{m['id']}/orderbook")["data"]
            choices = []
            for outcome in (1, 2):
                bids = (
                    book["bids"]
                    if outcome == 1
                    else [[str(1 - D(str(p))), q] for p, q in book["asks"]]
                )
                asks = (
                    book["asks"]
                    if outcome == 1
                    else [[str(1 - D(str(p))), q] for p, q in book["bids"]]
                )
                plan = plan_round_trip(bids, asks, m["feeRateBps"])
                if plan:
                    token = next(
                        o["onChainId"]
                        for o in m["outcomes"]
                        if o["indexSet"] == outcome
                    )
                    choices.append(
                        {
                            "marketId": m["id"],
                            "title": m["title"],
                            "outcome": outcome,
                            "tokenId": token,
                            "feeRateBps": m["feeRateBps"],
                            "plan": plan,
                            "book": book,
                        }
                    )
            return choices
        except Exception:  # noqa: BLE001 - do not echo remote credentials
            return []

    with ThreadPoolExecutor(max_workers=4) as pool:
        choices = [c for group in pool.map(check, markets[:60]) for c in group]
    if not choices:
        raise CaptureError("no bounded round-trip candidate; no entry")
    return min(choices, key=lambda c: D(c["plan"]["estimatedRoundTripLoss"]))


def crypto_plan(bids, asks, fee_bps):
    """Small long-only round trip with SDK-compatible net shares and exit depth."""
    if not bids or not asks or not 0 <= fee_bps <= 200:
        return None
    bid, bid_qty = (D(str(x)) for x in bids[0])
    ask, ask_qty = (D(str(x)) for x in asks[0])
    if not 0 < bid < ask <= D(".48"):
        return None
    qty = (D("1.40") / ask).quantize(D(".1"), rounding=ROUND_DOWN)
    cost, net = qty * ask, qty * (1 - D(fee_bps) / 10000)
    proceeds = bid * net * (1 - D(fee_bps) / 10000)
    stress_floor = (bid * net - D(".15")) * D(".97")
    if (
        cost < 1
        or cost * D("1.03") > D("1.50")
        or bid_qty < net * 3
        or ask_qty < qty * 3
        or cost - proceeds > D(".10")
        or stress_floor < D("1.05")
        or len(str(net.normalize()).replace(".", "").lstrip("0")) > 5
    ):
        return None
    return {
        k: str(v)
        for k, v in {
            "entryPrice": ask,
            "grossQuantity": qty,
            "entryCost": cost,
            "conservativeNetQuantity": net,
            "bid": bid,
            "ask": ask,
            "immediateNetProceeds": proceeds,
            "estimatedRoundTripLoss": cost - proceeds,
            "netExitFloor": stress_floor * (1 - D(fee_bps) / 10000),
            "maxLoss": D(".15"),
        }.items()
    }


def discover_crypto(request):
    matches = request("GET", "/v1/orders/matches", params={"first": 100})["data"]
    now = datetime.now(UTC)
    recent = {}
    for row in matches:
        m = row["market"]
        if m.get("marketVariant") != "CRYPTO_UP_DOWN":
            continue
        if (now - datetime.fromisoformat(row["executedAt"])).total_seconds() <= 60:
            recent.setdefault(m["id"], []).append(row["executedAt"])
    choices = []
    for mid, executions in recent.items():
        m = request("GET", f"/v1/markets/{mid}")["data"]
        schedule = m.get("rewards", {}).get("schedule", [])
        ends = [datetime.fromisoformat(r["endsAt"]) for r in schedule]
        if (
            m["tradingStatus"] != "OPEN"
            or m.get("resolution") is not None
            or m.get("makerFeeBps", 0) != 0
            or not ends
            or (max(ends) - now).total_seconds() < 120
        ):
            continue
        book = request("GET", f"/v1/markets/{mid}/orderbook")["data"]
        for outcome in (1, 2):
            bids = (
                book["bids"]
                if outcome == 1
                else [[str(1 - D(str(p))), q] for p, q in book["asks"]]
            )
            asks = (
                book["asks"]
                if outcome == 1
                else [[str(1 - D(str(p))), q] for p, q in book["bids"]]
            )
            plan = crypto_plan(bids, asks, m["feeRateBps"])
            if plan:
                choices.append(
                    {
                        "marketId": mid,
                        "title": m["title"],
                        "outcome": outcome,
                        "tokenId": next(
                            o["onChainId"]
                            for o in m["outcomes"]
                            if o["indexSet"] == outcome
                        ),
                        "feeRateBps": m["feeRateBps"],
                        "plan": plan,
                        "book": book,
                        "recentMatchCount": len(executions),
                        "lastMatchAt": max(executions),
                        "expiresAt": max(ends).isoformat(),
                    }
                )
    if not choices:
        raise CaptureError(
            "no active crypto market meets fee, depth and exit budget bounds"
        )
    return min(
        choices,
        key=lambda c: (-c["recentMatchCount"], D(c["plan"]["estimatedRoundTripLoss"])),
    )


def historical_inventory(positions):
    return sorted(
        (str(p["market"]["id"]), str(p["outcome"]["onChainId"]), str(p["amount"]))
        for p in positions
    )


def native_account_matches(native, calibration):
    native_cash = (
        next(
            (
                D(b["total"].split()[0])
                for b in native.get("balances", [])
                if b["currency"] == "USDT"
            ),
            None,
        )
        if native
        else None
    )
    chain_cash = D(calibration["after"]["cashWei"]) / D(10**18) if calibration else None
    return (
        native_cash is not None
        and chain_cash is not None
        and abs(native_cash - chain_cash) <= D("0.00000001")
    )


def native_pnl_matches(native, calibration):
    if not native or not calibration:
        return False
    rows = [
        r
        for r in native.get("nativeFeedback", [])
        if r.get("type") == "NativePositionApplied"
        and r.get("quantity") == "0"
        and r.get("realizedPnl")
    ]
    if not rows:
        return False
    reported = D(rows[-1]["realizedPnl"].split()[0])
    cash_delta = (
        D(calibration["after"]["cashWei"]) - D(calibration["before"]["cashWei"])
    ) / D(10**18)
    return abs(reported - cash_delta) <= D("0.00000001")


def cycle_economics(native, calibration, order_hashes):
    """Validate this flat-to-flat cycle's actual collateral flow, not quote PnL equality."""
    if not native or not calibration or not order_hashes:
        return {"matchesCash": False}
    owned = {h.lower() for h in order_hashes}
    seen, flow = {}, D(0)
    for row in calibration["comparisons"]:
        key = row["settlementId"], row["orderHash"].lower()
        if key[1] not in owned:
            continue
        if key in seen:
            if seen[key] != row:
                raise CaptureError("contradictory settlement evidence")
            continue
        seen[key] = row
        collateral, fee = D(row["collateralWei"]), D(row["feeWei"])
        if row["feeType"] not in ("SHARES", "COLLATERAL"):
            raise CaptureError("unsupported settlement fee asset")
        if row["side"] == "BUY":
            flow -= collateral
        elif row["side"] == "SELL" and row["feeType"] == "COLLATERAL":
            flow += collateral
        else:
            raise CaptureError("unsupported settlement direction")
        if row["feeType"] == "COLLATERAL":
            flow -= fee
    flow /= D(10**18)
    cash_delta = (
        D(calibration["after"]["cashWei"]) - D(calibration["before"]["cashWei"])
    ) / D(10**18)
    native_flow = (
        D(native["cashFlowUSDT"]) if native.get("cashFlowUSDT") is not None else None
    )
    closed = [
        r
        for r in native.get("nativeFeedback", [])
        if r.get("type") == "NativePositionApplied" and D(r.get("quantity", "1")) == 0
    ]
    return {
        "cashPnlUSDT": str(cash_delta),
        "settlementCashPnlUSDT": str(flow),
        "nativeFillCashPnlUSDT": str(native_flow) if native_flow is not None else None,
        "nativeQuoteRealizedPnl": closed[-1].get("realizedPnl") if closed else None,
        "settlements": len(seen),
        "matchesCash": bool(seen)
        and native_flow is not None
        and abs(flow - cash_delta) <= D("0.00000001")
        and abs(native_flow - cash_delta) <= D("0.00000001"),
    }


def wait_for_strategy_exit(node, collector, pause=time.sleep):
    """No holding deadline; a failed independent observer ends the test explicitly."""
    while node.poll() is None:
        if collector.poll() is not None:
            raise CaptureError("independent observer exited; inspect durable evidence")
        pause(0.5)
    return node.returncode


def history_settled_before_window(history, now):
    return all(
        (now - datetime.fromisoformat(row["executedAt"])).total_seconds() > 60
        for row in history
    )


def build_native_host():
    """Compile before authentication; supervise the actual Rust process, not Cargo."""
    result = subprocess.run(
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
            "--no-run",
            "--message-format=json",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise CaptureError(
            "Native host compilation failed; run the offline Cargo check"
        )
    executables = set()
    for line in result.stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("target", {}).get("name") == "private_api_live"
            and message.get("executable")
        ):
            executables.add(message["executable"])
    if len(executables) != 1 or not Path(next(iter(executables))).is_file():
        raise CaptureError("missing or ambiguous Native host executable")
    return [
        next(iter(executables)),
        "fill_live::mainnet_wait_for_strategy_exit",
        "--ignored",
        "--exact",
        "--nocapture",
    ]


def run(wait_for_exit=False, market_entry=False):
    native_command = build_native_host() if wait_for_exit else None
    continuous_observer = wait_for_exit or market_entry
    request, identity = authenticated_reader()
    positions_before = complete_pages(request, "/v1/positions", includeLost="true")
    if complete_pages(request, "/v1/orders", status="OPEN") or (
        any(
            p["market"].get("status") != "RESOLVED"
            or p["outcome"].get("status") != "LOST"
            for p in positions_before
        )
        if market_entry
        else bool(positions_before)
    ):
        raise CaptureError("calibration requires an independently flat account")
    history = complete_pages(request, "/v1/orders/matches", signerAddress=identity)
    if not history_settled_before_window(history, datetime.now(UTC)):
        raise CaptureError(
            "prior settlements overlap the native one-minute calibration window; no entry"
        )
    candidate = discover_crypto(request) if market_entry else discover(request)
    name = "live_fill_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output = (
        ROOT / "data/predict-validation"
        if market_entry
        else ROOT / "crates/nautilus-predict/tests/fixtures/private_api"
    ) / name
    output.mkdir(exist_ok=False)
    (output / "admission.json").write_text(
        json.dumps(
            {
                "source": "actual read-only discovery; preflight, not a profit forecast",
                "candidate": candidate,
            },
            indent=2,
        )
        + "\n"
    )
    config = ROOT / "env/predict-fill-calibration.toml"
    manifest = ROOT / "logs/predict-fill-proxy.json"
    marker = ROOT / "logs/predict-fill-wallet-ready.json"
    stop_marker = ROOT / "logs/predict-fill-wallet-stop.json"
    if any(p.exists() for p in (config, manifest, marker, stop_marker)):
        raise CaptureError("existing run control files; refusing overwrite")
    source = (ROOT / "env/predict-execution.toml").read_text()
    for key, value in [
        ("allow_order_writes", "true"),
        ("require_post_only", "false"),
        ("max_order_usdt", '"1.50"'),
        ("max_total_usdt", '"1.50"'),
    ]:
        source, count = re.subn(
            r"(?m)^" + key + r"\s*=.*$", key + " = " + value, source
        )
        if count != 1:
            raise CaptureError("missing or ambiguous control field")
    capture_name = name + "_independent"
    recorder = collector = node = None
    status = None
    failure = None
    policy = FillPolicy(identity, candidate, market_entry)
    owned = set()
    try:
        with os.fdopen(
            os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as f:
            owned.add(config)
            f.write(source)
        recorder = PrivateHttpRecorder(
            output / "http_responses.json", policy, record_reads=continuous_observer
        )
        with os.fdopen(
            os.open(manifest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as f:
            owned.add(manifest)
            json.dump(
                {
                    "origin": recorder.origin,
                    "waitForStrategyExit": wait_for_exit,
                    "marketEntry": market_entry,
                    "candidate": candidate,
                    "output": str(output),
                },
                f,
            )
        # Separate named read-only collector: preserve all earlier fixtures.
        with (output / "collector.log").open("w") as log:
            collector = subprocess.Popen(
                [
                    sys.executable,
                    str(ROOT / "tests/capture_predict_private_fixtures.py"),
                    "--capture",
                    "--calibrate",
                    "--calibration-name",
                    capture_name,
                    "--calibration-seconds",
                    "180",
                    *(
                        ["--calibration-market-id", str(candidate["marketId"])]
                        if market_entry
                        else []
                    ),
                    *(
                        ["--until-stop-file", str(stop_marker)]
                        if continuous_observer
                        else []
                    ),
                    "--ready-file",
                    str(marker),
                ],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.DEVNULL,
            )
        owned.update((marker, stop_marker))
        deadline = time.monotonic() + 45
        while not marker.exists():
            if collector.poll() is not None or time.monotonic() > deadline:
                raise CaptureError("collector not acknowledged; no entry")
            time.sleep(0.2)
        print(
            json.dumps(
                {
                    "marketId": candidate["marketId"],
                    "outcome": candidate["outcome"],
                    "entryCost": candidate["plan"]["entryCost"],
                    "netExitFloor": candidate["plan"]["netExitFloor"],
                    "maxBuyUsdt": "1.50",
                    "maxLossTrigger": "0.15",
                    "holdingDeadlineSeconds": 20
                    if market_entry
                    else (None if wait_for_exit else 150),
                    "entryWindowSeconds": 20 if market_entry else 60,
                    "exitOwner": "Rust strategy: maker profit or taker loss threshold",
                }
            ),
            flush=True,
        )
        with (output / "node.log").open("w") as log:
            node = subprocess.Popen(
                native_command
                or [
                    "cargo",
                    "test",
                    "-p",
                    "nautilus-predict",
                    "--features",
                    "execution",
                    "--locked",
                    "--test",
                    "private_api_live",
                    "fill_live::mainnet_bounded_round_trip",
                    "--",
                    "--ignored",
                    "--exact",
                    "--nocapture",
                ],
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            status = (
                wait_for_strategy_exit(node, collector)
                if wait_for_exit
                else node.wait(timeout=200)
            )
    except (CaptureError, subprocess.TimeoutExpired) as error:
        failure = (
            str(error) if type(error) is CaptureError else "bounded node wait expired"
        )
        status = 1
    except KeyboardInterrupt:
        failure = "manual stop; remaining inventory is not automatically liquidated"
        # Native handles SIGINT and drains its strategy cancellations. The final
        # read-only inventory check below must still report any remaining holding.
        status = 130
    finally:
        if node and node.poll() is None:
            os.killpg(node.pid, signal.SIGINT)
            try:
                node.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(node.pid, signal.SIGKILL)
                node.wait()
        try:
            if recorder:
                recorder.close()
            cleanup = final_cleanup(
                request,
                identity,
                recorder.submitted_hashes() if recorder else set(),
                policy.terms,
            )
            (output / "supervisor_cleanup.json").write_text(
                json.dumps(cleanup, indent=2) + "\n"
            )
            print(
                "Independent final OPEN count:",
                cleanup["verifiedOpenOrders"],
                flush=True,
            )
        finally:
            if collector:
                if continuous_observer:
                    stop_marker.write_text('{"nodeStopped":true}\n')
                try:
                    collector.wait(timeout=60 if continuous_observer else 205)
                except subprocess.TimeoutExpired:
                    collector.terminate()
                    collector.wait(timeout=5)
            for control in owned:
                control.unlink(missing_ok=True)
    orders = complete_pages(request, "/v1/orders", status="OPEN")
    positions = complete_pages(request, "/v1/positions", includeLost="true")
    independent_dir = (
        ROOT / "crates/nautilus-predict/tests/fixtures/private_api" / capture_name
    )
    if market_entry and independent_dir.exists():
        independent_dir = independent_dir.rename(output / "independent")
    independent = independent_dir / "calibration.json"
    calibration = json.loads(independent.read_text()) if independent.exists() else None
    native = (
        json.loads((output / "native_roundtrip.json").read_text())
        if (output / "native_roundtrip.json").exists()
        else None
    )
    account_matches = native_account_matches(native, calibration)
    economics = (
        cycle_economics(
            native, calibration, recorder.submitted_hashes() if recorder else set()
        )
        if wait_for_exit or market_entry
        else None
    )
    pnl_matches = (
        economics["matchesCash"]
        if economics
        else native_pnl_matches(native, calibration)
    )
    summary = {
        "source": "actual mainnet final checks",
        "nodeExitCode": status,
        "supervisorFailure": failure,
        "waitForStrategyExit": wait_for_exit,
        "economics": economics,
        "openOrders": len(orders),
        "positions": len(positions),
        "historicalInventoryUnchanged": historical_inventory(positions)
        == historical_inventory(positions_before),
        "marketEntry": market_entry,
        "independentSummary": calibration["summary"] if calibration else None,
        "nativeFeedbackAvailable": native is not None,
        "nativeAccountMatchesChain": account_matches,
        "nativeCyclePnlMatchesCash": pnl_matches,
        "complete": status == 0
        and account_matches
        and pnl_matches
        and collector.returncode == 0
        and not orders
        and (
            historical_inventory(positions) == historical_inventory(positions_before)
            if market_entry
            else not positions
        )
        and calibration is not None,
    }
    (output / "final_verification.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    print(json.dumps(summary), flush=True)
    print("Evidence:", output.relative_to(ROOT), flush=True)
    if not summary["complete"]:
        raise CaptureError(
            "round trip incomplete; inspect retained JSON, own order cleanup ran"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--execute-bounded-round-trip", action="store_true")
    mode.add_argument("--execute-crypto-market-round-trip", action="store_true")
    mode.add_argument(
        "--execute-wait-for-strategy-exit",
        action="store_true",
        help="real orders: no holding timeout; explicit approval required",
    )
    args = parser.parse_args()
    try:
        run(
            wait_for_exit=args.execute_wait_for_strategy_exit,
            market_entry=args.execute_crypto_market_round_trip,
        )
    except Exception as error:  # noqa: BLE001 - remote exceptions can contain credentials
        print(
            "Calibration failed:",
            str(error) if type(error) is CaptureError else type(error).__name__,
        )
        raise SystemExit(1) from None
