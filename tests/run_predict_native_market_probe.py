#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Explicit bounded Native strategy MARKET probe; --submit required. LGPL-3.0.

Actual Rust Strategy -> Risk -> Predict execution. Independent official SDK
oracle blocks forwarding unless amounts, hash, signature and metadata agree.
Default: two BUY .80 x 1.25, 1 USDT each. --parameter-matrix: five cases covering
0/100/200 bps, FOK omission and one-USDT quote sizing, <=1.02 USDT each. Ask must
stay .83-.90 and 2-10 cents above the final signed ratio. Cancel on Native
Accepted, with scoped fallback removal. No approvals, chain writes or SELLs.
"""

import argparse
import json
import os
import re
import signal
import subprocess
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal as D

from capture_predict_private_fixtures import ROOT, CaptureError
from predict_private_http_recorder import PrivateHttpRecorder, read_response_evidence
from predict_read_only_calibration import observe_wallet
from predict_sdk.types import Book, MarketHelperInput, Order, Side, SignatureType
from probe_predict_order_capabilities import authenticated_context
from run_predict_resting_live import final_cleanup


def probe_cases(matrix=False):
    baseline = {"name": "base_fok_zero", "bps": 0, "fok": True, "quote": False}
    if not matrix:
        return [baseline, {**baseline, "name": "base_gtc_zero", "fok": False}]
    return [
        baseline,
        {**baseline, "name": "base_fok_100bps", "bps": 100},
        {**baseline, "name": "base_fok_200bps", "bps": 200},
        {**baseline, "name": "base_gtc_100bps", "bps": 100, "fok": False},
        {**baseline, "name": "quote_fok_zero", "quote": True},
    ]


def signed_gap_is_safe(maker, taker, ask):
    return taker > 0 and D(".02") <= ask - D(maker) / D(taker) <= D(".10")


def run(submit, matrix=False):
    if not submit:
        raise CaptureError(
            "explicit --submit required; no authentication or order without it"
        )
    config = ROOT / "env/predict-native-market-probe.toml"
    manifest = ROOT / "logs/predict-native-market-probe.json"
    if config.exists() or manifest.exists():
        raise CaptureError("refusing to overwrite an existing run profile")
    _, private, identity, owner, builder, session, read = authenticated_context()
    market = read("/v1/markets/2508071")["data"]
    if market["tradingStatus"] != "OPEN" or market.get("resolution") is not None:
        raise CaptureError("selected market is not unresolved/open")
    token = str(next(o["onChainId"] for o in market["outcomes"] if o["indexSet"] == 1))
    output = (
        ROOT
        / "data/predict-validation"
        / ("native_market_strategy_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    output.mkdir(parents=True)
    secrets = [
        private["private_key"],
        identity,
        owner.address,
        session.headers["x-api-key"],
        session.headers["Authorization"].removeprefix("Bearer "),
    ]
    oracle = []
    cases = probe_cases(matrix)
    cap = D("1.02") if matrix else D(1)

    def validate(data, count):
        entry = {
            "index": count,
            "passed": False,
            "validationStartedAtUtc": datetime.now(UTC).isoformat(),
        }
        try:
            case = cases[count]
            order = data["order"]
            fresh = read("/v1/markets/2508071")["data"]
            book = read("/v1/markets/2508071/orderbook")["data"]
            ask = D(str(book["asks"][0][0]))
            expected = builder.get_market_order_amounts(
                MarketHelperInput(
                    side=Side.BUY, quantity_wei=125 * 10**16, slippage_bps=case["bps"]
                ),
                Book(
                    market_id=market["id"],
                    update_timestamp_ms=0,
                    asks=[(".80", "1.25")],
                    bids=[],
                ),
            )
            live_book_amounts = builder.get_market_order_amounts(
                MarketHelperInput(
                    side=Side.BUY, quantity_wei=125 * 10**16, slippage_bps=case["bps"]
                ),
                Book(
                    market_id=market["id"],
                    update_timestamp_ms=book["updateTimestampMs"],
                    asks=book["asks"],
                    bids=book["bids"],
                ),
            )
            fields = {
                "salt": order["salt"],
                "maker": order["maker"],
                "signer": order["signer"],
                "taker": order["taker"],
                "token_id": order["tokenId"],
                "maker_amount": order["makerAmount"],
                "taker_amount": order["takerAmount"],
                "expiration": str(order["expiration"]),
                "nonce": order["nonce"],
                "fee_rate_bps": order["feeRateBps"],
                "side": Side(order["side"]),
                "signature_type": SignatureType(order["signatureType"]),
            }
            typed = builder.build_typed_data(
                Order(**fields),
                is_neg_risk=market["isNegRisk"],
                is_yield_bearing=market["isYieldBearing"],
            )
            sdk_hash = builder.build_typed_data_hash(typed)
            sdk_signature = builder.sign_typed_data_order(typed).signature
            checks = {
                "bounded_count": count < len(cases),
                "market_open": fresh["tradingStatus"] == "OPEN"
                and fresh.get("resolution") is None,
                "market_identity": book["marketId"] == market["id"],
                "price_gap": D(".83") <= ask <= D(".90")
                and signed_gap_is_safe(
                    int(order["makerAmount"]), int(order["takerAmount"]), ask
                ),
                "account": order["maker"].lower() == identity.lower()
                and order["signer"].lower() == identity.lower(),
                "outcome": str(order["tokenId"]) == token,
                "side": order["side"] == 0,
                "fee": str(order["feeRateBps"]) == str(fresh["feeRateBps"]),
                "amounts": int(order["makerAmount"]) == expected.maker_amount
                and 0 < expected.maker_amount <= int(cap * 10**18)
                and int(order["takerAmount"]) == expected.taker_amount == 125 * 10**16,
                "strategy": data["strategy"] == "MARKET",
                "reference": int(data["pricePerShare"]) == expected.price_per_share,
                "slippage": data.get("slippageBps") == str(case["bps"]),
                "minimum_out": data.get("isMinAmountOut") is False,
                "fok": data.get("isFillOrKill") is True
                if case["fok"]
                else "isFillOrKill" not in data,
                "no_post_only": "isPostOnly" not in data,
                "expiration": time.time()
                < int(order["expiration"])
                <= time.time() + 310,
                "hash": sdk_hash.lower() == order["hash"].lower(),
                "signature": sdk_signature.lower().removeprefix("0x")
                == order["signature"].lower().removeprefix("0x"),
            }
            entry.update(
                checks=checks,
                passed=all(checks.values()),
                observedAsk=str(ask),
                case=case,
                signedBound=str(D(order["makerAmount"]) / D(order["takerAmount"])),
                officialHelperFromRealBook={
                    "pricePerShare": str(live_book_amounts.price_per_share),
                    "lastPrice": str(live_book_amounts.last_price),
                    "makerAmount": str(live_book_amounts.maker_amount),
                    "takerAmount": str(live_book_amounts.taker_amount),
                    "amount": str(live_book_amounts.amount),
                    "note": "Read-only comparison; not substituted into intentionally bounded probe",
                },
                marketFlags={
                    key: fresh[key]
                    for key in ("isNegRisk", "isYieldBearing", "feeRateBps")
                },
                verifyingContract=typed.domain["verifyingContract"],
                request=read_response_evidence({"data": data}, secrets),
            )
        except Exception as error:  # noqa: BLE001 - no sensitive SDK exception text
            entry["error_type"] = type(error).__name__
        entry["validationCompletedAtUtc"] = datetime.now(UTC).isoformat()
        oracle.append(entry)
        (output / "sdk_comparison.json").write_text(json.dumps(oracle, indent=2) + "\n")
        return entry["passed"]

    def request(method, path, **kwargs):
        if method == "GET":
            return read(path, **kwargs)
        if method != "POST" or path != "/v1/orders/remove":
            raise CaptureError("supervisor can only remove owned orders")
        response = session.post(
            "https://api.predict.fun" + path,
            timeout=15,
            allow_redirects=False,
            **kwargs,
        )
        if response.status_code != 200:
            raise CaptureError("supervisor removal failed")
        return response.json()

    def owns(row):
        o = row["order"]
        return (
            o["maker"].lower() == identity.lower()
            and o["signer"].lower() == identity.lower()
            and str(o["tokenId"]) == token
            and o["side"] == 0
            and 0 < int(o["makerAmount"]) <= int(cap * 10**18)
            and int(o["takerAmount"]) == 125 * 10**16
        )

    ack, stop = threading.Event(), threading.Event()
    wallet, observer = [], {}

    def watch():
        try:
            events, beats, seconds = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=None,
                on_subscribed=ack.set,
                stop_requested=stop.is_set,
                on_wallet_event=lambda e: wallet.append(
                    {**e, "observedAtUtc": datetime.now(UTC).isoformat()}
                ),
            )
            observer.update(heartbeats=beats, seconds=seconds, received=len(events))
        except Exception as error:  # noqa: BLE001 - safe diagnostics only
            observer["error_type"] = type(error).__name__

    recorder = child = None
    thread = threading.Thread(target=watch, daemon=True)
    owned_files = []
    summary = {
        "marketId": market["id"],
        "price": ".80",
        "quantity": "1.25",
        "maxSignedUSDTPerOrder": str(cap),
        "maxOrders": len(cases),
        "parameterMatrix": matrix,
    }
    try:
        source = (ROOT / "env/predict-execution.toml").read_text()
        for name, value in [
            ("allow_order_writes", "true"),
            ("require_post_only", "false"),
            ("max_order_usdt", '"' + str(cap) + '"'),
            ("max_total_usdt", '"' + str(cap * len(cases)) + '"'),
        ]:
            source, count = re.subn(
                r"(?m)^" + name + r"\s*=.*$", name + " = " + value, source
            )
            if count != 1:
                raise CaptureError("one-run profile field missing or ambiguous")
        with os.fdopen(
            os.open(config, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as stream:
            owned_files.append(config)
            stream.write(source)
        recorder = PrivateHttpRecorder(
            output / "http_responses.json",
            submission_policy=validate,
            record_reads=True,
            measure_latency=True,
        )
        with os.fdopen(
            os.open(manifest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as stream:
            owned_files.append(manifest)
            json.dump(
                {
                    "marketId": market["id"],
                    "origin": recorder.origin,
                    "output": str(output),
                    "parameterMatrix": matrix,
                },
                stream,
            )
        thread.start()
        if not ack.wait(25):
            raise CaptureError("independent wallet observer not acknowledged")
        with (output / "node.log").open("w") as log:
            child = subprocess.Popen(
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
                    "market_probe_live::mainnet_native_market_parameters",
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
            deadline = time.monotonic() + 80
            checked = set()
            while child.poll() is None:
                if time.monotonic() > deadline:
                    raise CaptureError("Native probe deadline")
                with recorder.lock:
                    attempts = [dict(e) for e in recorder.events if "requestTerms" in e]
                for attempt in attempts:
                    order_hash = attempt["requestTerms"]["hash"]
                    age = (
                        datetime.now(UTC)
                        - datetime.fromisoformat(attempt["observedAtUtc"])
                    ).total_seconds()
                    if order_hash not in checked and age > 6:
                        summary.setdefault("watchdogCleanup", []).append(
                            final_cleanup(
                                request, identity, [order_hash], verify_terms=owns
                            )
                        )
                        checked.add(order_hash)
                time.sleep(0.2)
            summary["nodeExitCode"] = child.returncode
    except Exception as error:  # noqa: BLE001 - secret-safe summary, cleanup always runs
        summary["error_type"] = type(error).__name__
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        if recorder is not None:
            recorder.close()
            hashes = recorder.submitted_hashes()
            summary["cleanup"] = final_cleanup(
                request, identity, hashes, verify_terms=owns
            )
            summary["finalOrders"] = []
            for order_hash in hashes:
                row = read("/v1/orders/" + order_hash)["data"]
                summary["finalOrders"].append(
                    {
                        "hash": order_hash,
                        "status": row["status"],
                        "amountFilled": row.get("amountFilled"),
                    }
                )
            stop.set()
            thread.join(timeout=12)
            summary["walletEvents"] = read_response_evidence(
                [e for e in wallet if e.get("orderHash", "").lower() in hashes], secrets
            )
        else:
            stop.set()
        summary["walletObserver"] = observer
        for path in owned_files:
            path.unlink(missing_ok=True)
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"evidence": str(output), "summary": summary}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--parameter-matrix", action="store_true")
    try:
        args = parser.parse_args()
        run(args.submit, args.parameter_matrix)
    except Exception as error:  # noqa: BLE001 - no sensitive traceback
        raise SystemExit("Native probe failed: " + type(error).__name__) from None
