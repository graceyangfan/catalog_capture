#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""LGPL-3.0-or-later. Two SDK controls for the bounded Native MARKET probe.

Requires --submit and a completed Native evidence directory. Independently builds
BUY .80 x 1.25 using official SDK; cancels on wallet orderAccepted, not HTTP 201.
Ask must remain .83-.90. At most two orders, one USDT each. No chain writes.
--minimum-out-controls requires the five-case Native matrix and tests the
official SDK minimum-out mode at 0/100 bps, FOK true in both cases.
"""

import argparse
import json
import threading
from datetime import UTC, datetime
from decimal import Decimal

import requests
from capture_predict_private_fixtures import ROOT, CaptureError
from predict_private_http_recorder import read_response_evidence, receipt_evidence
from predict_read_only_calibration import observe_wallet
from predict_sdk.types import Book, BuildOrderInput, MarketHelperInput, Side
from probe_predict_order_capabilities import authenticated_context
from run_predict_resting_live import final_cleanup


def run(submit, native_evidence, minimum_out_controls=False):
    if not submit:
        raise CaptureError("explicit --submit required")
    native = native_evidence.resolve()
    if native.parent != (ROOT / "data/predict-validation").resolve():
        raise CaptureError("expected a local Native probe evidence directory")
    source = json.loads((native / "summary.json").read_text())
    oracle = json.loads((native / "sdk_comparison.json").read_text())
    if not (
        source["marketId"] == 2508071
        and source["price"] == ".80"
        and source["quantity"] == "1.25"
        and source["nodeExitCode"] == 0
        and len(oracle) == (5 if minimum_out_controls else 2)
        and all(row["passed"] for row in oracle)
        and all(row["amountFilled"] == "0" for row in source["finalOrders"])
    ):
        raise CaptureError("Native control was incomplete or outside fixed bounds")
    _, private, identity, owner, builder, session, read = authenticated_context()
    secrets = [
        private["private_key"],
        identity,
        owner.address,
        session.headers["x-api-key"],
        session.headers["Authorization"],
    ]
    output = (
        ROOT
        / "data/predict-validation"
        / ("sdk_market_accept_cancel_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    output.mkdir()
    result = {
        "nativeEvidence": str(native),
        "marketId": 2508071,
        "cancelTrigger": "wallet orderAccepted",
        "minimumOutControls": minimum_out_controls,
        "cases": [],
    }
    lock = threading.Lock()
    known, cancel_threads = {}, []
    ack, stop = threading.Event(), threading.Event()

    def save():
        with lock:
            pending = output / "summary.pending"
            pending.write_text(json.dumps(result, indent=2) + "\n")
            pending.replace(output / "summary.json")

    def remove_on_accept(case, order_id):
        case["cancelStartedAtUtc"] = datetime.now(UTC).isoformat()
        try:
            with requests.Session() as remover:
                remover.headers.update(session.headers)
                reply = remover.post(
                    "https://api.predict.fun/v1/orders/remove",
                    json={"data": {"ids": [order_id]}},
                    timeout=15,
                    allow_redirects=False,
                )
                case["cancelHttpStatus"] = reply.status_code
                case["cancelReceipt"] = receipt_evidence(reply.json(), secrets)
        except Exception as error:  # noqa: BLE001 - preserve cleanup without exposing credentials
            case["cancelErrorType"] = type(error).__name__
        case["cancelCompletedAtUtc"] = datetime.now(UTC).isoformat()
        save()

    def wallet_event(event):
        order_hash = event.get("orderHash", "").lower()
        if order_hash not in known:
            return
        case, terminal = known[order_hash]
        observed = datetime.now(UTC).isoformat()
        case["walletEvents"].append(
            read_response_evidence({**event, "observedAtUtc": observed}, secrets)
        )
        if event.get("type") == "orderAccepted" and not case.get("cancelRequested"):
            order_id = str(event.get("orderId", ""))
            if not order_id.isdecimal():
                case["cancelErrorType"] = "MissingOrderId"
            else:
                case["cancelRequested"] = True
                worker = threading.Thread(
                    target=remove_on_accept, args=(case, order_id)
                )
                cancel_threads.append(worker)
                worker.start()
        if event.get("type") in {"orderNotAccepted", "orderCancelled", "orderExpired"}:
            terminal.set()
        save()

    def watch():
        try:
            events, beats, seconds = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=None,
                on_subscribed=ack.set,
                stop_requested=stop.is_set,
                on_wallet_event=wallet_event,
            )
            result["walletObserver"] = {
                "events": len(events),
                "heartbeats": beats,
                "seconds": seconds,
            }
        except Exception as error:  # noqa: BLE001 - preserve cleanup without exposing credentials
            result["walletObserverErrorType"] = type(error).__name__

    def request(method, path, **kwargs):
        if method == "GET":
            return read(path, **kwargs)
        if method != "POST" or path != "/v1/orders/remove":
            raise CaptureError("only owned-order cleanup is permitted")
        reply = session.post(
            "https://api.predict.fun" + path,
            timeout=15,
            allow_redirects=False,
            **kwargs,
        )
        if reply.status_code != 200:
            raise CaptureError("cleanup HTTP failure")
        return reply.json()

    def owns(row):
        order = row["order"]
        return (
            order["hash"].lower() in known
            and order["maker"].lower() == identity.lower()
            and order["signer"].lower() == identity.lower()
            and int(order["makerAmount"]) == 10**18
            and int(order["takerAmount"]) in {125 * 10**16, 12375 * 10**14}
            and order["side"] == 0
        )

    observer = threading.Thread(target=watch, daemon=True)
    observer.start()
    try:
        if not ack.wait(25):
            raise CaptureError("wallet observer was not acknowledged")
        for index in range(2):
            market = read("/v1/markets/2508071")["data"]
            book = read("/v1/markets/2508071/orderbook")["data"]
            ask = min(Decimal(str(p)) for p, _ in book["asks"])
            if not (
                market["tradingStatus"] == "OPEN"
                and market.get("resolution") is None
                and market["isNegRisk"] is False
                and market["isYieldBearing"] is True
                and book["marketId"] == 2508071
                and Decimal(".83") <= ask <= Decimal(".90")
            ):
                raise CaptureError("market moved outside the .03-.10 price gap")
            token = str(
                next(o["onChainId"] for o in market["outcomes"] if o["indexSet"] == 1)
            )
            amounts = builder.get_market_order_amounts(
                MarketHelperInput(
                    side=Side.BUY,
                    quantity_wei=125 * 10**16,
                    slippage_bps=100 if minimum_out_controls and index == 1 else 0,
                    is_min_amount_out=minimum_out_controls,
                ),
                Book(
                    market_id=2508071,
                    update_timestamp_ms=0,
                    asks=[(".80", "1.25")],
                    bids=[],
                ),
            )
            expected_taker = (
                12375 * 10**14 if minimum_out_controls and index == 1 else 125 * 10**16
            )
            if (amounts.maker_amount, amounts.taker_amount) != (10**18, expected_taker):
                raise CaptureError("SDK changed the fixed bounded economics")
            nonce = builder.contracts.yield_bearing_ctf_exchange.functions.nonces(
                identity
            ).call()
            order = builder.build_order(
                "MARKET",
                BuildOrderInput(
                    side=Side.BUY,
                    token_id=token,
                    maker_amount=amounts.maker_amount,
                    taker_amount=amounts.taker_amount,
                    fee_rate_bps=market["feeRateBps"],
                    nonce=nonce,
                    maker=identity,
                    signer=identity,
                ),
            )
            typed = builder.build_typed_data(
                order, is_neg_risk=False, is_yield_bearing=True
            )
            signed = builder.sign_typed_data_order(typed)
            order_hash = builder.build_typed_data_hash(typed).lower()
            wire = {
                "salt": signed.salt,
                "maker": signed.maker,
                "signer": signed.signer,
                "taker": signed.taker,
                "tokenId": signed.token_id,
                "makerAmount": signed.maker_amount,
                "takerAmount": signed.taker_amount,
                "expiration": int(signed.expiration),
                "nonce": signed.nonce,
                "feeRateBps": signed.fee_rate_bps,
                "side": int(signed.side),
                "signatureType": int(signed.signature_type),
                "hash": order_hash,
                "signature": "0x" + signed.signature.removeprefix("0x"),
            }
            data = {
                "strategy": "MARKET",
                "pricePerShare": str(amounts.price_per_share),
                "slippageBps": str(amounts.slippage_bps),
                "isMinAmountOut": amounts.is_min_amount_out,
                "order": wire,
            }
            if minimum_out_controls or index == 0:
                data["isFillOrKill"] = True
            # Minimum-out changes the signed denominator; check the final ratio.
            signed_bound = Decimal(amounts.maker_amount) / Decimal(amounts.taker_amount)
            if not Decimal(".02") <= ask - signed_bound <= Decimal(".10"):
                raise CaptureError("SDK signed ratio moved outside the 2-10 cent gap")
            case = {
                "index": index,
                "orderHash": order_hash,
                "observedAsk": str(ask),
                "signedBound": str(signed_bound),
                "targetShares": str(Decimal(amounts.amount) / 10**18),
                "request": read_response_evidence({"data": data}, secrets),
                "walletEvents": [],
                "verifyingContract": typed.domain["verifyingContract"],
            }
            terminal = threading.Event()
            known[order_hash] = case, terminal
            result["cases"].append(case)
            case["postStartedAtUtc"] = datetime.now(UTC).isoformat()
            save()  # Persist identity before the single POST, including ambiguous replies.
            reply = session.post(
                "https://api.predict.fun/v1/orders",
                json={"data": data},
                timeout=15,
                allow_redirects=False,
            )
            case["postCompletedAtUtc"] = datetime.now(UTC).isoformat()
            case["httpStatus"] = reply.status_code
            case["createReceipt"] = receipt_evidence(reply.json(), secrets)
            save()
            terminal.wait(4)
            for worker in cancel_threads:
                worker.join(timeout=16)
            final = read("/v1/orders/" + order_hash)["data"]
            case["finalOrder"] = {
                "status": final["status"],
                "amountFilled": final.get("amountFilled"),
            }
            if final.get("amountFilled") != "0" or reply.status_code != 201:
                raise CaptureError(
                    "unexpected fill or create failure; stop remaining controls"
                )
    except Exception as error:  # noqa: BLE001 - preserve cleanup without exposing credentials
        result["errorType"] = type(error).__name__
    finally:
        try:
            result["cleanup"] = final_cleanup(
                request, identity, list(known), verify_terms=owns
            )
        except Exception as error:  # noqa: BLE001 - preserve cleanup without exposing credentials
            result["cleanupErrorType"] = type(error).__name__
        for worker in cancel_threads:
            worker.join(timeout=16)
        stop.set()
        observer.join(timeout=12)
        save()
    print(
        json.dumps(
            {
                "evidence": str(output),
                "cases": len(result["cases"]),
                "errorType": result.get("errorType"),
                "cleanup": result.get("cleanup"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--native-evidence", type=Path, required=True)
    parser.add_argument("--minimum-out-controls", action="store_true")
    args = parser.parse_args()
    try:
        run(args.submit, args.native_evidence, args.minimum_out_controls)
    except Exception as error:  # noqa: BLE001 - preserve cleanup without exposing credentials
        raise SystemExit("SDK control failed: " + type(error).__name__) from None
