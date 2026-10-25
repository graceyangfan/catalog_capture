#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Explicit mainnet validation negatives. Never signs an executable order. LGPL-3.0."""

import argparse
import copy
import json
import os
import secrets
import threading
import traceback
from datetime import UTC, datetime

from capture_predict_private_fixtures import ROOT
from predict_private_http_recorder import read_response_evidence
from predict_read_only_calibration import observe_wallet
from predict_sdk.types import Order, Side, SignatureType
from probe_predict_order_capabilities import authenticated_context

INVALID_SIGNATURE = "0x" + "00" * 65
BASE = "https://api.predict.fun"


def require_unexecutable(body, require_expired=True):
    order = body["data"]["order"]
    if order["signature"] != INVALID_SIGNATURE:
        raise ValueError(
            "an all-zero invalid signature is mandatory; no valid order may be signed"
        )
    if require_expired and order["expiration"] != 1:
        raise ValueError("expired phase requires expiration=1")
    if (
        not require_expired
        and not int(datetime.now(UTC).timestamp())
        < order["expiration"]
        <= int(datetime.now(UTC).timestamp()) + 600
    ):
        raise ValueError("signature phase expiry must be within ten minutes")
    if (
        order["makerAmount"] != "1000000000000000000"
        or order["takerAmount"] != "100000000000000000000"
    ):
        raise ValueError("unexpected negative-control economics")


def run(submit, signature_only=False):
    _settings, private, identity, owner, builder, session, read = (
        authenticated_context()
    )
    private_values = [
        private["private_key"],
        identity,
        owner.address,
        session.headers["x-api-key"],
        session.headers["Authorization"].removeprefix("Bearer "),
    ]

    def clean(obj):
        return read_response_evidence(obj, private_values)

    markets = read("/v1/markets", params={"first": 100, "status": "OPEN"})["data"]
    market = next(
        (
            m
            for m in markets
            if m.get("tradingStatus") == "OPEN"
            and any(
                word in json.dumps(m).lower()
                for word in ["bitcoin", "ethereum", "btc-updown", "eth-updown", "bnb"]
            )
        ),
        None,
    )
    if market is None:
        raise ValueError("no active crypto candidate; no order sent")
    token = next(o["onChainId"] for o in market["outcomes"] if o["indexSet"] == 1)
    seed = secrets.randbelow(2_000_000_000)
    expiry = int(datetime.now(UTC).timestamp()) + 300 if signature_only else 1
    bodies = {}
    for strategy, delta in [("LIMIT", 0), ("MARKET", 1)]:
        order = Order(
            salt=str(seed + delta),
            maker=identity,
            signer=identity,
            taker="0x" + "00" * 20,
            token_id=str(token),
            maker_amount=str(10**18),
            taker_amount=str(100 * 10**18),
            expiration=str(expiry),
            nonce="0",
            fee_rate_bps=str(market["feeRateBps"]),
            side=Side.BUY,
            signature_type=SignatureType(0),
        )
        typed = builder.build_typed_data(
            order,
            is_neg_risk=market["isNegRisk"],
            is_yield_bearing=market["isYieldBearing"],
        )
        order_hash = builder.build_typed_data_hash(typed)
        wire = {
            "salt": order.salt,
            "maker": identity,
            "signer": identity,
            "taker": order.taker,
            "tokenId": order.token_id,
            "makerAmount": order.maker_amount,
            "takerAmount": order.taker_amount,
            "expiration": expiry,
            "nonce": "0",
            "feeRateBps": order.fee_rate_bps,
            "side": 0,
            "signatureType": 0,
            "hash": order_hash,
            "signature": INVALID_SIGNATURE,
        }
        data = {"strategy": strategy, "pricePerShare": str(10**16), "order": wire}
        if strategy == "LIMIT":
            data["isPostOnly"] = True
        else:
            data.update(slippageBps="0", isMinAmountOut=False)
        bodies[strategy] = {"data": data}
    fok = copy.deepcopy(bodies["MARKET"])
    fok["data"]["isFillOrKill"] = True
    cases = [
        ("limit_invalid_expired", bodies["LIMIT"]),
        ("limit_identical_repeat", bodies["LIMIT"]),
        ("market_gtc_invalid_expired", bodies["MARKET"]),
        ("market_gtc_identical_repeat", bodies["MARKET"]),
        ("market_fok_invalid_expired", fok),
    ]
    if signature_only:
        cases = [
            (label.replace("invalid_expired", "invalid_signature"), body)
            for label, body in cases
        ]
    output = (
        ROOT
        / "data/predict-validation"
        / ("unexecutable_feedback_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    output.mkdir(parents=True, exist_ok=False)
    result = {
        "source": "Mainnet HTTP/wallet observation of deliberately unexecutable negatives; SDK hash only, no order signature",
        "marketId": market["id"],
        "strictZeroFill": True,
        "phase": "invalid_signature"
        if signature_only
        else "expired_and_invalid_signature",
        "orderSignaturesCreated": 0,
        "cases": [],
        "walletEvents": [],
        "limitations": [
            "Validation negatives cannot establish original active-order duplicate semantics or partial-fill tail behavior."
        ],
    }

    def save():
        path = output / "capture.json"
        tmp = path.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(result, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)

    for _, body in cases:
        require_unexecutable(body, require_expired=not signature_only)
    result["preparedRequests"] = [
        {"name": label, "request": clean(body)} for label, body in cases
    ]
    save()
    if not submit:
        print(json.dumps({"prepared": str(output), "submitted": False}))
        return
    hashes = {b["data"]["order"]["hash"].lower() for _, b in cases}
    ready = threading.Event()
    stop = threading.Event()
    wallet = {}

    def watch():
        try:
            events, beats, elapsed = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=40,
                on_subscribed=ready.set,
                stop_requested=stop.is_set,
            )
            wallet.update(
                events=clean(
                    [e for e in events if e.get("orderHash", "").lower() in hashes]
                ),
                heartbeats=beats,
                elapsed=elapsed,
            )
        except Exception as error:  # noqa: BLE001 - redact credential-bearing SDK/transport failures
            wallet["errorType"] = type(error).__name__

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    if not ready.wait(25):
        stop.set()
        thread.join(6)
        result["wallet"] = wallet
        save()
        raise ValueError("wallet subscription not acknowledged; no order sent")
    created = []
    try:
        for label, body in cases:
            require_unexecutable(body, require_expired=not signature_only)
            entry = {
                "name": label,
                "request": clean(body),
                "startedAt": datetime.now(UTC).isoformat(),
                "transmissionIntent": True,
            }
            result["cases"].append(entry)
            save()
            response = session.post(
                BASE + "/v1/orders", json=body, timeout=12, allow_redirects=False
            )
            try:
                payload = response.json()
            except ValueError:
                payload = {"nonJsonResponse": True}
            entry.update(
                httpStatus=response.status_code,
                response=clean(payload),
                completedAt=datetime.now(UTC).isoformat(),
            )
            save()
            if response.status_code == 201 or payload.get("success") is True:
                route = payload.get("data", {}).get("orderId")
                if route:
                    created.append(str(route))
                result["unexpectedAcceptance"] = True
                save()
                break
        result["restByHash"] = []
        for order_hash in sorted(hashes):
            response = session.get(
                BASE + "/v1/orders/" + order_hash, timeout=12, allow_redirects=False
            )
            try:
                payload = response.json()
            except ValueError:
                payload = {"nonJsonResponse": True}
            result["restByHash"].append(
                {
                    "hash": order_hash,
                    "httpStatus": response.status_code,
                    "response": clean(payload),
                }
            )
        save()
    finally:
        if created:
            response = session.post(
                BASE + "/v1/orders/remove",
                json={"data": {"ids": created}},
                timeout=12,
                allow_redirects=False,
            )
            try:
                payload = response.json()
            except ValueError:
                payload = {"nonJsonResponse": True}
            result["cleanup"] = {
                "ids": created,
                "httpStatus": response.status_code,
                "response": clean(payload),
            }
        stop.set()
        thread.join(7)
        result["wallet"] = wallet
        result["walletEvents"] = wallet.get("events", [])
        save()
    result["submittedRequests"] = len(result["cases"])
    save()
    print(
        json.dumps(
            {
                "evidence": str(output / "capture.json"),
                "httpStatuses": [c.get("httpStatus") for c in result["cases"]],
                "walletEvents": len(result["walletEvents"]),
                "orderSignaturesCreated": 0,
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--submit-unexecutable", action="store_true")
    parser.add_argument("--signature-only", action="store_true")
    args = parser.parse_args()
    try:
        run(args.submit_unexecutable, args.signature_only)
    except Exception as error:  # noqa: BLE001 - redact credential-bearing SDK/transport failures
        print(
            json.dumps(
                {
                    "errorType": type(error).__name__,
                    "frames": [
                        {
                            "file": os.path.basename(f.filename),
                            "line": f.lineno,
                            "function": f.name,
                        }
                        for f in traceback.extract_tb(error.__traceback__)
                    ],
                    "detail": "Probe stopped; inspect retained sanitized evidence. No executable order was signed.",
                }
            )
        )
        raise SystemExit(1) from None
