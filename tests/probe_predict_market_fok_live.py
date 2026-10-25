"""Explicit bounded mainnet MARKET FOK probe. Requires --submit. LGPL-3.0.

One BUY only, 1 USDT signed collateral, price <= 0.01 and <= ask / 20.
No allowance changes, chain transactions, resubmission or unrelated cancellations.
"""

import argparse
import json
import os
import threading
from datetime import UTC, datetime
from decimal import Decimal as D

from capture_predict_private_fixtures import ROOT
from predict_read_only_calibration import complete_pages, observe_wallet
from predict_sdk.types import BuildOrderInput, Side
from probe_predict_order_capabilities import authenticated_context, safe_diagnostic


def check_bounds(price, quantity, ask):
    if not (D(0) < price <= D(".01") and price * 20 <= ask and price * quantity == 1):
        raise ValueError("probe price, distance or budget invalid")


def run(submit):
    _settings, private, identity, owner, builder, session, read = (
        authenticated_context()
    )
    secrets = [
        private["private_key"],
        identity,
        owner.address,
        session.headers["x-api-key"],
        session.headers["Authorization"].removeprefix("Bearer "),
    ]

    def request(method, path, **kwargs):
        if method != "GET":
            raise ValueError("read-only helper")
        return read(path, **kwargs)

    rows = read("/v1/markets", params={"first": 50, "status": "OPEN"})["data"]
    candidate = None
    for m in rows:
        if (
            m.get("tradingStatus") != "OPEN"
            or m.get("isNegRisk")
            or m.get("decimalPrecision") != 2
        ):
            continue
        book = read(f"/v1/markets/{m['id']}/orderbook")["data"]
        if (
            book.get("asks")
            and book.get("bids")
            and D(str(book["asks"][0][0])) >= D(".25")
        ):
            candidate = m
            break
    if candidate is None:
        raise ValueError("no distant-price candidate")
    m = candidate
    price, quantity = D(".01"), D(100)
    prefix = "yield_bearing_" if m["isYieldBearing"] else ""
    exchange = getattr(builder.contracts, prefix + "ctf_exchange")
    nonce = exchange.functions.nonces(identity).call()
    order = builder.build_order(
        "MARKET",
        BuildOrderInput(
            side=Side.BUY,
            token_id=next(o["onChainId"] for o in m["outcomes"] if o["indexSet"] == 1),
            maker_amount=10**18,
            taker_amount=100 * 10**18,
            fee_rate_bps=m["feeRateBps"],
            nonce=nonce,
            maker=identity,
            signer=identity,
        ),
    )
    typed = builder.build_typed_data(
        order, is_neg_risk=m["isNegRisk"], is_yield_bearing=m["isYieldBearing"]
    )
    signed = builder.sign_typed_data_order(typed)
    order_hash = builder.build_typed_data_hash(typed)
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
    secrets.append(wire["signature"])
    body = {
        "data": {
            "strategy": "MARKET",
            "pricePerShare": str(10**16),
            "slippageBps": "0",
            "isMinAmountOut": False,
            "isFillOrKill": True,
            "order": wire,
        }
    }
    # Refresh immediately before admission; the signed price is never raised to follow ask.
    book = read(f"/v1/markets/{m['id']}/orderbook")["data"]
    ask = min(D(str(row[0])) for row in book["asks"])
    check_bounds(price, quantity, ask)
    result = {
        "market_id": m["id"],
        "order_hash": order_hash,
        "price": str(price),
        "quantity": str(quantity),
        "max_collateral_usdt": "1",
        "observed_best_ask": str(ask),
        "signed_expiration": wire["expiration"],
        "submitted": False,
        "fok_full_execution_verified": False,
    }
    output = (
        ROOT
        / "data/predict-validation"
        / (
            "signed_market_fok_"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            + ".json"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        temporary = output.with_suffix(".tmp")
        with temporary.open("w") as f:
            json.dump(result, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        temporary.replace(output)

    save()
    if not submit:
        print(json.dumps({"evidence": str(output), **result}, indent=2))
        return
    ack = threading.Event()
    wallet = {}

    def watch():
        try:
            events, beats, seconds = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=12,
                on_subscribed=ack.set,
            )
            owned = [e for e in events if order_hash.lower() in json.dumps(e).lower()]
            # Preserve only sanitized economic/lifecycle evidence for this hash.
            from predict_private_http_recorder import read_response_evidence

            wallet.update(
                events=read_response_evidence(owned, secrets),
                heartbeats=beats,
                seconds=seconds,
            )
        except Exception as e:  # noqa: BLE001 - preserve cleanup and redact transport errors
            wallet["error_type"] = type(e).__name__

    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    if not ack.wait(25):
        raise ValueError("wallet observer did not acknowledge; no submission")
    # Durable transmission intent before a single POST; never retry an ambiguous write.
    result["submitted"] = True
    save()
    try:
        response = session.post(
            "https://api.predict.fun/v1/orders",
            json=body,
            timeout=15,
            allow_redirects=False,
        )
        payload = response.json()
        result["http_status"] = response.status_code
        result["success"] = payload.get("success") is True
        result["diagnostic"] = safe_diagnostic(payload, secrets)
    except Exception as e:  # noqa: BLE001 - preserve cleanup and redact transport errors
        result["submission_error_type"] = type(e).__name__
    finally:
        save()
        # Inspect and remove ONLY this run's hash even after an ambiguous POST.
        try:
            query = session.get(
                "https://api.predict.fun/v1/orders/" + order_hash,
                timeout=15,
                allow_redirects=False,
            )
            result["query_http_status"] = query.status_code
            if query.status_code == 200:
                row = query.json()["data"]
                if row["order"]["hash"].lower() != order_hash.lower():
                    raise ValueError("hash mismatch")
                result["order_status"] = row["status"]
                result["amount_filled_raw"] = row.get("amountFilled")
            opened = complete_pages(request, "/v1/orders", status="OPEN")
            ours = [
                r for r in opened if r["order"]["hash"].lower() == order_hash.lower()
            ]
            result["run_open_before_cleanup"] = len(ours)
            if ours:
                removed = session.post(
                    "https://api.predict.fun/v1/orders/remove",
                    json={"data": {"ids": [str(r["id"]) for r in ours]}},
                    timeout=15,
                    allow_redirects=False,
                )
                result["cleanup_http_status"] = removed.status_code
            final = complete_pages(request, "/v1/orders", status="OPEN")
            result["run_open_after_cleanup"] = sum(
                r["order"]["hash"].lower() == order_hash.lower() for r in final
            )
        except Exception as e:  # noqa: BLE001 - preserve cleanup and redact transport errors
            result["cleanup_error_type"] = type(e).__name__
        thread.join(timeout=20)
        result["wallet"] = wallet
        try:
            final_row = read("/v1/orders/" + order_hash)["data"]
            if final_row["order"]["hash"].lower() != order_hash.lower():
                raise ValueError("final hash mismatch")
            result["final_order_status"] = final_row["status"]
            result["final_amount_filled_raw"] = final_row.get("amountFilled")
        except Exception as e:  # noqa: BLE001 - preserve cleanup and redact transport errors
            result["final_query_error_type"] = type(e).__name__
        save()
    print(json.dumps({"evidence": str(output), **result}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submit", action="store_true")
    args = parser.parse_args()
    try:
        run(args.submit)
    except Exception as error:  # noqa: BLE001 - redact dependency errors
        raise SystemExit("probe failed: " + type(error).__name__) from None
