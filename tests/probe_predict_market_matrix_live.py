#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Bounded mainnet MARKET parameter comparison; requires --submit. LGPL-3.0.

At most eight BUYs, each signed collateral <= 1 USDT. Effective signed price
must be <= .02 and <= observed YES ask / 20. Never retry order creation.
--closer-cancel instead runs three BUYs at <= .50 and >= .25 below ask,
requesting removal immediately after HTTP acceptance; this cannot prevent fills.
No approvals, chain writes, SELLs or unrelated cancellations.
"""

import argparse
import copy
import json
import os
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal as D

from capture_predict_private_fixtures import ROOT
from predict_private_http_recorder import read_response_evidence
from predict_read_only_calibration import complete_pages, observe_wallet
from predict_sdk.types import Book, BuildOrderInput, MarketHelperInput, Side
from probe_predict_order_capabilities import authenticated_context, safe_diagnostic

SCALE = 10**18


def cases(closer=False):
    if closer:
        return [
            {
                "name": "closer_010_fok",
                "price": ".10",
                "quantity": "10",
                "bps": 0,
                "minimum": False,
            },
            {
                "name": "closer_050_fok",
                "price": ".50",
                "quantity": "2",
                "bps": 0,
                "minimum": False,
            },
            {
                "name": "closer_050_fok_omitted",
                "price": ".50",
                "quantity": "2",
                "bps": 0,
                "minimum": False,
                "omit": "isFillOrKill",
            },
        ]
    baseline = {"price": ".01", "quantity": "100", "bps": 0, "minimum": False}
    return [
        {"name": "baseline_fok", **baseline},
        {"name": "fok_omitted", **baseline, "omit": "isFillOrKill"},
        {"name": "fok_false", **baseline, "fok": False},
        {"name": "minimum_out_zero", **baseline, "minimum": True},
        {"name": "minimum_out_100bps", **baseline, "minimum": True, "bps": 100},
        {"name": "slippage_omitted", **baseline, "omit": "slippageBps"},
        {"name": "minimum_out_omitted", **baseline, "omit": "isMinAmountOut"},
        {"name": "price_quantity_pair", **baseline, "price": ".02", "quantity": "50"},
    ]


def amounts_for(builder, case):
    # Deliberately a single distant reference level, NOT an observed executable book.
    return builder.get_market_order_amounts(
        MarketHelperInput(
            side=Side.BUY,
            quantity_wei=int(D(case["quantity"]) * SCALE),
            slippage_bps=case["bps"],
            is_min_amount_out=case["minimum"],
        ),
        Book(
            market_id=0,
            update_timestamp_ms=0,
            asks=[(case["price"], case["quantity"])],
            bids=[],
        ),
    )


def check_bounds(body, ask, closer=False):
    data, order = body["data"], body["data"]["order"]
    maker, taker = int(order["makerAmount"]), int(order["takerAmount"])
    reference = D(data["pricePerShare"]) / SCALE
    maximum = D(".50") if closer else D(".02")
    distance = (
        D(maker) <= (ask - D(".25")) * taker and reference <= ask - D(".25")
        if closer
        else D(maker) * 20 <= ask * taker and reference * 20 <= ask
    )
    if not (
        data["strategy"] == "MARKET"
        and int(order["side"]) == int(Side.BUY)
        and 0 < maker <= SCALE
        and taker > 0
        and 0 < reference <= maximum
        and D(maker) / taker <= maximum
        and distance
        and int(order["expiration"]) > time.time()
        and int(order["expiration"]) <= time.time() + 310
    ):
        raise ValueError(
            "signed collateral, price, distance or expiration outside probe bounds"
        )


def envelope(signed, order_hash, amounts, case):
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
        "isFillOrKill": case.get("fok", True),
        "order": wire,
    }
    data.pop(case.get("omit", ""), None)
    return {"data": data}


def run(submit, closer=False):
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
    chosen = None
    for market in read("/v1/markets", params={"first": 50, "status": "OPEN"})["data"]:
        if (
            market.get("tradingStatus") != "OPEN"
            or market.get("isNegRisk")
            or market.get("decimalPrecision") != 2
        ):
            continue
        book = read(f"/v1/markets/{market['id']}/orderbook")["data"]
        if (
            book.get("asks")
            and book.get("bids")
            and D(str(book["asks"][0][0])) >= (D(".75") if closer else D(".50"))
        ):
            chosen = market
            break
    if chosen is None:
        raise ValueError("no sufficiently distant open candidate")
    market = chosen
    token = next(o["onChainId"] for o in market["outcomes"] if o["indexSet"] == 1)
    exchange = getattr(
        builder.contracts,
        ("yield_bearing_" if market["isYieldBearing"] else "") + "ctf_exchange",
    )
    nonce = exchange.functions.nonces(identity).call()
    journal = {
        "market_id": market["id"],
        "sdk": "predict-sdk==0.0.22",
        "max_cases": len(cases(closer)),
        "closer_cancel_mode": closer,
        "max_signed_collateral_per_case_usdt": "1",
        "cases": [],
        "submitted": submit,
        "scope": "distant-reference zero-match comparison, not executable-depth or successful matching proof",
    }
    output = (
        ROOT
        / "data/predict-validation"
        / (
            "market_parameter_matrix_"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            + ".json"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        temporary = output.with_suffix(".tmp")
        with temporary.open("w") as stream:
            json.dump(journal, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(output)

    ack, stop = threading.Event(), threading.Event()
    condition = threading.Condition()
    received, wallet_state = {}, {}

    def incoming(event):
        with condition:
            received.setdefault(str(event.get("orderHash", "")).lower(), []).append(
                event
            )
            condition.notify_all()

    def watch():
        try:
            _events, beats, seconds = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=None,
                on_subscribed=ack.set,
                stop_requested=stop.is_set,
                on_wallet_event=incoming,
            )
            wallet_state.update(heartbeats=beats, seconds=seconds)
        except Exception as error:  # noqa: BLE001 - never expose transport credentials
            wallet_state["error_type"] = type(error).__name__
        finally:
            with condition:
                condition.notify_all()

    def request(method, path, **kwargs):
        if method != "GET":
            raise ValueError("read-only pagination helper")
        return read(path, **kwargs)

    def resolve(result):
        order_hash = result["order_hash"]
        # Remove only this submitted hash if it remains OPEN after observation.
        opened = complete_pages(request, "/v1/orders", status="OPEN")
        ours = [r for r in opened if r["order"]["hash"].lower() == order_hash.lower()]
        result["open_before_cleanup"] = len(ours)
        if ours:
            response = session.post(
                "https://api.predict.fun/v1/orders/remove",
                json={"data": {"ids": [str(r["id"]) for r in ours]}},
                timeout=15,
                allow_redirects=False,
            )
            result["cleanup_http_status"] = response.status_code
            result["cleanup"] = read_response_evidence(response.json(), secrets)
        response = session.get(
            "https://api.predict.fun/v1/orders/" + order_hash,
            timeout=15,
            allow_redirects=False,
        )
        result["final_query_http_status"] = response.status_code
        if response.status_code == 200:
            row = response.json()["data"]
            if row["order"]["hash"].lower() != order_hash.lower():
                raise ValueError("query identity mismatch")
            result["final_status"] = row["status"]
            result["final_amount_filled"] = row.get("amountFilled")
            if D(str(row.get("amountFilled", "NaN"))) != 0:
                raise ValueError("positive or unknown fill; stop matrix")
            if row["status"] not in ("CANCELLED", "EXPIRED", "REJECTED", "FILLED"):
                raise ValueError("order not terminal after cleanup")
        elif not (result.get("http_status") == 400 and response.status_code == 404):
            raise ValueError("cannot prove submitted order terminal")
        final = complete_pages(request, "/v1/orders", status="OPEN")
        result["open_after_cleanup"] = sum(
            r["order"]["hash"].lower() == order_hash.lower() for r in final
        )
        if result["open_after_cleanup"]:
            raise ValueError("run order remains open; stop matrix")

    thread = threading.Thread(target=watch, daemon=True)
    save()
    if submit:
        thread.start()
        if not ack.wait(25):
            stop.set()
            raise ValueError("wallet not acknowledged; no orders sent")
    try:
        for case in cases(closer):
            if wallet_state.get("error_type"):
                raise ValueError("wallet disconnected; stop matrix")
            amounts = amounts_for(builder, case)
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
                order,
                is_neg_risk=market["isNegRisk"],
                is_yield_bearing=market["isYieldBearing"],
            )
            signed = builder.sign_typed_data_order(typed)
            order_hash = builder.build_typed_data_hash(typed)
            body = envelope(signed, order_hash, amounts, case)
            secrets.append(body["data"]["order"]["signature"])
            book = read(f"/v1/markets/{market['id']}/orderbook")["data"]
            ask = min(D(str(level[0])) for level in book["asks"])
            check_bounds(body, ask, closer)
            result = {
                "name": case["name"],
                "order_hash": order_hash,
                "observed_best_ask": str(ask),
                "target_shares": str(D(amounts.amount) / SCALE),
                "request": read_response_evidence(body, secrets),
                "transmission_attempted": False,
            }
            journal["cases"].append(result)
            save()
            if not submit:
                continue
            result["transmission_attempted"] = True
            save()
            try:
                response = session.post(
                    "https://api.predict.fun/v1/orders",
                    json=body,
                    timeout=15,
                    allow_redirects=False,
                )
                result["http_status"] = response.status_code
                received_at = time.monotonic_ns()
                payload = response.json()
                result["diagnostic"] = safe_diagnostic(payload, secrets)
                result["receipt"] = read_response_evidence(payload, secrets)
                if response.status_code == 201:
                    if closer:
                        receipt = payload["data"]
                        if receipt["orderHash"].lower() != order_hash.lower():
                            raise ValueError("receipt hash mismatch")
                        result["cancel_started_after_receipt_ms"] = (
                            time.monotonic_ns() - received_at
                        ) / 1_000_000
                        canceled = session.post(
                            "https://api.predict.fun/v1/orders/remove",
                            json={"data": {"ids": [str(receipt["orderId"])]}},
                            timeout=15,
                            allow_redirects=False,
                        )
                        result["immediate_cancel_http_status"] = canceled.status_code
                        result["immediate_cancel"] = read_response_evidence(
                            canceled.json(), secrets
                        )
                    with condition:
                        condition.wait_for(
                            lambda order_hash=order_hash: (
                                wallet_state.get("error_type")
                                or any(
                                    e.get("type")
                                    in (
                                        "orderNotAccepted",
                                        "orderCancelled",
                                        "orderFilled",
                                    )
                                    for e in received.get(order_hash.lower(), [])
                                )
                            ),
                            timeout=8,
                        )
            except Exception as error:  # noqa: BLE001 - resolve ambiguous transmission without retry
                result["submission_error_type"] = type(error).__name__
                journal["stop_reason"] = "ambiguous submission; no retry"
            finally:
                with condition:
                    result["wallet_events"] = read_response_evidence(
                        copy.deepcopy(received.get(order_hash.lower(), [])), secrets
                    )
                save()
                resolve(result)
                save()
            if journal.get("stop_reason"):
                break
    except Exception as error:  # noqa: BLE001 - preserve sanitized evidence
        journal["stop_error_type"] = type(error).__name__
        journal["stop_reason"] = safe_diagnostic({"message": str(error)}, secrets)
    finally:
        stop.set()
        if submit:
            thread.join(timeout=12)
        with condition:
            for result in journal["cases"]:
                result["wallet_events"] = read_response_evidence(
                    copy.deepcopy(received.get(result["order_hash"].lower(), [])),
                    secrets,
                )
        journal["wallet_observer"] = wallet_state
        save()
    print(
        json.dumps(
            {
                "evidence": str(output),
                "stop_reason": journal.get("stop_reason"),
                "cases": [
                    {
                        k: r.get(k)
                        for k in (
                            "name",
                            "http_status",
                            "diagnostic",
                            "final_status",
                            "final_amount_filled",
                            "open_after_cleanup",
                        )
                    }
                    for r in journal["cases"]
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument(
        "--closer-cancel",
        action="store_true",
        help="Three <=1 USDT orders, prices <=.50 and at least .25 below ask; cancel on HTTP acceptance",
    )
    try:
        args = parser.parse_args()
        run(args.submit, args.closer_cancel)
    except Exception as error:  # noqa: BLE001 - credentials stay private
        raise SystemExit("probe failed: " + type(error).__name__) from None
