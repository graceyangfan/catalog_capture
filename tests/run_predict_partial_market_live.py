#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""LGPL-3.0-or-later. One Native non-FOK BUY <= explicitly approved cap (at most 2 USDT); explicit --submit.

Observe the remainder for two seconds after Accepted, then cancel. Independent
wallet/REST/chain evidence preserves cancellation provenance. An owned holding
uses an opt-in Native OFI/maker-TP/timed-exit flow. SDK cleanup is a separate
last resort after the Native host stops; never represented as a Native exit.
No approvals, on-chain transactions, repeated BUYs or foreign cancellations.
"""

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import threading
import time
import tomllib
from datetime import UTC, datetime
from decimal import Decimal as D
from pathlib import Path

import requests
from capture_predict_private_fixtures import ROOT, CaptureError
from predict_partial_market_plan import (
    outcome_book,
    owned_cash_at_risk_wei,
    owned_exit_is_safe,
    owned_inventory_wei,
    partial_plan,
    remainder_provenance,
)
from predict_private_http_recorder import PrivateHttpRecorder, read_response_evidence
from predict_read_only_calibration import complete_pages, observe_wallet
from predict_sdk.types import (
    Book,
    BuildOrderInput,
    LimitHelperInput,
    MarketHelperInput,
    Order,
    Side,
    SignatureType,
)
from probe_predict_market_matrix_live import envelope
from probe_predict_order_capabilities import authenticated_context
from run_predict_fill_live import build_native_host
from run_predict_resting_live import final_cleanup


def search(
    seconds,
    watch_live_book=False,
    allow_micro=False,
    ofi_round_trip=False,
    max_buy=D("1.5"),
):
    """Public-key GETs only, bounded to one candidate; persist all scanned books."""
    target_principal = (max_buy - D(".01")) if ofi_round_trip else D("1.4")
    cfg = tomllib.loads((ROOT / "env/predict-execution.toml").read_text())
    key_path = Path(cfg["network"]["api_key_file"])
    if not key_path.is_absolute():
        key_path = ROOT / "env" / key_path
    http = requests.Session()
    http.headers["x-api-key"] = tomllib.loads(key_path.read_text())["api_key"]
    output = (
        ROOT
        / "data/predict-validation"
        / ("partial_search_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    output.mkdir(exist_ok=False)
    print(
        json.dumps(
            {"searchEvidence": str(output), "maxSeconds": seconds, "orderWrites": 0}
        ),
        flush=True,
    )
    cache = {}
    deadline = time.monotonic() + seconds

    def get(path, params=None):
        r = http.get(
            "https://api.predict.fun" + path,
            params=params,
            timeout=8,
            allow_redirects=False,
        )
        if r.status_code != 200:
            raise CaptureError("public search HTTP " + str(r.status_code))
        return r.json()["data"]

    with (output / "search.jsonl").open("x") as log:
        while time.monotonic() < deadline:
            try:
                rows = get("/v1/orders/matches", {"first": 100})
                now = datetime.now(UTC)
                ids = []
                for row in rows:
                    m = row.get("market", {})
                    if (
                        (
                            m.get("marketVariant") == "CRYPTO_UP_DOWN"
                            or re.search(
                                r"\b(bitcoin|ethereum|solana|bnb|btc|eth|xrp|dogecoin)\b",
                                m.get("title", ""),
                                re.IGNORECASE,
                            )
                        )
                        and m["id"] not in ids
                        and (
                            now - datetime.fromisoformat(row["executedAt"])
                        ).total_seconds()
                        < 90
                    ):
                        ids.append(m["id"])
                qualified = []
                for mid in ids[:10]:
                    if mid not in cache or time.monotonic() - cache[mid][0] > 30:
                        cache[mid] = (time.monotonic(), get(f"/v1/markets/{mid}"))
                    m = cache[mid][1]
                    ends = [
                        datetime.fromisoformat(e["endsAt"])
                        for e in m.get("rewards", {}).get("schedule", [])
                        if e.get("endsAt")
                    ]
                    if (
                        m.get("tradingStatus") != "OPEN"
                        or m.get("resolution") is not None
                        or not ends
                        or (
                            ofi_round_trip
                            and m.get("variantData", {}).get("startPrice") is None
                        )
                        or (max(ends) - datetime.now(UTC)).total_seconds()
                        < (210 if watch_live_book else 150)
                        or (
                            allow_micro
                            and (
                                (max(ends) - datetime.now(UTC)).total_seconds() > 900
                                or m.get("marketVariant") != "CRYPTO_UP_DOWN"
                            )
                        )
                    ):
                        continue
                    b = get(f"/v1/markets/{mid}/orderbook")
                    if b["marketId"] != mid:
                        raise CaptureError("public book identity mismatch")
                    for outcome in [1, 2]:
                        bids, asks = outcome_book(b, outcome)
                        plan = partial_plan(
                            bids,
                            asks,
                            int(m["feeRateBps"]),
                            allow_micro=allow_micro,
                            target_principal=target_principal,
                            max_principal=max_buy,
                        )
                        if (
                            watch_live_book
                            and bids
                            and asks
                            and int(m["feeRateBps"]) <= 200
                        ):
                            bid, bq = (D(str(x)) for x in bids[0])
                            ask, _ = (D(str(x)) for x in asks[0])
                            qty = (
                                (target_principal / ask).quantize(
                                    D(".01"), rounding="ROUND_DOWN"
                                )
                                if ask > 0
                                else D(0)
                            )
                            if (
                                0 < bid < ask <= D(".95")
                                and bid * qty * D(".98") * D(".97") >= D("1.05")
                                and bq >= qty * 3
                                and (
                                    not ofi_round_trip
                                    or ask * qty - bid * qty * D(".9604")
                                    <= max_buy * D(".08")
                                )
                            ):
                                plan = {
                                    "referencePrice": str(ask),
                                    "quantity": str(qty),
                                    "signedPrincipal": str(ask * qty),
                                    "watchSeedOnly": True,
                                }
                            else:
                                plan = None
                        row = {
                            "observedAtUtc": datetime.now(UTC).isoformat(),
                            "marketId": mid,
                            "title": m["title"],
                            "outcome": outcome,
                            "expiresAt": max(ends).isoformat(),
                            "market": m,
                            "book": b,
                            "plan": plan,
                            "watchLiveBook": watch_live_book,
                            "allowMicroRemainder": allow_micro,
                        }
                        log.write(json.dumps(row) + "\n")
                        log.flush()
                        if plan:
                            row["tokenId"] = next(
                                o["onChainId"]
                                for o in m["outcomes"]
                                if o["indexSet"] == outcome
                            )
                            qualified.append(row)
                if qualified:

                    def rank(row):
                        bids, asks = outcome_book(row["book"], row["outcome"])
                        notional = D(str(asks[0][0])) * D(str(asks[0][1]))
                        return (
                            partial_plan(
                                bids,
                                asks,
                                int(row["market"]["feeRateBps"]),
                                allow_micro=allow_micro,
                                target_principal=target_principal,
                                max_principal=max_buy,
                            )
                            is None,
                            abs(notional - D("1.2")),
                        )

                    row = min(qualified, key=rank) if ofi_round_trip else qualified[0]
                    row["selection"] = (
                        "Started price interval; prefer currently eligible thin level, then smaller L1 ask principal"
                        if ofi_round_trip
                        else "first eligible"
                    )
                    path = output / "candidate.json"
                    path.write_text(json.dumps(row, indent=2) + "\n")
                    print(
                        json.dumps(
                            {
                                "candidate": str(path),
                                "marketId": row["marketId"],
                                "outcome": row["outcome"],
                                "plan": row["plan"],
                            }
                        ),
                        flush=True,
                    )
                    return path
                print(
                    json.dumps(
                        {
                            "at": datetime.now(UTC).isoformat(),
                            "recentMarkets": len(ids),
                            "candidate": False,
                        }
                    ),
                    flush=True,
                )
            except Exception as e:  # noqa: BLE001 - safe public diagnostic
                log.write(
                    json.dumps(
                        {
                            "observedAtUtc": datetime.now(UTC).isoformat(),
                            "errorType": type(e).__name__,
                        }
                    )
                    + "\n"
                )
                log.flush()
            time.sleep(3)
    raise CaptureError("bounded public search found no eligible partial candidate")


def run(
    candidate_path,
    submit,
    native_executable=None,
    prepared_context=None,
    ofi_round_trip=False,
    max_buy=D("1.5"),
):
    if not submit:
        raise CaptureError("explicit --submit required before authentication")
    if native_executable:
        executable = Path(native_executable).resolve(strict=True)
        if not executable.is_file():
            raise CaptureError("isolated Native executable missing")
        command = [
            str(executable),
            "market_probe_live::mainnet_native_market_parameters",
            "--ignored",
            "--exact",
            "--nocapture",
        ]
    else:
        command = build_native_host()
        command[1] = "market_probe_live::mainnet_native_market_parameters"
    target_principal = (max_buy - D(".01")) if ofi_round_trip else D("1.4")
    candidate = json.loads(Path(candidate_path).read_text())
    market_id, outcome = candidate["marketId"], candidate["outcome"]
    plan, token = candidate["plan"], str(candidate["tokenId"])
    watch_live_book = candidate.get("watchLiveBook", False)
    allow_micro = candidate.get("allowMicroRemainder", False)
    profile = ROOT / "env/predict-native-market-probe.toml"
    manifest = ROOT / "logs/predict-native-market-probe.json"
    if profile.exists() or manifest.exists():
        raise CaptureError("run control files already owned")
    _, private, identity, owner, builder, session, _read = (
        prepared_context or authenticated_context()
    )
    secrets = [
        private["private_key"],
        identity,
        owner.address,
        session.headers["x-api-key"],
        session.headers["Authorization"].removeprefix("Bearer "),
    ]
    output = (
        ROOT
        / "data/predict-validation"
        / ("native_partial_market_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    output.mkdir(exist_ok=False)
    records, events, oracle = [], [], []
    metadata = {
        "observedAtUtc": datetime.now(UTC).isoformat(),
        "maxAdditionalBuyPrincipalUSDT": str(max_buy),
        "targetPrincipalUSDT": str(target_principal),
        "maxBuyOrders": 1,
        "ofiRoundTrip": ofi_round_trip,
        "executable": str(command[0]),
        "executableSha256": hashlib.sha256(Path(command[0]).read_bytes()).hexdigest(),
        "sourceSha256": {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [
                ROOT / "tests/run_predict_partial_market_live.py",
                ROOT / "tests/predict_partial_market_plan.py",
                ROOT / "crates/nautilus-predict/tests/support/market_probe_live.rs",
                ROOT
                / "crates/nautilus-predict/tests/support/market_probe_roundtrip.rs",
            ]
        },
    }
    (output / "run_metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    lock = threading.RLock()

    def save(name, value):
        with lock:
            tmp = output / (name + ".pending")
            with tmp.open("w") as f:
                json.dump(read_response_evidence(value, secrets), f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(output / name)

    def request(method, path, **kwargs):
        if method not in {"GET", "POST"} or (
            method == "POST" and path != "/v1/orders/remove"
        ):
            raise CaptureError("supervisor only reads or removes owned orders")
        item = {
            "method": method,
            "path": path,
            "params": kwargs.get("params"),
            "request": kwargs.get("json"),
            "observedAtUtc": datetime.now(UTC).isoformat(),
            "observedMonotonicNs": time.monotonic_ns(),
        }
        with lock:
            records.append(item)
            save("rest.json", records)
        r = session.request(
            method,
            "https://api.predict.fun" + path,
            timeout=12,
            allow_redirects=False,
            **kwargs,
        )
        item.update(
            status=r.status_code,
            response=r.json(),
            completedAtUtc=datetime.now(UTC).isoformat(),
            completedMonotonicNs=time.monotonic_ns(),
        )
        save("rest.json", records)
        if r.status_code != 200:
            raise CaptureError("supervisor HTTP " + str(r.status_code))
        return item["response"]

    def fresh(for_submit=False):
        m = request("GET", f"/v1/markets/{market_id}")["data"]
        b = request("GET", f"/v1/markets/{market_id}/orderbook")["data"]
        ends = [
            datetime.fromisoformat(e["endsAt"])
            for e in m.get("rewards", {}).get("schedule", [])
            if e.get("endsAt")
        ]
        bids, asks = outcome_book(b, outcome)
        p = partial_plan(
            bids,
            asks,
            int(m["feeRateBps"]),
            allow_micro=allow_micro,
            target_principal=target_principal,
            max_principal=max_buy,
        )
        if not (
            (
                not ofi_round_trip
                or m.get("variantData", {}).get("startPrice") is not None
            )
            and m.get("tradingStatus") == "OPEN"
            and m.get("resolution") is None
            and ends
            and (max(ends) - datetime.now(UTC)).total_seconds() >= 120
            and (
                not allow_micro
                or (
                    (max(ends) - datetime.now(UTC)).total_seconds() <= 900
                    and m.get("marketVariant") == "CRYPTO_UP_DOWN"
                )
            )
            and b["marketId"] == market_id
            and (
                (watch_live_book and not for_submit)
                or (
                    p
                    and (
                        watch_live_book
                        or (
                            p["referencePrice"] == plan["referencePrice"]
                            and D(p["quantity"]) == D(plan["quantity"])
                        )
                    )
                )
            )
        ):
            raise CaptureError(
                "candidate no longer satisfies bounded partial admission"
            )
        if (
            str(next(o["onChainId"] for o in m["outcomes"] if o["indexSet"] == outcome))
            != token
        ):
            raise CaptureError("token/outcome identity changed")
        return m, b

    market, book = fresh()
    prefix = "yield_bearing_" if market["isYieldBearing"] else ""
    ctf = getattr(
        builder.contracts,
        prefix
        + (
            "neg_risk_conditional_tokens"
            if market["isNegRisk"]
            else "conditional_tokens"
        ),
    )

    def chain():
        return {
            "observedAtUtc": datetime.now(UTC).isoformat(),
            "tokenWei": str(ctf.functions.balanceOf(identity, int(token)).call()),
            "cashWei": str(builder.contracts.usdt.functions.balanceOf(identity).call()),
        }

    before = chain()
    if int(before["tokenWei"]) or complete_pages(request, "/v1/orders", status="OPEN"):
        raise CaptureError("token must start flat and account must have no open orders")
    positions_before = complete_pages(request, "/v1/positions", includeLost="true")
    if any(
        p["market"].get("status") != "RESOLVED" or p["outcome"].get("status") != "LOST"
        for p in positions_before
    ):
        raise CaptureError("unrelated active holding; no entry")
    save(
        "admission.json",
        {
            "candidate": candidate,
            "market": market,
            "book": book,
            "chain": before,
            "positions": positions_before,
        },
    )

    submitted = []

    def validate(data, count):
        item = {
            "request": {"data": data},
            "checks": {},
            "observedAtUtc": datetime.now(UTC).isoformat(),
        }
        try:
            m = market
            buy = data["order"]["side"] == 0
            intent = json.loads(
                (
                    output / ("quote_intent.json" if buy else "exit_intent.json")
                ).read_text()
            )
            age_ns = time.time_ns() - int(intent["observedNs"])
            if not (0 <= age_ns <= 1_000_000_000):
                raise CaptureError("Native trigger frame is stale at relay")
            if buy:
                b = {
                    "source": "Native trigger frame persisted before signing",
                    "bids": [[intent["observedBid"], intent["observedBidSize"]]],
                    "asks": [[intent["observedAsk"], intent["observedAskSize"]]],
                    "intent": intent,
                }
                active_plan = partial_plan(
                    b["bids"],
                    b["asks"],
                    int(m["feeRateBps"]),
                    allow_micro=allow_micro,
                    target_principal=target_principal,
                    max_principal=max_buy,
                )
                if not active_plan or not (
                    intent["market_price"] == active_plan["referencePrice"]
                    and D(intent["quantity"]) == D(active_plan["quantity"])
                ):
                    raise CaptureError(
                        "Native trigger differs from independent partial admission"
                    )
                if ofi_round_trip and not (
                    intent["ofi"]["eligible"] is True
                    and D(intent["ofi"]["normalized"]) >= D(".25")
                    and D(intent["market_price"]) * D(intent["quantity"])
                    - D(intent["observedBid"]) * D(intent["quantity"]) * D(".9604")
                    <= max_buy * D(".08")
                ):
                    raise CaptureError("OFI or round-trip loss admission failed")
            else:
                if not ofi_round_trip or not submitted or count > 3:
                    raise CaptureError("Native exit outside owned round trip")
                # Local terminal alone never permits overlapping sales. Independently
                # confirm every earlier owned entry/exit is terminal at the venue.
                prior = [
                    request("GET", "/v1/orders/" + r["hash"])["data"] for r in submitted
                ]
                inventory = int(chain()["tokenWei"])
                if not owned_exit_is_safe(
                    count, prior, inventory, int(data["order"]["makerAmount"])
                ):
                    raise CaptureError(
                        "remote terminal or owned inventory barrier not established"
                    )
                b = {
                    "source": "Native exit intent plus independently confirmed owned inventory",
                    "intent": intent,
                    "chainTokenWei": str(inventory),
                    "priorOrders": prior,
                }
                active_plan = {
                    "quantity": intent["quantity"],
                    "referencePrice": intent["price"],
                }
            order = data["order"]
            fields = {
                k: order[k]
                for k in ["salt", "maker", "signer", "taker", "expiration", "nonce"]
            }
            fields["expiration"] = str(order["expiration"])
            fields.update(
                token_id=order["tokenId"],
                maker_amount=order["makerAmount"],
                taker_amount=order["takerAmount"],
                fee_rate_bps=order["feeRateBps"],
                side=Side(order["side"]),
                signature_type=SignatureType(order["signatureType"]),
            )
            typed = builder.build_typed_data(
                Order(**fields),
                is_neg_risk=m["isNegRisk"],
                is_yield_bearing=m["isYieldBearing"],
            )
            if data["strategy"] == "MARKET":
                expected = builder.get_market_order_amounts(
                    MarketHelperInput(
                        side=Side.BUY if buy else Side.SELL,
                        quantity_wei=int(D(active_plan["quantity"]) * 10**18),
                        slippage_bps=0 if buy else 300,
                    ),
                    Book(
                        market_id=market_id,
                        update_timestamp_ms=0,
                        asks=[(active_plan["referencePrice"], active_plan["quantity"])]
                        if buy
                        else [],
                        bids=[]
                        if buy
                        else [(active_plan["referencePrice"], active_plan["quantity"])],
                    ),
                )
            else:
                expected = builder.get_limit_order_amounts(
                    LimitHelperInput(
                        side=Side.SELL,
                        price_per_share_wei=int(
                            D(active_plan["referencePrice"]) * 10**18
                        ),
                        quantity_wei=int(D(active_plan["quantity"]) * 10**18),
                    )
                )
            item["checks"] = {
                "single_buy_or_owned_exit": count == 0 if buy else 1 <= count <= 3,
                "identity": order["maker"].lower()
                == identity.lower()
                == order["signer"].lower(),
                "token_matches_market": str(order["tokenId"]) == token,
                "correct_side": order["side"] == (0 if buy else 1),
                "fee": int(order["feeRateBps"]) == int(m["feeRateBps"]),
                "strategy": data["strategy"]
                == (
                    "MARKET"
                    if buy or intent["type"] == "NativeTakerExitSubmitted"
                    else "LIMIT"
                ),
                "execution_flags": (
                    "isFillOrKill" not in data and "isPostOnly" not in data
                    if buy
                    else (
                        data.get("isFillOrKill") is True and "isPostOnly" not in data
                        if data["strategy"] == "MARKET"
                        else data.get("isPostOnly") is True
                        and "isFillOrKill" not in data
                    )
                ),
                "slippage": (
                    data.get("slippageBps") == ("0" if buy else "300")
                    and data.get("isMinAmountOut") is False
                )
                if data["strategy"] == "MARKET"
                else "slippageBps" not in data,
                "budget_or_inventory": 0
                < int(order["makerAmount"])
                <= (int(max_buy * 10**18) if buy else inventory),
                "take_profit_cash": True
                if buy or data["strategy"] == "MARKET"
                else D(intent["expectedCashProfitAtLimit"]) > 0,
                "sdk_amounts": int(order["makerAmount"]) == expected.maker_amount
                and int(order["takerAmount"]) == expected.taker_amount,
                "reference": str(data["pricePerShare"])
                == str(expected.price_per_share),
                "expiry": time.time() < int(order["expiration"])
                and (
                    data["strategy"] == "LIMIT"
                    or int(order["expiration"]) <= time.time() + 310
                ),
                "hash": builder.build_typed_data_hash(typed).lower()
                == order["hash"].lower(),
                "signature_matches_sdk": builder.sign_typed_data_order(typed)
                .signature.removeprefix("0x")
                .lower()
                == order["signature"].removeprefix("0x").lower(),
            }
            item["nativeTriggerFrame"] = b
        except Exception as e:  # noqa: BLE001 - dependency errors may contain credentials
            item["errorType"] = type(e).__name__
        oracle.append(item)
        save("sdk_oracle.json", oracle)
        valid = bool(item["checks"]) and all(item["checks"].values())
        if valid:
            submitted.append(
                {
                    "hash": data["order"]["hash"].lower(),
                    "side": data["order"]["side"],
                    "strategy": data["strategy"],
                }
            )
        return valid

    def owns(row):
        o = row["order"]
        return (
            o["maker"].lower() == identity.lower() == o["signer"].lower()
            and str(o["tokenId"]) == token
            and (o["side"] == 1 or 0 < int(o["makerAmount"]) <= int(max_buy * 10**18))
        )

    ack, stop = threading.Event(), threading.Event()
    observer = {}

    def incoming(e):
        e = {
            **e,
            "observedAtUtc": datetime.now(UTC).isoformat(),
            "observedMonotonicNs": time.monotonic_ns(),
        }
        with lock:
            events.append(e)
            save("wallet_events.json", events)

    def watch():
        try:
            _, beats, seconds = observe_wallet(
                identity,
                session.headers["Authorization"].removeprefix("Bearer "),
                session.headers["x-api-key"],
                observation_secs=None,
                on_subscribed=ack.set,
                stop_requested=stop.is_set,
                on_wallet_event=incoming,
            )
            observer.update(heartbeats=beats, seconds=seconds)
        except Exception as e:  # noqa: BLE001 - dependency errors may contain credentials
            observer["errorType"] = type(e).__name__

    recorder = child = None
    owned = []
    thread = threading.Thread(target=watch, daemon=True)
    summary = {
        "nativeExecutable": command[0],
        "cashBeforeWei": before["cashWei"],
        "entryOwner": "Native Strategy/Risk/Predict crate",
        "exitPlan": "Native maker take-profit, then Native bounded taker; SDK only after stopped host and confirmed removals"
        if ofi_round_trip
        else "official SDK supervised cleanup",
        "ofiRoundTrip": ofi_round_trip,
        "maxBuyPrincipalUSDT": str(max_buy),
        "automaticRemainderTerminationProven": False,
    }
    try:
        source = (ROOT / "env/predict-execution.toml").read_text()
        for key, value in [
            ("allow_order_writes", "true"),
            ("require_post_only", "false"),
            ("max_order_usdt", '"' + str(max_buy) + '"'),
            ("max_total_usdt", '"' + str(max_buy) + '"'),
        ]:
            source, n = re.subn(
                r"(?m)^" + key + r"\s*=.*$", key + " = " + value, source
            )
            if n != 1:
                raise CaptureError("ambiguous bounded profile")
        with os.fdopen(
            os.open(profile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as f:
            owned.append(profile)
            f.write(source)
        recorder = PrivateHttpRecorder(
            output / "http_responses.json",
            validate,
            record_reads=True,
            measure_latency=True,
            full_responses=True,
        )
        with os.fdopen(
            os.open(manifest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
        ) as f:
            owned.append(manifest)
            json.dump(
                {
                    "marketId": market_id,
                    "outcome": outcome,
                    "referencePrice": plan["referencePrice"],
                    "quantity": plan["quantity"],
                    "partialProbe": True,
                    "watchLiveBook": watch_live_book,
                    "allowMicroRemainder": allow_micro,
                    "ofiRoundTrip": ofi_round_trip,
                    "targetPrincipal": str(target_principal),
                    "maxLossUSDT": str(max_buy * D(".08")),
                    "origin": recorder.origin,
                    "output": str(output),
                },
                f,
            )
        thread.start()
        if not ack.wait(25):
            raise CaptureError("wallet observer not acknowledged")
        # Native waits for a qualifying live BBO. The relay still performs the
        # independent fresh-book/SDK check immediately before forwarding a BUY.
        with (output / "node.log").open("w") as log:
            child = subprocess.Popen(
                command,
                cwd=ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            deadline = time.monotonic() + (
                180 if ofi_round_trip else 120 if watch_live_book else 60
            )
            watchdog = set()
            polled = {}
            terminal_seen = {}
            book_captured = set()
            while child.poll() is None:
                if time.monotonic() > deadline:
                    raise CaptureError("Native host timeout")
                with recorder.lock:
                    attempts = [dict(e) for e in recorder.events if "requestTerms" in e]
                for a in attempts:
                    h = a["requestTerms"]["hash"].lower()
                    if a.get("status") == 201 and h not in book_captured:
                        book_captured.add(h)
                        try:
                            request("GET", f"/v1/markets/{market_id}/orderbook")
                        except Exception as e:  # noqa: BLE001 - preserve safe diagnostic without delaying cleanup
                            summary.setdefault("postSubmitBookDiagnostics", []).append(
                                type(e).__name__
                            )
                    # Capture a remote terminal before our two-second cancellation
                    # even if the wallet channel omits a terminal notification.
                    if (
                        a.get("status") == 201
                        and time.monotonic() - polled.get(h, 0) >= 0.35
                    ):
                        polled[h] = time.monotonic()
                        try:
                            remote = request("GET", "/v1/orders/" + h)["data"]
                            if remote["status"] in {"FILLED", "CANCELLED", "EXPIRED", "REJECTED"}:
                                terminal_seen.setdefault(h, time.monotonic())
                                checkpoint_path = output / "native_checkpoint.json"
                                checkpoint = json.loads(checkpoint_path.read_text()) if checkpoint_path.exists() else {}
                                local = next((r for r in checkpoint.get("orders", []) if str(r.get("venueOrderId", "")).lower() == h), None)
                                if local and local["status"] not in {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"} and time.monotonic() - terminal_seen[h] > 3:
                                    summary["terminalDivergence"] = {"hash":h, "venueStatus":remote["status"], "nativeStatus":local["status"], "observedAtUtc":datetime.now(UTC).isoformat()}
                                    raise CaptureError("venue terminal did not reconcile into Native within three seconds")
                        except CaptureError:
                            raise
                        except Exception as e:  # noqa: BLE001 - retain safe diagnostic and continue watchdog
                            summary.setdefault("earlyRestDiagnostics", []).append(
                                type(e).__name__
                            )
                    if h not in watchdog and (
                        datetime.now(UTC) - datetime.fromisoformat(a["observedAtUtc"])
                    ).total_seconds() > (
                        20
                        if any(
                            r["hash"] == h
                            and r["side"] == 1
                            and r["strategy"] == "LIMIT"
                            for r in submitted
                        )
                        else 6
                    ):
                        summary.setdefault("watchdog", []).append(
                            final_cleanup(request, identity, [h], owns)
                        )
                        watchdog.add(h)
                own_hashes = {r["hash"] for r in submitted}
                first_fill = min((e["observedMonotonicNs"] for e in events if e.get("type") == "orderTransactionSuccess" and e.get("orderHash", "").lower() in {r["hash"] for r in submitted if r["side"] == 0}), default=None)
                if first_fill is not None and owned_inventory_wei(events, own_hashes) > 0 and time.monotonic_ns() - first_fill > 20_000_000_000:
                    summary["exposureWatchdog"] = "owned position remained open twenty seconds after the first confirmed BUY fill"
                    raise CaptureError(summary["exposureWatchdog"])
                time.sleep(0.1)
            summary["nativeExitCode"] = child.returncode
    except Exception as e:  # noqa: BLE001 - dependency errors may contain credentials
        summary["errorType"] = type(e).__name__
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGINT)
            try:
                child.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        all_hashes = recorder.submitted_hashes() if recorder else set()
        hashes = {r["hash"] for r in submitted if r["side"] == 0} & all_hashes
        try:
            if recorder:
                drain_deadline = time.monotonic() + 16
                while time.monotonic() < drain_deadline:
                    with recorder.lock:
                        pending = [
                            e
                            for e in recorder.events
                            if "requestTerms" in e
                            and e.get("status") is None
                            and not e.get("transportError")
                        ]
                    if not pending:
                        break
                    time.sleep(0.1)
                recorder.close()
                summary["cleanup"] = final_cleanup(request, identity, all_hashes, owns)
            summary["entryHashes"] = sorted(hashes)
            summary["nativeExitHashes"] = sorted(all_hashes - hashes)
            settled_after = time.monotonic() + 3
            for _ in range(40):
                snapshot = chain()
                save("chain_after_entry.json", snapshot)
                if int(snapshot["tokenWei"]) or (
                    time.monotonic() >= settled_after
                    and not any(
                        e.get("type") == "orderTransactionSubmitted"
                        and e.get("orderHash", "").lower() in hashes
                        for e in events
                    )
                ):
                    break
                time.sleep(0.5)
            checkpoint = (
                json.loads((output / "native_checkpoint.json").read_text())
                if (output / "native_checkpoint.json").exists()
                else {}
            )
            small_hold = any(
                e.get("type") == "RoundTripComplete"
                and e.get("reason", "").startswith("authorized_small")
                for e in checkpoint.get("journal", [])
            )
            summary["nativeRoundTripDisposition"] = [
                e
                for e in checkpoint.get("journal", [])
                if e.get("type") == "RoundTripComplete"
            ]
            owned_wei = owned_inventory_wei(events, all_hashes)
            summary["ownedConfirmedInventoryWei"] = str(owned_wei)
            if int(snapshot["tokenWei"]) > owned_wei:
                summary["inventoryOwnershipDifferenceWei"] = str(
                    int(snapshot["tokenWei"]) - owned_wei
                )
            cleanup_snapshot = {
                **snapshot,
                "tokenWei": str(min(int(snapshot["tokenWei"]), owned_wei)),
            }
            actual_cash_risk = owned_cash_at_risk_wei(events, all_hashes)
            summary["ownedConfirmedCashAtRiskWei"] = str(actual_cash_risk)
            if (
                int(cleanup_snapshot["tokenWei"])
                and small_hold
                and actual_cash_risk <= 10**17
            ):
                summary["exitDeferred"] = (
                    "authorized <=0.10 USDT cost/risk residual, no top-up; await short crypto settlement"
                )
            elif int(cleanup_snapshot["tokenWei"]):
                unwind_sdk(
                    cleanup_snapshot,
                    market,
                    outcome,
                    token,
                    identity,
                    builder,
                    session,
                    request,
                    save,
                    events,
                    summary,
                )
            summary["finalChain"] = chain()
            summary["finalOrders"] = [
                request("GET", "/v1/orders/" + h)["data"] for h in hashes
            ]
            summary["observedPartialEntry"] = any(
                0 < int(row.get("amountFilled", "0")) < int(row["order"]["takerAmount"])
                for row in summary["finalOrders"]
            )
            summary["matches"] = complete_pages(
                request,
                "/v1/orders/matches",
                signerAddress=identity,
                marketId=market_id,
            )
            summary["finalOpenOrders"] = complete_pages(
                request, "/v1/orders", status="OPEN"
            )
        except Exception as e:  # noqa: BLE001 - dependency errors may contain credentials
            summary["finalAuditErrorType"] = type(e).__name__
        finally:
            entry_ids = {str(r["id"]) for r in summary.get("finalOrders", [])}
            if recorder:
                with recorder.lock:
                    entry_ids.update(
                        str(e["responseBody"]["data"]["orderId"])
                        for e in recorder.events
                        if "requestTerms" in e
                        and e["requestTerms"]["hash"].lower() in hashes
                        and e.get("responseBody", {}).get("data", {}).get("orderId")
                    )
                    cancels = [
                        e["observedMonotonicNs"]
                        for e in recorder.events
                        if e["path"] == "/v1/orders/remove"
                        and entry_ids.intersection(e.get("requestIds", []))
                    ]
            else:
                cancels = []
            cancels.extend(
                e["observedMonotonicNs"]
                for e in records
                if e["method"] == "POST"
                and entry_ids.intersection(
                    e.get("request", {}).get("data", {}).get("ids", [])
                )
            )
            own_events = [e for e in events if e.get("orderHash", "").lower() in hashes]
            summary["firstCancelRequestMonotonicNs"] = min(cancels) if cancels else None
            summary["remainderProvenance"] = remainder_provenance(
                own_events,
                summary["firstCancelRequestMonotonicNs"],
                [
                    r
                    for r in records
                    if r["method"] == "GET"
                    and r["path"].lower() in {"/v1/orders/" + h for h in hashes}
                ],
            )
            summary["automaticRemainderTerminationProven"] = (
                summary.get("observedPartialEntry", False)
                and summary["remainderProvenance"]
                == "venue_terminal_observed_before_our_cancel_request"
            )
            summary["plannedPartialEntry"] = any(
                e.get("type") == "orderMarketMatchPlan"
                and 0 < int(e.get("plannedFill", "0")) < int(e.get("amount", "0"))
                for e in own_events
            )
            final_token=summary.get("finalChain", {}).get("tokenWei")
            if final_token is not None:
                summary["executionDisposition"]=("flat" if int(final_token)==0 else "authorized_tiny_hold" if int(summary.get("ownedConfirmedCashAtRiskWei",str(10**17+1)))<=10**17 else "unresolved_position")
            else:
                summary["executionDisposition"]="audit_incomplete"
            summary["exitSubmissionOwners"] = (["Native"] if summary.get("nativeExitHashes") else []) + (["official SDK"] if summary.get("sdkExitHash") else [])
            summary["walletObserver"] = observer
            stop.set()
            thread.join(timeout=10)
            save("summary.json", summary)
            for path in owned:
                path.unlink(missing_ok=True)
    print(
        json.dumps(
            {
                "evidence": str(output),
                "summary": read_response_evidence(summary, secrets),
            },
            indent=2,
        )
    )
    return summary


def unwind_sdk(
    snapshot,
    market,
    outcome,
    token,
    identity,
    builder,
    session,
    request,
    save,
    events,
    summary,
):
    """Liquidate this run's acquired token only, preserving any SDK rounding dust."""
    mid = market["id"]
    raw = int(snapshot["tokenWei"])
    b = request("GET", f"/v1/markets/{mid}/orderbook")["data"]
    bids, _ = outcome_book(b, outcome)
    amounts = builder.get_market_order_amounts(
        MarketHelperInput(side=Side.SELL, quantity_wei=raw, slippage_bps=300),
        Book(
            market_id=mid,
            update_timestamp_ms=b["updateTimestampMs"],
            asks=[],
            bids=bids,
        ),
    )
    save("sdk_exit_preparation.json", {
        "provenance":"official SDK cleanup preparation; not a submitted SELL",
        "ownedShareWei":str(raw), "book":b,
        "preparedMakerShareWei":str(amounts.maker_amount),
        "preparedMinimumCollateralWei":str(amounts.taker_amount),
        "observedAtUtc":datetime.now(UTC).isoformat(),
    })
    if not (0 < amounts.maker_amount <= raw and amounts.taker_amount >= 10**18):
        # A local admission failure is not proof of an exchange SELL rejection,
        # and a non-tiny remaining holding is not a successful cleanup.
        cash_risk=int(summary.get("ownedConfirmedCashAtRiskWei", str(10**17+1)))
        if cash_risk <= 10**17:
            summary["exitDeferred"]="confirmed cash risk fits the authorized tiny-holding allowance"
            return
        summary["exitFailure"]={
            "reason":"no_executable_depth" if amounts.maker_amount==0 else "below_local_sell_notional_policy",
            "ownedShareWei":str(raw), "confirmedCashAtRiskWei":str(cash_risk),
            "exchangeSellRejected":False,
        }
        raise CaptureError("cleanup could not sell a holding above the authorized tiny-holding allowance")
    prefix = "yield_bearing_" if market["isYieldBearing"] else ""
    exchange = getattr(
        builder.contracts,
        prefix + ("neg_risk_ctf_exchange" if market["isNegRisk"] else "ctf_exchange"),
    )
    order = builder.build_order(
        "MARKET",
        BuildOrderInput(
            side=Side.SELL,
            token_id=token,
            maker_amount=amounts.maker_amount,
            taker_amount=amounts.taker_amount,
            fee_rate_bps=market["feeRateBps"],
            nonce=exchange.functions.nonces(identity).call(),
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
    h = builder.build_typed_data_hash(typed)
    body = envelope(signed, h, amounts, {"fok": True})
    evidence = {
        "provenance": "official SDK cleanup SELL, separate from Native entry",
        "request": body,
        "observedAtUtc": datetime.now(UTC).isoformat(),
        "hash": h,
    }
    save("sdk_exit.json", evidence)
    r = session.post(
        "https://api.predict.fun/v1/orders",
        json=body,
        timeout=12,
        allow_redirects=False,
    )
    evidence.update(status=r.status_code, response=r.json())
    save("sdk_exit.json", evidence)
    summary["sdkExitHash"] = h
    for _ in range(30):
        row = request("GET", "/v1/orders/" + h)["data"]
        if row["status"] in {"FILLED", "CANCELLED", "EXPIRED", "REJECTED"}:
            break
        time.sleep(0.3)
    if row["status"] == "OPEN":
        request("POST", "/v1/orders/remove", json={"data": {"ids": [str(row["id"])]}})
    evidence["finalOrder"] = row
    save("sdk_exit.json", evidence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate")
    parser.add_argument(
        "--native-executable",
        help="previously compiled isolated Native test binary; retain its build provenance",
    )
    parser.add_argument("--search-seconds", type=int, default=360)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument(
        "--max-buy-usdt",
        choices=["1.5", "2"],
        default="1.5",
        help="Explicitly approved one-BUY principal cap; a larger cap must already be authorized",
    )
    parser.add_argument(
        "--ofi-round-trip",
        action="store_true",
        help="L1 OFI entry gate, Native post-only maker TP then bounded Native taker exit",
    )
    parser.add_argument(
        "--watch-live-book",
        action="store_true",
        help="arm Native on a liquid active crypto market and wait for a thin BBO before creating the single BUY",
    )
    parser.add_argument(
        "--allow-micro-remainder",
        action="store_true",
        help="explicit expected <=0.10 USDT partial holding until <=15-minute crypto expiry; no top-up BUY",
    )
    args = parser.parse_args()
    try:
        if not 1 <= args.search_seconds <= 900:
            raise CaptureError("search duration must be 1..900 seconds")
        if args.ofi_round_trip and not args.watch_live_book:
            raise CaptureError("OFI round trip requires Native live book monitoring")
        prepared = (
            authenticated_context() if args.submit and args.native_executable else None
        )
        deadline = time.monotonic() + args.search_seconds
        for preflight in range(10):
            remaining = int(deadline - time.monotonic())
            if remaining < 1:
                raise CaptureError("bounded search window elapsed; no external BUY")
            candidate = args.candidate or search(
                remaining,
                args.watch_live_book,
                args.allow_micro_remainder,
                args.ofi_round_trip,
                D(args.max_buy_usdt),
            )
            try:
                result = run(
                    candidate,
                    args.submit,
                    args.native_executable,
                    prepared,
                    args.ofi_round_trip,
                    D(args.max_buy_usdt),
                )
            except CaptureError as err:
                if (
                    not args.candidate
                    and str(err)
                    == "candidate no longer satisfies bounded partial admission"
                ):
                    print(
                        json.dumps(
                            {
                                "preflight": preflight,
                                "candidateChanged": True,
                                "orderWrites": 0,
                            }
                        ),
                        flush=True,
                    )
                    continue
                raise
            # A forwarded identity, including an unknown response, ends retries.
            # Never create another BUY after a transmitted order in this command.
            if (
                result.get("entryHashes")
                or result.get("finalAuditErrorType")
                or args.candidate
            ):
                break
            print(
                json.dumps(
                    {
                        "preflight": preflight,
                        "noExternalBuy": True,
                        "retryingWithinSearchWindow": True,
                    }
                ),
                flush=True,
            )
        else:
            raise CaptureError("bounded preflight retries exhausted; no external BUY")
    except Exception as e:  # noqa: BLE001 - dependency errors may contain credentials
        detail = str(e) if type(e) is CaptureError else type(e).__name__
        raise SystemExit("Partial probe stopped: " + detail) from None
