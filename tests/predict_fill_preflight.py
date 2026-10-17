"""Explicit read-only mainnet preflight; no order/approval transactions. LGPL-3.0-or-later."""

import argparse
import json
import os
import threading
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal as D

from capture_predict_private_fixtures import ROOT, CaptureError, private_toml
from predict_private_http_recorder import read_response_evidence
from predict_read_only_calibration import complete_pages
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.order_builder import ADDRESSES_BY_CHAIN_ID, make_contracts
from predict_sdk.types import Book, LimitHelperInput, MarketHelperInput, Side
from run_predict_fill_live import discover
from run_predict_resting_live import authenticated_reader
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware


def take_profit(plan, precision, delta=D(".003")):
    """Same cash floor/tick rule as Rust ExitStrategy; maker has no exit fee."""
    tick = D(10) ** -precision
    cost, net = D(plan["entryCost"]), D(plan["conservativeNetQuantity"])
    if cost <= 0 or net <= 0 or not 0 <= precision <= 6:
        raise CaptureError("invalid preflight units")
    price = max(D(plan["entryPrice"]) + delta, cost / net, D(plan["bid"]) + tick)
    price = (price / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    if price * net <= cost:
        price += tick
    if price > 1:
        raise CaptureError("no fee-covering maker price")
    return {
        "price": str(price),
        "quantity": str(net),
        "expectedMakerFeeUSDT": "0",
        "expectedCashProfitUSDT": str(price * net - cost),
        "tick": str(tick),
    }


def native_market_book_gate(book, market_id, observed_ms):
    """Mirror current submitter.rs admission, not a claimed official venue rule."""
    age = observed_ms - book["updateTimestampMs"]
    return {
        "marketIdMatches": book["marketId"] == market_id,
        "venueBookAgeMs": age,
        "allowedByCurrentAdapter": book["marketId"] == market_id and age >= 0,
        "policySource": "crates/nautilus-predict/src/execution/submitter.rs MARKET guard; requested book identity and non-future venue timestamp",
    }


def capture():
    request, identity = authenticated_reader()
    events, lock = [], threading.Lock()
    name = (
        "fill_preflight_readonly_"
        + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        + f"_{os.getpid()}"
    )
    output = (
        ROOT / "crates/nautilus-predict/tests/fixtures/private_api" / (name + ".json")
    )
    wire = output.with_suffix(".responses.jsonl").open("x")

    def get(method, path, **kwargs):
        if method != "GET" or not path.startswith("/v1/"):
            raise CaptureError("preflight permits GET only")
        started = datetime.now(UTC).isoformat()
        payload = request(method, path, **kwargs)
        with lock:
            events.append(
                {
                    "method": method,
                    "path": path,
                    "params": read_response_evidence(kwargs.get("params", {}), []),
                    "startedAtUtc": started,
                    "completedAtUtc": datetime.now(UTC).isoformat(),
                    "responseBody": read_response_evidence(payload, []),
                }
            )
            wire.write(json.dumps(events[-1], ensure_ascii=False) + "\n")
            wire.flush()
            os.fsync(wire.fileno())
        return payload

    orders = complete_pages(get, "/v1/orders", status="OPEN")
    positions = complete_pages(get, "/v1/positions", includeLost="true")
    candidate, failure = None, None
    try:
        candidate = discover(get)
        market = get("GET", f"/v1/markets/{candidate['marketId']}")["data"]
        if (
            market["tradingStatus"] != "OPEN"
            or market["feeRateBps"] != candidate["feeRateBps"]
        ):
            raise CaptureError("market metadata changed during preflight")
        profit = take_profit(candidate["plan"], market["decimalPrecision"])
        builder = OrderBuilder.make(ChainId.BNB_MAINNET)
        plan = candidate["plan"]
        wei = lambda value: int(D(value) * 10**18)
        book = candidate["book"]
        sdk_book = Book(
            market_id=book["marketId"],
            update_timestamp_ms=book["updateTimestampMs"],
            bids=book["bids"],
            asks=book["asks"],
        )
        if candidate["outcome"] == 2:
            sdk_book = Book(
                market_id=book["marketId"],
                update_timestamp_ms=book["updateTimestampMs"],
                bids=[[str(1 - D(str(p))), q] for p, q in book["asks"]],
                asks=[[str(1 - D(str(p))), q] for p, q in book["bids"]],
            )
        buy = builder.get_limit_order_amounts(
            LimitHelperInput(
                Side.BUY, wei(plan["entryPrice"]), wei(plan["grossQuantity"])
            )
        )
        maker = builder.get_limit_order_amounts(
            LimitHelperInput(Side.SELL, wei(profit["price"]), wei(profit["quantity"]))
        )
        stop = builder.get_market_order_amounts(
            MarketHelperInput(Side.SELL, wei(profit["quantity"]), slippage_bps=100),
            sdk_book,
        )
        if buy.maker_amount != wei(plan["entryCost"]) or maker.maker_amount != wei(
            profit["quantity"]
        ):
            raise CaptureError("SDK amount projection differs from plan")
        observed = next(
            e
            for e in events
            if e["path"] == f"/v1/markets/{candidate['marketId']}/orderbook"
        )
        elapsed = datetime.fromisoformat(observed["completedAtUtc"]) - datetime(
            1970, 1, 1, tzinfo=UTC
        )
        observed_ms = (
            elapsed.days * 86400000
            + elapsed.seconds * 1000
            + elapsed.microseconds // 1000
        )
        candidate.update(
            nativeMarketExitGate=native_market_book_gate(
                book, candidate["marketId"], observed_ms
            ),
            market=market,
            takeProfit=profit,
            sdkAmounts={
                key: {
                    "makerAmountWei": str(amount.maker_amount),
                    "takerAmountWei": str(amount.taker_amount),
                }
                for key, amount in (
                    ("entry", buy),
                    ("makerProfit", maker),
                    ("currentTakerExit", stop),
                )
            },
        )
    except CaptureError as error:
        failure = str(error)

    wire.close()
    settings = private_toml(ROOT / "env/predict-execution.toml")
    web3 = Web3(
        Web3.HTTPProvider(
            settings["network"]["rpc_url"], request_kwargs={"timeout": 15}
        )
    )
    web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if web3.eth.chain_id != 56:
        raise CaptureError("unexpected chain")
    contracts = make_contracts(web3, ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET])
    block = web3.eth.get_block("latest")
    usdt = web3.eth.contract(address=contracts.usdt.address, abi=contracts.usdt.abi)
    balance = usdt.functions.balanceOf(identity).call(block_identifier=block["number"])
    chain = {
        "blockNumber": block["number"],
        "blockHash": block["hash"].hex(),
        "cashWei": str(balance),
        "cashUSDT": str(D(balance) / 10**18),
    }
    if candidate is not None and failure is None:
        market = candidate["market"]
        prefix = "yield_bearing_" if market["isYieldBearing"] else ""
        exchange = getattr(
            contracts,
            prefix
            + ("neg_risk_ctf_exchange" if market["isNegRisk"] else "ctf_exchange"),
        )
        ctf = getattr(
            contracts,
            prefix
            + (
                "neg_risk_conditional_tokens"
                if market["isNegRisk"]
                else "conditional_tokens"
            ),
        )
        allowance = usdt.functions.allowance(identity, exchange.address).call(
            block_identifier=block["number"]
        )
        approved = (
            web3.eth.contract(address=ctf.address, abi=ctf.abi)
            .functions.isApprovedForAll(identity, exchange.address)
            .call(block_identifier=block["number"])
        )
        chain.update(
            buyAllowanceWei=str(allowance),
            sellApproved=approved,
            cashSufficient=balance >= int(D(candidate["plan"]["entryCost"]) * 10**18),
            allowanceSufficient=allowance
            >= int(D(candidate["plan"]["entryCost"]) * 10**18),
        )
    if web3.eth.get_block(block["number"])["hash"] != block["hash"]:
        raise CaptureError("preflight block reorganized")
    result = {
        "source": "actual mainnet GET and pinned read-only chain calls; discovery is not a profit forecast",
        "observedAtUtc": datetime.now(UTC).isoformat(),
        "currentOrders": orders,
        "currentPositions": positions,
        "chain": chain,
        "candidate": candidate,
        "discoveryFailure": failure,
        "responses": events,
        "marketScope": "existing planner: first 100 OPEN markets, at most first 60 eligible books; not exhaustive",
        "eligibleForProposedCalibration": not orders
        and not positions
        and failure is None
        and candidate is not None
        and chain.get("cashSufficient", False)
        and chain.get("allowanceSufficient", False)
        and chain.get("sellApproved", False)
        and candidate["nativeMarketExitGate"]["allowedByCurrentAdapter"],
        "capitalCapUSDT": "1.50",
        "lossTriggerUSDT": "0.15",
        "lossTriggerIsNotGuaranteedMaximum": True,
        "feesSource": "https://docs.predict.fun/the-basics/predict-fees-and-limits",
        "orderWrites": 0,
        "chainWrites": 0,
        "approvalTransactions": 0,
        "limits": [
            "single observed book, no proven reversal signal",
            "prices and admission must be refreshed before any order",
            "partial fills can leave below-minimum inventory",
            "real trading still requires explicit approval",
        ],
    }
    cleaned = read_response_evidence(result, [])
    with output.open("x") as stream:
        json.dump(cleaned, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                "file": str(output.relative_to(ROOT)),
                "openOrders": len(orders),
                "positions": len(positions),
                "cashUSDT": chain["cashUSDT"],
                "marketId": candidate["marketId"] if candidate else None,
                "eligible": cleaned["eligibleForProposedCalibration"],
                "discoveryFailure": failure,
            }
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-only", action="store_true", required=True)
    parser.parse_args()
    try:
        capture()
    except Exception as error:
        print(
            "Read-only preflight failed:",
            str(error) if type(error) is CaptureError else type(error).__name__,
        )
        raise SystemExit(1) from None
