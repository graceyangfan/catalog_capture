"""Read-only mainnet evidence collector; no execution/recording code. LGPL-3.0-or-later."""

import json
import time
from collections import defaultdict
from contextlib import closing
from datetime import UTC, datetime

import websocket
from web3 import Web3
from web3.logs import DISCARD
from web3.middleware import ExtraDataToPOAMiddleware


def complete_pages(request, path, **params):
    rows, seen, cursor = [], set(), None
    for _ in range(20):
        payload = request(
            "GET",
            path,
            params={"first": 100, **params, **({"after": cursor} if cursor else {})},
        )
        if payload.get("success") is not True or not isinstance(
            payload.get("data"), list
        ):
            raise ValueError("invalid page envelope")
        rows.extend(payload["data"])
        cursor = payload.get("cursor")
        if not cursor:
            return rows
        if cursor in seen:
            raise ValueError("repeated pagination cursor")
        seen.add(cursor)
    raise ValueError("incomplete bounded pagination")


def observe_wallet(
    identity,
    jwt,
    api_key,
    observation_secs=60,
    on_subscribed=None,
    stop_requested=None,
    on_wallet_event=None,
):
    """Test-only observer: one connection until elapsed window or explicit host completion.

    No reconnect, token refresh, order submission, or recovery ownership is introduced.
    A failed connection propagates; successfully observed events can be durably retained.
    """
    events, heartbeats, ack = [], 0, False
    topic = "predictWalletEvents/" + jwt
    started = time.monotonic()
    observed_started = None
    if observation_secs is None and stop_requested is None:
        raise ValueError("continuous observation requires an explicit stop source")
    with closing(
        websocket.create_connection(
            "wss://ws.predict.fun/ws", header={"x-api-key": api_key}, timeout=10
        )
    ) as socket:
        socket.send(
            json.dumps({"method": "subscribe", "requestId": 1, "params": [topic]})
        )
        while not (stop_requested is not None and stop_requested()) and (
            observed_started is None
            or observation_secs is None
            or time.monotonic() - observed_started < observation_secs
        ):
            socket.settimeout(5)
            try:
                frame = json.loads(socket.recv())
            except websocket.WebSocketTimeoutException:
                if not ack and time.monotonic() - started > 15:
                    raise ValueError("subscription ACK timeout")
                continue
            if frame.get("type") == "R" and frame.get("requestId") == 1:
                if frame.get("success") is not True:
                    raise ValueError("wallet subscription rejected")
                ack = True
                if observed_started is None:
                    observed_started = time.monotonic()
                    if on_subscribed is not None:
                        on_subscribed()
            elif frame.get("type") == "M" and frame.get("topic") == "heartbeat":
                heartbeats += 1
                socket.send(json.dumps({"method": "heartbeat", "data": frame["data"]}))
            elif frame.get("type") == "M" and frame.get("topic") == topic:
                event = frame["data"]
                if event.get("walletAddress", "").lower() != identity.lower():
                    raise ValueError("foreign wallet event")
                if on_wallet_event is not None:
                    on_wallet_event(event)
                events.append(event)
    if not ack:
        raise ValueError("missing subscription ACK")
    observed_seconds = time.monotonic() - observed_started
    return events, heartbeats, observed_seconds


def collect(
    request,
    builder,
    identity,
    jwt,
    api_key,
    timeout,
    observation_secs=60,
    on_subscribed=None,
    stop_requested=None,
    on_wallet_event=None,
):
    web3 = Web3(
        Web3.HTTPProvider(
            "https://bsc-dataseed.bnbchain.org", request_kwargs={"timeout": timeout}
        )
    )
    web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    if web3.eth.chain_id != 56:
        raise ValueError("wrong chain")
    contracts = builder.contracts
    receipts = {}

    def snapshot():
        block = web3.eth.get_block("latest")
        block_id = block["number"]
        matches = complete_pages(request, "/v1/orders/matches", signerAddress=identity)
        positions = complete_pages(request, "/v1/positions", includeLost="true")
        orders = complete_pages(request, "/v1/orders")
        cash = web3.eth.contract(address=contracts.usdt.address, abi=contracts.usdt.abi)
        balance = cash.functions.balanceOf(identity).call(block_identifier=block_id)
        tokens = {}
        for row in matches:
            market = row["market"]
            prefix = "yield_bearing_" if market["isYieldBearing"] else ""
            ctf = getattr(
                contracts,
                prefix
                + (
                    "neg_risk_conditional_tokens"
                    if market["isNegRisk"]
                    else "conditional_tokens"
                ),
            )
            for leg in [row["taker"], *row["makers"]]:
                if leg["signer"].lower() == identity.lower():
                    token = leg["outcome"]["onChainId"]
                    contract = web3.eth.contract(address=ctf.address, abi=ctf.abi)
                    tokens[token] = {
                        "contract": ctf.address,
                        "balanceWei": str(
                            contract.functions.balanceOf(identity, int(token)).call(
                                block_identifier=block_id
                            )
                        ),
                    }
        for position in positions:
            market = position["market"]
            prefix = "yield_bearing_" if market["isYieldBearing"] else ""
            ctf = getattr(
                contracts,
                prefix
                + (
                    "neg_risk_conditional_tokens"
                    if market["isNegRisk"]
                    else "conditional_tokens"
                ),
            )
            token = position["outcome"]["onChainId"]
            contract = web3.eth.contract(address=ctf.address, abi=ctf.abi)
            tokens[token] = {
                "contract": ctf.address,
                "balanceWei": str(
                    contract.functions.balanceOf(identity, int(token)).call(
                        block_identifier=block_id
                    )
                ),
            }
        position_checks = [
            {
                "tokenId": p["outcome"]["onChainId"],
                "apiWei": p["amount"],
                "chainWei": tokens[p["outcome"]["onChainId"]]["balanceWei"],
                "equal": int(p["amount"])
                == int(tokens[p["outcome"]["onChainId"]]["balanceWei"]),
            }
            for p in positions
        ]
        account = request("GET", "/v1/account")
        if account["data"]["address"].lower() != identity.lower():
            raise ValueError("snapshot account identity mismatch")
        if web3.eth.get_block(block_id)["hash"] != block["hash"]:
            raise ValueError("snapshot block reorganized")
        return {
            "block": block_id,
            "blockHash": block["hash"].hex(),
            "observedAt": datetime.now(UTC).isoformat(),
            "account": account,
            "positionChecks": position_checks,
            "matches": matches,
            "positions": positions,
            "orders": orders,
            "cashWei": str(balance),
            "tokenBalances": tokens,
        }

    before = snapshot()
    events, heartbeats, observed_seconds = observe_wallet(
        identity,
        jwt,
        api_key,
        observation_secs,
        on_subscribed,
        stop_requested,
        on_wallet_event,
    )
    ack = True
    after = snapshot()
    comparisons, cumulative, seen = [], defaultdict(lambda: [0, 0, 0]), {}
    for row in after["matches"]:
        market = row["market"]
        prefix = "yield_bearing_" if market["isYieldBearing"] else ""
        exchange = getattr(
            contracts,
            prefix
            + ("neg_risk_ctf_exchange" if market["isNegRisk"] else "ctf_exchange"),
        )
        tx = row["transactionHash"]
        if tx not in receipts:
            receipts[tx] = web3.eth.get_transaction_receipt(tx)
        receipt = receipts[tx]
        if receipt["status"] != 1 or receipt["blockNumber"] > after["block"]:
            raise ValueError("unsuccessful or unpinned settlement")
        decoded = (
            web3.eth.contract(address=exchange.address, abi=exchange.abi)
            .events.OrderFilled()
            .process_receipt(receipt, errors=DISCARD)
        )
        for leg in [row["taker"], *row["makers"]]:
            if leg["signer"].lower() != identity.lower():
                continue
            key = (row["settlementId"], leg["hash"])
            if key in seen:
                if seen[key] != leg:
                    raise ValueError("contradictory duplicate match")
                continue
            seen[key] = leg
            own = [
                dict(e["args"])
                for e in decoded
                if e["address"].lower() == exchange.address.lower()
                and e["args"]["orderHash"].hex().removeprefix("0x")
                == leg["hash"].removeprefix("0x")
                and e["args"]["maker"].lower() == identity.lower()
            ]
            if len(own) != 1:
                raise ValueError("ambiguous own OrderFilled")
            args = own[0]
            gross, fee = int(leg["amount"]), int(leg["fee"]["amount"])
            is_buy = leg["quoteType"] == "Bid"
            if int(args["takerAssetId"] if is_buy else args["makerAssetId"]) != int(
                leg["outcome"]["onChainId"]
            ):
                raise ValueError("REST/chain outcome token mismatch")
            chain_gross = int(
                args["takerAmountFilled"] if is_buy else args["makerAmountFilled"]
            )
            if gross != chain_gross or fee != int(args["fee"]):
                raise ValueError("REST/chain gross or fee mismatch")
            flow = settlement_flow(is_buy, gross, fee, leg["fee"]["type"])
            token = int(leg["outcome"]["onChainId"])
            ctf = getattr(
                contracts,
                prefix
                + (
                    "neg_risk_conditional_tokens"
                    if market["isNegRisk"]
                    else "conditional_tokens"
                ),
            )
            transfers = (
                web3.eth.contract(address=ctf.address, abi=ctf.abi)
                .events.TransferSingle()
                .process_receipt(receipt, errors=DISCARD)
            )
            net = sum(
                (
                    int(e["args"]["value"])
                    if e["args"]["to"].lower() == identity.lower()
                    else 0
                )
                - (
                    int(e["args"]["value"])
                    if e["args"]["from"].lower() == identity.lower()
                    else 0
                )
                for e in transfers
                if e["address"].lower() == ctf.address.lower()
                and int(e["args"]["id"]) == token
            )
            # Receipt-wide transfer totals are only attributable if this own token has one fill.
            own_token_fills = [
                e
                for e in decoded
                if e["args"]["maker"].lower() == identity.lower()
                and token
                in (int(e["args"]["takerAssetId"]), int(e["args"]["makerAssetId"]))
            ]
            if len(own_token_fills) != 1 or net != flow:
                raise ValueError("ambiguous or inconsistent net token transfer")
            cumulative[leg["hash"]][0] += gross
            cumulative[leg["hash"]][1] += fee
            cumulative[leg["hash"]][2] += net
            comparisons.append(
                {
                    "settlementId": key[0],
                    "orderHash": key[1],
                    "transactionHash": tx,
                    "block": receipt["blockNumber"],
                    "tokenId": str(token),
                    "grossWei": str(gross),
                    "feeWei": str(fee),
                    "netTransferWei": str(net),
                    "collateralWei": str(
                        args["makerAmountFilled"]
                        if is_buy
                        else args["takerAmountFilled"]
                    ),
                    "feeType": leg["fee"]["type"],
                    "side": "BUY" if is_buy else "SELL",
                }
            )
    return {
        "source": "live read-only; SDK 0.0.22 ABI; no order writes",
        "before": before,
        "after": after,
        "wallet": {
            "subscribed": ack,
            "seconds": round(observed_seconds, 3),
            "heartbeats": heartbeats,
            "events": events,
        },
        "comparisons": comparisons,
        "cumulative": {
            key: {
                "grossWei": str(v[0]),
                "feeWei": str(v[1]),
                "netWei": str(v[2]),
            }
            for key, v in cumulative.items()
        },
        "summary": {
            "matches": len(after["matches"]),
            "chainVerifiedOwnFills": len(comparisons),
            "walletEvents": len(events),
            "heartbeats": heartbeats,
            "positions": len(after["positions"]),
            "openOrders": len(after["orders"]),
            "cashWeiBefore": before["cashWei"],
            "cashWeiAfter": after["cashWei"],
            "cashUnchanged": before["cashWei"] == after["cashWei"],
        },
    }


def settlement_flow(is_buy, gross, fee, fee_type):
    """Expected own ERC-1155 delta; collateral fees do not change share inventory."""
    if gross <= 0 or fee < 0 or fee_type not in {"SHARES", "COLLATERAL"}:
        raise ValueError("invalid settlement fee facts")
    if fee_type == "SHARES":
        if not is_buy or fee > gross:
            raise ValueError("unsupported SHARES settlement direction")
        return gross - fee
    return gross if is_buy else -gross
