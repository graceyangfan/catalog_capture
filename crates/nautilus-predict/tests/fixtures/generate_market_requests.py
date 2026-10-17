#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""LGPL-3.0-or-later. Offline MARKET oracle using the public Hardhat development key.

No operator configuration, network, approvals or funded wallets are used.
"""

import json
from pathlib import Path
from unittest.mock import patch

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId, Side
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import Book, BuildOrderInput, MarketHelperInput

KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ACCOUNT = "0x1111111111111111111111111111111111111111"
CLOCK = 1_800_000_000
vectors = []
for account in (None, ACCOUNT):
    builder = OrderBuilder(
        ChainId.BNB_MAINNET,
        18,
        ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
        lambda: "123",
        Logger("ERROR"),
        Account.from_key(KEY),
        account,
    )
    for multi in (False, True):
        asks = [("0.01", "40"), ("0.02", "60")] if multi else [("0.01", "200")]
        bids = [("0.009", "40"), ("0.008", "60")] if multi else [("0.009", "200")]
        book = Book(
            market_id=42, update_timestamp_ms=CLOCK * 1000, asks=asks, bids=bids
        )
        for side in (Side.BUY, Side.SELL):
            for slippage in (0, 100):
                amounts = builder.get_market_order_amounts(
                    MarketHelperInput(
                        side=side, quantity_wei=100 * 10**18, slippage_bps=slippage
                    ),
                    book,
                )
                with patch("predict_sdk.order_builder.time.time", return_value=CLOCK):
                    order = builder.build_order(
                        "MARKET",
                        BuildOrderInput(
                            side=side,
                            token_id="11",
                            maker_amount=amounts.maker_amount,
                            taker_amount=amounts.taker_amount,
                            fee_rate_bps=200,
                            nonce=7,
                            salt="123",
                        ),
                    )
                for neg in (False, True):
                    for yielding in (False, True):
                        typed = builder.build_typed_data(
                            order, is_neg_risk=neg, is_yield_bearing=yielding
                        )
                        signed = builder.sign_typed_data_order(typed)
                        wire = {
                            k: v.lower() if isinstance(v, str) else int(v)
                            for k, v in typed.message.items()
                        }
                        wire["expiration"] = int(wire["expiration"])
                        wire["hash"] = builder.build_typed_data_hash(typed).lower()
                        wire["signature"] = (
                            "0x" + signed.signature.removeprefix("0x").lower()
                        )
                        vectors.append(
                            {
                                "predictAccount": account,
                                "isNegRisk": neg,
                                "isYieldBearing": yielding,
                                "side": int(side),
                                "quantityWei": str(100 * 10**18),
                                "levels": asks if side == Side.BUY else bids,
                                "lastPrice": str(amounts.last_price),
                                "body": {
                                    "data": {
                                        "strategy": "MARKET",
                                        "pricePerShare": str(amounts.price_per_share),
                                        "slippageBps": slippage,
                                        "order": wire,
                                    }
                                },
                            }
                        )

Path(__file__).with_name("market_requests.json").write_text(
    json.dumps(
        {
            "source": "official SDK 0.0.22; offline public Hardhat key; full quantity, default (not minimum-out) slippage",
            "sdkClockEpochSeconds": CLOCK,
            "vectors": vectors,
        },
        indent=2,
    )
    + "\n"
)
