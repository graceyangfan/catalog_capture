#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""LGPL-3.0-or-later. Official SDK unsigned value BUY oracle; no network/wallet."""

import json
from decimal import Decimal
from pathlib import Path

from predict_sdk import ChainId, OrderBuilder
from predict_sdk.types import Book, MarketHelperValueInput, Side

builder = OrderBuilder.make(ChainId.BNB_MAINNET)
vectors = []
for value, price in [
    ("1", ".01"),
    ("2", ".02"),
    ("2.000001", ".02"),
    ("1", ".31"),
    ("1.01", ".02"),
    ("12.345678", ".37"),
    ("1", "1"),
]:
    amounts = builder.get_market_order_amounts(
        MarketHelperValueInput(
            side=Side.BUY, value_wei=int(Decimal(value) * 10**18), slippage_bps=0
        ),
        Book(market_id=42, update_timestamp_ms=0, asks=[(price, "1000000")], bids=[]),
    )
    vectors.append(
        {
            "value": value,
            "price": price,
            "makerAmount": str(amounts.maker_amount),
            "takerAmount": str(amounts.taker_amount),
            "amount": str(amounts.amount),
            "pricePerShare": str(amounts.price_per_share),
        }
    )
Path(__file__).with_name("market_value_amounts.json").write_text(
    json.dumps(
        {
            "source": "Official PredictDotFun/sdk-python predict-sdk==0.0.22, unsigned single-reference-level value BUY, zero slippage",
            "vectors": vectors,
        },
        indent=2,
    )
    + "\n"
)
