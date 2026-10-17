#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""LGPL-3.0-or-later. SDK oracle from saved live public snapshots; no network or credentials."""

import json
from decimal import Decimal
from pathlib import Path

from predict_sdk import ChainId, OrderBuilder
from predict_sdk.constants import Side
from predict_sdk.types import Book, MarketHelperInput

BASE = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


def atomic(text):
    value = Decimal(text) * 10**18
    if value != value.to_integral_value() or value < 0:
        raise ValueError("public snapshot amount cannot be represented in wei")
    return int(value)


def compare(source):
    builder = OrderBuilder.make(ChainId.BNB_MAINNET)
    vectors = []
    for index, snapshot in enumerate(source["snapshots"]):
        for outcome in (1, 2):
            if outcome == 1:
                bids, asks = snapshot["bids"], snapshot["asks"]
            else:
                bids = [[str(1 - Decimal(p)), q] for p, q in snapshot["asks"]]
                asks = [[str(1 - Decimal(p)), q] for p, q in snapshot["bids"]]
            book = Book(
                source["marketId"], snapshot["updateTimestampMs"], asks=asks, bids=bids
            )
            for side in (Side.BUY, Side.SELL):
                amounts = builder.get_market_order_amounts(
                    MarketHelperInput(
                        side=side, quantity_wei=100 * 10**18, slippage_bps=0
                    ),
                    book,
                )
                vectors.append(
                    {
                        "snapshotIndex": index,
                        "outcome": outcome,
                        "side": int(side),
                        "quantityWei": str(100 * 10**18),
                        "slippageBps": 0,
                        "levelsWei": [
                            [str(atomic(p)), str(atomic(q))]
                            for p, q in (asks if side == Side.BUY else bids)
                        ],
                        "sdkFilledDepthWei": str(amounts.amount),
                        "nativeFullQuantityAdmissible": amounts.amount == 100 * 10**18,
                        "expected": {
                            "pricePerShare": str(amounts.price_per_share),
                            "lastPrice": str(amounts.last_price),
                            "makerAmount": str(amounts.maker_amount),
                            "takerAmount": str(amounts.taker_amount),
                        },
                    }
                )
    return {
        "source": "offline SDK 0.0.22 amounts from actual saved Rust strategy snapshots; hypothetical quotes, not orders or fills",
        "input": "market_read_only_livenode_20261015.json",
        "tradeAllowed": False,
        "vectors": vectors,
    }


if __name__ == "__main__":
    source = json.loads((BASE / "market_read_only_livenode_20261015.json").read_text())
    output = compare(source)
    with (BASE / "market_live_snapshot_sdk_quotes_20261015.json").open("x") as stream:
        json.dump(output, stream, indent=2)
        stream.write("\n")
    print(
        f"Saved {len(output['vectors'])} hypothetical SDK comparisons; no network or order submission."
    )
