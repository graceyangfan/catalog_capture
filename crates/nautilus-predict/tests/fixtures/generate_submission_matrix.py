#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""LGPL-3.0-or-later. Public-key SDK vectors for native GTC/GTD submit tests.

Direct SDK construction is offline; account ownership is not implied. Never
fund the public Hardhat wallet. No operator configuration is read.
"""
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import patch
from pathlib import Path

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId, Side
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import BuildOrderInput, LimitHelperInput

KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEPOSIT = "0x1111111111111111111111111111111111111111"
builder = OrderBuilder(ChainId.BNB_MAINNET, 18, ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
    lambda: "1", Logger("ERROR"), Account.from_key(KEY), DEPOSIT)
vectors = []
for tif, expiry, fee in [("GTC", 4102444800, 0), ("GTD", 300, 0),
                         ("GTC", 4102444800, 200), ("GTD", 300, 200)]:
    amounts = builder.get_limit_order_amounts(LimitHelperInput(
        side=Side.BUY, price_per_share_wei=10**15, quantity_wei=1000*10**18))
    with patch("predict_sdk.order_builder.time.time", return_value=0):
        order = builder.build_order("LIMIT", BuildOrderInput(side=Side.BUY, token_id="11",
        maker=DEPOSIT, signer=DEPOSIT, maker_amount=amounts.maker_amount,
        taker_amount=amounts.taker_amount, fee_rate_bps=fee, nonce=7, salt=str(uuid.UUID("00000000-0000-4000-8000-000000000001").int % 2**31), expires_at=datetime.fromtimestamp(expiry, timezone.utc)))
    typed = builder.build_typed_data(order, is_neg_risk=False, is_yield_bearing=False)
    signed = builder.sign_typed_data_order(typed)
    wire = {key: value.lower() if isinstance(value, str) else int(value)
            for key, value in typed.message.items()}
    wire["expiration"] = int(wire["expiration"])
    wire["hash"] = builder.build_typed_data_hash(typed).lower()
    wire["signature"] = "0x" + signed.signature.removeprefix("0x").lower()
    vectors.append({"time_in_force":tif, "fee_bps":fee, "body":{"data":{"strategy":"LIMIT",
        "isPostOnly":True,"pricePerShare":str(amounts.price_per_share),"order":wire}}})
Path(__file__).with_name("submission_matrix.json").write_text(json.dumps({
    "sdk":"PredictDotFun/sdk-python", "version":"0.0.22",
    "key_origin":"public Hardhat development key; offline fixture only", "sdk_clock_epoch_secs":0, "vectors":vectors,
},indent=2)+"\n")
