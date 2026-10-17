#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""Offline official SDK request vectors for minimum-size resting terms. LGPL-3.0-or-later.

Only a public Hardhat key is used. No operator configuration, authentication,
RPC or order HTTP request is read/sent. Explicit constructor omits contracts.
"""

import json
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from eth_account import Account
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, MAX_SALT
from predict_sdk.logger import Logger
from predict_sdk.types import BuildOrderInput, LimitHelperInput, Side
from web3 import Web3

PUBLIC_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ACCOUNT = "0x1111111111111111111111111111111111111111"
COMMANDS = [
    "12345678-1234-4234-9234-123456789001",
    "12345678-1234-4234-9234-123456789002",
]


def vectors():
    result = []
    for account in (None, ACCOUNT):
        builder = OrderBuilder(
            ChainId.BNB_MAINNET,
            18,
            ADDRESSES_BY_CHAIN_ID[ChainId.BNB_MAINNET],
            lambda: "1",
            Logger("ERROR"),
            Account.from_key(PUBLIC_KEY),
            account,
        )
        for neg_risk, yield_bearing in (
            (False, False),
            (True, False),
            (False, True),
            (True, True),
        ):
            for index, (price, quantity) in enumerate(
                ((10**16, 100 * 10**18), (2 * 10**16, 50 * 10**18))
            ):
                for profile in (
                    "sdk_default_range",
                    "runtime_uuid",
                    "runtime_sdk_range",
                ):
                    salt = (
                        MAX_SALT
                        if profile == "sdk_default_range"
                        else UUID(COMMANDS[index]).int
                    )
                    if profile == "runtime_sdk_range":
                        salt %= MAX_SALT
                    amounts = builder.get_limit_order_amounts(
                        LimitHelperInput(Side.BUY, price, quantity)
                    )
                    order = builder.build_order(
                        "LIMIT",
                        BuildOrderInput(
                            side=Side.BUY,
                            token_id="11",
                            maker_amount=amounts.maker_amount,
                            taker_amount=amounts.taker_amount,
                            fee_rate_bps=200,
                            nonce=7,
                            salt=str(salt),
                            expires_at=datetime(2100, 1, 1, tzinfo=UTC),
                        ),
                    )
                    typed = builder.build_typed_data(
                        order, is_neg_risk=neg_risk, is_yield_bearing=yield_bearing
                    )
                    signed = builder.sign_typed_data_order(typed)
                    camel = {
                        "salt": signed.salt,
                        "maker": signed.maker.lower(),
                        "signer": signed.signer.lower(),
                        "taker": signed.taker.lower(),
                        "tokenId": signed.token_id,
                        "makerAmount": signed.maker_amount,
                        "takerAmount": signed.taker_amount,
                        "expiration": int(signed.expiration),
                        "nonce": signed.nonce,
                        "feeRateBps": signed.fee_rate_bps,
                        "side": int(signed.side),
                        "signatureType": int(signed.signature_type),
                        "hash": builder.build_typed_data_hash(typed),
                        "signature": "0x" + signed.signature.removeprefix("0x").lower(),
                    }
                    result.append(
                        {
                            "chainId": 56,
                            "predictAccount": account,
                            "isNegRisk": neg_risk,
                            "isYieldBearing": yield_bearing,
                            "saltProfile": profile,
                            "commandId": COMMANDS[index],
                            "body": {
                                "data": {
                                    "strategy": "LIMIT",
                                    "isPostOnly": True,
                                    "pricePerShare": str(amounts.price_per_share),
                                    "order": camel,
                                }
                            },
                        }
                    )
    return result


if __name__ == "__main__":
    # Fail loudly if a future SDK version attempts RPC/HTTP during these offline vectors.
    with patch.object(
        Web3.HTTPProvider,
        "make_request",
        side_effect=AssertionError("offline fixture forbids RPC"),
    ):
        data = {
            "sdk": "PredictDotFun/sdk-python",
            "version": version("predict-sdk"),
            "sdkMaxDefaultSalt": MAX_SALT,
            "provenance": "offline public Hardhat key; no operator credential or mainnet request; SDK expiry string normalized to OpenAPI int64; hex byte strings canonicalized with lowercase 0x prefix",
            "vectors": vectors(),
        }
    path = (
        Path(__file__).resolve().parents[1]
        / "crates/nautilus-predict/tests/fixtures/runtime_limit_requests.json"
    )
    path.write_text(json.dumps(data, indent=2) + "\n")
    print(
        f"Saved {len(data['vectors'])} official SDK request vectors; no live requests."
    )
