#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22"]
# ///
"""Read-only official SDK signing parity; stdin/stdout never become fixture files.

LGPL-3.0-or-later. Receives a request-scoped order via stdin and reads the key
only from the existing private local TOML. No authentication/order POST or
blockchain write is performed. Outputs booleans, never signatures or keys.
"""

import json
import stat
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path

from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
from predict_sdk.types import BuildOrderInput, LimitHelperInput, Side
from web3 import Web3


def compare():
    path = Path(__file__).resolve().parents[1] / "env/predict-execution.toml"
    if not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise RuntimeError("unsafe local config permissions")
    settings = tomllib.loads(path.read_text())
    if settings["network"]["environment"] != "mainnet":
        raise RuntimeError("mainnet required")
    private = settings["privy"]
    deposit = (
        private["predict_account"]
        if private.get("mode", "predict_account") == "predict_account"
        else None
    )
    builder = OrderBuilder.make(
        ChainId.BNB_MAINNET,
        private["private_key"],
        OrderBuilderOptions(
            predict_account=Web3.to_checksum_address(deposit) if deposit else None
        ),
    )
    request = json.load(sys.stdin)
    fields = request["order"]
    amounts = builder.get_limit_order_amounts(
        LimitHelperInput(
            side=Side(fields["side"]),
            price_per_share_wei=int(request["priceWei"]),
            quantity_wei=int(
                fields["takerAmount"]
                if int(fields["side"]) == 0
                else fields["makerAmount"]
            ),
        )
    )
    order = builder.build_order(
        "LIMIT",
        BuildOrderInput(
            side=Side(fields["side"]),
            token_id=fields["tokenId"],
            maker_amount=int(fields["makerAmount"]),
            taker_amount=int(fields["takerAmount"]),
            maker=Web3.to_checksum_address(fields["maker"]),
            signer=Web3.to_checksum_address(fields["signer"]),
            nonce=int(fields["nonce"]),
            salt=fields["salt"],
            expires_at=datetime.fromtimestamp(int(fields["expiration"]), UTC),
            fee_rate_bps=int(fields["feeRateBps"]),
        ),
    )
    typed = builder.build_typed_data(
        order,
        is_neg_risk=request["isNegRisk"],
        is_yield_bearing=request["isYieldBearing"],
    )
    signed = builder.sign_typed_data_order(typed)
    expected_order = {
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
    expected_body = {
        "data": {
            "order": expected_order,
            "pricePerShare": str(amounts.price_per_share),
            "strategy": "LIMIT",
            "isPostOnly": True,
        }
    }
    return {
        "request_body_matches": expected_body == request.get("body"),
        "amounts_match": amounts.maker_amount == int(fields["makerAmount"])
        and amounts.taker_amount == int(fields["takerAmount"]),
        "hash_matches": builder.build_typed_data_hash(typed).lower()
        == request["hash"].lower(),
        "signature_matches": signed.signature.removeprefix("0x").lower()
        == request["signature"].removeprefix("0x").lower(),
    }


if __name__ == "__main__":
    try:
        print(json.dumps(compare()))
    except Exception as error:  # noqa: BLE001 - never expose remote/credential-bearing exception text
        print(json.dumps({"error_type": type(error).__name__}))
        raise SystemExit(1) from None
