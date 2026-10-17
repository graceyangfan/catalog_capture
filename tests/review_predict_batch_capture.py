#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3"]
# ///
"""Read-only SDK hash/amount check of captured batch orders. No API calls. LGPL-3.0-or-later."""

import argparse
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from capture_predict_private_fixtures import ROOT, private_toml
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.types import BuildOrderInput, LimitHelperInput, Side
from web3 import Web3


def review(output):
    settings = private_toml(ROOT / "env/predict-execution.toml")
    identity = Web3.to_checksum_address(settings["privy"]["predict_account"])
    builder = OrderBuilder.make(ChainId.BNB_MAINNET)
    events = json.loads((output / "http_responses.json").read_text())["events"]
    posts = [r for r in events if r["method"] == "POST" and r["path"] == "/v1/orders"]
    checks = []
    for post in posts:
        terms = post["requestTerms"]
        wire = next(
            r["responseBody"]["data"]
            for r in events
            if r["method"] == "GET"
            and r["path"].endswith(terms["hash"])
            and r.get("status") == 200
        )
        fields = wire["order"]
        assert fields["hash"] == terms["hash"]
        for key in terms.keys() - {"hash"}:
            assert str(fields[key]) == str(terms[key]), key
        amount = int(fields["takerAmount"])
        maker = int(fields["makerAmount"])
        price = maker * 10**18 // amount
        amounts = builder.get_limit_order_amounts(
            LimitHelperInput(
                side=Side.BUY, price_per_share_wei=price, quantity_wei=amount
            )
        )
        order = builder.build_order(
            "LIMIT",
            BuildOrderInput(
                side=Side.BUY,
                token_id=fields["tokenId"],
                maker_amount=maker,
                taker_amount=amount,
                maker=identity,
                signer=identity,
                nonce=int(fields["nonce"]),
                salt=fields["salt"],
                expires_at=datetime.fromtimestamp(int(fields["expiration"]), UTC),
                fee_rate_bps=int(fields["feeRateBps"]),
            ),
        )
        typed = builder.build_typed_data(
            order,
            is_neg_risk=wire["isNegRisk"],
            is_yield_bearing=wire["isYieldBearing"],
        )
        assert builder.build_typed_data_hash(typed).lower() == fields["hash"].lower()
        assert amounts.maker_amount == maker and amounts.taker_amount == amount
        assert post["responseBody"]["data"]["orderHash"] == fields["hash"]
        assert post["responseBody"]["data"]["orderId"] == wire["id"]
        assert post["responseBody"]["data"]["code"] == "OK"
        checks.append(
            {
                "hash": fields["hash"],
                "decimalOrderId": wire["id"],
                "officialSdkHashMatches": True,
                "officialSdkAmountsMatch": True,
                "receiptMatchesRestHashAndId": True,
                "nonce": str(fields["nonce"]),
                "feeRateBps": str(fields["feeRateBps"]),
                "expiration": str(fields["expiration"]),
                "makerUSDT": str(Decimal(maker) / 10**18),
                "quantity": str(Decimal(amount) / 10**18),
            }
        )
    assert len(checks) == 2
    result = {
        "source": "derived offline SDK 0.0.22 check of actual signed terms and subsequent REST rows; identities restored only in memory from local config",
        "apiCalls": 0,
        "orders": checks,
        "signatureRecomputed": False,
        "signatureNote": "signatures deliberately not retained; hash and amounts checked, not a new signature equality claim",
    }
    with (output / "official_sdk_validation.json").open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        result = review(args.directory)
    except Exception as error:  # never print local secrets or remote payload
        print("SDK review failed:", type(error).__name__)
        raise SystemExit(1) from None
    print(
        "Official SDK hashes, amounts and REST receipt identities match:",
        len(result["orders"]),
    )
