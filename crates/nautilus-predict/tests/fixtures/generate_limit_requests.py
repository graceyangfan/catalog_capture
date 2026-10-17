"""Generate exact LIMIT request vectors with official SDK 0.0.22, without RPC.

All keys are public Hardhat fixtures. Requests validate wire compatibility only;
funding, approvals, nonce and market admission are not inferred from these vectors.
"""

import json
from importlib.metadata import version
from pathlib import Path

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId, Side
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import BuildOrderInput, LimitHelperInput

TEST_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEPOSIT = "0x1111111111111111111111111111111111111111"


def generate():
    assert version("predict_sdk") == "0.0.22"
    wallet = Account.from_key(TEST_KEY)
    vectors = []
    for chain in (ChainId.BNB_MAINNET, ChainId.BNB_TESTNET):
        for deposit in (None, DEPOSIT):
            builder = OrderBuilder(chain, 18, ADDRESSES_BY_CHAIN_ID[chain],
                                   lambda: "1", Logger("ERROR"), wallet, deposit)
            for side in (Side.BUY, Side.SELL):
                amounts = builder.get_limit_order_amounts(LimitHelperInput(
                    side=side, price_per_share_wei=620000000000000000,
                    quantity_wei=100000000000000000000))
                identity = deposit or wallet.address
                order = builder.build_order("LIMIT", BuildOrderInput(
                    side=side, token_id=str(2**255 + 123456789),
                    maker_amount=amounts.maker_amount, taker_amount=amounts.taker_amount,
                    fee_rate_bps=250, maker=identity, signer=identity, nonce=0, salt="1"))
                typed = builder.build_typed_data(order, is_neg_risk=False, is_yield_bearing=False)
                signed = builder.sign_typed_data_order(typed)
                wire = {key: value.lower() if isinstance(value, str) else int(value)
                        for key, value in typed.message.items()}
                wire["expiration"] = int(wire["expiration"])
                wire["hash"] = builder.build_typed_data_hash(typed).lower()
                wire["signature"] = ("0x" + signed.signature.removeprefix("0x")).lower()
                # Execution policy belongs to the REST envelope, not EIP-712 Order.
                for post_only in (True, False):
                    vectors.append({"chain_id": int(chain), "predict_account": deposit,
                                    "body": {"data": {"strategy": "LIMIT", "isPostOnly": post_only,
                                                      "pricePerShare": str(amounts.price_per_share),
                                                      "order": wire}}})

    artifact = {"sdk": "PredictDotFun/sdk-python", "version": "0.0.22",
                "commit": "6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1",
                "key_origin": "Hardhat public development key #0; never fund this wallet",
                "vectors": vectors}
    Path(__file__).with_name("limit_requests.json").write_text(
        json.dumps(artifact, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    generate()
