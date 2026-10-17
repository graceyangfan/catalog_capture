"""Generate deterministic order vectors with the official Python SDK, without RPC.

Use PredictDotFun/sdk-python 0.0.22 at 6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1.
All keys are PUBLIC Hardhat fixtures. Boundary values exercise cryptographic encoding;
these fixtures are not venue-admissible example orders.
"""

import json
from importlib.metadata import version
from pathlib import Path

from eth_account import Account
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId, Side, SignatureType
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder
from predict_sdk.types import Order

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
            for neg_risk, yield_bearing in [(False, False), (True, False),
                                           (False, True), (True, True)]:
                for side in (Side.BUY, Side.SELL):
                    identity = deposit or wallet.address
                    order = Order(
                        salt=str(1 if side == Side.BUY else 2**63 - 1),
                        maker=identity, signer=identity, taker="0x" + "0" * 40,
                        token_id=str(2**255 + 123456789 if side == Side.BUY else 2**256 - 1),
                        maker_amount=str(123456789012345678901234567 if side == Side.BUY else 2**255 + 3),
                        taker_amount=str(8765432100000000000000000009 if side == Side.BUY else 2**224 + 7),
                        expiration="4102444800", nonce=str(0 if side == Side.BUY else 2**31 - 1),
                        fee_rate_bps="250", side=side, signature_type=SignatureType.EOA,
                    )
                    data = builder.build_typed_data(order, is_neg_risk=neg_risk,
                                                    is_yield_bearing=yield_bearing)
                    signed = builder.sign_typed_data_order(data)
                    # Python 0.0.22 emits bare hex for EOA; normalize only the textual prefix.
                    signature = "0x" + signed.signature.removeprefix("0x")
                    vectors.append({
                        "chain_id": int(chain), "predict_account": deposit,
                        "is_neg_risk": neg_risk, "is_yield_bearing": yield_bearing,
                        "verifying_contract": data.domain["verifyingContract"].lower(),
                        "order": {key: value.lower() if isinstance(value, str) else int(value)
                                  for key, value in data.message.items()},
                        "hash": builder.build_typed_data_hash(data).lower(),
                        "signature": signature.lower(),
                    })
    artifact = {
        "sdk": "PredictDotFun/sdk-python", "version": "0.0.22",
        "commit": "6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1",
        "key_origin": "Hardhat public development key #0; never fund this wallet",
        "normalization": "EOA signature receives a 0x prefix; signature bytes are unchanged",
        "vectors": vectors,
    }
    Path(__file__).with_name("order_vectors.json").write_text(
        json.dumps(artifact, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    generate()
