"""Regenerate offline vectors with the downloaded official Python SDK (no RPC).

Install PredictDotFun/sdk-python commit 6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1
into a disposable virtualenv, then run this file with that environment's Python.
The key below is Hardhat's PUBLIC development key; it is never an operator credential.
"""

import json
from importlib.metadata import version
from pathlib import Path

from eth_account import Account
from eth_account.messages import encode_defunct
from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID, ChainId
from predict_sdk.logger import Logger
from predict_sdk.order_builder import OrderBuilder


PUBLIC_TEST_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
DEPOSIT_ADDRESS = "0x1111111111111111111111111111111111111111"
MESSAGES = ["", "Predict.fun authentication\n nonce: offline-fixture ", "Sign in: π / 交易 🔑", "0x1234"]


def generate():
    assert version("predict_sdk") == "0.0.22"
    owner = Account.from_key(PUBLIC_TEST_KEY)
    vectors = []
    for chain in (ChainId.BNB_MAINNET, ChainId.BNB_TESTNET):
        for deposit in (None, DEPOSIT_ADDRESS):
            # Direct construction avoids make()'s RPC validation. The SDK signing method is
            # invoked unchanged; there are no mocked signature/hash functions.
            builder = OrderBuilder(
                chain_id=chain,
                precision=18,
                addresses=ADDRESSES_BY_CHAIN_ID[chain],
                generate_salt_fn=lambda: "1",
                logger=Logger("ERROR"),
                signer=owner,
                predict_account=deposit,
            )
            for message in MESSAGES:
                if deposit:
                    signature = builder.sign_predict_account_message(message)
                else:
                    signature = "0x" + owner.sign_message(encode_defunct(text=message)).signature.hex()
                vectors.append({
                    "chain_id": int(chain),
                    "predict_account": deposit,
                    "message": message,
                    "signer": deposit or owner.address.lower(),
                    "signature": signature.lower(),
                })
    artifact = {
        "sdk": "PredictDotFun/sdk-python",
        "version": "0.0.22",
        "commit": "6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1",
        "key_origin": "Hardhat public development key #0; never fund this wallet",
        "vectors": vectors,
    }
    Path(__file__).with_name("auth_vectors.json").write_text(
        json.dumps(artifact, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    generate()
