"""Mainnet validation-only probe. Never sends a valid order signature. LGPL-3.0."""

import copy
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import requests
from capture_predict_private_fixtures import ROOT, private_toml
from eth_account import Account
from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
from web3 import Web3

STAGE = "configuration"

INVALID_SIGNATURE = "0x" + "00" * 65


def invalid_order_body(row, flag):
    """Two independent invalidations; do not sign or reuse the original signature."""
    order = copy.deepcopy(row["order"])
    order.pop("hash", None)
    order["signature"] = INVALID_SIGNATURE
    order["expiration"] = 1
    data = {
        "strategy": "LIMIT",
        "pricePerShare": "1",
        "isPostOnly": False,
        "order": order,
    }
    if flag is not None:
        data["isFillOrKill"] = flag
    return {"data": data}


def assert_unexecutable(body):
    order = body["data"]["order"]
    if order["signature"] != INVALID_SIGNATURE or order["expiration"] != 1:
        raise ValueError("probe requires invalid signature AND expired order")


def safe_diagnostic(payload, secrets):
    text = " | ".join(str(payload.get(key, "")) for key in ["error", "message"])
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[redacted]")
    text = re.sub(r"0x[0-9a-fA-F]+", "[redacted-hex]", text)
    text = re.sub(r"eyJ[A-Za-z0-9_.-]+", "[redacted-jwt]", text)
    return text[:400]


def authenticated_context():
    global STAGE
    settings = private_toml(ROOT / "env/predict-execution.toml")
    if settings["network"]["environment"] != "mainnet":
        raise ValueError("expected mainnet config")
    private = settings["privy"]
    if private.get("mode") != "predict_account":
        raise ValueError("expected Predict Account configuration")
    owner = Account.from_key(private["private_key"])
    if (
        private.get("owner_address")
        and owner.address.lower() != private["owner_address"].lower()
    ):
        raise ValueError("owner guard mismatch")
    identity = Web3.to_checksum_address(private["predict_account"])
    key_path = Path(settings["network"]["api_key_file"])
    if not key_path.is_absolute():
        key_path = ROOT / "env" / key_path
    session = requests.Session()
    api_key = private_toml(key_path)["api_key"]
    session.headers["x-api-key"] = api_key
    base = "https://api.predict.fun"

    def read(path, **kwargs):
        response = session.get(base + path, timeout=15, allow_redirects=False, **kwargs)
        if response.status_code != 200:
            raise RuntimeError(f"read HTTP {response.status_code}")
        return response.json()

    STAGE = "SDK account initialization"
    builder = OrderBuilder.make(
        ChainId.BNB_MAINNET,
        private["private_key"],
        OrderBuilderOptions(predict_account=identity),
    )
    STAGE = "authentication challenge"
    challenge = read("/v1/auth/message")["data"]["message"]
    STAGE = "authentication signing"
    response = session.post(
        base + "/v1/auth",
        json={
            "signer": identity,
            "message": challenge,
            "signature": builder.sign_predict_account_message(challenge),
        },
        timeout=15,
        allow_redirects=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"authentication HTTP {response.status_code}")
    session.headers["Authorization"] = "Bearer " + response.json()["data"]["token"]
    if read("/v1/account")["data"]["address"].lower() != identity.lower():
        raise ValueError("account mismatch")
    return settings, private, identity, owner, builder, session, read


def run():
    global STAGE
    _settings, private, identity, owner, _builder, session, read = (
        authenticated_context()
    )
    api_key = session.headers["x-api-key"]
    base = "https://api.predict.fun"
    STAGE = "historical order lookup"
    rows = []
    for status in ["FILLED", "OPEN"]:
        rows = read("/v1/orders", params={"first": 1, "status": status})["data"]
        if rows:
            break
    if not rows:
        raise ValueError("no historical order available; no probe sent")
    result = {
        "valid_order_submissions": 0,
        "fok_execution_verified": False,
        "ioc_execution_verified": False,
        "cases": [],
    }
    for label, flag in [
        ("omitted", None),
        ("true", True),
        ("false", False),
        ("invalid_type", "not-a-boolean"),
    ]:
        body = invalid_order_body(rows[0], flag)
        assert_unexecutable(body)
        STAGE = "invalid order validation: " + label
        response = session.post(
            base + "/v1/orders", json=body, timeout=15, allow_redirects=False
        )
        # Retain no raw request, response, identity, token, signature or error text.
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        diagnostic = json.dumps(payload).lower()
        markers = [
            term
            for term in [
                "isfillorkill",
                "signature",
                "expir",
                "boolean",
                "price",
                "amount",
            ]
            if term in diagnostic
        ]
        safe = safe_diagnostic(
            payload,
            [
                private["private_key"],
                api_key,
                session.headers["Authorization"].removeprefix("Bearer "),
                identity,
                owner.address,
                INVALID_SIGNATURE,
            ],
        )
        result["cases"].append(
            {
                "diagnostic": safe,
                "diagnostic_markers": markers,
                "flag": label,
                "http_status": response.status_code,
                "success": payload.get("success") is True,
                "numeric_code": payload.get("code")
                if type(payload.get("code")) is int
                else None,
            }
        )
        if response.status_code < 300 or payload.get("success") is True:
            result["unexpected_acceptance"] = True
            break
    output = (
        ROOT
        / "data/predict-validation"
        / (
            "invalid_signature_fok_"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
            + ".json"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({"evidence": str(output), **result}, indent=2))


if __name__ == "__main__":
    try:
        run()
    except Exception as error:  # noqa: BLE001 - redact dependency errors
        # Exceptions from dependencies can contain credential-bearing URLs or payloads.
        raise SystemExit(
            "probe failed at " + STAGE + ": " + type(error).__name__
        ) from None
