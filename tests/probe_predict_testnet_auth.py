#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "eth-account>=0.13,<0.14",
#   "python-dotenv>=1,<2",
#   "requests>=2.32,<3",
# ]
# ///
"""Authenticate a dedicated EOA against Predict Testnet and inspect balances.

This probe is deliberately testnet-only and read-only. It signs the dynamic
Predict login message, verifies the returned JWT with ``GET /v1/account``, and
reads tBNB plus the exact Predict Testnet USDT contract balance. It never prints
the private key, signature, auth message, API key, or JWT.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable, Mapping
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_defunct

TESTNET_API_URL = "https://api-testnet.predict.fun"
TESTNET_API_HOST = "api-testnet.predict.fun"
TESTNET_RPC_URL = "https://bsc-testnet-dataseed.bnbchain.org"
TESTNET_CHAIN_ID = 97
PREDICT_TESTNET_USDT = "0xB32171ecD878607FFc4F8FC0bCcE6852BB3149E0"
DEFAULT_TIMEOUT_SECONDS = 15.0

JsonObject = dict[str, Any]
RequestJson = Callable[..., JsonObject]


class ProbeError(RuntimeError):
    """A safe-to-display probe failure which contains no credentials."""


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return None


def _validate_testnet_api_url(url: str) -> str:
    normalized = url.rstrip("/")
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or parsed.hostname != TESTNET_API_HOST:
        raise ProbeError(
            "this probe is testnet-only; PREDICT_TESTNET_API_URL must be "
            f"https://{TESTNET_API_HOST}",
        )
    return normalized


def _safe_error(response: requests.Response, operation: str) -> ProbeError:
    # Private endpoints may echo submitted authentication fields in any error field.
    return ProbeError(f"{operation} failed with HTTP {response.status_code}")


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    operation: str,
    timeout: float,
    headers: Mapping[str, str] | None = None,
    body: Mapping[str, Any] | None = None,
) -> JsonObject:
    try:
        response = session.request(
            method,
            url,
            headers=dict(headers or {}),
            json=dict(body) if body is not None else None,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ProbeError(
            f"{operation} network request failed: {exc.__class__.__name__}"
        ) from exc

    if not response.ok:
        raise _safe_error(response, operation)

    try:
        payload = response.json()
    except ValueError as exc:
        raise ProbeError(f"{operation} returned non-JSON data") from exc
    if not isinstance(payload, dict):
        raise ProbeError(f"{operation} returned an unexpected JSON shape")
    return payload


def _api_headers(api_key: str | None, jwt: str | None = None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "nautilus-predict-testnet-probe/1",
    }
    if api_key:
        headers["x-api-key"] = api_key
    if jwt:
        headers["Authorization"] = f"Bearer {jwt}"
    return headers


def authenticate_eoa(
    session: requests.Session,
    api_url: str,
    private_key: str,
    api_key: str | None,
    timeout: float,
    *,
    request: RequestJson = request_json,
) -> tuple[str, str]:
    try:
        account = Account.from_key(private_key)
    except (TypeError, ValueError) as exc:
        raise ProbeError(
            "PREDICT_TESTNET_PRIVATE_KEY is not a valid EVM private key"
        ) from exc

    message_payload = request(
        session,
        "GET",
        f"{api_url}/v1/auth/message",
        operation="get auth message",
        timeout=timeout,
        headers=_api_headers(api_key),
    )
    message = message_payload.get("data", {}).get("message")
    if (
        message_payload.get("success") is not True
        or not isinstance(message, str)
        or not message
    ):
        raise ProbeError("get auth message returned no usable message")

    signed = Account.sign_message(encode_defunct(text=message), private_key=private_key)
    signature = signed.signature.hex()
    if not signature.startswith("0x"):
        signature = f"0x{signature}"

    auth_payload = request(
        session,
        "POST",
        f"{api_url}/v1/auth",
        operation="exchange wallet signature for JWT",
        timeout=timeout,
        headers={**_api_headers(api_key), "Content-Type": "application/json"},
        body={
            "signer": account.address,
            "signature": signature,
            "message": message,
        },
    )
    token = auth_payload.get("data", {}).get("token")
    if (
        auth_payload.get("success") is not True
        or not isinstance(token, str)
        or not token
    ):
        raise ProbeError("wallet authentication returned no usable JWT")
    return account.address, token


def verify_account(
    session: requests.Session,
    api_url: str,
    api_key: str | None,
    jwt: str,
    timeout: float,
    *,
    request: RequestJson = request_json,
) -> None:
    payload = request(
        session,
        "GET",
        f"{api_url}/v1/account",
        operation="verify wallet JWT",
        timeout=timeout,
        headers=_api_headers(api_key, jwt),
    )
    if payload.get("success") is not True or not isinstance(
        payload.get("data"), Mapping
    ):
        raise ProbeError("wallet JWT verification returned an unexpected response")


def rpc_call(
    session: requests.Session,
    rpc_url: str,
    method: str,
    params: list[Any],
    timeout: float,
    *,
    request_id: int,
) -> str:
    try:
        response = session.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ProbeError(
            f"BNB Testnet RPC request failed: {exc.__class__.__name__}"
        ) from exc

    if not response.ok:
        raise ProbeError(f"BNB Testnet RPC failed with HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise ProbeError("BNB Testnet RPC returned non-JSON data") from exc
    if not isinstance(payload, Mapping) or not isinstance(payload.get("result"), str):
        raise ProbeError("BNB Testnet RPC returned an error or unexpected response")
    return payload["result"]


def _erc20_call_data(selector: str, address: str | None = None) -> str:
    if address is None:
        return selector
    normalized = address.removeprefix("0x")
    if len(normalized) != 40:
        raise ProbeError("derived wallet address has an unexpected shape")
    return f"{selector}{normalized.lower().rjust(64, '0')}"


def read_testnet_balances(
    session: requests.Session,
    rpc_url: str,
    address: str,
    timeout: float,
) -> tuple[int, int, int]:
    chain_id = int(
        rpc_call(session, rpc_url, "eth_chainId", [], timeout, request_id=1), 16
    )
    if chain_id != TESTNET_CHAIN_ID:
        raise ProbeError(
            f"PREDICT_TESTNET_RPC_URL returned chain ID {chain_id}, expected {TESTNET_CHAIN_ID}",
        )

    tbnb_wei = int(
        rpc_call(
            session,
            rpc_url,
            "eth_getBalance",
            [address, "latest"],
            timeout,
            request_id=2,
        ),
        16,
    )
    decimals = int(
        rpc_call(
            session,
            rpc_url,
            "eth_call",
            [
                {"to": PREDICT_TESTNET_USDT, "data": _erc20_call_data("0x313ce567")},
                "latest",
            ],
            timeout,
            request_id=3,
        ),
        16,
    )
    usdt_raw = int(
        rpc_call(
            session,
            rpc_url,
            "eth_call",
            [
                {
                    "to": PREDICT_TESTNET_USDT,
                    "data": _erc20_call_data("0x70a08231", address),
                },
                "latest",
            ],
            timeout,
            request_id=4,
        ),
        16,
    )
    return tbnb_wei, usdt_raw, decimals


def format_units(raw: int, decimals: int) -> str:
    with localcontext() as context:
        context.prec = max(50, len(str(raw)) + decimals + 1)
        value = Decimal(raw) / (Decimal(10) ** decimals)
    return format(value, "f")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authenticate an EOA against Predict Testnet and inspect test balances.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="dotenv file to load without overriding exported variables (default: .env)",
    )
    parser.add_argument(
        "--skip-account-check",
        action="store_true",
        help="Obtain a JWT but skip the read-only GET /v1/account verification.",
    )
    parser.add_argument(
        "--skip-balance-check",
        action="store_true",
        help="Skip BNB Testnet tBNB and Predict USDT balance reads.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-request timeout in seconds (default: 15).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.timeout <= 0:
        print("error: --timeout must be positive", file=sys.stderr)
        return 2

    load_dotenv(args.env_file, override=False)
    try:
        api_url = _validate_testnet_api_url(
            _first_env("PREDICT_TESTNET_API_URL") or TESTNET_API_URL,
        )
        private_key = _first_env(
            "PREDICT_TESTNET_PRIVATE_KEY",
            "PREDICT_WALLET_PRIVATE_KEY",
            "WALLET_PRIVATE_KEY",
        )
        if private_key is None:
            raise ProbeError(
                "set PREDICT_TESTNET_PRIVATE_KEY in an exported variable or the selected env file",
            )
        api_key = _first_env("PREDICT_API_KEY", "X_API_KEY", "x-api-key")
        rpc_url = _first_env("PREDICT_TESTNET_RPC_URL") or TESTNET_RPC_URL

        with requests.Session() as session:
            address, jwt = authenticate_eoa(
                session,
                api_url,
                private_key,
                api_key,
                args.timeout,
            )
            expected_address = _first_env("PREDICT_TESTNET_WALLET_ADDRESS")
            if expected_address and expected_address.lower() != address.lower():
                raise ProbeError(
                    "PREDICT_TESTNET_WALLET_ADDRESS does not match the configured private key",
                )

            print("Predict Testnet EOA authentication: OK")
            print(f"Wallet: {address}")
            print("JWT: obtained and kept in memory only")

            if not args.skip_account_check:
                verify_account(session, api_url, api_key, jwt, args.timeout)
                print("GET /v1/account: OK")

            if not args.skip_balance_check:
                tbnb_wei, usdt_raw, usdt_decimals = read_testnet_balances(
                    session,
                    rpc_url,
                    address,
                    args.timeout,
                )
                print(f"tBNB balance: {format_units(tbnb_wei, 18)}")
                print(
                    f"Predict Testnet USDT balance: {format_units(usdt_raw, usdt_decimals)}"
                )
                print(f"Predict Testnet USDT contract: {PREDICT_TESTNET_USDT}")

        return 0
    except ProbeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
