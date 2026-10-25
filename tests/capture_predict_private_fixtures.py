#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Explicit read-only capture of sanitized Predict private JSON for offline replay.

LGPL-3.0-or-later. Credentials, challenges, JWTs and signatures are never saved.
Run from the repository root; no recording/data adapter code is involved.
"""

import argparse
import json
import re
import stat
import tomllib
from decimal import Decimal
from pathlib import Path

import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
from web3 import Web3

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "crates/nautilus-predict/tests/fixtures/private_api"
OWNER = "0x0000000000000000000000000000000000000011"
DEPOSIT = "0x0000000000000000000000000000000000000022"


class CaptureError(RuntimeError):
    """Safe diagnostics authored locally, never remote response text."""


def private_toml(path):
    if not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise CaptureError(
            "local credential TOML must be a regular file with mode 0600"
        )
    try:
        return tomllib.loads(path.read_text())
    except (OSError, ValueError):
        raise CaptureError("invalid local TOML") from None


def capture(
    history=False,
    calibrate=False,
    calibration_seconds=60,
    ready_file=None,
    live_resting=False,
    calibration_name=None,
    stop_file=None,
    calibration_market_id=None,
):
    if stop_file is not None and (not calibrate or calibration_name is None):
        raise CaptureError("continuous observation requires a new named calibration")
    if stop_file is not None and stop_file.exists():
        raise CaptureError("continuous observation stop file already exists")
    if calibration_name is not None:
        if (
            not calibrate
            or live_resting
            or not calibration_name.replace("_", "").replace("-", "").isalnum()
        ):
            raise CaptureError("invalid separate calibration name")
        try:
            (OUTPUT / calibration_name).mkdir(parents=True, exist_ok=False)
        except OSError:
            raise CaptureError(
                "named calibration already exists; refusing to replace evidence"
            ) from None
    settings_path = ROOT / "env/predict-execution.toml"
    settings = private_toml(settings_path)
    if settings["network"]["environment"] != "mainnet":
        raise CaptureError("this capture requires explicit mainnet configuration")
    key_path = Path(settings["network"]["api_key_file"])
    if not key_path.is_absolute():
        key_path = settings_path.parent / key_path
    key = private_toml(key_path)["api_key"]
    private = settings["privy"]
    wallet = Account.from_key(private["private_key"])
    mode = private.get("mode", "predict_account")
    deposit = private["predict_account"] if mode == "predict_account" else None
    if mode not in ("predict_account", "eoa") or (
        mode == "eoa" and private["predict_account"]
    ):
        raise CaptureError("invalid explicit wallet mode")
    identity = Web3.to_checksum_address(deposit or wallet.address)
    if (
        private.get("owner_address")
        and wallet.address.lower() != private["owner_address"].lower()
    ):
        raise CaptureError("owner-address guard mismatch")
    builder = OrderBuilder.make(
        ChainId.BNB_MAINNET,
        private["private_key"],
        OrderBuilderOptions(predict_account=identity if deposit else None),
    )
    http = requests.Session()
    http.headers["x-api-key"] = key
    timeout = settings["network"]["timeout_secs"]

    def request(method, path, **kwargs):
        if method != "GET" and (method, path) != ("POST", "/v1/auth"):
            raise CaptureError("read-only capture forbids this HTTP operation")
        result = http.request(
            method, "https://api.predict.fun" + path, timeout=timeout, **kwargs
        )
        if result.status_code != 200:
            raise CaptureError(
                f"private read calibration rejected at {path}: HTTP {result.status_code}"
            )
        return result.json()

    challenge = request("GET", "/v1/auth/message")["data"]["message"]
    signature = (
        builder.sign_predict_account_message(challenge)
        if deposit
        else (
            "0x" + wallet.sign_message(encode_defunct(text=challenge)).signature.hex()
        )
    )
    jwt = request(
        "POST",
        "/v1/auth",
        json={
            "signer": identity,
            "message": challenge,
            "signature": signature,
        },
    )["data"]["token"]
    http.headers["Authorization"] = "Bearer " + jwt
    account = request("GET", "/v1/account")
    if account["data"]["address"].lower() != identity.lower():
        raise CaptureError("authenticated account identity mismatch")
    if calibrate:
        from predict_read_only_calibration import collect

        def subscribed():
            if ready_file is not None:
                ready_file.write_text('{"subscribed":true}\n')

        # The production recorder is untouched. This opt-in test collector retains
        # each wallet event before waiting further; a disconnect cannot erase them.
        import os
        from contextlib import nullcontext

        from predict_private_http_recorder import read_response_evidence

        stream = (
            (OUTPUT / calibration_name / "wallet_events.jsonl").open("x", buffering=1)
            if stop_file is not None
            else nullcontext(None)
        )
        with stream as journal:

            def observed(event):
                if journal is not None:
                    cleaned = read_response_evidence(
                        event, [key, private["private_key"], jwt, signature, challenge]
                    )
                    journal.write(json.dumps(cleaned) + "\n")
                    journal.flush()
                    os.fsync(journal.fileno())

            payloads = {
                "calibration.json": collect(
                    request,
                    builder,
                    identity,
                    jwt,
                    key,
                    timeout,
                    observation_secs=None
                    if stop_file is not None
                    else calibration_seconds,
                    on_subscribed=subscribed,
                    stop_requested=stop_file.exists if stop_file is not None else None,
                    on_wallet_event=observed,
                    market_id=calibration_market_id,
                )
            }
    elif history:

        def pages(path, params):
            rows = []
            seen = set()
            cursor = None
            for page in range(3):
                query = {"first": 100, **params}
                if cursor:
                    query["after"] = cursor
                payload = request("GET", path, params=query)
                if payload.get("success") is not True or not isinstance(
                    payload.get("data"), list
                ):
                    raise CaptureError("invalid historical page envelope")
                rows.extend(payload["data"])
                cursor = payload.get("cursor")
                if not cursor:
                    break
                if cursor in seen:
                    raise CaptureError("historical pagination repeated a cursor")
                seen.add(cursor)
            return {
                "success": True,
                "data": rows,
                "cursor": cursor,
                "_capture": {
                    "pages_read": page + 1,
                    "max_pages": 3,
                    "exhausted": not bool(cursor),
                },
            }

        payloads = {
            "matches.json": pages("/v1/orders/matches", {"signerAddress": identity}),
            "filled_orders.json": pages("/v1/orders", {"status": "FILLED"}),
        }
        for row in payloads["matches.json"]["data"]:
            participants = [row["taker"], *row["makers"]]
            if not any(
                leg["signer"].lower() == identity.lower() for leg in participants
            ):
                raise CaptureError(
                    "historical match does not belong to authenticated account"
                )
        for row in payloads["filled_orders.json"]["data"]:
            if row["order"]["maker"].lower() != identity.lower():
                raise CaptureError(
                    "historical order does not belong to authenticated account"
                )
    else:
        payloads = {
            "account.json": account,
            "orders.json": request("GET", "/v1/orders", params={"first": 100}),
            "positions.json": request(
                "GET", "/v1/positions", params={"first": 100, "includeLost": "true"}
            ),
        }
        # Captured balance is an observation, not a fabricated free balance.
        wei = builder.balance_of()
        payloads["balance.json"] = {
            "account": identity,
            "currency": "USDT",
            "decimals": 18,
            "balanceWei": str(wei),
            "source": "official SDK balance_of; latest observation",
        }

    def sanitize(value, field=""):
        if isinstance(value, dict):
            cleaned = {}
            for key, item in value.items():
                if key.lower() == "signature":
                    cleaned[key] = "public-fixture-signature"
                elif key.lower() not in {"token", "jwt", "privatekey"}:
                    cleaned[key] = sanitize(item, key)
            return cleaned
        if isinstance(value, list):
            return [sanitize(v, field) for v in value]
        if field in {"name", "imageUrl", "code", "cursor"} and value is not None:
            return None if field == "imageUrl" else "redacted-fixture"
        if isinstance(value, str):
            value = re.sub(
                re.escape(identity),
                DEPOSIT if deposit else OWNER,
                value,
                flags=re.IGNORECASE,
            )
            value = re.sub(re.escape(wallet.address), OWNER, value, flags=re.IGNORECASE)
            # Preserve token IDs/amounts and protocol values; scrub other wallet identities.
            value = re.sub(
                r"0x[0-9a-fA-F]{40}(?![0-9a-fA-F])",
                lambda m: (
                    m.group()
                    if m.group().lower() in {OWNER.lower(), DEPOSIT.lower()}
                    else "0x0000000000000000000000000000000000000033"
                ),
                value,
            )
            return value
        return value

    cleaned = {name: sanitize(data) for name, data in payloads.items()}
    encoded = json.dumps(cleaned)
    for secret in (key, private["private_key"], jwt, signature, challenge):
        if secret and secret in encoded:
            raise CaptureError("credential detected in fixture output")
    output = (
        OUTPUT
        / (
            calibration_name
            or ("live_resting" if live_resting else "read_only_calibration")
        )
        if calibrate
        else OUTPUT
    )
    output.mkdir(parents=True, exist_ok=True)
    for name, data in cleaned.items():
        (output / name).write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        )
    print(
        f"Saved {len(cleaned)} sanitized read-only fixtures; no authentication material or wallet addresses."
    )
    if calibrate:
        print(json.dumps(cleaned["calibration.json"]["summary"], sort_keys=True))
    elif history:
        for name, payload in cleaned.items():
            print(
                name,
                "rows",
                len(payload["data"]),
                "pages",
                payload["_capture"]["pages_read"],
                "exhausted",
                payload["_capture"]["exhausted"],
            )
    else:
        print("Observed USDT:", Decimal(wei) / Decimal(10**18))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--capture",
        action="store_true",
        required=True,
        help="explicitly contact official services and replace sanitized fixtures",
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="capture bounded match and FILLED order history instead of account snapshots",
    )
    parser.add_argument(
        "--calibrate",
        action="store_true",
        help="60-second read-only wallet/REST/chain calibration; preserves prior fixtures",
    )
    parser.add_argument("--calibration-seconds", type=int, default=60)
    parser.add_argument("--calibration-market-id", type=int)
    parser.add_argument(
        "--until-stop-file",
        type=Path,
        help="test observer stays connected until this host completion marker exists",
    )
    parser.add_argument(
        "--calibration-name",
        help="save a new named read-only calibration without replacing prior JSON",
    )
    parser.add_argument(
        "--ready-file",
        type=Path,
        help="non-secret ACK marker for coordinated Rust LiveNode tests",
    )
    parser.add_argument(
        "--live-resting",
        action="store_true",
        help="save a separate read-only observation of an explicitly authorized Rust order test",
    )
    args = parser.parse_args()
    if not 1 <= args.calibration_seconds <= 300:
        parser.error("calibration-seconds must be within 1..300")
    if (args.ready_file or args.live_resting) and not args.calibrate:
        parser.error("coordination options require --calibrate")
    if args.history and args.calibrate:
        parser.error("choose either history or calibration")
    try:
        capture(
            history=args.history,
            calibrate=args.calibrate,
            calibration_seconds=args.calibration_seconds,
            ready_file=args.ready_file,
            live_resting=args.live_resting,
            calibration_name=args.calibration_name,
            stop_file=args.until_stop_file,
            calibration_market_id=args.calibration_market_id,
        )
    except Exception as error:  # noqa: BLE001 - remote exceptions may contain credentials
        import traceback

        print(
            "Failure locations:",
            [
                (Path(f.filename).name, f.lineno, f.name)
                for f in traceback.extract_tb(error.__traceback__)
            ],
        )
        print(
            "Capture failed:",
            str(error) if type(error) is CaptureError else type(error).__name__,
        )
        raise SystemExit(1) from None
