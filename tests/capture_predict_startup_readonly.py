"""Read-only cold-start evidence; no order or chain writes. LGPL-3.0-or-later."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

from capture_predict_private_fixtures import CaptureError
from predict_read_only_calibration import complete_pages
from predict_sdk import ChainId, OrderBuilder
from predict_sdk.types import Order, Side, SignatureType
from run_predict_resting_live import authenticated_reader

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "crates/nautilus-predict/tests/fixtures/private_api"
OWN = "0x0000000000000000000000000000000000000022"
OTHER = "0x0000000000000000000000000000000000000033"
ZERO = "0x" + "0" * 40


def contract_hash(builder, row):
    wire = row["order"]
    order = Order(
        salt=wire["salt"],
        maker=wire["maker"],
        signer=wire["signer"],
        taker=wire["taker"],
        token_id=wire["tokenId"],
        maker_amount=wire["makerAmount"],
        taker_amount=wire["takerAmount"],
        expiration=str(wire["expiration"]),
        nonce=wire["nonce"],
        fee_rate_bps=wire["feeRateBps"],
        side=Side(wire["side"]),
        signature_type=SignatureType(wire["signatureType"]),
    )
    typed = builder.build_typed_data(
        order, is_neg_risk=row["isNegRisk"], is_yield_bearing=row["isYieldBearing"]
    )
    return builder.build_typed_data_hash(typed)


def collect(request, identity, known_hashes=()):
    """Only GET is passed beyond this boundary, even with a write-capable transport."""
    reads = []

    def get(method, path, **kwargs):
        if method != "GET" or not path.startswith("/v1/"):
            raise CaptureError("cold-start collector permits GET only")
        reply = request(method, path, **kwargs)
        reads.append(
            {"method": method, "path": path, "params": kwargs.get("params", {})}
        )
        return reply

    orders = complete_pages(get, "/v1/orders", status="OPEN")
    positions = complete_pages(get, "/v1/positions", includeLost="true")
    matches = complete_pages(get, "/v1/orders/matches", signerAddress=identity)
    hashes = {row["order"]["hash"] for row in orders} | set(known_hashes)
    for row in matches:
        own = [
            leg
            for leg in [row["taker"], *row["makers"]]
            if leg["signer"].lower() == identity.lower()
        ]
        if not own:
            raise CaptureError("foreign match in account query")
        hashes.update(leg["hash"] for leg in own)
    if len(hashes) > 32:
        raise CaptureError("cold-start evidence exceeds 32 distinct orders")
    builder = OrderBuilder.make(ChainId.BNB_MAINNET)
    reports = []
    for order_hash in sorted(hashes):
        if not re.fullmatch(r"0x[0-9a-fA-F]{64}", order_hash):
            raise CaptureError("invalid venue hash")
        row = get("GET", "/v1/orders/" + order_hash)["data"]
        wire = row["order"]
        if wire["hash"].lower() != order_hash.lower() or any(
            wire[key].lower() != identity.lower() for key in ("maker", "signer")
        ):
            raise CaptureError("order identity mismatch")
        if contract_hash(builder, row).lower() != order_hash.lower():
            raise CaptureError("official SDK order hash mismatch")
        reports.append(row)
    markets = {row["marketId"] for row in [*orders, *reports]} | {
        row["market"]["id"] for row in positions
    }
    definitions = [get("GET", f"/v1/markets/{mid}")["data"] for mid in sorted(markets)]
    return {
        "source": "actual mainnet read-only GET snapshots; SDK validates original order hash before identity redaction",
        "observedAt": datetime.now(UTC).isoformat(),
        "currentOrders": orders,
        "currentPositions": positions,
        "matches": matches,
        "ordersByHash": reports,
        "markets": definitions,
        "sdkOrderHashesVerified": len(reports),
        "requests": reads,
        "orderWrites": 0,
        "chainWrites": 0,
        "transforms": [
            "wallet addresses anonymized, zero taker preserved",
            "signatures removed; original SDK hash check happened before redaction",
        ],
    }


def sanitize(value, identity):
    if isinstance(value, dict):
        return {
            key: (
                "redacted-signature"
                if key.lower() == "signature"
                else sanitize(item, identity)
            )
            for key, item in value.items()
            if key.lower() not in {"jwt", "token", "privatekey"}
        }
    if isinstance(value, list):
        return [sanitize(item, identity) for item in value]
    if isinstance(value, str):
        return re.sub(
            r"0x[0-9a-fA-F]{40}(?![0-9a-fA-F])",
            lambda m: (
                ZERO
                if m.group().lower() == ZERO
                else OWN
                if m.group().lower() == identity.lower()
                else OTHER
            ),
            value,
        )
    return value


def main():
    request, identity = (
        authenticated_reader()
    )  # Auth POST only; never invoke cleanup/remove.
    prior = json.loads(
        (OUTPUT / "live_fill_20261016T123240Z_independent/calibration.json").read_text()
    )
    known = {event["orderHash"] for event in prior["wallet"]["events"]}
    result = sanitize(collect(request, identity, known), identity)
    name = (
        "cold_start_readonly_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + ".json"
    )
    with (OUTPUT / name).open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                "file": name,
                "ordersByHash": len(result["ordersByHash"]),
                "sdkHashesVerified": result["sdkOrderHashesVerified"],
                "currentOrders": len(result["currentOrders"]),
                "currentPositions": len(result["currentPositions"]),
                "orderWrites": 0,
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 -- remote diagnostic may expose auth headers.
        print("Read-only capture failed:", type(error).__name__)
        raise SystemExit(1) from None
