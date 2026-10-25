"""Discover public maker lock hints. Auth signing only; never build or submit orders."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from probe_predict_order_capabilities import authenticated_context


def lock_hints(value, path=""):
    """Retain explicit nested lock/delay/removal fields; absence proves no semantics."""
    found = {}
    if isinstance(value, dict):
        for key, child in value.items():
            name = f"{path}.{key}" if path else key
            if any(term in key.lower() for term in ("lock", "delay", "remov")):
                found[name] = child
            else:
                found.update(lock_hints(child, name))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.update(lock_hints(child, f"{path}[{index}]"))
    return found


def discover(read, max_pages):
    if not 1 <= max_pages <= 20:
        raise ValueError("max_pages must be 1..20")
    cursor = None
    seen = set()
    markets = {}
    pages = 0
    for _ in range(max_pages):
        params = {"first": 100, "status": "OPEN"}
        if cursor is not None:
            params["after"] = cursor
        payload = read("/v1/markets", params=params)
        pages += 1
        for row in payload["data"]:
            markets[str(row["id"])] = row
        cursor = payload.get("cursor")
        if not cursor:
            break
        if cursor in seen:
            raise ValueError("repeated pagination cursor")
        seen.add(cursor)
    return {
        "observed_at": datetime.now(UTC).isoformat(),
        "source": "mainnet GET /v1/markets; no order signing or submission",
        "pages": pages,
        "market_count": len(markets),
        "truncated": bool(cursor),
        "market_keys": sorted({key for row in markets.values() for key in row}),
        "markets_with_lock_hints": [
            {"market_id": row["id"], "hints": hints}
            for row in markets.values()
            if (hints := lock_hints(row))
        ],
        "conclusion": "Hints and absent hints do not prove cancellation lock behavior.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        *_, read = authenticated_context()
        result = discover(read, args.max_pages)
        # Preserve earlier observations; no credentials or wallet data are persisted.
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2)
        print(json.dumps(result, indent=2))
    except Exception as error:  # noqa: BLE001 - never expose credential-bearing errors
        raise SystemExit("discovery failed: " + type(error).__name__) from None
