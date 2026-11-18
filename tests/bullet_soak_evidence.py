"""Compare an opt-in Bullet soak with Native readback and independent public responses.

Operational outputs stay inside the ignored catalog. This tool does not submit orders,
change networking, or infer uninterrupted delivery from a successful reconnect.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
import json
from pathlib import Path
import re


def json_lines(path):
    with Path(path).open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def log_time_ns(line):
    timestamp = line.split()[0].removesuffix("Z")
    whole, _, fraction = timestamp.partition(".")
    return int(
        datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp()
    ) * 10**9 + int(fraction.ljust(9, "0"))


def prepare_recoveries(catalog, native_log, require_invalidation=False):
    lines = re.sub(r"\x1b\[[0-9;]*m", "", Path(native_log).read_text()).splitlines()
    requests = list(json_lines(catalog / "metadata/soak_reconnect_requests.jsonl"))
    accepted = [
        line
        for line in lines
        if "Requested socket reconnect for client bullet_main endpoint public-data"
        in line
    ]
    assert len(accepted) == len(requests) > 0, "not every Native command was accepted"
    assert not any(
        "Rejected socket reconnect request for client bullet_main" in line
        for line in lines
    )
    resets = []
    invalidations = []
    confirmations = defaultdict(dict)
    for line in lines:
        invalidation = re.search(r"Bullet book invalidated:.*ts_init_ns=(\d+)", line)
        if invalidation:
            invalidations.append(int(invalidation[1]))
        match = re.search(r"Bullet epoch (\d+) reconnected", line)
        if match:
            resets.append({"epoch": int(match[1]), "reset_ns": log_time_ns(line)})
        match = re.search(r"Bullet Subscribe confirmed: topic=(\S+), epoch=(\d+)", line)
        if match:
            confirmations[int(match[2])][match[1]] = log_time_ns(line)
    expected_topics = {"BTC-USD@depth20", "BTC-USD@aggTrade", "BTC-USD@markPrice"}
    assert set(confirmations[0]) == expected_topics, "initial ACKs missing"
    used = set()
    evidence = []
    for request, acceptance in zip(requests, accepted):
        accepted_ns = log_time_ns(acceptance)
        assert request["ts_init_ns"] <= accepted_ns < request["ts_init_ns"] + 5 * 10**9
        epoch = next(item for item in resets if item["reset_ns"] >= accepted_ns)
        assert epoch["epoch"] not in used, (
            "one epoch cannot prove two separate reconnects"
        )
        used.add(epoch["epoch"])
        acknowledgements = confirmations[epoch["epoch"]]
        assert set(acknowledgements) == expected_topics, "recovery ACKs missing"
        assert all(value >= epoch["reset_ns"] for value in acknowledgements.values())
        cuts = [time for time in invalidations if request["ts_init_ns"] <= time <= epoch["reset_ns"]]
        if require_invalidation:
            assert cuts, "reconnect did not invalidate the Native book before replacement"
        evidence.append(
            {
                **epoch,
                "request_ns": request["ts_init_ns"],
                "accepted_ns": accepted_ns,
                "confirmed_topics": acknowledgements,
                "invalidation_ns": min(cuts) if cuts else None,
            }
        )
    path = catalog / "metadata/soak_recovery_epochs.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


def compare(catalog, source, native_log):
    native = json.loads((catalog / "metadata/soak_native_readback.json").read_text())
    families = defaultdict(list)
    for row in json_lines(catalog / "metadata/soak_native_market_rows.jsonl"):
        families[row.get("type", "BulletFundingEstimate")].append(row)
    raw_counts = Counter()
    raw_trades = {}
    raw_prices = defaultdict(set)
    raw_errors = 0
    quantum = Decimal("0.00000001")
    with localcontext() as context:
        context.prec = 80
        for envelope in json_lines(source / "ws.jsonl"):
            if "error" in envelope:
                raw_errors += 1
                continue
            row = envelope["body"]
            raw_counts[row.get("e", "unknown")] += 1
            if row.get("e") == "aggTrade":
                key = str(row["a"])
                value = (
                    Decimal(row["p"]),
                    Decimal(row["q"]),
                    row["T"] * 1000,
                    "SELL" if row["m"] else "BUY",
                )
                if key in raw_trades:
                    assert raw_trades[key] == value, "conflicting source trade identity"
                raw_trades[key] = value
            if row.get("e") == "markPriceUpdate":
                raw_prices[row["E"] * 1000].add(
                    (
                        Decimal(row["p"]).quantize(quantum, rounding=ROUND_HALF_EVEN),
                        Decimal(row["i"]).quantize(quantum, rounding=ROUND_HALF_EVEN),
                    )
                )
        matched_trades = 0
        for row in families["TradeTick"]:
            if row["trade_id"] in raw_trades:
                assert (
                    Decimal(row["price"]),
                    Decimal(row["size"]),
                    row["ts_event"],
                    row["aggressor_side"],
                ) == raw_trades[row["trade_id"]], "trade wire/Native mismatch"
                matched_trades += 1
        assert matched_trades > 0, "no common trade control"
        matched_prices = 0
        for mark, index in zip(
            families["MarkPriceUpdate"], families["IndexPriceUpdate"]
        ):
            candidates = raw_prices.get(mark["ts_event"])
            if candidates:
                assert (
                    Decimal(mark["value"]),
                    Decimal(index["value"]),
                ) in candidates, "price wire/Native mismatch"
                matched_prices += 1
        assert matched_prices > 0, "no common oracle publication control"
        attempts = 0
        errors = 0
        history = defaultdict(list)
        for row in json_lines(source / "http.jsonl"):
            attempts += 1
            if row.get("status") != 200:
                errors += 1
                continue
            if "/fundingRate?" in row["path"]:
                for funding in row["body"]:
                    history[funding["fundingTime"] * 1000].append(
                        (funding, row["received_ns"])
                    )
        settled = []
        for row in native["settled"]:
            matches = history[row["ts_event"]]
            assert matches, "no source settlement control"
            assert all(
                Decimal(item["fundingRate"]) == Decimal(row["rate"]) * 8
                for item, _ in matches
            )
            first, receipt = min(matches, key=lambda match: match[1])
            settled.append(
                {
                    "settlement_ns": row["ts_event"],
                    "source_rate_8h": first["fundingRate"],
                    "hourly_gross_rate": row["rate"],
                    "native_receipt_ns": row["ts_init"],
                    "first_observer_receipt_ns": receipt,
                }
            )
    log = re.sub(r"\x1b\[[0-9;]*m", "", Path(native_log).read_text())
    errors_in_native_log = [line for line in log.splitlines() if "[ERROR]" in line]
    summary = {
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "catalog": str(catalog.resolve()),
        "source": str(source.resolve()),
        "native": native,
        "matched_source_trades": matched_trades,
        "native_trades_without_observer_match": len(families["TradeTick"])
        - matched_trades,
        "matched_source_price_pairs": matched_prices,
        "native_price_pairs_without_observer_match": len(families["MarkPriceUpdate"])
        - matched_prices,
        "source_ws_event_counts": dict(raw_counts),
        "source_ws_errors": raw_errors,
        "source_http_attempts": attempts,
        "source_http_errors": errors,
        "settled_source_comparisons": settled,
        "native_error_log_lines": errors_in_native_log,
    }
    path = catalog / "metadata/soak_validation.json"
    path.write_text(json.dumps(summary, indent=2) + "\n")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True, type=Path)
    parser.add_argument("--native-log", required=True, type=Path)
    parser.add_argument("--prepare-recovery", action="store_true")
    parser.add_argument("--require-invalidation", action="store_true")
    parser.add_argument("--source", type=Path)
    args = parser.parse_args()
    if args.prepare_recovery:
        print(json.dumps(prepare_recoveries(args.catalog, args.native_log, args.require_invalidation), indent=2))
    else:
        if args.source is None:
            parser.error("--source is required for comparison")
        print(compare(args.catalog, args.source, args.native_log))


if __name__ == "__main__":
    main()
