"""Offline checks shared by resting-order supervisors and evidence review.

LGPL-3.0-or-later. No credentials, network, subprocesses or trading capabilities.
"""

import json
from datetime import datetime, timezone
from collections import Counter
import re
from decimal import Decimal


class EvidenceError(ValueError):
    """Captured observations do not establish a completed resting-order run."""


def validate_evidence(output, *, write=True):
    """Independent captured wallet/chain facts must agree before another run."""
    report = json.loads((output / "result.json").read_text())
    capture = json.loads((output / "calibration.json").read_text())
    http = json.loads((output / "http_responses.json").read_text())
    before, after = capture["before"], capture["after"]
    posts = [
        row
        for row in http["events"]
        if row["method"] == "POST" and row["path"] == "/v1/orders"
    ]
    hashes = {row["requestTerms"]["hash"] for row in posts}
    accepted = {
        row["orderHash"]
        for row in capture["wallet"]["events"]
        if row["type"] == "orderAccepted"
    }
    canceled = {
        row["orderHash"]
        for row in capture["wallet"]["events"]
        if row["type"] == "orderCancelled"
    }
    matches = lambda snapshot: {
        (row["transactionHash"], str(row["settlementId"]), row["amountFilled"])
        for row in snapshot["matches"]
    }
    if not (
        len(posts) == len(hashes) == 2
        and all(row["status"] == 201 for row in posts)
        and hashes == accepted == canceled
        and before["cashWei"] == after["cashWei"]
        and before["tokenBalances"] == after["tokenBalances"]
        and before["positions"] == after["positions"]
        and matches(before) == matches(after)
        and not before["orders"]
        and not after["orders"]
        and report["fills"] == 0
        and report["rejected"] == report["denied"] == 0
        and all(
            report[key] == 2
            for key in (
                "submitted",
                "accepted",
                "canceled",
                "modifyRejected",
                "modifyTermsPreserved",
            )
        )
    ):
        raise EvidenceError(
            "independent mainnet evidence disagrees; stop further cases"
        )
    case = report.get("case", "auto")
    if case in {"batch-restart", "http-batch"}:
        validate_batch_feedback(output, report, capture, hashes, write=write)
    elif case != "auto":
        feedback = json.loads((output / "native_feedback.json").read_text())
        outcome, tif = case.split("-")
        if report["outcome"] != {"yes": 1, "no": 2}[outcome]:
            raise EvidenceError("recorded outcome differs from selected case")
        if any(
            row["details"]["outcomeIndex"] != report["outcome"]
            or row["details"]["marketId"] != report["marketId"]
            for row in capture["wallet"]["events"]
            if row.get("orderHash") in hashes
        ):
            raise EvidenceError(
                "wallet market/outcome differs from selected native instrument"
            )
        for row in posts:
            receipt = row["responseBody"]
            if not (
                receipt["success"] is True
                and receipt["data"]["code"] == "OK"
                and receipt["data"]["orderHash"] == row["requestTerms"]["hash"]
            ):
                raise EvidenceError("create receipt disagrees with signed hash")
            expiration = row["requestTerms"]["expiration"]
            if tif == "gtc":
                if expiration != 4102444800:
                    raise EvidenceError("GTC differs from SDK default expiration")
            else:
                observed = datetime.fromisoformat(row["observedAtUtc"]).timestamp()
                if not observed + 120 < expiration <= observed + 300:
                    raise EvidenceError(
                        "GTD expiration differs from bounded future timestamp"
                    )
        for kind in ("Accepted", "ModifyRejected", "Canceled"):
            events = [row for row in feedback if row["type"] == kind]
            if len(events) != 2 or {row["venueOrderId"] for row in events} != hashes:
                raise EvidenceError(
                    "native lifecycle disagrees with actual wallet/HTTP hashes"
                )
            for event in events:
                cached = event["cachedOrder"]
                if not cached["postOnly"] or cached["timeInForce"] != tif.upper():
                    raise EvidenceError(
                        "native order lost its post-only or time-in-force"
                    )
                if tif == "gtd":
                    wire = next(
                        row
                        for row in posts
                        if row["requestTerms"]["hash"] == event["venueOrderId"]
                    )
                    if (
                        cached["expireTimeNs"]
                        != wire["requestTerms"]["expiration"] * 10**9
                    ):
                        raise EvidenceError(
                            "native GTD expiration differs from signed request"
                        )
        locks = [
            Decimal(balance["locked"])
            for row in feedback
            if row["type"] == "AccountStateApplied"
            for balance in row["balances"]
        ]
        if Decimal(1) not in locks or not locks or locks[-1] != 0 or max(locks) > 1:
            raise EvidenceError(
                "strategy did not observe collateral reservation then unlock"
            )
    if (output / "latency.json").exists():
        analyze_latency(output, write=write)
    validation = {
        "verifiedAtUtc": datetime.now(timezone.utc).isoformat(),
        "case": report.get("case", "auto"),
        "createdOrders": 2,
        "walletAcceptedAndCanceled": 2,
        "cashWeiUnchanged": True,
        "positionsAndTokenBalancesUnchanged": True,
        "newMatches": 0,
        "finalOpenOrders": 0,
    }
    if write:
        (output / "evidence_validation.json").write_text(
            json.dumps(validation, indent=2) + "\n"
        )
    return validation


def validate_http_batch_removal(http):
    creates = [
        r for r in http["events"] if r["method"] == "POST" and r["path"] == "/v1/orders"
    ]
    removes = [
        r
        for r in http["events"]
        if r["method"] == "POST" and r["path"] == "/v1/orders/remove"
    ]
    ids = {r["responseBody"]["data"]["orderId"] for r in creates}
    if len(creates) != 2 or len(ids) != 2 or len(removes) != 1:
        raise EvidenceError(
            "expected exactly one HTTP batch removal for two created orders"
        )
    row = removes[0]
    requested = row.get("requestIds", [])
    receipt = row.get("responseBody", {})
    if not (
        len(requested) == 2
        and set(requested) == ids
        and row["status"] == 200
        and receipt.get("success") is True
        and len(receipt.get("removed", [])) == 2
        and set(receipt["removed"]) == ids
        and receipt.get("noop") == []
    ):
        raise EvidenceError("multi-ID HTTP request and per-order receipts disagree")
    return {"httpRequests": 1, "requestIds": requested, "allRemoved": True}


def validate_batch_feedback(output, report, capture, hashes, *, write=True):
    batch_http = None
    if report.get("case") == "http-batch":
        batch_http = validate_http_batch_removal(
            json.loads((output / "http_responses.json").read_text())
        )
    rows = json.loads((output / "native_input_and_callbacks.json").read_text())
    starts = {r["phase"]: r for r in rows if r["type"] == "StrategyOnStart"}
    if (
        starts["place"]["existingOrders"] != 0
        or starts["recover"]["existingOrders"] != 2
    ):
        raise EvidenceError("cold startup did not recover exactly two active orders")
    if not any(
        r["type"] == "BeforeNativeRun"
        and r["cachedOrders"] == 0
        and r["submissionSignerInstalled"] is False
        for r in rows
    ):
        raise EvidenceError("restart is not an empty-cache, no-submit host")
    pending = next(r for r in rows if r["type"] == "RealOpenOrdersBeforeColdStartup")[
        "orders"
    ]
    if {r["hash"] for r in pending} != hashes or any(
        r["amountFilled"] != "0" for r in pending
    ):
        raise EvidenceError("real handover orders differ from this run")
    if sum(r["type"] == "StrategyBatchCancelOrders" for r in rows) != 1:
        raise EvidenceError("expected one native batch cancel command")
    wallet = [
        r
        for r in capture["wallet"]["events"]
        if r["type"] == "orderCancelled" and r["orderHash"] in hashes
    ]
    ordering = []
    for order_hash in sorted(hashes):
        callbacks = [
            (i, r)
            for i, r in enumerate(rows)
            if r["phase"] == "recover"
            and r["type"] == "StrategyOrderCallback"
            and r["kind"] == "Canceled"
            and r["venueOrderId"] == order_hash
        ]
        reports = [
            (i, r)
            for i, r in enumerate(rows)
            if r["phase"] == "recover"
            and r["type"] == "RawOrderStatusReport"
            and r["report"]["venue_order_id"] == order_hash
            and r["report"]["order_status"] == "CANCELED"
        ]
        messages = [r for r in wallet if r["orderHash"] == order_hash]
        if len(callbacks) != 1 or not reports or not messages:
            raise EvidenceError(
                "REST/WS cancellation did not yield exactly one native callback"
            )
        matching = [
            (i, r)
            for i, r in reports
            if any(
                int(r["report"]["ts_last"]) == int(w["timestamp"]) * 1_000_000
                for w in messages
            )
        ]
        if not matching:
            raise EvidenceError(
                "Native cancellation input does not match the real wallet timestamp"
            )
        ordering.append(
            {
                "hash": order_hash,
                "callbackIndex": callbacks[0][0],
                "walletReportIndices": [i for i, _ in matching],
                "duplicateCanceledCallbacks": 0,
                "first": "native-callback-before-wallet-report"
                if callbacks[0][0] < matching[0][0]
                else "wallet-report-before-native-callback",
            }
        )
    account = [
        r
        for r in rows
        if r["phase"] == "recover" and r["type"] == "AccountStateApplied"
    ]
    locks = [Decimal(b["locked"]) for r in account for b in r["balances"]]
    if not locks or Decimal(2) not in locks or locks[-1] != 0 or max(locks) > 2:
        raise EvidenceError("two-order account reservation/unlock not observed")
    if write:
        (output / "batch_native_validation.json").write_text(
            json.dumps(
                {
                    "source": "derived checks of actual Native report subscriptions, strategy callbacks, independent wallet and REST",
                    "coldCacheOrdersBeforeRun": 0,
                    "restoredActiveOrders": 2,
                    "nativeBatchCommands": 1,
                    "httpBatchRemoval": batch_http,
                    "cancelOrdering": ordering,
                    "finalLocked": "0",
                    "newFills": 0,
                },
                indent=2,
            )
            + "\n"
        )


def analyze_latency(output, *, write=True):
    """Cold-path report; preserve missing WS samples and the small-sample limitation."""
    record = json.loads((output / "latency.json").read_text())
    snapshot = record["snapshot"]
    venue_validated = record.get("commandPath") == "venue_validated"
    counters, distributions = snapshot["counters"], snapshot["distributions"]
    required = (
        "decision_to_post",
        "command_to_post",
        "preparation",
        "create_request",
        "cancel_request",
        "cancel_to_strategy_callback",
        "ws_to_strategy_callback",
    )
    if not venue_validated:
        required += ("resource_collection", "admission_open_collection")
    elif any(name in distributions for name in ("resource_collection", "admission_open_collection", "admission_wait")):
        raise EvidenceError("venue-validated path unexpectedly measured a strict admission stage")
    if snapshot["dropped_samples"] or snapshot["invalid_intervals"]:
        raise EvidenceError(
            "latency samples were dropped or invalid; do not infer a fast path"
        )
    if any(
        not distributions.get(key, {}).get("completed_observations") for key in required
    ):
        raise EvidenceError(
            "missing measured private API or actual WS-to-strategy interval"
        )
    for key in required:
        values = distributions[key]
        if (
            not values["min_ns"]
            <= values["p50_ns"]
            <= values["p95_ns"]
            <= values["p99_ns"]
            <= values["max_ns"]
        ):
            raise EvidenceError("invalid latency percentile ordering")
        if key != "ws_to_strategy_callback" and values["completed_observations"] != 2:
            raise EvidenceError("expected exactly two completed command samples")
    if counters["create_calls"] != 2 or counters["remove_calls"] != 2:
        raise EvidenceError("unexpected create/remove invocation count")
    hits, misses = counters["metadata_hit"], counters["metadata_miss"]
    if hits + misses != 2:
        raise EvidenceError("market metadata attempts do not match two submissions")
    http = json.loads((output / "http_responses.json").read_text())["events"]
    routes, upstream = Counter(), {}
    for event in http:
        route = (
            event["method"]
            + " "
            + re.sub(
                r"/0x[0-9a-fA-F]{64}(?=/|$)|/[0-9]+(?=/|$)", "/<id>", event["path"]
            )
        )
        routes[route] += 1
        if "upstreamElapsedNs" in event:
            upstream.setdefault(route, []).append(event["upstreamElapsedNs"])

    def quantiles(values):
        values = sorted(values)
        return {
            "samples": len(values),
            **{
                f"p{p}_ms": values[(len(values) * p + 99) // 100 - 1] / 1e6
                for p in (50, 95, 99)
            },
        }

    resource = distributions.get("resource_collection")
    preparation = distributions["preparation"]
    create = distributions["create_request"]
    dominant = bool(resource) and (
        resource["min_ns"] >= preparation["max_ns"] * 0.6
        and resource["min_ns"] >= create["max_ns"]
    )
    report = {
        "source": "cold analysis of measured live run; no network or order writes",
        "commandPath": "venue_validated" if venue_validated else "legacy_strict_funding",
        "segments": {
            name: {
                "samples": v["completed_observations"],
                "retained": v["retained_observations"],
                **{f"p{p}_ms": v[f"p{p}_ns"] / 1e6 for p in (50, 95, 99)},
                "min_ms": v["min_ns"] / 1e6,
                "max_ms": v["max_ns"] / 1e6,
            }
            for name, v in distributions.items()
        },
        "apiRequests": {
            "scope": "execution RPC and private-client REST, including execution metadata/book reads; excludes independent collector/supervisor and public data-client requests",
            "rpc": counters["rpc_requests"],
            "openPageCalls": counters["open_page_requests"],
            "createCalls": counters["create_calls"],
            "removeCalls": counters["remove_calls"],
            "marketMetadataCalls": counters.get("metadata_requests", misses),
            "executionOrderbookCalls": counters.get("orderbook_requests"),
            "recordedRestRoutes": dict(sorted(routes.items())),
        },
        "metadata": {"hits": hits, "misses": misses, "hitRate": hits / (hits + misses)},
        "upstreamHttpByRoute": {
            route: quantiles(values) for route, values in upstream.items()
        },
        "observability": {
            key: snapshot[key]
            for key in (
                "dropped_samples",
                "pending_correlations",
                "lost_correlations",
                "invalid_intervals",
            )
        },
        "abandonedTimers": counters["abandoned_timings"],
        "nextOptimization": {
            "fundingDominatesPilot": dominant,
            "criterion": "minimum funding wall time >=60% of maximum preparation and >= maximum create-call time",
            "minimumFundingOverMaximumPreparation": resource["min_ns"]
            / preparation["max_ns"] if resource else None,
            "action": "Evaluate same-block read-only RPC batching before changing admission"
            if dominant
            else "No RPC batching justified by this pilot; inspect the measured largest segment first",
        },
        "limits": [
            "Only two creates/cancels: p95/p99 are sample maxima, not stable tail estimates",
            "Legacy resource_collection is per-submit wall time; current command path omits it",
            "RPC/OPEN totals also include startup and background account work",
            "HTTP totals include the safety relay; separately reported upstream times exclude its local pre-forward journal",
            "REST cancellation callbacks may have no WS counterpart; unmatched samples are not zero latency",
        ],
    }
    if write:
        (output / "latency_analysis.json").write_text(
            json.dumps(report, indent=2) + "\n"
        )
    return report
