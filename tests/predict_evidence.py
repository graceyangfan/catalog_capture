#!/usr/bin/env python3
"""Index and verify immutable Predict captures; no network, credentials or trading.

LGPL-3.0-or-later. Run after a collector has stopped. Refresh adds new captures
but refuses to replace an existing primary capture whose bytes changed.
"""
import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

CRATE = Path(__file__).resolve().parents[1] / "crates/nautilus-predict"
MANIFEST = CRATE / "tests/fixtures/evidence_manifest.json"


def tier(path, source):
    name, source = path.lower(), source.lower()
    if "synthetic" in name or "synthetic" in source or "loopback" in source:
        return "synthetic"
    if any(word in source for word in ("derived", "offline", "analytical", "hypothetical")):
        return "derived"
    if "validation" in name or "audit" in name or "review" in name:
        return "review"
    if "/live_" in name or "/exit_only_restart_" in name:
        return "primary_capture"
    if "mainnet" in source or "read-only get" in source or path.startswith("test_data/"):
        return "primary_capture"
    return "reference"


def inventory(crate=CRATE):
    entries = []
    staging = crate / "tests/fixtures/private_api/live_resting"
    for folder in (crate / "test_data", crate / "tests/fixtures"):
        for path in sorted(folder.rglob("*")):
            if (not path.is_file() or path.name == "evidence_manifest.json"
                    or path.is_relative_to(staging)):
                continue
            if path.suffix not in {".json", ".jsonl", ".log"}:
                continue
            raw = path.read_bytes()
            source = ""
            if path.suffix == ".json":
                data = json.loads(raw)
                if isinstance(data, dict):
                    source = str(data.get("source", ""))
            relative = path.relative_to(crate).as_posix()
            entries.append({"path": relative, "tier": tier(relative, source),
                "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    return {"version": 1, "policy": "Primary captures are immutable. Derived/synthetic outputs do not establish venue behavior.",
        "entries": sorted(entries, key=lambda e: e["path"])}


def verify(manifest, crate=CRATE, primary_only=False):
    failures = []
    for entry in manifest["entries"]:
        if primary_only and entry["tier"] != "primary_capture":
            continue
        path = crate / entry["path"]
        if not path.resolve().is_relative_to(crate.resolve()):
            raise ValueError("evidence path escapes crate")
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            failures.append(entry["path"])
    if failures:
        raise ValueError("Evidence missing or changed: " + ", ".join(failures))


def review_resting_run(directory):
    """Recheck completed local evidence without modifying any captured bytes."""
    from predict_resting_evidence import EvidenceError, validate_evidence, analyze_latency

    directory = Path(directory)
    validation = validate_evidence(directory, write=False)
    result = json.loads((directory / "result.json").read_text())
    cleanup = json.loads((directory / "supervisor_cleanup.json").read_text())
    final = json.loads((directory / "final_open_orders_check.json").read_text())
    for key in ("cleanupFailed", "nodeFailed", "feedbackWriteFailed"):
        if result.get(key, False):
            raise EvidenceError(f"run reports {key}")
    if cleanup.get("verifiedOpenOrders") != 0 or final.get("verifiedOpenOrders") != 0:
        raise EvidenceError("independent final OPEN checks are missing or nonzero")
    http = json.loads((directory / "http_responses.json").read_text())["events"]
    routes = Counter()
    durations = []
    for event in http:
        route = event["method"] + " " + re.sub(
            r"/0x[0-9a-fA-F]{64}(?=/|$)|/[0-9]+(?=/|$)", "/<id>", event["path"]
        )
        routes[route] += 1
        if event.get("completedAtUtc"):
            start = datetime.fromisoformat(event["observedAtUtc"])
            end = datetime.fromisoformat(event["completedAtUtc"])
            elapsed = end - start
            micros = (elapsed.days * 86400 + elapsed.seconds) * 1000000 + elapsed.microseconds
            if micros < 0:
                raise EvidenceError("HTTP wall clock moved backwards; timing is invalid")
            durations.append({"route": route, "status": event.get("status"),
                              "duration_ms": str(Decimal(micros) / 1000)})
    hashes = []
    for path in sorted(directory.iterdir()):
        if path.is_file() and path.suffix in {".json", ".jsonl", ".log"}:
            raw = path.read_bytes()
            hashes.append({"path": path.name, "bytes": len(raw),
                           "sha256": hashlib.sha256(raw).hexdigest()})
    return {"source": "offline review of captured resting-order facts; no network or writes",
            "case": validation["case"], "passed": True,
            "created_orders": validation["createdOrders"], "new_matches": 0,
            "final_open_orders": 0, "cash_and_positions_unchanged": True,
            "http_requests": len(http), "http_routes": dict(sorted(routes.items())),
            "http_durations": durations,
            "timing_limits": [
                "HTTP durations cover the test relay through upstream response, before local response forwarding.",
                "HTTP timestamps use the local wall clock and include relay overhead.",
                "Recorded venue event timestamps are not local WS receive or strategy callback times.",
                ("Explicit callback hooks measure WS-to-strategy latency; two orders cannot establish stable tail estimates."
                 if (directory / "latency.json").exists() else
                 "This capture does not establish WS-to-strategy latency or natural GTD expiry.")],
            "latency": analyze_latency(directory, write=False) if (directory / "latency.json").exists() else None,
            "run_metadata": json.loads((directory / "run_metadata.json").read_text())
            if (directory / "run_metadata.json").exists() else None,
            "files": hashes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--refresh", action="store_true", help="index new completed captures; protect prior primary bytes")
    mode.add_argument("--run", type=Path, help="review an existing resting-order directory offline; print JSON without changing recordings")
    args = parser.parse_args()
    if args.run is not None:
        print(json.dumps(review_resting_run(args.run), indent=2))
        return
    if args.refresh:
        if MANIFEST.exists():
            verify(json.loads(MANIFEST.read_text()), primary_only=True)
        report = inventory()
        MANIFEST.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    else:
        report = json.loads(MANIFEST.read_text())
        verify(report)
    print(json.dumps({"files": len(report["entries"]), "tiers": dict(Counter(e["tier"] for e in report["entries"]))}))


if __name__ == "__main__":
    main()
