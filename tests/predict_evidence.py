#!/usr/bin/env python3
"""Index and verify immutable Predict captures; no network, credentials or trading.

LGPL-3.0-or-later. Run after a collector has stopped. Refresh adds new captures
but refuses to replace an existing primary capture whose bytes changed.
"""
import argparse
import hashlib
import json
from collections import Counter
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
    for folder in (crate / "test_data", crate / "tests/fixtures"):
        for path in sorted(folder.rglob("*")):
            if not path.is_file() or path.name == "evidence_manifest.json":
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="index new completed captures; protect prior primary bytes")
    args = parser.parse_args()
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
