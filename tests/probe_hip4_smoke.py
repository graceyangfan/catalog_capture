#!/usr/bin/env python3
"""Live smoke: HIP-4 priceBinary discovery, capture, and refresh tick."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pq = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_CONFIG = PROJECT_ROOT / "examples" / "capture.hyperliquid-hip4-btc-smoke.toml"
METADATA_FILE = "metadata/hip4_universe_resolutions.jsonl"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run HIP-4 priceBinary live smoke test.")
    parser.add_argument("--seconds", type=int, default=75, help="Capture duration override.")
    parser.add_argument("--idle-poll-secs", type=int, default=15, help="HIP-4 idle poll override.")
    parser.add_argument("--catalog-root", default="/tmp", help="Parent dir for temp catalog.")
    parser.add_argument("--min-quote-rows", type=int, default=1)
    parser.add_argument("--min-mark-rows", type=int, default=1)
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--cargo", default="cargo")
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")

    timestamp = int(time.time())
    catalog_dir = Path(args.catalog_root) / f"nautilus-catalog-capture-hip4-smoke-{timestamp}"
    temp_config = Path(args.catalog_root) / f"capture.hyperliquid-hip4-smoke.{timestamp}.toml"
    write_temp_config(temp_config, catalog_dir, args.seconds, args.idle_poll_secs)

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)

    discovery = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "run",
        "--config",
        str(temp_config),
        "--dry-run-resolve",
        "--option-universe-format",
        "json",
    ]
    print("running discovery dry-run...", flush=True)
    discovery_proc = subprocess.run(
        discovery,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if discovery_proc.returncode != 0:
        print(discovery_proc.stdout)
        print(discovery_proc.stderr, file=sys.stderr)
        return discovery_proc.returncode

    hip4_reports = parse_hip4_reports(discovery_proc.stdout)
    if not hip4_reports:
        print("discovery failed: no HIP-4 resolution report", file=sys.stderr)
        print(discovery_proc.stdout)
        return 1

    report = hip4_reports[0]
    print("discovery_ok", flush=True)
    print(json.dumps(report, indent=2), flush=True)
    outcome_ids = report.get("outcome_instrument_ids") or []
    perp_id = report.get("perp_instrument_id")
    if len(outcome_ids) < 2:
        print("discovery failed: expected YES/NO outcome instrument ids", file=sys.stderr)
        return 1
    if not perp_id:
        print("discovery failed: missing perp instrument id", file=sys.stderr)
        return 1

    capture_cmd = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "run",
        "--config",
        str(temp_config),
        "--skip-post-run-report",
    ]
    print("running live capture...", flush=True)
    capture_proc = subprocess.run(
        capture_cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    print(capture_proc.stdout)
    if capture_proc.stderr:
        print(capture_proc.stderr, file=sys.stderr)
    if capture_proc.returncode != 0:
        return capture_proc.returncode

    metadata_path = catalog_dir / METADATA_FILE
    if not metadata_path.is_file():
        print(f"missing metadata file: {metadata_path}", file=sys.stderr)
        return 1

    records = load_jsonl(metadata_path)
    startup = [r for r in records if r.get("event_kind") == "startup"]
    if len(startup) != 1:
        print(f"expected 1 startup metadata record, got {len(startup)}", file=sys.stderr)
        return 1

    startup_record = startup[0]
    if startup_record.get("question_id") != report.get("question_id"):
        print("startup metadata question_id mismatch with discovery", file=sys.stderr)
        return 1

    quote_rows = count_parquet_rows(catalog_dir, "quotes", outcome_ids)
    mark_rows = count_parquet_rows(catalog_dir, "mark_prices", [perp_id])
    print(f"quote_rows={quote_rows} mark_rows={mark_rows}", flush=True)
    if quote_rows < args.min_quote_rows:
        print("insufficient outcome quote rows", file=sys.stderr)
        return 1
    if mark_rows < args.min_mark_rows:
        print("insufficient perp mark_price rows", file=sys.stderr)
        return 1

    refresh_records = [r for r in records if r.get("event_kind") == "refresh"]
    print(
        f"refresh_records={len(refresh_records)} (rotation delta only recorded on question change)",
        flush=True,
    )

    combined_output = capture_proc.stdout + capture_proc.stderr
    if "HIP-4 universe refresh failed" in combined_output:
        print("capture logged HIP-4 refresh failure", file=sys.stderr)
        return 1
    if "Cannot start a runtime from within a runtime" in combined_output:
        print("capture panicked during HIP-4 refresh", file=sys.stderr)
        return 1

    print("hip4_smoke_ok", flush=True)
    if args.cleanup:
        shutil.rmtree(catalog_dir, ignore_errors=True)
        temp_config.unlink(missing_ok=True)
    return 0


def write_temp_config(
    path: Path,
    catalog_dir: Path,
    capture_seconds: int,
    idle_poll_secs: int,
) -> None:
    text = SOURCE_CONFIG.read_text()
    text = text.replace(
        'catalog_uri = "file:///tmp/nautilus-catalog-capture-hyperliquid-hip4-smoke"',
        f'catalog_uri = "file://{catalog_dir}"',
    )
    lines = []
    for line in text.splitlines():
        if line.startswith("capture_seconds ="):
            lines.append(f"capture_seconds = {capture_seconds}")
        elif line.startswith("idle_poll_secs ="):
            lines.append(f"idle_poll_secs = {idle_poll_secs}")
        else:
            lines.append(line)
    path.write_text("\n".join(lines) + "\n")


def parse_hip4_reports(stdout: str) -> list[dict]:
    decoder = json.JSONDecoder()
    idx = 0
    while idx < len(stdout):
        while idx < len(stdout) and stdout[idx].isspace():
            idx += 1
        if idx >= len(stdout):
            break
        if stdout[idx] != "[":
            idx += 1
            continue
        try:
            payload, end = decoder.raw_decode(stdout, idx)
        except json.JSONDecodeError:
            idx += 1
            continue
        if (
            isinstance(payload, list)
            and payload
            and isinstance(payload[0], dict)
            and "market_class" in payload[0]
        ):
            return payload
        idx = end
    return []


def load_jsonl(path: Path) -> list[dict]:
    records = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def parquet_files_for_instrument(family_dir: Path, instrument_id: str) -> list[Path]:
    instrument_dir = family_dir / instrument_id
    if instrument_dir.is_dir():
        return sorted(instrument_dir.rglob("*.parquet"))
    return [
        path
        for path in family_dir.rglob("*.parquet")
        if instrument_id in path.parts
    ]


def count_parquet_rows(catalog_dir: Path, family: str, instrument_ids: list[str]) -> int:
    family_dir = catalog_dir / "data" / family
    if not family_dir.is_dir():
        return 0

    if pq is None:
        return sum(
            len(parquet_files_for_instrument(family_dir, instrument_id))
            for instrument_id in instrument_ids
        )

    total = 0
    for instrument_id in instrument_ids:
        for parquet_file in parquet_files_for_instrument(family_dir, instrument_id):
            total += pq.read_table(parquet_file).num_rows
    return total


if __name__ == "__main__":
    raise SystemExit(main())