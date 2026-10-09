#!/usr/bin/env python3
"""Live smoke: one HIP-4 priceBinary capture plus catalog readback."""

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
    parser.add_argument(
        "--min-quote-rows",
        type=int,
        default=1,
        help="Minimum quote rows; uses parquet-file count when pyarrow is unavailable",
    )
    parser.add_argument(
        "--min-mark-rows",
        type=int,
        default=0,
        help="Minimum perp mark rows when the selected HIP-4 config enables them",
    )
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--cargo", default="cargo")
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")

    timestamp = int(time.time())
    catalog_dir = Path(args.catalog_root) / f"catalog-capture-hip4-smoke-{timestamp}"
    temp_config = Path(args.catalog_root) / f"capture.hyperliquid-hip4-smoke.{timestamp}.toml"
    write_temp_config(temp_config, catalog_dir, args.seconds, args.idle_poll_secs)

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)

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
    outcome_ids = startup_record.get("outcome_instrument_ids") or []
    if len(outcome_ids) < 2:
        print("startup metadata lacks a YES/NO outcome pair", file=sys.stderr)
        return 1
    perp_id = startup_record.get("perp_instrument_id")
    if args.min_mark_rows > 0 and not perp_id:
        print(
            "min-mark-rows requires a config with include_perp_mark = true and mark_prices",
            file=sys.stderr,
        )
        return 1
    print("startup_resolution", flush=True)
    print(json.dumps(startup_record, indent=2), flush=True)

    quote_count, quote_unit = count_parquet_rows(catalog_dir, "quotes", outcome_ids)
    mark_count, mark_unit = (
        count_parquet_rows(catalog_dir, "mark_prices", [perp_id])
        if perp_id
        else (0, "rows")
    )
    print(
        f"quote_{quote_unit}={quote_count} mark_{mark_unit}={mark_count}",
        flush=True,
    )
    if quote_count < args.min_quote_rows:
        print("insufficient outcome quote data", file=sys.stderr)
        return 1
    if perp_id and mark_count < args.min_mark_rows:
        print("insufficient perp mark_price data", file=sys.stderr)
        return 1

    refresh_records = [r for r in records if r.get("event_kind") == "refresh"]
    print(
        f"refresh_records={len(refresh_records)} (rotation delta only recorded on question change)",
        flush=True,
    )

    combined_output = capture_proc.stdout + capture_proc.stderr
    if "Cannot start a runtime from within a runtime" in combined_output:
        print("capture panicked during HIP-4 refresh", file=sys.stderr)
        return 1
    refresh_failures = combined_output.count("HIP-4 universe refresh failed")
    if refresh_failures:
        print(
            f"refresh_failures={refresh_failures} (current confirmed universe retained)",
            flush=True,
        )

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
        'catalog_uri = "file://./data/hyperliquid-hip4-smoke"',
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


def count_parquet_rows(
    catalog_dir: Path, family: str, instrument_ids: list[str]
) -> tuple[int, str]:
    family_dir = catalog_dir / "data" / family
    if not family_dir.is_dir():
        return 0, "rows" if pq is not None else "files"

    if pq is None:
        return (
            sum(
                len(parquet_files_for_instrument(family_dir, instrument_id))
                for instrument_id in instrument_ids
            ),
            "files",
        )

    total = 0
    for instrument_id in instrument_ids:
        for parquet_file in parquet_files_for_instrument(family_dir, instrument_id):
            total += pq.read_table(parquet_file).num_rows
    return total, "rows"


if __name__ == "__main__":
    raise SystemExit(main())
