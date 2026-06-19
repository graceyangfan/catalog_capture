#!/usr/bin/env python3
"""Run live option-universe smoke captures and summarize parquet output."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional local validation dependency.
    pq = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
READBACK_PROBE = PROJECT_ROOT / "tests" / "python_catalog_option_universe_probe.py"
METRICS_PROBE = PROJECT_ROOT / "tests" / "python_option_universe_metrics_probe.py"

VENUE_CONFIGS = {
    "deribit": PROJECT_ROOT / "examples" / "capture.deribit-btc-universe.toml",
    "okx": PROJECT_ROOT / "examples" / "capture.okx-btc-universe.toml",
    "bybit": PROJECT_ROOT / "examples" / "capture.bybit-btc-universe.toml",
}

REQUIRED_FAMILIES = (
    "instruments",
    "quotes",
    "option_greeks",
    "mark_prices",
    "index_prices",
    "funding_rate_update",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one or more live option-universe capture smoke tests.",
    )
    parser.add_argument(
        "--venue",
        choices=(*VENUE_CONFIGS.keys(), "all"),
        default="all",
        help="Venue smoke test to run. Defaults to all supported venues.",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=30,
        help="Capture duration to inject into the temporary profile.",
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp",
        help="Directory where temporary smoke catalogs will be created.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove generated catalogs after successful validation.",
    )
    parser.add_argument(
        "--cargo",
        default="cargo",
        help="Cargo executable to use.",
    )
    parser.add_argument(
        "--skip-readback-probe",
        action="store_true",
        help="Only validate parquet files; skip Nautilus ParquetDataCatalog readback.",
    )
    parser.add_argument(
        "--metrics-probe",
        action="store_true",
        help="Print a lightweight ATM/skew metrics snapshot after readback.",
    )
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")

    venues = list(VENUE_CONFIGS) if args.venue == "all" else [args.venue]
    failures = []
    for venue in venues:
        try:
            run_venue_smoke(venue, args)
        except Exception as exc:  # noqa: BLE001
            failures.append((venue, exc))
            print(f"\n[{venue}] FAILED: {exc}", file=sys.stderr)

    if failures:
        print("\nFailures:", file=sys.stderr)
        for venue, exc in failures:
            print(f"- {venue}: {exc}", file=sys.stderr)
        return 1
    return 0


def run_venue_smoke(venue: str, args: argparse.Namespace) -> None:
    timestamp = int(time.time())
    catalog_dir = (
        Path(args.catalog_root)
        / f"nautilus-catalog-capture-{venue}-universe-smoke-{timestamp}"
    )
    temp_config = (
        Path(args.catalog_root)
        / f"capture.{venue}-btc-universe-smoke.{timestamp}.toml"
    )

    write_temp_config(VENUE_CONFIGS[venue], temp_config, catalog_dir, args.seconds)

    print(f"\n[{venue}] config={temp_config}", flush=True)
    print(f"[{venue}] catalog={catalog_dir}", flush=True)
    command = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "run",
        "--config",
        str(temp_config),
        "--print-option-universe",
        "--option-universe-format",
        "text",
    ]
    print(f"[{venue}] running live capture for {args.seconds}s", flush=True)
    output = run_and_stream(command)

    summary = summarize_catalog(catalog_dir)
    print_catalog_summary(venue, catalog_dir, summary)
    validate_summary(summary)

    if not args.skip_readback_probe:
        perp_id, option_ids = parse_resolution_output(output)
        run_readback_probe(catalog_dir, perp_id, option_ids)
        if args.metrics_probe:
            run_metrics_probe(catalog_dir, perp_id, option_ids)
    elif args.metrics_probe:
        perp_id, option_ids = parse_resolution_output(output)
        run_metrics_probe(catalog_dir, perp_id, option_ids)

    if args.cleanup:
        shutil.rmtree(catalog_dir)
        temp_config.unlink(missing_ok=True)
        print(f"[{venue}] cleaned up generated catalog and config")


def run_and_stream(command: list[str]) -> str:
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    lines = []
    for line in process.stdout:
        print(line, end="", flush=True)
        lines.append(line)
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    return "".join(lines)


def parse_resolution_output(output: str) -> tuple[str, list[str]]:
    perp_id = None
    option_ids: list[str] = []

    for raw_line in output.splitlines():
        line = strip_ansi(raw_line.strip())
        if line.startswith("perp="):
            perp_id = line.removeprefix("perp=").strip()
        elif line.startswith("options=["):
            options_text = line.removeprefix("options=[").removesuffix("]")
            option_ids = [
                value.strip()
                for value in options_text.split(",")
                if value.strip()
            ]

    if not perp_id or perp_id == "-":
        raise RuntimeError("failed to parse resolved perp id from capture output")
    if not option_ids:
        raise RuntimeError("failed to parse resolved option ids from capture output")
    return perp_id, option_ids


def strip_ansi(value: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", value)


def run_readback_probe(catalog_dir: Path, perp_id: str, option_ids: list[str]) -> None:
    print(
        f"[readback] probing {len(option_ids)} options plus {perp_id}",
        flush=True,
    )
    command = [
        sys.executable,
        str(READBACK_PROBE),
        str(catalog_dir),
        "--perp-id",
        perp_id,
    ]
    for option_id in option_ids:
        command.extend(["--option-id", option_id])
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def run_metrics_probe(catalog_dir: Path, perp_id: str, option_ids: list[str]) -> None:
    print(
        f"[metrics] computing snapshot for {len(option_ids)} options plus {perp_id}",
        flush=True,
    )
    command = [
        sys.executable,
        str(METRICS_PROBE),
        str(catalog_dir),
        "--perp-id",
        perp_id,
    ]
    for option_id in option_ids:
        command.extend(["--option-id", option_id])
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def write_temp_config(source: Path, target: Path, catalog_dir: Path, seconds: int) -> None:
    lines = []
    for line in source.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("capture_seconds ="):
            lines.append(f"capture_seconds = {seconds}")
        elif stripped.startswith("catalog_uri ="):
            lines.append(f'catalog_uri = "file://{catalog_dir}"')
        else:
            lines.append(line)
    target.write_text("\n".join(lines) + "\n")


def summarize_catalog(catalog_dir: Path) -> dict[str, dict[str, int | None]]:
    data_dir = catalog_dir / "data"
    if not data_dir.exists():
        raise RuntimeError(f"catalog data dir was not created: {data_dir}")

    summary: dict[str, dict[str, int | None]] = {}
    for family_dir in sorted(path for path in data_dir.iterdir() if path.is_dir()):
        files = sorted(family_dir.glob("**/*.parquet"))
        sample_rows = None
        if pq is not None:
            sample_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in files[:5])
        summary[family_dir.name] = {
            "files": len(files),
            "sample_rows_first_5": sample_rows,
        }
    return summary


def print_catalog_summary(
    venue: str,
    catalog_dir: Path,
    summary: dict[str, dict[str, int | None]],
) -> None:
    total_files = sum(int(values["files"]) for values in summary.values())
    print(f"\n[{venue}] parquet_files={total_files}")
    print(f"[{venue}] catalog={catalog_dir}")
    for family in sorted(summary):
        values = summary[family]
        rows = values["sample_rows_first_5"]
        row_text = "unavailable" if rows is None else str(rows)
        print(f"[{venue}] {family}: files={values['files']} sample_rows_first_5={row_text}")


def validate_summary(summary: dict[str, dict[str, int | None]]) -> None:
    missing = [
        family
        for family in REQUIRED_FAMILIES
        if summary.get(family, {}).get("files", 0) == 0
    ]
    if missing:
        raise RuntimeError(f"missing required parquet families: {', '.join(missing)}")

    if pq is None:
        print("pyarrow is not installed; skipped parquet row-count validation")
        return

    empty = [
        family
        for family in REQUIRED_FAMILIES
        if int(summary[family]["sample_rows_first_5"] or 0) == 0
    ]
    if empty:
        raise RuntimeError(f"required parquet families had zero sample rows: {', '.join(empty)}")


if __name__ == "__main__":
    raise SystemExit(main())
