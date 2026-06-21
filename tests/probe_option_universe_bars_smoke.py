#!/usr/bin/env python3
"""Run 3-minute live option-universe captures with perp bar readback validation."""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMOKE_PROBE = PROJECT_ROOT / "tests" / "probe_option_universe_smoke.py"
_CLI_VALIDATE = importlib.util.spec_from_file_location(
    "option_universe_cli_validate",
    PROJECT_ROOT / "tests" / "option_universe_cli_validate.py",
)
_cli_validate_mod = importlib.util.module_from_spec(_CLI_VALIDATE)
assert _CLI_VALIDATE.loader is not None
_CLI_VALIDATE.loader.exec_module(_cli_validate_mod)

_spec = importlib.util.spec_from_file_location("probe_option_universe_smoke", SMOKE_PROBE)
_smoke_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_smoke_mod)

BARS_SMOKE_VENUES = _cli_validate_mod.BARS_SMOKE_VENUES
BARS_SMOKE_PRESET = _cli_validate_mod.BARS_SMOKE_PRESET
BAR_TYPES_BY_VENUE = _cli_validate_mod.BAR_TYPES_BY_VENUE
VENUE_CONFIGS = _smoke_mod.VENUE_CONFIGS
write_temp_config = _smoke_mod.write_temp_config
run_and_stream = _smoke_mod.run_and_stream
summarize_catalog = _smoke_mod.summarize_catalog
print_catalog_summary = _smoke_mod.print_catalog_summary
parse_resolution_output = _smoke_mod.parse_resolution_output
run_validation_suite = _cli_validate_mod.run_validation_suite
READBACK_OPTION_SAMPLE_LIMIT = _smoke_mod.READBACK_OPTION_SAMPLE_LIMIT
ALL_STRIKES_VENUES = _smoke_mod.ALL_STRIKES_VENUES


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run live option-universe bar capture smokes (Step 6c, default 3 minutes)."
        ),
    )
    parser.add_argument(
        "--venue",
        choices=(*sorted(BARS_SMOKE_VENUES), "all"),
        default="all",
        help="Venue profile to run. 'all' runs deribit-research, bybit, and okx.",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=180,
        help="Capture duration to inject into the temporary profile.",
    )
    parser.add_argument(
        "--min-bar-rows",
        type=int,
        default=1,
        help="Minimum bar rows required during validation.",
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
        help="Only validate parquet files; skip ParquetDataCatalog readback.",
    )
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if args.min_bar_rows <= 0:
        parser.error("--min-bar-rows must be positive")

    venues = sorted(BARS_SMOKE_VENUES) if args.venue == "all" else [args.venue]
    failures = []
    for venue in venues:
        try:
            run_bars_venue_smoke(venue, args)
        except Exception as exc:  # noqa: BLE001
            failures.append((venue, exc))
            print(f"\n[{venue}] FAILED: {exc}", file=sys.stderr)

    if failures:
        print("\nFailures:", file=sys.stderr)
        for venue, exc in failures:
            print(f"- {venue}: {exc}", file=sys.stderr)
        return 1
    return 0


def run_bars_venue_smoke(venue: str, args: argparse.Namespace) -> None:
    timestamp = int(time.time())
    catalog_dir = (
        Path(args.catalog_root)
        / f"nautilus-catalog-capture-{venue}-universe-bars-smoke-{timestamp}"
    )
    temp_config = (
        Path(args.catalog_root)
        / f"capture.{venue}-btc-universe-bars-smoke.{timestamp}.toml"
    )
    bar_type = BAR_TYPES_BY_VENUE[venue]

    write_temp_config(VENUE_CONFIGS[venue], temp_config, catalog_dir, args.seconds)

    print(f"\n[{venue}] config={temp_config}", flush=True)
    print(f"[{venue}] catalog={catalog_dir}", flush=True)
    print(f"[{venue}] bar_type={bar_type}", flush=True)
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
        "--skip-post-run-report",
    ]
    print(f"[{venue}] running live capture for {args.seconds}s", flush=True)
    output = run_and_stream(command)

    summary = summarize_catalog(catalog_dir)
    print_catalog_summary(venue, catalog_dir, summary)
    assert_bar_family_present(venue, summary, bar_type)

    perp_id, option_ids = parse_resolution_output(output)
    readback_option_ids = option_ids
    if venue in ALL_STRIKES_VENUES and len(option_ids) > READBACK_OPTION_SAMPLE_LIMIT:
        readback_option_ids = option_ids[:READBACK_OPTION_SAMPLE_LIMIT]

    run_validation_suite(
        catalog_dir,
        temp_config,
        venue,
        args,
        readback_option_ids=None if args.skip_readback_probe else readback_option_ids,
        readback_perp_id=None if args.skip_readback_probe else perp_id,
        preset_override=BARS_SMOKE_PRESET,
    )

    if not args.skip_readback_probe:
        probe_cmd = [
            sys.executable,
            str(PROJECT_ROOT / "tests" / "python_catalog_option_universe_probe.py"),
            str(catalog_dir),
            "--perp-id",
            perp_id,
            "--min-rows",
            "1",
            "--min-trade-rows",
            "0",
            "--bar-type",
            bar_type,
            "--bars-only",
        ]
        subprocess.run(probe_cmd, cwd=PROJECT_ROOT, check=True)

    if args.cleanup:
        import shutil

        shutil.rmtree(catalog_dir, ignore_errors=True)
        temp_config.unlink(missing_ok=True)
        print(f"[{venue}] cleaned up generated catalog and config")

    print(f"[{venue}] option-universe bars smoke succeeded")


def assert_bar_family_present(
    venue: str,
    summary: dict[str, dict[str, int | None]],
    bar_type: str,
) -> None:
    for family in ("bar", "bars"):
        stats = summary.get(family)
        if not stats or int(stats.get("files", 0)) == 0:
            continue
        sample_rows = stats.get("sample_rows_first_5")
        print(
            f"[{venue}] bars: family={family} files={stats['files']} "
            f"sample_rows_first_5={sample_rows}",
            flush=True,
        )
        if sample_rows is not None and int(sample_rows) == 0:
            raise RuntimeError(f"[{venue}] bar parquet family {family} had zero sample rows")
        return

    raise RuntimeError(
        f"[{venue}] missing bar parquet family for {bar_type}; families={sorted(summary)}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
