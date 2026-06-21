#!/usr/bin/env python3
"""Run 3-minute live bar capture smokes across all supported venues (Step 6c)."""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_CLI_VALIDATE = importlib.util.spec_from_file_location(
    "option_universe_cli_validate",
    PROJECT_ROOT / "tests" / "option_universe_cli_validate.py",
)
_cli_validate_mod = importlib.util.module_from_spec(_CLI_VALIDATE)
assert _CLI_VALIDATE.loader is not None
_CLI_VALIDATE.loader.exec_module(_cli_validate_mod)

ALL_BARS_SMOKE_VENUES = _cli_validate_mod.ALL_BARS_SMOKE_VENUES

STANDALONE_BAR_PROBES = {
    "binance": PROJECT_ROOT / "tests" / "probe_binance_bars_smoke.py",
    "hyperliquid": PROJECT_ROOT / "tests" / "probe_hyperliquid_bars_smoke.py",
}
OPTION_UNIVERSE_BAR_PROBE = PROJECT_ROOT / "tests" / "probe_option_universe_bars_smoke.py"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run live perp 1m bar capture smokes for all supported venues "
            "(Step 6c, default 3 minutes)."
        ),
    )
    parser.add_argument(
        "--venue",
        choices=(*sorted(ALL_BARS_SMOKE_VENUES), "all"),
        default="all",
        help=(
            "Venue profile to run. 'all' runs binance, hyperliquid, "
            "deribit-research, bybit, and okx."
        ),
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=180,
        help="Capture duration to inject into temporary profiles.",
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

    venues = sorted(ALL_BARS_SMOKE_VENUES) if args.venue == "all" else [args.venue]
    failures: list[tuple[str, str]] = []
    for venue in venues:
        try:
            run_venue_bars_smoke(venue, args)
        except subprocess.CalledProcessError as exc:
            failures.append((venue, f"subprocess failed with exit code {exc.returncode}"))
            print(f"\n[{venue}] FAILED: exit code {exc.returncode}", file=sys.stderr)
        except Exception as exc:  # noqa: BLE001
            failures.append((venue, str(exc)))
            print(f"\n[{venue}] FAILED: {exc}", file=sys.stderr)

    if failures:
        print("\nFailures:", file=sys.stderr)
        for venue, message in failures:
            print(f"- {venue}: {message}", file=sys.stderr)
        return 1
    return 0


def run_venue_bars_smoke(venue: str, args: argparse.Namespace) -> None:
    command = build_probe_command(venue, args)
    print(f"\n[{venue}] running {command[1]}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    print(f"[{venue}] bars smoke succeeded", flush=True)


def build_probe_command(venue: str, args: argparse.Namespace) -> list[str]:
    if venue in STANDALONE_BAR_PROBES:
        command = [
            sys.executable,
            str(STANDALONE_BAR_PROBES[venue]),
            "--seconds",
            str(args.seconds),
            "--catalog-root",
            args.catalog_root,
            "--min-bar-rows",
            str(args.min_bar_rows),
            "--cargo",
            args.cargo,
        ]
    else:
        command = [
            sys.executable,
            str(OPTION_UNIVERSE_BAR_PROBE),
            "--venue",
            venue,
            "--seconds",
            str(args.seconds),
            "--catalog-root",
            args.catalog_root,
            "--min-bar-rows",
            str(args.min_bar_rows),
            "--cargo",
            args.cargo,
        ]
    if args.cleanup:
        command.append("--cleanup")
    if args.skip_readback_probe:
        command.append("--skip-readback-probe")
    return command


if __name__ == "__main__":
    raise SystemExit(main())
