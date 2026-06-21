#!/usr/bin/env python3
"""Run longer option-universe soak captures using pre-defined profile matrices.

Each profile reuses the smoke probe capture path. After every soak capture,
validation runs through the unified CLI command:

    validate-option-universe

That suite covers metadata lineage, ParquetDataCatalog readback, and catalog
parquet checks in one pass (see tests/option_universe_cli_validate.py).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SMOKE_PROBE = PROJECT_ROOT / "tests" / "probe_option_universe_smoke.py"

SOAK_PRESETS = {
    "daily-live": [
        "deribit-autorefresh",
        "okx-autorefresh",
        "bybit-autorefresh",
    ],
    "research-live": [
        "deribit-research",
        "okx",
        "bybit",
    ],
    "oi-ranked": [
        "deribit-oi-ranked-autorefresh",
        "bybit-oi-ranked",
        "okx-oi-ranked",
    ],
    "all-chain": [
        "deribit-all",
    ],
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run long-duration option-universe soak presets.",
    )
    parser.add_argument(
        "--preset",
        choices=tuple(SOAK_PRESETS.keys()) + ("full",),
        default="daily-live",
        help=(
            "Soak preset to run. 'full' runs daily-live, research-live, "
            "oi-ranked, then all-chain."
        ),
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=7200,
        help="Capture duration per venue/profile.",
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp",
        help="Directory where temporary soak catalogs will be created.",
    )
    parser.add_argument(
        "--cargo",
        default="cargo",
        help="Cargo executable to use.",
    )
    parser.add_argument(
        "--skip-readback-probe",
        action="store_true",
        help="Skip ParquetDataCatalog readback inside validate-option-universe.",
    )
    parser.add_argument(
        "--metrics-probe",
        action="store_true",
        help="Run the lightweight metrics probe after successful validation.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove generated catalogs after each successful soak run.",
    )
    parser.add_argument(
        "--require-contract-state",
        action="store_true",
        help="Require instrument_status and instrument_closes rows during validation.",
    )
    parser.add_argument(
        "--require-refresh-change",
        action="store_true",
        help=(
            "Require at least one runtime refresh delta for autorefresh profiles. "
            "Best used on longer rolling-live runs."
        ),
    )
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")

    preset_names = (
        ["daily-live", "research-live", "oi-ranked", "all-chain"]
        if args.preset == "full"
        else [args.preset]
    )

    for preset_name in preset_names:
        print(f"\n=== soak preset: {preset_name} ===", flush=True)
        for venue in SOAK_PRESETS[preset_name]:
            run_smoke_probe(venue, args)

    return 0


def run_smoke_probe(venue: str, args: argparse.Namespace) -> None:
    command = [
        sys.executable,
        str(SMOKE_PROBE),
        "--venue",
        venue,
        "--seconds",
        str(args.seconds),
        "--catalog-root",
        args.catalog_root,
        "--cargo",
        args.cargo,
    ]
    if args.skip_readback_probe:
        command.append("--skip-readback-probe")
    if args.metrics_probe:
        command.append("--metrics-probe")
    if args.cleanup:
        command.append("--cleanup")
    if args.require_contract_state:
        command.append("--require-contract-state")
    if args.require_refresh_change:
        command.append("--require-refresh-change")

    print(
        f"[soak] venue={venue} seconds={args.seconds} "
        f"cli_validate=validate-option-universe "
        f"readback={'off' if args.skip_readback_probe else 'on'}",
        flush=True,
    )
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


if __name__ == "__main__":
    raise SystemExit(main())