"""Shared option-universe CLI validation helpers for smoke and soak probes."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

AUTOREFRESH_VALIDATION_VENUES = frozenset(
    {
        "deribit-autorefresh",
        "okx-autorefresh",
        "bybit-autorefresh",
        "deribit-oi-ranked-autorefresh",
    }
)

VALIDATION_PRESET_BY_VENUE = {
    "deribit-autorefresh": "rolling-autorefresh",
    "okx-autorefresh": "rolling-autorefresh",
    "bybit-autorefresh": "rolling-autorefresh",
    "deribit-oi-ranked-autorefresh": "rolling-autorefresh",
    "deribit-research": "research",
    "bybit": "venue-trades",
    "okx": "venue-trades",
    "bybit-oi-ranked": "venue-trades",
    "okx-oi-ranked": "venue-trades",
}

METADATA_STRIKE_MODE_BY_VENUE = {
    "deribit-oi-ranked": ("oi-ranked", 3),
    "deribit-oi-ranked-autorefresh": ("oi-ranked", 3),
    "bybit-oi-ranked": ("oi-ranked", 3),
    "okx-oi-ranked": ("oi-ranked", 3),
    "deribit-all": ("all", None),
}


def run_validation_suite(
    catalog_dir: Path,
    config_path: Path,
    venue: str,
    args: argparse.Namespace,
    *,
    readback_option_ids: list[str] | None = None,
    readback_perp_id: str | None = None,
) -> None:
    """Run the unified validate-option-universe CLI suite."""
    command = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "validate-option-universe",
        "--catalog-uri",
        f"file://{catalog_dir}",
        "--config",
        str(config_path),
        "--option-universe-format",
        "text",
        "--skip-inspect",
    ]
    preset = VALIDATION_PRESET_BY_VENUE.get(venue)
    if preset is not None:
        command.extend(["--preset", preset])
    if args.require_contract_state:
        command.append("--require-contract-state")
    if args.require_refresh_change and venue in AUTOREFRESH_VALIDATION_VENUES:
        command.append("--require-refresh-change")

    strike_mode = METADATA_STRIKE_MODE_BY_VENUE.get(venue)
    if strike_mode is not None:
        mode, top_n = strike_mode
        command.extend(["--strike-mode", mode])
        if mode == "oi-ranked":
            command.extend(["--oi-ranked-top-n", str(top_n)])

    if args.skip_readback_probe:
        command.append("--skip-readback")
    if readback_perp_id is not None:
        command.extend(["--perp-id", readback_perp_id])
    for option_id in readback_option_ids or []:
        command.extend(["--option-id", option_id])

    print(f"[{venue}] cli validation suite preset={preset or 'inferred'}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
