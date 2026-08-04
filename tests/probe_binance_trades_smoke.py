#!/usr/bin/env python3
"""Run a short live Binance Futures perp capture with trades and validate readback."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from live_smoke_common import cleanup_probe_artifacts
from live_smoke_common import make_probe_paths
from live_smoke_common import print_catalog_summary
from live_smoke_common import PROJECT_ROOT
from live_smoke_common import run_capture_cli
from live_smoke_common import summarize_catalog
from live_smoke_common import write_temp_capture_config

SOURCE_CONFIG = PROJECT_ROOT / "examples" / "capture.binance-perp-trades.toml"
DERIVATIVES_PROBE = PROJECT_ROOT / "tests" / "python_catalog_derivatives_probe.py"
INSTRUMENT_ID = "ETHUSDT-PERP.BINANCE"
TRADE_FAMILY_NAMES = ("trade_tick", "trades")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a live Binance Futures perp trade capture smoke test.",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=180,
        help="Capture duration to inject into the temporary profile (default 3 minutes).",
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp",
        help="Directory where the temporary smoke catalog will be created.",
    )
    parser.add_argument(
        "--min-trade-rows",
        type=int,
        default=1,
        help="Minimum trade ticks required during PyO3 readback.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove generated catalog and config after successful validation.",
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
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if args.min_trade_rows < 0:
        parser.error("--min-trade-rows must be non-negative")

    catalog_dir, temp_config = make_probe_paths(
        args.catalog_root,
        "catalog-capture-binance-trades-smoke",
        "capture.binance-perp-trades-smoke",
    )
    write_temp_capture_config(SOURCE_CONFIG, temp_config, catalog_dir, args.seconds)

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)

    print(f"running live capture for {args.seconds}s", flush=True)
    run_capture_cli(args.cargo, temp_config)

    summary = summarize_catalog(catalog_dir)
    print_catalog_summary(catalog_dir, summary)
    assert_trade_family_present(summary)

    if not args.skip_readback_probe:
        probe_cmd = [
            sys.executable,
            str(DERIVATIVES_PROBE),
            str(catalog_dir),
            INSTRUMENT_ID,
            "1",
        ]
        if args.min_trade_rows > 0:
            probe_cmd.extend(["--min-trade-rows", str(args.min_trade_rows)])
        subprocess.run(probe_cmd, cwd=PROJECT_ROOT, check=True)

    if args.cleanup:
        cleanup_probe_artifacts(catalog_dir, temp_config)
        print("cleaned up generated catalog and config")

    print("Binance perp trades live smoke test succeeded")
    return 0

def assert_trade_family_present(summary: dict[str, dict[str, int | None]]) -> None:
    for family in TRADE_FAMILY_NAMES:
        stats = summary.get(family)
        if stats and int(stats.get("files", 0)) > 0:
            return
    raise RuntimeError(
        "expected trade parquet under data/trade_tick or data/trades; "
        f"got families={sorted(summary)}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
