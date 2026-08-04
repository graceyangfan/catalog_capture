#!/usr/bin/env python3
"""Run a short live Binance Futures custom-data capture and validate catalog readback."""

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

CATALOG_PROBE = PROJECT_ROOT / "tests" / "python_catalog_binance_custom_probe.py"
INSTRUMENT_ID = "ETHUSDT-PERP.BINANCE"

KIND_CONFIG = {
    "ticker": {
        "source_config": PROJECT_ROOT / "examples" / "capture.binance-futures-ticker.toml",
        "catalog_prefix": "catalog-capture-binance-futures-ticker-smoke",
        "config_prefix": "capture.binance-futures-ticker-smoke",
        "type_name": "BinanceFuturesTicker",
        "default_min_rows": 1,
    },
    "liquidation": {
        "source_config": PROJECT_ROOT / "examples" / "capture.binance-futures-liquidation.toml",
        "catalog_prefix": "catalog-capture-binance-futures-liquidation-smoke",
        "config_prefix": "capture.binance-futures-liquidation-smoke",
        "type_name": "BinanceFuturesLiquidation",
        "default_min_rows": 0,
        "all_market": True,
    },
}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a live Binance Futures custom-data smoke test.",
    )
    parser.add_argument(
        "--kind",
        choices=sorted(KIND_CONFIG),
        default="ticker",
        help="Custom data family to validate (default: ticker).",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=60,
        help="Capture duration to inject into the temporary profile (default: 60s).",
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp",
        help="Directory where the temporary smoke catalog will be created.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=None,
        help="Minimum custom rows required during readback (default depends on --kind).",
    )
    parser.add_argument(
        "--min-quotes",
        type=int,
        default=1,
        help="Minimum quote rows required during readback (default: 1).",
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
    if args.min_quotes < 0:
        parser.error("--min-quotes must be non-negative")

    spec = KIND_CONFIG[args.kind]
    min_rows = spec["default_min_rows"] if args.min_rows is None else args.min_rows
    if min_rows < 0:
        parser.error("--min-rows must be non-negative")

    catalog_dir, temp_config = make_probe_paths(
        args.catalog_root,
        spec["catalog_prefix"],
        spec["config_prefix"],
    )
    write_temp_capture_config(spec["source_config"], temp_config, catalog_dir, args.seconds)

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)
    print(f"type_name={spec['type_name']}", flush=True)
    print(
        f"instrument_id={'all-market' if spec.get('all_market') else INSTRUMENT_ID}",
        flush=True,
    )

    print(f"running live {args.kind} capture for {args.seconds}s", flush=True)
    run_capture_cli(args.cargo, temp_config)

    summary = summarize_catalog(catalog_dir)
    print_catalog_summary(catalog_dir, summary)
    assert_custom_family_present(summary, spec["type_name"], min_rows)

    if not args.skip_readback_probe:
        probe_cmd = [
            sys.executable,
            str(CATALOG_PROBE),
            str(catalog_dir),
            spec["type_name"],
        ]
        if spec.get("all_market"):
            probe_cmd.extend([str(min_rows), "--all-market", "--min-quotes", str(args.min_quotes)])
        else:
            probe_cmd.extend([INSTRUMENT_ID, str(min_rows), "--min-quotes", str(args.min_quotes)])
        subprocess.run(probe_cmd, cwd=PROJECT_ROOT, check=True)

    if args.cleanup:
        cleanup_probe_artifacts(catalog_dir, temp_config)
        print("cleaned up generated catalog and config")

    print(f"Binance Futures {args.kind} live smoke test succeeded")
    return 0

def assert_custom_family_present(
    summary: dict[str, dict[str, int | None]],
    type_name: str,
    min_rows: int,
) -> None:
    custom_stats = summary.get("custom")
    if not custom_stats or int(custom_stats.get("files", 0)) == 0:
        raise RuntimeError(
            f"expected custom parquet for {type_name}; got families={sorted(summary)}"
        )

    sample_rows = custom_stats.get("sample_rows_first_5")
    if sample_rows is not None and min_rows > 0 and int(sample_rows) == 0:
        raise RuntimeError(f"custom parquet family for {type_name} had zero sample rows")


if __name__ == "__main__":
    raise SystemExit(main())
