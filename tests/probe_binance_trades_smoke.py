#!/usr/bin/env python3
"""Run a short live Binance Futures perp capture with trades and validate readback."""

from __future__ import annotations

import argparse
import importlib.util
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

    timestamp = int(time.time())
    catalog_dir = (
        Path(args.catalog_root) / f"nautilus-catalog-capture-binance-trades-smoke-{timestamp}"
    )
    temp_config = Path(args.catalog_root) / f"capture.binance-perp-trades-smoke.{timestamp}.toml"
    write_temp_config(SOURCE_CONFIG, temp_config, catalog_dir, args.seconds)

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)

    command = [
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
    print(f"running live capture for {args.seconds}s", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)

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
        shutil.rmtree(catalog_dir, ignore_errors=True)
        temp_config.unlink(missing_ok=True)
        print("cleaned up generated catalog and config")

    print("Binance perp trades live smoke test succeeded")
    return 0


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
    catalog_dir: Path,
    summary: dict[str, dict[str, int | None]],
) -> None:
    total_files = sum(int(values["files"]) for values in summary.values())
    print(f"parquet_files={total_files}")
    print(f"catalog={catalog_dir}")
    for family in sorted(summary):
        values = summary[family]
        rows = values["sample_rows_first_5"]
        row_text = "unavailable" if rows is None else str(rows)
        print(f"{family}: files={values['files']} sample_rows_first_5={row_text}")


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
