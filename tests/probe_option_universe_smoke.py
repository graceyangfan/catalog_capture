#!/usr/bin/env python3
"""Run live option-universe smoke captures and summarize parquet output."""

from __future__ import annotations

import argparse
import json
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
DVOL_PROBE = PROJECT_ROOT / "tests" / "python_catalog_deribit_dvol_probe.py"

VENUE_CONFIGS = {
    "deribit": PROJECT_ROOT / "examples" / "capture.deribit-btc-universe.toml",
    "deribit-autorefresh": (
        PROJECT_ROOT / "examples" / "capture.deribit-btc-universe-autorefresh.toml"
    ),
    "okx": PROJECT_ROOT / "examples" / "capture.okx-btc-universe.toml",
    "okx-autorefresh": (
        PROJECT_ROOT / "examples" / "capture.okx-btc-universe-autorefresh.toml"
    ),
    "bybit": PROJECT_ROOT / "examples" / "capture.bybit-btc-universe.toml",
    "bybit-autorefresh": (
        PROJECT_ROOT / "examples" / "capture.bybit-btc-universe-autorefresh.toml"
    ),
    "deribit-research": (
        PROJECT_ROOT / "examples" / "capture.deribit-btc-universe-research.toml"
    ),
    "deribit-oi-ranked": (
        PROJECT_ROOT / "examples" / "capture.deribit-btc-universe-oi-ranked.toml"
    ),
    "deribit-oi-ranked-autorefresh": (
        PROJECT_ROOT
        / "examples"
        / "capture.deribit-btc-universe-oi-ranked-autorefresh.toml"
    ),
    "bybit-oi-ranked": (
        PROJECT_ROOT / "examples" / "capture.bybit-btc-universe-oi-ranked.toml"
    ),
    "okx-oi-ranked": (
        PROJECT_ROOT / "examples" / "capture.okx-btc-universe-oi-ranked.toml"
    ),
    "deribit-all": (
        PROJECT_ROOT / "examples" / "capture.deribit-btc-universe-all.toml"
    ),
}

STANDARD_VENUES = ("deribit", "okx", "bybit")
AUTOREFRESH_VENUES = ("deribit-autorefresh", "okx-autorefresh", "bybit-autorefresh")
AUTOREFRESH_VALIDATION_VENUES = frozenset(
    {
        "deribit-autorefresh",
        "okx-autorefresh",
        "bybit-autorefresh",
        "deribit-oi-ranked-autorefresh",
    }
)

REQUIRED_FAMILIES = (
    "instruments",
    "quotes",
    "option_greeks",
    "mark_prices",
    "index_prices",
    "funding_rate_update",
)

TRADE_FAMILY_NAMES = ("trade_tick", "trades")
VENUES_REQUIRING_TRADES = frozenset({"okx", "bybit", "okx-oi-ranked", "bybit-oi-ranked"})

FORWARD_PRICES_METADATA = Path("metadata") / "forward_prices.jsonl"
RESOLUTIONS_METADATA = Path("metadata") / "option_universe_resolutions.jsonl"
FORWARD_PRICE_SOURCE = "option_greeks_underlying_price"

OI_RANKED_VENUES = frozenset({
    "deribit-oi-ranked",
    "deribit-oi-ranked-autorefresh",
    "bybit-oi-ranked",
    "okx-oi-ranked",
})
OI_RANKED_AUTOREFRESH_VENUES = frozenset({"deribit-oi-ranked-autorefresh"})
ALL_STRIKES_VENUES = frozenset({"deribit-all"})
READBACK_OPTION_SAMPLE_LIMIT = 6
BAR_TYPES = {
    "deribit-research": ["BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL"],
    "bybit": ["BTCUSDT-LINEAR.BYBIT-1-MINUTE-LAST-EXTERNAL"],
    "okx": ["BTC-USD-SWAP.OKX-1-MINUTE-LAST-EXTERNAL"],
}
RESOLUTION_REQUIRED_FIELDS = (
    "event_kind",
    "venue_id",
    "underlying",
    "resolved_at_ns",
    "resolved_at_iso8601",
    "selected_expiry_ns",
    "selected_expiry_iso8601",
    "atm_reference",
    "atm_reference_source",
    "strike_selection_mode",
    "selected_strikes",
    "option_instrument_ids",
    "all_instrument_ids",
    "added_instrument_ids",
    "removed_instrument_ids",
)
REFRESH_ALLOWED_REASONS = {
    "expiry_roll",
    "atm_drift",
    "oi_rank_shift",
    "strike_window_shift",
}

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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one or more live option-universe capture smoke tests.",
    )
    parser.add_argument(
        "--venue",
        choices=(
            *VENUE_CONFIGS.keys(),
            "all",
            "all-autorefresh",
            "all-plus-research",
            "all-plus-oi-ranked",
            "all-oi-ranked",
        ),
        default="all",
        help=(
            "Venue smoke test to run. 'all' runs deribit/okx/bybit; "
            "'all-autorefresh' runs deribit/okx/bybit autorefresh profiles; "
            "'all-plus-research' also runs the Deribit research profile; "
            "'all-plus-oi-ranked' also runs Deribit OI-ranked; "
            "'all-oi-ranked' runs Deribit/Bybit/OKX OI-ranked profiles."
        ),
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
    parser.add_argument(
        "--require-contract-state",
        action="store_true",
        help="Require instrument_status and instrument_closes rows during readback probing.",
    )
    parser.add_argument(
        "--require-refresh-change",
        action="store_true",
        help=(
            "Require at least one runtime refresh delta in "
            "metadata/option_universe_resolutions.jsonl for autorefresh profiles."
        ),
    )
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")

    if args.venue == "all":
        venues = list(STANDARD_VENUES)
    elif args.venue == "all-autorefresh":
        venues = list(AUTOREFRESH_VENUES)
    elif args.venue == "all-plus-research":
        venues = [*STANDARD_VENUES, "deribit-research"]
    elif args.venue == "all-plus-oi-ranked":
        venues = [*STANDARD_VENUES, "deribit-oi-ranked"]
    elif args.venue == "all-oi-ranked":
        venues = ["deribit-oi-ranked", "bybit-oi-ranked", "okx-oi-ranked"]
    else:
        venues = [args.venue]
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
    run_cli_catalog_validation(catalog_dir, venue, args)
    resolution_rows, refresh_rows = validate_resolution_metadata(
        catalog_dir,
        venue,
        require_refresh_change=(
            args.require_refresh_change and venue in AUTOREFRESH_VALIDATION_VENUES
        ),
    )
    print(
        f"[{venue}] option_universe_resolutions.jsonl rows={resolution_rows} "
        f"refresh_rows={refresh_rows}",
        flush=True,
    )

    if venue in OI_RANKED_VENUES:
        validate_oi_ranked_resolution_metadata(catalog_dir, top_n=3)
        print(
            f"[{venue}] strike_selection_mode=oi_ranked",
        )
    if venue in ALL_STRIKES_VENUES:
        validate_all_strikes_resolution_metadata(catalog_dir)
        print(
            f"[{venue}] strike_selection_mode=all",
        )
    if venue in AUTOREFRESH_VALIDATION_VENUES:
        refresh_change_logs = count_refresh_change_logs(output)
        print(
            f"[{venue}] refresh_change_log_lines={refresh_change_logs}",
            flush=True,
        )

    perp_id, option_ids = parse_resolution_output(output)
    readback_option_ids = option_ids
    if venue in ALL_STRIKES_VENUES and len(option_ids) > READBACK_OPTION_SAMPLE_LIMIT:
        readback_option_ids = option_ids[:READBACK_OPTION_SAMPLE_LIMIT]
        print(
            f"[{venue}] readback sampling {len(readback_option_ids)} of "
            f"{len(option_ids)} resolved options",
            flush=True,
        )
    min_trade_rows = 1 if venue in VENUES_REQUIRING_TRADES else 0
    if not args.skip_readback_probe:
        run_readback_probe(
            catalog_dir,
            perp_id,
            readback_option_ids,
            min_trade_rows,
            args.require_contract_state,
            BAR_TYPES.get(venue, []),
        )
        if args.metrics_probe:
            run_metrics_probe(catalog_dir, perp_id, option_ids)
    elif args.metrics_probe:
        run_metrics_probe(catalog_dir, perp_id, option_ids)

    if venue == "deribit-research":
        run_dvol_probe(catalog_dir)

    if args.cleanup:
        shutil.rmtree(catalog_dir)
        temp_config.unlink(missing_ok=True)
        print(f"[{venue}] cleaned up generated catalog and config")


def run_cli_catalog_validation(
    catalog_dir: Path,
    venue: str,
    args: argparse.Namespace,
) -> None:
    preset = VALIDATION_PRESET_BY_VENUE.get(venue, "post-capture")
    command = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "validate-option-universe-catalog",
        "--catalog-uri",
        f"file://{catalog_dir}",
        "--option-universe-format",
        "text",
        "--preset",
        preset,
    ]
    if args.require_contract_state:
        command.append("--require-contract-state")
    if args.require_refresh_change and venue in AUTOREFRESH_VALIDATION_VENUES:
        command.append("--require-refresh-change")
    for bar_type in BAR_TYPES.get(venue, []):
        command.extend(["--bar-type", bar_type])
    print(f"[{venue}] cli catalog validation preset={preset}", flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


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


def run_readback_probe(
    catalog_dir: Path,
    perp_id: str,
    option_ids: list[str],
    min_trade_rows: int,
    require_contract_state: bool,
    bar_types: list[str],
) -> None:
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
        "--min-trade-rows",
        str(min_trade_rows),
    ]
    if require_contract_state:
        command.append("--require-contract-state")
    for bar_type in bar_types:
        command.extend(["--bar-type", bar_type])
    for option_id in option_ids:
        command.extend(["--option-id", option_id])
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def run_dvol_probe(catalog_dir: Path) -> None:
    print("[dvol] probing DeribitVolatilityIndex custom data", flush=True)
    command = [sys.executable, str(DVOL_PROBE), str(catalog_dir), "1"]
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


def load_resolution_records(catalog_dir: Path) -> list[dict]:
    path = catalog_dir / RESOLUTIONS_METADATA
    if not path.exists():
        raise RuntimeError(f"missing option universe resolution metadata: {path}")

    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not records:
        raise RuntimeError(f"option universe resolution metadata is empty: {path}")
    return records


def validate_resolution_metadata(
    catalog_dir: Path,
    venue: str,
    *,
    require_refresh_change: bool,
) -> tuple[int, int]:
    records = load_resolution_records(catalog_dir)
    startup_records = [record for record in records if record.get("event_kind") == "startup"]
    refresh_records = [record for record in records if record.get("event_kind") == "refresh"]

    if len(startup_records) != 1:
        raise RuntimeError(
            "expected exactly one startup resolution record, got "
            f"{len(startup_records)}"
        )

    startup = startup_records[0]
    for field in RESOLUTION_REQUIRED_FIELDS:
        if field not in startup:
            raise RuntimeError(
                f"resolution metadata missing field {field!r} in startup record"
            )

    if not startup["option_instrument_ids"]:
        raise RuntimeError("startup resolution selected no option instruments")
    if startup.get("perp_instrument_id") in (None, ""):
        raise RuntimeError("startup resolution missing perp_instrument_id")

    expected_mode = startup.get("strike_selection_mode")
    if not expected_mode:
        raise RuntimeError("startup resolution missing strike_selection_mode")

    for record in refresh_records:
        for field in RESOLUTION_REQUIRED_FIELDS:
            if field not in record:
                raise RuntimeError(
                    f"resolution metadata missing field {field!r} in refresh record"
                )
        if record.get("strike_selection_mode") != expected_mode:
            raise RuntimeError(
                "refresh metadata strike_selection_mode mismatch: "
                f"expected {expected_mode!r}, got {record.get('strike_selection_mode')!r}"
            )
        reason = record.get("rollover_reason")
        if reason not in REFRESH_ALLOWED_REASONS:
            raise RuntimeError(
                "refresh metadata rollover_reason unexpected: "
                f"got {reason!r}, expected one of {sorted(REFRESH_ALLOWED_REASONS)}"
            )
        if not record.get("added_instrument_ids") and not record.get(
            "removed_instrument_ids"
        ):
            raise RuntimeError(
                "refresh metadata should include added or removed instruments"
            )

    if require_refresh_change and not refresh_records:
        raise RuntimeError(
            f"{venue} expected at least one refresh delta but none were recorded"
        )

    return len(records), len(refresh_records)


def validate_oi_ranked_resolution_metadata(catalog_dir: Path, top_n: int) -> int:
    records = load_resolution_records(catalog_dir)
    startup_records = []
    for record in records:
        if record.get("event_kind") == "startup":
            startup_records.append(record)

    if not startup_records:
        raise RuntimeError(
            f"option universe resolution metadata missing startup event: {path}"
        )

    record = startup_records[0]
    for field in (
        "strike_selection_mode",
        "oi_ranked_top_n",
        "selected_strikes",
        "option_instrument_ids",
        "atm_reference_source",
    ):
        if field not in record:
            raise RuntimeError(
                f"resolution metadata missing field {field!r} in startup record"
            )

    if record["strike_selection_mode"] != "oi_ranked":
        raise RuntimeError(
            "resolution metadata strike_selection_mode mismatch: "
            f"expected 'oi_ranked', got {record['strike_selection_mode']!r}"
        )
    if record["oi_ranked_top_n"] != top_n:
        raise RuntimeError(
            "resolution metadata oi_ranked_top_n mismatch: "
            f"expected {top_n}, got {record['oi_ranked_top_n']!r}"
        )

    selected_strikes = record["selected_strikes"]
    option_ids = record["option_instrument_ids"]
    if not selected_strikes:
        raise RuntimeError("oi_ranked resolution selected no strikes")
    if len(selected_strikes) > top_n:
        raise RuntimeError(
            f"oi_ranked selected {len(selected_strikes)} strikes, expected at most {top_n}"
        )
    if len(option_ids) != len(selected_strikes) * 2:
        raise RuntimeError(
            "oi_ranked option_instrument_ids count should be 2x selected_strikes "
            f"(got {len(option_ids)} options for {len(selected_strikes)} strikes)"
        )

    print(
        f"[oi_ranked] strikes={selected_strikes} "
        f"atm_reference_source={record['atm_reference_source']}",
        flush=True,
    )
    return len(records)


def validate_all_strikes_resolution_metadata(catalog_dir: Path) -> int:
    records = load_resolution_records(catalog_dir)
    startup_records = []
    for record in records:
        if record.get("event_kind") == "startup":
            startup_records.append(record)

    if not startup_records:
        raise RuntimeError(
            f"option universe resolution metadata missing startup event: {path}"
        )

    record = startup_records[0]
    for field in (
        "strike_selection_mode",
        "selected_strikes",
        "option_instrument_ids",
        "atm_reference_source",
    ):
        if field not in record:
            raise RuntimeError(
                f"resolution metadata missing field {field!r} in startup record"
            )

    if record["strike_selection_mode"] != "all":
        raise RuntimeError(
            "resolution metadata strike_selection_mode mismatch: "
            f"expected 'all', got {record['strike_selection_mode']!r}"
        )
    if record.get("oi_ranked_top_n") is not None:
        raise RuntimeError("all-strikes resolution should not set oi_ranked_top_n")

    selected_strikes = record["selected_strikes"]
    option_ids = record["option_instrument_ids"]
    if len(selected_strikes) < 5:
        raise RuntimeError(
            "all-strikes resolution selected too few strikes for BTC nearest expiry: "
            f"{len(selected_strikes)}"
        )
    if len(option_ids) != len(selected_strikes) * 2:
        raise RuntimeError(
            "all-strikes option_instrument_ids count should be 2x selected_strikes "
            f"(got {len(option_ids)} options for {len(selected_strikes)} strikes)"
        )

    print(
        f"[all_strikes] strikes={len(selected_strikes)} "
        f"options={len(option_ids)} "
        f"atm_reference_source={record['atm_reference_source']}",
        flush=True,
    )
    return len(records)


def count_refresh_change_logs(output: str) -> int:
    return sum(
        1
        for line in output.splitlines()
        if "Option universe refresh venue_id=" in strip_ansi(line)
    )


def validate_forward_prices_metadata(catalog_dir: Path) -> int:
    path = catalog_dir / FORWARD_PRICES_METADATA
    if not path.exists():
        raise RuntimeError(f"missing forward price metadata: {path}")

    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"forward price metadata is empty: {path}")

    for line in lines:
        record = json.loads(line)
        for field in (
            "instrument_id",
            "forward_price",
            "ts_event_ns",
            "ts_init_ns",
            "source",
        ):
            if field not in record:
                raise RuntimeError(
                    f"forward price metadata missing field {field!r}: {line}"
                )
        if record["source"] != FORWARD_PRICE_SOURCE:
            raise RuntimeError(
                "forward price metadata source mismatch: "
                f"expected {FORWARD_PRICE_SOURCE!r}, got {record['source']!r}"
            )

    return len(lines)


def trade_family_stats(
    summary: dict[str, dict[str, int | None]],
) -> tuple[str | None, dict[str, int | None] | None]:
    for family in TRADE_FAMILY_NAMES:
        stats = summary.get(family)
        if stats and int(stats.get("files", 0)) > 0:
            return family, stats
    return None, None


def validate_summary(summary: dict[str, dict[str, int | None]], venue: str) -> None:
    missing = [
        family
        for family in REQUIRED_FAMILIES
        if summary.get(family, {}).get("files", 0) == 0
    ]
    if missing:
        raise RuntimeError(f"missing required parquet families: {', '.join(missing)}")

    trade_family, trade_stats = trade_family_stats(summary)
    if venue in VENUES_REQUIRING_TRADES:
        if trade_family is None:
            raise RuntimeError(
                f"missing required trade parquet family ({' or '.join(TRADE_FAMILY_NAMES)})"
            )
    elif trade_family is None:
        print(
            f"[{venue}] warning: no trade parquet yet "
            f"(Deribit trade WS delivery is still flaky in short smokes)",
            flush=True,
        )
    else:
        print(
            f"[{venue}] trades: family={trade_family} "
            f"files={trade_stats['files']}",
            flush=True,
        )

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

    if venue in VENUES_REQUIRING_TRADES and trade_stats is not None:
        if int(trade_stats["sample_rows_first_5"] or 0) == 0:
            raise RuntimeError("required trade parquet family had zero sample rows")


if __name__ == "__main__":
    raise SystemExit(main())
