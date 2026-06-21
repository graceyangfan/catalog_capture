#!/usr/bin/env python3
"""Offline IV term-structure derive job (Step 9b P0 prototype)."""

from __future__ import annotations

import argparse
import importlib.util
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

_IMPORT = Path(__file__).resolve().parents[1] / "tests" / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


@dataclass(frozen=True)
class IvTermRow:
    expiry: str
    expiry_ns: int | None
    option_count: int
    call_count: int
    put_count: int
    atm_strike: float | None
    atm_iv_decimal: float | None
    median_iv_decimal: float | None


@dataclass(frozen=True)
class IvTermManifest:
    job: str
    version: str
    created_at_utc: str
    catalog_dir: str
    perp_id: str | None
    perp_quote_mid: float | None
    option_ids: list[str]
    output_json: str
    rows: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Derive a lightweight IV term-structure panel from option_greeks "
            "in a Nautilus ParquetDataCatalog."
        ),
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument(
        "--perp-id",
        default=None,
        help="Optional hedge perp instrument id for ATM strike selection.",
    )
    parser.add_argument(
        "--option-id",
        action="append",
        default=[],
        help="Option instrument id to include. Repeat for multiple options.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for derived panel + manifest (default: <catalog_dir>/derived/iv_term).",
    )
    return parser.parse_args()


def numeric(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    for method_name in ("as_double", "as_decimal"):
        method = getattr(value, method_name, None)
        if method is not None:
            try:
                return float(method())
            except (TypeError, ValueError):
                pass
    try:
        return float(str(value))
    except ValueError:
        return None


def normalize_mark_iv(value: float) -> float:
    if abs(value) > 3.0:
        return value / 100.0
    return value


def parse_option_id(instrument_id: str) -> tuple[str, float, str]:
    base = instrument_id.split(".", maxsplit=1)[0]
    tokens = base.split("-")
    for index, token in enumerate(tokens):
        if token in {"C", "P"} and index >= 2:
            return tokens[index - 2], float(tokens[index - 1]), token
    raise ValueError(f"cannot parse option id: {instrument_id}")


def latest_quote_mid(catalog: ParquetDataCatalog, perp_id: str) -> float | None:
    quotes = catalog.query_quote_ticks([perp_id])
    for quote in reversed(quotes):
        bid = numeric(getattr(quote, "bid_price", None))
        ask = numeric(getattr(quote, "ask_price", None))
        if bid is None or ask is None:
            continue
        return (bid + ask) / 2.0
    return None


def discover_option_ids(catalog_dir: Path) -> list[str]:
    catalog_root = catalog_dir
    candidates: set[str] = set()
    for family in ("option_greeks", "option-greeks"):
        family_dir = catalog_root / "data" / family
        if not family_dir.exists():
            continue
        for child in family_dir.iterdir():
            if child.is_dir():
                candidates.add(f"{child.name}")
    return sorted(candidates)


def choose_atm_strike(strikes: list[float], perp_quote_mid: float | None) -> float | None:
    if not strikes:
        return None
    if perp_quote_mid is None:
        return float(median(strikes))
    return min(strikes, key=lambda strike: (abs(strike - perp_quote_mid), strike))


def build_iv_term_rows(
    catalog: ParquetDataCatalog,
    option_ids: list[str],
    perp_quote_mid: float | None,
) -> list[IvTermRow]:
    by_expiry: dict[str, dict] = {}

    for instrument_id in option_ids:
        greeks = catalog.query_option_greeks([instrument_id])
        if not greeks:
            continue
        latest = greeks[-1]
        expiry, strike, option_type = parse_option_id(instrument_id)
        mark_iv = numeric(getattr(latest, "mark_iv", None))
        if mark_iv is None:
            continue
        bucket = by_expiry.setdefault(
            expiry,
            {
                "strikes": [],
                "ivs": [],
                "calls": 0,
                "puts": 0,
                "expiry_ns": getattr(latest, "ts_event", None),
            },
        )
        bucket["strikes"].append(strike)
        bucket["ivs"].append(normalize_mark_iv(mark_iv))
        if option_type == "C":
            bucket["calls"] += 1
        else:
            bucket["puts"] += 1

    rows: list[IvTermRow] = []
    for expiry in sorted(by_expiry):
        bucket = by_expiry[expiry]
        atm_strike = choose_atm_strike(bucket["strikes"], perp_quote_mid)
        atm_iv = None
        if atm_strike is not None:
            atm_ivs = [
                iv
                for strike, iv in zip(bucket["strikes"], bucket["ivs"], strict=True)
                if strike == atm_strike
            ]
            if atm_ivs:
                atm_iv = sum(atm_ivs) / len(atm_ivs)
        rows.append(
            IvTermRow(
                expiry=expiry,
                expiry_ns=bucket["expiry_ns"],
                option_count=len(bucket["strikes"]),
                call_count=bucket["calls"],
                put_count=bucket["puts"],
                atm_strike=atm_strike,
                atm_iv_decimal=atm_iv,
                median_iv_decimal=float(median(bucket["ivs"])) if bucket["ivs"] else None,
            )
        )
    return rows


def write_outputs(
    output_dir: Path,
    rows: list[IvTermRow],
    manifest: IvTermManifest,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    panel_path = output_dir / "iv_term_panel.json"
    manifest_path = output_dir / "manifest.json"
    panel_path.write_text(
        json.dumps([asdict(row) for row in rows], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(asdict(manifest), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    catalog = ParquetDataCatalog(str(args.catalog_dir))
    option_ids = args.option_id or discover_option_ids(args.catalog_dir)
    if not option_ids:
        raise SystemExit(f"no option ids found under {args.catalog_dir}")

    perp_quote_mid = None
    if args.perp_id:
        perp_quote_mid = latest_quote_mid(catalog, args.perp_id)

    rows = build_iv_term_rows(catalog, option_ids, perp_quote_mid)
    if not rows:
        raise SystemExit("no IV term rows could be derived from supplied option ids")

    output_dir = args.output_dir or (args.catalog_dir / "derived" / "iv_term")
    manifest = IvTermManifest(
        job="derive_iv_term",
        version="0.1.0",
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        catalog_dir=str(args.catalog_dir),
        perp_id=args.perp_id,
        perp_quote_mid=perp_quote_mid,
        option_ids=option_ids,
        output_json=str(output_dir / "iv_term_panel.json"),
        rows=len(rows),
    )
    write_outputs(output_dir, rows, manifest)

    print("IV term derive job succeeded")
    print(f"catalog_dir={args.catalog_dir}")
    print(f"output_dir={output_dir}")
    print(f"rows={len(rows)} options={len(option_ids)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())