"""Compute a small option-universe metrics snapshot from a Nautilus catalog."""

from __future__ import annotations

import argparse
import importlib.util
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Any

_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.core.nautilus_pyo3 import ParquetDataCatalog  # noqa: E402


@dataclass(frozen=True)
class OptionSnapshot:
    instrument_id: str
    expiry: str
    strike: float
    option_type: str
    mark_iv_raw: float
    mark_iv_decimal: float
    delta: float
    gamma: float | None
    vega: float | None
    theta: float | None
    quote_mid: float | None
    quote_spread: float | None
    greeks_rows: int
    quote_rows: int


@dataclass(frozen=True)
class UniverseMetrics:
    catalog_dir: str
    perp_id: str
    perp_quote_mid: float | None
    atm_strike: float
    atm_iv: float | None
    low_put_iv: float | None
    high_call_iv: float | None
    rough_risk_reversal: float | None
    rough_wing_richness: float | None
    option_count: int
    options: list[OptionSnapshot]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute a lightweight ATM/skew snapshot from option-universe parquet "
            "through Nautilus ParquetDataCatalog."
        ),
    )
    parser.add_argument("catalog_dir", type=Path)
    parser.add_argument("--perp-id", required=True)
    parser.add_argument(
        "--option-id",
        action="append",
        default=[],
        help="Option instrument id to include. Repeat for multiple options.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of text.",
    )
    return parser.parse_args()


def parse_option_id(instrument_id: str) -> tuple[str, float, str]:
    base = instrument_id.split(".", maxsplit=1)[0]
    tokens = base.split("-")
    for index, token in enumerate(tokens):
        if token in {"C", "P"} and index >= 2:
            return tokens[index - 2], float(tokens[index - 1]), token
    raise ValueError(f"cannot parse option id: {instrument_id}")


def numeric(value: Any) -> float | None:
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

    for attr_name in ("value", "raw"):
        attr = getattr(value, attr_name, None)
        if attr is not None:
            try:
                return float(attr)
            except (TypeError, ValueError):
                pass

    try:
        return float(str(value))
    except ValueError:
        return None


def latest_quote_mid(quotes: list) -> tuple[float | None, float | None]:
    for quote in reversed(quotes):
        bid = numeric(getattr(quote, "bid_price", None))
        ask = numeric(getattr(quote, "ask_price", None))
        if bid is None or ask is None:
            continue
        return (bid + ask) / 2.0, ask - bid
    return None, None


def normalize_mark_iv(value: float) -> float:
    """Return IV as a decimal while preserving raw exchange/vendor values elsewhere."""
    if abs(value) > 3.0:
        return value / 100.0
    return value


def latest_option_snapshot(
    catalog: ParquetDataCatalog,
    instrument_id: str,
) -> OptionSnapshot:
    greeks = catalog.query_option_greeks([instrument_id])
    if not greeks:
        raise RuntimeError(f"missing option greeks for {instrument_id}")
    quotes = catalog.query_quote_ticks([instrument_id])

    latest_greeks = greeks[-1]
    expiry, strike, option_type = parse_option_id(instrument_id)
    mark_iv = numeric(getattr(latest_greeks, "mark_iv", None))
    delta = numeric(getattr(latest_greeks, "delta", None))
    if mark_iv is None or delta is None:
        raise RuntimeError(f"missing mark_iv or delta for {instrument_id}")

    quote_mid, quote_spread = latest_quote_mid(quotes)
    return OptionSnapshot(
        instrument_id=instrument_id,
        expiry=expiry,
        strike=strike,
        option_type=option_type,
        mark_iv_raw=mark_iv,
        mark_iv_decimal=normalize_mark_iv(mark_iv),
        delta=delta,
        gamma=numeric(getattr(latest_greeks, "gamma", None)),
        vega=numeric(getattr(latest_greeks, "vega", None)),
        theta=numeric(getattr(latest_greeks, "theta", None)),
        quote_mid=quote_mid,
        quote_spread=quote_spread,
        greeks_rows=len(greeks),
        quote_rows=len(quotes),
    )


def choose_atm_strike(
    snapshots: list[OptionSnapshot],
    perp_quote_mid: float | None,
) -> float:
    strikes = sorted({snapshot.strike for snapshot in snapshots})
    if not strikes:
        raise RuntimeError("no option strikes supplied")
    if perp_quote_mid is None:
        return float(median(strikes))
    return min(strikes, key=lambda strike: (abs(strike - perp_quote_mid), strike))


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def compute_metrics(
    catalog_dir: Path,
    perp_id: str,
    option_ids: list[str],
) -> UniverseMetrics:
    if not option_ids:
        raise ValueError("at least one --option-id is required")

    catalog = ParquetDataCatalog(str(catalog_dir))
    perp_quotes = catalog.query_quote_ticks([perp_id])
    perp_quote_mid, _ = latest_quote_mid(perp_quotes)
    snapshots = [
        latest_option_snapshot(catalog, option_id)
        for option_id in sorted(option_ids)
    ]

    atm_strike = choose_atm_strike(snapshots, perp_quote_mid)
    atm_iv = average(
        [
            snapshot.mark_iv_decimal
            for snapshot in snapshots
            if snapshot.strike == atm_strike
        ]
    )

    low_strike = min(snapshot.strike for snapshot in snapshots)
    high_strike = max(snapshot.strike for snapshot in snapshots)
    low_put_iv = average(
        [
            snapshot.mark_iv_decimal
            for snapshot in snapshots
            if snapshot.strike == low_strike and snapshot.option_type == "P"
        ]
    )
    high_call_iv = average(
        [
            snapshot.mark_iv_decimal
            for snapshot in snapshots
            if snapshot.strike == high_strike and snapshot.option_type == "C"
        ]
    )

    rough_risk_reversal = None
    if low_put_iv is not None and high_call_iv is not None:
        rough_risk_reversal = high_call_iv - low_put_iv

    wing_ivs = [value for value in (low_put_iv, high_call_iv) if value is not None]
    rough_wing_richness = None
    if atm_iv is not None and wing_ivs:
        rough_wing_richness = average(wing_ivs) - atm_iv

    return UniverseMetrics(
        catalog_dir=str(catalog_dir),
        perp_id=perp_id,
        perp_quote_mid=perp_quote_mid,
        atm_strike=atm_strike,
        atm_iv=atm_iv,
        low_put_iv=low_put_iv,
        high_call_iv=high_call_iv,
        rough_risk_reversal=rough_risk_reversal,
        rough_wing_richness=rough_wing_richness,
        option_count=len(snapshots),
        options=snapshots,
    )


def format_optional(value: float | None, precision: int = 6) -> str:
    if value is None:
        return "-"
    return f"{value:.{precision}f}"


def print_text(metrics: UniverseMetrics) -> None:
    print("Option universe metrics snapshot")
    print(f"Catalog dir: {metrics.catalog_dir}")
    print(f"Perp: {metrics.perp_id}")
    print(f"Perp quote mid: {format_optional(metrics.perp_quote_mid, 2)}")
    print(f"ATM strike: {metrics.atm_strike:g}")
    print(f"ATM IV decimal: {format_optional(metrics.atm_iv)}")
    print(f"Low-put IV decimal: {format_optional(metrics.low_put_iv)}")
    print(f"High-call IV decimal: {format_optional(metrics.high_call_iv)}")
    print(f"Rough risk reversal decimal: {format_optional(metrics.rough_risk_reversal)}")
    print(f"Rough wing richness decimal: {format_optional(metrics.rough_wing_richness)}")
    print("")
    print(
        "instrument_id,strike,type,mark_iv_raw,mark_iv_decimal,delta,"
        "quote_mid,quote_spread,greeks,quotes"
    )
    for snapshot in metrics.options:
        print(
            ",".join(
                [
                    snapshot.instrument_id,
                    f"{snapshot.strike:g}",
                    snapshot.option_type,
                    format_optional(snapshot.mark_iv_raw),
                    format_optional(snapshot.mark_iv_decimal),
                    format_optional(snapshot.delta),
                    format_optional(snapshot.quote_mid, 8),
                    format_optional(snapshot.quote_spread, 8),
                    str(snapshot.greeks_rows),
                    str(snapshot.quote_rows),
                ]
            )
        )


def main() -> int:
    args = parse_args()
    metrics = compute_metrics(args.catalog_dir, args.perp_id, args.option_id)
    if args.json:
        print(json.dumps(asdict(metrics), indent=2, sort_keys=True))
    else:
        print_text(metrics)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
