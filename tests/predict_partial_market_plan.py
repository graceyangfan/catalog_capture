"""LGPL-3.0-or-later. Financial admission and provenance for one bounded partial probe."""

from decimal import ROUND_DOWN
from decimal import Decimal as D


def outcome_book(book, outcome):
    if outcome == 1:
        return book["bids"], book["asks"]
    return (
        [[str(1 - D(str(p))), q] for p, q in book["asks"]],
        [[str(1 - D(str(p))), q] for p, q in book["bids"]],
    )


def partial_plan(
    bids,
    asks,
    fee_bps,
    allow_micro=False,
    target_principal=D("1.4"),
    max_principal=D("1.5"),
):
    target_principal = D(str(target_principal))
    max_principal = D(str(max_principal))
    if (
        not bids
        or not asks
        or not 0 <= fee_bps <= 200
        or not D(1) <= target_principal <= max_principal <= D(2)
    ):
        return None
    bids = sorted(((D(str(p)), D(str(q))) for p, q in bids), reverse=True)
    asks = sorted((D(str(p)), D(str(q))) for p, q in asks)
    bid, bid_qty = bids[0]
    best = asks[0][0]
    fee = D(fee_bps) / 10000
    for ask in sorted({p for p, _ in asks if 0 < p <= best * D("1.03")}):
        visible = sum(q for p, q in asks if p <= ask)
        cost = sum(p * q for p, q in asks if p <= ask)
        qty = (target_principal / ask).quantize(D(".01"), rounding=ROUND_DOWN)
        net_visible = visible * (1 - fee)
        exit_floor = bid * net_visible * D(".97")
        micro = allow_micro and D(".02") <= cost <= ask * visible <= D(".10")
        fill_ok = (
            D("1.10") <= cost <= ask * visible <= target_principal * D(".95")
            and exit_floor >= D("1.03")
            and cost - exit_floor * (1 - fee) <= D(".15")
        ) or (micro and bid * qty * (1 - fee) * D(".97") >= D("1.05"))
        if not (
            0 < bid < best <= ask <= D(".95")
            and fill_ok
            and qty >= visible * D("1.05")
            and 1 <= qty * ask <= max_principal
            and bid_qty >= qty * 3
            and len(str(qty.normalize()).replace(".", "").lstrip("0")) <= 5
        ):
            continue
        return {
            k: str(v)
            for k, v in {
                "observedBestAsk": best,
                "referencePrice": ask,
                "quantity": qty,
                "signedPrincipal": ask * qty,
                "visibleShares": visible,
                "visiblePrincipal": cost,
                "bestBid": bid,
                "visibleNetSharesEstimate": net_visible,
                "exitGrossFloorEstimate": exit_floor,
                "microRemainderAllowed": micro,
            }.items()
        }
    return None


def remainder_provenance(events, first_cancel_ns, rest_rows=()):
    """Receipt time establishes pre-request evidence only; a later terminal is ambiguous."""
    terminals = [
        e
        for e in events
        if e.get("type") in {"orderCancelled", "orderExpired", "orderNotAccepted"}
    ]
    terminals.extend(
        {"observedMonotonicNs": row["completedMonotonicNs"]}
        for row in rest_rows
        if row.get("response", {}).get("data", {}).get("status")
        in {"CANCELLED", "EXPIRED", "REJECTED"}
        and row.get("completedMonotonicNs") is not None
    )
    before = [
        e
        for e in terminals
        if first_cancel_ns is None or e["observedMonotonicNs"] < first_cancel_ns
    ]
    if before:
        return "venue_terminal_observed_before_our_cancel_request"
    if terminals:
        return "terminal_after_our_cancel_request_causality_not_proven"
    return "no_wallet_terminal_observed"


def owned_exit_is_safe(count, prior_orders, inventory_wei, sale_wei):
    """Require remote terminal barriers and confirmed inventory before any owned SELL."""
    return (
        1 <= count <= 3
        and len(prior_orders) == count
        and all(
            row.get("status") in {"FILLED", "CANCELLED", "EXPIRED", "REJECTED"}
            for row in prior_orders
        )
        and 0 < sale_wei <= inventory_wei
    )


def owned_inventory_wei(events, hashes):
    """Only unique successful settlements of this run's signed hashes own inventory."""
    seen = set()
    balance = 0
    for e in events:
        h = e.get("orderHash", "").lower()
        identity = (h, e.get("settlementId"))
        if (
            e.get("type") != "orderTransactionSuccess"
            or h not in hashes
            or not identity[1]
            or identity in seen
        ):
            continue
        seen.add(identity)
        size = int(e["fill"]["executedSizeWei"])
        fee = e.get("fee") or {}
        if e["details"]["quoteType"] == "BID":
            balance += size - (
                int(fee["amountWei"]) if fee.get("type") == "SHARES" else 0
            )
        elif e["details"]["quoteType"] == "ASK":
            balance -= size
        else:
            raise ValueError("unknown settlement side")
    return max(0, balance)


def owned_cash_at_risk_wei(events, hashes):
    seen = set()
    spent = 0
    for e in events:
        h = e.get("orderHash", "").lower()
        identity = (h, e.get("settlementId"))
        if (
            e.get("type") != "orderTransactionSuccess"
            or h not in hashes
            or not identity[1]
            or identity in seen
        ):
            continue
        seen.add(identity)
        value = int(e["fill"]["executedValueWei"])
        fee = e.get("fee") or {}
        cash_fee = int(fee["amountWei"]) if fee.get("type") == "COLLATERAL" else 0
        spent += (
            value + cash_fee
            if e["details"]["quoteType"] == "BID"
            else -value + cash_fee
        )
    return max(0, spent)
