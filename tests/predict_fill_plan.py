"""Test-only bounded round-trip admission. No order writes. LGPL-3.0-or-later."""

from decimal import ROUND_DOWN, Decimal

D = Decimal
MAX_COST = D("1.50")
MAX_LOSS = D("0.15")


def plan_round_trip(
    bids, asks, fee_bps, *, max_cost=MAX_COST, max_loss=MAX_LOSS, slippage_bps=100
):
    """Require depth and exit floor after BUY shares fee, price stress and SELL fee."""
    bids = [(D(str(p)), D(str(q))) for p, q in bids]
    asks = [(D(str(p)), D(str(q))) for p, q in asks]
    if not bids or not asks or not 0 <= fee_bps <= 10_000:
        return None
    if any(not 0 < p < 1 or q <= 0 for p, q in bids + asks):
        return None
    if bids != sorted(bids, reverse=True) or asks != sorted(asks):
        return None
    bid, ask = bids[0][0], asks[0][0]
    if not bid < ask <= D("0.10"):
        return None
    rate = D(fee_bps) / 10_000
    slip = D(slippage_bps) / 10_000
    # Small deterministic budget ladder; whole-share entry also keeps the planned fee-adjusted exit within SDK precision.
    for budget in (D("1.20"), D("1.30"), D("1.40"), D("1.50")):
        if budget > max_cost:
            continue
        qty = (budget / ask).quantize(D("1"), rounding=ROUND_DOWN)
        cost = qty * ask
        net = (qty * (1 - rate * min(ask, 1 - ask) / ask)).quantize(
            D(".00001"), rounding=ROUND_DOWN
        )
        # Entry must fit one displayed level with substantial size headroom.
        if qty <= 0 or asks[0][1] < qty * 5 or bids[0][1] < net * 5:
            continue
        sell_fee = rate * min(bid, 1 - bid) * net
        immediate_net = bid * net - sell_fee
        if cost - immediate_net > max_loss * D(2) / D(3):
            continue
        stress_bid = bid - max_loss / net
        signed_floor = net * stress_bid * (1 - slip)
        stress_net = signed_floor - rate * min(stress_bid, 1 - stress_bid) * net
        if stress_bid <= 0 or signed_floor < D("1.05") or stress_net < D("1.00"):
            continue
        return {
            k: str(v)
            for k, v in {
                "entryPrice": ask,
                "grossQuantity": qty,
                "entryCost": cost,
                "conservativeNetQuantity": net,
                "bid": bid,
                "ask": ask,
                "immediateNetProceeds": immediate_net,
                "estimatedRoundTripLoss": cost - immediate_net,
                "stressBid": stress_bid,
                "signedExitFloor": signed_floor,
                "netExitFloor": stress_net,
                "maxLoss": max_loss,
            }.items()
        }
    return None
