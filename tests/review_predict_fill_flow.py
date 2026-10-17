"""Offline audit of retained fill economics and callback provenance. LGPL-3.0-or-later."""

import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path

BASE = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


def read(name):
    return json.loads((BASE / name).read_text())


def money(value):
    return Decimal(str(value).split()[0])


def review():
    names = [
        "live_fill_20261016T123240Z/native_roundtrip.json",
        "live_fill_20261016T123240Z_independent/calibration.json",
        "live_fill_20261016T123240Z/pnl_scope_review.json",
        "live_fill_20261016T122233Z/account_consistency_review.json",
        "actual_trade_flow_native_replay_20261016_v2.json",
    ]
    native, independent, pnl_review, account_review, replay = map(read, names)
    events = independent["wallet"]["events"]
    successes = [e for e in events if e["type"] == "orderTransactionSuccess"]
    assert len(successes) == 2
    buy = next(e for e in successes if e["details"]["quoteType"] == "BID")
    sell = next(e for e in successes if e["details"]["quoteType"] == "ASK")
    assert buy["fee"]["type"] == "SHARES" and sell["fee"]["type"] == "COLLATERAL"
    for event in successes:
        matches = [
            r
            for r in independent["after"]["matches"]
            if r["settlementId"] == event["settlementId"]
            and r["taker"]["hash"] == event["orderHash"]
        ]
        assert len(matches) == 1
        matched = matches[0]
        assert int(matched["amountFilled"]) == int(event["fill"]["executedSizeWei"])
        assert int(matched["priceExecuted"]) == int(event["fill"]["executedPriceWei"])
        assert int(matched["taker"]["fee"]["amount"]) == int(event["fee"]["amountWei"])
        assert matched["taker"]["fee"]["type"] == event["fee"]["type"]
        chain = next(
            r
            for r in independent["comparisons"]
            if r["settlementId"] == event["settlementId"]
            and r["orderHash"] == event["orderHash"]
        )
        assert int(chain["grossWei"]) == int(event["fill"]["executedSizeWei"])
        assert int(chain["feeWei"]) == int(event["fee"]["amountWei"])
        assert int(chain["collateralWei"]) == int(event["fill"]["executedValueWei"])
    gross = int(buy["fill"]["executedSizeWei"])
    shares_fee = int(buy["fee"]["amountWei"])
    net = gross - shares_fee
    assert net == int(sell["fill"]["executedSizeWei"])
    proceeds = int(sell["fill"]["executedValueWei"]) - int(sell["fee"]["amountWei"])
    delta = proceeds - int(buy["fill"]["executedValueWei"])
    assert delta == int(independent["after"]["cashWei"]) - int(
        independent["before"]["cashWei"]
    )
    assert all(
        not independent[phase]["positions"] and not independent[phase]["orders"]
        for phase in ["before", "after"]
    )
    entry_fee_equivalent = (
        Decimal(shares_fee) * Decimal(buy["fill"]["executedPriceWei"]) / Decimal(10**36)
    )
    feedback = native["nativeFeedback"]
    callbacks = Counter(
        r["callback"] for r in feedback if r["type"] == "StrategyCallback"
    )
    historical_hashes = {
        leg["hash"]
        for r in independent["before"]["matches"]
        for leg in [r["taker"], *r["makers"]]
    }
    historical_fills = [
        r
        for r in feedback
        if r["type"] == "Filled" and r["clientOrderId"] in historical_hashes
    ]
    assert len(historical_fills) == 2
    assert callbacks["on_order_filled"] == 5
    assert pnl_review["nativeCyclePnlMatchesCash"] is False
    assert account_review["nativeAccountMatchesChain"] is False
    final_cash = money(native["balances"][0]["total"])
    assert final_cash == (Decimal(independent["after"]["cashWei"]) / 10**18).quantize(
        Decimal("0.00000001")
    )
    corrected = replay["nativeFeedback"]
    corrected_callbacks = Counter(
        r["callback"] for r in corrected if r["type"] == "StrategyCallback"
    )
    assert (
        corrected_callbacks["on_order_filled"] == 3
    )  # Two venue fills plus Native share-fee adjustment.
    for name in ["on_position_opened", "on_position_changed", "on_position_closed"]:
        assert corrected_callbacks[name] == 1
    positions = [
        r
        for r in corrected
        if r.get("callback")
        in ["on_position_opened", "on_position_changed", "on_position_closed"]
    ]
    assert [Decimal(r["cachedNetQuantity"]) for r in positions] == [
        Decimal(32),
        Decimal("31.36"),
        Decimal(0),
    ]
    assert money(replay["cycleRealizedPnl"]) == Decimal(delta) / 10**18
    assert money(replay["finalCash"]) == final_cash
    assert replay["finalPositions"] == replay["finalOpenOrders"] == 0
    topic_cash = [
        money(b["total"])
        for r in feedback
        if r["type"] == "NativeAccountReported"
        for b in r["balances"]
    ]
    assert (
        final_cash not in topic_cash
    )  # Cache freshness must not be equated with topic delivery.
    return {
        "source": "derived offline review of retained actual mainnet JSON and separately labeled corrected synthetic replay; no new venue calls",
        "sources": [
            {
                "path": name,
                "sha256": hashlib.sha256((BASE / name).read_bytes()).hexdigest(),
                "grade": "derived corrected Rust LiveNode replay"
                if "replay" in name
                else "actual capture"
                if name.endswith(("native_roundtrip.json", "calibration.json"))
                else "derived review of actual capture",
            }
            for name in names
        ],
        "actualEconomics": {
            "grossShares": str(Decimal(gross) / 10**18),
            "sharesFee": str(Decimal(shares_fee) / 10**18),
            "netShares": str(Decimal(net) / 10**18),
            "shareFeeUSDTEquivalent": str(entry_fee_equivalent),
            "buyCashDebit": "1.28",
            "sellGrossCredit": "1.22304",
            "sellCashFee": "0.0244608",
            "cashDelta": str(Decimal(delta) / 10**18),
            "finalNativeCash": str(final_cash),
            "walletMatchesAndChainAgree": True,
        },
        "actualCallbackCounts": dict(callbacks),
        "actualHistoricalFillsAdopted": len(historical_fills),
        "actualPositionCallbackQuantities": [
            r["cachedNetQuantity"]
            for r in feedback
            if r.get("callback")
            in ["on_position_opened", "on_position_changed", "on_position_closed"]
        ],
        "actualFinalNativeAccountMatchesChain": True,
        "actualFinalCashObservedOnAccountTopic": False,
        "actualCyclePnLAccepted": False,
        "firstCycleAccountAccepted": False,
        "correctedReplay": {
            "grade": "derived offline economic replay, not a new real fill",
            "callbackCounts": dict(corrected_callbacks),
            "netPositionSequence": [r["cachedNetQuantity"] for r in positions],
            "filledCallbacksIncludeNativeSyntheticAdjustment": True,
            "cashDeltaMatchesNativePnL": True,
            "finalPositions": 0,
            "finalOpenOrders": 0,
        },
        "nativeContractNotes": [
            "Rust Strategy exposes order and position callbacks but no built-in on_account hook in reviewed revision",
            "reported AccountState goes through runner portfolio endpoint to Cache; account topic observation is not a complete report-delivery contract",
            "Native reconciliation can generate synthetic order/fill callbacks; distinguish venue fills by owned orders and Native reconciliation metadata",
            "OrderFilled gross quantity is not fee-deducted spendable inventory",
        ],
        "newApiCalls": 0,
        "newOrderWrites": 0,
    }


if __name__ == "__main__":
    result = review()
    with (BASE / "fill_callback_flow_review_20261017.json").open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")
    print(
        "Retained real settlement economics and corrected Native callback flow verified offline."
    )
