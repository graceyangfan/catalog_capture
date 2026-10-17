"""Offline accounting identities; not a venue capture or Native execution harness."""

import argparse
import hashlib
import json
import unittest
from decimal import Decimal, localcontext
from pathlib import Path

D = Decimal
ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "crates/nautilus-predict/tests/fixtures/private_api"
NATIVE_REPLAY = (
    FIXTURES / "different_price_partial_share_fees_native_synthetic_20261017_v2.json"
)


def accounting(fills, sell_price="0.01", sell_fee="0"):
    """Compare two consistent conventions with gross-then-reduce accounting.

    Inputs are (gross shares, price, actual shares charged). The latter two
    projections model the existing quantity-only correction, not arbitrary
    Native position adjustments. Decimal deliberately isolates model error
    from Native f64 and Money rounding. No formula estimates a live fee.
    """
    with localcontext() as ctx:
        ctx.prec = 70
        fills = [tuple(map(D, fill)) for fill in fills]
        if not fills or any(q <= 0 or p <= 0 or not 0 <= f < q for q, p, f in fills):
            raise ValueError("Expected positive BUY fills and fee smaller than gross")
        price, exit_fee = D(sell_price), D(sell_fee)
        gross = sum(q for q, _, _ in fills)
        cash = sum(q * p for q, p, _ in fills)
        net = sum(q - f for q, _, f in fills)
        expense = sum(f * p for _, p, f in fills)
        basis = sum((q - f) * p for q, p, f in fills)
        proceeds = net * price - exit_fee
        expected = proceeds - cash
        result = {
            "gross": str(gross),
            "net": str(net),
            "cashDebit": str(cash),
            "feeQuoteEquivalent": str(expense),
            "orderVWAP": str(cash / gross),
            "sellPrice": str(price),
            "sellCollateralFee": str(exit_fee),
            "expectedFlatCashPnL": str(expected),
            "conventions": {},
        }
        projections = [
            ("expenseFeeAtFillPrice", basis / net, expense),
            ("capitalizeFeeInInventory", cash / net, D(0)),
            ("quantityCorrectionAfterAll", cash / gross, expense),
        ]
        qty, avg = D(0), D(0)
        for q, p, fee in fills:
            avg = (qty * avg + q * p) / (qty + q)
            qty += q - fee  # A quantity-only reduction retains average price.
        projections.append(("quantityCorrectionAfterEach", avg, expense))
        for name, average, fee_expense in projections:
            cost = net * average + fee_expense
            pnl = proceeds - cost
            result["conventions"][name] = {
                "positionAverage": str(average),
                "inventoryBasis": str(net * average),
                "feeExpense": str(fee_expense),
                "basisPlusFeeMinusCash": str(cost - cash),
                "flatPnL": str(pnl),
                "pnlMinusCashPnL": str(pnl - expected),
                "matchesCash": abs(pnl - expected) < D("1e-55"),
            }
        return result


def cases():
    # The variable-rate example uses an explicitly assumed floor to 18 decimals;
    # this is an algebra fixture, not a claim about a captured settlement.
    with localcontext() as ctx:
        ctx.prec = 70
        fee_at_60 = (D(20) * D(".02") * D(".4") / D(".6") * 10**18 // 1) / 10**18
    return {
        "zeroFee": accounting([("17", ".009", "0"), ("15", ".010", "0")]),
        "oneFill": accounting([("32", ".04", ".64")], ".039", ".0244608"),
        "constantShareFraction": accounting(
            [("17", ".009", ".34"), ("15", ".010", ".30")]
        ),
        "mixedTakerMaker": accounting([("17", ".009", ".34"), ("15", ".010", "0")]),
        "mixedMakerTakerReverse": accounting(
            [("15", ".010", "0"), ("17", ".009", ".34")]
        ),
        "differentFeeFractions": accounting(
            [("12", ".4", ".24"), ("20", ".6", str(fee_at_60))], ".7"
        ),
    }


def multi_position_review(flatten=False, duplicate=False):
    """Test-only expense-at-fill-price reference, NOT the live Native ledger.

    Fees below are explicit synthetic receipts compatible with an undiscounted
    200 bps curve. A/B are different markets, so their outcome costs never mix.
    """
    rows = [
        ("A1", "A:YES", "BUY", "100", ".20", "MAKER", "COLLATERAL", "0"),
        ("B1", "B:NO", "BUY", "80", ".40", "TAKER", "SHARES", "1.6"),
        ("A2", "A:YES", "BUY", "40", ".25", "TAKER", "SHARES", ".8"),
        ("A3", "A:YES", "SELL", "50", ".30", "MAKER", "COLLATERAL", "0"),
        ("B2", "B:NO", "BUY", "20", ".50", "MAKER", "COLLATERAL", "0"),
        ("A4", "A:YES", "BUY", "20", ".15", "TAKER", "SHARES", ".4"),
        ("B3", "B:NO", "SELL", "30", ".45", "TAKER", "COLLATERAL", ".27"),
        ("A5", "A:YES", "SELL", "108.8", ".22", "TAKER", "COLLATERAL", ".47872"),
        ("B4", "B:NO", "BUY", "10", ".30", "MAKER", "COLLATERAL", "0"),
    ]
    if flatten:
        rows.append(("B5", "B:NO", "SELL", "78.4", ".42", "MAKER", "COLLATERAL", "0"))
    if duplicate:
        rows = [row for row in rows for _ in range(2)]
    with localcontext() as ctx:
        ctx.prec = 70
        cash = D("100")
        positions, latest, seen, steps = {}, {}, set(), []
        for trade_id, key, side, amount, price, liquidity, asset, raw_fee in rows:
            if trade_id in seen:
                continue
            seen.add(trade_id)
            q, p, fee = D(amount), D(price), D(raw_fee)
            if liquidity == "MAKER" and fee:
                raise ValueError("Maker fee must be zero in these Predict scenarios")
            state = positions.setdefault(
                key, {"quantity": D(0), "basis": D(0), "realized": D(0), "fees": D(0)}
            )
            fee_value = fee * p if asset == "SHARES" else fee
            if side == "BUY":
                net = q - fee if asset == "SHARES" else q
                cash_change = -q * p - (fee if asset == "COLLATERAL" else 0)
                state["basis"] += net * p
                state["quantity"] += net
                state["realized"] -= fee_value
            else:
                if (
                    asset != "COLLATERAL"
                    or q > state["quantity"]
                    or state["quantity"] <= 0
                ):
                    raise ValueError(
                        "Reference SELL requires collateral fee and available inventory"
                    )
                average = state["basis"] / state["quantity"]
                cash_change = q * p - fee
                state["basis"] -= q * average
                state["quantity"] -= q
                state["realized"] += q * (p - average) - fee
            state["fees"] += fee_value
            cash += cash_change
            latest[key] = p
            equity = cash + sum(s["quantity"] * latest[k] for k, s in positions.items())
            total_pnl = sum(
                s["realized"] + s["quantity"] * latest[k] - s["basis"]
                for k, s in positions.items()
            )
            steps.append(
                {
                    "tradeId": trade_id,
                    "instrument": key,
                    "side": side,
                    "gross": amount,
                    "price": price,
                    "liquidity": liquidity,
                    "feeType": asset,
                    "feeAmount": raw_fee,
                    "cashChange": str(cash_change),
                    "accountCash": str(cash),
                    "quantity": str(state["quantity"]),
                    "average": str(state["basis"] / state["quantity"])
                    if state["quantity"]
                    else None,
                    "realizedPnL": str(state["realized"]),
                    "cashEquityMinusPositionPnL": str(equity - D(100) - total_pnl),
                }
            )
        marks = {"A:YES": D(".22"), "B:NO": D(".42")}
        summary = {}
        for key, s in positions.items():
            unrealized = s["quantity"] * marks[key] - s["basis"]
            summary[key] = {
                "quantity": str(s["quantity"]),
                "inventoryBasis": str(s["basis"]),
                "average": str(s["basis"] / s["quantity"]) if s["quantity"] else None,
                "feeQuoteEquivalent": str(s["fees"]),
                "realizedPnL": str(s["realized"]),
                "valuationPrice": str(marks[key]),
                "unrealizedPnL": str(unrealized),
                "totalPnL": str(s["realized"] + unrealized),
            }
        market_value = sum(s["quantity"] * marks[k] for k, s in positions.items())
        realized = sum(s["realized"] for s in positions.values())
        unrealized = sum(
            s["quantity"] * marks[k] - s["basis"] for k, s in positions.items()
        )
        locked = D("0") if flatten else D("2")
        return {
            "source": "synthetic test-only reference accounting, not Native results or venue receipts",
            "steps": steps,
            "positions": summary,
            "account": {
                "initialCash": "100",
                "totalCash": str(cash),
                "locked": str(locked),
                "free": str(cash - locked),
            },
            "marketValue": str(market_value),
            "equity": str(cash + market_value),
            "realizedPnL": str(realized),
            "unrealizedPnL": str(unrealized),
            "totalPnL": str(realized + unrealized),
            "cashEquityChange": str(cash + market_value - D(100)),
        }


def build_review():
    replay_bytes = NATIVE_REPLAY.read_bytes()
    replay = json.loads(replay_bytes)
    interleaved = next(s for s in replay["sessions"] if s["interleavedFeeCorrection"])
    return {
        "evidenceGrade": "synthetic Decimal accounting identities; not a new Rust LiveNode run or venue data",
        "newExchangeRequests": 0,
        "newOrders": 0,
        "nativeModified": False,
        "recordingLogicModified": False,
        "assumptions": "Long-only BUY acquisition then flat SELL; no other cash or asset flows; fees valued at their own fill price",
        "existingNativeReplay": {
            "path": str(NATIVE_REPLAY.relative_to(ROOT)),
            "sha256": hashlib.sha256(replay_bytes).hexdigest(),
            "positionOnlyExitProbe": interleaved["positionOnlyExitProbe"],
        },
        "cases": cases(),
        "multiPositionOpen": multi_position_review(),
        "multiPositionFlat": multi_position_review(flatten=True),
    }


class ShareFeeCostTests(unittest.TestCase):
    def test_consistent_conventions_conserve_cash(self):
        for name, case in cases().items():
            for convention in ("expenseFeeAtFillPrice", "capitalizeFeeInInventory"):
                with self.subTest(case=name, convention=convention):
                    self.assertTrue(case["conventions"][convention]["matchesCash"])

    def test_correction_timing_is_not_a_general_solution(self):
        rows = cases()
        self.assertTrue(
            rows["constantShareFraction"]["conventions"]["quantityCorrectionAfterAll"][
                "matchesCash"
            ]
        )
        self.assertFalse(
            rows["constantShareFraction"]["conventions"]["quantityCorrectionAfterEach"][
                "matchesCash"
            ]
        )
        for name in ("mixedTakerMaker", "differentFeeFractions"):
            with self.subTest(case=name):
                self.assertFalse(
                    rows[name]["conventions"]["quantityCorrectionAfterAll"][
                        "matchesCash"
                    ]
                )

    def test_pure_acquisition_totals_do_not_depend_on_fill_order(self):
        rows = cases()
        a, b = rows["mixedTakerMaker"], rows["mixedMakerTakerReverse"]
        for key in (
            "net",
            "cashDebit",
            "feeQuoteEquivalent",
            "orderVWAP",
            "expectedFlatCashPnL",
        ):
            self.assertEqual(D(a[key]), D(b[key]))
        for convention in ("expenseFeeAtFillPrice", "capitalizeFeeInInventory"):
            self.assertEqual(a["conventions"][convention], b["conventions"][convention])
        self.assertNotEqual(
            a["conventions"]["quantityCorrectionAfterEach"]["flatPnL"],
            b["conventions"]["quantityCorrectionAfterEach"]["flatPnL"],
        )

    def test_real_trade_economics_control(self):
        row = cases()["oneFill"]
        self.assertEqual(D(row["net"]), D("31.36"))
        self.assertEqual(D(row["expectedFlatCashPnL"]), D("-.0814208"))

    def test_decimal_projection_matches_retained_native_failure(self):
        report = build_review()
        probe = report["existingNativeReplay"]["positionOnlyExitProbe"]
        projection = report["cases"]["constantShareFraction"]["conventions"][
            "quantityCorrectionAfterEach"
        ]
        self.assertLess(
            abs(D(projection["flatPnL"]) - D(probe["nativeRealizedPnL"])),
            D(".00000001"),
        )
        self.assertFalse(probe["pnlMatchesCash"])

    def test_capitalized_cost_must_not_expense_same_fee_again(self):
        row = cases()["constantShareFraction"]
        capitalized = row["conventions"]["capitalizeFeeInInventory"]
        double_charged = D(capitalized["flatPnL"]) - D(row["feeQuoteEquivalent"])
        self.assertNotEqual(double_charged, D(row["expectedFlatCashPnL"]))

    def test_partial_exit_basis_and_remaining_inventory(self):
        with localcontext() as ctx:
            ctx.prec = 70
            row = cases()["constantShareFraction"]
            average = D(row["conventions"]["expenseFeeAtFillPrice"]["positionAverage"])
            net = D(row["net"])
            first, first_px, second_px = D("10"), D(".011"), D(".010")
            first_pnl = first * (first_px - average) - D(row["feeQuoteEquivalent"])
            total_pnl = first_pnl + (net - first) * (second_px - average)
            cash_pnl = (
                first * first_px + (net - first) * second_px - D(row["cashDebit"])
            )
            self.assertLess(abs(total_pnl - cash_pnl), D("1e-55"))

    def test_multiple_positions_buy_sell_cash_and_equity(self):
        row = multi_position_review()
        self.assertEqual(D(row["account"]["totalCash"]), D("73.68728"))
        self.assertEqual(D(row["equity"]), D("106.61528"))
        self.assertLess(abs(D(row["totalPnL"]) - D("6.61528")), D("1e-55"))
        self.assertEqual(D(row["positions"]["A:YES"]["quantity"]), 0)
        self.assertEqual(D(row["positions"]["B:NO"]["quantity"]), D("78.4"))
        self.assertLess(
            abs(D(row["positions"]["A:YES"]["totalPnL"]) - D("5.45728")), D("1e-55")
        )
        self.assertLess(
            abs(D(row["positions"]["B:NO"]["totalPnL"]) - D("1.158")), D("1e-55")
        )
        for step in row["steps"]:
            self.assertLess(abs(D(step["cashEquityMinusPositionPnL"])), D("1e-55"))

    def test_multiple_positions_flattening_matches_cash(self):
        row = multi_position_review(flatten=True)
        self.assertEqual(D(row["account"]["totalCash"]), D("106.61528"))
        self.assertTrue(all(D(s["quantity"]) == 0 for s in row["positions"].values()))
        self.assertLess(abs(D(row["realizedPnL"]) - D("6.61528")), D("1e-55"))
        self.assertLess(abs(D(row["unrealizedPnL"])), D("1e-55"))

    def test_multiple_positions_duplicate_receipts_do_not_change_reference(self):
        self.assertEqual(multi_position_review(), multi_position_review(duplicate=True))

    def test_locked_cash_is_not_another_equity_expense(self):
        row = multi_position_review()
        cash = row["account"]
        self.assertEqual(D(cash["totalCash"]), D(cash["locked"]) + D(cash["free"]))
        self.assertEqual(D(row["equity"]), D(cash["totalCash"]) + D(row["marketValue"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path)
    args, test_args = parser.parse_known_args()
    if args.record:
        with args.record.open("x") as output:
            json.dump(build_review(), output, indent=2)
            output.write("\n")
        print(f"Saved synthetic accounting review: {args.record}")
    else:
        unittest.main(argv=[__file__, *test_args])
