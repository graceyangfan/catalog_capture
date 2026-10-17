"""Independent Decimal projection examples for the account/precision RFC research.

These are analytical examples, not venue captures or another production ledger.
"""
import json
import math
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, localcontext
from pathlib import Path
import sys
import unittest


def project(value, places, rounding):
    return value.quantize(Decimal(1).scaleb(-places), rounding=rounding)


def examples():
    with localcontext() as context:
        context.prec = 60
        fee = Decimal("0.000000004900000001")
        raw_fee = fee * 100
        tiny_fee = Decimal("0.000000000000000001")
        gross = Decimal("17.000000000000000099")
        shares_fee = Decimal("0.340000000000000001")
        separate_net = project(gross, 16, ROUND_FLOOR) - project(shares_fee, 16, ROUND_CEILING)
        raw_net = gross - shares_fee
        return {
            "source": "independent analytical Decimal examples, not venue data or a Native test",
            "moneyFees100": {
                "raw": str(raw_fee),
                "sumOfIndividuallyRounded": str(project(fee, 8, ROUND_HALF_EVEN) * 100),
                "roundedCumulative": str(project(raw_fee, 8, ROUND_HALF_EVEN)),
                "rawMinusProjected": str(raw_fee - project(raw_fee, 8, ROUND_HALF_EVEN)),
            },
            "tinySharesFees100": {
                "raw": str(tiny_fee * 100),
                "sumOfIndividualCeilings": str(project(tiny_fee, 16, ROUND_CEILING) * 100),
                "cumulativeCeiling": str(project(tiny_fee * 100, 16, ROUND_CEILING)),
            },
            "nonCommutingGrossAndFeeProjection": {
                "rawNet": str(raw_net),
                "separateConservativeNet": str(separate_net),
                "singleNetFloor": str(project(raw_net, 16, ROUND_FLOOR)),
                "separateNetResidual": str(raw_net - separate_net),
                "singleNetResidual": str(raw_net - project(raw_net, 16, ROUND_FLOOR)),
                "warning": "Single-net projection cannot replace gross last_qty in OrderFilled.",
            },
            "f64UlpAt32": str(Decimal.from_float(math.ulp(32.0))),
            "quantum16": "0.0000000000000001",
            "quantum18": "0.000000000000000001",
            "precisionIsNotOrderIncrement": True,
        }


class ProjectionResearch(unittest.TestCase):
    def test_cumulative_money_projection_retains_small_fees(self):
        row = examples()["moneyFees100"]
        self.assertEqual(Decimal(row["sumOfIndividuallyRounded"]), 0)
        self.assertEqual(Decimal(row["roundedCumulative"]), Decimal("0.00000049"))
        self.assertEqual(Decimal(row["rawMinusProjected"]), Decimal("0.000000000000000100"))

    def test_cumulative_quantity_ceiling_avoids_per_fill_overdeduction(self):
        row = examples()["tinySharesFees100"]
        self.assertEqual(Decimal(row["cumulativeCeiling"]), Decimal(row["raw"]))
        self.assertEqual(Decimal(row["sumOfIndividualCeilings"]), Decimal(row["raw"]) * 100)

    def test_separate_projection_is_not_exact_net_projection(self):
        row = examples()["nonCommutingGrossAndFeeProjection"]
        self.assertEqual(Decimal(row["separateNetResidual"]), Decimal("0.000000000000000198"))
        self.assertLess(Decimal(row["singleNetResidual"]), Decimal("1e-16"))
        self.assertLess(Decimal(row["separateConservativeNet"]), Decimal(row["singleNetFloor"]))

    def test_decimal_places_do_not_imply_float_significant_precision(self):
        self.assertGreater(Decimal(examples()["f64UlpAt32"]), Decimal("1e-16"))


if __name__ == "__main__":
    write = "--write-json" in sys.argv
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(ProjectionResearch))
    if not result.wasSuccessful():
        raise SystemExit(1)
    if write:
        destination = Path(__file__).resolve().parents[1] / "crates/nautilus-predict/tests/fixtures/private_api/quantity_fee_account_precision_research_20261017.json"
        with destination.open("x") as output:
            json.dump(examples(), output, indent=2)
            output.write("\n")
        print(destination)
