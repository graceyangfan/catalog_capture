"""LGPL-3.0-or-later. Bounds for the explicitly invoked live probe; no network."""

import unittest
from decimal import Decimal

from run_predict_native_market_probe import probe_cases, signed_gap_is_safe


class SignedPriceBounds(unittest.TestCase):
    def test_slippage_changes_the_distance_checked_before_forwarding(self):
        shares = 125 * 10**16
        self.assertTrue(signed_gap_is_safe(10**18, shares, Decimal(".82")))
        self.assertFalse(signed_gap_is_safe(101 * 10**16, shares, Decimal(".82")))
        self.assertTrue(signed_gap_is_safe(102 * 10**16, shares, Decimal(".84")))
        self.assertFalse(signed_gap_is_safe(102 * 10**16, shares, Decimal(".83")))
        self.assertFalse(signed_gap_is_safe(10**18, shares, Decimal(".80")))
        self.assertFalse(signed_gap_is_safe(10**18, shares, Decimal(".99")))
        self.assertFalse(signed_gap_is_safe(10**18, 0, Decimal(".84")))

    def test_matrix_preserves_budget_and_native_supported_quote_mode(self):
        cases = probe_cases(True)
        self.assertEqual(len(cases), 5)
        self.assertEqual([c["bps"] for c in cases], [0, 100, 200, 100, 0])
        self.assertEqual([c["fok"] for c in cases], [True, True, True, False, True])
        self.assertEqual(
            [c["quote"] for c in cases], [False, False, False, False, True]
        )
        self.assertEqual(len(probe_cases(False)), 2)


if __name__ == "__main__":
    unittest.main()
