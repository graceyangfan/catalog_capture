"""LGPL-3.0-or-later. Live-probe financial bounds; no network or credentials."""

import unittest
from decimal import Decimal as D

from run_predict_fill_live import FillPolicy, crypto_plan


class CryptoMarketBounds(unittest.TestCase):
    def test_small_quantity_preserves_budget_after_slippage_and_share_fee(self):
        p = crypto_plan([[".41", "100"]], [[".42", "100"]], 200)
        self.assertIsNotNone(p)
        self.assertEqual(D(p["grossQuantity"]), D("3.3"))
        self.assertEqual(D(p["conservativeNetQuantity"]), D("3.234"))
        self.assertLessEqual(D(p["entryCost"]) * D("1.03"), D("1.5"))
        self.assertLess(D(p["estimatedRoundTripLoss"]), D(".1"))
        for bids, asks in [
            ([[".30", "100"]], [[".42", "100"]]),
            ([[".41", "1"]], [[".42", "100"]]),
            ([[".41", "100"]], [[".42", "1"]]),
            ([[".49", "100"]], [[".5", "100"]]),
        ]:
            self.assertIsNone(crypto_plan(bids, asks, 200))

    def test_policy_requires_native_market_fok_and_exact_signed_slippage(self):
        plan = crypto_plan([[".41", "100"]], [[".42", "100"]], 200)
        policy = FillPolicy(
            "account", {"tokenId": "42", "feeRateBps": 200, "plan": plan}, True
        )
        order = {
            "maker": "account",
            "signer": "account",
            "tokenId": "42",
            "feeRateBps": "200",
            "hash": "0x" + "1" * 64,
            "side": 0,
            "makerAmount": str(int(D(plan["entryCost"]) * D("1.03") * 10**18)),
            "takerAmount": str(int(D(plan["grossQuantity"]) * 10**18)),
        }
        data = {
            "order": order,
            "strategy": "MARKET",
            "pricePerShare": str(42 * 10**16),
            "slippageBps": "300",
            "isFillOrKill": True,
            "isMinAmountOut": False,
        }
        self.assertTrue(policy(data, 0))
        self.assertFalse(policy(data, 4))
        for key, value in [
            ("isFillOrKill", False),
            ("isMinAmountOut", True),
            ("slippageBps", "100"),
            ("pricePerShare", str(49 * 10**16)),
        ]:
            self.assertFalse(policy({**data, key: value}, 0))
        order["makerAmount"] = str(2 * 10**18)
        self.assertFalse(policy(data, 0))


if __name__ == "__main__":
    unittest.main()
