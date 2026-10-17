"""Exit-only relay boundaries; no credentials or venue connection."""
import unittest
from run_predict_exit_only_live import ExitOnlyPolicy


class ExitOnlyPolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = ExitOnlyPolicy("0x123", "11", "31.36", 200)
        self.data = {"strategy": "LIMIT", "isPostOnly": True, "order": {
            "maker": "0x123", "signer": "0x123", "side": 1,
            "tokenId": "11", "feeRateBps": "200", "makerAmount": "31360000000000000000"}}

    def test_existing_inventory_sell_and_bounded_taker(self):
        self.assertTrue(self.policy(self.data, 0))
        self.data.update(strategy="MARKET", slippageBps=100)
        self.assertTrue(self.policy(self.data, 1))
        self.data["slippageBps"] = 101
        self.assertFalse(self.policy(self.data, 1))

    def test_buy_is_forbidden_even_as_first_submission(self):
        self.data["order"]["side"] = 0
        self.assertFalse(self.policy(self.data, 0))
        self.assertFalse(self.policy(self.data, 1))

    def test_oversell_wrong_token_zero_and_attempt_limit(self):
        self.assertFalse(self.policy(self.data, 3))
        for key, value in [("makerAmount", "31360000000000000001"), ("makerAmount", "0"), ("tokenId", "22"), ("signer", "0x999")]:
            old = self.data["order"][key]
            self.data["order"][key] = value
            self.assertFalse(self.policy(self.data, 0))
            self.data["order"][key] = old


if __name__ == "__main__":
    unittest.main()
