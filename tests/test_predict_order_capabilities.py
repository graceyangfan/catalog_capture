"""Validation-only probe must never forward a usable order. LGPL-3.0."""

import copy
import unittest

from probe_predict_order_capabilities import (
    INVALID_SIGNATURE,
    assert_unexecutable,
    invalid_order_body,
)


class CapabilityProbeTests(unittest.TestCase):
    def test_every_variant_discards_original_signature_hash_and_expiry(self):
        original = {
            "order": {
                "hash": "original-hash",
                "signature": "original-signature",
                "expiration": 4102444800,
                "makerAmount": "100",
            }
        }
        before = copy.deepcopy(original)
        for flag in [None, True, False, "not-a-boolean"]:
            body = invalid_order_body(original, flag)
            assert_unexecutable(body)
            self.assertEqual(body["data"]["order"]["signature"], INVALID_SIGNATURE)
            self.assertNotIn("hash", body["data"]["order"])
            self.assertEqual(body["data"]["order"]["makerAmount"], "100")
        self.assertEqual(original, before)

    def test_either_missing_invalidation_blocks_forwarding(self):
        for signature, expiry in [
            ("valid-signature", 1),
            (INVALID_SIGNATURE, 4102444800),
        ]:
            with self.assertRaises(ValueError):
                assert_unexecutable(
                    {"data": {"order": {"signature": signature, "expiration": expiry}}}
                )


class SignedProbeBoundsTests(unittest.TestCase):
    def test_only_one_usdt_and_distant_price_are_admitted(self):
        from decimal import Decimal as D

        from probe_predict_market_fok_live import check_bounds

        check_bounds(D(".01"), D(100), D(".31"))
        for price, quantity, ask in [
            (".02", "50", ".50"),
            (".01", "101", ".31"),
            (".01", "100", ".19"),
            ("0", "100", ".31"),
        ]:
            with self.assertRaises(ValueError):
                check_bounds(D(price), D(quantity), D(ask))

    def test_diagnostics_redact_secrets_hex_and_jwt(self):
        from probe_predict_order_capabilities import safe_diagnostic

        text = safe_diagnostic(
            {"error": "secret-sentinel", "message": "0x" + "ab" * 65 + " eyJabc.def"},
            ["secret-sentinel"],
        )
        self.assertNotIn("secret-sentinel", text)
        self.assertNotIn("ab" * 65, text)
        self.assertNotIn("eyJabc", text)


class MarketMatrixBoundsTests(unittest.TestCase):
    def test_closer_mode_preserves_budget_and_a_quarter_dollar_price_gap(self):
        import time
        from decimal import Decimal as D

        from predict_sdk import ChainId, OrderBuilder
        from probe_predict_market_matrix_live import amounts_for, cases, check_bounds

        builder = OrderBuilder.make(ChainId.BNB_MAINNET)
        for case in cases(closer=True):
            amounts = amounts_for(builder, case)
            body = {
                "data": {
                    "strategy": "MARKET",
                    "pricePerShare": str(amounts.price_per_share),
                    "order": {
                        "side": 0,
                        "makerAmount": str(amounts.maker_amount),
                        "takerAmount": str(amounts.taker_amount),
                        "expiration": int(time.time()) + 300,
                    },
                }
            }
            check_bounds(body, D(".84"), closer=True)
            self.assertEqual(amounts.maker_amount, 10**18)
            if case["price"] == ".50":
                with self.assertRaises(ValueError):
                    check_bounds(body, D(".74"), closer=True)
            with self.assertRaises(ValueError):
                check_bounds(body, D(".84"))

    def test_signed_ratio_and_reference_both_require_distance_and_one_usdt_cap(self):
        import time
        from decimal import Decimal as D

        from probe_predict_market_matrix_live import check_bounds

        body = {
            "data": {
                "strategy": "MARKET",
                "pricePerShare": str(10**16),
                "order": {
                    "side": 0,
                    "makerAmount": str(10**18),
                    "takerAmount": str(100 * 10**18),
                    "expiration": int(time.time()) + 300,
                },
            }
        }
        check_bounds(body, D(".50"))
        for key, value in [
            ("makerAmount", str(10**18 + 1)),
            ("takerAmount", str(10**18)),
            ("side", 1),
            ("expiration", 1),
            ("expiration", int(time.time()) + 400),
        ]:
            changed = copy.deepcopy(body)
            changed["data"]["order"][key] = value
            with self.assertRaises(ValueError):
                check_bounds(changed, D(".50"))
        changed = copy.deepcopy(body)
        changed["data"]["pricePerShare"] = str(3 * 10**16)
        with self.assertRaises(ValueError):
            check_bounds(changed, D(".50"))
        with self.assertRaises(ValueError):
            check_bounds(body, D(".19"))

    def test_official_minimum_out_slippage_changes_minimum_without_raising_spend(self):
        from predict_sdk import ChainId, OrderBuilder
        from probe_predict_market_matrix_live import amounts_for, cases

        builder = OrderBuilder.make(ChainId.BNB_MAINNET)
        variants = cases()
        self.assertEqual(len(variants), 8)
        baseline = amounts_for(builder, variants[0])
        minimum = amounts_for(builder, variants[4])
        self.assertEqual(baseline.maker_amount, 10**18)
        self.assertEqual(minimum.maker_amount, 10**18)
        self.assertEqual(baseline.taker_amount, 100 * 10**18)
        self.assertEqual(minimum.taker_amount, 99 * 10**18)
        for case in variants:
            self.assertLessEqual(amounts_for(builder, case).maker_amount, 10**18)
