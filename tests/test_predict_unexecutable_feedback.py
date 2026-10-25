"""Strict no-fill probe guards; these tests never authenticate or send requests. LGPL-3.0."""

import copy
import unittest
from datetime import UTC, datetime

from probe_predict_unexecutable_feedback import INVALID_SIGNATURE, require_unexecutable


class UnexecutableFeedbackGuards(unittest.TestCase):
    def body(self, expiry=1):
        return {
            "data": {
                "order": {
                    "signature": INVALID_SIGNATURE,
                    "expiration": expiry,
                    "makerAmount": str(10**18),
                    "takerAmount": str(100 * 10**18),
                }
            }
        }

    def test_expired_phase_requires_both_invalid_signature_and_expiry(self):
        require_unexecutable(self.body())
        for field, value in [
            ("signature", "0x" + "11" * 65),
            ("expiration", int(datetime.now(UTC).timestamp()) + 300),
            ("makerAmount", str(2 * 10**18)),
        ]:
            body = self.body()
            body["data"]["order"][field] = value
            with self.assertRaises(ValueError):
                require_unexecutable(body)

    def test_signature_phase_never_accepts_a_nonzero_signature(self):
        body = self.body(int(datetime.now(UTC).timestamp()) + 300)
        require_unexecutable(body, require_expired=False)
        for signature in ["", "0x" + "11" * 65, None]:
            wrong = copy.deepcopy(body)
            wrong["data"]["order"]["signature"] = signature
            with self.assertRaises(ValueError):
                require_unexecutable(wrong, require_expired=False)

    def test_signature_phase_rejects_unbounded_or_expired_terms(self):
        for expiry in [1, int(datetime.now(UTC).timestamp()) + 3600]:
            with self.assertRaises(ValueError):
                require_unexecutable(self.body(expiry), require_expired=False)
