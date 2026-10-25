"""Bounded maker discovery must remain read-only and not infer lock semantics."""

import unittest

from probe_predict_maker_locks_readonly import discover, lock_hints


class MakerLockDiscoveryTests(unittest.TestCase):
    def test_nested_hints_preserve_false_zero_and_null(self):
        self.assertEqual(
            lock_hints(
                {
                    "variantData": {"lockSeconds": 0, "removalLockedUntil": None},
                    "rewards": [{"delayEnabled": False}],
                }
            ),
            {
                "variantData.lockSeconds": 0,
                "variantData.removalLockedUntil": None,
                "rewards[0].delayEnabled": False,
            },
        )

    def test_pagination_is_bounded_and_get_only(self):
        calls = []

        def read(path, *, params):
            calls.append((path, params))
            return {
                "data": [{"id": len(calls), "variantData": {}}],
                "cursor": str(len(calls)),
            }

        report = discover(read, 2)
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            calls[1], ("/v1/markets", {"first": 100, "status": "OPEN", "after": "1"})
        )
        self.assertEqual(report["market_count"], 2)
        self.assertTrue(report["truncated"])
        self.assertEqual(report["markets_with_lock_hints"], [])

    def test_completed_pages_and_explicit_hint(self):
        report = discover(lambda *a, **kw: {"data": [{"id": 42, "removalDelay": 5}]}, 2)
        self.assertFalse(report["truncated"])
        self.assertEqual(report["pages"], 1)
        self.assertEqual(
            report["markets_with_lock_hints"],
            [{"market_id": 42, "hints": {"removalDelay": 5}}],
        )

    def test_repeated_cursor_fails_instead_of_claiming_complete(self):
        with self.assertRaises(ValueError):
            discover(lambda *a, **kw: {"data": [], "cursor": "same"}, 3)

    def test_invalid_caps_never_read(self):
        def forbidden(*a, **kw):
            self.fail("invalid bound must not issue a read")

        for cap in [0, 21]:
            with self.assertRaises(ValueError):
                discover(forbidden, cap)


if __name__ == "__main__":
    unittest.main()
