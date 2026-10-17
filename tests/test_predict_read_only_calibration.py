"""Offline checks for bounded capture and retained live calibration. LGPL-3.0-or-later."""

import json
import unittest
from pathlib import Path

from predict_read_only_calibration import complete_pages


class PaginationTests(unittest.TestCase):
    def test_complete_history_keeps_all_pages(self):
        calls = []

        def request(method, path, params):
            calls.append((method, params))
            return {
                "success": True,
                "data": [len(calls)],
                "cursor": "next" if len(calls) == 1 else None,
            }

        self.assertEqual(complete_pages(request, "/v1/orders/matches"), [1, 2])
        self.assertEqual(calls[1][1]["after"], "next")
        self.assertTrue(all(method == "GET" for method, _ in calls))

    def test_repeated_cursor_rejected(self):
        with self.assertRaisesRegex(ValueError, "repeated"):
            complete_pages(
                lambda *a, **k: {"success": True, "data": [], "cursor": "same"},
                "/v1/positions",
            )

    def test_page_bound_is_not_empty_success(self):
        cursor = iter(range(100))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            complete_pages(
                lambda *a, **k: {
                    "success": True,
                    "data": [],
                    "cursor": str(next(cursor)),
                },
                "/v1/orders",
            )

    def test_malformed_page_rejected(self):
        with self.assertRaisesRegex(ValueError, "envelope"):
            complete_pages(lambda *a, **k: {"success": False, "data": []}, "/v1/orders")


class CapturedEvidenceTests(unittest.TestCase):
    def test_live_capture_exact_partial_totals_and_replay(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "crates/nautilus-predict/tests/fixtures/private_api/read_only_calibration/calibration.json"
        )
        if not path.exists():
            self.skipTest("live calibration fixture not captured")
        data = json.loads(path.read_text())
        baseline = json.loads((path.parent.parent / "matches.json").read_text())

        # The fresh history must still match the real vectors consumed by Rust tests.
        def own_legs(rows):
            return {
                (row["settlementId"], leg["hash"]): (
                    leg["amount"],
                    leg["price"],
                    leg["fee"],
                )
                for row in rows
                for leg in [row["taker"], *row["makers"]]
                if leg["signer"].lower() == "0x0000000000000000000000000000000000000022"
            }

        self.assertEqual(own_legs(data["after"]["matches"]), own_legs(baseline["data"]))
        self.assertTrue(data["wallet"]["subscribed"])
        self.assertGreaterEqual(data["wallet"]["seconds"], 60)
        seen, totals = set(), {}
        # A second identical replay must have no second accounting effect.
        for row in data["comparisons"] * 2:
            key = row["settlementId"], row["orderHash"]
            if key in seen:
                continue
            seen.add(key)
            gross, fee, net = (
                int(row[k]) for k in ("grossWei", "feeWei", "netTransferWei")
            )
            self.assertEqual(gross - fee, net)
            # These two observed historical fills were at p=0.13, feeRateBps=200.
            self.assertEqual(fee, gross * 200 // 10_000)
            self.assertEqual(
                int(row["collateralWei"]), gross * 130_000_000_000_000_000 // 10**18
            )
            total = totals.setdefault(row["orderHash"], [0, 0])
            total[0] += gross
            total[1] += fee
        self.assertEqual(len(seen), data["summary"]["chainVerifiedOwnFills"])
        for order, (gross, fee) in totals.items():
            self.assertEqual(
                data["cumulative"][order],
                {
                    "grossWei": str(gross),
                    "feeWei": str(fee),
                    "netWei": str(gross - fee),
                },
            )
        for snapshot in (data["before"], data["after"]):
            for check in snapshot.get("positionChecks", []):
                self.assertEqual(int(check["apiWei"]), int(check["chainWei"]))
            # Empty REST holdings are checked against the discovered historical token universe.
            if not snapshot["positions"]:
                self.assertTrue(
                    all(
                        int(v["balanceWei"]) == 0
                        for v in snapshot["tokenBalances"].values()
                    )
                )
        self.assertEqual(
            data["before"]["account"]["data"]["address"],
            data["after"]["account"]["data"]["address"],
        )


if __name__ == "__main__":
    unittest.main()
