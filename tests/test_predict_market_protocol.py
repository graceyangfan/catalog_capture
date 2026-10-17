"""Offline mainnet-observation provenance and capture isolation. LGPL-3.0-or-later."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from capture_predict_private_fixtures import CaptureError, capture
from compare_predict_market_observation import BASE, compare


class MarketProtocolTests(unittest.TestCase):
    def test_invalid_capture_name_cannot_read_credentials_or_start_requests(self):
        with patch("capture_predict_private_fixtures.private_toml") as read:
            for name in ("../old", "public_snapshots.json", "/outside"):
                with self.subTest(name=name), self.assertRaises(CaptureError):
                    capture(calibrate=True, calibration_name=name)
            read.assert_not_called()

    def test_existing_capture_is_never_replaced(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "saved").mkdir()
            with (
                patch("capture_predict_private_fixtures.OUTPUT", output),
                patch("capture_predict_private_fixtures.private_toml") as read,
            ):
                with self.assertRaises(CaptureError):
                    capture(calibrate=True, calibration_name="saved")
                read.assert_not_called()

    def test_named_capture_reserves_directory_before_authentication_and_cannot_be_reused(
        self,
    ):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            with (
                patch("capture_predict_private_fixtures.OUTPUT", output),
                patch(
                    "capture_predict_private_fixtures.private_toml",
                    side_effect=CaptureError("fixture auth failure"),
                ) as read,
            ):
                with self.assertRaises(CaptureError):
                    capture(calibrate=True, calibration_name="unique")
                self.assertTrue((output / "unique").is_dir())
                with self.assertRaises(CaptureError):
                    capture(calibrate=True, calibration_name="unique")
                self.assertEqual(read.call_count, 1)

    def test_actual_strategy_and_independent_capture_remain_read_only(self):
        live = json.loads(
            (BASE / "market_read_only_livenode_20261015.json").read_text()
        )
        self.assertFalse(live["tradeAllowed"])
        self.assertEqual(live["strategySubmissions"], 0)
        self.assertEqual(live["fills"], 0)
        self.assertGreater(live["accountReportsObserved"], 0)
        self.assertGreater(len(live["snapshots"]), 0)
        self.assertTrue(
            all(row["marketId"] == live["marketId"] for row in live["snapshots"])
        )
        capture = json.loads(
            (BASE / "market_read_only_20261015/calibration.json").read_text()
        )
        self.assertEqual(capture["before"]["cashWei"], capture["after"]["cashWei"])
        self.assertEqual(capture["before"]["orders"], [])
        self.assertEqual(capture["after"]["orders"], [])
        self.assertEqual(capture["before"]["positions"], capture["after"]["positions"])
        self.assertEqual(capture["wallet"]["events"], [])

    def test_saved_actual_book_sdk_oracle_is_reproducible_without_operator_config(self):
        live = json.loads(
            (BASE / "market_read_only_livenode_20261015.json").read_text()
        )
        saved = json.loads(
            (BASE / "market_live_snapshot_sdk_quotes_20261015.json").read_text()
        )
        self.assertEqual(compare(live), saved)
        self.assertEqual({row["side"] for row in saved["vectors"]}, {0, 1})
        self.assertEqual({row["outcome"] for row in saved["vectors"]}, {1, 2})

    def test_recovery_sessions_are_synthetic_and_never_claim_submission_or_live_profit(
        self,
    ):
        fixture = json.loads(
            (BASE / "market_recovery_synthetic_20261015.json").read_text()
        )
        self.assertIn("synthetic", fixture["source"])
        self.assertEqual(len(fixture["sessions"]), 4)
        for row in fixture["sessions"]:
            self.assertEqual(row["venueCreates"], 0)
            self.assertEqual(row["strategySubmissions"], 0)
            self.assertEqual(row["entryGross"], "100")
            self.assertEqual(row["entryNet"], "98")
            self.assertTrue(
                all(
                    event["details"]["strategyType"] == "MARKET"
                    for event in row["walletEvents"]
                )
            )


if __name__ == "__main__":
    unittest.main()
