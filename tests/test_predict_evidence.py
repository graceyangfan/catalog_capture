"""Protect completed venue evidence and keep synthetic evidence visibly separate."""
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from predict_evidence import CRATE, MANIFEST, inventory, review_resting_run, tier, verify
from predict_resting_evidence import EvidenceError


class EvidenceTests(unittest.TestCase):
    def test_retained_recordings_match_the_frozen_manifest(self):
        report = json.loads(MANIFEST.read_text())
        verify(report)
        names = {e["path"]: e for e in report["entries"]}
        for name in ["live_fill_20261017T051620Z/entry_wallet_observation.json",
                     "exit_only_restart_20261017T053156Z/native_roundtrip.json",
                     "exit_only_restart_20261017T053156Z_independent/wallet_events.jsonl"]:
            self.assertEqual(names["tests/fixtures/private_api/" + name]["tier"], "primary_capture")

    def test_changed_and_missing_primary_captures_fail_instead_of_reindexing(self):
        with tempfile.TemporaryDirectory() as directory:
            crate = Path(directory)
            folder = crate / "tests/fixtures/private_api/live_run"
            folder.mkdir(parents=True)
            path = folder / "wallet_events.jsonl"
            path.write_text('{"type":"orderAccepted"}\n')
            frozen = inventory(crate)
            path.write_text('{"type":"different"}\n')
            with self.assertRaises(ValueError):
                verify(frozen, crate, primary_only=True)
            path.unlink()
            with self.assertRaises(ValueError):
                verify(frozen, crate, primary_only=True)

    def test_derived_real_facts_and_synthetic_cases_are_not_venue_captures(self):
        self.assertEqual(tier("tests/fixtures/actual_replay.json", "offline derived actual mainnet facts"), "derived")
        self.assertEqual(tier("tests/fixtures/actual.json", "synthetic, not mainnet"), "synthetic")
        self.assertEqual(tier("tests/fixtures/live_case_synthetic.json", "actual economic inputs"), "synthetic")

    def test_transient_recordings_cannot_enter_the_frozen_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            crate = Path(directory)
            staging = crate / "tests/fixtures/private_api/live_resting"
            retained = staging.with_name("live_resting_retained")
            staging.mkdir(parents=True)
            retained.mkdir()
            (staging / "result.json").write_text('{"source":"mainnet"}')
            (retained / "result.json").write_text('{"source":"mainnet"}')
            entries = inventory(crate)["entries"]
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["path"], "tests/fixtures/private_api/live_resting_retained/result.json")

    def test_resting_review_is_offline_and_preserves_every_input_byte(self):
        original = CRATE / "tests/fixtures/private_api/live_resting_yes-gtd_20261015T103440Z"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "capture"
            shutil.copytree(original, output)
            before = {p.name: p.read_bytes() for p in output.iterdir() if p.is_file()}
            result = review_resting_run(output)
            self.assertTrue(result["passed"])
            self.assertEqual(result["created_orders"], 2)
            self.assertEqual(result["new_matches"], 0)
            self.assertGreater(result["http_requests"], 0)
            self.assertIsNone(result["run_metadata"])
            self.assertEqual(len(result["files"]), len(before))
            self.assertEqual(before, {p.name: p.read_bytes() for p in output.iterdir() if p.is_file()})
            self.assertTrue(any("not local WS receive" in x for x in result["timing_limits"]))

    def test_zero_open_cleanup_alone_is_not_a_completed_run(self):
        original = CRATE / "tests/fixtures/private_api/live_resting_yes-gtd_20261015T103440Z"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "capture"
            shutil.copytree(original, output)
            final = output / "final_open_orders_check.json"
            final.unlink()
            with self.assertRaises(FileNotFoundError):
                review_resting_run(output)
            final.write_text('{"verifiedOpenOrders":1}')
            with self.assertRaises(EvidenceError):
                review_resting_run(output)
            final.write_text('{"verifiedOpenOrders":0}')
            result = json.loads((output / "result.json").read_text())
            result["nodeFailed"] = True
            (output / "result.json").write_text(json.dumps(result))
            with self.assertRaises(EvidenceError):
                review_resting_run(output)


if __name__ == "__main__":
    unittest.main()
