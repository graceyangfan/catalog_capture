"""Protect completed venue evidence and keep synthetic evidence visibly separate."""
import json
import tempfile
import unittest
from pathlib import Path

from predict_evidence import CRATE, MANIFEST, inventory, tier, verify


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


if __name__ == "__main__":
    unittest.main()
