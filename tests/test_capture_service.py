#!/usr/bin/env python3
"""Offline service-script contracts; the recorder and Cargo are isolated test stubs."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class CaptureServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="capture-service-tests-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        (self.root / "scripts").mkdir(parents=True)
        (self.root / "bin").mkdir()
        (self.root.parent / "nautilus_trader_quantity_commission").mkdir()
        for script in (
            "run-capture-service.sh",
            "run-mainnet-capture.sh",
            "cleanup-tmp-captures.sh",
        ):
            shutil.copy2(ROOT / "scripts" / script, self.root / "scripts" / script)
        self.config = self.root / "capture.toml"
        self.config.write_text(
            '[runtime]\ncapture_seconds = 0\n[output]\ncatalog_uri = "file://./data/test"\n'
        )
        self.trace = self.root / "trace.jsonl"
        recorder = self.root / "fake-recorder"
        recorder.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "row = {'command':sys.argv[1], 'args':sys.argv[2:]}\n"
            "if '--config' in sys.argv:\n"
            "    path = Path(sys.argv[sys.argv.index('--config')+1])\n"
            "    row['config'] = path.read_text()\n"
            "with open(os.environ['CAPTURE_TEST_TRACE'], 'a') as output:\n"
            "    output.write(json.dumps(row)+'\\n')\n"
            "if sys.argv[1]=='validate':\n"
            "    sys.exit(int(os.environ.get('CAPTURE_TEST_VALIDATE_EXIT','0')))\n"
            "if sys.argv[1]=='run':\n"
            "    sys.exit(int(os.environ.get('CAPTURE_TEST_RUN_EXIT','0')))\n"
        )
        recorder.chmod(0o755)
        shutil.copy2(recorder, self.root / "bin" / "catalog-capture-cli")
        cargo = self.root / "fake-cargo"
        cargo.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, shutil, sys\n"
            "from pathlib import Path\n"
            "root=Path.cwd()\n"
            "with open(os.environ['CAPTURE_TEST_TRACE'], 'a') as output:\n"
            "    output.write(json.dumps({'command':'cargo','args':sys.argv[1:],"
            "'jobs':os.environ.get('CARGO_BUILD_JOBS')})+'\\n')\n"
            "target=root/'target'/'release'\n"
            "target.mkdir(parents=True,exist_ok=True)\n"
            "shutil.copy2(root/'fake-recorder', target/'catalog-capture-cli')\n"
        )
        cargo.chmod(0o755)
        self.env = dict(os.environ)
        self.env.pop("CARGO_BUILD_JOBS", None)
        self.env.pop("CAPTURE_SECONDS", None)
        self.env.update(CAPTURE_TEST_TRACE=str(self.trace), CARGO=str(cargo))

    def run_script(self, name, *args, **env):
        return subprocess.run(
            [str(self.root / "scripts" / name), *map(str, args)],
            cwd=self.root.parent,
            env={**self.env, **env},
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def rows(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()]

    def test_prebuilt_validation_precedes_recording_and_preserves_exit(self):
        result = self.run_script(
            "run-capture-service.sh", "--config", self.config, "--prebuilt",
            CAPTURE_TEST_RUN_EXIT="7",
        )
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual([row["command"] for row in self.rows()], ["validate", "run"])

    def test_validation_failure_never_starts_recording(self):
        result = self.run_script(
            "run-capture-service.sh", "--config", self.config, "--prebuilt",
            CAPTURE_TEST_VALIDATE_EXIT="9",
        )
        self.assertEqual(result.returncode, 9)
        self.assertEqual([row["command"] for row in self.rows()], ["validate"])

    def test_mainnet_reuses_runner_and_removes_finite_config(self):
        result = self.run_script(
            "run-mainnet-capture.sh", self.config, CAPTURE_SECONDS="9"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.rows()
        self.assertEqual([row["command"] for row in rows], ["cargo", "validate", "run"])
        self.assertEqual(rows[0]["jobs"], "2")
        self.assertIn("--locked", rows[0]["args"])
        self.assertIn("capture_seconds = 9", rows[-1]["config"])
        self.assertIn("capture_seconds = 0", self.config.read_text())
        config_path = Path(rows[-1]["args"][1])
        self.assertFalse(config_path.exists())

    def test_post_run_validation_uses_existing_cli_command(self):
        result = self.run_script(
            "run-capture-service.sh", "--config", self.config, "--prebuilt", "--validate"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.rows()[-1]["command"], "validate-option-universe-catalog")
        self.assertNotIn("--config", self.rows()[-1]["args"])

    def test_cleanup_preserves_all_targets_if_any_part_is_unsealed(self):
        data = self.root / "data"
        (data / "closed").mkdir(parents=True)
        (data / "open").mkdir()
        sealed = data / "closed" / "a.parquet"
        part = data / "open" / "a.parquet.part"
        sealed.write_bytes(b"sealed")
        part.write_bytes(b"part")
        result = self.run_script("cleanup-tmp-captures.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(sealed.exists())
        self.assertTrue(part.exists())
        part.unlink()
        result = self.run_script("cleanup-tmp-captures.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(list(data.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
