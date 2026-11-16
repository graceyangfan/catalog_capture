"""Resolve Native Python readback against the pinned fork instead of a stale install."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class NautilusImportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.helper = self.root / "catalog_capture" / "tests" / "nautilus_import.py"
        self.helper.parent.mkdir(parents=True)
        shutil.copyfile(Path(__file__).with_name("nautilus_import.py"), self.helper)
        self.fork = self.root / "nautilus_trader_quantity_commission"
        self.package(self.fork / "python", "quantity")
        self.package(self.root / "nautilus_trader", "other-checkout")
        self.stale = self.root / "site-packages"
        self.package(self.stale, "stale-install")

    @staticmethod
    def package(root, origin):
        package = root / "nautilus_trader"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text(f"origin = {origin!r}\n")

    def run_resolver(self, override=None):
        env = os.environ.copy()
        env.pop("NAUTILUS_TRADER_ROOT", None)
        if override is not None:
            env["NAUTILUS_TRADER_ROOT"] = str(override)
        script = f"""
import importlib.util, json, sys
sys.path.insert(0, {str(self.stale)!r})
import nautilus_trader
assert nautilus_trader.origin == 'stale-install'
spec = importlib.util.spec_from_file_location('resolver', {str(self.helper)!r})
resolver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resolver)
root = resolver.ensure_nautilus_trader_path()
import nautilus_trader
print(json.dumps({{'root': str(root), 'origin': nautilus_trader.origin, 'file': nautilus_trader.__file__}}))
"""
        return subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )

    def test_default_uses_quantity_fork_over_other_checkout_and_loaded_install(self):
        result = self.run_resolver()
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["origin"], "quantity")
        self.assertEqual(Path(report["root"]), self.fork)
        self.assertEqual(
            Path(report["file"]), self.fork / "python/nautilus_trader/__init__.py"
        )

    def test_explicit_checkout_and_python_directory_are_supported(self):
        other = self.root / "explicit"
        self.package(other / "python", "explicit")
        for override in (other, other / "python"):
            with self.subTest(override=override):
                result = self.run_resolver(override)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)["origin"], "explicit")

    def test_missing_checkout_does_not_fall_back_to_installed_package(self):
        result = self.run_resolver(self.root / "missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nautilus_trader not found", result.stderr)

    def test_existing_directory_without_package_is_rejected(self):
        empty = self.root / "empty"
        empty.mkdir()
        result = self.run_resolver(empty)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Python package not found", result.stderr)


if __name__ == "__main__":
    unittest.main()
