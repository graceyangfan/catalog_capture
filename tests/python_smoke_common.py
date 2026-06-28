from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

from nautilus_import import ensure_nautilus_trader_path
from nautilus_import import project_root

ensure_nautilus_trader_path()

PROJECT_ROOT = project_root()


def make_temp_catalog_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def run_fixture_example(example_name: str, catalog_dir: Path) -> None:
    cmd = [
        "cargo",
        "run",
        "-p",
        "catalog-capture-runtime-adapter",
        "--features",
        "example-binaries",
        "--example",
        example_name,
        "--",
        str(catalog_dir),
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def cleanup_catalog_dir(catalog_dir: Path) -> None:
    shutil.rmtree(catalog_dir, ignore_errors=True)
