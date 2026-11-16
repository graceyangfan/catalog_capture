"""Resolve nautilus_trader on sys.path for catalog probe scripts."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def resolve_nautilus_trader_root() -> Path:
    env_root = os.environ.get("NAUTILUS_TRADER_ROOT")
    if env_root:
        return Path(env_root)

    project_root = Path(__file__).resolve().parents[1]
    return project_root.parent / "nautilus_trader_quantity_commission"


def ensure_nautilus_trader_path() -> Path:
    """Prefer a local nautilus_trader checkout over any stale site-packages install."""
    root = resolve_nautilus_trader_root()
    if not root.is_dir():
        raise SystemExit(
            f"nautilus_trader not found at {root}; "
            "set NAUTILUS_TRADER_ROOT to your checkout"
        )

    package_root = root / "python" if (root / "python" / "nautilus_trader").is_dir() else root
    if not (package_root / "nautilus_trader" / "__init__.py").is_file():
        raise SystemExit(
            f"nautilus_trader Python package not found under {root}; "
            "set NAUTILUS_TRADER_ROOT to the checkout or its python directory"
        )

    package_root_str = str(package_root)
    if package_root_str in sys.path:
        sys.path.remove(package_root_str)
    sys.path.insert(0, package_root_str)

    for name in list(sys.modules):
        if name == "nautilus_trader" or name.startswith("nautilus_trader."):
            del sys.modules[name]

    return root


def project_root() -> Path:
    """Return the catalog_capture repository root."""
    return Path(__file__).resolve().parents[1]


def load_and_ensure() -> Path:
    """Entry point for probe scripts: ensure path before importing nautilus_trader."""
    return ensure_nautilus_trader_path()
