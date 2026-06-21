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
    return project_root.parent / "nautilus_trader"


def ensure_nautilus_trader_path() -> Path:
    """Prefer a local nautilus_trader checkout over any stale site-packages install."""
    root = resolve_nautilus_trader_root()
    if not root.is_dir():
        raise SystemExit(
            f"nautilus_trader not found at {root}; "
            "set NAUTILUS_TRADER_ROOT to your checkout"
        )

    root_str = str(root)
    if root_str in sys.path:
        sys.path.remove(root_str)
    sys.path.insert(0, root_str)

    for name in list(sys.modules):
        if name == "nautilus_trader" or name.startswith("nautilus_trader."):
            del sys.modules[name]

    return root


def load_and_ensure() -> Path:
    """Entry point for probe scripts: ensure path before importing nautilus_trader."""
    return ensure_nautilus_trader_path()
