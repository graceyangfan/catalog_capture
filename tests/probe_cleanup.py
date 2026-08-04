#!/usr/bin/env python3
"""Shared helpers for live probe temp catalog/config cleanup."""

from __future__ import annotations

import shutil
import time
from pathlib import Path

SMOKE_CATALOG_GLOB = "catalog-capture-*"
SMOKE_CONFIG_GLOBS = (
    "capture.*-smoke*.toml",
    "capture.*-universe-smoke*.toml",
)


def cleanup_probe_artifacts(*paths: Path) -> None:
    """Remove probe-generated catalog dirs and temp config files."""
    for path in paths:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink(missing_ok=True)


def find_stale_tmp_captures(
    root: Path = Path("/tmp"),
    *,
    max_age_secs: int | None = None,
) -> list[Path]:
    """List probe smoke catalogs/configs under root, optionally filtered by age."""
    now = time.time()
    matches: list[Path] = []
    for pattern in (SMOKE_CATALOG_GLOB, *SMOKE_CONFIG_GLOBS):
        for path in root.glob(pattern):
            if not path.exists():
                continue
            if max_age_secs is not None:
                age = now - path.stat().st_mtime
                if age < max_age_secs:
                    continue
            matches.append(path)
    return sorted(matches)


def cleanup_stale_tmp_captures(
    root: Path = Path("/tmp"),
    *,
    max_age_secs: int | None = 0,
    dry_run: bool = False,
) -> list[Path]:
    """Delete stale probe artifacts. Default: remove all matching paths."""
    targets = find_stale_tmp_captures(root, max_age_secs=max_age_secs)
    if dry_run:
        return targets
    for path in targets:
        cleanup_probe_artifacts(path)
    return targets
