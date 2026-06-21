#!/usr/bin/env python3
"""Remove stale live-probe catalogs and temp TOML configs from /tmp."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from probe_cleanup import cleanup_stale_tmp_captures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clean probe-generated catalogs and temp configs from /tmp.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("/tmp"),
        help="Directory to scan (default: /tmp).",
    )
    parser.add_argument(
        "--min-age-secs",
        type=int,
        default=0,
        help="Only remove artifacts at least this many seconds old (default: 0 = all matches).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching paths without deleting them.",
    )
    args = parser.parse_args()

    removed = cleanup_stale_tmp_captures(
        args.root,
        max_age_secs=args.min_age_secs,
        dry_run=args.dry_run,
    )
    action = "would remove" if args.dry_run else "removed"
    if not removed:
        print(f"No matching probe artifacts under {args.root}")
        return 0

    for path in removed:
        print(f"{action}: {path}")
    print(f"{action} {len(removed)} path(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())