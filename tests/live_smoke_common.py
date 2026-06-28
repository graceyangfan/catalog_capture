from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional local validation dependency.
    pq = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def make_probe_paths(catalog_root: str, catalog_prefix: str, config_prefix: str) -> tuple[Path, Path]:
    timestamp = int(time.time())
    catalog_dir = Path(catalog_root) / f"{catalog_prefix}-{timestamp}"
    temp_config = Path(catalog_root) / f"{config_prefix}.{timestamp}.toml"
    return catalog_dir, temp_config


def write_temp_capture_config(source: Path, target: Path, catalog_dir: Path, seconds: int) -> None:
    lines = []
    for line in source.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("capture_seconds ="):
            lines.append(f"capture_seconds = {seconds}")
        elif stripped.startswith("catalog_uri ="):
            lines.append(f'catalog_uri = "file://{catalog_dir}"')
        else:
            lines.append(line)
    target.write_text("\n".join(lines) + "\n")


def run_capture_cli(cargo: str, temp_config: Path) -> None:
    command = [
        cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "run",
        "--config",
        str(temp_config),
        "--skip-post-run-report",
    ]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def summarize_catalog(catalog_dir: Path) -> dict[str, dict[str, int | None]]:
    data_dir = catalog_dir / "data"
    if not data_dir.exists():
        raise RuntimeError(f"catalog data dir was not created: {data_dir}")

    summary: dict[str, dict[str, int | None]] = {}
    for family_dir in sorted(path for path in data_dir.iterdir() if path.is_dir()):
        files = sorted(family_dir.glob("**/*.parquet"))
        sample_rows = None
        if pq is not None:
            sample_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in files[:5])
        summary[family_dir.name] = {
            "files": len(files),
            "sample_rows_first_5": sample_rows,
        }
    return summary


def print_catalog_summary(catalog_dir: Path, summary: dict[str, dict[str, int | None]]) -> None:
    total_files = sum(int(values["files"]) for values in summary.values())
    print(f"parquet_files={total_files}")
    print(f"catalog={catalog_dir}")
    for family in sorted(summary):
        values = summary[family]
        rows = values["sample_rows_first_5"]
        row_text = "unavailable" if rows is None else str(rows)
        print(f"{family}: files={values['files']} sample_rows_first_5={row_text}")


def cleanup_probe_artifacts(catalog_dir: Path, temp_config: Path) -> None:
    shutil.rmtree(catalog_dir, ignore_errors=True)
    temp_config.unlink(missing_ok=True)
