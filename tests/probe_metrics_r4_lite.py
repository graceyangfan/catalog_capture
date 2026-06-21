#!/usr/bin/env python3
"""Light R4 soak: short live capture with metrics export and Prometheus assertions."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_CONFIG = PROJECT_ROOT / "examples" / "capture.binance-perp-bars.toml"
DEFAULT_METRICS_URL = "http://127.0.0.1:9898/metrics"


def write_temp_config(
    source: Path,
    destination: Path,
    catalog_dir: Path,
    capture_seconds: int,
    metrics_port: int,
) -> None:
    text = source.read_text(encoding="utf-8")
    text = re.sub(
        r'^catalog_uri = "file://[^"]*"',
        f'catalog_uri = "file://{catalog_dir}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    text = re.sub(
        r"^capture_seconds = \d+",
        f"capture_seconds = {capture_seconds}",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    metrics_block = (
        "\n[runtime.metrics]\n"
        "enabled = true\n"
        'bind_addr = "127.0.0.1"\n'
        f"port = {metrics_port}\n"
        "refresh_interval_secs = 2\n"
    )
    if "[runtime.metrics]" not in text:
        runtime_end = text.find("\n[output]")
        if runtime_end == -1:
            raise RuntimeError("could not locate [output] section in source config")
        text = text[:runtime_end] + metrics_block + text[runtime_end:]
    destination.write_text(text, encoding="utf-8")


def fetch_metrics(url: str, timeout_secs: float = 5.0) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "probe_metrics_r4_lite"})
    with urllib.request.urlopen(request, timeout=timeout_secs) as response:
        return response.read().decode("utf-8")


def parse_prometheus_value(body: str, metric_name: str, labels: str = "") -> float | None:
    needle = metric_name if not labels else f"{metric_name}{labels}"
    for line in body.splitlines():
        if line.startswith("#") or not line.startswith(metric_name):
            continue
        if labels and labels not in line:
            continue
        _, _, value = line.partition(" ")
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def wait_for_metrics(
    url: str,
    timeout_secs: float,
    process: subprocess.Popen | None = None,
) -> str:
    deadline = time.time() + timeout_secs
    last_error: Exception | None = None
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            break
        try:
            body = fetch_metrics(url)
            accepted = parse_prometheus_value(body, "catalog_capture_accepted_items_total")
            if accepted is not None and accepted > 0.0:
                return body
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
        time.sleep(1.0)
    raise RuntimeError(f"metrics endpoint did not become ready at {url}: {last_error}")


def assert_metrics_snapshot(body: str) -> None:
    dropped = parse_prometheus_value(body, "catalog_capture_dropped_items_total")
    assert dropped is not None, "missing catalog_capture_dropped_items_total"
    assert dropped == 0.0, f"expected dropped_items_total=0, got {dropped}"

    active_partitions = parse_prometheus_value(body, "catalog_capture_active_partitions")
    assert active_partitions is not None, "missing catalog_capture_active_partitions"
    assert active_partitions >= 0.0

    rss = parse_prometheus_value(body, "catalog_capture_process_rss_bytes")
    if rss is None or rss <= 0.0:
        print(
            "warning: catalog_capture_process_rss_bytes unavailable on this platform",
            flush=True,
        )

    accepted = parse_prometheus_value(body, "catalog_capture_accepted_items_total")
    assert accepted is not None and accepted > 0.0, (
        "expected catalog_capture_accepted_items_total > 0 during live capture"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a short live capture and validate Prometheus metrics export (R4 lite).",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=60,
        help="Capture duration injected into the temporary profile (default 60s).",
    )
    parser.add_argument(
        "--catalog-root",
        default="/tmp",
        help="Directory where the temporary smoke catalog will be created.",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=9898,
        help="Metrics HTTP port to enable in the temporary profile.",
    )
    parser.add_argument(
        "--metrics-url",
        default=None,
        help="Override metrics URL (default http://127.0.0.1:<port>/metrics).",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove generated catalog and config after successful validation.",
    )
    parser.add_argument(
        "--cargo",
        default="cargo",
        help="Cargo executable to use.",
    )
    args = parser.parse_args()

    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if args.metrics_port <= 0:
        parser.error("--metrics-port must be positive")

    metrics_url = args.metrics_url or f"http://127.0.0.1:{args.metrics_port}/metrics"
    timestamp = int(time.time())
    catalog_dir = (
        Path(args.catalog_root) / f"nautilus-catalog-capture-metrics-r4-lite-{timestamp}"
    )
    temp_config = (
        Path(args.catalog_root) / f"capture.metrics-r4-lite.{timestamp}.toml"
    )
    write_temp_config(
        SOURCE_CONFIG,
        temp_config,
        catalog_dir,
        args.seconds,
        args.metrics_port,
    )

    print(f"config={temp_config}", flush=True)
    print(f"catalog={catalog_dir}", flush=True)
    print(f"metrics_url={metrics_url}", flush=True)

    command = [
        args.cargo,
        "run",
        "-p",
        "catalog-capture-cli",
        "--",
        "run",
        "--config",
        str(temp_config),
        "--skip-post-run-report",
    ]
    print(f"running live capture for {args.seconds}s", flush=True)
    process = subprocess.Popen(command, cwd=PROJECT_ROOT)
    try:
        # Metrics HTTP shuts down with the capture process; sample while it is alive.
        body = wait_for_metrics(
            metrics_url,
            timeout_secs=max(float(args.seconds) + 30.0, 45.0),
            process=process,
        )
        exit_code = process.wait(timeout=args.seconds + 90)
        if exit_code != 0:
            raise RuntimeError(f"capture process exited with status {exit_code}")
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)

    assert_metrics_snapshot(body)
    print("R4 lite metrics probe succeeded")
    print(f"dropped_items_total=0 accepted_items_total={parse_prometheus_value(body, 'catalog_capture_accepted_items_total')}")
    print(f"active_partitions={parse_prometheus_value(body, 'catalog_capture_active_partitions')}")
    print(f"process_rss_bytes={parse_prometheus_value(body, 'catalog_capture_process_rss_bytes')}")

    if args.cleanup:
        shutil.rmtree(catalog_dir, ignore_errors=True)
        temp_config.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())