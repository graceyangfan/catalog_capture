#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["predict-sdk==0.0.22", "requests>=2.32,<3", "websocket-client>=1.8,<2"]
# ///
"""Record known historical orders through our crate; relay blocks all trading writes. LGPL-3.0."""

import argparse
import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime

from run_predict_queries_readonly import ROOT, ReadOnlyRelay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--survey",
        action="store_true",
        help="bounded maker/taker history and order-state survey; no order writes",
    )
    args = parser.parse_args()
    target = (
        "mainnet_record_order_lifecycle_survey_readonly"
        if args.survey
        else "mainnet_record_historical_order_semantics_readonly"
    )
    folder = (
        ROOT
        / "data/predict-validation"
        / ("order_history_readonly_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    )
    folder.mkdir(parents=True, exist_ok=False)
    output = folder / "http_responses.json"
    with tempfile.TemporaryDirectory(prefix=".predict-history-", dir=ROOT) as temporary:
        from pathlib import Path

        work = Path(temporary)
        phase = work / "phase.txt"
        phase.write_text(
            "historical-lifecycle-survey" if args.survey else "historical-order-query"
        )
        feedback = work / "feedback.json"
        # This host makes no RPC requests. The relay denies any chain write even
        # if the selected Rust test is changed accidentally.
        relay = ReadOnlyRelay("https://unreachable.invalid", phase, output)
        env = os.environ.copy()
        env.update(
            PREDICT_HISTORY_READONLY_HTTP_ORIGIN=f"http://127.0.0.1:{relay.server.server_port}",
            PREDICT_HISTORY_READONLY_FEEDBACK=str(feedback),
            PREDICT_HISTORY_READONLY_PHASE=str(phase),
        )
        completed = None
        try:
            completed = subprocess.run(
                [
                    "cargo",
                    "test",
                    "--offline",
                    "-p",
                    "nautilus-predict",
                    "--features",
                    "execution",
                    "--test",
                    "private_api_live",
                    target,
                    "--",
                    "--exact",
                    "--ignored",
                ],
                cwd=ROOT,
                check=False,
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
        finally:
            data = json.loads(feedback.read_text()) if feedback.exists() else None
            relay.close(
                data, success=completed is not None and completed.returncode == 0
            )
        print(
            json.dumps(
                {
                    "recording": str(output),
                    "rustPassed": completed.returncode == 0,
                    "orderWrites": 0,
                    "chainWrites": 0,
                }
            )
        )
        if completed.returncode != 0:
            raise SystemExit(
                "Historical read-only Rust probe failed; inspect sanitized retained recording"
            )


if __name__ == "__main__":
    main()
