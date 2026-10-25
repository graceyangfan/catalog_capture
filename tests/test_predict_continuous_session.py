"""Offline host/observer boundaries; no exchange connections. LGPL-3.0-or-later."""

import copy
import io
import tempfile
from contextlib import redirect_stdout
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from capture_predict_private_fixtures import CaptureError
from predict_read_only_calibration import observe_wallet
from run_predict_fill_live import (
    build_native_host,
    cycle_economics,
    wait_for_strategy_exit,
    run,
)

FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


class Process:
    def __init__(self, polls=None, code=0):
        self.polls = polls
        self.returncode = code

    def poll(self):
        if self.polls is None:
            return None
        if self.polls:
            self.polls -= 1
            return None
        return self.returncode


class HostTests(unittest.TestCase):
    def test_wait_exceeds_old_two_hundred_second_deadline_without_stopping_node(self):
        waits = []
        self.assertEqual(
            wait_for_strategy_exit(Process(1000), Process(), waits.append), 0
        )
        self.assertEqual(sum(waits), 500)

    def test_dead_observer_is_an_explicit_failure_and_never_claims_flat(self):
        with self.assertRaisesRegex(CaptureError, "observer exited"):
            wait_for_strategy_exit(
                Process(), Process(0, 1), lambda _: self.fail("must fail promptly")
            )

    def test_real_recorded_cash_flow_does_not_require_quote_pnl_to_equal_cash(self):
        calibration = json.loads(
            (
                FIXTURES / "live_fill_20261016T123240Z_independent/calibration.json"
            ).read_text()
        )
        http = json.loads(
            (FIXTURES / "live_fill_20261016T123240Z/http_responses.json").read_text()
        )
        hashes = {
            e["requestTerms"]["hash"] for e in http["events"] if "requestTerms" in e
        }
        native = {
            "cashFlowUSDT": "-0.08142080",
            "nativeFeedback": [
                {
                    "type": "NativePositionApplied",
                    "quantity": "0",
                    "realizedPnl": "-0.05582080 USDT",
                }
            ],
        }
        result = cycle_economics(native, calibration, hashes)
        self.assertTrue(result["matchesCash"])
        self.assertEqual(result["settlements"], 2)
        self.assertEqual(result["nativeQuoteRealizedPnl"], "-0.05582080 USDT")
        self.assertEqual(
            cycle_economics(native, calibration, {"unrelated"})["matchesCash"], False
        )
        duplicate = copy.deepcopy(calibration)
        duplicate["comparisons"] += copy.deepcopy(duplicate["comparisons"])
        self.assertEqual(cycle_economics(native, duplicate, hashes), result)
        conflicting = copy.deepcopy(
            next(r for r in duplicate["comparisons"] if r["orderHash"] in hashes)
        )
        conflicting["feeWei"] = "1"
        duplicate["comparisons"].append(conflicting)
        with self.assertRaisesRegex(CaptureError, "contradictory"):
            cycle_economics(native, duplicate, hashes)
        native["cashFlowUSDT"] = "-0.05582080"
        self.assertFalse(cycle_economics(native, calibration, hashes)["matchesCash"])
        calibration["after"]["cashWei"] = str(
            int(calibration["after"]["cashWei"]) + 10**18
        )
        self.assertFalse(cycle_economics(native, calibration, hashes)["matchesCash"])

    def test_continuous_supervisor_coordinates_native_host_observer_and_final_checks(
        self,
    ):
        calibration = json.loads(
            (
                FIXTURES / "live_fill_20261016T123240Z_independent/calibration.json"
            ).read_text()
        )
        original = json.loads(
            (FIXTURES / "live_fill_20261016T123240Z/native_roundtrip.json").read_text()
        )
        original["cashFlowUSDT"] = "-0.08142080"
        for row in original["nativeFeedback"]:
            if (
                row.get("type") == "NativePositionApplied"
                and row.get("quantity") == "0"
            ):
                row["realizedPnl"] = "-0.05582080 USDT"
        admission = json.loads(
            (FIXTURES / "live_fill_20261016T123240Z/admission.json").read_text()
        )
        http = json.loads(
            (FIXTURES / "live_fill_20261016T123240Z/http_responses.json").read_text()
        )
        hashes = {
            e["requestTerms"]["hash"] for e in http["events"] if "requestTerms" in e
        }
        candidate = admission["candidate"]
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "env").mkdir()
            (root / "logs").mkdir()
            (root / "crates/nautilus-predict/tests/fixtures/private_api").mkdir(
                parents=True
            )
            (root / "env/predict-execution.toml").write_text(
                '[test]\nallow_order_writes = false\nrequire_post_only = true\nmax_order_usdt = "1"\nmax_total_usdt = "1"\n'
            )

            class Recorder:
                origin = "http://127.0.0.1:9999"
                closed = False

                def __init__(self, output, policy, record_reads):
                    self.assert_reads = record_reads
                    calls.append(("recorder", record_reads))

                def submitted_hashes(self):
                    return hashes

                def close(self):
                    self.closed = True

            class Child:
                returncode = 0

                def __init__(self, command, **kwargs):
                    self.command = command
                    self.collector = "--until-stop-file" in command
                    calls.append(("child", command))
                    if self.collector:
                        Path(command[command.index("--ready-file") + 1]).write_text(
                            '{"subscribed":true}'
                        )
                    else:
                        control = json.loads(
                            (root / "logs/predict-fill-proxy.json").read_text()
                        )
                        if not control["waitForStrategyExit"]:
                            raise AssertionError("mode must reach Native")
                        (Path(control["output"]) / "native_roundtrip.json").write_text(
                            json.dumps(original)
                        )

                def poll(self):
                    return None if self.collector else 0

                def wait(self, timeout=None):
                    if not self.collector:
                        raise AssertionError(
                            "continuous node must not use wait(timeout)"
                        )
                    stop = Path(
                        self.command[self.command.index("--until-stop-file") + 1]
                    )
                    if not stop.exists():
                        raise AssertionError(
                            "observer stop must follow Native shutdown and own-order cleanup"
                        )
                    name = self.command[self.command.index("--calibration-name") + 1]
                    target = (
                        root
                        / "crates/nautilus-predict/tests/fixtures/private_api"
                        / name
                    )
                    target.mkdir()
                    (target / "calibration.json").write_text(json.dumps(calibration))
                    return 0

            def request(method, path, **kwargs):
                self.assertEqual(method, "GET", "no simulated order writes expected")
                return {"success": True, "data": []}

            with (
                patch("run_predict_fill_live.ROOT", root),
                patch(
                    "run_predict_fill_live.build_native_host",
                    return_value=[
                        "/test/native-host",
                        "fill_live::mainnet_wait_for_strategy_exit",
                        "--ignored",
                        "--exact",
                        "--nocapture",
                    ],
                ),
                patch(
                    "run_predict_fill_live.authenticated_reader",
                    return_value=(request, "owner"),
                ),
                patch("run_predict_fill_live.discover", return_value=candidate),
                patch("run_predict_fill_live.PrivateHttpRecorder", Recorder),
                patch("run_predict_fill_live.subprocess.Popen", Child),
                redirect_stdout(io.StringIO()),
            ):
                run(wait_for_exit=True)
            summaries = list(
                root.glob(
                    "crates/nautilus-predict/tests/fixtures/private_api/live_fill_*/final_verification.json"
                )
            )
            self.assertEqual(len(summaries), 1)
            summary = json.loads(summaries[0].read_text())
            self.assertTrue(summary["complete"])
            self.assertTrue(summary["economics"]["matchesCash"])
            node_command = [
                command
                for kind, command in calls
                if kind == "child" and "/test/native-host" in command
            ][0]
            self.assertEqual(node_command[0], "/test/native-host")
            self.assertNotIn("cargo", node_command)
            self.assertIn("fill_live::mainnet_wait_for_strategy_exit", node_command)
            self.assertFalse((root / "env/predict-fill-calibration.toml").exists())
            self.assertEqual(list((root / "logs").iterdir()), [])

    def test_host_build_selects_the_exact_executable_without_running_it(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "private_api_live"
            executable.touch()
            completed = type(
                "Build",
                (),
                {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "reason": "compiler-artifact",
                            "target": {"name": "private_api_live"},
                            "executable": str(executable),
                        }
                    ),
                },
            )()
            with patch(
                "run_predict_fill_live.subprocess.run", return_value=completed
            ) as build:
                command = build_native_host()
            self.assertEqual(command[0], str(executable))
            self.assertNotIn("--", command)
            args = build.call_args.args[0]
            self.assertIn("--no-run", args)
            self.assertIn("execution", args)
            self.assertIn("fill_live::mainnet_wait_for_strategy_exit", command)

    def test_failed_or_ambiguous_build_never_starts_the_host(self):
        for code, stdout in [
            (1, ""),
            (0, ""),
            (
                0,
                json.dumps(
                    {
                        "reason": "compiler-artifact",
                        "target": {"name": "other"},
                        "executable": "/other",
                    }
                ),
            ),
        ]:
            completed = type("Build", (), {"returncode": code, "stdout": stdout})()
            with (
                patch("run_predict_fill_live.subprocess.run", return_value=completed),
                self.assertRaises(CaptureError),
            ):
                build_native_host()


class Socket:
    def __init__(self, frames):
        self.frames = frames
        self.index = 0
        self.clock = 0
        self.sent = []
        self.closed = False

    def send(self, value):
        self.sent.append(json.loads(value))

    def settimeout(self, _):
        pass

    def recv(self):
        self.clock += 120
        frame = self.frames[self.index]
        self.index += 1
        if isinstance(frame, Exception):
            raise frame
        return json.dumps(frame)

    def close(self):
        self.closed = True


class ObserverTests(unittest.TestCase):
    def test_wallet_stays_on_one_subscription_past_old_collector_limit(self):
        event = {"walletAddress": "owner", "type": "orderAccepted", "id": "17"}
        socket = Socket(
            [
                {"type": "R", "requestId": 1, "success": True},
                *[{"type": "M", "topic": "heartbeat", "data": i} for i in range(6)],
                {"type": "M", "topic": "predictWalletEvents/jwt", "data": event},
            ]
        )
        retained, ready = [], []
        with (
            patch(
                "predict_read_only_calibration.websocket.create_connection",
                return_value=socket,
            ) as connect,
            patch(
                "predict_read_only_calibration.time.monotonic",
                side_effect=lambda: socket.clock,
            ),
        ):
            events, heartbeats, seconds = observe_wallet(
                "owner",
                "jwt",
                "key",
                None,
                lambda: ready.append(True),
                lambda: socket.index == len(socket.frames),
                retained.append,
            )
        self.assertEqual(seconds, 840)
        self.assertEqual(events, [event])
        self.assertEqual(retained, events)
        self.assertEqual(heartbeats, 6)
        self.assertEqual(ready, [True])
        self.assertEqual(len([r for r in socket.sent if r["method"] == "subscribe"]), 1)
        self.assertEqual(len([r for r in socket.sent if r["method"] == "heartbeat"]), 6)
        self.assertTrue(socket.closed)
        connect.assert_called_once()

    def test_disconnect_preserves_already_observed_events_and_fails(self):
        event = {"walletAddress": "owner", "type": "orderFilled"}
        socket = Socket(
            [
                {"type": "R", "requestId": 1, "success": True},
                {"type": "M", "topic": "predictWalletEvents/jwt", "data": event},
                ConnectionError("disconnected"),
            ]
        )
        retained = []
        with (
            patch(
                "predict_read_only_calibration.websocket.create_connection",
                return_value=socket,
            ),
            self.assertRaises(ConnectionError),
        ):
            observe_wallet(
                "owner",
                "jwt",
                "key",
                None,
                stop_requested=lambda: False,
                on_wallet_event=retained.append,
            )
        self.assertEqual(retained, [event])
        self.assertTrue(socket.closed)

    def test_default_observation_remains_time_bounded(self):
        socket = Socket(
            [
                {"type": "R", "requestId": 1, "success": True},
                {"type": "M", "topic": "heartbeat", "data": 1},
            ]
        )
        with (
            patch(
                "predict_read_only_calibration.websocket.create_connection",
                return_value=socket,
            ),
            patch(
                "predict_read_only_calibration.time.monotonic",
                side_effect=lambda: socket.clock,
            ),
        ):
            events, beats, seconds = observe_wallet("owner", "jwt", "key", 60)
        self.assertEqual((events, beats, seconds), ([], 1, 120))

    def test_continuous_observer_requires_an_explicit_stop_source(self):
        with self.assertRaisesRegex(ValueError, "explicit stop"):
            observe_wallet("owner", "jwt", "key", None)


if __name__ == "__main__":
    unittest.main()
