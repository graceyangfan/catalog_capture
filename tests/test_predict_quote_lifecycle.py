"""Saved synthetic native quote lifecycle evidence. LGPL-3.0-or-later."""

import json
import unittest
from decimal import Decimal
from pathlib import Path

FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "crates/nautilus-predict/tests/fixtures/private_api"
)


class PredictQuoteLifecycleTests(unittest.TestCase):
    def test_dynamic_strategy_quotes_keep_only_current_custom_stream(self):
        fixture = json.loads(
            (
                FIXTURES / "dynamic_quotes_strategy_rollover_synthetic_20261016.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 2)
        self.assertEqual(
            {s["backend"] for s in fixture["sessions"]}, {"Sockudo", "Tungstenite"}
        )
        for session in fixture["sessions"]:
            self.assertEqual(session["mainnetWrites"], 0)
            rows, frames = session["feedback"], session["frames"]
            instruments = {r["id"] for r in rows if r["type"] == "instrument"}
            self.assertEqual(
                instruments,
                {f"{m}-{o}.PREDICT" for m in (2859346, 2859347) for o in (1, 2)},
            )
            first_new = next(
                i
                for i, r in enumerate(rows)
                if r["type"] == "custom" and r["market"] == 2859347
            )
            self.assertFalse(
                any(
                    r["type"] == "custom" and r["market"] == 2859346
                    for r in rows[first_new + 1 :]
                )
            )
            boundary = next(
                f["data"]["updateTimestampMs"]
                for f in frames
                if f["data"]["marketId"] == 2859347
            )
            custom = [r for r in rows if r["type"] == "custom"]
            native_custom = [r for r in rows if r["type"] == "strategy_custom"]
            self.assertEqual(
                [(r["market"], r["ts"], r["count"]) for r in custom],
                [(r["market"], r["ts"], r["count"]) for r in native_custom],
            )
            for row in native_custom:
                self.assertEqual(row["identifier"], str(row["market"]))
                self.assertEqual(
                    row["selector"],
                    {
                        "predict_price_feed_symbol": "BTCUSDT",
                        "predict_title_asset": "bitcoin",
                        "predict_interval_secs": "300",
                    },
                )
            quotes = [r for r in rows if r["type"] == "quote"]
            self.assertTrue(
                all(
                    any(
                        q["id"] == f"2859346-{o}.PREDICT" and q["ts"] > boundary * 10**6
                        for q in quotes
                    )
                    for o in (1, 2)
                )
            )
            self.assertEqual(
                [r["id"] for r in rows if r["type"] == "release"],
                ["2859346-1.PREDICT", "2859346-2.PREDICT"],
            )
            late = next(f for f in frames if f["data"]["orderCount"] == 999)
            self.assertFalse(
                any(
                    q["id"].startswith("2859346-")
                    and q["ts"] == late["data"]["updateTimestampMs"] * 10**6
                    for q in quotes
                )
            )
            for q in quotes:
                market, outcome = q["id"].removesuffix(".PREDICT").split("-")
                book = next(
                    f["data"]
                    for f in frames
                    if f["data"]["marketId"] == int(market)
                    and f["data"]["updateTimestampMs"] * 10**6 == q["ts"]
                )
                bid = max(Decimal(str(level[0])) for level in book["bids"])
                ask = min(Decimal(str(level[0])) for level in book["asks"])
                expected = (bid, ask) if outcome == "1" else (1 - ask, 1 - bid)
                self.assertEqual((Decimal(q["bid"]), Decimal(q["ask"])), expected)
            old = "predictOrderbook/2859346"
            for method in ("subscribe", "unsubscribe"):
                self.assertEqual(
                    sum(
                        c.get("method") == method and c.get("params") == [old]
                        for c in session["commands"]
                    ),
                    1,
                )

    def test_terminal_send_failures_retain_intent_until_new_generation(self):
        fixture = json.loads(
            (FIXTURES / "quote_send_failures_synthetic_20261016.json").read_text()
        )
        self.assertEqual(len(fixture["sessions"]), 4)
        self.assertEqual(
            {(s["backend"], s["stage"]) for s in fixture["sessions"]},
            {
                (b, s)
                for b in ("Sockudo", "Tungstenite")
                for s in ("subscribe", "unsubscribe")
            },
        )
        for s in fixture["sessions"]:
            self.assertEqual(s["mainnetWrites"], 0)
            self.assertIn(f"failed to {s['stage']} Predict orderbook", s["error"])
            self.assertIn("client closed", s["error"])
            stable = s["stable"]
            self.assertEqual(stable["retainedQuotes"], 1)
            self.assertEqual(stable["retainedMarketReferences"], 1)
            self.assertTrue(stable["terminalGenerationCancelled"])
            self.assertTrue(stable["newGenerationStartsEmpty"])
            self.assertEqual(stable["newGenerationExplicitSubscribeQuotes"], 1)
            self.assertEqual(stable["newRequestId"], 1)
            if s["stage"] == "unsubscribe":
                self.assertEqual(len(s["frames"]), 2)
                self.assertGreater(
                    s["frames"][1]["data"]["updateTimestampMs"],
                    s["frames"][0]["data"]["updateTimestampMs"],
                )
                self.assertFalse(stable["queuedLateQuoteEmitted"])
