"""Focused negative and exact-number controls for the opt-in Bullet evidence tool."""

import json
from pathlib import Path
import tempfile
import unittest

from bullet_soak_evidence import compare, prepare_recoveries


class BulletSoakEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.catalog = self.root / "catalog"
        self.metadata = self.catalog / "metadata"
        self.metadata.mkdir(parents=True)
        self.source = self.root / "source"
        self.source.mkdir()
        self.log = self.root / "native.log"
        self.log.write_text("")

    def write_rows(self, path, rows):
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))

    def test_recovery_rejects_enqueued_command_without_accepted_endpoint(self):
        self.write_rows(
            self.metadata / "soak_reconnect_requests.jsonl", [{"ts_init_ns": 1}]
        )
        self.log.write_text(
            "2026-11-18T12:00:00Z Rejected socket reconnect request for client bullet_main endpoint public-data: UnknownEndpoint\n"
        )
        with self.assertRaisesRegex(AssertionError, "accepted"):
            prepare_recoveries(self.catalog, self.log)
        self.assertFalse((self.metadata / "soak_recovery_epochs.json").exists())

    def test_recovery_requires_all_three_new_epoch_acknowledgements(self):
        request_ns = 1795003200000000000
        self.write_rows(
            self.metadata / "soak_reconnect_requests.jsonl",
            [{"ts_init_ns": request_ns}],
        )
        lines = []
        for topic in ("depth20", "aggTrade", "markPrice"):
            lines.append(
                f"2026-11-18T11:59:00.000000000Z Bullet Subscribe confirmed: topic=BTC-USD@{topic}, epoch=0"
            )
        lines.extend(
            [
                "2026-11-18T12:00:00.000000001Z Requested socket reconnect for client bullet_main endpoint public-data",
                "2026-11-18T12:00:01.000000002Z Bullet epoch 1 reconnected",
            ]
        )
        for topic in ("depth20", "aggTrade"):
            lines.append(
                f"2026-11-18T12:00:01.100000000Z Bullet Subscribe confirmed: topic=BTC-USD@{topic}, epoch=1"
            )
        self.log.write_text("\n".join(lines))
        with self.assertRaisesRegex(AssertionError, "ACKs"):
            prepare_recoveries(self.catalog, self.log)
        self.log.write_text(
            "\n".join(
                lines
                + [
                    "2026-11-18T12:00:01.100000001Z Bullet Subscribe confirmed: topic=BTC-USD@markPrice, epoch=1"
                ]
            )
        )
        result = prepare_recoveries(self.catalog, self.log)
        self.assertEqual(result[0]["reset_ns"], request_ns + 1000000002)
        self.assertEqual(len(result[0]["confirmed_topics"]), 3)

    def test_compare_exact_decimals_and_native_aggressor_names(self):
        native = {"settled": [{"ts_event": 1000, "ts_init": 2000, "rate": "-0.0001"}]}
        (self.metadata / "soak_native_readback.json").write_text(json.dumps(native))
        self.write_rows(
            self.metadata / "soak_native_market_rows.jsonl",
            [
                {
                    "type": "TradeTick",
                    "trade_id": "9007199254740993",
                    "price": "100.00000000",
                    "size": "0.0001",
                    "ts_event": 10000,
                    "aggressor_side": "SELL",
                },
                {"type": "MarkPriceUpdate", "ts_event": 10000, "value": "100.00000000"},
                {
                    "type": "IndexPriceUpdate",
                    "ts_event": 10000,
                    "value": "101.12345679",
                },
            ],
        )
        self.write_rows(
            self.source / "ws.jsonl",
            [
                {
                    "body": {
                        "e": "aggTrade",
                        "a": 9007199254740993,
                        "p": "100",
                        "q": "0.0001",
                        "T": 10,
                        "m": True,
                    }
                },
                {
                    "body": {
                        "e": "markPriceUpdate",
                        "E": 10,
                        "p": "100.000000005",
                        "i": "101.123456789",
                    }
                },
            ],
        )
        self.write_rows(
            self.source / "http.jsonl",
            [
                {
                    "status": 200,
                    "path": "/fapi/v1/fundingRate?symbol=BTC-USD",
                    "received_ns": 3000,
                    "body": [{"fundingTime": 1, "fundingRate": "-0.0008"}],
                },
                {"error": "synthetic observer timeout"},
            ],
        )
        result = json.loads(compare(self.catalog, self.source, self.log).read_text())
        self.assertEqual(result["matched_source_trades"], 1)
        self.assertEqual(result["matched_source_price_pairs"], 1)
        self.assertEqual(result["source_http_errors"], 1)
        self.assertEqual(
            result["settled_source_comparisons"][0]["hourly_gross_rate"], "-0.0001"
        )
        (self.metadata / "soak_native_market_rows.jsonl").write_text(
            (self.metadata / "soak_native_market_rows.jsonl")
            .read_text()
            .replace('"size": "0.0001"', '"size": "0.0002"')
        )
        with self.assertRaisesRegex(AssertionError, "trade wire/Native mismatch"):
            compare(self.catalog, self.source, self.log)


if __name__ == "__main__":
    unittest.main()
