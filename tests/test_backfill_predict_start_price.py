"""Offline regression tests: uv run --with pyarrow python -m unittest discover -s tests -p test_backfill_predict_start_price.py"""

import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/backfill_predict_start_price.py"
spec = importlib.util.spec_from_file_location("backfill_predict_start_price", SCRIPT)
backfill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill)


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def instrument(self, identity, info=None):
        path = self.root / "data/binary_option" / identity / "1-2.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        schema = pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("info", pa.string()),
                pa.field("ts_init", pa.uint64(), nullable=False),
            ],
            metadata={
                b"type_name": b"BinaryOption",
                b"instrument_id": identity.encode(),
            },
        )
        values = None if info is None else json.dumps(info)
        table = pa.Table.from_arrays(
            [
                pa.array([identity, identity]),
                pa.array([values, values], type=pa.string()),
                pa.array([1, 2], type=pa.uint64()),
            ],
            schema=schema,
        )
        pq.write_table(table, path, compression="snappy", row_group_size=1)
        return path

    def test_backfill_preserves_schema_values_books_and_is_idempotent(self):
        paths = [
            self.instrument("42-1.PREDICT", {"other": "keep"}),
            self.instrument("42-2.PREDICT"),
        ]
        foreign = self.instrument("99-1.OTHER")
        foreign_bytes = foreign.read_bytes()
        book = self.root / "data/custom/PredictOrderbookSnapshot/42/book.parquet"
        book.parent.mkdir(parents=True)
        book.write_bytes(b"orderbook must never be opened or changed")
        originals = {path: path.read_bytes() for path in paths}
        calls = []

        def fetch(market_id):
            calls.append(market_id)
            return "81566.995123456789123"

        stats = backfill.backfill(self.root, fetch)
        self.assertEqual(calls, ["42"])
        self.assertEqual(stats["files"], 2)
        self.assertEqual(stats["rows"], 4)
        for path in paths:
            before = pq.ParquetFile(io.BytesIO(originals[path]))
            after = pq.ParquetFile(path)
            self.assertTrue(
                before.schema_arrow.equals(after.schema_arrow, check_metadata=True)
            )
            self.assertEqual(after.num_row_groups, 2)
            self.assertEqual(
                after.metadata.row_group(0).column(0).compression, "SNAPPY"
            )
            self.assertEqual(before.read().drop(["info"]), after.read().drop(["info"]))
            for value in after.read()["info"].to_pylist():
                self.assertEqual(
                    json.loads(value)["start_price"], "81566.995123456789123"
                )
                if path == paths[0]:
                    self.assertEqual(json.loads(value)["other"], "keep")
            self.assertEqual(
                path.with_name(path.name + ".pre-start-price.bak").read_bytes(),
                originals[path],
            )
        self.assertEqual(
            book.read_bytes(), b"orderbook must never be opened or changed"
        )
        self.assertEqual(foreign.read_bytes(), foreign_bytes)
        updated = {path: path.read_bytes() for path in paths}
        self.assertEqual(backfill.backfill(self.root, fetch)["files"], 0)
        self.assertEqual(calls, ["42"])
        self.assertEqual(updated, {path: path.read_bytes() for path in paths})

    def test_dry_run_null_errors_and_existing_value_leave_files_unchanged(self):
        missing = self.instrument("42-1.PREDICT")
        present = self.instrument("43-1.PREDICT", {"start_price": "1"})
        original = missing.read_bytes()
        present_original = present.read_bytes()
        self.assertEqual(backfill.backfill(self.root, lambda _: "2", True)["files"], 1)
        self.assertEqual(missing.read_bytes(), original)
        self.assertEqual(backfill.backfill(self.root, lambda _: None)["unavailable"], 1)

        def failure(_):
            raise ValueError("API failure")

        self.assertEqual(backfill.backfill(self.root, failure)["errors"], 1)
        self.assertEqual(missing.read_bytes(), original)
        self.assertEqual(present.read_bytes(), present_original)
        self.assertFalse(list(self.root.rglob("*.bak")))

    def test_api_parses_decimal_without_float_and_checks_identity(self):
        payload = (
            '{"success":true,"data":{"id":42,"variantData":'
            '{"type":"CRYPTO_UP_DOWN","startPrice":81566.995123456789123}}}'
        )
        with patch.object(backfill, "build_opener") as opener:
            opener.return_value.open.return_value = io.BytesIO(payload.encode())
            self.assertEqual(
                backfill.fetch_start_price("42", "test-key"), "81566.995123456789123"
            )
            request = opener.return_value.open.call_args.args[0]
            self.assertEqual(request.get_header("User-agent"), "catalog-capture")
            opener.return_value.open.return_value = io.BytesIO(payload.encode())
            with self.assertRaisesRegex(ValueError, "mismatch"):
                backfill.fetch_start_price("43", "test-key")

    def test_rewrite_failure_preserves_original(self):
        path = self.instrument("42-1.PREDICT")
        original = path.read_bytes()
        with patch.object(
            backfill.pq, "ParquetWriter", side_effect=OSError("disk full")
        ):
            self.assertEqual(backfill.backfill(self.root, lambda _: "2")["errors"], 1)
        self.assertEqual(path.read_bytes(), original)
        self.assertFalse(list(path.parent.glob("*.tmp")))
        self.assertFalse(list(path.parent.glob("*.bak")))

    def test_json_extension_footer_metadata_and_existing_backup(self):
        path = self.instrument("42-1.PREDICT", {"other": "keep"})
        table = backfill.read_instrument(path)
        index = table.schema.get_field_index("info")
        field = pa.field("info", pa.json_())
        array = pa.ExtensionArray.from_storage(
            pa.json_(), table["info"].combine_chunks()
        )
        table = table.set_column(index, field, array)
        with pq.ParquetWriter(path, table.schema) as writer:
            writer.add_key_value_metadata({"custom_footer": "keep"})
            writer.write_table(table)
        original = path.read_bytes()
        stats = backfill.backfill(self.root, lambda _: "81566.995")
        self.assertEqual(stats["errors"], 0)
        self.assertEqual(stats["rows"], 2)
        reader = pq.ParquetFile(path)
        self.assertEqual(reader.schema_arrow.field("info").type, pa.json_())
        self.assertEqual(reader.metadata.metadata[b"custom_footer"], b"keep")
        self.assertEqual(reader.schema_arrow.metadata[b"type_name"], b"BinaryOption")
        # An interrupted previous run may leave a backup; never overwrite it.
        path.write_bytes(original)
        stats = backfill.backfill(self.root, lambda _: "81566.995")
        self.assertEqual(stats["errors"], 1)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(
            path.with_name(path.name + ".pre-start-price.bak").read_bytes(), original
        )


if __name__ == "__main__":
    unittest.main()
