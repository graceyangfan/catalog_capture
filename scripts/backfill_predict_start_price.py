#!/usr/bin/env python3
"""Fill missing Predict instrument info.start_price; never open orderbook files.

Requires Python 3.11+ and pyarrow. Stop the recorder before using this script.
"""

import argparse
import json
import os
import re
import shutil
import tempfile
import time
import tomllib
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.request import HTTPRedirectHandler, Request, build_opener

import pyarrow as pa
import pyarrow.parquet as pq

INSTRUMENT_ID = re.compile(r"([1-9][0-9]*)-[12]\.PREDICT")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("API redirect rejected")


def fetch_start_price(market_id, api_key):
    request = Request(
        f"https://api.predict.fun/v1/markets/{market_id}",
        headers={
            "x-api-key": api_key,
            "Accept": "application/json",
            "User-Agent": "catalog-capture",
        },
    )
    with build_opener(NoRedirect).open(request, timeout=30) as response:
        payload = json.load(response, parse_float=Decimal)
    if not isinstance(payload, dict):
        raise TypeError("API response must be an object")
    market = payload.get("data", {})
    if not isinstance(market, dict):
        raise TypeError("API market must be an object")
    if payload.get("success") is not True or str(market.get("id")) != market_id:
        raise ValueError("API response success/market ID mismatch")
    variant = market.get("variantData") or {}
    if not isinstance(variant, dict):
        raise TypeError("API variantData must be an object")
    if variant.get("type") != "CRYPTO_UP_DOWN":
        raise ValueError("market is not CRYPTO_UP_DOWN")
    value = variant.get("startPrice")
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("invalid startPrice")
    price = Decimal(str(value))
    if not price.is_finite() or price < 0:
        raise ValueError("invalid startPrice")
    return format(price, "f")


def read_instrument(path):
    with pq.ParquetFile(path) as reader:
        # Rust embeds metadata in ARROW:schema. read() alone may drop it.
        return reader.read().replace_schema_metadata(reader.schema_arrow.metadata)


def patched_table(table, instrument_id, price=None):
    """Return (table, missing count), preserving all existing values and schema."""
    if table.column("id").to_pylist() != [instrument_id] * table.num_rows:
        raise ValueError("instrument ID differs from its catalog partition")
    index = table.schema.get_field_index("info")
    if index < 0:
        raise ValueError("missing Nautilus info column")
    info_type = table.schema.field(index).type
    is_json = (
        isinstance(info_type, pa.BaseExtensionType)
        and info_type.extension_name == "arrow.json"
    )
    storage_type = info_type.storage_type if is_json else info_type
    if not pa.types.is_string(storage_type):
        raise ValueError("expected Nautilus UTF-8 info column")
    values = table.column("info").to_pylist()
    missing = 0
    for i, value in enumerate(values):
        info = {} if value is None else json.loads(value)
        if not isinstance(info, dict):
            raise TypeError("instrument info must be an object")
        if info.get("start_price") is not None:
            continue
        missing += 1
        if price is not None:
            info["start_price"] = price
            values[i] = json.dumps(info, ensure_ascii=False, separators=(",", ":"))
    if price is not None and missing:
        values = pa.array(values, type=storage_type)
        if is_json:
            values = pa.ExtensionArray.from_storage(info_type, values)
        table = table.set_column(index, table.schema.field(index), values)
    return table, missing


def replace_instrument(path, table):
    """Preserve row groups, compression and schema, with a backup and atomic replace."""
    backup = path.with_name(path.name + ".pre-start-price.bak")
    if backup.exists():
        raise ValueError(f"backup already exists: {backup}; inspect before retrying")
    original = pq.ParquetFile(path)
    compression = {
        original.metadata.row_group(0)
        .column(i)
        .path_in_schema: original.metadata.row_group(0).column(i).compression.lower()
        for i in range(original.metadata.num_columns)
    }
    fd, temporary = tempfile.mkstemp(
        prefix=".start-price-", suffix=".tmp", dir=path.parent
    )
    os.close(fd)
    temporary = Path(temporary)
    try:
        with pq.ParquetWriter(
            temporary, table.schema, compression=compression
        ) as writer:
            # Rust's writer also stores catalog metadata in the Parquet footer,
            # outside Arrow schema metadata. Keep it for native catalog readers.
            metadata = {
                key: value
                for key, value in (original.metadata.metadata or {}).items()
                if key != b"ARROW:schema"
            }
            writer.add_key_value_metadata(metadata)
            offset = 0
            for group in range(original.num_row_groups):
                count = original.metadata.row_group(group).num_rows
                writer.write_table(table.slice(offset, count), row_group_size=count)
                offset += count
        restored = read_instrument(temporary)
        if not restored.equals(table, check_metadata=True):
            raise ValueError("rewritten instrument failed readback verification")
        shutil.copymode(path, temporary)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        # Exclusive creation keeps an earlier original from being overwritten.
        with backup.open("xb") as output, path.open("rb") as source:
            shutil.copyfileobj(source, output)
            output.flush()
            os.fsync(output.fileno())
        shutil.copystat(path, backup)
        original.close()
        os.replace(temporary, path)
    finally:
        original.close()
        temporary.unlink(missing_ok=True)


def backfill(catalog, fetch, dry_run=False):
    root = catalog / "data" / "binary_option"
    if not root.is_dir():
        raise ValueError(f"missing instrument directory: {root}")
    markets = {}
    for folder in sorted(root.iterdir()):
        match = INSTRUMENT_ID.fullmatch(folder.name)
        if folder.is_dir() and match:
            markets.setdefault(match[1], []).extend(sorted(folder.glob("*.parquet")))
    stats = {
        "markets": len(markets),
        "files": 0,
        "rows": 0,
        "unavailable": 0,
        "errors": 0,
    }
    for market_id, paths in markets.items():
        try:
            pending = []
            for path in paths:
                # ParquetFile avoids partition inference changing the catalog schema.
                table = read_instrument(path)
                _, count = patched_table(table, path.parent.name)
                if count:
                    pending.append(path)
            if not pending:
                continue
            price = fetch(market_id)
            if price is None:
                stats["unavailable"] += 1
                print(f"{market_id}: API startPrice is null; unchanged")
                continue
            for path in pending:
                table = read_instrument(path)
                table, count = patched_table(table, path.parent.name, price)
                if not dry_run:
                    replace_instrument(path, table)
                stats["files"] += 1
                stats["rows"] += count
            print(
                f"{market_id}: start_price={price}; {len(pending)} instrument files"
                + (" (dry run)" if dry_run else "")
            )
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            InvalidOperation,
            pa.ArrowException,
        ) as error:
            stats["errors"] += 1
            print(f"{market_id}: {type(error).__name__}: {error}")
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--catalog", type=Path, required=True, help="local catalog root"
    )
    parser.add_argument(
        "--credentials-file",
        type=Path,
        default=Path(os.environ.get("PREDICT_CREDENTIALS_FILE", "env/predictfun.toml")),
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="fetch and report without writing"
    )
    args = parser.parse_args()
    with args.credentials_file.open("rb") as stream:
        api_key = tomllib.load(stream)["api_key"]
    if not isinstance(api_key, str) or not api_key.strip():
        parser.error("credentials file must contain a nonempty api_key")

    def fetch(market_id):
        try:
            return fetch_start_price(market_id, api_key)
        finally:
            time.sleep(0.25)

    stats = backfill(args.catalog, fetch, args.dry_run)
    print(json.dumps(stats, sort_keys=True))
    return int(bool(stats["errors"] or stats["unavailable"]))


if __name__ == "__main__":
    raise SystemExit(main())
