from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Iterable

_IMPORT = Path(__file__).resolve().parent / "nautilus_import.py"
_spec = importlib.util.spec_from_file_location("nautilus_import", _IMPORT)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
_mod.ensure_nautilus_trader_path()

from nautilus_trader.persistence.catalog import ParquetDataCatalog  # noqa: E402


def load_catalog(catalog_dir: Path) -> ParquetDataCatalog:
    assert catalog_dir.exists(), f"catalog_dir does not exist: {catalog_dir}"
    return ParquetDataCatalog(str(catalog_dir))


def assert_monotonic_ts_init(rows: list, label: str) -> None:
    for prev, curr in zip(rows, rows[1:]):
        assert curr.ts_init >= prev.ts_init, (
            f"{label} ts_init not monotonic: {prev.ts_init} -> {curr.ts_init}"
        )


def require_instrument(catalog: ParquetDataCatalog, instrument_id: str):
    instruments = catalog.instruments(instrument_ids=[instrument_id])
    assert instruments, f"expected instrument metadata for {instrument_id}"
    assert str(instruments[0].id) == instrument_id
    return instruments


def print_probe_header(title: str, catalog_dir: Path, extra: Iterable[tuple[str, object]]) -> None:
    print(title)
    print(f"Catalog dir: {catalog_dir}")
    for key, value in extra:
        print(f"{key}: {value}")
