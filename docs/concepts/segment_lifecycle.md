# Segment lifecycle

Long-running jobs can append into an active temp file and **seal** on a wall-clock
boundary into Nautilus catalog-readable parquet names.

## Modes

| Mode | Behavior |
|------|----------|
| `chunked` (default) | Each flush → `ParquetDataCatalog::write_to_parquet` |
| `segment` | Append under same `data/{type}/{id}/` dirs; seal uses `timestamps_to_filename` |

## Config sketch

```toml
[output.lifecycle]
mode = "segment"

[output.lifecycle.segment]
row_group_rows = 5000

[output.lifecycle.durability]
sync_interval_ms = 1000

[output.lifecycle.seal]
enabled = true
schedule = "06:00"
timezone = "UTC"
interval_secs = 86400
```

## Files (Nautilus layout)

- Active (not catalog-queryable): `data/{type}/{instrument_id}/{open_ts}.parquet.part`
- Sealed: `data/{type}/{instrument_id}/{start}_{end}.parquet` via `timestamps_to_filename`

Keep a single stable `catalog_uri` for the job. Examples:

- `examples/capture.hyperliquid-perp-daily.toml` — perp day files at 06:00 UTC  
- `examples/capture.hyperliquid-hip4-btc-daily.toml` — **HIP-4 daily** instrument
  refresh + **same 06:00 UTC seal** as contract day boundary  
- `examples/operator/*-unattended.toml` — long option-universe runs  

HIP-4: universe poll (which YES/NO) is separate from seal (file day). See
[HIP-4 capture](../how_to/hip4_capture.md).
