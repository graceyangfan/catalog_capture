# Segment lifecycle

Long-running jobs can append into an active `.part.parquet` and **seal** on a
wall-clock boundary into catalog-readable files.

## Modes

| Mode | Behavior |
|------|----------|
| `chunked` (default) | Each flush writes a new immutable Parquet file |
| `segment` | Append row groups to `.part` until seal rename |

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

## Files

- Active: `data/{family}/{instrument_id}/{open_ts}.part.parquet` (not queried)
- Sealed: `data/{family}/{instrument_id}/{start}_{end}.parquet` (catalog-readable)

Keep a single stable `catalog_uri` for the job. Examples:
`examples/capture.hyperliquid-perp-daily.toml`,
`examples/operator/*-unattended.toml`.
