# Custom Data Contract

## Goal

Define how this project should treat adapter-emitted custom data so that:

- capture remains catalog-native
- adapter-emitted research signals can be preserved without forcing everything into built-in market-data families
- downstream strategy and ML pipelines have a stable naming contract

## Project stance

This project should treat custom data as a first-class pass-through path for types that venue
adapters already emit.

Built-in market-data families remain the default for:

- quotes
- trades
- bars
- book deltas
- mark prices
- index prices
- funding rates
- instrument state families
- option greeks

Custom data should be used when the adapter emits information that is:

- research-critical
- not naturally expressible as an existing built-in data family
- specific to a venue, derivatives product, or proprietary research feed

This project should not invent new research schemas as part of its default surface.

If we later add a custom family that adapters do not already emit, that should be a separate
design discussion covering:

- the canonical Rust type
- adapter ownership
- PyO3/readback expectations
- long-term catalog compatibility

## Naming guidance

Custom data names should be:

- stable
- research-meaningful
- identical to the type names already emitted by venue adapters

Do not rename or alias adapter-emitted custom data inside this project. The writer should capture
the `DataType` exactly as the adapter presents it.

## Identifier guidance

When the custom data is associated with one instrument or hedge leg, use:

- `identifier = instrument_id`

When the custom data is series-level or venue-level, choose the narrowest stable identifier that still matches the research retrieval pattern.

Examples:

- instrument-specific custom data:
  - `ETHUSDT-PERP.BINANCE`
- option series snapshot:
  - a series identifier or normalized chain key
- venue-wide volatility index:
  - a stable venue/index identifier such as `BTC_USD.DERIBIT`

## Metadata guidance

Metadata should be used for low-cardinality semantic qualifiers, not as an uncontrolled dump.

Recommended metadata keys include:

- `family`
- `source`
- `venue`
- `market_type`
- `aggregation`

Metadata should come from the adapter-emitted `DataType` contract where possible. This project
should not fabricate metadata just to make local examples look richer.

## CLI contract

CLI `custom_data` configuration should remain declarative.

Example shape:

```toml
[[capture.custom_data]]
type_name = "<adapter-emitted-type-name>"
identifier = "ETHUSDT-PERP.BINANCE"
```

This allows the runtime to subscribe to the matching `DataType` without leaking adapter protocol details into the capture project.

## Validation levels

Custom data validation should be split into two levels.

### Level 1: catalog discoverability

Validate:

- parquet files are written
- custom type appears in `list_data_types()`
- catalog layout is stable and queryable by type name

### Level 2: typed Python readback

Validate:

- a Python or PyO3 class is registered for the custom type
- dynamic query returns typed objects

Level 2 requires a Python-visible class. Not every research type needs that on day one.

That means this project can productively capture adapter-native custom families even when the
corresponding Python-visible class is not yet packaged, as long as catalog discoverability remains
intact.
