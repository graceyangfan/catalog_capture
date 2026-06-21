# catalog-capture-runtime-adapter

Runtime integration for `CatalogCaptureActor`: subscribes to a materialized capture plan,
buffers events, and writes Nautilus-native Parquet catalog assets.

Includes dynamic option-universe refresh and online option-metrics observers.