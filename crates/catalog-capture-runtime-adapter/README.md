# catalog-capture-runtime-adapter

Runtime integration for `CatalogCaptureActor`: subscribes to a materialized capture plan,
buffers events, and writes catalog-native Parquet assets readable by PyO3 `ParquetDataCatalog`.

Includes dynamic option-universe refresh and online option-metrics observers.