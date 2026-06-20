use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use nautilus_core::string::conversions::to_snake_case;
use nautilus_model::{
    data::{
        close::InstrumentClose, Bar, CustomData, FundingRateUpdate, HasTsInit, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta, QuoteTick, TradeTick,
    },
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use parquet::basic::Compression;

use crate::config::{CaptureConfig, CompressionKind, LayoutCompatibility};

pub trait CaptureSink<T> {
    fn write_batch(&self, batch: Vec<T>) -> Result<Vec<PathBuf>>;
}

#[derive(Debug)]
pub struct NautilusCatalogSink {
    catalog: ParquetDataCatalog,
    local_root: PathBuf,
    layout_compatibility: LayoutCompatibility,
}

impl NautilusCatalogSink {
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        let compression = match config.compression {
            CompressionKind::Snappy => Compression::SNAPPY,
            CompressionKind::Zstd => Compression::ZSTD(Default::default()),
        };

        let uri = config
            .catalog_uri
            .strip_prefix("file://")
            .unwrap_or(&config.catalog_uri);
        let catalog = ParquetDataCatalog::new(
            Path::new(uri),
            None,
            Some(config.flush_rows),
            Some(compression),
            Some(config.flush_rows),
        );

        Ok(Self {
            catalog,
            local_root: PathBuf::from(uri),
            layout_compatibility: config.layout_compatibility.clone(),
        })
    }

    fn range_from_ts<T: HasTsInit>(data: &[T]) -> (u64, u64) {
        let start = data.first().expect("non-empty batch").ts_init().as_u64();
        let end = data.last().expect("non-empty batch").ts_init().as_u64();
        (start, end)
    }

    pub fn write_quote_ticks(&self, data: Vec<QuoteTick>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(&path, "quote_tick", instrument_id.to_string().as_str())?;
        Ok(path)
    }

    pub fn write_trade_ticks(&self, data: Vec<TradeTick>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(&path, "trade_tick", instrument_id.to_string().as_str())?;
        Ok(path)
    }

    pub fn write_bars(&self, data: Vec<Bar>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let bar_type = data.first().expect("non-empty batch").bar_type;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(&path, "bar", bar_type.to_string().as_str())?;
        Ok(path)
    }

    pub fn write_order_book_deltas(&self, data: Vec<OrderBookDelta>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "order_book_deltas",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_mark_price_updates(&self, data: Vec<MarkPriceUpdate>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "mark_price_update",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_index_price_updates(&self, data: Vec<IndexPriceUpdate>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "index_price_updates",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_funding_rate_updates(&self, data: Vec<FundingRateUpdate>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "funding_rate_update",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_instrument_statuses(&self, data: Vec<InstrumentStatus>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "instrument_status",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_instrument_closes(&self, data: Vec<InstrumentClose>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            "instrument_closes",
            instrument_id.to_string().as_str(),
        )?;
        Ok(path)
    }

    pub fn write_option_greeks(&self, data: Vec<OptionGreeks>) -> Result<PathBuf> {
        let (start, end) = Self::range_from_ts(&data);
        let instrument_id = data.first().expect("non-empty batch").instrument_id;
        let path = self.catalog.write_to_parquet(
            data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(&path, "option_greeks", instrument_id.to_string().as_str())?;
        Ok(path)
    }

    pub fn write_instruments(&self, data: Vec<InstrumentAny>) -> Result<Vec<PathBuf>> {
        let mirrored_specs: Vec<(String, String)> = data
            .iter()
            .map(|instrument| {
                (
                    Self::python_legacy_instrument_prefix(instrument).to_string(),
                    Instrument::id(instrument).to_string(),
                )
            })
            .collect();
        let paths = self.catalog.write_instruments(data)?;
        for (path, (legacy_prefix, instrument_id)) in paths.iter().zip(mirrored_specs.iter()) {
            self.mirror_market_data_path(path, legacy_prefix.as_str(), instrument_id.as_str())?;
        }
        Ok(paths)
    }

    pub fn write_custom_data_batch(&self, data: Vec<CustomData>) -> Result<PathBuf> {
        let first = data.first().expect("non-empty batch");
        let type_name = first.data_type.type_name().to_string();
        let identifier = first.data_type.identifier().map(str::to_string);
        let path = self
            .catalog
            .write_custom_data_batch(data, None, None, Some(false))?;
        self.mirror_custom_data_path(&path, &type_name, identifier.as_deref())?;
        Ok(path)
    }

    fn mirror_market_data_path(
        &self,
        original_path: &Path,
        legacy_prefix: &str,
        identifier: &str,
    ) -> Result<()> {
        if self.layout_compatibility != LayoutCompatibility::RustCanonicalWithPythonLegacyMirror {
            return Ok(());
        }

        let filename = original_path
            .file_name()
            .expect("catalog write returns a file path");
        let legacy_dir = self
            .local_root
            .join("data")
            .join(legacy_prefix)
            .join(identifier);
        let legacy_path = legacy_dir.join(filename);
        let source_path = self.resolve_local_source_path(original_path);
        Self::link_or_copy(&source_path, &legacy_path)
    }

    fn mirror_custom_data_path(
        &self,
        original_path: &Path,
        type_name: &str,
        identifier: Option<&str>,
    ) -> Result<()> {
        if self.layout_compatibility != LayoutCompatibility::RustCanonicalWithPythonLegacyMirror {
            return Ok(());
        }

        let filename = original_path
            .file_name()
            .expect("catalog write returns a file path");
        let legacy_prefix = format!("custom_{}", to_snake_case(type_name));
        let mut legacy_dir = self.local_root.join("data").join(legacy_prefix);
        if let Some(identifier) = identifier {
            legacy_dir = legacy_dir.join(identifier);
        }
        let legacy_path = legacy_dir.join(filename);
        let source_path = self.resolve_local_source_path(original_path);
        Self::link_or_copy(&source_path, &legacy_path)
    }

    fn resolve_local_source_path(&self, original_path: &Path) -> PathBuf {
        if original_path.exists() {
            return original_path.to_path_buf();
        }

        if let Ok(stripped) = original_path.strip_prefix("/") {
            let candidate = self.local_root.join(stripped);
            if candidate.exists() {
                return candidate;
            }
        }

        self.local_root.join(original_path)
    }

    fn link_or_copy(source: &Path, destination: &Path) -> Result<()> {
        if destination.exists() {
            return Ok(());
        }

        fs::create_dir_all(
            destination
                .parent()
                .expect("destination file always has a parent directory"),
        )?;

        match fs::hard_link(source, destination) {
            Ok(()) => Ok(()),
            Err(_) => {
                fs::copy(source, destination)?;
                Ok(())
            }
        }
    }

    fn python_legacy_instrument_prefix(instrument: &InstrumentAny) -> &'static str {
        match instrument {
            InstrumentAny::Betting(_) => "betting_instrument",
            InstrumentAny::BinaryOption(_) => "binary_option",
            InstrumentAny::Cfd(_) => "cfd",
            InstrumentAny::Commodity(_) => "commodity",
            InstrumentAny::CryptoFuture(_) => "crypto_future",
            InstrumentAny::CryptoFuturesSpread(_) => "crypto_futures_spread",
            InstrumentAny::CryptoOption(_) => "crypto_option",
            InstrumentAny::CryptoOptionSpread(_) => "crypto_option_spread",
            InstrumentAny::CryptoPerpetual(_) => "crypto_perpetual",
            InstrumentAny::CurrencyPair(_) => "currency_pair",
            InstrumentAny::Equity(_) => "equity",
            InstrumentAny::FuturesContract(_) => "futures_contract",
            InstrumentAny::FuturesSpread(_) => "futures_spread",
            InstrumentAny::IndexInstrument(_) => "index_instrument",
            InstrumentAny::OptionContract(_) => "option_contract",
            InstrumentAny::OptionSpread(_) => "option_spread",
            InstrumentAny::PerpetualContract(_) => "perpetual_contract",
            InstrumentAny::TokenizedAsset(_) => "tokenized_asset",
        }
    }
}

impl CaptureSink<QuoteTick> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<QuoteTick>) -> Result<Vec<PathBuf>> {
        self.write_quote_ticks(batch).map(|path| vec![path])
    }
}

impl CaptureSink<TradeTick> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<TradeTick>) -> Result<Vec<PathBuf>> {
        self.write_trade_ticks(batch).map(|path| vec![path])
    }
}

impl CaptureSink<Bar> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<Bar>) -> Result<Vec<PathBuf>> {
        self.write_bars(batch).map(|path| vec![path])
    }
}

impl CaptureSink<OrderBookDelta> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<OrderBookDelta>) -> Result<Vec<PathBuf>> {
        self.write_order_book_deltas(batch).map(|path| vec![path])
    }
}

impl CaptureSink<MarkPriceUpdate> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<MarkPriceUpdate>) -> Result<Vec<PathBuf>> {
        self.write_mark_price_updates(batch).map(|path| vec![path])
    }
}

impl CaptureSink<IndexPriceUpdate> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<IndexPriceUpdate>) -> Result<Vec<PathBuf>> {
        self.write_index_price_updates(batch).map(|path| vec![path])
    }
}

impl CaptureSink<FundingRateUpdate> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<FundingRateUpdate>) -> Result<Vec<PathBuf>> {
        self.write_funding_rate_updates(batch)
            .map(|path| vec![path])
    }
}

impl CaptureSink<InstrumentStatus> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<InstrumentStatus>) -> Result<Vec<PathBuf>> {
        self.write_instrument_statuses(batch).map(|path| vec![path])
    }
}

impl CaptureSink<InstrumentClose> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<InstrumentClose>) -> Result<Vec<PathBuf>> {
        self.write_instrument_closes(batch).map(|path| vec![path])
    }
}

impl CaptureSink<OptionGreeks> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<OptionGreeks>) -> Result<Vec<PathBuf>> {
        self.write_option_greeks(batch).map(|path| vec![path])
    }
}

impl CaptureSink<InstrumentAny> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<InstrumentAny>) -> Result<Vec<PathBuf>> {
        self.write_instruments(batch)
    }
}

impl CaptureSink<CustomData> for NautilusCatalogSink {
    fn write_batch(&self, batch: Vec<CustomData>) -> Result<Vec<PathBuf>> {
        self.write_custom_data_batch(batch).map(|path| vec![path])
    }
}
