// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::path::{Path, PathBuf};

use anyhow::Result;
use nautilus_model::{
    data::{
        close::InstrumentClose, Bar, CatalogPathPrefix, CustomData, FundingRateUpdate, HasTsInit,
        IndexPriceUpdate, InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta,
        QuoteTick, TradeTick,
    },
    instruments::InstrumentAny,
};
use nautilus_persistence::backend::catalog::{
    CatalogPathPrefix as PersistenceCatalogPathPrefix, ParquetDataCatalog,
};
use nautilus_serialization::arrow::{ArrowSchemaProvider, EncodeToRecordBatch};
use parquet::basic::Compression;
use serde::Serialize;

use crate::{
    catalog_layout::{
        instrument_identifier, instrument_legacy_prefix, legacy_market_data_prefix,
        mirror_custom_data_path, mirror_market_data_path,
    },
    config::{CaptureConfig, CompressionKind, LayoutCompatibility},
    lifecycle::SegmentCaptureSink,
    runtime::FlushResult,
};

pub trait CaptureSink<T> {
    fn write_batch(&mut self, _partition_key: &str, batch: Vec<T>) -> Result<Vec<PathBuf>>;

    fn on_tick(&mut self, _now_ns: u64) -> Result<FlushResult> {
        Ok(FlushResult::default())
    }

    fn seal_all(&mut self) -> Result<FlushResult> {
        Ok(FlushResult::default())
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all()
    }

    fn is_segment_mode(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub enum CatalogSink<T> {
    Chunked(NautilusCatalogSink),
    Segment(SegmentCaptureSink<T>),
}

impl<T> CatalogSink<T>
where
    T: HasTsInit
        + EncodeToRecordBatch
        + CatalogPathPrefix
        + PersistenceCatalogPathPrefix
        + ArrowSchemaProvider
        + Serialize
        + Clone,
{
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        if config.lifecycle.is_segment_mode() {
            Ok(Self::Segment(SegmentCaptureSink::from_config(config)?))
        } else {
            Ok(Self::Chunked(NautilusCatalogSink::from_config(config)?))
        }
    }
}

impl<T> CatalogSink<T> {
    #[must_use]
    pub fn is_segment_mode(&self) -> bool {
        matches!(self, Self::Segment(_))
    }
}

impl<T> CaptureSink<T> for CatalogSink<T>
where
    T: HasTsInit
        + EncodeToRecordBatch
        + CatalogPathPrefix
        + PersistenceCatalogPathPrefix
        + ArrowSchemaProvider
        + Serialize
        + Clone,
{
    fn write_batch(&mut self, partition_key: &str, batch: Vec<T>) -> Result<Vec<PathBuf>> {
        match self {
            Self::Chunked(sink) => sink.write_encoded_batch(batch).map(|path| vec![path]),
            Self::Segment(sink) => sink.write_batch_mut(partition_key, batch),
        }
    }

    fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.on_tick(now_ns),
        }
    }

    fn seal_all(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_all(),
        }
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_all_for_shutdown(),
        }
    }

    fn is_segment_mode(&self) -> bool {
        CatalogSink::is_segment_mode(self)
    }
}

/// Flush-driven catalog writer for reference families (instruments, custom data).
///
/// These families use heterogeneous catalog paths and always remain chunked even when
/// market-data capture runs in segment lifecycle mode.
pub type ChunkedCatalogSink = NautilusCatalogSink;

pub fn chunked_catalog_sink_from_config(config: &CaptureConfig) -> Result<NautilusCatalogSink> {
    NautilusCatalogSink::from_config(config)
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

    fn range_from_ts<T: HasTsInit>(data: &[T]) -> Result<(u64, u64)> {
        let (Some(start), Some(end)) = (data.first(), data.last()) else {
            anyhow::bail!("cannot derive timestamp range from empty batch");
        };
        Ok((start.ts_init().as_u64(), end.ts_init().as_u64()))
    }

    pub fn write_encoded_batch<T>(&self, data: Vec<T>) -> Result<PathBuf>
    where
        T: HasTsInit
            + EncodeToRecordBatch
            + CatalogPathPrefix
            + PersistenceCatalogPathPrefix
            + Serialize
            + Clone,
    {
        let (start, end) = Self::range_from_ts(&data)?;
        let metadata = EncodeToRecordBatch::chunk_metadata(&data);
        let identifier = metadata
            .get("instrument_id")
            .or_else(|| metadata.get("bar_type"))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("batch metadata missing instrument_id or bar_type"))?;
        let path = self.catalog.write_to_parquet(
            &data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
        )?;
        self.mirror_market_data_path(
            &path,
            legacy_market_data_prefix(<T as CatalogPathPrefix>::path_prefix()),
            identifier.as_str(),
        )?;
        Ok(path)
    }

    fn write_encoded_paths<T>(&self, batch: Vec<T>) -> Result<Vec<PathBuf>>
    where
        T: HasTsInit
            + EncodeToRecordBatch
            + CatalogPathPrefix
            + PersistenceCatalogPathPrefix
            + Serialize
            + Clone,
    {
        self.write_encoded_batch(batch).map(|path| vec![path])
    }

    pub fn write_instruments(&self, data: Vec<InstrumentAny>) -> Result<Vec<PathBuf>> {
        let mirrored_specs: Vec<(String, String)> = data
            .iter()
            .map(|instrument| {
                (
                    instrument_legacy_prefix(instrument),
                    instrument_identifier(instrument),
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
        mirror_custom_data_path(
            &self.local_root,
            self.layout_compatibility.clone(),
            &path,
            &type_name,
            identifier.as_deref(),
        )?;
        Ok(path)
    }

    fn mirror_market_data_path(
        &self,
        original_path: &Path,
        legacy_prefix: &str,
        identifier: &str,
    ) -> Result<()> {
        mirror_market_data_path(
            &self.local_root,
            self.layout_compatibility.clone(),
            original_path,
            legacy_prefix,
            identifier,
        )
    }
}

impl CaptureSink<QuoteTick> for NautilusCatalogSink {
    fn write_batch(&mut self, _partition_key: &str, batch: Vec<QuoteTick>) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<TradeTick> for NautilusCatalogSink {
    fn write_batch(&mut self, _partition_key: &str, batch: Vec<TradeTick>) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<Bar> for NautilusCatalogSink {
    fn write_batch(&mut self, _partition_key: &str, batch: Vec<Bar>) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<OrderBookDelta> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<OrderBookDelta>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<MarkPriceUpdate> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<MarkPriceUpdate>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<IndexPriceUpdate> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<IndexPriceUpdate>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<FundingRateUpdate> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<FundingRateUpdate>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<InstrumentStatus> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<InstrumentStatus>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<InstrumentClose> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<InstrumentClose>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<OptionGreeks> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<OptionGreeks>,
    ) -> Result<Vec<PathBuf>> {
        self.write_encoded_paths(batch)
    }
}

impl CaptureSink<InstrumentAny> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<InstrumentAny>,
    ) -> Result<Vec<PathBuf>> {
        self.write_instruments(batch)
    }
}

impl CaptureSink<CustomData> for NautilusCatalogSink {
    fn write_batch(
        &mut self,
        _partition_key: &str,
        batch: Vec<CustomData>,
    ) -> Result<Vec<PathBuf>> {
        self.write_custom_data_batch(batch).map(|path| vec![path])
    }
}
