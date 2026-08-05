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

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Mutex,
};

// Path is used for catalog URI roots.

use anyhow::{Context, Result};
use nautilus_core::UnixNanos;
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
    config::{CaptureConfig, CompressionKind},
    lifecycle::{SegmentCaptureSink, SegmentCustomDataSink},
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

/// Flush-driven catalog writer for **instruments** (and other non-segment reference paths).
///
/// Instruments stay chunked (definitions are sparse). Custom data uses
/// [`CustomDataCatalogSink`] so segment mode gets daily `.part` + seal.
pub type ChunkedCatalogSink = NautilusCatalogSink;

pub fn chunked_catalog_sink_from_config(config: &CaptureConfig) -> Result<NautilusCatalogSink> {
    NautilusCatalogSink::from_config(config)
}

/// Custom-data sink: **segment** when `output.lifecycle.mode = segment` (append
/// `data/custom/{Type}/…/*.parquet.part`, seal at schedule), else chunked catalog files.
#[derive(Debug)]
pub enum CustomDataCatalogSink {
    Chunked(NautilusCatalogSink),
    Segment(SegmentCustomDataSink),
}

impl CustomDataCatalogSink {
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        if config.lifecycle.is_segment_mode() {
            Ok(Self::Segment(SegmentCustomDataSink::from_config(config)?))
        } else {
            // Non-fatal: same text as validate/run advisories — smoke OK, prod should use segment.
            log::warn!("{}", crate::advisories::CHUNKED_CUSTOM_DATA_ADVISORY);
            Ok(Self::Chunked(NautilusCatalogSink::from_config(config)?))
        }
    }

    #[must_use]
    pub fn is_segment_mode(&self) -> bool {
        matches!(self, Self::Segment(_))
    }
}

impl CaptureSink<CustomData> for CustomDataCatalogSink {
    fn write_batch(
        &mut self,
        partition_key: &str,
        batch: Vec<CustomData>,
    ) -> Result<Vec<PathBuf>> {
        match self {
            Self::Chunked(sink) => sink.write_custom_data_batch(batch).map(|path| vec![path]),
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
        CustomDataCatalogSink::is_segment_mode(self)
    }
}

pub fn custom_data_catalog_sink_from_config(config: &CaptureConfig) -> Result<CustomDataCatalogSink> {
    CustomDataCatalogSink::from_config(config)
}

#[derive(Debug)]
pub struct NautilusCatalogSink {
    catalog: ParquetDataCatalog,
    /// **Chunked-mode only.** Segment custom data uses [`SegmentCustomDataSink`] and
    /// never hits this path.
    ///
    /// When `mode = chunked`, snapshot custom (e.g. BookSummary) may flush many rows
    /// with the same `ts_init`; catalog closed intervals reject touching ranges, so
    /// we advance file-name intervals without mutating row timestamps.
    custom_last_end_ns: Mutex<HashMap<String, u64>>,
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
            custom_last_end_ns: Mutex::new(HashMap::new()),
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
        let path = self.catalog.write_to_parquet(
            &data,
            Some(start.into()),
            Some(end.into()),
            Some(false),
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
        self.catalog.write_instruments(data)
    }

    /// Chunked-only: shift file interval so `prev_end < next_start`.
    fn disjoint_file_interval(last_end: Option<u64>, data_start: u64, data_end: u64) -> (u64, u64) {
        let mut start = data_start;
        let mut end = data_end.max(data_start);
        if let Some(prev_end) = last_end {
            if start <= prev_end {
                start = prev_end.saturating_add(1);
            }
            if end < start {
                end = start;
            }
        }
        (start, end)
    }

    fn custom_partition_key(data: &[CustomData]) -> Result<(String, String, Option<String>)> {
        let first = data
            .first()
            .context("cannot derive custom-data partition key from empty batch")?;
        let type_name = first.data.type_name().to_string();
        let identifier = first.data_type.identifier().map(str::to_string);
        let key = match identifier.as_deref() {
            Some(id) => format!("{type_name}/{id}"),
            None => type_name.clone(),
        };
        Ok((key, type_name, identifier))
    }

    fn custom_data_ts_range(data: &[CustomData]) -> Result<(u64, u64)> {
        let mut iter = data.iter().map(|item| item.ts_init().as_u64());
        let Some(first) = iter.next() else {
            anyhow::bail!("cannot derive timestamp range from empty custom-data batch");
        };
        let (min_ts, max_ts) = iter.fold((first, first), |(min_ts, max_ts), ts| {
            (min_ts.min(ts), max_ts.max(ts))
        });
        Ok((min_ts, max_ts))
    }

    fn seed_custom_last_end(&self, type_name: &str, identifier: Option<&str>) -> Option<u64> {
        let directory = self
            .catalog
            .make_path_custom_data(type_name, identifier)
            .ok()?;
        let intervals = self.catalog.get_directory_intervals(&directory).ok()?;
        intervals.into_iter().map(|(_, end)| end).max()
    }

    pub fn write_custom_data_batch(&self, data: Vec<CustomData>) -> Result<PathBuf> {
        if data.is_empty() {
            return Ok(PathBuf::new());
        }

        let (key, type_name, identifier) = Self::custom_partition_key(&data)?;
        let (data_start, data_end) = Self::custom_data_ts_range(&data)?;

        let (start, end) = {
            let mut last_ends = self
                .custom_last_end_ns
                .lock()
                .map_err(|_| anyhow::anyhow!("custom_last_end_ns mutex poisoned"))?;
            if !last_ends.contains_key(&key) {
                if let Some(seed) =
                    self.seed_custom_last_end(&type_name, identifier.as_deref())
                {
                    last_ends.insert(key.clone(), seed);
                }
            }
            let last_end = last_ends.get(&key).copied();
            Self::disjoint_file_interval(last_end, data_start, data_end)
        };

        let path = self.catalog.write_custom_data_batch(
            data,
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(end)),
            Some(false),
        )?;

        // Only advance watermark after a successful write so failed attempts can retry.
        // Empty path means catalog skipped an empty batch.
        if !path.as_os_str().is_empty() {
            let mut last_ends = self
                .custom_last_end_ns
                .lock()
                .map_err(|_| anyhow::anyhow!("custom_last_end_ns mutex poisoned"))?;
            last_ends.insert(key, end);
        }

        Ok(path)
    }
}

#[cfg(test)]
mod custom_interval_tests {
    use super::NautilusCatalogSink;

    #[test]
    fn disjoint_file_interval_advances_past_previous_end() {
        assert_eq!(
            NautilusCatalogSink::disjoint_file_interval(None, 100, 100),
            (100, 100)
        );
        // Same-ts snapshot split across flushes must not touch previous end.
        assert_eq!(
            NautilusCatalogSink::disjoint_file_interval(Some(100), 100, 100),
            (101, 101)
        );
        // Contiguous multi-poll batch that would touch (prev_end == next_start).
        assert_eq!(
            NautilusCatalogSink::disjoint_file_interval(Some(200), 200, 300),
            (201, 300)
        );
        // Already strictly after previous end — leave data range intact.
        assert_eq!(
            NautilusCatalogSink::disjoint_file_interval(Some(100), 150, 180),
            (150, 180)
        );
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
