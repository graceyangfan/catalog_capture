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
        Bar, CustomData, FundingRateUpdate, HasTsInit, IndexPriceUpdate, InstrumentStatus,
        MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
        close::InstrumentClose,
    },
    enums::RecordFlag,
    instruments::InstrumentAny,
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::HasCatalogDataType,
    common::paths::CatalogPathPrefix,
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

    fn write_initialization(&mut self, partition_key: &str, data: T) -> Result<FlushResult> {
        let files = self.write_batch(partition_key, vec![data])?;
        Ok(FlushResult {
            files,
            rows: 1,
            bytes: 0,
        })
    }

    fn on_tick(&mut self, _now_ns: u64) -> Result<FlushResult> {
        Ok(FlushResult::default())
    }

    fn seal_all(&mut self) -> Result<FlushResult> {
        Ok(FlushResult::default())
    }

    fn seal_partition(&mut self, _key: &str) -> Result<FlushResult> {
        Ok(FlushResult::default())
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all()
    }

    fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
        match key {
            Some(key) => self.seal_partition(key),
            None => self.seal_all(),
        }
    }

    fn active_writers(&self) -> usize {
        0
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
        + HasCatalogDataType
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
        + HasCatalogDataType
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

    fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_partition(key),
        }
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_all_for_shutdown(),
        }
    }

    fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.break_continuity(key),
        }
    }

    fn active_writers(&self) -> usize {
        match self {
            Self::Chunked(_) => 0,
            Self::Segment(sink) => sink.active_writers(),
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
    fn write_batch(&mut self, partition_key: &str, batch: Vec<CustomData>) -> Result<Vec<PathBuf>> {
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

    fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_partition(key),
        }
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment(sink) => sink.seal_all_for_shutdown(),
        }
    }

    fn active_writers(&self) -> usize {
        match self {
            Self::Chunked(_) => 0,
            Self::Segment(sink) => sink.active_writers(),
        }
    }

    fn is_segment_mode(&self) -> bool {
        CustomDataCatalogSink::is_segment_mode(self)
    }
}

pub fn custom_data_catalog_sink_from_config(
    config: &CaptureConfig,
) -> Result<CustomDataCatalogSink> {
    CustomDataCatalogSink::from_config(config)
}

/// Catalog sink for grouped order-book deltas.
///
/// The ingress queue carries one `OrderBookDeltas` item per venue message so a
/// snapshot cannot be partially admitted. Segment mode expands each message in
/// bounded chunks into the ordinary `order_book_deltas` parquet stream. The
/// on-disk contract therefore remains unchanged while the queue avoids one
/// allocation per delta. Source fields and equal-timestamp message order are
/// retained. A clock rollback is an explicit continuity failure, never a rewrite.
#[derive(Debug)]
pub enum BookDeltaCatalogSink {
    Chunked(NautilusCatalogSink),
    Segment {
        sink: SegmentCaptureSink<OrderBookDelta>,
        boundary: HashMap<String, BookDeltaBoundaryState>,
    },
}

const BOOK_DELTA_WRITE_CHUNK_ROWS: usize = 4_096;

/// Internal segment handoff state carried by [`BookDeltaCatalogSink`].
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct BookDeltaBoundaryState {
    /// An uncertain write must not start a later segment with unchecked increments.
    needs_snapshot: bool,
    /// Last timestamp emitted for this instrument's segment stream.
    last_ts_init_ns: Option<u64>,
}

impl BookDeltaCatalogSink {
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        if config.lifecycle.is_segment_mode() {
            Ok(Self::Segment {
                sink: SegmentCaptureSink::from_config(config)?,
                boundary: HashMap::new(),
            })
        } else {
            Ok(Self::Chunked(NautilusCatalogSink::from_config(config)?))
        }
    }

    #[must_use]
    pub fn is_segment_mode(&self) -> bool {
        matches!(self, Self::Segment { .. })
    }

    fn prepare_segment_batch(
        &mut self,
        partition_key: &str,
        grouped: OrderBookDeltas,
    ) -> Result<Option<Vec<OrderBookDelta>>> {
        let Self::Segment { boundary, .. } = self else {
            unreachable!("segment batch")
        };
        let state = boundary.entry(partition_key.to_owned()).or_default();
        let is_snapshot = RecordFlag::F_SNAPSHOT.matches(grouped.flags);
        if state.needs_snapshot && !is_snapshot {
            return Ok(None);
        }
        let raw = grouped.ts_init.as_u64();
        if !state.needs_snapshot && state.last_ts_init_ns.is_some_and(|last| raw < last) {
            anyhow::bail!(
                "order-book arrival clock moved backwards for {partition_key}; a new replay window is required"
            );
        }
        state.last_ts_init_ns = Some(raw);
        if is_snapshot {
            state.needs_snapshot = false;
        }
        Ok(Some(grouped.deltas))
    }
}

impl CaptureSink<OrderBookDeltas> for BookDeltaCatalogSink {
    fn write_batch(
        &mut self,
        partition_key: &str,
        batch: Vec<OrderBookDeltas>,
    ) -> Result<Vec<PathBuf>> {
        let result = (|| match self {
            Self::Chunked(sink) => {
                let mut deltas = Vec::new();
                for grouped in batch {
                    deltas.extend(grouped.deltas);
                }
                if deltas.is_empty() {
                    Ok(Vec::new())
                } else {
                    sink.write_encoded_paths(deltas)
                }
            }
            Self::Segment { .. } => {
                let mut paths = Vec::new();
                for grouped in batch {
                    let is_snapshot = RecordFlag::F_SNAPSHOT.matches(grouped.flags);
                    if is_snapshot
                        && let Self::Segment { sink, boundary } = self
                        && boundary
                            .get(partition_key)
                            .is_some_and(|state| state.last_ts_init_ns.is_some())
                    {
                        // A venue reinitialization or local recovery is a new research window.
                        sink.seal_partition(partition_key)?;
                        boundary.remove(partition_key);
                    }
                    let Some(rows) = self.prepare_segment_batch(partition_key, grouped)? else {
                        continue;
                    };
                    let Self::Segment { sink, .. } = self else {
                        unreachable!("segment batch preparation changed sink mode")
                    };
                    let mut chunk = Vec::with_capacity(BOOK_DELTA_WRITE_CHUNK_ROWS.min(rows.len()));
                    for delta in rows {
                        chunk.push(delta);
                        if chunk.len() == BOOK_DELTA_WRITE_CHUNK_ROWS {
                            paths.extend(sink.write_batch_mut(partition_key, chunk)?);
                            chunk = Vec::with_capacity(BOOK_DELTA_WRITE_CHUNK_ROWS);
                        }
                    }
                    if !chunk.is_empty() {
                        paths.extend(sink.write_batch_mut(partition_key, chunk)?);
                    }
                }
                Ok(paths)
            }
        })();
        if result.is_err()
            && let Self::Segment { boundary, .. } = self
        {
            boundary
                .entry(partition_key.to_string())
                .or_default()
                .needs_snapshot = true;
        }
        result
    }

    fn write_initialization(&mut self, key: &str, data: OrderBookDeltas) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => {
                let rows = data.deltas.len();
                let files = self.write_batch(key, vec![data])?;
                Ok(FlushResult {
                    files,
                    rows,
                    bytes: 0,
                })
            }
            Self::Segment { sink, boundary } => {
                if boundary.get(key).is_some_and(|state| state.needs_snapshot) {
                    return Ok(FlushResult::default());
                }
                let rows = data.deltas.len();
                let files = sink.write_initialization(key, data.deltas)?;
                let bytes = files
                    .iter()
                    .filter_map(|path| std::fs::metadata(path).ok())
                    .map(|meta| meta.len())
                    .sum();
                Ok(FlushResult { files, rows, bytes })
            }
        }
    }

    fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.on_tick(now_ns),
        }
    }

    fn seal_all(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.seal_all(),
        }
    }

    fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, boundary } => {
                let result = sink.seal_partition(key)?;
                boundary.remove(key);
                Ok(result)
            }
        }
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.seal_all_for_shutdown(),
        }
    }

    fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, boundary } => {
                let result = sink.break_continuity(key);
                match key {
                    Some(key) => boundary.entry(key.to_owned()).or_default().needs_snapshot = true,
                    None => {
                        for state in boundary.values_mut() {
                            state.needs_snapshot = true;
                        }
                    }
                }
                result
            }
        }
    }

    fn active_writers(&self) -> usize {
        match self {
            Self::Chunked(_) => 0,
            Self::Segment { sink, .. } => sink.active_writers(),
        }
    }

    fn is_segment_mode(&self) -> bool {
        Self::is_segment_mode(self)
    }
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
            + HasCatalogDataType
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
            + HasCatalogDataType
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
            if !last_ends.contains_key(&key)
                && let Some(seed) = self.seed_custom_last_end(&type_name, identifier.as_deref())
            {
                last_ends.insert(key.clone(), seed);
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

#[cfg(test)]
mod book_delta_batch_tests {
    use std::{fs, path::PathBuf};

    use nautilus_model::{
        data::{OrderBookDelta, OrderBookDeltas, stubs::stub_deltas},
        enums::{BookAction, BookType, RecordFlag},
        orderbook::OrderBook,
        types::Quantity,
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

    use super::{BookDeltaCatalogSink, CaptureSink};
    use crate::{
        config::CaptureConfig,
        item::PartitionKey,
        lifecycle::{LifecycleConfig, LifecycleMode, SealConfigFile},
    };

    fn temp_catalog() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "book-delta-batch-sink-{}",
            nautilus_core::UUID4::new()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp catalog");
        dir
    }

    #[test]
    fn failed_l2_write_waits_for_a_snapshot_before_recording_more_increments() {
        let root = temp_catalog();
        fs::create_dir_all(root.join("data")).unwrap();
        let obstruction = root.join("data/order_book_deltas");
        fs::write(&obstruction, b"temporary obstruction").unwrap();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        let snapshot = stub_deltas();
        let id = snapshot.instrument_id;
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        assert!(sink.write_batch(&key, vec![snapshot.clone()]).is_err());
        fs::remove_file(obstruction).unwrap();
        let mut increment = snapshot.deltas[1];
        increment.action = BookAction::Update;
        increment.flags = RecordFlag::F_LAST as u8;
        sink.write_batch(&key, vec![OrderBookDeltas::new(id, vec![increment])])
            .unwrap();
        let sealed = sink.seal_all().unwrap();
        assert!(
            sealed.files.is_empty(),
            "unchecked increments must not become a clean segment"
        );
        sink.write_batch(&key, vec![snapshot.clone()]).unwrap();
        sink.seal_all().unwrap();
        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let rows = catalog
            .order_book_deltas(Some(vec![id.to_string()]), None, None)
            .unwrap();
        assert_eq!(rows.len(), snapshot.deltas.len());
        assert_eq!(rows[0].action, BookAction::Clear);
        drop(catalog);
        drop(sink);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn grouped_snapshot_writes_canonical_rows_in_order() {
        let root = temp_catalog();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Segment,
                seal: SealConfigFile {
                    enabled: false,
                    ..SealConfigFile::default()
                },
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let mut grouped = stub_deltas();
        grouped.deltas.last_mut().expect("stub snapshot rows").flags |= RecordFlag::F_LAST as u8;
        let instrument_id = grouped.instrument_id;
        let partition = PartitionKey::catalog_data::<OrderBookDelta>(instrument_id).stable_key();
        let mut sink = BookDeltaCatalogSink::from_config(&config).expect("sink");

        sink.write_batch(&partition, vec![grouped.clone()])
            .expect("write grouped snapshot");
        sink.seal_all_for_shutdown().expect("seal");

        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let rows = catalog
            .order_book_deltas(Some(vec![instrument_id.to_string()]), None, None)
            .expect("read grouped snapshot");
        assert_eq!(rows.len(), grouped.deltas.len());
        assert_eq!(rows[0].action, BookAction::Clear);
        assert!(RecordFlag::F_SNAPSHOT.matches(rows[0].flags));
        assert!(RecordFlag::F_LAST.matches(rows.last().expect("last row").flags));
        assert!(
            rows.windows(2)
                .all(|pair| pair[0].ts_init <= pair[1].ts_init)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn grouped_batch_estimates_one_queue_payload_for_many_rows() {
        let grouped = stub_deltas();
        let row_count = grouped.deltas.len();
        assert!(row_count > 1);
        let estimated = std::mem::size_of::<OrderBookDeltas>()
            + row_count * std::mem::size_of::<OrderBookDelta>();
        assert!(estimated > std::mem::size_of::<OrderBookDelta>() * row_count);
    }

    #[test]
    fn clock_rollback_starts_a_new_window_instead_of_rewriting_market_time() {
        let root = temp_catalog();
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..Default::default()
        };
        let mut initial = stub_deltas();
        for row in &mut initial.deltas {
            row.ts_init = 200.into();
            row.ts_event = 200.into();
        }
        initial.deltas.last_mut().unwrap().flags |= RecordFlag::F_LAST as u8;
        let initial = OrderBookDeltas::new(initial.instrument_id, initial.deltas);
        let id = initial.instrument_id;
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
        sink.write_batch(&key, vec![initial.clone()]).unwrap();
        let mut bad = initial.deltas[1];
        bad.flags = RecordFlag::F_LAST as u8;
        bad.ts_init = 150.into();
        bad.ts_event = 150.into();
        assert!(
            sink.write_batch(&key, vec![OrderBookDeltas::new(id, vec![bad])])
                .is_err()
        );
        sink.break_continuity(Some(&key)).unwrap();
        let mut next = initial.clone();
        for row in &mut next.deltas {
            row.ts_init = 150.into();
            row.ts_event = 150.into();
        }
        let next = OrderBookDeltas::new(id, next.deltas);
        sink.write_batch(&key, vec![next.clone()]).unwrap();
        sink.seal_all_for_shutdown().unwrap();
        let windows = crate::replay::read_book_replay_windows(&root, id).unwrap();
        assert_eq!(windows.len(), 2);
        assert!(windows.iter().any(|w| w.data == vec![initial.clone()]));
        assert!(windows.iter().any(|w| w.data == vec![next.clone()]));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn boundary_checkpoint_preserves_final_book_but_changes_the_event_tape() {
        let raw_root = temp_catalog();
        let checkpoint_root = temp_catalog();
        let mut initial = stub_deltas();
        for row in &mut initial.deltas {
            row.ts_init = 100.into();
        }
        initial.deltas.last_mut().unwrap().flags |= RecordFlag::F_LAST as u8;
        initial = OrderBookDeltas::new(initial.instrument_id, initial.deltas);
        let id = initial.instrument_id;
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let mut source_book = OrderBook::new(id, BookType::L2_MBP);
        source_book.apply_deltas(&initial).unwrap();
        let checkpoint = source_book.to_deltas(source_book.ts_last, 150.into());
        let mut update = initial.deltas[1];
        update.action = BookAction::Update;
        update.flags = RecordFlag::F_LAST as u8;
        update.sequence = initial.sequence + 1;
        update.order.size = Quantity::from("333");
        update.ts_event = 200.into();
        update.ts_init = 200.into();
        let continuation = OrderBookDeltas::new(id, vec![update]);
        let write = |root: &PathBuf, frames: Vec<OrderBookDeltas>| {
            let config = CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            };
            let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
            sink.write_batch(&key, frames).unwrap();
            sink.seal_all_for_shutdown().unwrap();
            let mut catalog = ParquetDataCatalog::new(root, None, None, None, None);
            catalog
                .order_book_deltas(Some(vec![id.to_string()]), None, None)
                .unwrap()
        };
        let raw = write(&raw_root, vec![initial.clone(), continuation.clone()]);
        let checkpointed = write(&checkpoint_root, vec![initial, checkpoint, continuation]);
        let frame_count = |rows: &[OrderBookDelta]| {
            rows.iter()
                .filter(|row| RecordFlag::F_LAST.matches(row.flags))
                .count()
        };
        assert_eq!(frame_count(&raw), 2);
        assert_eq!(
            frame_count(&checkpointed),
            3,
            "F_LAST-aware replay sees an extra event"
        );
        let mut raw_book = OrderBook::new(id, BookType::L2_MBP);
        let mut checkpoint_book = OrderBook::new(id, BookType::L2_MBP);
        raw_book
            .apply_deltas(&OrderBookDeltas::new(id, raw))
            .unwrap();
        checkpoint_book
            .apply_deltas(&OrderBookDeltas::new(id, checkpointed))
            .unwrap();
        assert_eq!(
            raw_book.bids_as_map(None),
            checkpoint_book.bids_as_map(None)
        );
        assert_eq!(
            raw_book.asks_as_map(None),
            checkpoint_book.asks_as_map(None)
        );
        fs::remove_dir_all(raw_root).unwrap();
        fs::remove_dir_all(checkpoint_root).unwrap();
    }

    #[test]
    fn physical_seals_preserve_the_native_event_tape_without_a_checkpoint() {
        let continuous_root = temp_catalog();
        let segmented_root = temp_catalog();
        let mut initial = stub_deltas();
        for row in &mut initial.deltas {
            row.ts_init = 100.into();
            row.ts_event = 100.into();
        }
        initial.deltas.last_mut().unwrap().flags |= RecordFlag::F_LAST as u8;
        let initial = OrderBookDeltas::new(initial.instrument_id, initial.deltas);
        let id = initial.instrument_id;
        let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
        let mut frames = vec![initial.clone()];
        for ts in [150_u64, 200] {
            let mut row = initial.deltas[1];
            row.flags = RecordFlag::F_LAST as u8;
            row.action = BookAction::Update;
            row.ts_init = ts.into();
            row.ts_event = ts.into();
            row.sequence = ts;
            row.order.size = Quantity::from("333");
            frames.push(OrderBookDeltas::new(id, vec![row]));
        }
        for (root, seal) in [(&continuous_root, false), (&segmented_root, true)] {
            let config = CaptureConfig {
                catalog_uri: format!("file://{}", root.display()),
                ..Default::default()
            };
            let mut sink = BookDeltaCatalogSink::from_config(&config).unwrap();
            for frame in &frames {
                sink.write_batch(&key, vec![frame.clone()]).unwrap();
                if seal {
                    sink.seal_all().unwrap();
                }
            }
            sink.seal_all_for_shutdown().unwrap();
        }
        let mut first = ParquetDataCatalog::new(&continuous_root, None, None, None, None);
        let mut second = ParquetDataCatalog::new(&segmented_root, None, None, None, None);
        let a = first
            .order_book_deltas(Some(vec![id.to_string()]), None, None)
            .unwrap();
        let b = second
            .order_book_deltas(Some(vec![id.to_string()]), None, None)
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(
            a,
            frames
                .into_iter()
                .flat_map(|frame| frame.deltas)
                .collect::<Vec<_>>()
        );
        fs::remove_dir_all(continuous_root).unwrap();
        fs::remove_dir_all(segmented_root).unwrap();
    }
}
