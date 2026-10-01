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
        close::InstrumentClose, Bar, CustomData, FundingRateUpdate, HasTsInit, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, OrderBookDelta, OrderBookDeltas,
        QuoteTick, TradeTick,
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
/// allocation per delta. Segment mode also applies the snapshot handoff
/// sequence gate and persists a monotonic local arrival timestamp per message.
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
    /// Highest source sequence represented by the most recent snapshot.
    snapshot_sequence: Option<u64>,
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
    ) -> Option<Vec<OrderBookDelta>> {
        let Self::Segment { boundary, .. } = self else {
            unreachable!("segment batches are only prepared in segment mode")
        };

        let state = boundary.entry(partition_key.to_string()).or_default();
        let is_snapshot = RecordFlag::F_SNAPSHOT.matches(grouped.flags);

        // A boundary snapshot already contains every update through its high-water
        // sequence. Drop only a later-arriving ordinary batch which it already covers.
        // Sequence zero is deliberately not compared: not all venues provide a
        // comparable source sequence.
        if !is_snapshot
            && grouped.sequence > 0
            && state
                .snapshot_sequence
                .is_some_and(|snapshot| grouped.sequence <= snapshot)
        {
            log::debug!(
                "catalog-capture: dropping stale order-book batch already covered by boundary snapshot (partition={partition_key}, sequence={}, snapshot_sequence={:?})",
                grouped.sequence,
                state.snapshot_sequence,
            );
            return None;
        }

        // Keep each venue message atomic in time. When the local arrival clock
        // rolls back at a segment boundary, advance only the persisted ts_init;
        // ts_event, source sequence, actions, and book flags remain untouched.
        let raw_ts_init_ns = grouped.ts_init.as_u64();
        let normalized_ts_init_ns = state
            .last_ts_init_ns
            .filter(|last| raw_ts_init_ns <= *last)
            .map_or(raw_ts_init_ns, |last| last.saturating_add(1));
        if normalized_ts_init_ns != raw_ts_init_ns {
            log::debug!(
                "catalog-capture: normalized order-book arrival timestamp at segment boundary (partition={partition_key}, raw_ts_init_ns={raw_ts_init_ns}, normalized_ts_init_ns={normalized_ts_init_ns})"
            );
        }

        let mut rows = grouped.deltas;
        if normalized_ts_init_ns != raw_ts_init_ns {
            for row in &mut rows {
                row.ts_init = normalized_ts_init_ns.into();
            }
        }
        state.last_ts_init_ns = Some(normalized_ts_init_ns);

        if is_snapshot {
            state.snapshot_sequence = (grouped.sequence > 0).then_some(grouped.sequence);
        } else if state
            .snapshot_sequence
            .is_some_and(|snapshot| grouped.sequence > snapshot)
        {
            // The first post-snapshot source batch has crossed the handoff. Do
            // not retain a stale filter for the rest of the segment.
            state.snapshot_sequence = None;
        }

        Some(rows)
    }

    fn finish_segment_seal(&mut self, result: Result<FlushResult>) -> Result<FlushResult> {
        if result.is_ok() {
            if let Self::Segment { boundary, .. } = self {
                // Boundary state belongs to the active segment. The next segment
                // starts with a fresh managed-book snapshot.
                boundary.clear();
            }
        }
        result
    }
}

impl CaptureSink<OrderBookDeltas> for BookDeltaCatalogSink {
    fn write_batch(
        &mut self,
        partition_key: &str,
        batch: Vec<OrderBookDeltas>,
    ) -> Result<Vec<PathBuf>> {
        match self {
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
                    let Some(rows) = self.prepare_segment_batch(partition_key, grouped) else {
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
        }
    }

    fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.on_tick(now_ns),
        }
    }

    fn seal_all(&mut self) -> Result<FlushResult> {
        let result = match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.seal_all(),
        };
        self.finish_segment_seal(result)
    }

    fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        let result = match self {
            Self::Chunked(_) => Ok(FlushResult::default()),
            Self::Segment { sink, .. } => sink.seal_all_for_shutdown(),
        };
        self.finish_segment_seal(result)
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
            if !last_ends.contains_key(&key) {
                if let Some(seed) = self.seed_custom_last_end(&type_name, identifier.as_deref()) {
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

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{stubs::stub_deltas, BookOrder, OrderBookDelta, OrderBookDeltas},
        enums::{BookAction, BookType, OrderSide, RecordFlag},
        orderbook::OrderBook,
        types::{Price, Quantity},
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
        assert!(rows
            .windows(2)
            .all(|pair| pair[0].ts_init <= pair[1].ts_init));

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
    fn boundary_preserves_book_state_when_arrival_clock_rolls_back() {
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
        let instrument_id = "BTC-PERP.TEST".parse().expect("instrument");
        let initial = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("2"), 1),
                RecordFlag::F_LAST as u8,
                2,
                UnixNanos::from(2),
                UnixNanos::from(100),
            )],
        );
        let mut source_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        source_book
            .apply_deltas(&initial)
            .expect("initial source book");
        let snapshot = source_book.to_deltas(UnixNanos::from(2), UnixNanos::from(200));

        let stale = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Buy, Price::from("99"), Quantity::from("9"), 2),
                RecordFlag::F_LAST as u8,
                2,
                UnixNanos::from(2),
                UnixNanos::from(150),
            )],
        );
        let continuation = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Sell, Price::from("101"), Quantity::from("3"), 3),
                RecordFlag::F_LAST as u8,
                3,
                UnixNanos::from(3),
                UnixNanos::from(150),
            )],
        );
        let partition = PartitionKey::catalog_data::<OrderBookDelta>(instrument_id).stable_key();
        let mut sink = BookDeltaCatalogSink::from_config(&config).expect("sink");
        sink.write_batch(
            &partition,
            vec![snapshot.clone(), stale, continuation.clone()],
        )
        .expect("write boundary batches");
        sink.seal_all_for_shutdown().expect("seal");

        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let rows = catalog
            .order_book_deltas(Some(vec![instrument_id.to_string()]), None, None)
            .expect("read boundary batches");
        assert!(rows
            .windows(2)
            .all(|pair| pair[0].ts_init <= pair[1].ts_init));
        assert!(RecordFlag::F_SNAPSHOT.matches(rows[0].flags));
        assert_eq!(
            rows.len(),
            snapshot.deltas.len() + continuation.deltas.len()
        );

        let replay = OrderBookDeltas::new(instrument_id, rows);
        let mut replay_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        replay_book
            .apply_deltas(&replay)
            .expect("replay normalized stream");

        let mut expected_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        expected_book
            .apply_deltas(&initial)
            .expect("expected initial book");
        expected_book
            .apply_deltas(&continuation)
            .expect("expected continuation");
        assert_eq!(
            replay_book.bids_as_map(None),
            expected_book.bids_as_map(None)
        );
        assert_eq!(
            replay_book.asks_as_map(None),
            expected_book.asks_as_map(None)
        );
        assert_eq!(replay_book.sequence, expected_book.sequence);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dual_writer_replay_matches_with_and_without_boundary_snapshot() {
        let continuous_root = temp_catalog();
        let segmented_root = temp_catalog();
        let continuous_config = CaptureConfig {
            catalog_uri: format!("file://{}", continuous_root.display()),
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Chunked,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let segmented_config = CaptureConfig {
            catalog_uri: format!("file://{}", segmented_root.display()),
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
        let instrument_id = "BTC-PERP.TEST".parse().expect("instrument");
        let initial = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("2"), 1),
                RecordFlag::F_LAST as u8,
                2,
                UnixNanos::from(2),
                UnixNanos::from(100),
            )],
        );
        let mut source_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        source_book
            .apply_deltas(&initial)
            .expect("initial source book");
        let boundary_snapshot = source_book.to_deltas(UnixNanos::from(2), UnixNanos::from(200));
        let late_duplicate = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Update,
                BookOrder::new(OrderSide::Buy, Price::from("100"), Quantity::from("2"), 1),
                RecordFlag::F_LAST as u8,
                2,
                UnixNanos::from(2),
                UnixNanos::from(150),
            )],
        );
        let continuation = OrderBookDeltas::new(
            instrument_id,
            vec![OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                BookOrder::new(OrderSide::Sell, Price::from("101"), Quantity::from("3"), 2),
                RecordFlag::F_LAST as u8,
                3,
                UnixNanos::from(3),
                UnixNanos::from(150),
            )],
        );
        let late_duplicate_rows = late_duplicate.deltas.len();

        let partition = PartitionKey::catalog_data::<OrderBookDelta>(instrument_id).stable_key();
        let mut continuous =
            BookDeltaCatalogSink::from_config(&continuous_config).expect("continuous sink");
        continuous
            .write_batch(
                &partition,
                vec![
                    initial.clone(),
                    late_duplicate.clone(),
                    continuation.clone(),
                ],
            )
            .expect("write continuous stream");
        continuous
            .seal_all_for_shutdown()
            .expect("seal continuous stream");

        let mut segmented =
            BookDeltaCatalogSink::from_config(&segmented_config).expect("segmented sink");
        segmented
            .write_batch(
                &partition,
                vec![
                    initial,
                    boundary_snapshot.clone(),
                    late_duplicate,
                    continuation,
                ],
            )
            .expect("write segmented stream");
        segmented
            .seal_all_for_shutdown()
            .expect("seal segmented stream");

        let mut continuous_catalog =
            ParquetDataCatalog::new(&continuous_root, None, None, None, None);
        let continuous_rows = continuous_catalog
            .order_book_deltas(Some(vec![instrument_id.to_string()]), None, None)
            .expect("read continuous stream");
        let mut segmented_catalog =
            ParquetDataCatalog::new(&segmented_root, None, None, None, None);
        let segmented_rows = segmented_catalog
            .order_book_deltas(Some(vec![instrument_id.to_string()]), None, None)
            .expect("read segmented stream");

        assert!(segmented_rows
            .windows(2)
            .all(|pair| pair[0].ts_init <= pair[1].ts_init));
        assert!(segmented_rows
            .iter()
            .any(|row| RecordFlag::F_SNAPSHOT.matches(row.flags)));
        assert_eq!(
            segmented_rows.len(),
            continuous_rows.len() + boundary_snapshot.deltas.len() - late_duplicate_rows
        );

        let mut continuous_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        continuous_book
            .apply_deltas(&OrderBookDeltas::new(instrument_id, continuous_rows))
            .expect("replay continuous stream");
        let mut segmented_book = OrderBook::new(instrument_id, BookType::L2_MBP);
        segmented_book
            .apply_deltas(&OrderBookDeltas::new(instrument_id, segmented_rows))
            .expect("replay segmented stream");

        assert_eq!(
            continuous_book.bids_as_map(None),
            segmented_book.bids_as_map(None)
        );
        assert_eq!(
            continuous_book.asks_as_map(None),
            segmented_book.asks_as_map(None)
        );
        assert_eq!(continuous_book.sequence, segmented_book.sequence);

        let _ = fs::remove_dir_all(continuous_root);
        let _ = fs::remove_dir_all(segmented_root);
    }
}
