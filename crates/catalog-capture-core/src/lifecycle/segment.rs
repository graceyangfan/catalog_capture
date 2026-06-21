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
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, UInt64Array};
use nautilus_core::UnixNanos;
use nautilus_model::data::{CatalogPathPrefix, HasTsInit};
use nautilus_persistence::backend::catalog::{timestamps_to_filename, ParquetDataCatalog};
use nautilus_serialization::arrow::{ArrowSchemaProvider, EncodeToRecordBatch};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde::Serialize;

use crate::{
    catalog_layout::{legacy_market_data_prefix, mirror_market_data_path},
    config::{CaptureConfig, CompressionKind, LayoutCompatibility},
    lifecycle::ResolvedSealSchedule,
    runtime::FlushResult,
};

const PART_SUFFIX: &str = ".part.parquet";

#[derive(Debug)]
struct SegmentOpenParams {
    directory: PathBuf,
    legacy_prefix: String,
    identifier: String,
    schema_metadata: HashMap<String, String>,
    open_ts_ns: u64,
}

#[derive(Debug)]
struct ActiveSegment {
    directory: PathBuf,
    part_path: PathBuf,
    writer: ArrowWriter<File>,
    legacy_prefix: String,
    identifier: String,
    schema_metadata: HashMap<String, String>,
    min_ts_ns: u64,
    max_ts_ns: u64,
    row_count: u64,
    last_sync_ns: u64,
}

#[derive(Debug)]
pub struct SegmentCaptureSink<T> {
    catalog: ParquetDataCatalog,
    local_root: PathBuf,
    layout_compatibility: LayoutCompatibility,
    seal: Option<ResolvedSealSchedule>,
    compression: Compression,
    row_group_rows: usize,
    sync_interval_ns: u64,
    segments: HashMap<String, ActiveSegment>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SegmentCaptureSink<T> {
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

impl<T> SegmentCaptureSink<T>
where
    T: HasTsInit
        + EncodeToRecordBatch
        + CatalogPathPrefix
        + ArrowSchemaProvider
        + Serialize
        + Clone,
{
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        if !config.lifecycle.is_segment_mode() {
            bail!("SegmentCaptureSink requires output.lifecycle.mode = segment");
        }

        let uri = config
            .catalog_uri
            .strip_prefix("file://")
            .unwrap_or(&config.catalog_uri);
        if !config.catalog_uri.starts_with("file://") {
            bail!("segment lifecycle requires a file:// catalog_uri");
        }

        let compression = match config.compression {
            CompressionKind::Snappy => Compression::SNAPPY,
            CompressionKind::Zstd => Compression::ZSTD(Default::default()),
        };

        let catalog = ParquetDataCatalog::new(
            Path::new(uri),
            None,
            Some(config.flush_rows),
            Some(compression),
            Some(config.flush_rows),
        );

        let mut sink = Self {
            catalog,
            local_root: PathBuf::from(uri),
            layout_compatibility: config.layout_compatibility.clone(),
            seal: config.lifecycle.resolved_seal()?,
            row_group_rows: config.lifecycle.batch_row_threshold(config.flush_rows),
            sync_interval_ns: config
                .lifecycle
                .durability
                .sync_interval_ms
                .saturating_mul(1_000_000),
            compression,
            segments: HashMap::new(),
            _marker: std::marker::PhantomData,
        };
        sink.recover_orphan_parts()?;
        Ok(sink)
    }

    /// Seal orphaned `.part.parquet` files left by a crashed process.
    pub fn recover_orphan_parts(&mut self) -> Result<usize> {
        let family_dir = self.local_root.join("data").join(T::path_prefix());
        if !family_dir.is_dir() {
            return Ok(0);
        }

        let mut recovered = 0usize;
        for (part_path, identifier) in Self::collect_orphan_part_files(&family_dir)? {
            if self
                .segments
                .values()
                .any(|segment| segment.part_path == part_path)
            {
                continue;
            }
            match self.finalize_orphan_part(&part_path, &identifier) {
                Ok(Some(_)) => recovered += 1,
                Ok(None) => {}
                Err(err) => {
                    eprintln!(
                        "catalog-capture: skipping unrecoverable orphan segment {}: {err}",
                        part_path.display()
                    );
                }
            }
        }
        Ok(recovered)
    }

    fn collect_orphan_part_files(root: &Path) -> Result<Vec<(PathBuf, String)>> {
        let mut orphans = Vec::new();
        Self::walk_orphan_part_files(root, &mut orphans)?;
        orphans.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(orphans)
    }

    fn walk_orphan_part_files(dir: &Path, orphans: &mut Vec<(PathBuf, String)>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::walk_orphan_part_files(&path, orphans)?;
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !file_name.ends_with(PART_SUFFIX) {
                continue;
            }
            let Some(identifier) = path
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                .map(str::to_string)
            else {
                continue;
            };
            orphans.push((path, identifier));
        }
        Ok(())
    }

    fn finalize_orphan_part(&self, part_path: &Path, identifier: &str) -> Result<Option<PathBuf>> {
        let metadata = fs::metadata(part_path)?;
        if metadata.len() == 0 {
            let _ = fs::remove_file(part_path);
            return Ok(None);
        }

        let file = File::open(part_path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;
        let Some((min_ts_ns, max_ts_ns)) = Self::min_max_ts_init_from_reader(&mut reader)? else {
            let _ = fs::remove_file(part_path);
            return Ok(None);
        };
        let directory = part_path
            .parent()
            .expect("part path always has a parent")
            .to_path_buf();
        let final_name =
            timestamps_to_filename(UnixNanos::from(min_ts_ns), UnixNanos::from(max_ts_ns));
        let final_path = directory.join(final_name);
        fs::rename(part_path, &final_path)
            .with_context(|| format!("failed to recover orphan segment {}", part_path.display()))?;
        self.mirror_market_data_path(
            &final_path,
            legacy_market_data_prefix(T::path_prefix()),
            identifier,
        )?;
        Ok(Some(final_path))
    }

    fn open_segment(&mut self, partition_key: &str, params: SegmentOpenParams) -> Result<()> {
        fs::create_dir_all(&params.directory)?;
        let part_path = params
            .directory
            .join(format!("{}{PART_SUFFIX}", params.open_ts_ns));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&part_path)?;
        let schema = T::get_schema(Some(params.schema_metadata.clone())).into();
        let writer = ArrowWriter::try_new(file, schema, Some(self.writer_props()))?;
        self.segments.insert(
            partition_key.to_string(),
            ActiveSegment {
                directory: params.directory,
                part_path,
                writer,
                legacy_prefix: params.legacy_prefix,
                identifier: params.identifier,
                schema_metadata: params.schema_metadata,
                min_ts_ns: 0,
                max_ts_ns: 0,
                row_count: 0,
                last_sync_ns: params.open_ts_ns,
            },
        );
        Ok(())
    }

    fn writer_props(&self) -> WriterProperties {
        WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_row_count(Some(self.row_group_rows))
            .build()
    }

    fn min_max_ts_init_from_reader(
        reader: &mut parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    ) -> Result<Option<(u64, u64)>> {
        let mut min_ts_ns = None::<u64>;
        let mut max_ts_ns = None::<u64>;
        let mut saw_rows = false;

        for batch in reader.by_ref() {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            saw_rows = true;
            let column_index = batch
                .schema()
                .fields()
                .iter()
                .position(|field| field.name() == "ts_init")
                .ok_or_else(|| anyhow!("orphan part missing ts_init column"))?;
            let column = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("orphan part ts_init column has unexpected type"))?;
            for &value in column.values() {
                min_ts_ns = Some(min_ts_ns.map_or(value, |current| current.min(value)));
                max_ts_ns = Some(max_ts_ns.map_or(value, |current| current.max(value)));
            }
        }

        Ok(match (saw_rows, min_ts_ns, max_ts_ns) {
            (true, Some(min_ts), Some(max_ts)) => Some((min_ts, max_ts)),
            _ => None,
        })
    }

    pub fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        let mut result = FlushResult::default();
        let keys: Vec<String> = self.segments.keys().cloned().collect();
        for key in keys {
            let partial = self.tick_segment(&key, now_ns)?;
            result.rows += partial.rows;
            result.bytes += partial.bytes;
            result.files.extend(partial.files);
        }
        Ok(result)
    }

    pub fn seal_all(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(true)
    }

    pub fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(false)
    }

    fn seal_all_internal(&mut self, reopen_after_seal: bool) -> Result<FlushResult> {
        let mut result = FlushResult::default();
        let keys: Vec<String> = self.segments.keys().cloned().collect();
        for key in keys {
            let partial = self.seal_segment(&key, reopen_after_seal)?;
            result.rows += partial.rows;
            result.bytes += partial.bytes;
            result.files.extend(partial.files);
        }
        Ok(result)
    }

    fn tick_segment(&mut self, key: &str, now_ns: u64) -> Result<FlushResult> {
        let Some(segment) = self.segments.get_mut(key) else {
            return Ok(FlushResult::default());
        };

        if self.sync_interval_ns > 0
            && now_ns.saturating_sub(segment.last_sync_ns) >= self.sync_interval_ns
        {
            segment.writer.flush()?;
            segment.writer.inner_mut().sync_all()?;
            segment.last_sync_ns = now_ns;
        }

        Ok(FlushResult::default())
    }

    fn seal_segment(&mut self, key: &str, reopen_after_seal: bool) -> Result<FlushResult> {
        let Some(segment) = self.segments.remove(key) else {
            return Ok(FlushResult::default());
        };

        if segment.row_count == 0 {
            let _ = segment.writer.into_inner();
            let _ = fs::remove_file(&segment.part_path);
            return Ok(FlushResult::default());
        }

        segment.writer.close()?;
        let final_name = timestamps_to_filename(
            UnixNanos::from(segment.min_ts_ns),
            UnixNanos::from(segment.max_ts_ns),
        );
        let final_path = segment.directory.join(final_name);
        fs::rename(&segment.part_path, &final_path)
            .with_context(|| format!("failed to seal segment {}", segment.part_path.display()))?;

        self.mirror_market_data_path(&final_path, &segment.legacy_prefix, &segment.identifier)?;

        let bytes = fs::metadata(&final_path)
            .map(|meta| meta.len())
            .unwrap_or(0);

        if reopen_after_seal && self.seal.is_some() {
            self.open_segment(
                key,
                SegmentOpenParams {
                    directory: segment.directory.clone(),
                    legacy_prefix: segment.legacy_prefix.clone(),
                    identifier: segment.identifier.clone(),
                    schema_metadata: segment.schema_metadata.clone(),
                    open_ts_ns: segment.max_ts_ns,
                },
            )?;
        }

        Ok(FlushResult {
            files: vec![final_path],
            rows: segment.row_count as usize,
            bytes,
        })
    }

    pub fn write_batch_mut(&mut self, partition_key: &str, batch: Vec<T>) -> Result<Vec<PathBuf>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let min_ts_ns = batch
            .iter()
            .map(|row| row.ts_init().as_u64())
            .min()
            .expect("non-empty batch");
        let max_ts_ns = batch
            .iter()
            .map(|row| row.ts_init().as_u64())
            .max()
            .expect("non-empty batch");

        let schema_metadata = EncodeToRecordBatch::chunk_metadata(&batch);
        let identifier = schema_metadata
            .get("instrument_id")
            .or_else(|| schema_metadata.get("bar_type"))
            .cloned()
            .ok_or_else(|| anyhow!("segment batch metadata missing instrument_id or bar_type"))?;

        let directory_rel = self
            .catalog
            .make_path(T::path_prefix(), Some(identifier.as_str()))?;
        let directory = self.local_root.join(directory_rel);
        let legacy_prefix = legacy_market_data_prefix(T::path_prefix()).to_string();

        if !self.segments.contains_key(partition_key) {
            self.open_segment(
                partition_key,
                SegmentOpenParams {
                    directory,
                    legacy_prefix,
                    identifier: identifier.clone(),
                    schema_metadata: schema_metadata.clone(),
                    open_ts_ns: min_ts_ns,
                },
            )?;
        }

        let segment = self
            .segments
            .get_mut(partition_key)
            .expect("segment opened above");
        let batches = self.catalog.data_to_record_batches(batch)?;
        for record_batch in &batches {
            segment.writer.write(record_batch)?;
            segment.row_count += record_batch.num_rows() as u64;
        }

        if segment.min_ts_ns == 0 {
            segment.min_ts_ns = min_ts_ns;
        } else {
            segment.min_ts_ns = segment.min_ts_ns.min(min_ts_ns);
        }
        segment.max_ts_ns = segment.max_ts_ns.max(max_ts_ns);

        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::QuoteTick,
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::config::CaptureConfig;
    use crate::lifecycle::{LifecycleConfig, LifecycleMode, SealConfigFile};

    fn quote(instrument_id: InstrumentId, ts: u64) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from("1.0001"),
            Price::from("1.0002"),
            Quantity::from("100"),
            Quantity::from("100"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    fn temp_segment_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", nautilus_core::UUID4::new()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn segment_capture_config(dir: &Path, lifecycle: LifecycleConfig) -> CaptureConfig {
        CaptureConfig {
            catalog_uri: format!("file://{}", dir.display()),
            lifecycle,
            ..CaptureConfig::default()
        }
    }

    fn segment_lifecycle(seal: SealConfigFile) -> LifecycleConfig {
        LifecycleConfig {
            mode: LifecycleMode::Segment,
            seal,
            ..LifecycleConfig::default()
        }
    }

    #[test]
    fn segment_append_and_seal_produces_catalog_parquet() {
        let dir = temp_segment_dir("segment-writer-test");
        let config = segment_capture_config(
            &dir,
            segment_lifecycle(SealConfigFile {
                enabled: false,
                ..SealConfigFile::default()
            }),
        );

        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|BTC-USD-PERP.HYPERLIQUID|_";

        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");
        sink.write_batch_mut(
            partition_key,
            vec![quote(instrument_id, 1_000), quote(instrument_id, 2_000)],
        )
        .expect("append");

        let sealed = sink.seal_all().expect("seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(sealed.files[0].exists());
        assert!(!sealed.files[0].to_string_lossy().contains(".part"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_tick_syncs_without_wall_clock_seal() {
        let dir = temp_segment_dir("segment-tick-sync-test");
        let config = segment_capture_config(
            &dir,
            segment_lifecycle(SealConfigFile {
                enabled: true,
                schedule: "06:00".to_string(),
                timezone: "UTC".to_string(),
                interval_secs: 86_400,
            }),
        );

        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|BTC-USD-PERP.HYPERLIQUID|_";
        let base_ts = 1_718_640_000_000_000_000_u64;

        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");
        sink.write_batch_mut(
            partition_key,
            vec![
                quote(instrument_id, base_ts),
                quote(instrument_id, base_ts + 1_000),
            ],
        )
        .expect("append");

        let ticked = sink
            .on_tick(base_ts + 86_400 * 1_000_000_000)
            .expect("tick");
        assert!(
            ticked.files.is_empty(),
            "worker tick should only fsync; wall-clock seal is actor-driven"
        );
        assert!(sink.segments.contains_key(partition_key));

        let sealed = sink.seal_all().expect("actor-scheduled seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(!sealed.files[0].to_string_lossy().contains(".part"));
        assert!(
            sink.segments.contains_key(partition_key),
            "scheduled seal should reopen the segment"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn seal_all_for_shutdown_leaves_no_active_part_files() {
        let dir = temp_segment_dir("segment-shutdown-seal-test");
        let config = segment_capture_config(
            &dir,
            segment_lifecycle(SealConfigFile {
                enabled: true,
                schedule: "06:00".to_string(),
                ..SealConfigFile::default()
            }),
        );

        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|BTC-USD-PERP.HYPERLIQUID|_";
        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");
        sink.write_batch_mut(
            partition_key,
            vec![quote(instrument_id, 1_000), quote(instrument_id, 2_000)],
        )
        .expect("append");

        let sealed = sink.seal_all_for_shutdown().expect("shutdown seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(sink.segments.is_empty());

        let instrument_dir = dir
            .join("data")
            .join("quotes")
            .join(instrument_id.to_string());
        let part_files: Vec<_> = fs::read_dir(&instrument_dir)
            .expect("instrument dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.to_string_lossy().contains(".part"))
            .collect();
        assert!(
            part_files.is_empty(),
            "shutdown seal should not leave active .part files: {part_files:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recover_orphan_part_seals_leftover_active_file() {
        let dir = temp_segment_dir("segment-recover-test");
        let config = segment_capture_config(
            &dir,
            segment_lifecycle(SealConfigFile {
                enabled: false,
                ..SealConfigFile::default()
            }),
        );

        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|BTC-USD-PERP.HYPERLIQUID|_";

        {
            let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");
            sink.write_batch_mut(
                partition_key,
                vec![quote(instrument_id, 1_000), quote(instrument_id, 2_000)],
            )
            .expect("append");
            let segment = sink
                .segments
                .remove(partition_key)
                .expect("segment should exist");
            segment.writer.close().expect("close orphan part");
            assert!(segment.part_path.exists());
            drop(sink);
        }

        let recovered =
            SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("recovery sink");
        assert!(recovered.segments.is_empty());
        let sealed_count = fs::read_dir(
            dir.join("data")
                .join("quotes")
                .join(instrument_id.to_string()),
        )
        .expect("instrument dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet")
                && !entry.path().to_string_lossy().contains(".part")
        })
        .count();
        assert_eq!(sealed_count, 1, "orphan .part should be sealed on startup");

        let _ = fs::remove_dir_all(&dir);
    }
}
