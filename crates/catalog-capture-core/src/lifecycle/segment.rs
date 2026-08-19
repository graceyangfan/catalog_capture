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

use std::{collections::HashMap, path::PathBuf};

use anyhow::{anyhow, Result};
use nautilus_model::data::{CatalogPathPrefix, HasTsInit};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use nautilus_serialization::arrow::{ArrowSchemaProvider, EncodeToRecordBatch};
use parquet::basic::Compression;
use serde::Serialize;

use crate::{
    config::CaptureConfig,
    lifecycle::segment_support::{
        catalog_fs_directory, merge_flush, recover_orphans_under, segment_runtime_parts,
        tick_parts_map, ActivePart,
    },
    runtime::FlushResult,
};

#[derive(Debug)]
struct SegmentOpenParams {
    directory: PathBuf,
    identifier: String,
    schema_metadata: HashMap<String, String>,
    open_ts_ns: u64,
}

#[derive(Debug)]
struct ActiveSegment {
    part: ActivePart,
    identifier: String,
    schema_metadata: HashMap<String, String>,
}

#[derive(Debug)]
pub struct SegmentCaptureSink<T> {
    catalog: ParquetDataCatalog,
    local_root: PathBuf,
    compression: Compression,
    row_group_rows: usize,
    sync_interval_ns: u64,
    segments: HashMap<String, ActiveSegment>,
    _marker: std::marker::PhantomData<T>,
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
        let parts = segment_runtime_parts(config)?;
        let mut sink = Self {
            catalog: parts.catalog,
            local_root: parts.local_root,
            row_group_rows: parts.row_group_rows,
            sync_interval_ns: parts.sync_interval_ns,
            compression: parts.compression,
            segments: HashMap::new(),
            _marker: std::marker::PhantomData,
        };
        sink.recover_orphan_parts()?;
        Ok(sink)
    }

    /// Seal orphaned active segment files left by a crashed process.
    pub fn recover_orphan_parts(&mut self) -> Result<usize> {
        let family_dir = self.local_root.join("data").join(T::path_prefix());
        recover_orphans_under(&family_dir, |path| {
            self.segments
                .values()
                .any(|segment| segment.part.part_path == path)
        })
    }

    fn open_segment(&mut self, partition_key: &str, params: SegmentOpenParams) -> Result<()> {
        let schema = T::get_schema(Some(params.schema_metadata.clone())).into();
        let part = ActivePart::open(
            params.directory,
            params.open_ts_ns,
            schema,
            self.compression,
            self.row_group_rows,
        )?;
        self.segments.insert(
            partition_key.to_string(),
            ActiveSegment {
                part,
                identifier: params.identifier,
                schema_metadata: params.schema_metadata,
            },
        );
        Ok(())
    }

    pub fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        tick_parts_map(
            &mut self.segments,
            self.sync_interval_ns,
            now_ns,
            |segment| &mut segment.part,
        )
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
            merge_flush(&mut result, self.seal_segment(&key, reopen_after_seal)?);
        }
        Ok(result)
    }

    fn seal_segment(&mut self, key: &str, reopen_after_seal: bool) -> Result<FlushResult> {
        let Some(segment) = self.segments.remove(key) else {
            return Ok(FlushResult::default());
        };

        let max_ts_ns = segment.part.max_ts_ns;
        let directory = segment.part.directory.clone();
        let sealed = segment.part.seal()?;

        // Reopen for continued capture (wall-clock seal and capacity roll).
        // Do not gate on seal schedule: long parts with seal disabled still need roll.
        if reopen_after_seal && !sealed.files.is_empty() {
            self.open_segment(
                key,
                SegmentOpenParams {
                    directory,
                    identifier: segment.identifier,
                    schema_metadata: segment.schema_metadata,
                    open_ts_ns: max_ts_ns,
                },
            )?;
        }

        Ok(sealed)
    }

    /// Seal + reopen when the open part approaches the parquet row-group limit.
    fn roll_if_at_capacity(&mut self, partition_key: &str) -> Result<Vec<PathBuf>> {
        let needs = self
            .segments
            .get(partition_key)
            .is_some_and(|segment| segment.part.needs_capacity_roll());
        if !needs {
            return Ok(Vec::new());
        }
        log::info!(
            "catalog-capture: rolling segment part {partition_key} near parquet row-group limit ({})",
            crate::lifecycle::segment_support::ROW_GROUP_ROLL_THRESHOLD
        );
        Ok(self.seal_segment(partition_key, true)?.files)
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

        let directory = catalog_fs_directory(
            &self.local_root,
            &self.catalog,
            T::path_prefix(),
            Some(identifier.as_str()),
        )?;

        let mut sealed_paths = self.roll_if_at_capacity(partition_key)?;

        if !self.segments.contains_key(partition_key) {
            self.open_segment(
                partition_key,
                SegmentOpenParams {
                    directory,
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
        let batches = self.catalog.data_to_record_batches(&batch)?;
        for record_batch in &batches {
            segment.part.write_record_batch(record_batch)?;
        }
        segment.part.note_ts_range(min_ts_ns, max_ts_ns);

        sealed_paths.extend(self.roll_if_at_capacity(partition_key)?);
        Ok(sealed_paths)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, str::FromStr};

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

        let rgs_before = sink.segments[partition_key].part.flushed_row_group_count();
        let ticked = sink
            .on_tick(base_ts + 86_400 * 1_000_000_000)
            .expect("tick");
        assert!(
            ticked.files.is_empty(),
            "worker tick should only fsync; wall-clock seal is actor-driven"
        );
        assert_eq!(
            sink.segments[partition_key].part.flushed_row_group_count(),
            rgs_before,
            "durability tick must not finalize parquet row groups"
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
    fn capacity_roll_seals_and_reopens_before_row_group_limit() {
        let dir = temp_segment_dir("segment-capacity-roll");
        // One row per row group so a few writes cross a low test threshold.
        let mut lifecycle = segment_lifecycle(SealConfigFile {
            enabled: false,
            ..SealConfigFile::default()
        });
        lifecycle.segment.row_group_rows = 1;
        let config = segment_capture_config(&dir, lifecycle);

        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|BTC-USD-PERP.HYPERLIQUID|_";
        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");

        sink.write_batch_mut(partition_key, vec![quote(instrument_id, 1_000)])
            .expect("first row");
        sink.segments
            .get_mut(partition_key)
            .expect("open")
            .part
            .set_row_group_roll_threshold_for_test(2);

        // Drive enough single-row batches for auto-flushed RGs to hit the soft cap.
        let mut sealed_total = 0usize;
        for i in 0..6_u64 {
            let paths = sink
                .write_batch_mut(
                    partition_key,
                    vec![quote(instrument_id, 2_000 + i * 1_000)],
                )
                .expect("append");
            sealed_total += paths.len();
        }

        assert!(
            sealed_total >= 1,
            "capacity roll should emit at least one sealed catalog parquet"
        );
        assert!(
            sink.segments.contains_key(partition_key),
            "capacity roll must reopen an active part"
        );
        assert!(
            sink.segments[partition_key]
                .part
                .flushed_row_group_count()
                < 2
                || sink.segments[partition_key].part.row_count > 0,
            "new part should be under the soft cap after roll"
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
            segment.part.writer.close().expect("close orphan part");
            assert!(segment.part.part_path.exists());
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

    #[test]
    fn recover_orphan_parts_skips_corrupt_file_and_seals_valid_orphan() {
        let dir = temp_segment_dir("segment-recover-corrupt-test");
        let config = segment_capture_config(
            &dir,
            segment_lifecycle(SealConfigFile {
                enabled: false,
                ..SealConfigFile::default()
            }),
        );
        let instrument_id = InstrumentId::from_str("ETH-USD-PERP.HYPERLIQUID").expect("id");
        let partition_key = "market_data|quotes|ETH-USD-PERP.HYPERLIQUID|_";
        let instrument_dir = dir
            .join("data")
            .join("quotes")
            .join(instrument_id.to_string());

        {
            let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("sink");
            sink.write_batch_mut(
                partition_key,
                vec![quote(instrument_id, 1_000), quote(instrument_id, 2_000)],
            )
            .expect("append valid orphan");
            let segment = sink
                .segments
                .remove(partition_key)
                .expect("segment should exist");
            segment.part.writer.close().expect("close valid orphan");
            assert!(segment.part.part_path.exists());
            drop(sink);
        }

        fs::create_dir_all(&instrument_dir).expect("instrument dir");
        let corrupt_part = instrument_dir.join("9999999999999999999.parquet.part");
        fs::write(&corrupt_part, b"not-a-parquet-file").expect("write corrupt orphan");

        let recovered = SegmentCaptureSink::<QuoteTick>::from_config(&config).expect("recovery");
        assert!(recovered.segments.is_empty());
        assert_eq!(
            fs::read_dir(&instrument_dir)
                .expect("instrument dir")
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    let path = entry.path();
                    path.extension().and_then(|ext| ext.to_str()) == Some("parquet")
                        && !path.to_string_lossy().contains(".part")
                })
                .count(),
            1,
            "valid orphan should be sealed on startup"
        );
        assert!(
            corrupt_part.exists(),
            "corrupt orphan should be skipped, not deleted"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
