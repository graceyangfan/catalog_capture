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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{Result, anyhow};
use arrow::datatypes::Schema;
use nautilus_model::data::HasTsInit;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_persistence::common::paths::CatalogPathPrefix;
use nautilus_serialization::arrow::EncodeToRecordBatch;
use parquet::basic::Compression;
use serde::Serialize;

use crate::{
    config::CaptureConfig,
    lifecycle::segment_support::{
        ActivePart, catalog_fs_directory, recover_orphans_under, segment_runtime_parts,
        tick_parts_map,
    },
    runtime::{FlushResult, collect_flush_results},
};

#[derive(Debug)]
struct SegmentOpenParams {
    directory: PathBuf,
    identifier: String,
    schema: Arc<Schema>,
    open_ts_ns: u64,
    initialization_rows: usize,
}

#[derive(Debug)]
struct ActiveSegment {
    part: ActivePart,
    identifier: String,
    schema: Arc<Schema>,
}

#[derive(Debug)]
pub struct SegmentCaptureSink<T> {
    catalog: ParquetDataCatalog,
    local_root: PathBuf,
    compression: Compression,
    row_group_rows: usize,
    row_group_bytes: usize,
    sync_interval_ns: u64,
    segments: HashMap<String, ActiveSegment>,
    window_ids: HashMap<String, (String, u64)>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> SegmentCaptureSink<T>
where
    T: HasTsInit + EncodeToRecordBatch + CatalogPathPrefix + Serialize + Clone,
{
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        let parts = segment_runtime_parts(config)?;
        let mut sink = Self {
            catalog: parts.catalog,
            local_root: parts.local_root,
            row_group_rows: parts.row_group_rows,
            row_group_bytes: parts.row_group_bytes,
            sync_interval_ns: parts.sync_interval_ns,
            compression: parts.compression,
            segments: HashMap::new(),
            window_ids: HashMap::new(),
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
        let (window_id, segment_index) = self
            .window_ids
            .entry(partition_key.to_owned())
            .or_insert_with(|| (nautilus_core::UUID4::new().to_string(), 0));
        let mut metadata = params.schema.metadata().clone();
        metadata.insert(crate::replay::WINDOW_ID_KEY.into(), window_id.clone());
        metadata.insert(
            crate::replay::SEGMENT_INDEX_KEY.into(),
            segment_index.to_string(),
        );
        metadata.insert(
            crate::replay::CONTRACT_KEY.into(),
            crate::replay::CONTRACT_VERSION.into(),
        );
        metadata.insert(
            crate::replay::INITIALIZATION_ROWS_KEY.into(),
            params.initialization_rows.to_string(),
        );
        let schema = Arc::new(params.schema.as_ref().clone().with_metadata(metadata));
        let part = ActivePart::open(
            params.directory,
            params.open_ts_ns,
            Arc::clone(&schema),
            self.compression,
            self.row_group_rows,
            self.row_group_bytes,
        )?;
        self.segments.insert(
            partition_key.to_string(),
            ActiveSegment {
                part,
                identifier: params.identifier,
                schema,
            },
        );
        Ok(())
    }

    #[must_use]
    pub fn active_writers(&self) -> usize {
        self.segments.len()
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
        self.seal_all_internal(false)
    }

    pub fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
        self.window_ids.remove(key);
        self.seal_segment(key, false)
    }

    /// End continuity for one partition, or conservatively for the whole family.
    pub fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
        match key {
            Some(key) => self.seal_partition(key),
            None => {
                self.window_ids.clear();
                self.seal_all()
            }
        }
    }

    pub fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(false)
    }

    fn seal_all_internal(&mut self, reopen_after_seal: bool) -> Result<FlushResult> {
        let keys: Vec<String> = self.segments.keys().cloned().collect();
        collect_flush_results(
            keys.iter()
                .map(|key| self.seal_segment(key, reopen_after_seal)),
        )
    }

    fn seal_segment(&mut self, key: &str, reopen_after_seal: bool) -> Result<FlushResult> {
        let Some(segment) = self.segments.remove(key) else {
            return Ok(FlushResult::default());
        };

        let max_ts_ns = segment.part.max_ts_ns;
        let directory = segment.part.directory.clone();
        let sealed = match segment.part.seal() {
            Ok(sealed) => sealed,
            Err(error) => {
                self.window_ids.remove(key);
                return Err(error);
            }
        };

        if !sealed.files.is_empty()
            && let Some((_, index)) = self.window_ids.get_mut(key)
        {
            *index = index.saturating_add(1);
        }

        // Reopen for continued capture (wall-clock seal and capacity roll).
        // Do not gate on seal schedule: long parts with seal disabled still need roll.
        if reopen_after_seal && !sealed.files.is_empty() {
            self.open_segment(
                key,
                SegmentOpenParams {
                    directory,
                    identifier: segment.identifier,
                    schema: segment.schema,
                    open_ts_ns: max_ts_ns,
                    initialization_rows: 0,
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
        self.write_batch_inner(partition_key, batch, 0)
    }

    pub fn write_initialization(
        &mut self,
        partition_key: &str,
        batch: Vec<T>,
    ) -> Result<Vec<PathBuf>> {
        anyhow::ensure!(
            !self.segments.contains_key(partition_key),
            "initialization requires a fresh physical segment"
        );
        let count = batch.len();
        self.write_batch_inner(partition_key, batch, count)
    }

    fn write_batch_inner(
        &mut self,
        partition_key: &str,
        batch: Vec<T>,
        initialization_rows: usize,
    ) -> Result<Vec<PathBuf>> {
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

        let batches = self.catalog.data_to_record_batches(&batch)?;
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .expect("non-empty input produces at least one record batch");

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
                    schema,
                    open_ts_ns: min_ts_ns,
                    initialization_rows,
                },
            )?;
        }

        let segment = self
            .segments
            .get_mut(partition_key)
            .expect("segment opened above");
        let written = batches
            .iter()
            .try_for_each(|batch| segment.part.write_record_batch(batch));
        if let Err(error) = written {
            self.window_ids.remove(partition_key);
            if let Some(segment) = self.segments.remove(partition_key) {
                segment.part.quarantine();
            }
            return Err(error);
        }
        let segment = self
            .segments
            .get_mut(partition_key)
            .expect("successful writer retained");
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
    fn failed_arrow_append_is_quarantined_and_later_rows_use_a_fresh_writer() {
        let dir = temp_segment_dir("segment-recover-append");
        let mut cfg = segment_capture_config(&dir, segment_lifecycle(SealConfigFile::default()));
        cfg.lifecycle.segment.row_group_rows = 1;
        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&cfg).unwrap();
        let id = InstrumentId::from("AUDUSD.SIM");
        let key = "quotes";
        sink.write_batch_mut(key, vec![quote(id, 1)]).unwrap();
        let old_path = sink.segments[key].part.part_path.clone();
        // Force a real OS append failure without a production fault-injection API.
        *sink.segments.get_mut(key).unwrap().part.writer.inner_mut() =
            fs::File::open(&old_path).unwrap();
        assert!(
            sink.write_batch_mut(key, (2..=1000).map(|ts| quote(id, ts)).collect())
                .is_err()
        );
        assert!(!sink.segments.contains_key(key));
        assert!(old_path.with_extension("failed").exists());
        sink.write_batch_mut(key, vec![quote(id, 1001)]).unwrap();
        sink.seal_all().unwrap();
        let mut catalog = ParquetDataCatalog::new(&dir, None, None, None, None);
        let rows = catalog
            .quote_ticks(Some(vec![id.to_string()]), None, None)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_init.as_u64(), 1001);
        drop(catalog);
        drop(sink);
        fs::remove_dir_all(dir).unwrap();
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

        let mut catalog = ParquetDataCatalog::new(&dir, None, None, None, None);
        let rows = catalog
            .quote_ticks(Some(vec![instrument_id.to_string()]), None, None)
            .expect("sealed quotes should be readable");
        assert_eq!(
            rows,
            vec![quote(instrument_id, 1_000), quote(instrument_id, 2_000)]
        );
        ParquetDataCatalog::check_ascending_timestamps(&rows, "segment quotes")
            .expect("sealed timestamps should be ascending");

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
            !sink.segments.contains_key(partition_key),
            "scheduled seal must not create an empty replacement part"
        );

        sink.write_batch_mut(partition_key, vec![quote(instrument_id, base_ts + 2_000)])
            .expect("lazy next part");
        assert!(sink.segments.contains_key(partition_key));

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
                .write_batch_mut(partition_key, vec![quote(instrument_id, 2_000 + i * 1_000)])
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
            sink.segments[partition_key].part.flushed_row_group_count() < 2
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

    #[test]
    fn encoded_byte_target_flushes_before_the_row_count_target() {
        let root = temp_segment_dir("capture-writer-bytes");
        let mut config = segment_capture_config(&root, LifecycleConfig::default());
        config.max_buffer_bytes = 1;
        config.lifecycle.segment.row_group_rows = 100_000;
        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).unwrap();
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let rows = (1..=1024_u64).map(|ts| quote(id, ts)).collect();
        sink.write_batch_mut("quotes", rows).unwrap();
        let groups = sink
            .segments
            .values()
            .next()
            .unwrap()
            .part
            .flushed_row_group_count();
        sink.seal_all().unwrap();
        fs::remove_dir_all(root).unwrap();
        assert!(
            groups > 0,
            "the encoded byte target must flush before 100,000 rows"
        );
    }

    #[test]
    fn synced_open_part_without_footer_is_not_recovered_as_catalog_data() {
        let root = temp_segment_dir("capture-open-part-crash-contract");
        let mut config = segment_capture_config(&root, LifecycleConfig::default());
        config.lifecycle.segment.row_group_rows = 1;
        let id = InstrumentId::from("BTCUSDT.BINANCE");
        let mut sink = SegmentCaptureSink::<QuoteTick>::from_config(&config).unwrap();
        sink.write_batch_mut("quotes", (1..=1024).map(|ts| quote(id, ts)).collect())
            .unwrap();
        let mut segment = sink.segments.remove("quotes").unwrap();
        segment.part.writer.flush().unwrap();
        segment.part.writer.inner_mut().sync_all().unwrap();
        let part_path = segment.part.part_path.clone();
        assert!(
            fs::metadata(&part_path).unwrap().len() > 4,
            "row-group bytes reached the file"
        );
        // Model process loss after fsync without the close/footer/rename path.
        drop(segment);
        drop(sink);
        let recovered = SegmentCaptureSink::<QuoteTick>::from_config(&config).unwrap();
        assert!(recovered.segments.is_empty());
        assert!(
            part_path.exists(),
            "unrecoverable evidence must remain available"
        );
        let sealed = fs::read_dir(part_path.parent().unwrap())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "parquet")
            })
            .count();
        assert_eq!(
            sealed, 0,
            "fsync alone does not create a readable Parquet footer"
        );
        drop(recovered);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn invalid_writer_byte_target_returns_a_configuration_error() {
        let root = temp_segment_dir("capture-writer-invalid-bytes");
        let mut config = segment_capture_config(&root, LifecycleConfig::default());
        config.max_buffer_bytes = 0;
        assert!(SegmentCaptureSink::<QuoteTick>::from_config(&config).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
