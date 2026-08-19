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

//! Segment lifecycle for custom data (`data/custom/{Type}/{id}/`).
//!
//! Request snapshots such as `DeribitBookSummary` append into `*.parquet.part` and seal
//! to catalog filenames on the same wall-clock schedule as market-data families.
//! This avoids chunked mode's per-interval catalog parquet explosion.

use std::{collections::HashMap, path::PathBuf};

use anyhow::{bail, Result};
use arrow::datatypes::SchemaRef;
use nautilus_model::data::CustomData;
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use nautilus_persistence::backend::custom::prepare_custom_data_batch;
use parquet::basic::Compression;

use crate::{
    config::CaptureConfig,
    lifecycle::segment_support::{
        catalog_fs_custom_directory, merge_flush, recover_orphans_under, segment_runtime_parts,
        tick_parts_map, ts_init_range_from_batch, ActivePart,
    },
    runtime::FlushResult,
};

#[derive(Debug)]
struct CustomSegmentOpenParams {
    directory: PathBuf,
    type_name: String,
    identifier: Option<String>,
    schema: SchemaRef,
    open_ts_ns: u64,
}

#[derive(Debug)]
struct ActiveCustomSegment {
    part: ActivePart,
    type_name: String,
    identifier: Option<String>,
    schema: SchemaRef,
}

/// Append-only custom-data writer with wall-clock seal (same lifecycle as market segment mode).
#[derive(Debug)]
pub struct SegmentCustomDataSink {
    catalog: ParquetDataCatalog,
    local_root: PathBuf,
    compression: Compression,
    row_group_rows: usize,
    sync_interval_ns: u64,
    segments: HashMap<String, ActiveCustomSegment>,
}

impl SegmentCustomDataSink {
    pub fn from_config(config: &CaptureConfig) -> Result<Self> {
        let parts = segment_runtime_parts(config)?;
        let mut sink = Self {
            catalog: parts.catalog,
            local_root: parts.local_root,
            row_group_rows: parts.row_group_rows,
            sync_interval_ns: parts.sync_interval_ns,
            compression: parts.compression,
            segments: HashMap::new(),
        };
        sink.recover_orphan_parts()?;
        Ok(sink)
    }

    /// Seal orphaned custom `*.parquet.part` left by a crashed process.
    pub fn recover_orphan_parts(&mut self) -> Result<usize> {
        let custom_root = self.local_root.join("data").join("custom");
        recover_orphans_under(&custom_root, |path| {
            self.segments
                .values()
                .any(|segment| segment.part.part_path == path)
        })
    }

    fn open_segment(&mut self, partition_key: &str, params: CustomSegmentOpenParams) -> Result<()> {
        let part = ActivePart::open(
            params.directory,
            params.open_ts_ns,
            params.schema.clone(),
            self.compression,
            self.row_group_rows,
        )?;
        self.segments.insert(
            partition_key.to_string(),
            ActiveCustomSegment {
                part,
                type_name: params.type_name,
                identifier: params.identifier,
                schema: params.schema,
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
                CustomSegmentOpenParams {
                    directory,
                    type_name: segment.type_name,
                    identifier: segment.identifier,
                    schema: segment.schema,
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
            "catalog-capture: rolling custom segment part {partition_key} near parquet row-group limit ({})",
            crate::lifecycle::segment_support::ROW_GROUP_ROLL_THRESHOLD
        );
        Ok(self.seal_segment(partition_key, true)?.files)
    }

    pub fn write_batch_mut(
        &mut self,
        partition_key: &str,
        batch: Vec<CustomData>,
    ) -> Result<Vec<PathBuf>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }

        let (record_batch, type_name, identifier, _start, _end) = prepare_custom_data_batch(batch)?;
        let (min_ts_ns, max_ts_ns) = ts_init_range_from_batch(&record_batch)?;
        let directory = catalog_fs_custom_directory(
            &self.local_root,
            &self.catalog,
            &type_name,
            identifier.as_deref(),
        )?;
        let schema = record_batch.schema();

        let mut sealed_paths = self.roll_if_at_capacity(partition_key)?;

        if !self.segments.contains_key(partition_key) {
            self.open_segment(
                partition_key,
                CustomSegmentOpenParams {
                    directory,
                    type_name: type_name.clone(),
                    identifier: identifier.clone(),
                    schema: schema.clone(),
                    open_ts_ns: min_ts_ns,
                },
            )?;
        } else if let Some(segment) = self.segments.get(partition_key) {
            if segment.schema.as_ref() != schema.as_ref() {
                bail!(
                    "custom segment schema mismatch for partition {partition_key} type {type_name}"
                );
            }
        }

        let segment = self
            .segments
            .get_mut(partition_key)
            .expect("segment opened above");
        segment.part.write_record_batch(&record_batch)?;
        segment.part.note_ts_range(min_ts_ns, max_ts_ns);

        sealed_paths.extend(self.roll_if_at_capacity(partition_key)?);
        Ok(sealed_paths)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, str::FromStr, sync::Arc};

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{CustomData, DataType},
        identifiers::InstrumentId,
    };
    use nautilus_persistence::test_data::RustTestCustomData;
    use nautilus_serialization::arrow::custom::ensure_custom_data_registered;

    use super::*;
    use crate::{
        config::CaptureConfig,
        lifecycle::{
            segment_support::PART_SUFFIX, LifecycleConfig, LifecycleMode, SealConfigFile,
        },
    };

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "catalog-custom-segment-{}-{}-{}",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn segment_config(dir: &Path) -> CaptureConfig {
        CaptureConfig {
            enabled: true,
            catalog_uri: format!("file://{}", dir.display()),
            flush_rows: 10_000,
            flush_interval_ms: 1_000,
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Segment,
                segment: crate::lifecycle::SegmentLifecycleConfig {
                    row_group_rows: 100,
                },
                durability: crate::lifecycle::DurabilityConfig {
                    sync_interval_ms: 1_000,
                },
                seal: SealConfigFile {
                    enabled: true,
                    schedule: "06:00".to_string(),
                    timezone: "UTC".to_string(),
                    interval_secs: 86_400,
                },
            },
            ..CaptureConfig::default()
        }
    }

    fn custom_row(ts: u64, value: f64) -> CustomData {
        let instrument_id = InstrumentId::from_str("RUST.TEST").expect("id");
        let payload = RustTestCustomData {
            instrument_id,
            value,
            flag: true,
            ts_event: UnixNanos::from(ts),
            ts_init: UnixNanos::from(ts),
        };
        let data_type = DataType::new("RustTestCustomData", None, Some(instrument_id.to_string()));
        CustomData::new(Arc::new(payload), data_type)
    }

    #[test]
    fn custom_segment_appends_same_ts_batches_to_one_part() {
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("same-ts");
        let config = segment_config(&dir);
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";
        let ts = 5_000_u64;

        // Many snapshot-style batches with identical ts_init must not create many catalog files.
        for value in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sink.write_batch_mut(partition, vec![custom_row(ts, value)])
                .expect("append");
        }

        assert_eq!(sink.segments.len(), 1);
        let part = &sink.segments[partition].part.part_path;
        assert!(
            part.to_string_lossy().ends_with(PART_SUFFIX),
            "expected active .part, got {}",
            part.display()
        );
        assert!(part.is_file());

        // No sealed catalog parquet yet — only the open part.
        let sealed_before = walk_sealed_parquet(&dir);
        assert!(
            sealed_before.is_empty(),
            "segment mode must not emit catalog parquet until seal: {sealed_before:?}"
        );

        let sealed = sink.seal_all_for_shutdown().expect("seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(sealed.files[0].is_file());
        assert!(!sealed.files[0].to_string_lossy().contains(".part"));
        assert!(
            sealed.files[0]
                .to_string_lossy()
                .contains("data/custom/RustTestCustomData")
                || sealed.files[0]
                    .components()
                    .any(|c| c.as_os_str() == "RustTestCustomData"),
            "expected custom layout path, got {}",
            sealed.files[0].display()
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn custom_segment_seal_reopens_when_schedule_enabled() {
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("reopen");
        let config = segment_config(&dir);
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";

        sink.write_batch_mut(partition, vec![custom_row(10_000, 1.0)])
            .expect("append");
        let sealed = sink.seal_all().expect("scheduled seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(
            sink.segments.contains_key(partition),
            "scheduled seal should reopen active custom segment"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn book_summary_style_polls_append_one_part_then_seal() {
        // Mirrors DeribitBookSummary: large same-ts snapshot each poll, many polls.
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("book-summary-style");
        let config = segment_config(&dir);
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";

        for poll in 0..5_u64 {
            let ts = 1_000_000 + poll * 1_000_000_000;
            // ~poll-sized batch with identical ts_init (snapshot semantics).
            let batch: Vec<CustomData> = (0..80)
                .map(|i| custom_row(ts, f64::from(i)))
                .collect();
            sink.write_batch_mut(partition, batch).expect("poll append");
        }

        assert_eq!(sink.segments.len(), 1, "one open segment for the partition");
        assert_eq!(
            sink.segments[partition].part.row_count, 400,
            "all poll rows stay in the open part"
        );
        assert!(
            walk_sealed_parquet(&dir).is_empty(),
            "no catalog parquet until seal"
        );

        let sealed = sink.seal_all_for_shutdown().expect("seal");
        assert_eq!(sealed.files.len(), 1);
        assert_eq!(sealed.rows, 400);
        assert!(sealed.files[0].is_file());
        assert!(!sealed.files[0].to_string_lossy().contains(PART_SUFFIX));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn cloud_like_book_summary_packs_polls_into_few_row_groups() {
        // Production-shaped: 50k RG, ~1k-row polls, durability ticks between polls.
        // At cloud rate this is ~1 min/RG; here we write 120 polls → expect ≤3 RGs.
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("cloud-like-pack");
        let mut config = segment_config(&dir);
        config.lifecycle.segment.row_group_rows =
            crate::lifecycle::CUSTOM_ROW_GROUP_ROWS;
        config.lifecycle.durability.sync_interval_ms = 1;
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";

        let poll_rows = crate::lifecycle::CUSTOM_MEMORY_FLUSH_ROWS;
        let polls = 120_u64;
        for poll in 0..polls {
            let ts = 1_000_000 + poll * 1_000_000_000;
            let batch: Vec<CustomData> = (0..poll_rows)
                .map(|i| custom_row(ts, f64::from(i as u32)))
                .collect();
            sink.write_batch_mut(partition, batch).expect("poll");
            // Durability tick every poll (old bug sealed 1 RG here).
            sink.on_tick(ts + 500_000_000).expect("tick");
        }

        let part = &sink.segments[partition].part;
        assert_eq!(part.row_count, polls * poll_rows as u64);
        let rgs = part.flushed_row_group_count();
        // 120_000 rows / 50_000 = 2 full RGs flushed; remainder open in progress.
        assert!(
            rgs <= 3,
            "expected few RGs with 50k packing + no tick-flush, got {rgs}"
        );
        assert!(
            rgs < crate::lifecycle::ROW_GROUP_ROLL_THRESHOLD,
            "must stay far under soft roll"
        );
        assert!(walk_sealed_parquet(&dir).is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn durability_tick_does_not_finalize_row_groups() {
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("tick-no-rg");
        let config = segment_config(&dir);
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";

        // Small batches under row_group_rows so data stays in the open group.
        sink.write_batch_mut(partition, vec![custom_row(1_000, 1.0)])
            .expect("append");
        let rgs_before = sink.segments[partition].part.flushed_row_group_count();

        for step in 1..=5_u64 {
            sink.on_tick(1_000 + step * 2_000_000_000).expect("tick");
        }

        assert_eq!(
            sink.segments[partition].part.flushed_row_group_count(),
            rgs_before,
            "durability tick must fsync only — not ArrowWriter::flush (RG seal)"
        );
        assert!(walk_sealed_parquet(&dir).is_empty());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn capacity_roll_mid_part_for_custom_data() {
        ensure_custom_data_registered::<RustTestCustomData>();
        let dir = temp_dir("capacity-roll");
        let mut config = segment_config(&dir);
        config.lifecycle.segment.row_group_rows = 1;
        config.lifecycle.seal.enabled = false;
        let mut sink = SegmentCustomDataSink::from_config(&config).expect("sink");
        let partition = "custom_data|RustTestCustomData|RUST.TEST|_";

        sink.write_batch_mut(partition, vec![custom_row(1_000, 1.0)])
            .expect("seed");
        sink.segments
            .get_mut(partition)
            .expect("open")
            .part
            .set_row_group_roll_threshold_for_test(2);

        let mut sealed_from_write = 0usize;
        for i in 0..6_u64 {
            let paths = sink
                .write_batch_mut(partition, vec![custom_row(2_000 + i * 1_000, f64::from(i as u32))])
                .expect("append");
            sealed_from_write += paths.len();
        }

        assert!(
            sealed_from_write >= 1,
            "custom capacity roll should seal mid-part"
        );
        assert!(sink.segments.contains_key(partition));
        assert!(
            !walk_sealed_parquet(&dir).is_empty(),
            "sealed catalog files must exist after capacity roll"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    fn walk_sealed_parquet(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            let Ok(entries) = fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|e| e == "parquet")
                    && !p.to_string_lossy().ends_with(PART_SUFFIX)
                {
                    out.push(p);
                }
            }
        }
        walk(dir, &mut out);
        out
    }
}
