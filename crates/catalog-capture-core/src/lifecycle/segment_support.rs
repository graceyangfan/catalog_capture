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

//! Shared segment lifecycle primitives for market and custom sinks.
//!
//! In-progress files use a non-queryable `*.parquet.part` suffix so Nautilus
//! `list_parquet_files` never picks them up. Seal renames via `timestamps_to_filename`.

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use nautilus_core::UnixNanos;
use nautilus_persistence::backend::parquet::{
    catalog::ParquetDataCatalog, paths::timestamps_to_filename,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{
    config::{CaptureConfig, CompressionKind},
    runtime::{FlushResult, collect_flush_results},
};

/// Must NOT end with `.parquet` (catalog queries match any `*.parquet`).
pub(crate) const PART_SUFFIX: &str = ".parquet.part";

// Capacity constants: single derivation source (`row_group_capacity`).
pub(crate) use crate::lifecycle::row_group_capacity::ROW_GROUP_ROLL_THRESHOLD;

#[derive(Debug)]
pub(crate) struct SegmentRuntimeParts {
    pub catalog: ParquetDataCatalog,
    pub local_root: PathBuf,
    pub compression: Compression,
    pub row_group_rows: usize,
    pub row_group_bytes: usize,
    pub sync_interval_ns: u64,
}

pub(crate) fn segment_runtime_parts(config: &CaptureConfig) -> Result<SegmentRuntimeParts> {
    crate::budget::validate_capture_config(config)?;
    if !config.lifecycle.is_segment_mode() {
        bail!("segment sink requires output.lifecycle.mode = segment");
    }
    if !config.catalog_uri.starts_with("file://") {
        bail!("segment lifecycle requires a file:// catalog_uri");
    }
    // Validate seal schedule at sink construction (actor owns the wall-clock timer).
    let _ = config.lifecycle.resolved_seal()?;

    let uri = config
        .catalog_uri
        .strip_prefix("file://")
        .unwrap_or(&config.catalog_uri);
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

    Ok(SegmentRuntimeParts {
        catalog,
        local_root: PathBuf::from(uri),
        row_group_rows: config.lifecycle.batch_row_threshold(config.flush_rows),
        row_group_bytes: config.max_buffer_bytes,
        sync_interval_ns: config
            .lifecycle
            .durability
            .sync_interval_ms
            .saturating_mul(1_000_000),
        compression,
    })
}

pub(crate) fn catalog_fs_directory(
    local_root: &Path,
    catalog: &ParquetDataCatalog,
    type_name: &str,
    identifier: Option<&str>,
) -> Result<PathBuf> {
    let made = catalog.make_path(type_name, identifier)?;
    let made_path = PathBuf::from(&made);
    if made_path.is_absolute() {
        return Ok(made_path);
    }
    Ok(local_root.join(made_path))
}

pub(crate) fn catalog_fs_custom_directory(
    local_root: &Path,
    catalog: &ParquetDataCatalog,
    type_name: &str,
    identifier: Option<&str>,
) -> Result<PathBuf> {
    let made = catalog.make_path_custom_data(type_name, identifier)?;
    let made_path = PathBuf::from(&made);
    if made_path.is_absolute() {
        return Ok(made_path);
    }
    Ok(local_root.join(made_path))
}

pub(crate) fn writer_props(
    compression: Compression,
    row_group_rows: usize,
    row_group_bytes: usize,
) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(row_group_rows))
        .set_max_row_group_bytes(Some(row_group_bytes))
        .build()
}

pub(crate) fn part_path_for(directory: &Path, open_ts_ns: u64) -> PathBuf {
    directory.join(format!(
        "{open_ts_ns}-{}{PART_SUFFIX}",
        nautilus_core::UUID4::new()
    ))
}

pub(crate) fn open_arrow_writer(
    part_path: &Path,
    schema: arrow::datatypes::SchemaRef,
    compression: Compression,
    row_group_rows: usize,
    row_group_bytes: usize,
) -> Result<ArrowWriter<File>> {
    if let Some(parent) = part_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(part_path)?;
    Ok(ArrowWriter::try_new(
        file,
        schema,
        Some(writer_props(compression, row_group_rows, row_group_bytes)),
    )?)
}

pub(crate) fn min_max_ts_init_from_reader(
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
        let (batch_min, batch_max) = ts_init_range_from_batch(&batch)?;
        min_ts_ns = Some(min_ts_ns.map_or(batch_min, |current| current.min(batch_min)));
        max_ts_ns = Some(max_ts_ns.map_or(batch_max, |current| current.max(batch_max)));
    }

    Ok(match (saw_rows, min_ts_ns, max_ts_ns) {
        (true, Some(min_ts), Some(max_ts)) => Some((min_ts, max_ts)),
        _ => None,
    })
}

pub(crate) fn ts_init_range_from_batch(batch: &RecordBatch) -> Result<(u64, u64)> {
    let column_index = batch
        .schema()
        .fields()
        .iter()
        .position(|field| field.name() == "ts_init")
        .ok_or_else(|| anyhow!("batch missing ts_init column"))?;
    let column = batch.column(column_index);
    let Some(column) = column.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Err(anyhow!(
            "ts_init column must use Timestamp(Nanosecond, Some(\"UTC\"))"
        ));
    };
    let values = {
        (0..column.len())
            .map(|index| {
                anyhow::ensure!(!column.is_null(index), "ts_init column contains null");
                u64::try_from(column.value(index))
                    .map_err(|_| anyhow!("ts_init column contains a negative timestamp"))
            })
            .collect::<Result<Vec<_>>>()?
    };
    if values.is_empty() {
        bail!("ts_init column is empty");
    }
    let mut min_ts = values[0];
    let mut max_ts = min_ts;
    for value in values.into_iter().skip(1) {
        min_ts = min_ts.min(value);
        max_ts = max_ts.max(value);
    }
    Ok((min_ts, max_ts))
}

/// Close writer (caller), rename `.part` → catalog parquet name, return path + size.
pub(crate) fn rename_part_to_catalog_parquet(
    part_path: &Path,
    directory: &Path,
    min_ts_ns: u64,
    max_ts_ns: u64,
) -> Result<(PathBuf, u64)> {
    let final_name = timestamps_to_filename(UnixNanos::from(min_ts_ns), UnixNanos::from(max_ts_ns));
    // Native accepts a single alphanumeric replay identity suffix. Deriving it
    // from the part identity makes publication idempotent and avoids equal-time
    // filename collisions without changing a market timestamp.
    let identity: String = part_path
        .file_name()
        .context("missing part filename")?
        .to_string_lossy()
        .trim_end_matches(PART_SUFFIX)
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    let final_name = final_name.replace(".parquet", &format!("_{identity}.parquet"));
    let final_path = directory.join(final_name);
    if let Err(error) = fs::hard_link(part_path, &final_path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let same_file = fs::metadata(part_path)
                .ok()
                .zip(fs::metadata(&final_path).ok())
                .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino());
            if !same_file {
                return Err(error).with_context(|| {
                    format!(
                        "failed to publish without overwriting {}",
                        final_path.display()
                    )
                });
            }
        }
        #[cfg(not(unix))]
        return Err(error).with_context(|| format!("failed to publish {}", final_path.display()));
    }
    fs::remove_file(part_path)?;
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    let bytes = fs::metadata(&final_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    Ok((final_path, bytes))
}

pub(crate) fn finalize_orphan_part_file(part_path: &Path) -> Result<Option<PathBuf>> {
    let metadata = fs::metadata(part_path)?;
    if metadata.len() == 0 {
        let _ = fs::remove_file(part_path);
        return Ok(None);
    }

    let file = File::open(part_path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    let Some((min_ts_ns, max_ts_ns)) = min_max_ts_init_from_reader(&mut reader)? else {
        let _ = fs::remove_file(part_path);
        return Ok(None);
    };
    let directory = part_path
        .parent()
        .expect("part path always has a parent")
        .to_path_buf();
    let (final_path, _) =
        rename_part_to_catalog_parquet(part_path, &directory, min_ts_ns, max_ts_ns)?;
    Ok(Some(final_path))
}

pub(crate) fn walk_orphan_part_files(dir: &Path, orphans: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_orphan_part_files(&path, orphans)?;
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if file_name.ends_with(PART_SUFFIX) {
            orphans.push(path);
        }
    }
    Ok(())
}

pub(crate) fn collect_orphan_part_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut orphans = Vec::new();
    walk_orphan_part_files(root, &mut orphans)?;
    orphans.sort();
    Ok(orphans)
}

pub(crate) fn update_ts_bounds(min_ts_ns: &mut u64, max_ts_ns: &mut u64, lo: u64, hi: u64) {
    if *min_ts_ns == 0 {
        *min_ts_ns = lo;
    } else {
        *min_ts_ns = (*min_ts_ns).min(lo);
    }
    *max_ts_ns = (*max_ts_ns).max(hi);
}

/// Open `*.parquet.part` writer plus seal/tick bookkeeping shared by market and custom sinks.
#[derive(Debug)]
pub(crate) struct ActivePart {
    pub directory: PathBuf,
    pub part_path: PathBuf,
    pub writer: ArrowWriter<File>,
    pub min_ts_ns: u64,
    pub max_ts_ns: u64,
    pub row_count: u64,
    pub last_sync_ns: u64,
    /// Soft cap on flushed row groups; seal+reopen when reached (see [`ROW_GROUP_ROLL_THRESHOLD`]).
    row_group_roll_threshold: usize,
}

impl ActivePart {
    /// Retire an uncertain encoder without retrying or exposing its bytes as a clean file.
    pub(crate) fn quarantine(self) {
        let path = self.part_path.clone();
        drop(self);
        quarantine_part_file(&path);
    }
    pub(crate) fn open(
        directory: PathBuf,
        open_ts_ns: u64,
        schema: arrow::datatypes::SchemaRef,
        compression: Compression,
        row_group_rows: usize,
        row_group_bytes: usize,
    ) -> Result<Self> {
        let part_path = part_path_for(&directory, open_ts_ns);
        let writer = open_arrow_writer(
            &part_path,
            schema,
            compression,
            row_group_rows,
            row_group_bytes,
        )?;
        Ok(Self {
            directory,
            part_path,
            writer,
            min_ts_ns: 0,
            max_ts_ns: 0,
            row_count: 0,
            last_sync_ns: open_ts_ns,
            row_group_roll_threshold: ROW_GROUP_ROLL_THRESHOLD,
        })
    }

    pub(crate) fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.row_count += batch.num_rows() as u64;
        Ok(())
    }

    pub(crate) fn note_ts_range(&mut self, lo: u64, hi: u64) {
        update_ts_bounds(&mut self.min_ts_ns, &mut self.max_ts_ns, lo, hi);
    }

    /// Number of row groups already finalized into the open part file.
    #[must_use]
    pub(crate) fn flushed_row_group_count(&self) -> usize {
        self.writer.flushed_row_groups().len()
    }

    /// True when this part should seal and reopen before more appends.
    #[must_use]
    pub(crate) fn needs_capacity_roll(&self) -> bool {
        self.flushed_row_group_count() >= self.row_group_roll_threshold
    }

    /// Durability only: fsync bytes already on the part file.
    ///
    /// **Must not** call [`ArrowWriter::flush`] here. That finalizes a parquet row
    /// group; doing so every `sync_interval` exhausts the format limit of
    /// [`crate::lifecycle::PARQUET_MAX_ROW_GROUPS`] on high-rate streams
    /// (e.g. Deribit BookSummary). Row groups close when they hit
    /// `max_row_group_row_count`, on capacity roll, or on seal/close.
    pub(crate) fn tick(&mut self, sync_interval_ns: u64, now_ns: u64) -> Result<()> {
        if sync_interval_ns > 0 && now_ns.saturating_sub(self.last_sync_ns) >= sync_interval_ns {
            self.writer.inner_mut().sync_all()?;
            self.last_sync_ns = now_ns;
        }
        Ok(())
    }

    /// Close the writer and rename `.part` → catalog parquet (or drop empty parts).
    pub(crate) fn seal(self) -> Result<FlushResult> {
        if self.row_count == 0 {
            let _ = self.writer.into_inner();
            let _ = fs::remove_file(&self.part_path);
            return Ok(FlushResult::default());
        }

        let Self {
            directory,
            part_path,
            writer,
            min_ts_ns,
            max_ts_ns,
            row_count,
            ..
        } = self;
        let result = (|| {
            writer.close()?;
            // A periodic sync cannot cover a footer which did not exist yet.
            File::open(&part_path)?.sync_all()?;
            let (final_path, bytes) =
                rename_part_to_catalog_parquet(&part_path, &directory, min_ts_ns, max_ts_ns)?;
            Ok(FlushResult {
                files: vec![final_path],
                rows: row_count as usize,
                bytes,
            })
        })();
        if result.is_err() {
            quarantine_part_file(&part_path);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn set_row_group_roll_threshold_for_test(&mut self, threshold: usize) {
        self.row_group_roll_threshold = threshold.max(1);
    }
}

fn quarantine_part_file(path: &Path) {
    if let Err(error) = fs::rename(path, path.with_extension("failed")) {
        log::error!(
            "catalog-capture: failed to quarantine {}: {error}",
            path.display()
        );
    }
}

/// Seal crash leftovers under `root`, skipping paths still held by live writers.
pub(crate) fn recover_orphans_under(
    root: &Path,
    is_active: impl Fn(&Path) -> bool,
) -> Result<usize> {
    let mut recovered = 0usize;
    for part_path in collect_orphan_part_files(root)? {
        if is_active(&part_path) {
            continue;
        }
        match finalize_orphan_part_file(&part_path) {
            Ok(Some(_)) => recovered += 1,
            Ok(None) => {}
            Err(err) => {
                log::warn!(
                    "catalog-capture: skipping unrecoverable orphan segment {}: {err}",
                    part_path.display()
                );
            }
        }
    }
    Ok(recovered)
}

/// Tick every open part in a partition map (values expose `.part: ActivePart`).
pub(crate) fn tick_parts_map<V>(
    segments: &mut std::collections::HashMap<String, V>,
    sync_interval_ns: u64,
    now_ns: u64,
    part_mut: impl Fn(&mut V) -> &mut ActivePart,
) -> Result<FlushResult> {
    collect_flush_results(segments.values_mut().map(|segment| {
        part_mut(segment)
            .tick(sync_interval_ns, now_ns)
            .map(|_| FlushResult::default())
    }))
}

#[cfg(test)]
mod publication_tests {
    use super::*;

    #[test]
    fn equal_timestamp_publication_preserves_both_files_and_retry_is_idempotent() {
        let root =
            std::env::temp_dir().join(format!("publication-{}", nautilus_core::UUID4::new()));
        fs::create_dir_all(&root).unwrap();
        let first = part_path_for(&root, 100);
        let second = part_path_for(&root, 100);
        fs::write(&first, b"first").unwrap();
        fs::write(&second, b"second").unwrap();
        let (published, _) = rename_part_to_catalog_parquet(&first, &root, 100, 100).unwrap();
        let (other, _) = rename_part_to_catalog_parquet(&second, &root, 100, 100).unwrap();
        assert_ne!(published, other);
        assert_eq!(fs::read(&published).unwrap(), b"first");
        assert_eq!(fs::read(other).unwrap(), b"second");
        #[cfg(unix)]
        {
            // Model process death after link succeeds but before the part is unlinked.
            fs::hard_link(&published, &first).unwrap();
            let (retried, _) = rename_part_to_catalog_parquet(&first, &root, 100, 100).unwrap();
            assert_eq!(retried, published);
            assert!(!first.exists());
        }
        // A different inode at the same destination must never be overwritten.
        fs::write(&first, b"unrelated").unwrap();
        assert!(rename_part_to_catalog_parquet(&first, &root, 100, 100).is_err());
        assert_eq!(fs::read(published).unwrap(), b"first");
        assert_eq!(fs::read(first).unwrap(), b"unrelated");
        fs::remove_dir_all(root).unwrap();
    }
}
