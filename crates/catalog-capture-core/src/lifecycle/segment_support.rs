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

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, UInt64Array};
use arrow::record_batch::RecordBatch;
use nautilus_core::UnixNanos;
use nautilus_persistence::backend::catalog::{timestamps_to_filename, ParquetDataCatalog};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

use crate::{
    config::{CaptureConfig, CompressionKind},
    lifecycle::ResolvedSealSchedule,
    runtime::FlushResult,
};

/// Must NOT end with `.parquet` (catalog queries match any `*.parquet`).
pub(crate) const PART_SUFFIX: &str = ".parquet.part";

#[derive(Debug)]
pub(crate) struct SegmentRuntimeParts {
    pub catalog: ParquetDataCatalog,
    pub local_root: PathBuf,
    pub seal: Option<ResolvedSealSchedule>,
    pub compression: Compression,
    pub row_group_rows: usize,
    pub sync_interval_ns: u64,
}

pub(crate) fn segment_runtime_parts(config: &CaptureConfig) -> Result<SegmentRuntimeParts> {
    if !config.lifecycle.is_segment_mode() {
        bail!("segment sink requires output.lifecycle.mode = segment");
    }
    if !config.catalog_uri.starts_with("file://") {
        bail!("segment lifecycle requires a file:// catalog_uri");
    }

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
        seal: config.lifecycle.resolved_seal()?,
        row_group_rows: config.lifecycle.batch_row_threshold(config.flush_rows),
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

pub(crate) fn writer_props(compression: Compression, row_group_rows: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(row_group_rows))
        .build()
}

pub(crate) fn part_path_for(directory: &Path, open_ts_ns: u64) -> PathBuf {
    directory.join(format!("{open_ts_ns}{PART_SUFFIX}"))
}

pub(crate) fn open_arrow_writer(
    part_path: &Path,
    schema: arrow::datatypes::SchemaRef,
    compression: Compression,
    row_group_rows: usize,
) -> Result<ArrowWriter<File>> {
    if let Some(parent) = part_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(part_path)?;
    Ok(ArrowWriter::try_new(
        file,
        schema,
        Some(writer_props(compression, row_group_rows)),
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
    let column = batch
        .column(column_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| anyhow!("ts_init column has unexpected type"))?;
    if column.is_empty() {
        bail!("ts_init column is empty");
    }
    let mut min_ts = column.value(0);
    let mut max_ts = min_ts;
    for i in 1..column.len() {
        let value = column.value(i);
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
    let final_name =
        timestamps_to_filename(UnixNanos::from(min_ts_ns), UnixNanos::from(max_ts_ns));
    let final_path = directory.join(final_name);
    fs::rename(part_path, &final_path).with_context(|| {
        format!(
            "failed to seal segment part {}",
            part_path.display()
        )
    })?;
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

pub(crate) fn merge_flush(result: &mut FlushResult, partial: FlushResult) {
    result.rows += partial.rows;
    result.bytes += partial.bytes;
    result.files.extend(partial.files);
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
}

impl ActivePart {
    pub(crate) fn open(
        directory: PathBuf,
        open_ts_ns: u64,
        schema: arrow::datatypes::SchemaRef,
        compression: Compression,
        row_group_rows: usize,
    ) -> Result<Self> {
        let part_path = part_path_for(&directory, open_ts_ns);
        let writer = open_arrow_writer(&part_path, schema, compression, row_group_rows)?;
        Ok(Self {
            directory,
            part_path,
            writer,
            min_ts_ns: 0,
            max_ts_ns: 0,
            row_count: 0,
            last_sync_ns: open_ts_ns,
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

    /// Fsync the open part when `sync_interval_ns` has elapsed.
    pub(crate) fn tick(&mut self, sync_interval_ns: u64, now_ns: u64) -> Result<()> {
        if sync_interval_ns > 0 && now_ns.saturating_sub(self.last_sync_ns) >= sync_interval_ns {
            self.writer.flush()?;
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
        writer.close()?;
        let (final_path, bytes) =
            rename_part_to_catalog_parquet(&part_path, &directory, min_ts_ns, max_ts_ns)?;
        Ok(FlushResult {
            files: vec![final_path],
            rows: row_count as usize,
            bytes,
        })
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
    for segment in segments.values_mut() {
        part_mut(segment).tick(sync_interval_ns, now_ns)?;
    }
    Ok(FlushResult::default())
}
