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

use std::{collections::HashMap, path::PathBuf, time::Instant};

use anyhow::Result;

use crate::{
    buffer::PartitionBuffer,
    config::CaptureConfig,
    failures::{FailureOperation, FailureTracker},
    item::CaptureItem,
    metrics::{CaptureMetrics, FlushReason},
    sink::CaptureSink,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushResult {
    pub files: Vec<PathBuf>,
    pub rows: usize,
    pub bytes: u64,
}

#[derive(Debug)]
pub struct CaptureRuntime<T, S> {
    pub config: CaptureConfig,
    pub sink: S,
    pub metrics: CaptureMetrics,
    pub failures: FailureTracker,
    pub partitions: HashMap<String, PartitionBuffer<T>>,
}

impl<T, S> CaptureRuntime<T, S>
where
    S: CaptureSink<T>,
{
    pub fn new(config: CaptureConfig, sink: S) -> Self {
        Self {
            failures: FailureTracker::new(config.max_active_partitions),
            config,
            sink,
            metrics: CaptureMetrics::default(),
            partitions: HashMap::new(),
        }
    }

    pub fn submit(&mut self, item: CaptureItem<T>) -> Result<FlushResult> {
        let key = item.partition_key.stable_key();
        let mut result = FlushResult::default();

        if !self.partitions.contains_key(&key)
            && self.partitions.len() >= self.config.max_active_partitions
        {
            let flushed = self.flush_oldest_partition();
            if flushed.is_err() {
                // This item has not entered a partition yet. The oldest partition's
                // write failure and this rejected input are distinct affected scopes.
                self.failures.record(
                    Some(&key),
                    FailureOperation::Admission,
                    Some((item.event_ts_ns, item.event_ts_ns)),
                );
            }
            result = merge_flush_results(result, flushed?);
        }

        let row_threshold = self
            .config
            .lifecycle
            .batch_row_threshold(self.config.flush_rows);
        let flush_reason = {
            let partition = self.partitions.entry(key.clone()).or_default();
            partition.push(
                item.payload,
                item.event_ts_ns,
                item.estimated_bytes,
                item.row_count,
            );
            partition.should_flush_reason(row_threshold, self.config.max_buffer_bytes)
        };

        self.metrics.accepted_items += 1;

        if let Some(reason) = flush_reason {
            result = merge_flush_results(result, self.flush_partition_with_reason(&key, reason)?);
        }

        while self.total_pending_bytes() > self.config.max_total_buffer_bytes {
            let before = self.total_pending_bytes();
            result = merge_flush_results(result, self.flush_oldest_partition()?);
            if self.total_pending_bytes() >= before {
                break;
            }
        }

        self.sync_active_partitions();
        Ok(result)
    }

    pub fn submit_initialization(&mut self, item: CaptureItem<T>) -> Result<FlushResult> {
        let key = item.partition_key.stable_key();
        let started = Instant::now();
        let written = self.sink.write_initialization(&key, item.payload);
        self.metrics.sink_write.record(started.elapsed());
        match written {
            Ok(result) => {
                self.metrics.flushed_batches += u64::from(result.rows > 0);
                self.metrics.flushed_rows += result.rows as u64;
                self.metrics.completed_files += result.files.len() as u64;
                self.metrics.completed_file_bytes += result.bytes;
                Ok(result)
            }
            Err(error) => {
                self.failures
                    .record(Some(&key), FailureOperation::Write, None);
                let _ = self.break_continuity(Some(&key));
                Err(error)
            }
        }
    }

    pub fn flush_partition(&mut self, key: &str) -> Result<FlushResult> {
        self.flush_partition_with_reason(key, FlushReason::Manual)
    }

    pub fn flush_partition_with_reason(
        &mut self,
        key: &str,
        reason: FlushReason,
    ) -> Result<FlushResult> {
        let Some(partition) = self.partitions.remove(key) else {
            self.sync_active_partitions();
            return Ok(FlushResult::default());
        };

        if partition.items.is_empty() {
            self.sync_active_partitions();
            return Ok(FlushResult::default());
        }

        let rows = partition.pending_rows;
        let started = Instant::now();
        let written = self.sink.write_batch(key, partition.items);
        self.metrics.sink_write.record(started.elapsed());
        if written.is_err() {
            self.failures.record(
                Some(key),
                FailureOperation::Write,
                partition.min_ts_ns.zip(partition.max_ts_ns),
            );
            // The consumed batch cannot be retried safely. End its replay window
            // immediately, before any already-queued later messages are processed.
            if self.sink.break_continuity(Some(key)).is_err() {
                self.failures
                    .record(Some(key), FailureOperation::Seal, None);
            }
        }
        self.sync_active_partitions();
        let files = written?;
        let bytes = files
            .iter()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|metadata| metadata.len())
            .sum::<u64>();

        self.metrics.flushed_batches += 1;
        self.metrics.flushed_rows += rows as u64;
        self.metrics.completed_files += files.len() as u64;
        self.metrics.completed_file_bytes += bytes;
        self.metrics.flush_reasons.record(reason);
        self.sync_active_partitions();

        Ok(FlushResult { files, rows, bytes })
    }

    pub fn flush_all(&mut self) -> Result<FlushResult> {
        self.flush_all_with_reason(FlushReason::Manual)
    }

    pub fn flush_all_with_reason(&mut self, reason: FlushReason) -> Result<FlushResult> {
        let keys: Vec<String> = self.partitions.keys().cloned().collect();
        let result = collect_flush_results(
            keys.iter()
                .map(|key| self.flush_partition_with_reason(key, reason)),
        );
        self.sync_active_partitions();
        result
    }

    fn sync_active_partitions(&mut self) {
        self.metrics.active_partitions = self
            .partitions
            .values()
            .filter(|partition| !partition.items.is_empty())
            .count() as u64;
        self.metrics.buffered_bytes = self.total_pending_bytes() as u64;
    }

    fn total_pending_bytes(&self) -> usize {
        self.partitions
            .values()
            .map(|partition| partition.pending_bytes)
            .sum()
    }

    fn oldest_partition_key(&self) -> Option<String> {
        self.partitions
            .iter()
            .filter(|(_, partition)| !partition.items.is_empty())
            .min_by(|(left_key, left), (right_key, right)| {
                let left_ts = left.min_ts_ns.unwrap_or(u64::MAX);
                let right_ts = right.min_ts_ns.unwrap_or(u64::MAX);
                left_ts.cmp(&right_ts).then_with(|| left_key.cmp(right_key))
            })
            .map(|(key, _)| key.clone())
    }

    fn flush_oldest_partition(&mut self) -> Result<FlushResult> {
        let Some(key) = self.oldest_partition_key() else {
            return Ok(FlushResult::default());
        };
        self.flush_partition_with_reason(&key, FlushReason::Budget)
    }

    pub fn on_tick(&mut self, now_ns: u64) -> Result<FlushResult> {
        let started = Instant::now();
        let result = self.sink.on_tick(now_ns);
        self.metrics.sink_sync.record(started.elapsed());
        if result.is_err() {
            self.failures.record(None, FailureOperation::Sync, None);
            let _ = self.break_continuity(None);
        }
        let result = result?;
        if !result.files.is_empty() {
            self.metrics.flushed_batches += 1;
            self.metrics.completed_files += result.files.len() as u64;
            self.metrics.completed_file_bytes += result.bytes;
            self.metrics.flush_reasons.record(FlushReason::Seal);
        }
        Ok(result)
    }

    /// Close the old replay window before processing later FIFO entries.
    pub fn break_continuity(&mut self, key: Option<&str>) -> Result<FlushResult> {
        let flushed = match key {
            Some(key) => self.flush_partition(key),
            None => self.flush_all(),
        };
        let started = Instant::now();
        let sealed = self.sink.break_continuity(key);
        self.metrics.sink_seal.record(started.elapsed());
        if sealed.is_err() {
            self.failures.record(key, FailureOperation::Seal, None);
        }
        if let Ok(result) = &sealed {
            self.metrics.completed_files += result.files.len() as u64;
            self.metrics.completed_file_bytes += result.bytes;
        }
        collect_flush_results([flushed, sealed])
    }

    pub fn seal_all(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(FlushReason::Seal, |sink| sink.seal_all())
    }

    pub fn seal_partition(&mut self, key: &str) -> Result<FlushResult> {
        let flushed = self.flush_partition_with_reason(key, FlushReason::Seal);
        let started = Instant::now();
        let sealed = self.sink.seal_partition(key);
        self.metrics.sink_seal.record(started.elapsed());
        if sealed.is_err() {
            self.failures
                .record(Some(key), FailureOperation::Seal, None);
        }
        if let Ok(sealed) = &sealed
            && !sealed.files.is_empty()
        {
            self.metrics.flushed_batches += 1;
            self.metrics.completed_files += sealed.files.len() as u64;
            self.metrics.completed_file_bytes += sealed.bytes;
            self.metrics.flush_reasons.record(FlushReason::Seal);
        }
        collect_flush_results([flushed, sealed])
    }

    pub fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(FlushReason::Shutdown, |sink| sink.seal_all_for_shutdown())
    }

    fn seal_all_internal(
        &mut self,
        reason: FlushReason,
        seal: impl FnOnce(&mut S) -> Result<FlushResult>,
    ) -> Result<FlushResult> {
        let flushed = self.flush_all_with_reason(reason);
        let started = Instant::now();
        let sealed = seal(&mut self.sink);
        self.metrics.sink_seal.record(started.elapsed());
        if sealed.is_err() {
            self.failures.record(None, FailureOperation::Seal, None);
        }
        if let Ok(sealed) = &sealed
            && !sealed.files.is_empty()
        {
            self.metrics.flushed_batches += 1;
            self.metrics.completed_files += sealed.files.len() as u64;
            self.metrics.completed_file_bytes += sealed.bytes;
            self.metrics.flush_reasons.record(reason);
        }

        collect_flush_results([flushed, sealed])
    }
}

/// Attempt every partition even when an earlier operation fails.
pub(crate) fn collect_flush_results(
    results: impl IntoIterator<Item = Result<FlushResult>>,
) -> Result<FlushResult> {
    let mut flushed = FlushResult::default();
    let mut failure = None;
    for result in results {
        match result {
            Ok(result) => flushed = merge_flush_results(flushed, result),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    match failure {
        Some(error) => Err(error),
        None => Ok(flushed),
    }
}

fn merge_flush_results(left: FlushResult, right: FlushResult) -> FlushResult {
    FlushResult {
        files: left.files.into_iter().chain(right.files).collect(),
        rows: left.rows.saturating_add(right.rows),
        bytes: left.bytes.saturating_add(right.bytes),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyhow::Result;

    use super::{CaptureRuntime, FlushResult};
    use crate::{
        config::CaptureConfig,
        item::{CaptureItem, PartitionKey},
        sink::CaptureSink,
    };

    #[derive(Default)]
    struct TestSink;

    impl CaptureSink<u64> for TestSink {
        fn write_batch(&mut self, _partition_key: &str, _batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn one_failed_partition_still_flushes_and_seals_healthy_partitions() {
        #[derive(Default)]
        struct PartialFailure {
            rows: Vec<u64>,
            sealed: bool,
        }
        impl CaptureSink<u64> for PartialFailure {
            fn write_batch(&mut self, _: &str, rows: Vec<u64>) -> Result<Vec<PathBuf>> {
                if rows.contains(&1) {
                    anyhow::bail!("one partition failed");
                }
                self.rows.extend(rows);
                Ok(Vec::new())
            }
            fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
                self.sealed = true;
                Ok(FlushResult::default())
            }
        }
        let mut runtime = CaptureRuntime::new(CaptureConfig::default(), PartialFailure::default());
        for value in [1, 2] {
            runtime
                .submit(CaptureItem {
                    partition_key: PartitionKey::market_data("test", value),
                    event_ts_ns: value,
                    init_ts_ns: Some(value),
                    row_count: 1,
                    estimated_bytes: 8,
                    payload: value,
                })
                .unwrap();
        }
        assert!(runtime.seal_all_for_shutdown().is_err());
        assert_eq!(runtime.sink.rows, [2]);
        assert!(runtime.sink.sealed);
        assert!(runtime.partitions.is_empty());
        assert_eq!(runtime.metrics.buffered_bytes, 0);
        let snapshot = runtime.failures.snapshot();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(
            snapshot.records[0].partition.as_deref(),
            Some(PartitionKey::market_data("test", 1).stable_key().as_str())
        );
        assert_eq!(runtime.metrics.sink_write.count, 2);
        assert_eq!(runtime.metrics.sink_seal.count, 1);
    }

    #[test]
    fn seal_failure_has_partition_scope_and_sync_failure_has_unknown_family_scope() {
        #[derive(Default)]
        struct ControlFailure;
        impl CaptureSink<u64> for ControlFailure {
            fn write_batch(&mut self, _: &str, _: Vec<u64>) -> Result<Vec<PathBuf>> {
                Ok(Vec::new())
            }
            fn seal_partition(&mut self, _: &str) -> Result<FlushResult> {
                anyhow::bail!("seal failed")
            }
            fn on_tick(&mut self, _: u64) -> Result<FlushResult> {
                anyhow::bail!("sync failed")
            }
        }
        let mut runtime = CaptureRuntime::new(CaptureConfig::default(), ControlFailure);
        let mut cursor = 0;
        assert!(runtime.seal_partition("A").is_err());
        let affected = runtime.failures.changed_partitions(&mut cursor).unwrap();
        assert!(affected.contains("A"));
        assert!(!affected.contains("B"));
        assert!(runtime.on_tick(0).is_err());
        assert_eq!(
            runtime.failures.changed_partitions(&mut cursor),
            Some(crate::failures::AffectedPartitions::Family)
        );
        assert_eq!(runtime.metrics.sink_seal.count, 2);
        assert_eq!(runtime.metrics.sink_sync.count, 1);
    }

    #[test]
    fn active_partitions_counts_only_buffered_partitions() {
        let mut runtime = CaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10,
                ..CaptureConfig::default()
            },
            TestSink,
        );

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "A"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                row_count: 1,
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 2,
                init_ts_ns: Some(2),
                row_count: 1,
                estimated_bytes: 8,
                payload: 2,
            })
            .expect("submit B");
        assert_eq!(runtime.metrics.active_partitions, 2);

        let key_a = PartitionKey::market_data("quotes", "A").stable_key();
        runtime.flush_partition(&key_a).expect("flush A");
        assert_eq!(runtime.metrics.active_partitions, 1);

        runtime.flush_all().expect("flush all");
        assert_eq!(runtime.metrics.active_partitions, 0);
    }

    #[test]
    fn max_active_partitions_flushes_oldest_before_opening_new_partition() {
        let mut runtime = CaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10,
                max_active_partitions: 1,
                max_total_buffer_bytes: 1024 * 1024,
                ..CaptureConfig::default()
            },
            TestSink,
        );

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "A"),
                event_ts_ns: 100,
                init_ts_ns: Some(100),
                row_count: 1,
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 200,
                init_ts_ns: Some(200),
                row_count: 1,
                estimated_bytes: 8,
                payload: 2,
            })
            .expect("submit B");

        assert_eq!(runtime.metrics.flush_reasons.budget, 1);
        assert_eq!(runtime.metrics.active_partitions, 1);
        assert!(
            runtime
                .partitions
                .contains_key(&PartitionKey::market_data("quotes", "B").stable_key())
        );
    }

    #[test]
    fn max_total_buffer_bytes_flushes_oldest_until_under_cap() {
        let mut runtime = CaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10_000,
                max_buffer_bytes: 200,
                max_total_buffer_bytes: 150,
                max_active_partitions: 16,
                ..CaptureConfig::default()
            },
            TestSink,
        );

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "A"),
                event_ts_ns: 100,
                init_ts_ns: Some(100),
                row_count: 1,
                estimated_bytes: 100,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 200,
                init_ts_ns: Some(200),
                row_count: 1,
                estimated_bytes: 100,
                payload: 2,
            })
            .expect("submit B");

        assert!(runtime.metrics.flush_reasons.budget >= 1);
        assert!(runtime.total_pending_bytes() <= 150);
    }

    #[derive(Default)]
    struct SealTrackingSink {
        flush_rows: usize,
        seal_rows: usize,
    }

    impl CaptureSink<u64> for SealTrackingSink {
        fn write_batch(&mut self, _partition_key: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            self.flush_rows += batch.len();
            Ok(vec![PathBuf::from(format!(
                "flush-{}.parquet",
                self.flush_rows
            ))])
        }

        fn seal_all(&mut self) -> Result<FlushResult> {
            self.seal_rows = 3;
            Ok(FlushResult {
                files: vec![PathBuf::from("sealed.parquet")],
                rows: self.seal_rows,
                bytes: 30,
            })
        }

        fn is_segment_mode(&self) -> bool {
            true
        }
    }

    #[test]
    fn seal_all_internal_merges_flush_and_seal_results() {
        let mut runtime = CaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10_000,
                ..CaptureConfig::default()
            },
            SealTrackingSink::default(),
        );

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "A"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                row_count: 1,
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit");

        let result = runtime.seal_all().expect("seal");
        assert_eq!(result.rows, 4, "rows should include flush (1) and seal (3)");
        assert_eq!(
            result.files.len(),
            2,
            "files should include flush and seal outputs"
        );
        assert_eq!(result.bytes, 30, "bytes should include sealed file payload");
    }
}
