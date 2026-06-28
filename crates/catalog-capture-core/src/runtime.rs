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

use anyhow::Result;

use crate::{
    buffer::PartitionBuffer,
    config::CaptureConfig,
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
    pub partitions: HashMap<String, PartitionBuffer<T>>,
}

impl<T, S> CaptureRuntime<T, S>
where
    S: CaptureSink<T>,
{
    pub fn new(config: CaptureConfig, sink: S) -> Self {
        Self {
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
            result = merge_flush_results(result, self.flush_oldest_partition()?);
        }

        let row_threshold = self
            .config
            .lifecycle
            .batch_row_threshold(self.config.flush_rows);
        let flush_reason = {
            let partition = self.partitions.entry(key.clone()).or_default();
            partition.push(item.payload, item.event_ts_ns, item.estimated_bytes);
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
        let files = self.sink.write_batch(key, partition.items)?;
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
        let mut result = FlushResult::default();
        let keys: Vec<String> = self.partitions.keys().cloned().collect();

        for key in keys {
            let partial = self.flush_partition_with_reason(&key, reason)?;
            result.rows += partial.rows;
            result.bytes += partial.bytes;
            result.files.extend(partial.files);
        }

        self.sync_active_partitions();
        Ok(result)
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
        let result = self.sink.on_tick(now_ns)?;
        if !result.files.is_empty() {
            self.metrics.flushed_batches += 1;
            self.metrics.flushed_rows += result.rows as u64;
            self.metrics.completed_files += result.files.len() as u64;
            self.metrics.completed_file_bytes += result.bytes;
            self.metrics.flush_reasons.record(FlushReason::Seal);
        }
        Ok(result)
    }

    pub fn seal_all(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(FlushReason::Seal, |sink| sink.seal_all())
    }

    pub fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
        self.seal_all_internal(FlushReason::Shutdown, |sink| sink.seal_all_for_shutdown())
    }

    fn seal_all_internal(
        &mut self,
        reason: FlushReason,
        seal: impl FnOnce(&mut S) -> Result<FlushResult>,
    ) -> Result<FlushResult> {
        let flushed = self.flush_all_with_reason(reason)?;
        let sealed = seal(&mut self.sink)?;
        let sealed_file_count = sealed.files.len();

        if sealed_file_count > 0 {
            self.metrics.flushed_batches += 1;
            self.metrics.flushed_rows += sealed.rows as u64;
            self.metrics.completed_files += sealed_file_count as u64;
            self.metrics.completed_file_bytes += sealed.bytes;
            self.metrics.flush_reasons.record(reason);
        }

        Ok(merge_flush_results(flushed, sealed))
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
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 2,
                init_ts_ns: Some(2),
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
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 200,
                init_ts_ns: Some(200),
                estimated_bytes: 8,
                payload: 2,
            })
            .expect("submit B");

        assert_eq!(runtime.metrics.flush_reasons.budget, 1);
        assert_eq!(runtime.metrics.active_partitions, 1);
        assert!(runtime
            .partitions
            .contains_key(&PartitionKey::market_data("quotes", "B").stable_key()));
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
                estimated_bytes: 100,
                payload: 1,
            })
            .expect("submit A");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "B"),
                event_ts_ns: 200,
                init_ts_ns: Some(200),
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
