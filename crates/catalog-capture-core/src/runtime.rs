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
        let flush_reason = {
            let partition = self.partitions.entry(key.clone()).or_default();
            partition.push(item.payload, item.event_ts_ns, item.estimated_bytes);
            partition.should_flush_reason(self.config.flush_rows, self.config.max_buffer_bytes)
        };

        self.metrics.accepted_items += 1;
        self.metrics.active_partitions = self.partitions.len() as u64;

        if let Some(reason) = flush_reason {
            return self.flush_partition_with_reason(&key, reason);
        }

        Ok(FlushResult::default())
    }

    pub fn flush_partition(&mut self, key: &str) -> Result<FlushResult> {
        self.flush_partition_with_reason(key, FlushReason::Manual)
    }

    pub fn flush_partition_with_reason(
        &mut self,
        key: &str,
        reason: FlushReason,
    ) -> Result<FlushResult> {
        let Some(partition) = self.partitions.get_mut(key) else {
            return Ok(FlushResult::default());
        };

        if partition.items.is_empty() {
            return Ok(FlushResult::default());
        }

        let drained = partition.take();
        let rows = drained.pending_rows;
        let files = self.sink.write_batch(drained.items)?;
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

        self.metrics.active_partitions = self
            .partitions
            .values()
            .filter(|partition| !partition.items.is_empty())
            .count() as u64;

        Ok(result)
    }
}
