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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Rows,
    Bytes,
    Interval,
    Seal,
    Shutdown,
    Manual,
    Budget,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct FlushReasonMetrics {
    pub row_threshold: u64,
    pub byte_threshold: u64,
    pub interval: u64,
    pub seal: u64,
    pub shutdown: u64,
    pub manual: u64,
    pub budget: u64,
}

impl FlushReasonMetrics {
    pub fn record(&mut self, reason: FlushReason) {
        match reason {
            FlushReason::Rows => self.row_threshold += 1,
            FlushReason::Bytes => self.byte_threshold += 1,
            FlushReason::Interval => self.interval += 1,
            FlushReason::Seal => self.seal += 1,
            FlushReason::Shutdown => self.shutdown += 1,
            FlushReason::Manual => self.manual += 1,
            FlushReason::Budget => self.budget += 1,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct CaptureMetrics {
    pub accepted_items: u64,
    pub dropped_items: u64,
    pub flushed_batches: u64,
    pub flushed_rows: u64,
    pub completed_files: u64,
    pub completed_file_bytes: u64,
    /// Partitions with buffered rows not yet flushed to disk.
    pub active_partitions: u64,
    /// Items waiting in the background capture queue (pre-worker).
    pub queued_items: u64,
    /// Summed pending bytes across all partition buffers in this runtime.
    pub buffered_bytes: u64,
    pub flush_reasons: FlushReasonMetrics,
}

impl CaptureMetrics {
    #[must_use]
    pub fn average_file_bytes(&self) -> u64 {
        self.completed_file_bytes
            .checked_div(self.completed_files)
            .unwrap_or(0)
    }

    pub fn merge(&mut self, other: &Self) {
        self.accepted_items += other.accepted_items;
        self.dropped_items += other.dropped_items;
        self.flushed_batches += other.flushed_batches;
        self.flushed_rows += other.flushed_rows;
        self.completed_files += other.completed_files;
        self.completed_file_bytes += other.completed_file_bytes;
        self.active_partitions += other.active_partitions;
        self.queued_items += other.queued_items;
        self.buffered_bytes += other.buffered_bytes;
        self.flush_reasons.row_threshold += other.flush_reasons.row_threshold;
        self.flush_reasons.byte_threshold += other.flush_reasons.byte_threshold;
        self.flush_reasons.interval += other.flush_reasons.interval;
        self.flush_reasons.seal += other.flush_reasons.seal;
        self.flush_reasons.shutdown += other.flush_reasons.shutdown;
        self.flush_reasons.manual += other.flush_reasons.manual;
        self.flush_reasons.budget += other.flush_reasons.budget;
    }

    #[must_use]
    pub fn summary_line(&self) -> String {
        format!(
            "accepted={} dropped={} flushed_rows={} completed_files={} completed_bytes={} \
             active_partitions={} queued_items={} buffered_bytes={} \
             flush_reasons={{rows:{}, bytes:{}, interval:{}, seal:{}, shutdown:{}, manual:{}, budget:{}}}",
            self.accepted_items,
            self.dropped_items,
            self.flushed_rows,
            self.completed_files,
            self.completed_file_bytes,
            self.active_partitions,
            self.queued_items,
            self.buffered_bytes,
            self.flush_reasons.row_threshold,
            self.flush_reasons.byte_threshold,
            self.flush_reasons.interval,
            self.flush_reasons.seal,
            self.flush_reasons.shutdown,
            self.flush_reasons.manual,
            self.flush_reasons.budget,
        )
    }
}
