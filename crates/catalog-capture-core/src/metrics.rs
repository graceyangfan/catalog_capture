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

use std::time::Duration;

use serde::Serialize;

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

/// Inclusive operation time; maxima expose stalls without per-event histogram allocation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OperationTiming {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}

impl OperationTiming {
    pub fn record(&mut self, elapsed: Duration) {
        let ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.count = self.count.saturating_add(1);
        self.total_ns = self.total_ns.saturating_add(ns);
        self.max_ns = self.max_ns.max(ns);
    }

    fn merge(&mut self, other: &Self) {
        self.count = self.count.saturating_add(other.count);
        self.total_ns = self.total_ns.saturating_add(other.total_ns);
        self.max_ns = self.max_ns.max(other.max_ns);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct CaptureMetrics {
    pub accepted_items: u64,
    pub dropped_items: u64,
    /// Failed worker batches, including encoding, sealing and durability errors.
    pub write_errors: u64,
    /// Workers that exited unexpectedly or have unusable shared state.
    pub failed_workers: u64,
    pub flushed_batches: u64,
    /// Rows passed successfully from partition buffers to the sink, counted once.
    pub flushed_rows: u64,
    pub completed_files: u64,
    pub completed_file_bytes: u64,
    /// Partitions with buffered rows not yet flushed to disk.
    pub active_partitions: u64,
    /// Items waiting in the background capture queue (pre-worker).
    pub queued_items: u64,
    /// Effective ingress limits after applying family overrides.
    pub queue_capacity: u64,
    pub queue_max_bytes: u64,
    /// Estimated payload bytes waiting in the family queue, excluding the worker batch.
    pub queued_bytes: u64,
    /// Lifetime queue high-water marks; short bursts remain visible between scrapes.
    pub peak_queued_items: u64,
    pub peak_queued_bytes: u64,
    /// Summed pending bytes across all partition buffers in this runtime.
    pub buffered_bytes: u64,
    /// Current ingress age; measured with a monotonic clock, not source timestamps.
    pub oldest_queued_ns: u64,
    /// Worker-owned batch, separate from producer queue and partition buffers.
    pub in_flight_items: u64,
    pub in_flight_bytes: u64,
    pub worker_busy_ns: u64,
    pub active_writers: u64,
    pub queue_wait: OperationTiming,
    /// Includes model encoding and Arrow append/compression; excludes later seal/tick.
    pub sink_write: OperationTiming,
    pub sink_sync: OperationTiming,
    pub sink_seal: OperationTiming,
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
        self.write_errors += other.write_errors;
        self.failed_workers += other.failed_workers;
        self.flushed_batches += other.flushed_batches;
        self.flushed_rows += other.flushed_rows;
        self.completed_files += other.completed_files;
        self.completed_file_bytes += other.completed_file_bytes;
        self.active_partitions += other.active_partitions;
        self.queued_items += other.queued_items;
        self.queue_capacity += other.queue_capacity;
        self.queue_max_bytes += other.queue_max_bytes;
        self.queued_bytes += other.queued_bytes;
        // Aggregated peaks sum family maxima; they are not a simultaneous process peak.
        self.peak_queued_items += other.peak_queued_items;
        self.peak_queued_bytes += other.peak_queued_bytes;
        self.buffered_bytes += other.buffered_bytes;
        self.oldest_queued_ns = self.oldest_queued_ns.max(other.oldest_queued_ns);
        self.in_flight_items += other.in_flight_items;
        self.in_flight_bytes += other.in_flight_bytes;
        self.worker_busy_ns = self.worker_busy_ns.max(other.worker_busy_ns);
        self.active_writers += other.active_writers;
        self.queue_wait.merge(&other.queue_wait);
        self.sink_write.merge(&other.sink_write);
        self.sink_sync.merge(&other.sink_sync);
        self.sink_seal.merge(&other.sink_seal);
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
            "accepted={} dropped={} write_errors={} failed_workers={} flushed_rows={} completed_files={} completed_bytes={} \
             active_partitions={} queued_items={} queued_bytes={} peak_queued_items={} peak_queued_bytes={} buffered_bytes={} \
             flush_reasons={{rows:{}, bytes:{}, interval:{}, seal:{}, shutdown:{}, manual:{}, budget:{}}}",
            self.accepted_items,
            self.dropped_items,
            self.write_errors,
            self.failed_workers,
            self.flushed_rows,
            self.completed_files,
            self.completed_file_bytes,
            self.active_partitions,
            self.queued_items,
            self.queued_bytes,
            self.peak_queued_items,
            self.peak_queued_bytes,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timings_sum_counts_and_totals_but_take_maxima_across_families() {
        let mut first = CaptureMetrics::default();
        first.sink_write.record(Duration::from_millis(3));
        let mut second = CaptureMetrics {
            oldest_queued_ns: 10,
            in_flight_bytes: 100,
            ..Default::default()
        };
        second.sink_write.record(Duration::from_millis(5));
        first.merge(&second);
        assert_eq!(first.sink_write.count, 2);
        assert_eq!(first.sink_write.total_ns, 8_000_000);
        assert_eq!(first.sink_write.max_ns, 5_000_000);
        assert_eq!(first.oldest_queued_ns, 10);
        assert_eq!(first.in_flight_bytes, 100);
    }
}
