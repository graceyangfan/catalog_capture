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
    Shutdown,
    Manual,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushReasonMetrics {
    pub row_threshold: u64,
    pub byte_threshold: u64,
    pub interval: u64,
    pub shutdown: u64,
    pub manual: u64,
}

impl FlushReasonMetrics {
    pub fn record(&mut self, reason: FlushReason) {
        match reason {
            FlushReason::Rows => self.row_threshold += 1,
            FlushReason::Bytes => self.byte_threshold += 1,
            FlushReason::Interval => self.interval += 1,
            FlushReason::Shutdown => self.shutdown += 1,
            FlushReason::Manual => self.manual += 1,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureMetrics {
    pub accepted_items: u64,
    pub dropped_items: u64,
    pub flushed_batches: u64,
    pub flushed_rows: u64,
    pub completed_files: u64,
    pub completed_file_bytes: u64,
    pub active_partitions: u64,
    pub flush_reasons: FlushReasonMetrics,
}

impl CaptureMetrics {
    #[must_use]
    pub fn average_file_bytes(&self) -> u64 {
        self.completed_file_bytes
            .checked_div(self.completed_files)
            .unwrap_or(0)
    }
}
