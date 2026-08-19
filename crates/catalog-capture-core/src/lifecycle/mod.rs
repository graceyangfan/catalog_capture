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

pub mod row_group_capacity;
pub mod seal;
pub mod segment;
pub mod segment_custom;
pub(crate) mod segment_support;

#[cfg(test)]
mod integration;

pub use row_group_capacity::{
    estimated_row_groups, min_row_group_rows_for_day, seconds_to_hard_limit,
    CLOUD_BOOK_SUMMARY_POLL_ROWS, CLOUD_BOOK_SUMMARY_ROWS_PER_SEC, CUSTOM_MEMORY_FLUSH_ROWS,
    CUSTOM_ROW_GROUP_ROWS, DEFAULT_SEAL_INTERVAL_SECS, PARQUET_MAX_ROW_GROUPS,
    ROW_GROUP_ROLL_THRESHOLD,
};
pub use seal::{
    next_seal_boundary_ns, parse_seal_schedule, parse_seal_timezone, resolve_seal_schedule,
    should_seal_at, ResolvedSealSchedule, SealConfigFile,
};
pub use segment::SegmentCaptureSink;
pub use segment_custom::SegmentCustomDataSink;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleMode {
    /// Immediate catalog parquet per flush — opt-in for short smoke tests only.
    Chunked,
    /// Append `*.parquet.part` and seal on schedule (production default).
    #[default]
    Segment,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentLifecycleConfig {
    #[serde(default = "default_row_group_rows")]
    pub row_group_rows: usize,
}

impl Default for SegmentLifecycleConfig {
    fn default() -> Self {
        Self {
            row_group_rows: default_row_group_rows(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DurabilityConfig {
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            sync_interval_ms: default_sync_interval_ms(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleConfig {
    #[serde(default)]
    pub mode: LifecycleMode,
    #[serde(default)]
    pub segment: SegmentLifecycleConfig,
    #[serde(default)]
    pub durability: DurabilityConfig,
    #[serde(default)]
    pub seal: SealConfigFile,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            mode: LifecycleMode::Segment,
            segment: SegmentLifecycleConfig::default(),
            durability: DurabilityConfig::default(),
            seal: SealConfigFile::default(),
        }
    }
}

impl LifecycleConfig {
    #[must_use]
    pub fn is_segment_mode(&self) -> bool {
        matches!(self.mode, LifecycleMode::Segment)
    }

    #[must_use]
    pub fn batch_row_threshold(&self, fallback_flush_rows: usize) -> usize {
        if self.is_segment_mode() {
            self.segment.row_group_rows.max(1)
        } else {
            fallback_flush_rows.max(1)
        }
    }

    pub fn resolved_seal(&self) -> anyhow::Result<Option<ResolvedSealSchedule>> {
        if !self.is_segment_mode() || !self.seal.enabled {
            return Ok(None);
        }
        resolve_seal_schedule(&self.seal).map(Some)
    }
}

const fn default_row_group_rows() -> usize {
    5_000
}

const fn default_sync_interval_ms() -> u64 {
    1_000
}
