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

pub mod seal;
pub mod segment;

pub use seal::{
    next_seal_boundary_ns, parse_seal_schedule, parse_seal_timezone, resolve_seal_schedule,
    should_seal_at, ResolvedSealSchedule, SealConfigFile,
};
pub use segment::SegmentCaptureSink;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleMode {
    #[default]
    Chunked,
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
            mode: LifecycleMode::Chunked,
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
