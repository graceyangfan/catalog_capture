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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{flush_profile::CaptureFlushFamily, lifecycle::LifecycleConfig};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompressionKind {
    Snappy,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropNewest,
    DropOldest,
    FailFast,
}

/// Catalog directory layout is always Nautilus Trader **Rust** `ParquetDataCatalog`
/// canonical paths. Python legacy path mirrors are not supported.
///
/// Retained as a single-variant enum so operators and docs can still name the
/// contract explicitly (`rust_canonical_only`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum LayoutCompatibility {
    /// Write only Rust-canonical catalog paths (required for Rust backtest).
    #[default]
    RustCanonicalOnly,
}

/// Optional ingress-only overrides; partition and encoder budgets are independent.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct QueueLimits {
    pub capacity: Option<usize>,
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureConfig {
    pub enabled: bool,
    pub catalog_uri: String,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    pub queue_capacity: usize,
    /// Ingress byte limit; omission preserves the legacy max_total_buffer_bytes limit.
    #[serde(default)]
    pub queue_max_bytes: Option<usize>,
    #[serde(default)]
    pub queues: BTreeMap<CaptureFlushFamily, QueueLimits>,
    pub flush_rows: usize,
    pub flush_interval_ms: u64,
    /// Per-partition payload buffer cap and Parquet's estimated encoded row-group target.
    pub max_buffer_bytes: usize,
    /// Aggregate partition-buffer cap in one family runtime; also the legacy ingress
    /// fallback when queue_max_bytes is absent. Excludes worker batches and Arrow buffers.
    #[serde(default = "default_max_total_buffer_bytes")]
    pub max_total_buffer_bytes: usize,
    /// Maximum concurrently open partition buffers in one family runtime.
    #[serde(default = "default_max_active_partitions")]
    pub max_active_partitions: usize,
    pub compression: CompressionKind,
    pub overflow_policy: OverflowPolicy,
    /// Always `RustCanonicalOnly`. Field kept for explicit config/docs alignment.
    #[serde(default)]
    pub layout_compatibility: LayoutCompatibility,
}

impl CaptureConfig {
    #[must_use]
    pub fn ingress_max_bytes(&self) -> usize {
        self.queue_max_bytes.unwrap_or(self.max_total_buffer_bytes)
    }
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            catalog_uri: String::from("file://./data/catalog"),
            lifecycle: LifecycleConfig::default(),
            queue_capacity: 10_000,
            queue_max_bytes: None,
            queues: BTreeMap::new(),
            flush_rows: 5_000,
            flush_interval_ms: 1_000,
            max_buffer_bytes: 32 * 1024 * 1024,
            max_total_buffer_bytes: default_max_total_buffer_bytes(),
            max_active_partitions: default_max_active_partitions(),
            compression: CompressionKind::Snappy,
            overflow_policy: OverflowPolicy::DropNewest,
            layout_compatibility: LayoutCompatibility::RustCanonicalOnly,
        }
    }
}

const fn default_max_total_buffer_bytes() -> usize {
    512 * 1024 * 1024
}

const fn default_max_active_partitions() -> usize {
    128
}
