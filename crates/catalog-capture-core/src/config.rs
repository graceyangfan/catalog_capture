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

use serde::{Deserialize, Serialize};

use crate::lifecycle::LifecycleConfig;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureConfig {
    pub enabled: bool,
    pub catalog_uri: String,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    pub queue_capacity: usize,
    pub flush_rows: usize,
    pub flush_interval_ms: u64,
    pub max_buffer_bytes: usize,
    /// Summed pending bytes cap across all partitions in one family runtime.
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

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            catalog_uri: String::from("file:///tmp/catalog-capture"),
            lifecycle: LifecycleConfig::default(),
            queue_capacity: 10_000,
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
