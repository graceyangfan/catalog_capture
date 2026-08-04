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

use anyhow::{bail, Result};
use catalog_capture_core::{CompressionKind, LayoutCompatibility, LifecycleConfig, OverflowPolicy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_catalog_uri")]
    pub catalog_uri: String,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_layout_compatibility")]
    pub layout_compatibility: String,
    #[serde(default = "default_flush_rows")]
    pub flush_rows: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    #[serde(default = "default_max_total_buffer_bytes")]
    pub max_total_buffer_bytes: usize,
    #[serde(default = "default_max_active_partitions")]
    pub max_active_partitions: usize,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_overflow_policy")]
    pub overflow_policy: String,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            catalog_uri: default_catalog_uri(),
            compression: default_compression(),
            layout_compatibility: default_layout_compatibility(),
            flush_rows: default_flush_rows(),
            flush_interval_ms: default_flush_interval_ms(),
            max_buffer_bytes: default_max_buffer_bytes(),
            max_total_buffer_bytes: default_max_total_buffer_bytes(),
            max_active_partitions: default_max_active_partitions(),
            queue_capacity: default_queue_capacity(),
            overflow_policy: default_overflow_policy(),
            lifecycle: LifecycleConfig::default(),
        }
    }
}

fn default_catalog_uri() -> String {
    "file:///tmp/catalog-capture".to_string()
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn default_layout_compatibility() -> String {
    "rust_canonical_only".to_string()
}

const fn default_flush_rows() -> usize {
    5_000
}

const fn default_flush_interval_ms() -> u64 {
    1_000
}

const fn default_max_buffer_bytes() -> usize {
    32 * 1024 * 1024
}

const fn default_max_total_buffer_bytes() -> usize {
    512 * 1024 * 1024
}

const fn default_max_active_partitions() -> usize {
    128
}

const fn default_queue_capacity() -> usize {
    10_000
}

fn default_overflow_policy() -> String {
    "drop_oldest".to_string()
}

pub(crate) fn parse_compression(value: &str) -> Result<CompressionKind> {
    match value.to_ascii_lowercase().as_str() {
        "snappy" => Ok(CompressionKind::Snappy),
        "zstd" => Ok(CompressionKind::Zstd),
        other => bail!("unsupported compression {other}; expected snappy|zstd"),
    }
}

pub(crate) fn parse_overflow_policy(value: &str) -> Result<OverflowPolicy> {
    match value.to_ascii_lowercase().as_str() {
        "drop_newest" => Ok(OverflowPolicy::DropNewest),
        "drop_oldest" => Ok(OverflowPolicy::DropOldest),
        "fail_fast" => Ok(OverflowPolicy::FailFast),
        other => {
            bail!("unsupported overflow_policy {other}; expected drop_newest|drop_oldest|fail_fast")
        }
    }
}

pub(crate) fn parse_layout_compatibility(value: &str) -> Result<LayoutCompatibility> {
    match value.to_ascii_lowercase().as_str() {
        "rust_canonical_only" => Ok(LayoutCompatibility::RustCanonicalOnly),
        "rust_canonical_with_python_legacy_mirror" => bail!(
            "layout_compatibility `rust_canonical_with_python_legacy_mirror` was removed. \
             Catalog layout is Nautilus Trader Rust ParquetDataCatalog only \
             (set layout_compatibility = \"rust_canonical_only\" or omit the field). \
             Python legacy path mirrors are no longer supported."
        ),
        other => bail!(
            "unsupported layout_compatibility `{other}`; only `rust_canonical_only` is supported \
             (Rust ParquetDataCatalog layout for direct Rust backtest)"
        ),
    }
}
