use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum LayoutCompatibility {
    RustCanonicalOnly,
    RustCanonicalWithPythonLegacyMirror,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaptureConfig {
    pub enabled: bool,
    pub catalog_uri: String,
    pub queue_capacity: usize,
    pub flush_rows: usize,
    pub flush_interval_ms: u64,
    pub max_buffer_bytes: usize,
    pub compression: CompressionKind,
    pub overflow_policy: OverflowPolicy,
    pub layout_compatibility: LayoutCompatibility,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            catalog_uri: String::from("file:///tmp/nautilus-catalog"),
            queue_capacity: 10_000,
            flush_rows: 5_000,
            flush_interval_ms: 1_000,
            max_buffer_bytes: 32 * 1024 * 1024,
            compression: CompressionKind::Snappy,
            overflow_policy: OverflowPolicy::DropNewest,
            layout_compatibility: LayoutCompatibility::RustCanonicalWithPythonLegacyMirror,
        }
    }
}
