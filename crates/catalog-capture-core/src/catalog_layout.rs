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

//! Nautilus Trader **Rust** catalog path contract (Track L / L5–L6).
//!
//! Capture writes only through `ParquetDataCatalog` under:
//!
//! ```text
//! {catalog_root}/
//!   data/
//!     quotes/…  trades/…  bars/…  mark_prices/…  …
//!     custom/{TypeName}/[{identifier}/]…
//!   metadata/
//!     capture_run.json
//!     …
//! ```
//!
//! Custom **subscribe** and **request** paths share one sink
//! (`write_custom_data_batch`) → both land under `data/custom/{type_name}/`.

use std::path::{Path, PathBuf};

use nautilus_model::instruments::{Instrument, InstrumentAny};

/// Instrument id string used in catalog partition keys.
#[must_use]
pub fn instrument_identifier(instrument: &InstrumentAny) -> String {
    Instrument::id(instrument).to_string()
}

/// Relative directory for a built-in market-data family under the catalog root.
///
/// Example: `market_data_dir("quotes")` → `data/quotes`.
#[must_use]
pub fn market_data_dir(type_name: &str) -> PathBuf {
    PathBuf::from("data").join(type_name)
}

/// Relative directory for custom data (subscribe or request) under the catalog root.
///
/// Mirrors Nautilus `ParquetDataCatalog::make_path_custom_data`:
/// `data/custom/{type_name}[/{identifier}]`.
#[must_use]
pub fn custom_data_dir(type_name: &str, identifier: Option<&str>) -> PathBuf {
    let mut path = PathBuf::from("data").join("custom").join(type_name);
    if let Some(id) = identifier {
        let safe = id.replace("//", "/");
        for segment in safe.split('/').filter(|s| !s.is_empty() && *s != "..") {
            path.push(segment);
        }
    }
    path
}

/// True if `path` (absolute or catalog-relative) is under `data/custom/{type_name}/…`.
///
/// `catalog_root` is used when `path` is absolute; relative paths returned by
/// `ParquetDataCatalog` are matched on the `data/custom/…` suffix.
#[must_use]
pub fn path_is_under_custom_type(catalog_root: &Path, path: &Path, type_name: &str) -> bool {
    path_contains_dir_prefix(path, &custom_data_dir(type_name, None))
        || path.starts_with(catalog_root.join(custom_data_dir(type_name, None)))
}

/// True if `path` is under `data/{family}/…` (e.g. `quotes`).
#[must_use]
pub fn path_is_under_market_family(catalog_root: &Path, path: &Path, family: &str) -> bool {
    path_contains_dir_prefix(path, &market_data_dir(family))
        || path.starts_with(catalog_root.join(market_data_dir(family)))
}

fn path_contains_dir_prefix(path: &Path, relative_prefix: &Path) -> bool {
    let path_s = path.to_string_lossy().replace('\\', "/");
    let prefix_s = relative_prefix.to_string_lossy().replace('\\', "/");
    path_s.contains(&*prefix_s)
}

/// Known CLI custom type names (must match `catalog-capture-cli` registry).
/// Used for path-audit tests so rename drift is visible.
pub const KNOWN_CUSTOM_TYPE_NAMES: &[&str] = &[
    "BinanceFuturesLiquidation",
    "BinanceFuturesTicker",
    "DeribitVolatilityIndex",
    "HyperliquidOpenInterest",
    "DeribitBookSummary",
];

/// Asserts every known custom type name maps to a `data/custom/{name}` relative dir.
pub fn assert_known_custom_type_dirs() {
    for name in KNOWN_CUSTOM_TYPE_NAMES {
        let dir = custom_data_dir(name, None);
        assert_eq!(
            dir,
            PathBuf::from("data").join("custom").join(name),
            "custom type {name} path contract"
        );
        let with_id = custom_data_dir(name, Some("BTC:option"));
        assert!(
            with_id.ends_with("BTC:option")
                || with_id.components().any(|c| c.as_os_str() == "BTC:option"),
            "identifier segment missing for {name}: {}",
            with_id.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{CaptureConfig, CompressionKind, LayoutCompatibility, OverflowPolicy},
        lifecycle::LifecycleConfig,
        sink::NautilusCatalogSink,
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{CustomData, DataType, QuoteTick},
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };
    use nautilus_persistence::test_data::RustTestCustomData;
    use nautilus_serialization::arrow::custom::ensure_custom_data_registered;
    use std::{fs, str::FromStr, sync::Arc};

    fn temp_catalog(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "catalog-layout-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn capture_config(dir: &Path) -> CaptureConfig {
        CaptureConfig {
            enabled: true,
            catalog_uri: format!("file://{}", dir.display()),
            lifecycle: LifecycleConfig::default(),
            queue_capacity: 1_000,
            flush_rows: 10,
            flush_interval_ms: 1_000,
            max_buffer_bytes: 8 * 1024 * 1024,
            max_total_buffer_bytes: 32 * 1024 * 1024,
            max_active_partitions: 16,
            compression: CompressionKind::Snappy,
            overflow_policy: OverflowPolicy::DropOldest,
            layout_compatibility: LayoutCompatibility::RustCanonicalOnly,
        }
    }

    #[test]
    fn known_custom_type_dirs_match_catalog_contract() {
        assert_known_custom_type_dirs();
    }

    #[test]
    fn custom_data_dir_with_identifier_segments() {
        let path = custom_data_dir("DeribitBookSummary", Some("BTC:option"));
        assert_eq!(
            path,
            PathBuf::from("data/custom/DeribitBookSummary/BTC:option")
        );
    }

    #[test]
    fn write_quotes_lands_under_data_quotes() {
        let root = temp_catalog("quotes");
        let sink = NautilusCatalogSink::from_config(&capture_config(&root)).expect("sink");
        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.TEST").expect("id");
        let quote = QuoteTick::new(
            instrument_id,
            Price::from("100.0"),
            Price::from("100.5"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
        );
        let path = sink.write_encoded_batch(vec![quote]).expect("write quotes");
        assert!(
            path_is_under_market_family(&root, &path, "quotes"),
            "expected quotes under data/quotes, got {}",
            path.display()
        );
        assert!(path.extension().is_some_and(|e| e == "parquet"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn write_custom_data_lands_under_data_custom_type() {
        ensure_custom_data_registered::<RustTestCustomData>();
        let root = temp_catalog("custom");
        let sink = NautilusCatalogSink::from_config(&capture_config(&root)).expect("sink");

        let instrument_id = InstrumentId::from_str("RUST.TEST").expect("id");
        let payload = RustTestCustomData {
            instrument_id,
            value: 1.23,
            flag: true,
            ts_event: UnixNanos::from(2_000),
            ts_init: UnixNanos::from(2_000),
        };
        let data_type = DataType::new("RustTestCustomData", None, Some(instrument_id.to_string()));
        let custom = CustomData::new(Arc::new(payload), data_type);

        let path = sink
            .write_custom_data_batch(vec![custom])
            .expect("write custom");
        assert!(
            path_is_under_custom_type(&root, &path, "RustTestCustomData"),
            "expected custom path under data/custom/RustTestCustomData, got {}",
            path.display()
        );
        assert!(path.to_string_lossy().contains("RUST.TEST"));
        assert!(path.extension().is_some_and(|e| e == "parquet"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn subscribe_and_request_share_same_custom_path_contract() {
        // L6: both channels use type_name → data/custom/{type_name}/…
        // Partition keys differ only by in-memory metadata; on-disk path uses type_name.
        let subscribe = custom_data_dir("DeribitVolatilityIndex", Some("btc_usd"));
        let request = custom_data_dir("DeribitBookSummary", Some("BTC:option"));
        assert!(subscribe.starts_with("data/custom/DeribitVolatilityIndex"));
        assert!(request.starts_with("data/custom/DeribitBookSummary"));
        // Different types must not share a directory.
        assert_ne!(subscribe.parent(), request.parent());
    }
}
