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

use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use catalog_capture_core::{
    plan::{CapturePlan, CustomDataCaptureSpec},
    CaptureConfig, CompressionKind, LayoutCompatibility, OverflowPolicy,
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::{Params, UnixNanos};
use nautilus_deribit::data_types::{register_deribit_custom_data, DeribitVolatilityIndex};
use nautilus_model::data::{CustomData, DataType};

fn build_capture_config(catalog_dir: &Path) -> CaptureConfig {
    CaptureConfig {
        enabled: true,
        catalog_uri: format!("file://{}", catalog_dir.display()),
        queue_capacity: 128,
        flush_rows: 8,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 8 * 1024 * 1024,
        compression: CompressionKind::Snappy,
        overflow_policy: OverflowPolicy::DropOldest,
        layout_compatibility: LayoutCompatibility::RustCanonicalOnly,
        ..CaptureConfig::default()
    }
}

fn build_data_type() -> DataType {
    let mut metadata = Params::new();
    metadata.insert(
        "index_name".to_string(),
        serde_json::Value::String("btc_usd".to_string()),
    );
    DataType::new("DeribitVolatilityIndex", Some(metadata), None)
}

fn main() -> Result<()> {
    register_deribit_custom_data();

    let catalog_dir = env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/nautilus-deribit-dvol-fixture"));

    std::fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create {}", catalog_dir.display()))?;

    let data_type = build_data_type();
    let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
        build_capture_config(&catalog_dir),
        CapturePlan {
            custom_data: vec![CustomDataCaptureSpec {
                data_type: data_type.clone(),
            }],
            ..Default::default()
        },
    ))?;

    for point in [
        DeribitVolatilityIndex::new(
            "btc_usd".to_string(),
            63.25,
            UnixNanos::from(10_000_000_u64),
            UnixNanos::from(10_000_000_u64),
        ),
        DeribitVolatilityIndex::new(
            "btc_usd".to_string(),
            64.5,
            UnixNanos::from(10_500_000_u64),
            UnixNanos::from(10_500_000_u64),
        ),
    ] {
        let custom = CustomData::new(Arc::new(point), data_type.clone());
        DataActor::on_data(&mut actor, &custom)?;
    }

    let _ = actor.flush_all()?;

    println!("Deribit DVOL fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Custom type: DeribitVolatilityIndex");
    Ok(())
}
