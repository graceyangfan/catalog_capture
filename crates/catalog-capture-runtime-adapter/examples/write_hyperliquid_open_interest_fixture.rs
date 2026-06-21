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

use std::{env, fs, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{CustomDataCaptureSpec, InstrumentCaptureSpec},
    CapturePlan,
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::{Params, UnixNanos};
use nautilus_hyperliquid::data_types::{register_hyperliquid_custom_data, HyperliquidOpenInterest};
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::{ActorId, InstrumentId},
    instruments::{crypto_perpetual::CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;

fn resolve_catalog_dir() -> Result<PathBuf> {
    if let Some(arg1) = env::args().nth(1) {
        return Ok(PathBuf::from(arg1));
    }

    if let Ok(dir) = env::var("CATALOG_DIR") {
        return Ok(PathBuf::from(dir));
    }

    anyhow::bail!("Provide catalog directory as first arg or CATALOG_DIR env var");
}

fn build_instrument(instrument_id: InstrumentId, ts: UnixNanos) -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
        instrument_id,
        nautilus_model::identifiers::Symbol::new("ETH"),
        Currency::ETH(),
        Currency::USD(),
        Currency::USD(),
        false,
        2,
        3,
        Price::from("0.01"),
        Quantity::from("0.001"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        ts,
        ts,
    ))
}

fn main() -> Result<()> {
    register_hyperliquid_custom_data();

    let catalog_dir = resolve_catalog_dir()?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let instrument_id = InstrumentId::from_str("ETH-USD-PERP.HYPERLIQUID")?;
    let ts = UnixNanos::from(9_000_000);
    let instrument = build_instrument(instrument_id, ts);

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 2,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        JsonValue::String(instrument_id.to_string()),
    );

    let custom_type = DataType::new(
        "HyperliquidOpenInterest",
        Some(metadata),
        Some(instrument_id.to_string()),
    );

    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        custom_data: vec![CustomDataCaptureSpec {
            data_type: custom_type.clone(),
        }],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-HYPERLIQUID_OI_FIXTURE")),
        capture,
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;
    DataActor::on_instrument(&mut actor, &instrument)?;

    let original = [
        HyperliquidOpenInterest::new(
            instrument_id,
            Decimal::from_str("12345.6789")?,
            UnixNanos::from(9_000_000),
            UnixNanos::from(9_000_000),
        ),
        HyperliquidOpenInterest::new(
            instrument_id,
            Decimal::from_str("12388.5000")?,
            UnixNanos::from(9_001_000),
            UnixNanos::from(9_001_000),
        ),
    ];

    for item in original.iter().cloned() {
        let custom = CustomData::new(Arc::new(item), custom_type.clone());
        DataActor::on_data(&mut actor, &custom)?;
    }

    let _ = actor.flush_all()?;

    println!("Hyperliquid open-interest fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Instrument id: {}", instrument_id);
    println!("Custom type: HyperliquidOpenInterest");

    Ok(())
}
