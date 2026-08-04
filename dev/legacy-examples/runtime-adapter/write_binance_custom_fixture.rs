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
    plan::{CapturePlan, CustomDataCaptureSpec, InstrumentCaptureSpec},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_binance::data_types::{
    register_binance_custom_data, BinanceFuturesLiquidation, BinanceFuturesTicker,
};
use nautilus_common::actor::DataActor;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{CustomData, DataType},
    enums::OrderSide,
    identifiers::{ActorId, InstrumentId},
    instruments::{stubs::crypto_perpetual_ethusdt, InstrumentAny},
    types::{Price, Quantity},
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

fn custom_type(type_name: &str, instrument_id: InstrumentId) -> DataType {
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        JsonValue::String(instrument_id.to_string()),
    );
    DataType::new(type_name, Some(metadata), Some(instrument_id.to_string()))
}

fn main() -> Result<()> {
    register_binance_custom_data();

    let catalog_dir = resolve_catalog_dir()?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let instrument_id = InstrumentId::from_str("ETHUSDT-PERP.BINANCE")?;

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 2,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let ticker_type = custom_type("BinanceFuturesTicker", instrument_id);
    let liquidation_type = custom_type("BinanceFuturesLiquidation", instrument_id);
    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        custom_data: vec![
            CustomDataCaptureSpec {
                data_type: ticker_type.clone(),
            },
            CustomDataCaptureSpec {
                data_type: liquidation_type.clone(),
            },
        ],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-BINANCE_CUSTOM_FIXTURE")),
        capture,
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
        dynamic_hip4_universe: None,
        metrics_snapshot: None,
        metrics_refresh_interval_secs: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;
    DataActor::on_instrument(&mut actor, &instrument)?;

    let ticker = BinanceFuturesTicker::new(
        instrument_id,
        Decimal::from_str_exact("12.34")?,
        Decimal::from_str_exact("5.67")?,
        Decimal::from_str_exact("2634.123456")?,
        Decimal::from_str_exact("2640.000001")?,
        Decimal::from_str_exact("0.010000")?,
        Decimal::from_str_exact("2600.000000")?,
        Decimal::from_str_exact("2660.000000")?,
        Decimal::from_str_exact("2550.000000")?,
        Decimal::from_str_exact("1234.567890")?,
        Decimal::from_str_exact("3254321.123456")?,
        UnixNanos::from(10_u64),
        UnixNanos::from(11_u64),
        100,
        200,
        300,
        UnixNanos::from(12_u64),
        UnixNanos::from(13_u64),
    );
    let liquidation = BinanceFuturesLiquidation::new(
        instrument_id,
        OrderSide::Sell,
        Price::from("2641.10"),
        Price::from("2640.50"),
        Quantity::from("0.250"),
        Quantity::from("1.500"),
        UnixNanos::from(20_u64),
        UnixNanos::from(21_u64),
    );

    DataActor::on_data(
        &mut actor,
        &CustomData::new(Arc::new(ticker), ticker_type.clone()),
    )?;
    DataActor::on_data(
        &mut actor,
        &CustomData::new(Arc::new(liquidation), liquidation_type.clone()),
    )?;

    let _ = actor.flush_all()?;

    println!("Binance custom-data fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Instrument id: {}", instrument_id);
    println!("Custom types: BinanceFuturesTicker, BinanceFuturesLiquidation");

    Ok(())
}
