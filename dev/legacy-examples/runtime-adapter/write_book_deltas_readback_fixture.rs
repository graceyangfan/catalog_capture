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

use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{BookDeltasCaptureSpec, CapturePlan, InstrumentCaptureSpec},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BookOrder, OrderBookDelta, OrderBookDeltas},
    enums::{BookAction, BookType, OrderSide},
    identifiers::{ActorId, InstrumentId},
    instruments::{stubs::crypto_option_btc_deribit, InstrumentAny},
    types::{Price, Quantity},
};

fn main() -> Result<()> {
    let catalog_dir = env::args()
        .nth(1)
        .map(PathBuf::from)
        .context("usage: write_book_deltas_readback_fixture <catalog_dir>")?;
    fs::create_dir_all(&catalog_dir)?;

    let option_id = InstrumentId::from("BTC-13JAN23-16000-P.DERIBIT");
    let perp_id = InstrumentId::from("BTC-PERPETUAL.DERIBIT");

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 8,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let plan = CapturePlan {
        instruments: vec![
            InstrumentCaptureSpec {
                instrument_id: perp_id,
            },
            InstrumentCaptureSpec {
                instrument_id: option_id,
            },
        ],
        book_deltas: vec![BookDeltasCaptureSpec {
            instrument_id: option_id,
            book_type: BookType::L2_MBP,
        }],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-BOOK_DELTAS_FIXTURE")),
        capture: capture.clone(),
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
        dynamic_hip4_universe: None,
        metrics_snapshot: None,
        metrics_refresh_interval_secs: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;
    let option = InstrumentAny::CryptoOption(crypto_option_btc_deribit(
        3,
        1,
        Price::from("0.001"),
        Quantity::from("0.1"),
    ));
    DataActor::on_instrument(&mut actor, &option)?;

    let deltas = OrderBookDeltas::new(
        option_id,
        vec![
            OrderBookDelta::new(
                option_id,
                BookAction::Add,
                BookOrder::new(
                    OrderSide::Buy,
                    Price::from("0.1200"),
                    Quantity::from("1.0"),
                    1,
                ),
                0,
                1,
                UnixNanos::from(1_000),
                UnixNanos::from(1_000),
            ),
            OrderBookDelta::new(
                option_id,
                BookAction::Add,
                BookOrder::new(
                    OrderSide::Sell,
                    Price::from("0.1210"),
                    Quantity::from("1.5"),
                    2,
                ),
                0,
                2,
                UnixNanos::from(2_000),
                UnixNanos::from(2_000),
            ),
        ],
    );
    DataActor::on_book_deltas(&mut actor, &deltas)?;

    let _ = actor.flush_all()?;

    println!("Book deltas readback fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Option id: {}", option_id);
    println!("Order book deltas: {}", deltas.deltas.len());
    Ok(())
}
