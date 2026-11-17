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

//! Current OI: resolved TOML -> Native REST request -> actor -> sealed Native catalog.

use super::{build_capture_actor_config, validate_runtime};
use crate::{
    config::{CliConfigFile, resolve_config},
    custom_data::register_request_types,
};
use axum::{Router, http::Uri, routing::get};
use nautilus_binance::{
    common::enums::BinanceProductType, config::BinanceDataClientConfig,
    data_types::BinanceFuturesOpenInterest, futures::BinanceFuturesDataClient,
};
use nautilus_common::{
    actor::{Component, DataActor},
    cache::Cache,
    clients::DataClient,
    clock::VirtualClock,
    live::runner::replace_data_event_sender,
    messages::{
        DataEvent,
        data::{
            DataCommand, DataResponse, DataResponseEnvelope, RequestCommand, RequestCustomData,
        },
    },
    msgbus::{self, MessagingSwitchboard, TypedIntoHandler},
    timer::TimeEvent,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::Data,
    identifiers::TraderId,
    instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

fn config() -> CliConfigFile {
    toml::from_str(include_str!(
        "../../../../examples/capture.binance-futures-open-interest.toml"
    ))
    .unwrap()
}

#[test]
fn current_oi_config_resolves_native_identity_and_product_client() {
    let resolved = resolve_config(config()).unwrap();
    validate_runtime(&resolved).unwrap();
    assert!(resolved.plan.quotes.is_empty() && resolved.plan.book_deltas.is_empty());
    for request in &resolved.plan.custom_data_requests {
        let id = request.data_type.identifier().unwrap();
        assert_eq!(request.client_id.as_deref(), Some("binance_usdm"));
        assert_eq!(
            request
                .data_type
                .metadata()
                .unwrap()
                .get("instrument_id")
                .unwrap(),
            id
        );
        assert!(
            resolved
                .plan
                .instruments
                .iter()
                .any(|item| item.instrument_id.to_string() == id)
        );
    }
}

#[test]
fn current_oi_rejects_wrong_identity_product_and_client() {
    for (instrument, identifier, client) in [
        ("BTCUSDT.BINANCE", None, None),
        ("BTCUSDT-PERP.OTHER", None, None),
        ("BTCUSDT-PERP.BINANCE", Some("ETHUSDT-PERP.BINANCE"), None),
        ("BTCUSDT-PERP.BINANCE", None, Some("missing_client")),
        ("BTCUSD_PERP.BINANCE", None, None),
    ] {
        let mut config = config();
        let request = &mut config.capture.custom_data_requests[0];
        request
            .metadata
            .insert("instrument_id".into(), instrument.into());
        request.identifier = identifier.map(String::from);
        request.client_id = client.map(String::from);
        assert!(
            resolve_config(config).is_err(),
            "accepted {instrument} {identifier:?} {client:?}"
        );
    }
    let mut missing = config();
    missing.capture.custom_data_requests[0].metadata.clear();
    assert!(resolve_config(missing).is_err());
    let mut ambiguous = config();
    let mut second = ambiguous.venues[0].clone();
    second.id = "second_usdm".into();
    ambiguous.venues.push(second);
    assert!(resolve_config(ambiguous).is_err());
}

#[tokio::test]
async fn current_oi_native_http_response_seals_and_reads_back_without_precision_loss() {
    // Synthetic protocol fixture with distinct symbols and exact decimal tokens.
    let app = Router::new().route("/fapi/v1/openInterest", get(|uri: Uri| async move {
        let symbol = uri.query().unwrap().strip_prefix("symbol=").unwrap();
        let quantity = match symbol {
            "BTCUSDT" => "10659.509123456789",
            "ETHUSDT" => "98765.432109876543",
            _ => panic!("wrong Native request symbol: {symbol}"),
        };
        ([(axum::http::header::CONTENT_TYPE, "application/json")],
            serde_json::json!({"symbol":symbol,"openInterest":quantity,"time":1589437530011_u64}).to_string())
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let root = std::env::temp_dir().join(format!("capture-current-oi-{}", UUID4::new()));
    std::fs::create_dir_all(&root).unwrap();
    let mut file = config();
    file.output.catalog_uri = format!("file://{}", root.display());
    let config = resolve_config(file).unwrap();
    register_request_types(&config.plan.custom_data_requests);
    let commands = Arc::new(Mutex::new(Vec::<RequestCustomData>::new()));
    let received = commands.clone();
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        TypedIntoHandler::from(move |command: DataCommand| {
            if let DataCommand::Request(RequestCommand::Data(request)) = command {
                received.lock().unwrap().push(request);
            }
        }),
    );
    let (sender, mut events) = tokio::sync::mpsc::unbounded_channel();
    replace_data_event_sender(sender);
    let native = BinanceFuturesDataClient::new(
        "binance_usdm".into(),
        BinanceDataClientConfig {
            product_type: BinanceProductType::UsdM,
            base_url_http: Some(format!("http://{address}")),
            ..Default::default()
        },
        BinanceProductType::UsdM,
    )
    .unwrap();
    let cache = Rc::new(RefCell::new(Cache::new(None, None)));
    let mut btc_definition = crypto_perpetual_ethusdt();
    btc_definition.id = "BTCUSDT-PERP.BINANCE".into();
    btc_definition.raw_symbol = "BTCUSDT".into();
    btc_definition.base_currency = nautilus_model::types::Currency::BTC();
    btc_definition.max_price = None;
    for instrument in [
        InstrumentAny::from(btc_definition),
        InstrumentAny::from(crypto_perpetual_ethusdt()),
    ] {
        cache.borrow_mut().add_instrument(instrument).unwrap();
    }
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let mut actor = catalog_capture_runtime_adapter::CatalogCaptureActor::new(
        build_capture_actor_config(&config, &config.plan, &[], &[], &[], None, None).unwrap(),
    )
    .unwrap();
    actor
        .register(TraderId::from("OI-001"), clock.clone(), cache)
        .unwrap();
    DataActor::on_start(&mut actor).unwrap();
    let mut requests = std::mem::take(&mut *commands.lock().unwrap());
    assert_eq!(requests.len(), 2);
    let old_eth = requests.pop().unwrap();
    let btc = requests.pop().unwrap();
    assert_eq!(old_eth.data_type.identifier(), Some("ETHUSDT-PERP.BINANCE"));
    native.request_data(btc).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap();
    let DataEvent::Response(response) = event else {
        panic!("expected Native REST response")
    };
    let mut expected = BTreeMap::new();
    remember_native_oi(&response, &mut expected);
    // Also exercise the batch envelope used by existing request adapters. The
    // payload remains the actual Native HTTP result; ETH below uses single items.
    let DataResponse::Data(mut response) = response else {
        panic!("expected custom response")
    };
    let payload = response
        .data
        .downcast_ref::<nautilus_model::data::CustomData>()
        .unwrap()
        .clone();
    response.data = Arc::new(vec![payload]);
    let response = DataResponseEnvelope::new(DataResponse::Data(response), true, None);
    actor.handle_response(&response);
    assert_eq!(
        actor.metrics_snapshot_data().custom_data_requests[0].rows,
        1
    );
    assert!(!actor.metrics_snapshot_data().custom_data_requests[0].in_flight);

    // Timeout then re-fire ETH. Keep the late observation, but do not let it
    // finish the current poll.
    clock.borrow_mut().set_time(30_000_000_000.into());
    actor
        .on_time_event(&TimeEvent::new(
            "CUSTOM_DATA_REQUEST:1".into(),
            UUID4::new(),
            30_000_000_000.into(),
            30_000_000_000.into(),
        ))
        .unwrap();
    let current_eth = commands.lock().unwrap().pop().unwrap();
    assert_ne!(current_eth.request_id, old_eth.request_id);
    native.request_data(old_eth).unwrap();
    let DataEvent::Response(response) =
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("expected response")
    };
    remember_native_oi(&response, &mut expected);
    actor.handle_response(&DataResponseEnvelope::new(response, true, None));
    let metrics = actor.metrics_snapshot_data();
    assert!(metrics.custom_data_requests[1].in_flight);
    assert_eq!(metrics.custom_data_requests[1].rows, 1);
    native.request_data(current_eth).unwrap();
    let DataEvent::Response(response) =
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!("expected response")
    };
    remember_native_oi(&response, &mut expected);
    actor.handle_response(&DataResponseEnvelope::new(response, true, None));
    assert_eq!(
        actor.metrics_snapshot_data().custom_data_requests[1].rows,
        2
    );
    assert!(!actor.metrics_snapshot_data().custom_data_requests[1].in_flight);
    actor.shutdown_all().unwrap();
    assert!(!actor.metrics_snapshot_data().has_recording_failure());
    drop(actor);
    drop(native);
    server.abort();

    let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
    for (id, quantity) in [
        ("BTCUSDT-PERP.BINANCE", "10659.509123456789"),
        ("ETHUSDT-PERP.BINANCE", "98765.432109876543"),
    ] {
        let directory = root.join("data/custom/BinanceFuturesOpenInterest").join(id);
        assert!(directory.is_dir());
        assert!(
            std::fs::read_dir(&directory).unwrap().all(|entry| entry
                .unwrap()
                .path()
                .extension()
                .unwrap()
                == "parquet")
        );
        let rows = catalog
            .query_custom_data_dynamic(
                "BinanceFuturesOpenInterest",
                Some(&[id.to_string()]),
                None,
                None,
                None,
                None,
                false,
            )
            .unwrap();
        assert_eq!(rows.len(), expected[id].len());
        for (row, original) in rows.iter().zip(&expected[id]) {
            let Data::Custom(data) = row else {
                panic!("expected Native custom data")
            };
            assert_eq!(data.data_type.identifier(), Some(id));
            let oi = data
                .data
                .as_any()
                .downcast_ref::<BinanceFuturesOpenInterest>()
                .unwrap();
            assert_eq!(oi.instrument_id.to_string(), id);
            assert_eq!(oi.open_interest, quantity.parse::<Decimal>().unwrap());
            assert_eq!(oi.ts_event.as_u64(), 1589437530011 * 1_000_000);
            assert_eq!(oi.ts_init, original.ts_init);
            assert_eq!(oi.ts_event, original.ts_event);
        }
    }
    assert_eq!(catalog.query_instruments(None).unwrap().len(), 2);
    drop(catalog);
    std::fs::remove_dir_all(root).unwrap();
}

fn remember_native_oi(
    response: &DataResponse,
    expected: &mut BTreeMap<String, Vec<BinanceFuturesOpenInterest>>,
) {
    let DataResponse::Data(response) = response else {
        panic!("expected custom response")
    };
    let custom = response
        .data
        .downcast_ref::<nautilus_model::data::CustomData>()
        .unwrap();
    let oi = custom
        .data
        .as_any()
        .downcast_ref::<BinanceFuturesOpenInterest>()
        .unwrap();
    expected
        .entry(oi.instrument_id.to_string())
        .or_default()
        .push(oi.clone());
}
