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

//! Opt-in public mainnet smoke and readback of operator-produced rolling catalogs.
use std::{path::PathBuf, time::Duration};

use catalog_capture_core::{
    BookDeltasCaptureSpec, CaptureConfig, CapturePlan, CustomDataCaptureSpec,
    IndexPriceCaptureSpec, InstrumentCaptureSpec, TradeCaptureSpec,
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::enums::Environment;
use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
use nautilus_lighter_ext::{
    LighterBinaryDataClientConfig, LighterBinaryDataClientFactory, LighterOpenInterest,
    common::{instrument_id, open_interest_data_type},
    http::LighterBinaryHttpClient,
    register_lighter_ext_custom_data,
};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{Data, IndexPriceUpdate},
    enums::BookType,
    identifiers::TraderId,
    instruments::Instrument,
};
use nautilus_network::http::{HttpClient, Method};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde_json::{Value, json};

#[tokio::test(flavor = "current_thread")]
#[ignore = "public mainnet access; records for 20 seconds"]
async fn mainnet_fixed_market_catalog_readback() {
    register_lighter_ext_custom_data();
    let http = HttpClient::builder().timeout_secs(15).build().unwrap();
    // Near expiry, wait for the next observing window rather than treating an
    // ordinary venue boundary as an adapter regression. Selection ends before connect.
    let market = tokio::time::timeout(Duration::from_secs(80), async {
        loop {
            let response = http.request(Method::GET,
                "https://mainnet.zklighter.elliot.ai/api/v1/events?active=true&tag=eth-updown-15m&limit=50".into(),
                None, None, None, None, None).await.unwrap();
            let events: Value = serde_json::from_slice(&response.body).unwrap();
            assert_eq!(events["code"], 200);
            let now = get_atomic_clock_realtime().get_time_ms();
            if let Some(event) = events["events"].as_array().unwrap().iter().find(|e| {
                e["start_at"].as_u64().unwrap() + 10_000 <= now
                    && now + 30_000 < e["end_at"].as_u64().unwrap()
            }) {
                break event["metadata"]["market_id"].as_u64().unwrap();
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }).await.expect("a current observation window must become available");
    let id = instrument_id(market).unwrap();
    let config = LighterBinaryDataClientConfig {
        market_ids: vec![market],
        ..Default::default()
    };
    let expected = LighterBinaryHttpClient::new(&config)
        .unwrap()
        .load_markets(&[market], get_atomic_clock_realtime().get_time_ns())
        .await
        .unwrap()
        .remove(0);
    let retained = std::env::var_os("LIGHTER_TEST_CATALOG");
    let root = retained.as_ref().map_or_else(
        || std::env::temp_dir().join(format!("lighter-mainnet-{}", UUID4::new())),
        PathBuf::from,
    );
    std::fs::create_dir(&root).expect("a new catalog directory");
    println!("Recording public mainnet catalog at {}", root.display());
    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id: id }],
        trades: vec![TradeCaptureSpec { instrument_id: id }],
        book_deltas: vec![BookDeltasCaptureSpec {
            instrument_id: id,
            book_type: BookType::L2_MBP,
            depth: None,
        }],
        index_prices: vec![IndexPriceCaptureSpec { instrument_id: id }],
        custom_data: vec![CustomDataCaptureSpec {
            data_type: open_interest_data_type(market),
        }],
        ..Default::default()
    };
    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", root.display()),
        flush_rows: 32,
        flush_interval_ms: 100,
        ..Default::default()
    };
    let actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(capture, plan)).unwrap();
    let mut node = LiveNode::builder(TraderId::new("CAPTURE-001"), Environment::Live)
        .unwrap()
        .with_name("LIGHTER-BINARY-MAINNET-READBACK")
        .with_timeout_connection(30)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            Some("LIGHTER".into()),
            Box::new(LighterBinaryDataClientFactory::new()),
            Box::new(config),
        )
        .unwrap()
        .build()
        .unwrap();
    node.add_actor(actor).unwrap();
    let stop = node.handle();
    let capture_start_secs = get_atomic_clock_realtime().get_time_ms() / 1000;
    let deadline = async {
        tokio::time::sleep(Duration::from_secs(20)).await;
        stop.stop();
    };
    let (_, result) = tokio::join!(deadline, node.run());
    result.unwrap();
    let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    assert_eq!(instruments.len(), 1);
    assert_eq!(
        instruments[0].info().unwrap()["start_price"],
        expected.instrument.info().unwrap()["start_price"]
    );
    let trades = catalog.trade_ticks(None, None, None).unwrap();
    assert!(!trades.is_empty());
    let deltas = catalog.order_book_deltas(None, None, None).unwrap();
    assert!(!deltas.is_empty());
    let replay = catalog_capture_core::validate_order_book_replay(&root, id, true).unwrap();
    let oracle = catalog
        .query::<IndexPriceUpdate>(None, None, None, None, None, true)
        .unwrap();
    assert!(oracle.len() > 1);
    assert!(
        oracle.last().unwrap().ts_event.as_u64() / 1_000_000_000 >= capture_start_secs + 10,
        "must record live oracle updates after the historical bootstrap"
    );
    assert!(
        oracle
            .iter()
            .all(|p| p.instrument_id == id && p.value.precision == 16)
    );
    let rows = catalog
        .query_custom_data_dynamic("LighterOpenInterest", None, None, None, None, None, true)
        .unwrap();
    assert!(!rows.is_empty());
    for row in &rows {
        let Data::Custom(custom) = row else {
            panic!("custom OI")
        };
        assert_eq!(custom.data_type, open_interest_data_type(market));
        let oi = custom
            .data
            .as_any()
            .downcast_ref::<LighterOpenInterest>()
            .unwrap();
        assert_eq!(oi.instrument_id, id);
    }
    let evidence = json!({
        "catalog": root,
        "instrument_id": id.to_string(),
        "start_price": instruments[0].info().unwrap()["start_price"],
        "trades": trades.len(),
        "book_deltas": deltas.len(),
        "book_replay": replay,
        "open_interest_rows": rows.len(),
        "oracle_rows": oracle.len(),
        "oracle_precision": 16,
    });
    println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
    if retained.is_none() {
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Read an existing real rolling catalog; this test never connects or modifies trading state.
#[test]
#[ignore = "requires LIGHTER_TEST_CATALOG from an actual CLI recording"]
fn rolling_catalog_readback() {
    register_lighter_ext_custom_data();
    let root = PathBuf::from(std::env::var("LIGHTER_TEST_CATALOG").expect("catalog path"));
    let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    let trades = catalog.trade_ticks(None, None, None).unwrap();
    let deltas = catalog.order_book_deltas(None, None, None).unwrap();
    let prices = catalog
        .query::<IndexPriceUpdate>(None, None, None, None, None, true)
        .unwrap();
    let custom = catalog
        .query_custom_data_dynamic("LighterOpenInterest", None, None, None, None, None, true)
        .unwrap();
    let mut latest = std::collections::BTreeMap::new();
    for instrument in instruments {
        let id = instrument.id();
        if latest
            .get(&id)
            .is_none_or(|old: &nautilus_model::instruments::InstrumentAny| {
                old.ts_init() < instrument.ts_init()
            })
        {
            latest.insert(id, instrument);
        }
    }
    let generations: usize = std::env::var("LIGHTER_TEST_GENERATIONS")
        .map_or(3, |value| value.parse().expect("positive generation count"));
    assert!(generations >= 2);
    assert_eq!(
        latest.len(),
        2 * generations,
        "both BTC and ETH for every generation"
    );
    let active = std::env::var_os("LIGHTER_TEST_ACTIVE").is_some();
    let mut result = Vec::new();
    for (id, instrument) in latest {
        if active && instrument.expiration_ns().unwrap() > get_atomic_clock_realtime().get_time_ns()
        {
            continue;
        }
        let start_price = instrument.info().unwrap()["start_price"]
            .as_str()
            .expect("authoritative start price");
        assert_eq!(start_price.split('.').nth(1).unwrap().len(), 16);
        let market_trades: Vec<_> = trades.iter().filter(|t| t.instrument_id == id).collect();
        let market_deltas: Vec<_> = deltas.iter().filter(|d| d.instrument_id == id).collect();
        let oracle: Vec<_> = prices.iter().filter(|p| p.instrument_id == id).collect();
        let oi: Vec<_> = custom
            .iter()
            .filter_map(|row| {
                let Data::Custom(row) = row else {
                    return None;
                };
                let oi = row
                    .data
                    .as_any()
                    .downcast_ref::<LighterOpenInterest>()
                    .unwrap();
                assert_eq!(row.data_type, open_interest_data_type(oi.market_id));
                (oi.instrument_id == id).then_some(oi)
            })
            .collect();
        assert!(!market_trades.is_empty(), "{id} trades");
        assert_eq!(
            market_trades
                .iter()
                .map(|tick| tick.trade_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            market_trades.len(),
            "{id} trade replay deduplication"
        );
        assert!(!market_deltas.is_empty(), "{id} deltas");
        assert!(!oi.is_empty(), "{id} OI");
        assert!(oracle.len() > 1, "{id} oracle");
        assert!(oracle.iter().all(|p| p.value.precision == 16));
        let replay = catalog_capture_core::validate_order_book_replay(&root, id, true).unwrap();
        result.push(json!({
            "instrument_id": id.to_string(),
            "start_price": start_price,
            "trades": market_trades.len(),
            "book_deltas": market_deltas.len(),
            "open_interest_rows": oi.len(),
            "oracle_rows": oracle.len(),
            "oracle_precision": 16,
            "book_replay": replay
        }));
    }
    let audit: Vec<Value> = std::fs::read_to_string(root.join("lighter_universe_events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let purged: Vec<_> = audit
        .iter()
        .filter(|row| row["kind"] == "cache_purged")
        .collect();
    assert_eq!(purged.len(), 2 * (generations - 1));
    assert!(
        purged
            .iter()
            .all(|row| row["details"]["cache_contains_instrument"] == false)
    );
    fn all_files(root: &std::path::Path) -> Vec<PathBuf> {
        std::fs::read_dir(root)
            .unwrap()
            .flat_map(|entry| {
                let p = entry.unwrap().path();
                if p.is_dir() { all_files(&p) } else { vec![p] }
            })
            .collect()
    }
    if !active {
        assert!(
            all_files(&root)
                .iter()
                .all(|p| !p.to_string_lossy().ends_with(".part")),
            "clean shutdown seals every stream"
        );
    } else {
        assert_eq!(result.len(), 2 * (generations - 1));
        assert!(
            all_files(&root)
                .iter()
                .any(|p| p.to_string_lossy().ends_with(".part"))
        );
    }
    let evidence = json!({
        "generations_per_series": generations,
        "catalog": root,
        "markets": result,
        "lifecycle": audit,
    });
    println!("{}", serde_json::to_string_pretty(&evidence).unwrap());
}
