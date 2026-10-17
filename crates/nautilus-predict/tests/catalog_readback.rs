// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Opt-in readback of a completed six-selector live capture. No network or credentials required.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use nautilus_model::{data::Data, instruments::Instrument};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_predict::{PredictOrderbookSnapshot, register_predict_custom_data};

#[test]
#[ignore = "requires PREDICT_PROBE_CATALOG and PREDICT_START_PRICE_API_JSON from a live capture"]
fn live_start_price_matches_api_and_reads_native_snapshots() {
    let root = std::env::var("PREDICT_PROBE_CATALOG").expect("set PREDICT_PROBE_CATALOG");
    let evidence = std::env::var("PREDICT_START_PRICE_API_JSON")
        .expect("set PREDICT_START_PRICE_API_JSON to a market_id -> decimal string JSON file");
    let expected: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(evidence).unwrap()).unwrap();
    assert!(!expected.is_empty());
    register_predict_custom_data();
    let mut catalog = ParquetDataCatalog::new(Path::new(&root), None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    assert_eq!(instruments.len(), expected.len() * 2);
    for (market_id, price) in &expected {
        for outcome in [1, 2] {
            let id = format!("{market_id}-{outcome}.PREDICT");
            let instrument = instruments
                .iter()
                .find(|item| item.id().to_string() == id)
                .unwrap();
            let info = instrument.info().expect("live start_price must persist");
            assert_eq!(info.len(), 1);
            assert_eq!(info["start_price"].as_str(), Some(price.as_str()));
            println!("{id}: info.start_price={price}");
        }
    }
    let rows = catalog
        .query_custom_data_dynamic(
            "PredictOrderbookSnapshot",
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    let mut counts = BTreeMap::<String, usize>::new();
    for row in &rows {
        let Data::Custom(custom) = row else {
            panic!("expected custom data")
        };
        let book = custom
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        assert!(expected.contains_key(&book.market_id.to_string()));
        assert!(!book.bids.is_empty() || !book.asks.is_empty());
        *counts.entry(book.market_id.to_string()).or_default() += 1;
    }
    assert_eq!(counts.len(), expected.len());
    assert!(!rows.is_empty());
    println!("native catalog snapshots: {counts:?}");
    fn assert_sealed(path: &Path) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                assert_sealed(&path);
            } else {
                assert!(!path.to_string_lossy().ends_with(".part"));
            }
        }
    }
    assert_sealed(Path::new(&root));
}

#[test]
#[ignore = "requires PREDICT_PROBE_CATALOG from a BTC 5m recording across one boundary"]
fn btc_selector_rolls_and_reads_back() {
    let root = std::env::var("PREDICT_PROBE_CATALOG").expect("set PREDICT_PROBE_CATALOG");
    register_predict_custom_data();
    let mut catalog = ParquetDataCatalog::new(Path::new(&root), None, None, None, None);
    let instruments = catalog.instruments(None, None, None).unwrap();
    let rows = catalog
        .query_custom_data_dynamic(
            "PredictOrderbookSnapshot",
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    let mut markets: BTreeMap<u64, (usize, u64, u64)> = BTreeMap::new();
    let mut nonempty = 0;
    let mut settled = 0;
    let mut pending = 0;
    for row in &rows {
        let Data::Custom(custom) = row else {
            panic!("expected custom data")
        };
        let book = custom
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        assert_eq!(
            custom.data_type.identifier(),
            Some(book.market_id.to_string().as_str())
        );
        let metadata = custom.data_type.metadata().unwrap();
        assert_eq!(metadata["predict_price_feed_symbol"], "BTCUSDT");
        assert_eq!(metadata["predict_title_asset"], "bitcoin");
        assert_eq!(metadata["predict_interval_secs"], "300");
        assert_eq!(
            book.instrument_id.to_string(),
            format!("{}-1.PREDICT", book.market_id)
        );
        assert_eq!(
            book.ts_event.as_u64() % 1_000_000,
            0,
            "venue event precision stays milliseconds"
        );
        assert!(book.ts_init.as_u64() > 0);
        let entry = markets.entry(book.market_id).or_insert((0, u64::MAX, 0));
        entry.0 += 1;
        entry.1 = entry.1.min(book.ts_init.as_u64());
        entry.2 = entry.2.max(book.ts_init.as_u64());
        nonempty += usize::from(!book.bids.is_empty() || !book.asks.is_empty());
        settled += usize::from(book.last_order_settled.is_some());
        pending += usize::from(book.settlements_pending.is_some());
    }
    assert_eq!(markets.len(), 2, "one confirmed market rollover");
    assert_eq!(
        instruments.len(),
        4,
        "both outcomes for each recorded market"
    );
    assert!(nonempty > 0);
    let mut windows = markets.values().collect::<Vec<_>>();
    windows.sort_by_key(|(_, first, _)| *first);
    assert!(
        windows[0].2 <= windows[1].1,
        "retired stream ends before successor output begins"
    );
    for market_id in markets.keys() {
        for outcome in [1, 2] {
            assert!(
                instruments
                    .iter()
                    .any(|instrument| instrument.id().to_string()
                        == format!("{market_id}-{outcome}.PREDICT"))
            );
        }
        let folder =
            Path::new(&root).join(format!("data/custom/PredictOrderbookSnapshot/{market_id}"));
        let files = std::fs::read_dir(folder)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(files.len(), 1, "one sealed file per market");
        assert_eq!(
            files[0]
                .extension()
                .and_then(|extension| extension.to_str()),
            Some("parquet")
        );
    }
    fn no_parts(path: &Path) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                no_parts(&path);
            } else {
                assert!(
                    !path.to_string_lossy().ends_with(".part"),
                    "no open segment after shutdown"
                );
            }
        }
    }
    no_parts(Path::new(&root));
    println!(
        "BTC live readback: instruments={} snapshots={} nonempty={nonempty} settled={settled} pending={pending} markets={markets:?}",
        instruments.len(),
        rows.len()
    );
}

#[test]
#[ignore = "requires PREDICT_PROBE_CATALOG from a completed BTC/ETH/BNB 5m/15m recording"]
fn six_selectors_roll_and_read_back() {
    let root = std::env::var("PREDICT_PROBE_CATALOG").expect("set PREDICT_PROBE_CATALOG");
    register_predict_custom_data();
    let mut catalog = ParquetDataCatalog::new(Path::new(&root), None, None, None, None);
    // The profile also records Spot SBE definitions in the same catalog.
    let instruments = catalog
        .instruments(None, None, None)
        .unwrap()
        .into_iter()
        .filter(|instrument| instrument.id().venue.as_str() == "PREDICT")
        .collect::<Vec<_>>();
    let rows = catalog
        .query_custom_data_dynamic(
            "PredictOrderbookSnapshot",
            None,
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    let mut groups: BTreeMap<(String, String), BTreeMap<u64, usize>> = BTreeMap::new();
    let mut nonempty_markets = BTreeSet::new();
    let mut settled = 0;
    let mut pending = 0;
    let mut nonempty = 0;
    let mut observations = Vec::new();
    for row in &rows {
        let Data::Custom(custom) = row else {
            panic!("expected custom data");
        };
        let snapshot = custom
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        assert_eq!(
            custom.data_type.identifier(),
            Some(snapshot.market_id.to_string().as_str())
        );
        assert!(
            instruments
                .iter()
                .any(|instrument| instrument.id() == snapshot.instrument_id)
        );
        assert!(
            instruments
                .iter()
                .any(|instrument| instrument.id().to_string()
                    == format!("{}-2.PREDICT", snapshot.market_id))
        );
        let metadata = custom.data_type.metadata().unwrap();
        let symbol = metadata
            .get("predict_price_feed_symbol")
            .unwrap()
            .as_str()
            .unwrap();
        let interval = metadata
            .get("predict_interval_secs")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(snapshot.ts_event.as_u64() % 1_000_000, 0);
        assert!(snapshot.ts_init.as_u64() > 0);
        if !snapshot.bids.is_empty() || !snapshot.asks.is_empty() {
            nonempty_markets.insert((symbol.to_string(), interval.to_string(), snapshot.market_id));
        }
        observations.push((
            snapshot.ts_init.as_u64(),
            (symbol.to_string(), interval.to_string()),
        ));
        *groups
            .entry((symbol.into(), interval.into()))
            .or_default()
            .entry(snapshot.market_id)
            .or_default() += 1;
        settled += usize::from(snapshot.last_order_settled.is_some());
        pending += usize::from(snapshot.settlements_pending.is_some());
        nonempty += usize::from(!snapshot.bids.is_empty() || !snapshot.asks.is_empty());
    }
    for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] {
        for interval in ["300", "900"] {
            let markets = &groups[&(symbol.into(), interval.into())];
            assert_eq!(
                markets.len(),
                2,
                "expected one rollover for {symbol} {interval}"
            );
            for (market_id, count) in markets {
                assert!(*count > 0);
                assert!(
                    nonempty_markets.contains(&(
                        symbol.to_string(),
                        interval.to_string(),
                        *market_id
                    )),
                    "each selector must record nonempty depth on both sides of rollover"
                );
                let folder = Path::new(&root)
                    .join(format!("data/custom/PredictOrderbookSnapshot/{market_id}"));
                let files = std::fs::read_dir(folder)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>();
                assert_eq!(files.len(), 1, "one sealed snapshot file per market");
                assert_eq!(
                    files[0].extension().and_then(|value| value.to_str()),
                    Some("parquet")
                );
            }
        }
    }
    assert_eq!(groups.len(), 6);
    assert_eq!(instruments.len(), 24);
    assert!(nonempty > 0, "must read actual non-empty orderbook depth");
    let first_ts = observations.iter().map(|(ts, _)| *ts).min().unwrap();
    let mut first_35_seconds = BTreeMap::new();
    for (_, key) in observations
        .iter()
        .filter(|(ts, _)| *ts < first_ts + 35_000_000_000)
    {
        *first_35_seconds.entry(key.clone()).or_insert(0usize) += 1;
    }
    assert_eq!(
        first_35_seconds.len(),
        6,
        "all selectors must deliver during startup"
    );
    println!("first_35_seconds_from_first_snapshot={first_35_seconds:?}");
    println!(
        "instruments={} snapshots={} nonempty={} settled={} pending={} groups={groups:?}",
        instruments.len(),
        rows.len(),
        nonempty,
        settled,
        pending
    );
}
