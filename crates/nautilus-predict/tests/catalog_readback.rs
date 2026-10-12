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

use std::{collections::BTreeMap, path::Path};

use nautilus_model::{data::Data, instruments::Instrument};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_predict::{PredictOrderbookSnapshot, register_predict_custom_data};

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
        "instruments={} snapshots={} settled={} pending={} groups={groups:?}",
        instruments.len(),
        rows.len(),
        settled,
        pending
    );
}
