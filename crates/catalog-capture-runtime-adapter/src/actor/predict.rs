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

//! Native subscriptions share the existing snapshot-confirmed rolling selector.

use nautilus_common::{
    actor::DataActor,
    msgbus::{self, switchboard},
};
use nautilus_model::enums::BookType;
use nautilus_predict::common::predict_outcome_instrument_id;

use super::*;
use crate::actor_runtime::optional_submit;

#[derive(Debug, Clone, Copy)]
pub(super) struct PredictNativeSubscription {
    instrument_id: InstrumentId,
    depth: bool,
    trades: bool,
}

impl CatalogCaptureActor {
    pub(super) fn predict_market_has_subscribers(market: u64) -> Result<bool> {
        for outcome in [1, 2] {
            let id = predict_outcome_instrument_id(market, outcome)?;
            if msgbus::subscriber_count_depth(switchboard::get_book_depth_topic(id)) > 0
                || msgbus::exact_subscriber_count_quotes(switchboard::get_quotes_topic(id)) > 0
                || msgbus::exact_subscriber_count_trades(switchboard::get_trades_topic(id)) > 0
                || msgbus::subscriptions_count_any(switchboard::get_instrument_topic(id).as_str())?
                    > 0
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn reap_retired_predict_markets(&mut self) -> Result<()> {
        // As in the Lighter recorder, wait for a later event turn so queued Native
        // unsubscribe commands have removed our handlers before checking other owners.
        for market in std::mem::take(&mut self.retiring_predict_market_ids) {
            if Self::predict_market_has_subscribers(market)? {
                self.retiring_predict_market_ids.insert(market);
            } else {
                self.purge_retired_predict_market_from_cache(market)?;
            }
        }
        Ok(())
    }

    fn predict_option(&self, selector: &CryptoUpDownSelectorKey, option: &str) -> bool {
        self.plan.custom_data.iter().any(|spec| {
            if spec.data_type.type_name() != "PredictCryptoUpDown" {
                return false;
            }
            let Some(m) = spec.data_type.metadata() else {
                return false;
            };
            let text = |key: &str| m.get(key).and_then(serde_json::Value::as_str);
            let key = text("price_feed_symbol")
                .zip(text("title_asset"))
                .zip(text("interval_secs").and_then(|s| s.parse::<u64>().ok()))
                .and_then(|((feed, title), interval)| {
                    CryptoUpDownSelectorKey::new(feed, title, interval).ok()
                });
            key.as_ref() == Some(selector) && text(option) == Some("true")
        })
    }

    pub(super) fn predict_native_accepts(&self, id: InstrumentId, option: &str) -> bool {
        self.predict_native_subscriptions.values().any(|active| {
            active.instrument_id == id
                && match option {
                    "record_depth" => active.depth,
                    "record_trades" => active.trades,
                    _ => false,
                }
        })
    }

    pub(super) fn sync_predict_native(
        &mut self,
        data_type: &DataType,
        retired: Option<u64>,
    ) -> Result<()> {
        let Some(key) = predict_output_selector_key(data_type)? else {
            return Ok(());
        };
        let market: u64 = data_type
            .identifier()
            .ok_or_else(|| anyhow::anyhow!("Predict snapshot lacks market identity"))?
            .parse()?;
        let id = predict_outcome_instrument_id(market, 1)?;
        if self
            .predict_native_subscriptions
            .get(&key)
            .is_some_and(|active| active.instrument_id == id)
        {
            return Ok(());
        }
        let depth = self.predict_option(&key, "record_depth");
        let trades = self.predict_option(&key, "record_trades");
        if !depth && !trades {
            return Ok(());
        }
        anyhow::ensure!(
            self.cache().instrument(&id).is_some(),
            "Predict native capture requires instrument definition before snapshot"
        );
        let client = Some(ClientId::from("PREDICT"));
        if depth {
            self.subscribe_book_depth(id, BookType::L2_MBP, None, client, false, None);
        }
        if trades {
            self.subscribe_trades(id, client, None);
        }
        self.predict_native_subscriptions.insert(
            key,
            PredictNativeSubscription {
                instrument_id: id,
                depth,
                trades,
            },
        );
        if let Some(old) = retired {
            let old_id = predict_outcome_instrument_id(old, 1)?;
            if !self
                .predict_native_subscriptions
                .values()
                .any(|active| active.instrument_id == old_id)
            {
                if depth
                    && !self
                        .plan
                        .book_depth
                        .iter()
                        .any(|s| s.instrument_id == old_id)
                {
                    self.unsubscribe_book_depth(old_id, client, None);
                    if let Some(runtime) = &self.book_depth_runtime {
                        runtime.seal_partition(
                            &PartitionKey::catalog_data::<OrderBookDepth>(old_id).stable_key(),
                        )?;
                    }
                }
                if trades && !self.plan.trades.iter().any(|s| s.instrument_id == old_id) {
                    self.unsubscribe_trades(old_id, client, None);
                    if let Some(runtime) = &self.trade_runtime {
                        runtime.seal_partition(
                            &PartitionKey::catalog_data::<TradeTick>(old_id).stable_key(),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn stop_predict_native(&mut self) {
        let subscriptions = std::mem::take(&mut self.predict_native_subscriptions);
        for (_, active) in subscriptions {
            let id = active.instrument_id;
            let client = Some(ClientId::from("PREDICT"));
            if active.depth && !self.plan.book_depth.iter().any(|s| s.instrument_id == id) {
                self.unsubscribe_book_depth(id, client, None);
            }
            if active.trades && !self.plan.trades.iter().any(|s| s.instrument_id == id) {
                self.unsubscribe_trades(id, client, None);
            }
        }
    }

    pub(super) fn submit_book_depth(&mut self, depth: OrderBookDepth) -> Result<()> {
        if !self.predict_native_accepts(depth.instrument_id, "record_depth")
            && !self
                .plan
                .book_depth
                .iter()
                .any(|s| s.instrument_id == depth.instrument_id)
        {
            return Ok(());
        }
        // Full depth can exceed the model's inline levels. Account for level storage
        // conservatively so queue byte limits still bound deep-book recording.
        let estimated_bytes = std::mem::size_of::<OrderBookDepth>()
            + (depth.bids.len() + depth.asks.len())
                * std::mem::size_of::<nautilus_model::data::BookOrder>()
            + (depth.bid_counts.len() + depth.ask_counts.len()) * std::mem::size_of::<u32>();
        optional_submit(
            &self.book_depth_runtime,
            catalog_capture_core::item::CaptureItem {
                partition_key: PartitionKey::catalog_data::<OrderBookDepth>(depth.instrument_id),
                event_ts_ns: depth.ts_event.as_u64(),
                init_ts_ns: Some(depth.ts_init.as_u64()),
                row_count: 1,
                estimated_bytes,
                payload: depth,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use catalog_capture_core::plan::BookDepthCaptureSpec;
    use nautilus_model::{
        data::BookOrder,
        enums::{OrderSide, RecordFlag},
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

    use super::*;

    #[test]
    fn native_depth_writer_persists_full_snapshots_and_reads_them_back() {
        let dir =
            std::env::temp_dir().join(format!("predict-depth-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let id = predict_outcome_instrument_id(42, 1).unwrap();
        let plan = CapturePlan {
            book_depth: vec![BookDepthCaptureSpec {
                instrument_id: id,
                book_type: BookType::L2_MBP,
                depth: None,
            }],
            ..CapturePlan::default()
        };
        let mut actor = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", dir.display()),
                ..CaptureConfig::default()
            },
            plan,
        ))
        .unwrap();
        assert!(actor.book_depth_runtime.is_some());
        assert_eq!(actor.enabled_background_worker_count(), 2);
        let first = OrderBookDepth::new(
            id,
            [BookOrder::new(
                OrderSide::Buy,
                Price::from("0.62"),
                Quantity::from("2.0000"),
                0,
            )],
            [BookOrder::new(
                OrderSide::Sell,
                Price::from("0.63"),
                Quantity::from("3.0000"),
                0,
            )],
            [0],
            [0],
            RecordFlag::F_SNAPSHOT as u8,
            0,
            100.into(),
            101.into(),
        );
        let second = OrderBookDepth::new(
            id,
            [],
            [],
            [],
            [],
            RecordFlag::F_SNAPSHOT as u8,
            0,
            102.into(),
            103.into(),
        );
        actor.submit_book_depth(first.clone()).unwrap();
        actor.submit_book_depth(second.clone()).unwrap();
        actor.shutdown_all().unwrap();
        let mut catalog = ParquetDataCatalog::new(&dir, None, None, None, None);
        let rows = catalog
            .order_book_depths(Some(vec![id.to_string()]), None, None)
            .unwrap();
        assert_eq!(rows, vec![first, second]);
        drop(catalog);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
