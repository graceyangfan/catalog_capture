// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Epoch filtering and public market-data publication. Private wallet events belong to execution.

use anyhow::Context;
use nautilus_common::messages::DataEvent;
use nautilus_model::data::Data;
use nautilus_network::{RECONNECTED, transport::Message};

use super::runtime::PredictDataSession;
use crate::{
    PredictOrderbookSnapshot, PredictOrderbookWire,
    websocket::parse::{
        PredictWebSocketMessage, heartbeat_message, parse_message, parse_topic_market_id,
    },
};

impl PredictDataSession {
    pub(super) async fn handle_message(
        &mut self,
        epoch: u64,
        message: Message,
    ) -> anyhow::Result<()> {
        let text = message
            .as_text()
            .context("Predict sent a non-text application frame")?;
        if text == RECONNECTED {
            self.current_epoch = epoch;
            self.failed_epoch = None;
            self.snapshots.clear();
            let market_ids = self.subscriptions.keys().copied().collect::<Vec<_>>();
            for market_id in market_ids {
                self.send_subscription("subscribe", market_id).await?;
            }
            let mut trade_markets = self
                .trades
                .values()
                .map(|c| c.market_id)
                .collect::<Vec<_>>();
            trade_markets.sort_unstable();
            trade_markets.dedup();
            for market_id in trade_markets {
                self.send_trade_subscription("subscribe", market_id).await?;
            }
            return Ok(());
        }
        if epoch != self.current_epoch || self.failed_epoch == Some(epoch) {
            return Ok(());
        }
        let (topic, data) = match parse_message(text)? {
            PredictWebSocketMessage::Heartbeat(data) => {
                self.client
                    .send_text(heartbeat_message(data), None)
                    .await
                    .context("failed to echo Predict heartbeat")?;
                return Ok(());
            }
            PredictWebSocketMessage::Informational => return Ok(()),
            PredictWebSocketMessage::Market { topic, data } => (topic, data),
        };
        if let Some(market) = topic.strip_prefix("predictTrades/") {
            let market_id = market
                .parse::<u64>()
                .context("invalid Predict trade topic")?;
            return self.handle_trades(
                market_id,
                data.context("Predict trade frame is missing data")?.get(),
            );
        }
        // Ignore unrelated multiplexed topics.
        if !topic.starts_with("predictOrderbook/") {
            log::debug!("Predict market frame ignored unrelated topic={topic:?}");
            return Ok(());
        }
        let market_id = parse_topic_market_id(&topic)?;
        if !self.subscriptions.contains_key(&market_id) {
            return Ok(());
        }
        let context = self
            .contexts
            .get(&market_id)
            .cloned()
            .or_else(|| {
                self.dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .context(market_id)
            })
            .or_else(|| {
                self.quotes
                    .values()
                    .find(|q| q.context.market_id == market_id)
                    .map(|q| q.context.clone())
            })
            .or_else(|| {
                self.depths
                    .values()
                    .find(|d| d.context.market_id == market_id)
                    .map(|d| d.context.clone())
            })
            .with_context(|| format!("Predict frame for unconfigured market {market_id}"))?;
        let wire: PredictOrderbookWire =
            serde_json::from_str(data.context("Predict market frame is missing data")?.get())
                .context("failed to decode Predict orderbook snapshot")?;
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire, self.clock.get_time_ns())?;
        // Late native depth owners receive the same confirmed source snapshot without
        // requiring a duplicate physical subscribe or waiting for a new book update.
        self.snapshots.insert(market_id, snapshot.clone());
        let mut native_owners = 0;
        for id in [context.yes_instrument_id, context.no_instrument_id] {
            if let Some(subscription) = self.depths.get_mut(&id) {
                native_owners += 1;
                if subscription
                    .last_timestamp
                    .is_some_and(|last| snapshot.ts_event < last)
                {
                    continue;
                }
                let depth =
                    super::depth::depth_from_snapshot(&snapshot, &context, id, subscription.depth)?;
                self.data_sender
                    .send(DataEvent::Data(Data::BookDepth(Box::new(depth))))?;
                subscription.last_timestamp = Some(snapshot.ts_event);
            }
        }
        for id in [context.yes_instrument_id, context.no_instrument_id] {
            let Some(subscription) = self.quotes.get_mut(&id) else {
                continue;
            };
            native_owners += 1;
            if let Some(quote) = super::quotes::quote_from_snapshot(&snapshot, &context, id)? {
                if subscription.last_quote.is_some_and(|last| {
                    quote.ts_event < last.ts_event
                        || (quote.ts_event == last.ts_event
                            && quote.bid_price == last.bid_price
                            && quote.ask_price == last.ask_price
                            && quote.bid_size == last.bid_size
                            && quote.ask_size == last.ask_size)
                }) {
                    continue;
                }
                self.data_sender.send(DataEvent::Data(Data::Quote(quote)))?;
                subscription.last_quote = Some(quote);
            }
        }
        // Native-only interest must not create an unsolicited custom-data stream.
        let subscription = &self.subscriptions[&market_id];
        if subscription.owners > native_owners {
            self.data_sender.send(DataEvent::Data(Data::Custom(
                snapshot.into_custom_data_with_type(subscription.data_type.clone())?,
            )))?;
        }
        self.confirm_dynamic_snapshot(market_id).await?;
        Ok(())
    }
}
