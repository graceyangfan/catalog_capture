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

//! Explicit public mainnet probe: real Rust callbacks, no execution client or signer.
use super::*;
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::{LiveNode, LiveNodeHandle, config::LiveDataEngineConfig};
use nautilus_model::{
    data::{CustomData, OrderBookDepth},
    enums::BookType,
    identifiers::{InstrumentId, TraderId},
};
use nautilus_predict::{
    PredictCryptoUpDownSelector, PredictHttpClient, discover_crypto_up_down_market,
};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use std::{cell::RefCell, rc::Rc};

#[derive(Debug)]
struct Market {
    id: u64,
    instruments: [InstrumentId; 2],
    phase: u8,
    counts: [usize; 3],
}
#[derive(Debug)]
struct Feed {
    core: StrategyCore,
    markets: Vec<Market>,
    journal: Rc<RefCell<Vec<Value>>>,
    handle: LiveNodeHandle,
}
impl Feed {
    fn custom(&mut self, id: u64, subscribe: bool) {
        let data_type = predict_orderbook_data_type(id);
        if subscribe {
            self.subscribe_data(data_type, Some(ClientId::from("PREDICT")), None);
        } else {
            self.unsubscribe_data(data_type, Some(ClientId::from("PREDICT")), None);
        }
    }
    fn depth(&mut self, id: InstrumentId, subscribe: bool) {
        if subscribe {
            self.subscribe_book_depth(
                id,
                BookType::L2_MBP,
                None,
                Some(ClientId::from("PREDICT")),
                true,
                None,
            );
        } else {
            self.unsubscribe_book_depth(id, Some(ClientId::from("PREDICT")), None);
        }
    }
    fn advance(&mut self, index: usize) {
        let market = &mut self.markets[index];
        let ready = match market.phase {
            0 => market.counts.iter().all(|n| *n >= 20),
            1 => market.counts[1..].iter().all(|n| *n >= 20),
            2 => market.counts[1] >= 20,
            3 => market.counts[0] >= 20,
            _ => false,
        };
        if !ready {
            return;
        }
        let phase = market.phase;
        let id = market.id;
        let [yes, no] = market.instruments;
        self.journal
            .borrow_mut()
            .push(json!({"transition":phase,"market_id":id,"counts":market.counts}));
        market.phase += 1;
        market.counts = [0; 3];
        match phase {
            0 => self.custom(id, false),
            1 => self.depth(no, false),
            2 => {
                self.depth(yes, false);
                self.custom(id, true);
            }
            3 => self.custom(id, false),
            _ => unreachable!(),
        }
        if self.markets.iter().all(|market| market.phase == 4) {
            self.handle.stop();
        }
    }
}
impl DataActor for Feed {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for index in 0..self.markets.len() {
            let market_id = self.markets[index].id;
            let instruments = self.markets[index].instruments;
            self.custom(market_id, true);
            for id in instruments {
                self.depth(id, true);
            }
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        for index in 0..self.markets.len() {
            let market = &self.markets[index];
            let (id, instruments, phase) = (market.id, market.instruments, market.phase);
            if matches!(phase, 0 | 3) {
                self.custom(id, false);
            }
            if phase <= 2 {
                self.depth(instruments[0], false);
            }
            if phase <= 1 {
                self.depth(instruments[1], false);
            }
        }
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let snapshot = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        let index = self
            .markets
            .iter()
            .position(|market| market.id == snapshot.market_id)
            .unwrap();
        let market = &mut self.markets[index];
        anyhow::ensure!(
            matches!(market.phase, 0 | 3),
            "unsubscribed CustomData callback"
        );
        assert_eq!(data.data_type, predict_orderbook_data_type(market.id));
        market.counts[0] += 1;
        self.journal
            .borrow_mut()
            .push(json!({"kind":"custom","phase":market.phase,"snapshot":snapshot}));
        self.advance(index);
        Ok(())
    }
    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        let index = self
            .markets
            .iter()
            .position(|market| market.instruments.contains(&depth.instrument_id))
            .unwrap();
        let market = &mut self.markets[index];
        let side = usize::from(depth.instrument_id == market.instruments[1]);
        anyhow::ensure!(
            market.phase <= 2 && (market.phase != 2 || side == 0),
            "unsubscribed depth callback"
        );
        market.counts[side + 1] += 1;
        let phase = market.phase;
        let cache = self.cache();
        let book = cache.order_book(&depth.instrument_id).unwrap();
        assert_eq!(
            book.best_bid_price(),
            depth.bids.first().map(|order| order.price)
        );
        assert_eq!(
            book.best_ask_price(),
            depth.asks.first().map(|order| order.price)
        );
        if !depth.bids.is_empty() && !depth.asks.is_empty() {
            let quote = cache.quote(&depth.instrument_id).unwrap();
            assert_eq!(
                (quote.bid_price, quote.ask_price),
                (depth.bids[0].price, depth.asks[0].price)
            );
        }
        drop(cache);
        self.journal
            .borrow_mut()
            .push(json!({"kind":"depth","phase":phase,"depth":depth}));
        self.advance(index);
        Ok(())
    }
}
nautilus_strategy!(Feed, {});

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit public mainnet data only; PREDICT_PUBLIC_EVIDENCE_PATH is create_new JSON"]
async fn mainnet_custom_depth_lifecycle_read_only() -> anyhow::Result<()> {
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_PUBLIC_EVIDENCE_PATH")?)?;
    let path = std::env::var("PREDICT_API_KEY_FILE")
        .unwrap_or_else(|_| format!("{}/../../env/predictfun.toml", env!("CARGO_MANIFEST_DIR")));
    #[derive(serde::Deserialize)]
    struct Credentials {
        api_key: SecretString,
    }
    let credentials: Credentials = toml::from_str(&std::fs::read_to_string(path)?)
        .map_err(|_| anyhow::anyhow!("invalid public API credential file"))?;
    let http = PredictHttpClient::new(credentials.api_key.expose_secret(), None, 15)?;
    let mut selected = vec![];
    for interval_secs in [300, 900] {
        let selector = PredictCryptoUpDownSelector {
            price_feed_symbol: "BTCUSDT".into(),
            title_asset: "Bitcoin".into(),
            interval_secs,
        };
        let market = discover_crypto_up_down_market(
            &http,
            &selector,
            get_atomic_clock_realtime().get_time_ns().as_u64(),
        )
        .await?;
        selected.push(json!({"market_id":market.market_id,"interval_secs":interval_secs,"window_start_ns":market.window.start_ns,"window_end_ns":market.window.end_ns}));
    }
    let mut captures = vec![];
    for (backend_index, backend) in [TransportBackend::default(), TransportBackend::Tungstenite]
        .into_iter()
        .enumerate()
    {
        let market_ids = selected
            .iter()
            .map(|market| market["market_id"].as_u64().unwrap())
            .collect::<Vec<_>>();
        let mut node = LiveNode::builder(TraderId::from("PUBLIC-TEST-001"), Environment::Live)?
            .with_logging(nautilus_common::logging::logger::LoggerConfig {
                stdout_level: log::LevelFilter::Warn,
                ..Default::default()
            })
            .with_data_engine_config(LiveDataEngineConfig {
                emit_quotes_from_book: true,
                ..Default::default()
            })
            .add_data_client(
                Some("PREDICT".into()),
                Box::new(nautilus_predict::PredictDataClientFactory::new()),
                Box::new(PredictDataClientConfig {
                    api_key: credentials.api_key.clone(),
                    market_ids: market_ids.clone(),
                    base_url_http: None,
                    base_url_ws: std::env::var("PREDICT_PUBLIC_WS_URL").ok(),
                    http_timeout_secs: 15,
                    ws_timeout_secs: 15,
                    dynamic_refresh_secs: 30,
                    transport_backend: backend,
                }),
            )?
            .build()?;
        let handle = node.handle();
        let journal = Rc::new(RefCell::new(vec![]));
        let markets = market_ids
            .into_iter()
            .map(|id| {
                Ok(Market {
                    id,
                    instruments: [
                        nautilus_predict::instrument_id_from_outcome(id, 1)?,
                        nautilus_predict::instrument_id_from_outcome(id, 2)?,
                    ],
                    phase: 0,
                    counts: [0; 3],
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        node.add_strategy(Feed {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(format!("PUBLIC-FEED-{backend_index:03}").into()),
                ..Default::default()
            }),
            markets,
            journal: journal.clone(),
            handle: handle.clone(),
        })?;
        let result = tokio::time::timeout(Duration::from_secs(90), node.run()).await;
        if result.is_err() {
            handle.stop();
        }
        captures.push(json!({"backend":format!("{backend:?}"),"events":journal.borrow().as_slice(),"completed":matches!(&result, Ok(Ok(())))}));
        if !matches!(result, Ok(Ok(()))) {
            serde_json::to_writer_pretty(
                output,
                &json!({"selected":selected,"captures":captures,"order_writes":0,"execution_client":false}),
            )?;
            anyhow::bail!("mainnet market-data probe did not complete; partial callbacks saved");
        }
    }
    serde_json::to_writer_pretty(
        output,
        &json!({"selected":selected,"captures":captures,"order_writes":0,"execution_client":false,"purpose":"public market-data lifecycle and exact-decimal validation"}),
    )?;
    Ok(())
}
