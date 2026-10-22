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

//! Actual Rust strategy callbacks on LiveNode; only loopback public market data.
use super::*;
use nautilus_common::{actor::DataActor, enums::Environment};
use nautilus_live::{
    node::LiveNodeHandle,
    node::{LiveNode, config::LiveDataEngineConfig},
};
use nautilus_model::{
    data::{CustomData, OrderBookDepth},
    enums::BookType,
    identifiers::TraderId,
};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use std::{cell::RefCell, rc::Rc};

#[derive(Debug, Default)]
struct Observed {
    custom: usize,
    depth: usize,
}
#[derive(Debug)]
struct Feed {
    core: StrategyCore,
    custom: bool,
    depth: bool,
    managed: bool,
    frames: usize,
    ack: mpsc::UnboundedSender<()>,
    observed: Rc<RefCell<Observed>>,
    handle: LiveNodeHandle,
}
impl Feed {
    fn received(&self, custom: bool) {
        let mut observed = self.observed.borrow_mut();
        if custom {
            observed.custom += 1;
        } else {
            observed.depth += 1;
        }
        self.ack.send(()).unwrap();
        if (!self.custom || observed.custom == self.frames)
            && (!self.depth || observed.depth == self.frames)
        {
            self.handle.stop();
        }
    }
}
impl DataActor for Feed {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.custom {
            self.subscribe_data(
                predict_orderbook_data_type(MARKET_ID),
                Some(ClientId::from("PREDICT")),
                None,
            );
        }
        if self.depth {
            self.subscribe_book_depth(
                nautilus_predict::instrument_id_from_outcome(MARKET_ID, 1)?,
                BookType::L2_MBP,
                None,
                Some(ClientId::from("PREDICT")),
                self.managed,
                None,
            );
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        if self.custom {
            self.unsubscribe_data(
                predict_orderbook_data_type(MARKET_ID),
                Some(ClientId::from("PREDICT")),
                None,
            );
        }
        if self.depth {
            self.unsubscribe_book_depth(
                nautilus_predict::instrument_id_from_outcome(MARKET_ID, 1)?,
                Some(ClientId::from("PREDICT")),
                None,
            );
        }
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        assert!(self.custom);
        let book = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .unwrap();
        assert_eq!(data.data_type, predict_orderbook_data_type(MARKET_ID));
        assert_eq!(book.order_count, self.observed.borrow().custom as u64);
        assert!(book.last_order_settled.is_some() && book.settlements_pending.is_some());
        self.received(true);
        Ok(())
    }
    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        assert!(self.depth);
        assert_eq!(
            depth.instrument_id,
            nautilus_predict::instrument_id_from_outcome(MARKET_ID, 1)?
        );
        let cache = self.cache();
        if self.managed {
            let book = cache.order_book(&depth.instrument_id).unwrap();
            let quote = cache.quote(&depth.instrument_id).unwrap();
            assert_eq!(book.best_bid_price(), Some(depth.bids[0].price));
            assert_eq!(quote.bid_price, depth.bids[0].price);
            assert_eq!(quote.ts_event, depth.ts_event);
        } else {
            assert!(!cache.has_order_book(&depth.instrument_id));
            assert!(cache.quote(&depth.instrument_id).is_none());
        }
        drop(cache);
        self.received(false);
        Ok(())
    }
}
nautilus_strategy!(Feed, {});

#[derive(Clone)]
struct Stream {
    frames: usize,
    callbacks: usize,
    ack: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>>,
    subscriptions: Arc<AtomicUsize>,
}
async fn socket(ws: WebSocketUpgrade, State(state): State<Stream>) -> Response {
    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut source) = socket.split();
        while let Some(Ok(Message::Text(text))) = source.next().await {
            let request: Value = serde_json::from_str(&text).unwrap();
            if request["method"] != "subscribe" {
                continue;
            }
            assert_eq!(
                state.subscriptions.fetch_add(1, Ordering::AcqRel),
                0,
                "one physical topic"
            );
            let mut ack = state.ack.lock().await;
            for index in 0..state.frames {
                let mut book: Value = serde_json::from_str(BOOK).unwrap();
                book["orderCount"] = json!(index);
                book["updateTimestampMs"] = json!(1_700_000_000_123u64 + index as u64);
                book["bids"][0][0] = json!(if index.is_multiple_of(2) {
                    "0.62"
                } else {
                    "0.61"
                });
                let frame =
                    json!({"type":"M","topic":format!("predictOrderbook/{MARKET_ID}"),"data":book})
                        .to_string();
                if sink.send(Message::Text(frame.into())).await.is_err() {
                    return;
                }
                for _ in 0..state.callbacks {
                    if tokio::time::timeout(Duration::from_secs(5), ack.recv())
                        .await
                        .unwrap()
                        .is_none()
                    {
                        return;
                    }
                }
            }
        }
    })
}
async fn run(custom: bool, depth: bool, managed: bool, frames: usize) {
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let stream = Stream {
        frames,
        callbacks: usize::from(custom) + usize::from(depth),
        ack: Arc::new(tokio::sync::Mutex::new(ack_rx)),
        subscriptions: subscriptions.clone(),
    };
    let router = Router::new()
        .route("/v1/markets/{id}", get(market))
        .route("/ws", get(socket))
        .with_state(stream);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut node = LiveNode::builder(TraderId::from("DEPTH-TEST-001"), Environment::Live)
        .unwrap()
        .with_logging(nautilus_common::logging::logger::LoggerConfig {
            stdout_level: log::LevelFilter::Warn,
            ..Default::default()
        })
        .with_data_engine_config(LiveDataEngineConfig {
            emit_quotes_from_book: managed,
            ..Default::default()
        })
        .with_delay_post_stop_secs(0)
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(nautilus_predict::PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key: SecretString::from("fixture-key"),
                market_ids: vec![MARKET_ID],
                base_url_http: Some(format!("http://{address}")),
                base_url_ws: Some(format!("ws://{address}/ws")),
                http_timeout_secs: 5,
                ws_timeout_secs: 5,
                dynamic_refresh_secs: 30,
                transport_backend: TransportBackend::Tungstenite,
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    let handle = node.handle();
    let observed = Rc::new(RefCell::new(Observed::default()));
    node.add_strategy(Feed {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("FEED-001".into()),
            ..Default::default()
        }),
        custom,
        depth,
        managed,
        frames,
        ack: ack_tx,
        observed: observed.clone(),
        handle,
    })
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), node.run())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(subscriptions.load(Ordering::Acquire), 1);
    let observed = observed.borrow();
    assert_eq!(observed.custom, if custom { frames } else { 0 });
    assert_eq!(observed.depth, if depth { frames } else { 0 });
    server.abort();
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_receives_custom_and_native_depth_with_managed_state_ready() {
    for (custom, depth, managed) in [
        (true, false, false),
        (false, true, false),
        (false, true, true),
        (true, true, true),
    ] {
        run(custom, depth, managed, 20).await;
    }
}
