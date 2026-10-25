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

//! Isolated-clock dynamic quotes through a real Rust Strategy and LiveNode; no execution client.
#![cfg(feature = "execution")]
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    extract::{
        Path as RoutePath, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    actor::DataActor,
    enums::Environment,
    msgbus::{self, ShareableMessageHandler},
};
use nautilus_core::{
    Params, UnixNanos, string::secret::SecretString, time::get_atomic_clock_realtime,
};
use nautilus_live::node::{LiveNode, config::LiveExecutionEngineConfig};
use nautilus_model::{
    data::{CustomData, DataType, QuoteTick},
    identifiers::{ClientId, InstrumentId, StrategyId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::{
    PredictDataClientConfig, PredictDataClientFactory, PredictOrderbookSnapshot,
};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use serde_json::{Value, json};
use tokio::sync::mpsc;
const OLD: u64 = 2859346;
const NEW: u64 = OLD + 1;
fn instrument(market: u64, outcome: u8) -> InstrumentId {
    nautilus_predict::instrument_id_from_outcome(market, outcome).unwrap()
}
fn selector() -> DataType {
    let metadata = Params::new(
        [
            ("price_feed_symbol".into(), json!("BTC/USDT")),
            ("title_asset".into(), json!("Bitcoin")),
            ("interval_secs".into(), json!("300")),
        ]
        .into(),
    );
    DataType::new("PredictCryptoUpDown", Some(metadata), None)
}
fn responses() -> DataType {
    let key =
        nautilus_predict::common::CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
    let output = nautilus_predict::predict_crypto_up_down_orderbook_data_type(OLD, &key);
    DataType::new(output.type_name(), output.metadata().cloned(), None)
}
fn market(id: u64) -> Value {
    let mut m: Value =
        serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
    m["data"]["id"] = json!(id);
    m["data"]["status"] = json!("OPEN");
    m["data"]["tradingStatus"] = json!("OPEN");
    m["data"]["variantData"]["priceFeedSymbol"] = json!("BTC/USDT");
    if id == NEW {
        m["data"]["title"] = json!("Bitcoin Up or Down - October 3, 6:05PM-6:10PM ET");
    }
    m
}
fn snapshot(id: u64, ts: u64, bid: &str, count: u64) -> Value {
    let mut b: Value =
        serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
    b["marketId"] = json!(id);
    b["lastOrderSettled"]["marketId"] = json!(id);
    b["updateTimestampMs"] = json!(ts);
    b["orderCount"] = json!(count);
    b["bids"] = json!([[bid, "1500.25"]]);
    json!({"type":"M","topic":format!("predictOrderbook/{id}"),"data":b})
}
#[derive(Clone)]
struct VenueState {
    start: u64,
    books: Arc<Mutex<BTreeMap<u64, Value>>>,
    commands: Arc<Mutex<Vec<Value>>>,
    frames: Arc<Mutex<Vec<Value>>>,
    sockets: Arc<Mutex<Vec<mpsc::UnboundedSender<Message>>>>,
}
async fn category(RoutePath(slug): RoutePath<String>, State(v): State<VenueState>) -> String {
    let id = if slug == format!("btc-updown-5m-{}", v.start) {
        Some(OLD)
    } else if slug == format!("btc-updown-5m-{}", v.start + 300) {
        Some(NEW)
    } else {
        None
    };
    json!({"success":true,"data":{"markets":id.map(|id|vec![market(id)["data"].clone()]).unwrap_or_default()}}).to_string()
}
async fn definition(RoutePath(id): RoutePath<u64>) -> String {
    assert!([OLD, NEW].contains(&id));
    market(id).to_string()
}
async fn websocket(ws: WebSocketUpgrade, State(v): State<VenueState>) -> Response {
    ws.on_upgrade(move |s| socket(s, v))
}
async fn socket(s: WebSocket, v: VenueState) {
    let (mut sink, mut source) = s.split();
    let (tx, mut rx) = mpsc::unbounded_channel();
    v.sockets.lock().unwrap().push(tx);
    let mut active = BTreeSet::new();
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    loop {
        let frames = tokio::select! {
            message=source.next()=>{
                let Some(Ok(Message::Text(text)))=message else {break};
                let c:Value=serde_json::from_str(&text).unwrap();v.commands.lock().unwrap().push(c.clone());
                if let Some(topic)=c["params"][0].as_str() { let id:u64=topic.strip_prefix("predictOrderbook/").unwrap().parse().unwrap();
                    match c["method"].as_str().unwrap(){"subscribe"=>{active.insert(id);},"unsubscribe"=>{active.remove(&id);},_=>panic!("unknown method")}
                }
                vec![]
            }
            Some(message)=rx.recv()=>{sink.send(message).await.unwrap();vec![]}
            _=tick.tick()=>{let books=v.books.lock().unwrap();active.iter().filter_map(|id|books.get(id).cloned()).collect::<Vec<_>>()}
        };
        for frame in frames {
            v.frames.lock().unwrap().push(frame.clone());
            if sink
                .send(Message::Text(frame.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
    }
}
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    phase: Rc<Cell<u8>>,
    depth: bool,
    journal: Rc<RefCell<Vec<Value>>>,
    owned: BTreeSet<InstrumentId>,
    audit: Option<ShareableMessageHandler>,
}
impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let journal = self.journal.clone();
        let handler = ShareableMessageHandler::from_typed(move |c: &CustomData| {
            if let Some(b) = c.data.as_any().downcast_ref::<PredictOrderbookSnapshot>() {
                journal.borrow_mut().push(json!({"type":"custom","market":b.market_id,"ts":b.ts_event.as_u64(),"count":b.order_count}));
            }
        });
        msgbus::subscribe_any(
            "data.PredictOrderbookSnapshot*".into(),
            handler.clone(),
            None,
        );
        self.audit = Some(handler);
        self.subscribe_instruments(Venue::new("PREDICT"), Some(ClientId::new("PREDICT")), None);
        // Native local-only response subscription; no second venue interest is added.
        self.subscribe_data(responses(), None, None);
        self.subscribe_data(selector(), Some(ClientId::new("PREDICT")), None);
        Ok(())
    }
    fn on_instrument(&mut self, i: &InstrumentAny) -> anyhow::Result<()> {
        if self.owned.insert(i.id()) {
            if self.depth {
                self.subscribe_book_depth(
                    i.id(),
                    nautilus_model::enums::BookType::L2_MBP,
                    None,
                    Some(ClientId::new("PREDICT")),
                    true,
                    None,
                );
            } else {
                self.subscribe_quotes(i.id(), Some(ClientId::new("PREDICT")), None);
            }
            self.journal
                .borrow_mut()
                .push(json!({"type":"instrument","id":i.id().to_string()}));
        }
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        {
            assert_eq!(
                data.data_type.identifier(),
                Some(book.market_id.to_string().as_str())
            );
            assert_eq!(data.data_type.metadata(), responses().metadata());
            self.journal.borrow_mut().push(json!({"type":"strategy_custom","market":book.market_id,"ts":book.ts_event.as_u64(),"count":book.order_count,"identifier":data.data_type.identifier(),"selector":data.data_type.metadata()}));
        }
        Ok(())
    }
    fn on_book_depth(
        &mut self,
        depth: &nautilus_model::data::OrderBookDepth,
    ) -> anyhow::Result<()> {
        assert!(self.depth);
        let quote = {
            let cache = self.cache();
            let book = cache.order_book(&depth.instrument_id).unwrap();
            let quote = cache.quote(&depth.instrument_id).unwrap();
            assert_eq!(book.best_bid_price(), Some(depth.bids[0].price));
            assert_eq!(quote.bid_price, depth.bids[0].price);
            quote
        };
        self.on_quote(&quote)
    }

    fn on_quote(&mut self, q: &QuoteTick) -> anyhow::Result<()> {
        self.journal.borrow_mut().push(json!({"type":"quote","id":q.instrument_id.to_string(),"ts":q.ts_event.as_u64(),"bid":q.bid_price.to_string(),"ask":q.ask_price.to_string()}));
        let target = match self.phase.get() {
            1 => Some(instrument(OLD, 1)),
            2 => Some(instrument(OLD, 2)),
            _ => None,
        };
        if let Some(id) = target
            && self.owned.remove(&id)
        {
            if self.depth {
                self.unsubscribe_book_depth(id, Some(ClientId::new("PREDICT")), None);
            } else {
                self.unsubscribe_quotes(id, Some(ClientId::new("PREDICT")), None);
            }
            self.journal
                .borrow_mut()
                .push(json!({"type":"release","id":id.to_string()}));
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        for id in std::mem::take(&mut self.owned) {
            if self.depth {
                self.unsubscribe_book_depth(id, Some(ClientId::new("PREDICT")), None);
            } else {
                self.unsubscribe_quotes(id, Some(ClientId::new("PREDICT")), None);
            }
        }
        self.unsubscribe_data(responses(), None, None);
        self.unsubscribe_data(selector(), Some(ClientId::new("PREDICT")), None);
        if let Some(h) = self.audit.take() {
            msgbus::unsubscribe_any("data.PredictOrderbookSnapshot*".into(), &h);
        }
        Ok(())
    }
}
nautilus_strategy!(Probe, {});
struct ClockGuard;
impl Drop for ClockGuard {
    fn drop(&mut self) {
        get_atomic_clock_realtime().make_realtime();
    }
}
async fn barrier(label: &str, mut p: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !p() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("dynamic quote barrier: {label}"));
}
async fn run(backend: TransportBackend, saved: Option<Value>) -> Value {
    run_with_depth(backend, saved, false).await
}
async fn run_with_depth(backend: TransportBackend, saved: Option<Value>, depth: bool) -> Value {
    nautilus_predict::register_predict_custom_data();
    let start = chrono::DateTime::parse_from_rfc3339("2026-10-03T22:00:00Z")
        .unwrap()
        .timestamp() as u64;
    let clock = get_atomic_clock_realtime();
    clock.make_static();
    clock.set_time(UnixNanos::from((start + 299) * 1_000_000_000));
    let _guard = ClockGuard;
    let boundary = (start + 300) * 1000;
    let frame = |id, ts, bid: &str, count| {
        let generated = snapshot(id, ts, bid, count);
        if let Some(saved) = &saved {
            let recorded = saved["frames"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| {
                    r["data"]["marketId"] == id
                        && r["data"]["updateTimestampMs"] == ts
                        && r["data"]["orderCount"] == count
                })
                .unwrap();
            assert_eq!(recorded, &generated);
            recorded.clone()
        } else {
            generated
        }
    };
    let venue = VenueState {
        start,
        books: Arc::new(Mutex::new(BTreeMap::from([
            (OLD, frame(OLD, boundary - 1000, "0.62", 41)),
            (NEW, frame(NEW, boundary, "0.62", 43)),
        ]))),
        commands: Arc::default(),
        frames: Arc::default(),
        sockets: Arc::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/v1/categories/{slug}", get(category))
        .route("/v1/markets/{id}", get(definition))
        .route("/ws", get(websocket))
        .with_state(venue.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut node = LiveNode::builder(TraderId::new("QUOTES-001"), Environment::Live)
        .unwrap()
        .with_name("PredictDynamicQuotesOffline")
        .with_data_engine_config(nautilus_live::node::config::LiveDataEngineConfig {
            emit_quotes_from_book: depth,
            ..Default::default()
        })
        .with_timeout_connection(5)
        .with_delay_post_stop_secs(0)
        .with_exec_engine_config(LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            ..Default::default()
        })
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(PredictDataClientConfig {
                api_key: SecretString::from("loopback-only"),
                market_ids: vec![],
                base_url_http: Some(format!("http://{addr}")),
                base_url_ws: Some(format!("ws://{addr}/ws")),
                http_timeout_secs: 5,
                ws_timeout_secs: 5,
                dynamic_refresh_secs: 1,
                transport_backend: backend,
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    let phase = Rc::new(Cell::new(0));
    let journal = Rc::new(RefCell::new(vec![]));
    node.add_strategy(Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("QUOTE-ROLLOVER-001")),
            ..Default::default()
        }),
        phase: phase.clone(),
        depth,
        journal: journal.clone(),
        owned: BTreeSet::new(),
        audit: None,
    })
    .unwrap();
    let stop = node.handle();
    let has_quote = |id: InstrumentId, ts: u64| {
        journal
            .borrow()
            .iter()
            .any(|r| r["type"] == "quote" && r["id"] == id.to_string() && r["ts"] == ts * 1_000_000)
    };
    let driver = async {
        barrier("initial dynamic YES and NO quotes", || {
            has_quote(instrument(OLD, 1), boundary - 1000)
                && has_quote(instrument(OLD, 2), boundary - 1000)
        })
        .await;
        clock.set_time(UnixNanos::from((start + 300) * 1_000_000_000));
        barrier("successor custom and quotes", || {
            journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "custom" && r["market"] == NEW)
                && has_quote(instrument(NEW, 2), boundary)
        })
        .await;
        venue
            .books
            .lock()
            .unwrap()
            .insert(OLD, frame(OLD, boundary + 1, "0.61", 44));
        barrier("old quote contexts survive selector retirement", || {
            has_quote(instrument(OLD, 1), boundary + 1)
                && has_quote(instrument(OLD, 2), boundary + 1)
        })
        .await;
        assert!(
            !venue
                .commands
                .lock()
                .unwrap()
                .iter()
                .any(|c| c["method"] == "unsubscribe"
                    && c["params"][0] == format!("predictOrderbook/{OLD}"))
        );
        phase.set(1);
        venue
            .books
            .lock()
            .unwrap()
            .insert(OLD, frame(OLD, boundary + 2, "0.60", 45));
        barrier("YES owner release", || {
            journal
                .borrow()
                .iter()
                .any(|r| r["type"] == "release" && r["id"] == instrument(OLD, 1).to_string())
        })
        .await;
        venue
            .books
            .lock()
            .unwrap()
            .insert(OLD, frame(OLD, boundary + 3, "0.59", 46));
        barrier("remaining NO owner still receives quotes", || {
            has_quote(instrument(OLD, 2), boundary + 3)
        })
        .await;
        assert!(
            !venue
                .commands
                .lock()
                .unwrap()
                .iter()
                .any(|c| c["method"] == "unsubscribe"
                    && c["params"][0] == format!("predictOrderbook/{OLD}"))
        );
        phase.set(2);
        venue
            .books
            .lock()
            .unwrap()
            .insert(OLD, frame(OLD, boundary + 4, "0.58", 47));
        barrier("last old owner unsubscribes physical topic", || {
            venue.commands.lock().unwrap().iter().any(|c| {
                c["method"] == "unsubscribe" && c["params"][0] == format!("predictOrderbook/{OLD}")
            })
        })
        .await;
        let late = frame(OLD, boundary + 9, "0.57", 999);
        let fresh = frame(NEW, boundary + 10, "0.61", 48);
        for frame in [late, fresh] {
            venue.frames.lock().unwrap().push(frame.clone());
            venue
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Text(frame.to_string().into()))
                .unwrap();
        }
        barrier(
            "successor barrier after retired unsolicited old frame",
            || has_quote(instrument(NEW, 2), boundary + 10),
        )
        .await;
        stop.stop();
    };
    let deadline = async {
        tokio::time::sleep(Duration::from_secs(25)).await;
        panic!("dynamic quote watchdog {:?}", journal.borrow());
    };
    tokio::select! {r=async{let(r,())=tokio::join!(node.run(),driver);r.unwrap();}=>r,()=deadline=>{}}
    server.abort();
    let rows = journal.borrow();
    let observed = |kind| {
        rows.iter()
            .filter(|r| r["type"] == kind)
            .map(|r| (r["market"].clone(), r["ts"].clone(), r["count"].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        observed("custom"),
        observed("strategy_custom"),
        "native on_data must receive selector-scoped snapshots without another venue subscription"
    );
    let first_new = rows
        .iter()
        .position(|r| r["type"] == "custom" && r["market"] == NEW)
        .unwrap();
    assert!(
        !rows[first_new + 1..]
            .iter()
            .any(|r| r["type"] == "custom" && r["market"] == OLD)
    );
    assert!(!rows.iter().any(|r| r["type"] == "quote"
        && r["id"].as_str().unwrap().starts_with(&OLD.to_string())
        && r["ts"] == (boundary + 9) * 1_000_000));
    assert_eq!(rows.iter().filter(|r| r["type"] == "release").count(), 2);
    let commands = venue.commands.lock().unwrap();
    assert_eq!(
        commands
            .iter()
            .filter(|c| c["method"] == "subscribe"
                && c["params"][0] == format!("predictOrderbook/{OLD}"))
            .count(),
        1
    );
    assert_eq!(
        commands
            .iter()
            .filter(|c| c["method"] == "unsubscribe"
                && c["params"][0] == format!("predictOrderbook/{OLD}"))
            .count(),
        1
    );
    json!({"backend":format!("{backend:?}"),"source":"synthetic Rust Strategy + LiveNode dynamic quotes; no execution client","commands":*commands,"frames":*venue.frames.lock().unwrap(),"feedback":*rows,"mainnetWrites":0})
}
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/dynamic_quotes_strategy_rollover_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn dynamic_quote_owners_survive_rollover_then_last_release_drops_late_frames() {
    let fixture: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for backend in [TransportBackend::default(), TransportBackend::Tungstenite] {
        let expected = fixture["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["backend"] == format!("{backend:?}"))
            .unwrap();
        let actual = run(backend, Some(expected.clone())).await;
        let observed = |s: &Value| {
            s["feedback"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["type"] == "quote" || r["type"] == "release")
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(observed(&actual), observed(expected));
        let native = run_with_depth(backend, None, true).await;
        // Depth also delivers unchanged snapshots, so the strategy may release an
        // owner earlier than the quote-only fixture. Final ownership must agree;
        // run_with_depth checks Native book/quote facts and last-owner wire removal.
        let releases = |s: &Value| {
            observed(s)
                .into_iter()
                .filter(|r| r["type"] == "release")
                .collect::<Vec<_>>()
        };
        assert_eq!(releases(&native), releases(expected));
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback dynamic quote JSON; never overwrite"]
async fn record_dynamic_quotes_rollover_json() {
    let sessions = vec![
        run(TransportBackend::default(), None).await,
        run(TransportBackend::Tungstenite, None).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path())
        .unwrap();
    serde_json::to_writer_pretty(
        file,
        &json!({"source":"synthetic offline dynamic quotes; no order writes","sessions":sessions}),
    )
    .unwrap();
}
