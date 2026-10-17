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

//! Recorded market definitions through Native requests, callbacks and lifecycle cancellation.
#![cfg(feature = "live")]

use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};

use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    http::HeaderMap,
    response::Response,
    routing::get,
};
use nautilus_common::{
    actor::DataActor,
    clients::DataClient,
    enums::Environment,
    live::runner::replace_data_event_sender,
    messages::{DataEvent, DataResponse, data::RequestInstrument},
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_live::node::{LiveNode, config::LiveExecutionEngineConfig};
use nautilus_model::{
    identifiers::{ClientId, InstrumentId, StrategyId, TraderId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_predict::{PredictDataClient, PredictDataClientConfig, PredictDataClientFactory};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use serde_json::json;
use tokio::sync::{Notify, mpsc};

const MARKET: &str = include_str!("../test_data/market_2859346.json");
const MARKET_ID: u64 = 2_859_346;

#[derive(Clone, Default)]
struct Venue {
    requested: Arc<Notify>,
    release: Option<Arc<Notify>>,
}
async fn definition(State(venue): State<Venue>, headers: HeaderMap) -> &'static str {
    assert_eq!(headers["x-api-key"], "request-test-key");
    venue.requested.notify_one();
    if let Some(release) = venue.release {
        release.notified().await;
    }
    MARKET
}
async fn websocket(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(|mut socket| async move { while socket.recv().await.is_some() {} })
}
async fn venue(state: Venue) -> (PredictDataClientConfig, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route(&format!("/v1/markets/{MARKET_ID}"), get(definition))
        .route("/ws", get(websocket))
        .with_state(state);
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (
        PredictDataClientConfig {
            api_key: "request-test-key".into(),
            market_ids: vec![],
            base_url_http: Some(format!("http://{addr}")),
            base_url_ws: Some(format!("ws://{addr}/ws")),
            http_timeout_secs: 3,
            ws_timeout_secs: 3,
            dynamic_refresh_secs: 30,
            transport_backend: Default::default(),
        },
        server,
    )
}
fn instrument(outcome: u8) -> InstrumentId {
    nautilus_predict::instrument_id_from_outcome(MARKET_ID, outcome).unwrap()
}
fn request(instrument_id: InstrumentId) -> RequestInstrument {
    RequestInstrument::new(
        instrument_id,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn venue_request_preserves_correlation_and_does_not_require_preloading() {
    let (config, server) = venue(Venue::default()).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
    let mut command = request(instrument(2));
    let mut params = Params::new();
    params.insert("force_instrument_update".into(), json!(true));
    command.params = Some(params.clone());
    command.start = Some("2020-01-01T00:00:00Z".parse().unwrap());
    command.end = Some("2020-01-02T00:00:00Z".parse().unwrap());
    let correlation_id = command.request_id;
    client.request_instrument(command).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let Some(DataEvent::Instrument(definition)) = rx.recv().await else {
            panic!("missing publication")
        };
        assert_eq!(definition.id(), instrument(2));
        let Some(DataEvent::Response(DataResponse::Instrument(response))) = rx.recv().await else {
            panic!("missing correlated response")
        };
        assert_eq!(response.correlation_id, correlation_id);
        assert_eq!(response.client_id, ClientId::new("PREDICT"));
        assert_eq!(response.instrument_id, instrument(2));
        assert_eq!(response.data.id(), instrument(2));
        assert_eq!(response.params, Some(params));
        assert_eq!(
            response.start,
            Some(UnixNanos::from(1_577_836_800_000_000_000))
        );
        assert_eq!(
            response.end,
            Some(UnixNanos::from(1_577_923_200_000_000_000))
        );
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    assert!(
        rx.try_recv().is_err(),
        "one request must produce exactly one response"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_identity_and_stopped_generation_cannot_publish_a_response() {
    let state = Venue {
        release: Some(Arc::new(Notify::new())),
        ..Default::default()
    };
    let (config, server) = venue(state.clone()).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    replace_data_event_sender(tx);
    let mut client = PredictDataClient::new(ClientId::new("PREDICT"), config).unwrap();
    for id in ["02859346-1.PREDICT", "2859346-3.PREDICT", "2859346-1.OTHER"] {
        assert!(
            client
                .request_instrument(request(id.parse().unwrap()))
                .is_err()
        );
    }
    let mut wrong_client = request(instrument(1));
    wrong_client.client_id = Some(ClientId::new("OTHER"));
    assert!(client.request_instrument(wrong_client).is_err());
    client.request_instrument(request(instrument(1))).unwrap();
    tokio::time::timeout(Duration::from_secs(3), state.requested.notified())
        .await
        .unwrap();
    client.reset().unwrap();
    state.release.unwrap().notify_one();
    client.disconnect().await.unwrap();
    assert!(
        rx.try_recv().is_err(),
        "retired requests must not publish definitions or responses"
    );
    assert!(client.request_instrument(request(instrument(1))).is_err());
    server.abort();
}

#[derive(Debug)]
struct RequestStrategy {
    core: StrategyCore,
    received: Rc<RefCell<Vec<InstrumentId>>>,
}
impl DataActor for RequestStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        assert!(self.cache().instrument(&instrument(1)).is_none());
        for outcome in [1, 2] {
            self.request_instrument(
                instrument(outcome),
                None,
                None,
                Some(ClientId::new("PREDICT")),
                None,
            )?;
        }
        Ok(())
    }
    fn on_instrument(&mut self, definition: &InstrumentAny) -> anyhow::Result<()> {
        assert_eq!(
            self.cache().instrument(&definition.id()).as_ref(),
            Some(definition)
        );
        self.received.borrow_mut().push(definition.id());
        Ok(())
    }
}
nautilus_strategy!(RequestStrategy);

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_instrument_requests_complete_native_callbacks_once() {
    let (config, server) = venue(Venue::default()).await;
    let mut node = LiveNode::builder(TraderId::new("REQUEST-001"), Environment::Live)
        .unwrap()
        .with_name("PredictInstrumentRequestOffline")
        .with_exec_engine_config(LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            ..Default::default()
        })
        .with_delay_post_stop_secs(0)
        .add_data_client(
            Some("PREDICT".into()),
            Box::new(PredictDataClientFactory::new()),
            Box::new(config),
        )
        .unwrap()
        .build()
        .unwrap();
    let received = Rc::new(RefCell::new(vec![]));
    node.add_strategy(RequestStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("REQUEST-PROBE-001")),
            ..Default::default()
        }),
        received: received.clone(),
    })
    .unwrap();
    let handle = node.handle();
    let driver = async {
        while received.borrow().len() < 2 {
            tokio::task::yield_now().await;
        }
        handle.stop();
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                node.run().await.unwrap();
            },
            driver
        );
    })
    .await
    .unwrap();
    let mut actual = received.borrow().clone();
    actual.sort();
    assert_eq!(actual, vec![instrument(1), instrument(2)]);
    server.abort();
}
