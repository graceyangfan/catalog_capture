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

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::UnixNanos;
use nautilus_live::SocketControl;
use nautilus_model::{data::Data, instruments::Instrument};
use nautilus_network::{
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{EpochMessageHandler, WebSocketClient, WebSocketConfig},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    ProphetLastTradePrice,
    protocol::{Book, Market, ProphetHttp, millis, now},
};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Interest {
    pub(crate) depth: Option<UnixNanos>,
    pub(crate) last_price: Option<UnixNanos>,
}

impl Interest {
    pub(crate) fn active(self) -> bool {
        self.depth.is_some() || self.last_price.is_some()
    }
}

#[derive(Deserialize)]
struct Stream<'a> {
    stream: &'a str,
    #[serde(borrow)]
    data: Push<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Push<'a> {
    biz_key: &'a str,
    #[serde(borrow)]
    content: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Identity<'a> {
    market_id: u64,
    msg_type: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LastPrice {
    update_timestamp_ms: u64,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    trade_price: Decimal,
}

pub(crate) struct MarketStream {
    pub(crate) market: Market,
    pub(crate) http: ProphetHttp,
    pub(crate) url: String,
    pub(crate) sender: EventSender<DataEvent>,
    pub(crate) control: SocketControl,
    pub(crate) interests: watch::Receiver<Interest>,
    pub(crate) cancel: CancellationToken,
}

impl MarketStream {
    pub(crate) async fn run(mut self) {
        let mut idle = self.interests.clone();
        let cancel = self.cancel.clone();
        let mut last_book = UnixNanos::default();
        let mut last_price = UnixNanos::default();
        let mut retry = Duration::from_millis(500);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                result = idle.wait_for(|interest| interest.active()) => if result.is_err() { return },
            }
            let attempt = cancel.child_token();
            let _stop_attempt = attempt.clone().drop_guard();
            let started = Instant::now();
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = idle.wait_for(|interest| !interest.active()) => {
                    retry = Duration::from_millis(500);
                    continue;
                },
                result = self.connect(attempt, &mut last_book, &mut last_price) => result,
            };
            if let Err(error) = result {
                log::warn!(
                    "Prophet market {} stream interrupted: {error:#}",
                    self.market.id
                );
            }
            if started.elapsed() >= Duration::from_secs(20) {
                retry = Duration::from_millis(500);
            }
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = idle.wait_for(|interest| !interest.active()) => {},
                () = tokio::time::sleep(retry) => retry = (retry * 2).min(Duration::from_secs(10)),
            }
        }
    }

    async fn connect(
        &mut self,
        cancel: CancellationToken,
        last_book: &mut UnixNanos,
        last_price: &mut UnixNanos,
    ) -> anyhow::Result<()> {
        let (tx, mut raw) = mpsc::channel(128);
        let overflow = Arc::new(AtomicBool::new(false));
        let overflow_flag = Arc::clone(&overflow);
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            if matches!(
                tx.try_send((epoch, now(), message)),
                Err(mpsc::error::TrySendError::Full(_))
            ) {
                overflow_flag.store(true, Ordering::Release);
            }
        });
        let socket = WebSocketClient::epoch_builder()
            .config(
                WebSocketConfig::builder()
                    .url(self.url.clone())
                    .headers(create_standard_nautilus_headers())
                    .connect_timeout_ms(8_000)
                    .reconnect_max_attempts(0)
                    .max_message_size_bytes(64 * 1024)
                    .max_frame_size_bytes(64 * 1024)
                    .build()?,
            )
            .epoch_handler(handler)
            .state_sink(self.control.sink())
            .cancellation_token(cancel.clone())
            .connect()
            .await?;
        let reconnect = cancel.clone();
        self.control.register(move || {
            if reconnect.is_cancelled() {
                ReconnectRequestOutcome::Closed
            } else {
                reconnect.cancel();
                ReconnectRequestOutcome::Accepted
            }
        });
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => Ok(()),
            result = self.receive(&socket, &mut raw, &overflow, last_book, last_price) => result,
        };
        socket.disconnect().await;
        result
    }

    async fn receive(
        &mut self,
        socket: &WebSocketClient,
        raw: &mut mpsc::Receiver<(u64, UnixNanos, Message)>,
        overflow: &AtomicBool,
        last_book: &mut UnixNanos,
        last_price: &mut UnixNanos,
    ) -> anyhow::Result<()> {
        let epoch = socket.connection_epoch();
        let channels = self.http.channels(self.market.id).await?;
        socket
            .send_text_on_connection(
                json!({"id":"1","method":"SUBSCRIBE","params":channels}).to_string(),
                None,
                epoch,
            )
            .await?;
        let mut ack = false;
        let mut requested_depth = None;
        let mut heartbeat = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(15),
            Duration::from_secs(15),
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut idle_deadline = Instant::now() + Duration::from_secs(40);
        loop {
            anyhow::ensure!(
                !overflow.load(Ordering::Acquire),
                "Prophet input queue overflow"
            );
            anyhow::ensure!(
                socket.connection_epoch() == epoch && socket.is_active(),
                "Prophet socket generation changed"
            );
            let depth_interest = self.interests.borrow_and_update().depth;
            if ack && depth_interest.is_some() && depth_interest != requested_depth {
                let book = self.http.book(&self.market).await?;
                anyhow::ensure!(
                    socket.connection_epoch() == epoch
                        && socket.is_active()
                        && !overflow.load(Ordering::Acquire),
                    "Prophet socket changed during snapshot request"
                );
                self.publish_book(book, now(), last_book)?;
            }
            requested_depth = if ack { depth_interest } else { None };

            tokio::select! {
                biased;
                update = self.interests.changed() => { update?; }
                _ = heartbeat.tick() => {
                    anyhow::ensure!(ack, "Prophet subscribe acknowledgement timed out");
                    socket.send_text_on_connection(
                        json!({"id":"2","method":"GET_PROPERTY","params":["combined"]}).to_string(),
                        None, epoch,
                    ).await?;
                }
                () = tokio::time::sleep_until(idle_deadline) => anyhow::bail!("Prophet socket idle"),
                next = raw.recv() => {
                    let (frame_epoch, received, message) = next.context("Prophet input closed")?;
                    idle_deadline = Instant::now() + Duration::from_secs(40);
                    anyhow::ensure!(
                        socket.connection_epoch() == epoch && socket.is_active()
                            && !overflow.load(Ordering::Acquire),
                        "Prophet socket changed before frame delivery"
                    );
                    if frame_epoch != epoch {
                        continue;
                    }
                    match message {
                        Message::Text(text) => {
                            if let Ok(frame) = serde_json::from_slice::<Stream<'_>>(&text) {
                                if ack {
                                    self.publish_frame(frame, &channels, received, last_book, last_price)?;
                                }
                            } else {
                                let control: Value = serde_json::from_slice(&text)?;
                                anyhow::ensure!(control.get("stream").is_none(), "malformed Prophet frame");
                                if control["id"] == "1" {
                                    anyhow::ensure!(control.get("result") == Some(&Value::Null), "Prophet subscribe rejected: {control}");
                                    ack = true;
                                }
                            }
                        }
                        Message::Ping(payload) => socket.send_pong_on_connection(payload.to_vec(), epoch).await?,
                        Message::Close(_) => anyhow::bail!("Prophet socket closed"),
                        _ => {}
                    }
                }
            }
        }
    }

    fn publish_frame(
        &self,
        frame: Stream<'_>,
        channels: &[String; 2],
        received: UnixNanos,
        last_book: &mut UnixNanos,
        last_price: &mut UnixNanos,
    ) -> anyhow::Result<()> {
        let identity: Identity<'_> = serde_json::from_str(frame.data.content.get())?;
        anyhow::ensure!(
            identity.market_id == self.market.id,
            "Prophet frame market mismatch"
        );
        match (frame.stream, frame.data.biz_key, identity.msg_type) {
            (stream, "PREDICTX_OBOOK", "orderbook") if stream == channels[0] => {
                let book = serde_json::from_str(frame.data.content.get())?;
                self.publish_book(book, received, last_book)?;
            }
            (stream, "PREDICTX_TRADE", "tradePrice") if stream == channels[1] => {
                let price: LastPrice = serde_json::from_str(frame.data.content.get())?;
                let ts_event = millis(price.update_timestamp_ms)?;
                let interest = self.interests.borrow();
                if !self.cancel.is_cancelled()
                    && interest.last_price.is_some_and(|since| received >= since)
                    && ts_event >= *last_price
                {
                    let price = ProphetLastTradePrice {
                        instrument_id: self.market.instrument.id(),
                        market_id: self.market.id,
                        price: self.market.price(price.trade_price)?,
                        ts_event,
                        ts_init: received,
                    };
                    self.sender
                        .send(DataEvent::Data(Data::Custom(price.into_custom_data())))?;
                    *last_price = ts_event;
                }
            }
            _ => anyhow::bail!("unexpected Prophet stream, business key or message type"),
        }
        Ok(())
    }

    fn publish_book(
        &self,
        book: Book,
        received: UnixNanos,
        last: &mut UnixNanos,
    ) -> anyhow::Result<()> {
        let ts_event = millis(book.timestamp)?;
        let interest = self.interests.borrow();
        if !self.cancel.is_cancelled()
            && interest.depth.is_some_and(|since| received >= since)
            && ts_event >= *last
        {
            let depth = self.market.depth(book, received)?;
            self.sender
                .send(DataEvent::Data(Data::BookDepth(Box::new(depth))))?;
            *last = ts_event;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use axum::{
        Json, Router,
        extract::{State, WebSocketUpgrade, ws},
        response::Response,
        routing::{get, post},
    };
    use nautilus_live::SocketControlFactory;
    use nautilus_model::{
        enums::AssetClass,
        identifiers::{ClientId, Symbol, Venue},
        instruments::{BinaryOption, InstrumentAny},
        types::{Currency, Price, Quantity},
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::{PROPHET, ProphetDataClientConfig, instrument_id, last_price_data_type};

    const MARKET: u64 = 17949854;
    const TS: u64 = 1794289918116;
    const BOOK: &str = include_str!("../tests/fixtures/knight-book.json");
    const TRADE: &str = include_str!("../tests/fixtures/knight-last-price.json");

    #[derive(Clone, Default)]
    struct Server {
        presubs: Arc<AtomicUsize>,
        connections: Arc<AtomicUsize>,
        close_first: Arc<Notify>,
    }

    async fn pre_subscribe(State(state): State<Server>, Json(request): Json<Value>) -> Json<Value> {
        state.presubs.fetch_add(1, Ordering::Relaxed);
        assert_eq!(
            request["subscribeList"][0]["subParamList"][0]["val"],
            MARKET.to_string()
        );
        let book: Value = serde_json::from_str(BOOK).unwrap();
        let trade: Value = serde_json::from_str(TRADE).unwrap();
        Json(json!({"code":"000000","success":true,"data":{"subKeys":{
            "1":book["stream"],"2":trade["stream"]
        }}}))
    }

    async fn snapshot(State(state): State<Server>) -> Json<Value> {
        let book: Value = serde_json::from_str(BOOK).unwrap();
        let book = &book["data"]["content"];
        let ts = if state.connections.load(Ordering::Relaxed) == 1 {
            TS - 1
        } else {
            TS + 2
        };
        Json(json!({"code":"000000","success":true,"data":{
            "tokenId":"123","timestamp":ts,"bids":book["bids"],"asks":book["asks"]
        }}))
    }

    async fn socket(State(state): State<Server>, upgrade: WebSocketUpgrade) -> Response {
        upgrade.on_upgrade(move |mut socket| async move {
            let generation = state.connections.fetch_add(1, Ordering::Relaxed);
            let Some(Ok(ws::Message::Text(request))) = socket.recv().await else {
                return;
            };
            let request: Value = serde_json::from_str(&request).unwrap();
            let mut book: Value = serde_json::from_str(BOOK).unwrap();
            let mut price: Value = serde_json::from_str(TRADE).unwrap();
            assert_eq!(
                request,
                json!({"id":"1","method":"SUBSCRIBE","params":[book["stream"],price["stream"]]})
            );
            socket
                .send(ws::Message::Text(r#"{"result":null,"id":"1"}"#.into()))
                .await
                .unwrap();
            if generation > 0 {
                // An old full book is still unusable after the second connection's fresh baseline.
                socket.send(ws::Message::Text(BOOK.into())).await.unwrap();
                book["data"]["content"]["updateTimestampMs"] = json!(TS + 3);
                price["data"]["content"]["updateTimestampMs"] = json!(TS + 3);
            }
            socket
                .send(ws::Message::Text(book.to_string().into()))
                .await
                .unwrap();
            socket
                .send(ws::Message::Text(price.to_string().into()))
                .await
                .unwrap();
            if generation == 0 {
                state.close_first.notified().await;
                let _ = socket.send(ws::Message::Close(None)).await;
            } else {
                while socket.recv().await.is_some_and(|result| result.is_ok()) {}
            }
        })
    }

    #[tokio::test]
    async fn reconnect_reacquires_channels_and_a_snapshot_without_suppressing_same_price() {
        let state = Server::default();
        let app = Router::new()
            .route("/ws", get(socket))
            .route(
                "/bapi/defi/v1/public/wallet-direct/ws/pre-subscribe",
                post(pre_subscribe),
            )
            .route(
                "/bapi/defi/v1/public/wallet-direct/predictx/order-book",
                post(snapshot),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let id = instrument_id(MARKET).unwrap();
        let instrument = BinaryOption::builder()
            .instrument_id(id)
            .raw_symbol(Symbol::new("17949854"))
            .asset_class(AssetClass::Alternative)
            .currency(Currency::get_or_create_crypto("SCORE"))
            .activation_ns(1.into())
            .expiration_ns(u64::MAX.into())
            .price_precision(3)
            .size_precision(6)
            .price_increment(Price::from("0.001"))
            .size_increment(Quantity::from("0.000001"))
            .ts_event(0.into())
            .ts_init(0.into())
            .build()
            .unwrap();
        let mut config = ProphetDataClientConfig::new(vec![MARKET]);
        config.http_url = format!("http://{addr}");
        let http = ProphetHttp::new(&config).unwrap();
        let (sender, mut events) = mpsc::unbounded_channel();
        let (_owner, interests) = watch::channel(Interest {
            depth: Some(now()),
            last_price: Some(now()),
        });
        let cancel = CancellationToken::new();
        let control = SocketControlFactory::new(ClientId::new(PROPHET), Some(Venue::new(PROPHET)))
            .control("test");
        let stream = MarketStream {
            market: Market {
                id: MARKET,
                token_id: "123".into(),
                instrument: InstrumentAny::BinaryOption(instrument),
            },
            http,
            url: format!("ws://{addr}/ws"),
            sender: sender.into(),
            control,
            interests,
            cancel: cancel.clone(),
        };
        let task = tokio::spawn(stream.run());

        for timestamps in [[TS - 1, TS, TS], [TS + 2, TS + 3, TS + 3]] {
            for (i, timestamp) in timestamps.into_iter().enumerate() {
                let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
                    .await
                    .unwrap()
                    .unwrap();
                match event {
                    DataEvent::Data(Data::BookDepth(book)) if i < 2 => {
                        assert_eq!(book.instrument_id, id);
                        assert_eq!(book.ts_event, millis(timestamp).unwrap());
                        assert_eq!(book.bids[0].price, Price::from("0.119"));
                        assert_eq!(book.bids[0].size, Quantity::from("20454.000000"));
                    }
                    DataEvent::Data(Data::Custom(event)) if i == 2 => {
                        assert_eq!(event.data_type, last_price_data_type(MARKET));
                        let price = event
                            .data
                            .as_any()
                            .downcast_ref::<ProphetLastTradePrice>()
                            .unwrap();
                        assert_eq!(price.ts_event, millis(timestamp).unwrap());
                        assert_eq!(price.price, Price::from("0.179"));
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            state.close_first.notify_one();
        }
        assert_eq!(state.presubs.load(Ordering::Relaxed), 2);
        cancel.cancel();
        task.await.unwrap();
        server.abort();
    }
}
