// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Live public market-data client for Bullet.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{
    cache::InstrumentLookupError,
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            CustomDataResponse, DataResponse, RequestCustomData, SubscribeBookDeltas,
            SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument, SubscribeMarkPrices,
            SubscribeTrades, UnsubscribeBookDeltas, UnsubscribeFundingRates,
            UnsubscribeIndexPrices, UnsubscribeInstrument, UnsubscribeMarkPrices,
            UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    AtomicMap,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{SocketControlFactory, task::TaskGroup};
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    common::{BULLET_BOOK_DEPTH, BULLET_VENUE},
    config::BulletDataClientConfig,
    funding::{FundingCommand, estimate_instrument, run_funding_worker},
    http::BulletHttpClient,
    websocket::{
        BulletWebSocketSession,
        messages::{BulletCommand, BulletDataKind, BulletTopic},
    },
};

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct BulletDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: BulletDataClientConfig,
    http_client: BulletHttpClient,
    socket_factory: SocketControlFactory,
    data_sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    instruments_by_market: Arc<AtomicMap<Ustr, InstrumentAny>>,
    command_tx: Option<mpsc::Sender<BulletCommand>>,
    funding_tx: Option<mpsc::Sender<FundingCommand>>,
    tasks: TaskGroup,
    cancellation: CancellationToken,
    connected: AtomicBool,
}

impl BulletDataClient {
    pub fn new(client_id: ClientId, config: BulletDataClientConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.funding_poll_interval_secs > 0,
            "Bullet funding poll interval must be positive"
        );
        let tasks = TaskGroup::new();
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            http_client: BulletHttpClient::new(&config)?,
            socket_factory: SocketControlFactory::new(client_id, Some(*BULLET_VENUE)),
            data_sender: get_data_event_sender(),
            instruments: Arc::new(AtomicMap::new()),
            instruments_by_market: Arc::new(AtomicMap::new()),
            command_tx: None,
            cancellation: tasks.cancellation_token(),
            tasks,
            funding_tx: None,
            connected: AtomicBool::new(false),
            config,
        })
    }

    async fn bootstrap_instruments(&self) -> anyhow::Result<Vec<InstrumentAny>> {
        let instruments = self
            .http_client
            .request_instruments(&self.config.instrument_provider, self.clock.get_time_ns())
            .await?;
        self.replace_instrument_cache(&instruments);
        Ok(instruments)
    }

    fn replace_instrument_cache(&self, instruments: &[InstrumentAny]) {
        let by_id = instruments
            .iter()
            .map(|instrument| (instrument.id(), instrument.clone()))
            .collect::<AHashMap<_, _>>();
        let by_market = instruments
            .iter()
            .map(|instrument| (instrument.raw_symbol().inner(), instrument.clone()))
            .collect::<AHashMap<_, _>>();
        self.instruments.store(by_id);
        self.instruments_by_market.store(by_market);
    }

    fn send_command(&self, command: BulletCommand) -> anyhow::Result<()> {
        let sender = self
            .command_tx
            .as_ref()
            .context("Bullet WebSocket session is not connected")?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                anyhow::anyhow!("Bullet subscription command queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("Bullet subscription command queue is closed")
            }
        })
    }

    fn subscribe_topic(
        &self,
        kind: BulletDataKind,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(instrument_ids, |id| {
            self.require_instrument(id)?;
            self.send_command(BulletCommand::Subscribe(BulletTopic::new(kind, id)?))
        })
    }

    fn unsubscribe_topic(
        &self,
        kind: BulletDataKind,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(instrument_ids, |id| {
            self.send_command(BulletCommand::Unsubscribe(BulletTopic::new(kind, id)?))
        })
    }

    fn require_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id).into())
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for BulletDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*BULLET_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting Bullet data client: client_id={}, environment={:?}",
            self.client_id,
            self.config.environment,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.funding_tx = None;
        self.connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if !self.tasks.is_open() {
            self.tasks
                .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
                .await
                .context("failed to drain previous Bullet task generation")?;
            self.tasks
                .start_generation()
                .context("failed to start Bullet task generation")?;
            self.cancellation = self.tasks.cancellation_token();
        }

        let instruments = self.bootstrap_instruments().await?;
        for instrument in instruments {
            self.data_sender
                .send(DataEvent::Data(Data::Instrument(Box::new(instrument))))?;
        }

        let socket_control = self.socket_factory.control("public-data");
        let (command_tx, session) = BulletWebSocketSession::connect(
            &self.config,
            Arc::clone(&self.instruments_by_market),
            self.data_sender.clone(),
            self.clock,
            self.cancellation.clone(),
            socket_control,
        )
        .await?;
        self.tasks
            .spawn(session.run())
            .context("failed to spawn Bullet WebSocket session")?;
        self.command_tx = Some(command_tx);
        let (funding_tx, funding_rx) = mpsc::channel(256);
        self.tasks.spawn(run_funding_worker(
            self.http_client.clone(),
            funding_rx,
            self.data_sender.clone(),
            self.clock,
            self.cancellation.clone(),
            Duration::from_secs(self.config.funding_poll_interval_secs),
        ))?;
        self.funding_tx = Some(funding_tx);
        self.connected.store(true, Ordering::Release);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.funding_tx = None;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
            .await
            .context("failed to stop Bullet tasks")?;
        self.instruments.store(AHashMap::new());
        self.instruments_by_market.store(AHashMap::new());
        self.connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            let instrument = self.require_instrument(id)?;
            self.data_sender
                .send(DataEvent::Data(Data::Instrument(Box::new(instrument))))?;
            Ok(())
        })
    }

    fn unsubscribe_instrument(
        &mut self,
        _unsubscription: &UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, subscription: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            subscription.book_type == BookType::L2_MBP,
            "Bullet only supports L2_MBP book deltas",
        );
        anyhow::ensure!(
            subscription.depth.map(std::num::NonZeroUsize::get) == Some(BULLET_BOOK_DEPTH),
            "Bullet requires explicit depth=20; full-depth semantics are not established",
        );
        self.subscribe_topic(BulletDataKind::OrderBook, &subscription.instrument_ids)
    }

    fn unsubscribe_book_deltas(
        &mut self,
        unsubscription: &UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(BulletDataKind::OrderBook, &unsubscription.instrument_ids)
    }

    fn subscribe_trades(&mut self, subscription: SubscribeTrades) -> anyhow::Result<()> {
        self.subscribe_topic(BulletDataKind::Trades, &subscription.instrument_ids)
    }

    fn unsubscribe_trades(&mut self, unsubscription: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.unsubscribe_topic(BulletDataKind::Trades, &unsubscription.instrument_ids)
    }

    fn subscribe_mark_prices(&mut self, subscription: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_topic(BulletDataKind::MarkPrice, &subscription.instrument_ids)
    }

    fn unsubscribe_mark_prices(
        &mut self,
        unsubscription: &UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(BulletDataKind::MarkPrice, &unsubscription.instrument_ids)
    }

    fn subscribe_index_prices(&mut self, subscription: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_topic(BulletDataKind::IndexPrice, &subscription.instrument_ids)
    }

    fn unsubscribe_index_prices(
        &mut self,
        unsubscription: &UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(BulletDataKind::IndexPrice, &unsubscription.instrument_ids)
    }
    fn subscribe_funding_rates(
        &mut self,
        subscription: SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            self.require_instrument(id)?;
            self.funding_tx
                .as_ref()
                .context("Bullet funding worker is not connected")?
                .try_send(FundingCommand::Subscribe(id))
                .context("Bullet funding command admission failed")?;
            Ok(())
        })
    }
    fn unsubscribe_funding_rates(
        &mut self,
        subscription: &UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            crate::common::market_from_instrument_id(id)?;
            self.funding_tx
                .as_ref()
                .context("Bullet funding worker is not connected")?
                .try_send(FundingCommand::Unsubscribe(id))
                .context("Bullet funding command admission failed")?;
            Ok(())
        })
    }
    fn request_data(&self, request: RequestCustomData) -> anyhow::Result<()> {
        let id = estimate_instrument(&request.data_type)?;
        self.require_instrument(id)?;
        anyhow::ensure!(
            request.start.is_none() && request.end.is_none() && request.limit.is_none(),
            "Bullet funding estimates support current snapshots only"
        );
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancel = self.cancellation.clone();
        self.tasks.spawn(async move {
            let result = tokio::select! { biased; () = cancel.cancelled() => return, result = http.request_funding_estimate(id, clock) => result };
            if cancel.is_cancelled() { return; }
            match result {
                Ok(payload) => {
                    let ts_init=payload.ts_init;
                    let custom=payload.into_custom_data();
                    let response=DataResponse::Data(CustomDataResponse::new(request.request_id, request.client_id, Some(*BULLET_VENUE), request.data_type, custom, None, None, ts_init, request.params));
                    if let Err(error)=sender.send(DataEvent::Response(response)) { log::error!("Bullet funding estimate delivery failed: {error}"); }
                }
                Err(error) => log::error!("Bullet funding estimate request failed for {id}: {error:#}"),
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::{
        live::runner::replace_data_event_sender, messages::data::SubscriptionError,
    };

    #[tokio::test]
    async fn client_stop_and_disconnect_drain_the_book_before_reconnecting() {
        use axum::{
            Router,
            extract::{State, WebSocketUpgrade, ws::Message},
            response::IntoResponse,
            routing::get,
        };
        use std::sync::atomic::AtomicUsize;
        async fn socket(
            State(connections): State<Arc<AtomicUsize>>,
            upgrade: WebSocketUpgrade,
        ) -> impl IntoResponse {
            let generation = connections.fetch_add(1, Ordering::AcqRel);
            upgrade.on_upgrade(move |mut socket| async move {
                while let Some(Ok(Message::Text(text))) = socket.recv().await {
                    let request: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if request["method"] != "SUBSCRIBE" { continue; }
                    let ack = serde_json::json!({"e":"subscribe","id":request["id"],"E":1,"result":"success"});
                    socket.send(Message::Text(ack.to_string().into())).await.unwrap();
                    let mut frame: serde_json::Value = serde_json::from_slice(include_bytes!("../test_data/ws_depth_snapshot.json")).unwrap();
                    let u = [42_550_033_u64, 2_744_357, 1][generation];
                    frame["U"] = serde_json::json!(u);
                    frame["u"] = serde_json::json!(u);
                    if socket.send(Message::Text(frame.to_string().into())).await.is_err() { break; }
                }
            })
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/ws", get(socket))
                    .route(
                        "/fapi/v1/exchangeInfo",
                        get(|| async {
                            include_str!("../test_data/http_exchange_info_mainnet.json")
                        }),
                    )
                    .with_state(Arc::new(AtomicUsize::new(0))),
            )
            .await
            .unwrap();
        });
        let (sender, mut events) = mpsc::unbounded_channel();
        replace_data_event_sender(sender);
        let id = crate::common::instrument_id_from_market("BTC-USD").unwrap();
        let mut client = BulletDataClient::new(
            ClientId::from("BULLET"),
            BulletDataClientConfig {
                base_url_http: Some(format!("http://{address}")),
                base_url_ws: Some(format!("ws://{address}/ws")),
                instrument_provider: crate::config::BulletInstrumentProviderConfig {
                    load_all: false,
                    load_ids: Some(vec![id]),
                },
                ..Default::default()
            },
        )
        .unwrap();
        let mut book = nautilus_model::orderbook::OrderBook::new(id, BookType::L2_MBP);
        for (generation, expected_u) in [42_550_033, 2_744_357, 1].into_iter().enumerate() {
            client.connect().await.unwrap();
            // stop() is synchronous: connect() must drain that generation before replacement.
            if generation > 0 {
                let Some(DataEvent::Data(Data::BookDeltas(cut))) = events.recv().await else {
                    panic!("old-generation invalidation must precede new metadata")
                };
                assert_eq!(cut.sequence, 0);
                book.apply_deltas(&cut).unwrap();
                assert_eq!(book.sequence, 0);
            }
            client
                .subscribe_book_deltas(SubscribeBookDeltas::new(
                    vec![id],
                    BookType::L2_MBP,
                    Some(client.client_id),
                    None,
                    nautilus_core::UUID4::new(),
                    1.into(),
                    std::num::NonZeroUsize::new(20),
                    true,
                    None,
                    None,
                ))
                .unwrap();
            let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(DataEvent::Data(Data::BookDeltas(frame))) = events.recv().await {
                        break frame;
                    }
                }
            })
            .await
            .unwrap();
            book.apply_deltas(&snapshot).unwrap();
            assert_eq!(book.sequence, expected_u);
            assert_eq!(book.bids(None).count(), 2);
            if generation == 0 {
                client.stop().unwrap();
            } else {
                client.disconnect().await.unwrap();
            }
        }
        let Some(DataEvent::Data(Data::BookDeltas(cut))) = events.recv().await else {
            panic!("final invalidation")
        };
        book.apply_deltas(&cut).unwrap();
        assert_eq!(book.sequence, 0);
        assert!(events.try_recv().is_err());
        server.abort();
    }

    #[tokio::test]
    async fn batch_release_reports_queue_admission_and_continues_after_invalid_member() {
        let (events, _rx) = mpsc::unbounded_channel();
        replace_data_event_sender(events);
        let mut client =
            BulletDataClient::new(ClientId::from("BULLET"), BulletDataClientConfig::default())
                .unwrap();
        let first = crate::common::instrument_id_from_market("BTC-USD").unwrap();
        let second = crate::common::instrument_id_from_market("ETH-USD").unwrap();
        let invalid = InstrumentId::from("BTCUSDT.BINANCE");
        let (tx, mut queue) = mpsc::channel(2);
        client.command_tx = Some(tx);
        let error = client
            .unsubscribe_topic(BulletDataKind::Trades, &[first, invalid, second])
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<SubscriptionError>()
                .unwrap()
                .instrument_ids_accepted,
            vec![first, second]
        );
        for id in [first, second] {
            let BulletCommand::Unsubscribe(topic) = queue.try_recv().unwrap() else {
                panic!("unsubscribe")
            };
            assert_eq!(topic.instrument_id().unwrap(), id);
        }
        client
            .unsubscribe_topic(BulletDataKind::Trades, &[first])
            .unwrap();
        let error = client
            .unsubscribe_topic(BulletDataKind::Trades, &[second, first])
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<SubscriptionError>()
                .unwrap()
                .instrument_ids_accepted,
            vec![second]
        );
        drop(queue);
        let error = client
            .unsubscribe_topic(BulletDataKind::Trades, &[first, second])
            .unwrap_err();
        assert!(
            error
                .downcast_ref::<SubscriptionError>()
                .unwrap()
                .instrument_ids_accepted
                .is_empty()
        );
    }
}
