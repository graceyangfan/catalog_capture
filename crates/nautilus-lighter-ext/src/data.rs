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

//! Live public market-data client for LighterBinary.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::messages::data::{SubscribeCustomData, UnsubscribeCustomData};
use nautilus_common::{
    cache::InstrumentLookupError,
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            InstrumentResponse, InstrumentsResponse, RequestInstrument, RequestInstruments,
            SubscribeBookDeltas, SubscribeIndexPrices, SubscribeInstrument, SubscribeTrades,
            UnsubscribeBookDeltas, UnsubscribeIndexPrices, UnsubscribeInstrument,
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

use crate::{
    common::{market_id, open_interest_market},
    config::LighterBinaryDataClientConfig,
    http::LighterBinaryHttpClient,
    models::MarketContext,
    websocket::{BinarySession, Command, Kind, Topic},
};
fn lighter_venue() -> Venue {
    Venue::new("LIGHTER")
}

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct LighterBinaryDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: LighterBinaryDataClientConfig,
    http_client: LighterBinaryHttpClient,
    socket_factory: SocketControlFactory,
    data_sender: EventSender<DataEvent>,
    contexts: Arc<AtomicMap<u64, MarketContext>>,
    command_tx: Option<mpsc::Sender<Command>>,
    tasks: TaskGroup,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
    discovery_inflight: Arc<AtomicBool>,
}

struct ConnectionGuard(Arc<AtomicBool>);
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl LighterBinaryDataClient {
    pub fn new(client_id: ClientId, config: LighterBinaryDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let tasks = TaskGroup::new();
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            http_client: LighterBinaryHttpClient::new(&config)?,
            socket_factory: SocketControlFactory::new(client_id, Some(lighter_venue())),
            data_sender: get_data_event_sender(),
            contexts: Arc::new(AtomicMap::new()),
            command_tx: None,
            cancellation: tasks.cancellation_token(),
            tasks,
            connected: Arc::new(AtomicBool::new(false)),
            discovery_inflight: Arc::new(AtomicBool::new(false)),
            config,
        })
    }

    fn send_command(&self, command: Command) -> anyhow::Result<()> {
        let sender = self
            .command_tx
            .as_ref()
            .context("LighterBinary WebSocket session is not connected")?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                anyhow::anyhow!("LighterBinary subscription command queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("LighterBinary subscription command queue is closed")
            }
        })
    }

    fn subscribe_topic(&self, kind: Kind, instrument_ids: &[InstrumentId]) -> anyhow::Result<()> {
        execute_instrument_subscriptions(instrument_ids, |id| {
            self.require_instrument(id)?;
            self.send_command(Command::Subscribe(Topic {
                kind,
                market: market_id(id)?,
            }))
        })
    }

    fn unsubscribe_topic(&self, kind: Kind, instrument_ids: &[InstrumentId]) -> anyhow::Result<()> {
        execute_instrument_subscriptions(instrument_ids, |id| {
            self.send_command(Command::Unsubscribe(Topic {
                kind,
                market: market_id(id)?,
            }))
        })
    }

    fn require_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.contexts
            .get_cloned(&market_id(instrument_id)?)
            .map(|context| context.instrument)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id).into())
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for LighterBinaryDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(lighter_venue())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting Lighter binary data client: client_id={}",
            self.client_id,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.contexts = Arc::new(AtomicMap::new());
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
                .start_generation()
                .context("failed to start LighterBinary task generation")?;
            self.cancellation = self.tasks.cancellation_token();
        }

        let contexts = if self.config.market_ids.is_empty() {
            Vec::new()
        } else {
            self.http_client
                .load_markets(&self.config.market_ids, self.clock.get_time_ns())
                .await?
        };
        self.contexts = Arc::new(AtomicMap::from(
            contexts
                .into_iter()
                .map(|c| (c.market_id, c))
                .collect::<AHashMap<_, _>>(),
        ));
        self.discovery_inflight = Arc::new(AtomicBool::new(false));
        for context in self.contexts.load().values() {
            self.data_sender
                .send(DataEvent::Data(Data::Instrument(Box::new(
                    context.instrument.clone(),
                ))))?;
        }

        let socket_control = self.socket_factory.control("public-data");
        let (command_tx, session) = BinarySession::connect(
            &self.config,
            Arc::clone(&self.contexts),
            self.data_sender.clone(),
            self.clock,
            self.cancellation.clone(),
            socket_control,
        )
        .await?;
        let connected = Arc::clone(&self.connected);
        self.tasks
            .spawn(async move {
                let _guard = ConnectionGuard(connected);
                session.run().await;
            })
            .context("failed to spawn LighterBinary WebSocket session")?;
        self.command_tx = Some(command_tx);

        self.connected.store(true, Ordering::Release);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
            .await
            .context("failed to stop LighterBinary tasks")?;
        self.contexts = Arc::new(AtomicMap::new());
        self.connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.client_id.is_none_or(|id| id == self.client_id)
                && request.venue.is_none_or(|v| v == lighter_venue()),
            "instrument request targets another client/venue"
        );
        if request
            .params
            .as_ref()
            .is_none_or(|p| !p.contains_key("updown_tags"))
        {
            let data = self
                .contexts
                .load()
                .values()
                .map(|c| c.instrument.clone())
                .collect();
            self.data_sender
                .send(DataEvent::Response(DataResponse::Instruments(
                    InstrumentsResponse::new(
                        request.request_id,
                        self.client_id,
                        lighter_venue(),
                        data,
                        None,
                        None,
                        self.clock.get_time_ns(),
                        request.params,
                    ),
                )))?;
            return Ok(());
        }
        let params = request
            .params
            .clone()
            .context("rolling Lighter instrument request requires params")?;
        let tags: Vec<String> = serde_json::from_value(
            params
                .get("updown_tags")
                .cloned()
                .context("updown_tags missing")?,
        )?;
        let prefetch = params
            .get("prefetch_secs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(30);
        for tag in &tags {
            crate::common::validate_updown_tag(tag)?;
        }
        anyhow::ensure!(
            !tags.is_empty() && tags.len() <= 2 && prefetch <= 300,
            "invalid Lighter rolling instrument request"
        );
        anyhow::ensure!(
            !self.discovery_inflight.swap(true, Ordering::AcqRel),
            "Lighter discovery already in flight"
        );
        let inflight = Arc::clone(&self.discovery_inflight);
        let contexts = Arc::clone(&self.contexts);
        let http = self.http_client.clone();
        let clock = self.clock;
        let sender = self.data_sender.clone();
        let client_id = self.client_id;
        let cancel = self.cancellation.clone();
        let deadline = Duration::from_secs(self.config.http_timeout_secs.saturating_mul(3));
        let result = self.tasks.spawn(async move {
            let _guard = ConnectionGuard(inflight);
            let known = contexts.load().iter().map(|(id, c)| (*id, c.clone())).collect();
            let discovery = http.request_universe(&tags, prefetch, &known, clock.get_time_ns());
            let result = tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                result = tokio::time::timeout(deadline, discovery) => {
                    result.map_err(|_| anyhow::anyhow!("Lighter discovery overall deadline elapsed"))
                        .and_then(|result| result)
                }
            };
            if cancel.is_cancelled() {
                return;
            }
            let mut response_params = request.params.clone().unwrap_or_default();
            let data = match result {
                Ok(updates) => {
                    let updates: Vec<_> = updates.into_iter().filter(|context| {
                        context.instrument.expiration_ns().is_some_and(|end| end > clock.get_time_ns())
                    }).collect();
                    let data = updates.iter().map(|context| context.instrument.clone()).collect();
                    contexts.rcu(|map| {
                        for context in &updates {
                            map.insert(context.market_id, context.clone());
                        }
                    });
                    data
                }
                Err(error) => {
                    log::warn!("Lighter universe discovery failed: {error:#}");
                    let mut params = indexmap::IndexMap::clone(&response_params);
                    params.insert("lighter_error".into(), serde_json::json!(format!("{error:#}")));
                    response_params = nautilus_core::Params::new(params);
                    Vec::new()
                }
            };
            let response = InstrumentsResponse::new(
                request.request_id, client_id, lighter_venue(), data, None, None,
                clock.get_time_ns(), Some(response_params),
            );
            if let Err(error) = sender.send(DataEvent::Response(DataResponse::Instruments(response))) {
                log::error!("Lighter discovery response failed: {error}");
            }
        });
        if result.is_err() {
            self.discovery_inflight.store(false, Ordering::Release);
        }
        result.map_err(Into::into)
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.client_id.is_none_or(|id| id == self.client_id),
            "instrument request targets another client"
        );
        let instrument_id = request.single_instrument_id()?;
        let instrument = self.require_instrument(instrument_id)?;
        self.data_sender
            .send(DataEvent::Response(DataResponse::Instrument(Box::new(
                InstrumentResponse::new(
                    request.request_id,
                    self.client_id,
                    instrument_id,
                    instrument,
                    nautilus_core::datetime::datetime_to_unix_nanos(request.start),
                    nautilus_core::datetime::datetime_to_unix_nanos(request.end),
                    self.clock.get_time_ns(),
                    request.params,
                ),
            ))))?;
        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            let instrument = self.require_instrument(id)?;
            let market = market_id(id)?;
            self.data_sender
                .send(DataEvent::Data(Data::Instrument(Box::new(instrument))))?;
            self.send_command(Command::Definition {
                market,
                subscribe: true,
            })
        })
    }

    fn unsubscribe_instrument(
        &mut self,
        unsubscription: &UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&unsubscription.instrument_ids, |id| {
            self.send_command(Command::Definition {
                market: market_id(id)?,
                subscribe: false,
            })
        })
    }

    fn subscribe_book_deltas(&mut self, subscription: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            subscription.book_type == BookType::L2_MBP,
            "LighterBinary only supports L2_MBP book deltas",
        );
        anyhow::ensure!(
            subscription.depth.is_none(),
            "LighterBinary full L2 does not support a finite depth selector",
        );
        self.subscribe_topic(Kind::Book, &subscription.instrument_ids)
    }

    fn unsubscribe_book_deltas(
        &mut self,
        unsubscription: &UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(Kind::Book, &unsubscription.instrument_ids)
    }

    fn subscribe_trades(&mut self, subscription: SubscribeTrades) -> anyhow::Result<()> {
        self.subscribe_topic(Kind::Trades, &subscription.instrument_ids)
    }

    fn unsubscribe_trades(&mut self, unsubscription: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.unsubscribe_topic(Kind::Trades, &unsubscription.instrument_ids)
    }

    fn subscribe_index_prices(&mut self, subscription: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_topic(Kind::Oracle, &subscription.instrument_ids)
    }

    fn unsubscribe_index_prices(
        &mut self,
        unsubscription: &UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(Kind::Oracle, &unsubscription.instrument_ids)
    }

    fn subscribe(&mut self, cmd: SubscribeCustomData) -> anyhow::Result<()> {
        let market = open_interest_market(&cmd.data_type)?;
        let context = self
            .contexts
            .get_cloned(&market)
            .context("OI market is not configured")?;
        self.subscribe_topic(Kind::OpenInterest, &[context.instrument.id()])
    }
    fn unsubscribe(&mut self, cmd: &UnsubscribeCustomData) -> anyhow::Result<()> {
        let market = open_interest_market(&cmd.data_type)?;
        self.send_command(Command::Unsubscribe(Topic {
            market,
            kind: Kind::OpenInterest,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::{
        live::runner::replace_data_event_sender, messages::data::SubscriptionError,
    };

    #[tokio::test]
    async fn batch_release_reports_queue_admission_and_continues_after_invalid_member() {
        let (events, _rx) = mpsc::unbounded_channel();
        replace_data_event_sender(events);
        let mut client = LighterBinaryDataClient::new(
            ClientId::from("LIGHTER"),
            LighterBinaryDataClientConfig::default(),
        )
        .unwrap();
        let first = crate::common::instrument_id(1).unwrap();
        let second = crate::common::instrument_id(2).unwrap();
        let invalid = InstrumentId::from("BTCUSDT.BINANCE");
        let (tx, mut queue) = mpsc::channel(2);
        client.command_tx = Some(tx);
        let error = client
            .unsubscribe_topic(Kind::Trades, &[first, invalid, second])
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<SubscriptionError>()
                .unwrap()
                .instrument_ids_accepted,
            vec![first, second]
        );
        for market in [1, 2] {
            assert!(
                matches!(queue.try_recv().unwrap(), Command::Unsubscribe(topic) if topic.market == market)
            );
        }
        client.unsubscribe_topic(Kind::Trades, &[first]).unwrap();
        let error = client
            .unsubscribe_topic(Kind::Trades, &[second, first])
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
            .unsubscribe_topic(Kind::Trades, &[first, second])
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
