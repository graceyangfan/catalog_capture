// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

use anyhow::{Context, ensure};
use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, execute_instrument_subscriptions},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeInstrument, UnsubscribeBookDeltas, UnsubscribeInstrument,
        },
    },
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{SocketControlFactory, task::TaskGroup};
use nautilus_model::{
    data::{Data, OrderBookDelta, OrderBookDeltas},
    enums::{BookType, RecordFlag},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc;

use crate::{
    DEFAULT_DEPTH,
    instruments::{BITFINEX, load_instruments},
    validate_depth,
    websocket::{Command, Session},
};

#[derive(Debug, Clone)]
pub struct BitfinexDataClientConfig {
    pub instruments: Vec<InstrumentId>,
    pub http_url: String,
    pub ws_url: String,
}

impl Default for BitfinexDataClientConfig {
    fn default() -> Self {
        Self {
            instruments: Vec::new(),
            http_url: "https://api-pub.bitfinex.com".to_owned(),
            ws_url: "wss://api-pub.bitfinex.com/ws/2".to_owned(),
        }
    }
}

impl ClientConfig for BitfinexDataClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Default)]
pub struct BitfinexDataClientFactory;

impl DataClientFactory for BitfinexDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<BitfinexDataClientConfig>()
            .context("invalid Bitfinex data client config")?;
        let client_id = ClientId::from(name);
        Ok(Box::new(BitfinexDataClient {
            config: config.clone(),
            client_id,
            sender: get_data_event_sender(),
            socket: SocketControlFactory::new(client_id, Some(Venue::from(BITFINEX))),
            tasks: TaskGroup::new(),
            commands: None,
            instruments: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
        }))
    }

    fn name(&self) -> &'static str {
        BITFINEX
    }

    fn config_type(&self) -> &'static str {
        "BitfinexDataClientConfig"
    }
}

#[derive(Debug)]
struct BitfinexDataClient {
    config: BitfinexDataClientConfig,
    client_id: ClientId,
    sender: EventSender<DataEvent>,
    socket: SocketControlFactory,
    tasks: TaskGroup,
    commands: Option<mpsc::Sender<Command>>,
    instruments: BTreeMap<InstrumentId, InstrumentAny>,
    subscriptions: BTreeMap<InstrumentId, usize>,
}

impl BitfinexDataClient {
    fn command(&self, command: Command) -> anyhow::Result<()> {
        self.commands
            .as_ref()
            .context("Bitfinex data client is disconnected")?
            .try_send(command)
            .context("Bitfinex subscription queue is unavailable")?;
        Ok(())
    }

    fn instrument(&self, id: InstrumentId) -> anyhow::Result<&InstrumentAny> {
        self.instruments
            .get(&id)
            .with_context(|| format!("Bitfinex instrument {id} was not loaded"))
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for BitfinexDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }
    fn venue(&self) -> Option<Venue> {
        Some(Venue::from(BITFINEX))
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.commands = None;
        self.subscriptions.clear();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }
    fn is_connected(&self) -> bool {
        self.commands.as_ref().is_some_and(|tx| !tx.is_closed())
    }
    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        if !self.tasks.is_open() {
            self.tasks.start_generation()?;
        }
        ensure!(
            !self.config.instruments.is_empty(),
            "Bitfinex requires selected instruments"
        );
        let instruments = load_instruments(
            &self.config.http_url,
            &self.config.instruments,
            get_atomic_clock_realtime().get_time_ns(),
        )
        .await?;
        self.instruments = instruments
            .into_iter()
            .map(|instrument| (instrument.id(), instrument))
            .collect();
        let (tx, rx) = mpsc::channel(128);
        let session = Session::connect(
            self.config.ws_url.clone(),
            self.sender.clone(),
            self.tasks.cancellation_token(),
            self.socket.control("public-data"),
            rx,
        )
        .await?;
        for instrument in self.instruments.values() {
            self.sender.send(DataEvent::Data(Data::Instrument(Box::new(
                instrument.clone(),
            ))))?;
        }
        self.tasks.spawn(session.run())?;
        self.commands = Some(tx);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await?;
        self.instruments.clear();
        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            let instrument = self.instrument(id)?.clone();
            self.sender
                .send(DataEvent::Data(Data::Instrument(Box::new(instrument))))?;
            Ok(())
        })
    }

    fn unsubscribe_instrument(&mut self, _: &UnsubscribeInstrument) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, subscription: SubscribeBookDeltas) -> anyhow::Result<()> {
        ensure!(
            subscription.book_type == BookType::L3_MBO,
            "Bitfinex R0 requires L3_MBO"
        );
        let depth = subscription
            .depth
            .map_or(DEFAULT_DEPTH, |depth| depth.get());
        validate_depth(depth)?;
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            self.instrument(id)?;
            if let Some(existing) = self.subscriptions.get(&id) {
                ensure!(*existing == depth, "conflicting Bitfinex R0 depth for {id}");
                return Ok(());
            }
            self.command(Command::Subscribe(id, depth))?;
            self.subscriptions.insert(id, depth);
            Ok(())
        })
    }

    fn unsubscribe_book_deltas(
        &mut self,
        subscription: &UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&subscription.instrument_ids, |id| {
            if self.subscriptions.contains_key(&id) {
                self.command(Command::Unsubscribe(id))?;
                self.subscriptions.remove(&id);
            }
            Ok(())
        })
    }
}

pub(crate) fn book_invalidation(
    id: InstrumentId,
    sequence: u64,
    now: UnixNanos,
) -> OrderBookDeltas {
    let mut clear = OrderBookDelta::clear(id, sequence, now, now);
    // A venue snapshot always has F_SNAPSHOT, even if it has zero orders.
    clear.flags = RecordFlag::F_LAST as u8;
    OrderBookDeltas::new(id, vec![clear])
}
