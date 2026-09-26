// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Live public market-data client for Extended.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{
    cache::InstrumentLookupError,
    clients::DataClient,
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        data::{
            SubscribeBookDeltas, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
            SubscribeMarkPrices, SubscribeTrades, UnsubscribeBookDeltas, UnsubscribeFundingRates,
            UnsubscribeIndexPrices, UnsubscribeInstrument, UnsubscribeMarkPrices,
            UnsubscribeTrades,
        },
        DataEvent,
    },
};
use nautilus_core::{
    time::{get_atomic_clock_realtime, AtomicTime},
    AtomicMap,
};
use nautilus_live::{task::TaskGroup, SocketControlFactory};
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    common::EXTENDED_VENUE,
    config::ExtendedDataClientConfig,
    http::ExtendedHttpClient,
    websocket::{
        messages::{ExtendedCommand, ExtendedDataKind, ExtendedTopic},
        ExtendedWebSocketSession,
    },
};

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct ExtendedDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: ExtendedDataClientConfig,
    http_client: ExtendedHttpClient,
    socket_factory: SocketControlFactory,
    data_sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    instruments_by_market: Arc<AtomicMap<Ustr, InstrumentAny>>,
    command_tx: Option<mpsc::Sender<ExtendedCommand>>,
    tasks: TaskGroup,
    cancellation: CancellationToken,
    connected: AtomicBool,
}

impl ExtendedDataClient {
    pub fn new(client_id: ClientId, config: ExtendedDataClientConfig) -> anyhow::Result<Self> {
        let tasks = TaskGroup::new();
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            http_client: ExtendedHttpClient::new(&config)?,
            socket_factory: SocketControlFactory::new(client_id, Some(*EXTENDED_VENUE)),
            data_sender: get_data_event_sender(),
            instruments: Arc::new(AtomicMap::new()),
            instruments_by_market: Arc::new(AtomicMap::new()),
            command_tx: None,
            cancellation: tasks.cancellation_token(),
            tasks,
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

    fn spawn_instrument_refresh(&self) -> anyhow::Result<()> {
        let minutes = self.config.update_instruments_interval_mins;
        if minutes == 0 {
            return Ok(());
        }
        let interval = Duration::from_secs(minutes.saturating_mul(60));
        let http = self.http_client.clone();
        let provider = self.config.instrument_provider.clone();
        let by_id = Arc::clone(&self.instruments);
        let by_market = Arc::clone(&self.instruments_by_market);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let cancellation = self.cancellation.clone();

        self.tasks
            .spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => break,
                        () = tokio::time::sleep(interval) => {
                            match http.request_instruments(&provider, clock.get_time_ns()).await {
                                Ok(instruments) => {
                                    by_id.store(instruments.iter().map(|instrument| {
                                        (instrument.id(), instrument.clone())
                                    }).collect());
                                    by_market.store(instruments.iter().map(|instrument| {
                                        (instrument.raw_symbol().inner(), instrument.clone())
                                    }).collect());
                                    for instrument in instruments {
                                        if let Err(error) = sender.send(DataEvent::Instrument(instrument)) {
                                            log::warn!("Failed to publish refreshed Extended instrument: {error}");
                                        }
                                    }
                                }
                                Err(error) => log::warn!("Failed to refresh Extended instruments: {error:#}"),
                            }
                        }
                    }
                }
            })
            .context("failed to spawn Extended instrument refresh")
    }

    fn send_command(&self, command: ExtendedCommand) -> anyhow::Result<()> {
        let sender = self
            .command_tx
            .as_ref()
            .context("Extended WebSocket session is not connected")?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                anyhow::anyhow!("Extended subscription command queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("Extended subscription command queue is closed")
            }
        })
    }

    fn subscribe_topic(
        &self,
        kind: ExtendedDataKind,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        self.require_instrument(instrument_id)?;
        self.send_command(ExtendedCommand::Subscribe(ExtendedTopic::new(
            kind,
            instrument_id,
        )?))
    }

    fn unsubscribe_topic(
        &self,
        kind: ExtendedDataKind,
        instrument_id: InstrumentId,
    ) -> anyhow::Result<()> {
        self.send_command(ExtendedCommand::Unsubscribe(ExtendedTopic::new(
            kind,
            instrument_id,
        )?))
    }

    fn require_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id).into())
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for ExtendedDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*EXTENDED_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting Extended data client: client_id={}, environment={:?}",
            self.client_id,
            self.config.environment,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
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
                .context("failed to start Extended task generation")?;
            self.cancellation = self.tasks.cancellation_token();
        }

        let instruments = self.bootstrap_instruments().await?;
        for instrument in instruments {
            self.data_sender.send(DataEvent::Instrument(instrument))?;
        }

        let socket_control = self.socket_factory.control("public-data");
        let (command_tx, session) = ExtendedWebSocketSession::connect(
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
            .context("failed to spawn Extended WebSocket session")?;
        self.command_tx = Some(command_tx);
        self.spawn_instrument_refresh()?;
        self.connected.store(true, Ordering::Release);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.tasks.begin_shutdown();
        self.command_tx = None;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), DISCONNECT_TIMEOUT)
            .await
            .context("failed to stop Extended tasks")?;
        self.instruments.store(AHashMap::new());
        self.instruments_by_market.store(AHashMap::new());
        self.connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        let instrument = self.require_instrument(subscription.instrument_id)?;
        self.data_sender.send(DataEvent::Instrument(instrument))?;
        Ok(())
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
            "Extended only supports L2_MBP book deltas",
        );
        anyhow::ensure!(
            subscription.depth.is_none(),
            "Extended full L2 does not support a finite depth selector",
        );
        self.subscribe_topic(ExtendedDataKind::OrderBook, subscription.instrument_id)
    }

    fn unsubscribe_book_deltas(
        &mut self,
        unsubscription: &UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(ExtendedDataKind::OrderBook, unsubscription.instrument_id)
    }

    fn subscribe_trades(&mut self, subscription: SubscribeTrades) -> anyhow::Result<()> {
        self.subscribe_topic(ExtendedDataKind::Trades, subscription.instrument_id)
    }

    fn unsubscribe_trades(&mut self, unsubscription: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.unsubscribe_topic(ExtendedDataKind::Trades, unsubscription.instrument_id)
    }

    fn subscribe_mark_prices(&mut self, subscription: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_topic(ExtendedDataKind::MarkPrice, subscription.instrument_id)
    }

    fn unsubscribe_mark_prices(
        &mut self,
        unsubscription: &UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(ExtendedDataKind::MarkPrice, unsubscription.instrument_id)
    }

    fn subscribe_index_prices(&mut self, subscription: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_topic(ExtendedDataKind::IndexPrice, subscription.instrument_id)
    }

    fn unsubscribe_index_prices(
        &mut self,
        unsubscription: &UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(ExtendedDataKind::IndexPrice, unsubscription.instrument_id)
    }

    fn subscribe_funding_rates(
        &mut self,
        subscription: SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.subscribe_topic(ExtendedDataKind::FundingRate, subscription.instrument_id)
    }

    fn unsubscribe_funding_rates(
        &mut self,
        unsubscription: &UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.unsubscribe_topic(ExtendedDataKind::FundingRate, unsubscription.instrument_id)
    }
}
