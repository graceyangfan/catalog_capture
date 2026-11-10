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

use std::{collections::BTreeMap, time::Duration};

use anyhow::Context;
use nautilus_common::{
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent, DataResponse,
        data::{
            InstrumentResponse, InstrumentsResponse, RequestInstrument, RequestInstruments,
            SubscribeBookDepth, SubscribeCustomData, SubscribeInstrument, UnsubscribeBookDepth,
            UnsubscribeCustomData, UnsubscribeInstrument,
        },
    },
};
use nautilus_live::{SocketControlFactory, task::TaskGroup};
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::InstrumentAny,
};
use tokio::sync::watch;

use crate::{
    PROPHET, ProphetDataClientConfig, last_price_market, market_id,
    protocol::{Market, ProphetHttp, now},
    websocket::{Interest, MarketStream},
};

#[derive(Debug)]
pub struct ProphetDataClient {
    id: ClientId,
    config: ProphetDataClientConfig,
    http: ProphetHttp,
    sender: EventSender<DataEvent>,
    tasks: TaskGroup,
    markets: BTreeMap<u64, Market>,
    interests: BTreeMap<u64, watch::Sender<Interest>>,
}

impl ProphetDataClient {
    pub fn new(id: ClientId, config: ProphetDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            id,
            http: ProphetHttp::new(&config)?,
            config,
            sender: get_data_event_sender(),
            tasks: TaskGroup::new(),
            markets: BTreeMap::new(),
            interests: BTreeMap::new(),
        })
    }

    fn definition(&self, id: InstrumentId) -> anyhow::Result<InstrumentAny> {
        Ok(self
            .markets
            .get(&market_id(id)?)
            .with_context(|| format!("Prophet instrument {id} is not configured"))?
            .instrument
            .clone())
    }

    fn set_interest(&self, id: u64, depth: bool, enabled: bool) -> anyhow::Result<()> {
        let sender = self.interests.get(&id).with_context(|| {
            format!("Prophet market {id} is not configured or the client is stopped")
        })?;
        anyhow::ensure!(
            sender.receiver_count() > 0,
            "Prophet market {id} task has stopped"
        );
        sender.send_if_modified(|interest| {
            let field = if depth {
                &mut interest.depth
            } else {
                &mut interest.last_price
            };
            if field.is_some() == enabled {
                return false;
            }
            *field = enabled.then(now);
            true
        });
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for ProphetDataClient {
    fn client_id(&self) -> ClientId {
        self.id
    }

    fn venue(&self) -> Option<Venue> {
        Some(Venue::new(PROPHET))
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        for sender in self.interests.values() {
            sender.send_replace(Interest::default());
        }
        self.tasks.begin_shutdown();
        self.interests.clear();
        self.markets.clear();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        !self.interests.is_empty()
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
        let markets = tokio::time::timeout(
            Duration::from_secs(25),
            self.http.load_markets(&self.config.market_ids),
        )
        .await
        .context("Prophet instrument discovery timed out")??;
        let socket_factory = SocketControlFactory::new(self.id, self.venue());
        for market in markets.values() {
            self.sender.send(DataEvent::Data(Data::Instrument(Box::new(
                market.instrument.clone(),
            ))))?;
        }
        let mut interests = BTreeMap::new();
        for (&id, market) in &markets {
            let (tx, rx) = watch::channel(Interest::default());
            let stream = MarketStream {
                market: market.clone(),
                http: self.http.clone(),
                url: self.config.ws_url.clone(),
                sender: self.sender.clone(),
                control: socket_factory.control(format!("public-{id}")),
                interests: rx,
                cancel: self.tasks.cancellation_token(),
            };
            self.tasks.spawn(stream.run())?;
            interests.insert(id, tx);
        }
        self.markets = markets;
        self.interests = interests;
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await?;
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.client_id.is_none_or(|id| id == self.id),
            "request targets another client"
        );
        let id = request.single_instrument_id()?;
        let response = InstrumentResponse::new(
            request.request_id,
            self.id,
            id,
            self.definition(id)?,
            nautilus_core::datetime::datetime_to_unix_nanos(request.start),
            nautilus_core::datetime::datetime_to_unix_nanos(request.end),
            now(),
            request.params,
        );
        self.sender
            .send(DataEvent::Response(DataResponse::Instrument(Box::new(
                response,
            ))))?;
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.client_id.is_none_or(|id| id == self.id)
                && request
                    .venue
                    .is_none_or(|venue| Some(venue) == self.venue()),
            "request targets another client or venue"
        );
        let response = InstrumentsResponse::new(
            request.request_id,
            self.id,
            Venue::new(PROPHET),
            self.markets
                .values()
                .map(|market| market.instrument.clone())
                .collect(),
            None,
            None,
            now(),
            request.params,
        );
        self.sender
            .send(DataEvent::Response(DataResponse::Instruments(response)))?;
        Ok(())
    }

    fn subscribe_instrument(&mut self, request: SubscribeInstrument) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&request.instrument_ids, |id| {
            self.sender.send(DataEvent::Data(Data::Instrument(Box::new(
                self.definition(id)?,
            ))))?;
            Ok(())
        })
    }

    fn unsubscribe_instrument(&mut self, _request: &UnsubscribeInstrument) -> anyhow::Result<()> {
        Ok(())
    }

    fn subscribe_book_depth(&mut self, request: SubscribeBookDepth) -> anyhow::Result<()> {
        anyhow::ensure!(
            request.book_type == BookType::L2_MBP && request.depth.is_none(),
            "Prophet supports full L2_MBP snapshots only"
        );
        execute_instrument_subscriptions(&request.instrument_ids, |id| {
            self.set_interest(market_id(id)?, true, true)
        })
    }

    fn unsubscribe_book_depth(&mut self, request: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&request.instrument_ids, |id| {
            self.set_interest(market_id(id)?, true, false)
        })
    }

    fn subscribe(&mut self, request: SubscribeCustomData) -> anyhow::Result<()> {
        self.set_interest(last_price_market(&request.data_type)?, false, true)
    }

    fn unsubscribe(&mut self, request: &UnsubscribeCustomData) -> anyhow::Result<()> {
        self.set_interest(last_price_market(&request.data_type)?, false, false)
    }
}
