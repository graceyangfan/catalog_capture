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

//! Public market data for the Binance Pick (Prophet) Scores campaign.
#![deny(unsafe_code)]

mod data;
mod protocol;
mod websocket;

use std::{collections::BTreeSet, sync::Arc};

use anyhow::Context;
use nautilus_common::{
    clients::DataClient,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, DataType},
    identifiers::{ClientId, InstrumentId, Venue},
    types::Price,
};
use nautilus_serialization::arrow_custom_data;

pub use data::ProphetDataClient;

pub const PROPHET: &str = "PROPHET";
const LAST_PRICE_TYPE: &str = "ProphetLastTradePrice";
const MAX_MARKETS: usize = 8;

pub fn instrument_id(market_id: u64) -> anyhow::Result<InstrumentId> {
    anyhow::ensure!(market_id > 0, "Prophet market ID must be nonzero");
    Ok(InstrumentId::from(
        format!("{market_id}-0.{PROPHET}").as_str(),
    ))
}

pub fn market_id(id: InstrumentId) -> anyhow::Result<u64> {
    anyhow::ensure!(
        id.venue == Venue::new(PROPHET),
        "expected a Prophet instrument"
    );
    let market = id
        .symbol
        .as_str()
        .strip_suffix("-0")
        .context("expected Prophet outcome index 0")?;
    parse_market_id(market)
}

fn parse_market_id(id: &str) -> anyhow::Result<u64> {
    let value: u64 = id.parse().context("expected a decimal Prophet market ID")?;
    anyhow::ensure!(
        value > 0 && id == value.to_string(),
        "invalid Prophet market ID {id}"
    );
    Ok(value)
}

pub fn last_price_data_type(market_id: u64) -> DataType {
    DataType::new(LAST_PRICE_TYPE, None, Some(market_id.to_string()))
}

pub fn last_price_market(data_type: &DataType) -> anyhow::Result<u64> {
    anyhow::ensure!(
        data_type.type_name() == LAST_PRICE_TYPE && data_type.metadata().is_none(),
        "expected ProphetLastTradePrice with a market ID identifier and no metadata"
    );
    parse_market_id(
        data_type
            .identifier()
            .context("missing Prophet market ID identifier")?,
    )
}

/// Latest price observation. This feed can repeat a price and supplies no fill ID or size.
#[arrow_custom_data]
#[custom_data]
pub struct ProphetLastTradePrice {
    pub instrument_id: InstrumentId,
    pub market_id: u64,
    pub price: Price,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl ProphetLastTradePrice {
    pub fn into_custom_data(self) -> CustomData {
        let data_type = last_price_data_type(self.market_id);
        CustomData::new(Arc::new(self), data_type)
    }
}

pub fn register_prophet_custom_data() {
    nautilus_serialization::ensure_custom_data_registered::<ProphetLastTradePrice>();
}

#[derive(Debug, Clone)]
pub struct ProphetDataClientConfig {
    pub market_ids: Vec<u64>,
    pub http_url: String,
    pub ws_url: String,
}

impl ProphetDataClientConfig {
    pub fn new(market_ids: Vec<u64>) -> Self {
        Self {
            market_ids,
            http_url: "https://www.binance.com".into(),
            ws_url: "wss://nbstream.binance.com/w3w/stream".into(),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let ids: BTreeSet<_> = self.market_ids.iter().copied().collect();
        anyhow::ensure!(
            (1..=MAX_MARKETS).contains(&ids.len())
                && !ids.contains(&0)
                && ids.len() == self.market_ids.len(),
            "Prophet requires 1–{MAX_MARKETS} distinct nonzero fixed market IDs"
        );
        Ok(())
    }
}

impl ClientConfig for ProphetDataClientConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ProphetDataClientFactory;

impl DataClientFactory for ProphetDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: nautilus_common::cache::CacheView,
        _clock: std::rc::Rc<std::cell::RefCell<dyn nautilus_common::clock::Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<ProphetDataClientConfig>()
            .context("expected ProphetDataClientConfig")?;
        Ok(Box::new(ProphetDataClient::new(
            ClientId::from(name),
            config.clone(),
        )?))
    }

    fn name(&self) -> &'static str {
        PROPHET
    }

    fn config_type(&self) -> &'static str {
        "ProphetDataClientConfig"
    }
}
