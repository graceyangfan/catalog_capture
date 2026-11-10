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

use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{BookOrder, OrderBookDepth},
    enums::{AssetClass, OrderSide, RecordFlag},
    identifiers::Symbol,
    instruments::{BinaryOption, Instrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};
use rust_decimal::Decimal;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use ustr::Ustr;

use crate::{ProphetDataClientConfig, instrument_id};

const PUBLIC: &str = "/bapi/defi/v1/public/wallet-direct";
const SEASON: &str = "/bapi/defi/v1/friendly/wallet-direct/growth/prophet/season/query";
const SIZE_PRECISION: u8 = 6;

pub(crate) fn now() -> UnixNanos {
    get_atomic_clock_realtime().get_time_ns()
}

pub(crate) fn millis(value: u64) -> anyhow::Result<UnixNanos> {
    anyhow::ensure!(value > 0, "missing Prophet timestamp");
    Ok(value
        .checked_mul(1_000_000)
        .context("Prophet timestamp overflow")?
        .into())
}

#[derive(Debug, Clone)]
pub(crate) struct Market {
    pub(crate) id: u64,
    pub(crate) token_id: String,
    pub(crate) instrument: InstrumentAny,
}

impl Market {
    pub(crate) fn price(&self, price: Decimal) -> anyhow::Result<Price> {
        let precision = self.instrument.price_precision();
        anyhow::ensure!(
            (Decimal::ZERO..=Decimal::ONE).contains(&price)
                && price.normalize().scale() <= u32::from(precision),
            "invalid Prophet price {price} at precision {precision}"
        );
        Ok(Price::from_decimal_dp(price, precision)?)
    }

    pub(crate) fn depth(&self, book: Book, received: UnixNanos) -> anyhow::Result<OrderBookDepth> {
        let ts_event = millis(book.timestamp)?;
        let mut depth = OrderBookDepth::new(
            self.instrument.id(),
            [],
            [],
            [],
            [],
            RecordFlag::F_SNAPSHOT as u8,
            0,
            ts_event,
            received,
        );
        for (side, levels, orders, counts) in [
            (
                OrderSide::Buy,
                book.bids,
                &mut depth.bids,
                &mut depth.bid_counts,
            ),
            (
                OrderSide::Sell,
                book.asks,
                &mut depth.asks,
                &mut depth.ask_counts,
            ),
        ] {
            for level in levels {
                anyhow::ensure!(
                    level.size >= Decimal::ZERO
                        && level.size.normalize().scale() <= u32::from(SIZE_PRECISION),
                    "unsupported Prophet size {}",
                    level.size
                );
                if !level.size.is_zero() {
                    orders.push(BookOrder::new(
                        side,
                        self.price(level.price)?,
                        Quantity::from_decimal_dp(level.size, SIZE_PRECISION)?,
                        0,
                    ));
                    counts.push(0);
                }
            }
        }
        depth
            .bids
            .sort_unstable_by_key(|order| std::cmp::Reverse(order.price));
        depth.asks.sort_unstable_by_key(|order| order.price);
        anyhow::ensure!(
            [&depth.bids, &depth.asks]
                .into_iter()
                .all(|side| side.windows(2).all(|w| w[0].price != w[1].price)),
            "duplicate Prophet price level"
        );
        Ok(depth)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProphetHttp {
    base: String,
    client: HttpClient,
}

#[derive(Deserialize)]
struct Envelope {
    code: String,
    success: bool,
    message: Option<String>,
    data: Option<Box<serde_json::value::RawValue>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Season {
    season_id: String,
}

#[derive(Deserialize)]
struct EventPage {
    events: Vec<Event>,
    offset: usize,
    total: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Event {
    event_slug: String,
    title: String,
    start_date: u64,
    end_date: u64,
    ungrouped_markets: Option<Vec<MarketWire>>,
    gameplay_groups: Option<Vec<Group>>,
}

#[derive(Deserialize)]
struct Group {
    markets: Option<Vec<MarketWire>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketWire {
    market_id: u64,
    market_topic_id: u64,
    title: String,
    decimal_precision: u8,
    outcomes: Vec<Outcome>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Outcome {
    outcome_index: u8,
    token_id: String,
    name: String,
}

#[derive(Deserialize)]
pub(crate) struct Level {
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    price: Decimal,
    #[serde(with = "rust_decimal::serde::arbitrary_precision")]
    size: Decimal,
}

#[derive(Deserialize)]
pub(crate) struct Book {
    #[serde(alias = "updateTimestampMs")]
    pub(crate) timestamp: u64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    token_id: String,
    #[serde(flatten)]
    book: Book,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreSubscribe {
    sub_keys: BTreeMap<String, String>,
}

impl ProphetHttp {
    pub(crate) fn new(config: &ProphetDataClientConfig) -> anyhow::Result<Self> {
        let mut headers: HashMap<_, _> = create_standard_nautilus_headers().into_iter().collect();
        headers.insert("Content-Type".into(), "application/json".into());
        Ok(Self {
            base: config.http_url.trim_end_matches('/').into(),
            client: HttpClient::builder()
                .headers(headers)
                .redirect_policy(HttpRedirectPolicy::Reject)
                .timeout_secs(8)
                .build()?,
        })
    }

    async fn post<T: DeserializeOwned>(&self, path: &str, body: Value) -> anyhow::Result<T> {
        let response = self
            .client
            .request(
                Method::POST,
                format!("{}{path}", self.base),
                None,
                None,
                Some(serde_json::to_vec(&body)?),
                None,
                None,
            )
            .await?;
        anyhow::ensure!(
            response.status.is_success(),
            "Prophet HTTP {} for {path}",
            response.status.as_u16()
        );
        let reply: Envelope = serde_json::from_slice(&response.body)?;
        anyhow::ensure!(
            reply.success && reply.code == "000000",
            "Prophet API {} for {path}: {}",
            reply.code,
            reply.message.unwrap_or_default()
        );
        Ok(serde_json::from_str(
            reply.data.context("missing Prophet data")?.get(),
        )?)
    }

    pub(crate) async fn load_markets(&self, ids: &[u64]) -> anyhow::Result<BTreeMap<u64, Market>> {
        let season: Season = self.post(SEASON, json!({})).await?;
        anyhow::ensure!(!season.season_id.is_empty(), "missing Prophet season ID");
        let mut result = BTreeMap::new();
        let mut offset = 0;
        for _ in 0..20 {
            let page: EventPage = self
                .post(
                    &format!("{PUBLIC}/predictx/event/list"),
                    json!({"offset":offset,"limit":20,"seasonId":season.season_id}),
                )
                .await?;
            anyhow::ensure!(
                page.offset == offset,
                "unexpected Prophet pagination offset"
            );
            anyhow::ensure!(
                !page.events.is_empty() || offset >= page.total,
                "Prophet pagination stalled"
            );
            offset += page.events.len();
            for event in page.events {
                let markets = event.ungrouped_markets.iter().flatten().chain(
                    event
                        .gameplay_groups
                        .iter()
                        .flatten()
                        .flat_map(|group| group.markets.iter().flatten()),
                );
                for market in markets.filter(|market| ids.contains(&market.market_id)) {
                    let context = market.build(&event, &season.season_id)?;
                    anyhow::ensure!(
                        result.insert(market.market_id, context).is_none(),
                        "duplicate Prophet market {}",
                        market.market_id
                    );
                }
            }
            if result.len() == ids.len() {
                return Ok(result);
            }
            if offset >= page.total {
                break;
            }
        }
        let missing: Vec<_> = ids.iter().filter(|id| !result.contains_key(id)).collect();
        anyhow::bail!(
            "Prophet season {} did not return configured markets {missing:?}",
            season.season_id
        )
    }

    pub(crate) async fn channels(&self, market_id: u64) -> anyhow::Result<[String; 2]> {
        let address = json!([{"paramKey":"address","op":"EQ","val":market_id.to_string()}]);
        let response: PreSubscribe = self
            .post(
                &format!("{PUBLIC}/ws/pre-subscribe"),
                json!({
                    "subscribeList":[
                        {"id":1,"bizKey":"PREDICTX_OBOOK","subParamList":address},
                        {"id":2,"bizKey":"PREDICTX_TRADE","subParamList":address}
                    ]
                }),
            )
            .await?;
        let book = response
            .sub_keys
            .get("1")
            .context("missing Prophet book subKey")?;
        let trade = response
            .sub_keys
            .get("2")
            .context("missing Prophet trade subKey")?;
        anyhow::ensure!(
            book.starts_with("w3pc:PREDICTX_OBOOK:") && trade.starts_with("w3pc:PREDICTX_TRADE:"),
            "unexpected Prophet subKey prefixes"
        );
        Ok([book.clone(), trade.clone()])
    }

    pub(crate) async fn book(&self, market: &Market) -> anyhow::Result<Book> {
        let snapshot: Snapshot = self
            .post(
                &format!("{PUBLIC}/predictx/order-book"),
                json!({"marketId":market.id,"tokenId":market.token_id}),
            )
            .await?;
        anyhow::ensure!(
            snapshot.token_id == market.token_id,
            "Prophet REST book token mismatch"
        );
        Ok(snapshot.book)
    }
}

impl MarketWire {
    fn build(&self, event: &Event, season: &str) -> anyhow::Result<Market> {
        let first = self
            .outcomes
            .iter()
            .find(|o| o.outcome_index == 0)
            .context("missing Prophet outcome index 0")?;
        let second = self
            .outcomes
            .iter()
            .find(|o| o.outcome_index == 1)
            .context("missing Prophet outcome index 1")?;
        anyhow::ensure!(
            self.outcomes.len() == 2
                && first.token_id != second.token_id
                && [first, second]
                    .into_iter()
                    .all(|outcome| !outcome.token_id.is_empty()
                        && outcome.token_id.bytes().all(|byte| byte.is_ascii_digit())),
            "invalid Prophet binary outcome tokens"
        );
        let activation = millis(event.start_date)?;
        let expiration = millis(event.end_date)?;
        anyhow::ensure!(
            activation < expiration && self.decimal_precision <= 16,
            "invalid Prophet market metadata"
        );
        let dp = self.decimal_precision;
        let info = indexmap::IndexMap::from([
            ("season_id".to_owned(), json!(season)),
            ("event_slug".to_owned(), json!(event.event_slug)),
            ("market_id".to_owned(), json!(self.market_id)),
            ("market_topic_id".to_owned(), json!(self.market_topic_id)),
            ("outcome_token_id".to_owned(), json!(first.token_id)),
            ("opposite_token_id".to_owned(), json!(second.token_id)),
            ("market_title".to_owned(), json!(self.title)),
            ("collateral_unit".to_owned(), json!("campaign_scores")),
        ]);
        let observed = now();
        let instrument = BinaryOption::builder()
            .instrument_id(instrument_id(self.market_id)?)
            .raw_symbol(Symbol::new_checked(self.market_id.to_string())?)
            .asset_class(AssetClass::Alternative)
            .currency(Currency::get_or_create_crypto("SCORE"))
            .activation_ns(activation)
            .expiration_ns(expiration)
            .price_precision(dp)
            .size_precision(SIZE_PRECISION)
            .price_increment(Price::from_decimal_dp(Decimal::new(1, dp.into()), dp)?)
            .size_increment(Quantity::from_decimal_dp(
                Decimal::new(1, SIZE_PRECISION.into()),
                SIZE_PRECISION,
            )?)
            .outcome(Ustr::from(first.name.as_str()))
            .event_id(Ustr::from(event.event_slug.as_str()))
            .description(Ustr::from(event.title.as_str()))
            .info(Params::new(info))
            .ts_event(observed)
            .ts_init(observed)
            .build()?;
        Ok(Market {
            id: self.market_id,
            token_id: first.token_id.clone(),
            instrument: InstrumentAny::BinaryOption(instrument),
        })
    }
}
