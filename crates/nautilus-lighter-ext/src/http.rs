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

//! Binary endpoints absent from upstream. Books and trades use upstream wire models/parsers.
use std::collections::HashMap;

use anyhow::Context;
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::AssetClass,
    identifiers::Symbol,
    instruments::Instrument,
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use ustr::Ustr;

use crate::{
    common::{instrument_id, oracle_price},
    config::LighterBinaryDataClientConfig,
    models::{BinaryEvent, BinaryMarket, MarketContext, OraclePoint},
};

#[derive(Debug, Clone)]
pub struct LighterBinaryHttpClient {
    base_url: String,
    client: HttpClient,
}
impl LighterBinaryHttpClient {
    pub fn new(config: &LighterBinaryDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            base_url: config.http_url().trim_end_matches('/').into(),
            client: HttpClient::builder()
                .headers(
                    create_standard_nautilus_headers()
                        .into_iter()
                        .collect::<HashMap<_, _>>(),
                )
                .redirect_policy(HttpRedirectPolicy::Reject)
                .default_quota(*nautilus_lighter::common::rate_limit::LIGHTER_REST_QUOTA)
                .timeout_secs(config.http_timeout_secs)
                .maybe_proxy_url(config.proxy_url.clone())
                .build()?,
        })
    }
    async fn get<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}{path}", self.base_url),
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        anyhow::ensure!(
            response.status.is_success(),
            "Lighter binary HTTP {} for {path}",
            response.status.as_u16()
        );
        let value: Value = serde_json::from_slice(&response.body)?;
        anyhow::ensure!(value["code"] == 200, "Lighter binary API error: {value}");
        serde_json::from_value(value).context("invalid Lighter binary response")
    }
    pub async fn load_markets(
        &self,
        ids: &[u64],
        now: UnixNanos,
    ) -> anyhow::Result<Vec<MarketContext>> {
        anyhow::ensure!(!ids.is_empty(), "fixed binary capture requires market_ids");
        let mut contexts = Vec::new();
        for id in ids {
            // One explicit filter avoids cursor truncation when many recurring events are live.
            let response: Value = self
                .get(&format!("/events?market_ids={id}&limit=50"))
                .await?;
            let events: Vec<BinaryEvent> = serde_json::from_value(response["events"].clone())?;
            let mut matches = events.into_iter().flat_map(|event| {
                event
                    .order_books
                    .clone()
                    .into_iter()
                    .filter(move |m| m.market_id == *id)
                    .map(move |m| (event.clone(), m))
            });
            let (event, market) = matches
                .next()
                .context(format!("binary market {id} not returned by /events"))?;
            anyhow::ensure!(matches.next().is_none(), "ambiguous event for market {id}");
            contexts.push(
                self.build_context(event, market, *id, None, false, now)
                    .await?,
            );
        }
        let mut sources = HashMap::new();
        for context in &contexts {
            if let Some(previous) = sources.insert(context.source.channel(), &context.source) {
                anyhow::ensure!(
                    previous == &context.source,
                    "one oracle wire channel has competing provider/feed identities"
                );
            }
        }
        for (i, context) in contexts.iter().enumerate() {
            anyhow::ensure!(
                contexts[..i]
                    .iter()
                    .all(|other| other.source.channel() != context.source.channel()
                        || other.source == context.source),
                "conflicting Lighter oracle providers on one physical channel"
            );
        }
        Ok(contexts)
    }
    async fn build_context(
        &self,
        event: BinaryEvent,
        market: BinaryMarket,
        id: u64,
        previous: Option<&MarketContext>,
        allow_pending: bool,
        now: UnixNanos,
    ) -> anyhow::Result<MarketContext> {
        anyhow::ensure!(
            market.market_type == "binary_options",
            "market {id} is not binary"
        );
        anyhow::ensure!(
            event.start_at.is_multiple_of(1000) && event.end_at > event.start_at,
            "invalid binary observation window"
        );
        for token in [
            &event.metadata.oracle.symbol,
            &event.metadata.oracle.duration,
            &event.metadata.oracle.provider,
            &event.metadata.oracle.feed_id,
        ] {
            anyhow::ensure!(
                !token.is_empty()
                    && token
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
                "invalid oracle identity"
            );
        }
        anyhow::ensure!(
            event.metadata.market_id == id && event.metadata.kind == "updown",
            "event metadata does not identify the requested Up/Down market"
        );
        let start = event.start_at / 1000;
        anyhow::ensure!(
            allow_pending || event.start_at <= now.as_u64() / 1_000_000,
            "fixed capture requires an observation window which has started (market {id})"
        );
        let mut info = Params::new();
        let previous_price = previous
            .filter(|old| old.start_secs == start && old.source == event.metadata.oracle)
            .and_then(|old| old.instrument.info())
            .and_then(|info| info.get("start_price"))
            .cloned();
        let price = if previous_price.is_some() {
            previous_price
        } else if event.start_at <= now.as_u64() / 1_000_000 {
            let path = format!(
                "/upDownCandles?symbol={}&duration={}&resolution=1s&start_timestamp={start}&end_timestamp={start}&limit=1",
                event.metadata.oracle.symbol, event.metadata.oracle.duration,
            );
            let response: Value = self.get(&path).await?;
            let points: Vec<OraclePoint> = serde_json::from_value(response["candles"].clone())?;
            let point = points
                .iter()
                .find(|p| p.timestamp == start && event.metadata.oracle.validates(p));
            point
                .map(|p| oracle_price(p.close).map(|price| json!(price.to_string())))
                .transpose()?
        } else {
            None
        };
        if let Some(price) = price {
            info.insert("start_price".into(), price);
            info.insert("start_price_timestamp".into(), json!(start));
        } else {
            anyhow::ensure!(
                allow_pending,
                "authoritative boundary oracle unavailable for market {id}; refusing an approximate start_price"
            );
        }
        info.insert("market_id".into(), json!(id));
        info.insert("event_id".into(), json!(event.event_id));
        info.insert(
            "underlying_market_id".into(),
            json!(event.metadata.underlying_market_id),
        );
        info.insert("observation_start_ms".into(), json!(event.start_at));
        info.insert(
            "oracle".into(),
            json!({
                "symbol": event.metadata.oracle.symbol,
                "duration": event.metadata.oracle.duration,
                "provider": event.metadata.oracle.provider,
                "feed_id": event.metadata.oracle.feed_id,
                "resolution": "1s",
                "unit": "USD",
                "precision": 16,
            }),
        );
        info.insert("status".into(), json!(market.status));
        info.insert("event_tag".into(), json!(event.tag));
        info.insert("is_frozen".into(), json!(market.is_frozen));
        let dp = market.supported_price_decimals;
        let sp = market.supported_size_decimals;
        anyhow::ensure!(
            dp <= 16 && sp <= 16,
            "binary precision exceeds Nautilus fixed-point precision"
        );
        let currency = Currency::USDC();
        let binary = BinaryOption::builder()
            .instrument_id(instrument_id(id)?)
            .raw_symbol(Symbol::new_checked(&market.symbol)?)
            .asset_class(AssetClass::Alternative)
            .currency(currency)
            .activation_ns(UnixNanos::from(
                market
                    .start_timestamp
                    .checked_mul(1_000_000)
                    .context("activation overflow")?,
            ))
            .expiration_ns(UnixNanos::from(
                event
                    .end_at
                    .checked_mul(1_000_000)
                    .context("expiration overflow")?,
            ))
            .price_precision(dp)
            .size_precision(sp)
            .price_increment(Price::from_decimal_dp(Decimal::new(1, dp.into()), dp)?)
            .size_increment(Quantity::from_decimal_dp(Decimal::new(1, sp.into()), sp)?)
            .min_quantity(Quantity::from_decimal_dp(market.min_base_amount, sp)?)
            .min_notional(Money::from_decimal(market.min_quote_amount, currency)?)
            .min_price(Price::from_decimal_dp(Decimal::ZERO, dp)?)
            .max_price(Price::from_decimal_dp(Decimal::ONE, dp)?)
            .outcome(Ustr::from("Up"))
            .event_id(Ustr::from(event.event_id.to_string().as_str()))
            .description(Ustr::from(event.title.as_str()))
            .info(info)
            .ts_event(now)
            .ts_init(now)
            .build()?;
        Ok(MarketContext {
            market_id: id,
            instrument: InstrumentAny::BinaryOption(binary),
            source: event.metadata.oracle,
            start_secs: start,
        })
    }

    /// Current observations and imminent successors.
    /// Selection is venue discovery; retention and capture ownership remain downstream policy.
    pub async fn request_universe(
        &self,
        tags: &[String],
        prefetch_secs: u64,
        known: &HashMap<u64, MarketContext>,
        now: UnixNanos,
    ) -> anyhow::Result<Vec<MarketContext>> {
        anyhow::ensure!(
            !tags.is_empty() && tags.len() <= 2 && prefetch_secs <= 300,
            "invalid Lighter universe request"
        );
        let now_ms = now.as_u64() / 1_000_000;
        let mut selected = std::collections::BTreeMap::new();
        for tag in tags {
            crate::common::validate_updown_tag(tag)?;
            let mut cursor = None;
            for page in 0..20 {
                let suffix = cursor.map_or(String::new(), |c: u64| format!("&cursor={c}"));
                let response: Value = self
                    .get(&format!("/events?active=true&tag={tag}&limit=50{suffix}"))
                    .await?;
                let events: Vec<BinaryEvent> = serde_json::from_value(response["events"].clone())?;
                for event in events {
                    anyhow::ensure!(
                        event.tag == *tag,
                        "Lighter event tag does not match request"
                    );
                    if event.end_at > now_ms
                        && event.start_at <= now_ms.saturating_add(prefetch_secs * 1000)
                    {
                        for market in &event.order_books {
                            if market.market_type == "binary_options" {
                                selected.insert(market.market_id, (event.clone(), market.clone()));
                            }
                        }
                    }
                }
                let next = response["next_cursor"].as_u64().filter(|c| *c > 0);
                if next.is_none() {
                    break;
                }
                anyhow::ensure!(next != cursor, "Lighter event cursor did not advance");
                anyhow::ensure!(page < 19, "Lighter event pagination exceeded twenty pages");
                cursor = next;
            }
        }
        anyhow::ensure!(
            selected.len() <= 8,
            "Lighter universe exceeds eight current/prefetched markets"
        );
        let mut contexts = Vec::new();
        for (id, (event, market)) in selected {
            contexts.push(
                self.build_context(event, market, id, known.get(&id), true, now)
                    .await?,
            );
        }
        for (i, context) in contexts.iter().enumerate() {
            anyhow::ensure!(
                contexts[..i]
                    .iter()
                    .all(|other| other.source.channel() != context.source.channel()
                        || other.source == context.source),
                "conflicting Lighter oracle providers on one physical channel"
            );
        }
        Ok(contexts)
    }
}
