// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use anyhow::{Context, ensure};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CurrencyPair, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::{HttpClient, HttpRedirectPolicy, Method};
use serde_json::Value;

pub const BITFINEX: &str = "BITFINEX";

pub fn supported_instrument(id: InstrumentId) -> bool {
    id.venue.as_str() == BITFINEX && matches!(id.symbol.as_str(), "BTCUSD" | "ETHUSD")
}

pub(crate) async fn load_instruments(
    base_url: &str,
    ids: &[InstrumentId],
    now: UnixNanos,
) -> anyhow::Result<Vec<InstrumentAny>> {
    let http = HttpClient::builder()
        .redirect_policy(HttpRedirectPolicy::Reject)
        .timeout_secs(10)
        .build()?;
    let response = http
        .request(
            Method::GET,
            format!(
                "{}/v2/conf/pub:list:pair:exchange,pub:info:pair",
                base_url.trim_end_matches('/')
            ),
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
    ensure!(
        response.status.is_success(),
        "Bitfinex pair metadata HTTP {}",
        response.status.as_u16()
    );
    let response: Value = serde_json::from_slice(&response.body)?;
    let pairs = response[0]
        .as_array()
        .context("missing Bitfinex pair list")?;
    let info = response[1]
        .as_array()
        .context("missing Bitfinex pair info")?;
    ids.iter()
        .map(|&id| {
            ensure!(
                supported_instrument(id),
                "unsupported Bitfinex spot instrument {id}"
            );
            let pair = id.symbol.as_str();
            ensure!(
                pairs.iter().any(|value| value.as_str() == Some(pair)),
                "Bitfinex pair {pair} is unavailable"
            );
            let limits = &info
                .iter()
                .find(|entry| entry[0].as_str() == Some(pair))
                .with_context(|| format!("missing Bitfinex pair info for {pair}"))?[1];
            let size = |index: usize| -> anyhow::Result<Quantity> {
                let token = limits[index]
                    .as_str()
                    .context("missing Bitfinex order size bound")?;
                Ok(Quantity::from_decimal_dp(token.parse()?, 8)?)
            };
            let metadata = Params::new([("price_significant_digits".to_owned(), 5.into())].into());
            // The fixed increment represents storage precision. The venue's effective
            // tick varies with price and follows the five-significant-digit rule.
            let instrument = CurrencyPair::builder()
                .instrument_id(id)
                .raw_symbol(Symbol::from(format!("t{pair}")))
                .base_currency(Currency::from(&pair[..3]))
                .quote_currency(Currency::from("USD"))
                .price_precision(8)
                .size_precision(8)
                .price_increment(Price::from("0.00000001"))
                .size_increment(Quantity::from("0.00000001"))
                .min_quantity(size(3)?)
                .max_quantity(size(4)?)
                .info(metadata)
                .ts_event(now)
                .ts_init(now)
                .build()?;
            Ok(InstrumentAny::CurrencyPair(instrument))
        })
        .collect()
}
