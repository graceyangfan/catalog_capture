// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::collections::HashMap;

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

/// Checks the local selector syntax. Listing status is verified when loading metadata.
pub fn supported_instrument(id: InstrumentId) -> bool {
    id.venue.as_str() == BITFINEX && spot_pair(id.symbol.as_str()).is_some()
}

fn spot_pair(pair: &str) -> Option<(&str, &str)> {
    // Bitfinex joins two three-character currency codes, using a colon
    // when either code is longer.
    let (base, quote) = pair
        .split_once(':')
        .or_else(|| (pair.is_ascii() && pair.len() == 6).then(|| pair.split_at(3)))?;
    let valid = |code: &str| {
        !code.is_empty()
            && code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    };
    (valid(base) && valid(quote) && !(base.starts_with("TEST") && quote.starts_with("TEST")))
        .then_some((base, quote))
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
                "{}/v2/conf/pub:list:pair:exchange,pub:info:pair,pub:map:currency:sym",
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
    let response = serde_json::from_slice(&response.body)?;
    parse_instruments(&response, ids, now)
}

fn parse_instruments(
    response: &Value,
    ids: &[InstrumentId],
    now: UnixNanos,
) -> anyhow::Result<Vec<InstrumentAny>> {
    let pairs = response[0]
        .as_array()
        .context("missing Bitfinex pair list")?;
    let info = response[1]
        .as_array()
        .context("missing Bitfinex pair info")?;
    let aliases = response[2]
        .as_array()
        .context("missing Bitfinex currency symbol map")?
        .iter()
        .map(|entry| {
            Ok((
                entry[0]
                    .as_str()
                    .context("invalid Bitfinex currency code")?,
                entry[1]
                    .as_str()
                    .context("invalid Bitfinex currency alias")?,
            ))
        })
        .collect::<anyhow::Result<HashMap<_, _>>>()?;
    let currency = |code: &str| {
        let canonical = aliases
            .get(code)
            .copied()
            .unwrap_or(code)
            .to_ascii_uppercase();
        Currency::get_or_create_crypto(canonical)
    };
    ids.iter()
        .map(|&id| {
            ensure!(id.venue.as_str() == BITFINEX, "unexpected venue for {id}");
            let pair = id.symbol.as_str();
            let (base, quote) = spot_pair(pair)
                .with_context(|| format!("invalid Bitfinex spot pair symbol {pair}"))?;
            ensure!(
                pairs.iter().any(|value| value.as_str() == Some(pair)),
                "Bitfinex spot pair {pair} is not listed"
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
                .base_currency(currency(base))
                .quote_currency(currency(quote))
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

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::Instrument;
    use serde_json::json;

    use super::*;

    // Representative rows from Bitfinex's public spot configuration. The omitted
    // metadata positions are unused by the recorder.
    fn catalogue() -> Value {
        json!([
            [
                "BTCUSD",
                "ETHUSD",
                "ETHBTC",
                "BTCUST",
                "SOLUSD",
                "AVAX:USD",
                "OPXUSD",
                "TESTADA:TESTUSD"
            ],
            [
                [
                    "BTCUSD",
                    [1358182043000_i64, null, null, "0.00004", "2000.0"]
                ],
                [
                    "ETHUSD",
                    [1457539475000_i64, null, null, "0.0008", "5000.0"]
                ],
                [
                    "ETHBTC",
                    [1457539859000_i64, null, null, "0.0008", "5000.0"]
                ],
                [
                    "BTCUST",
                    [1552298737974_i64, null, null, "0.00004", "2000.0"]
                ],
                ["SOLUSD", [1614246014264_i64, null, null, "0.02", "50000.0"]],
                [
                    "AVAX:USD",
                    [1600862465185_i64, null, null, "0.08", "50000.0"]
                ],
                ["OPXUSD", [1690451918963_i64, null, null, "0.6", "250000.0"]],
                ["TESTADA:TESTUSD", [null, null, null, "4.0", "250000.0"]]
            ],
            [["UST", "USDt"], ["OPX", "OP"]]
        ])
    }

    #[test]
    fn listed_spot_definitions_keep_wire_symbols_and_use_catalogue_currencies() {
        let cases = [
            ("BTCUSD", "BTC", "USD", "0.00004"),
            ("ETHUSD", "ETH", "USD", "0.0008"),
            ("ETHBTC", "ETH", "BTC", "0.0008"),
            ("BTCUST", "BTC", "USDT", "0.00004"),
            ("SOLUSD", "SOL", "USD", "0.02"),
            ("AVAX:USD", "AVAX", "USD", "0.08"),
            ("OPXUSD", "OP", "USD", "0.6"),
        ];
        let ids = cases.map(|(pair, ..)| InstrumentId::from(format!("{pair}.BITFINEX").as_str()));
        let definitions = parse_instruments(&catalogue(), &ids, UnixNanos::from(5)).unwrap();
        for ((pair, base, quote, min), (id, definition)) in
            cases.into_iter().zip(ids.into_iter().zip(definitions))
        {
            let InstrumentAny::CurrencyPair(instrument) = definition else {
                panic!("expected a spot currency pair");
            };
            assert_eq!(instrument.id(), id);
            assert_eq!(instrument.raw_symbol().as_str(), format!("t{pair}"));
            assert_eq!(instrument.base_currency.code.as_str(), base);
            assert_eq!(instrument.quote_currency.code.as_str(), quote);
            assert_eq!(instrument.min_quantity(), Some(Quantity::from(min)));
        }
    }

    #[test]
    fn rejects_unlisted_and_paper_pairs_before_subscription() {
        let id = InstrumentId::from("BNBUSD.BITFINEX");
        assert!(supported_instrument(id));
        assert!(
            parse_instruments(&catalogue(), &[id], UnixNanos::default())
                .unwrap_err()
                .to_string()
                .contains("not listed")
        );
        let paper = InstrumentId::from("TESTADA:TESTUSD.BITFINEX");
        assert!(!supported_instrument(paper));
        assert!(parse_instruments(&catalogue(), &[paper], UnixNanos::default()).is_err());
    }
}
