// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Public HTTP bootstrap, using the official SDK exchangeInfo wire schema.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, Method, create_standard_nautilus_headers,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    common::{BULLET_VENUE, instrument_id_from_market},
    config::{BulletDataClientConfig, BulletInstrumentProviderConfig},
};

#[derive(Debug, Clone)]
pub struct BulletHttpClient {
    base_url: String,
    client: HttpClient,
}
impl BulletHttpClient {
    pub fn new(config: &BulletDataClientConfig) -> anyhow::Result<Self> {
        let client = HttpClient::builder()
            .headers(
                create_standard_nautilus_headers()
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
            )
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(config.http_timeout_secs)
            .maybe_proxy_url(config.proxy_url.clone())
            .build()
            .context("failed to build Bullet HTTP client")?;
        Ok(Self {
            base_url: config.http_url().trim_end_matches('/').to_string(),
            client,
        })
    }
    pub(crate) async fn request_public(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}{}", self.base_url, path),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .with_context(|| format!("Bullet public request failed: {path}"))?;
        anyhow::ensure!(
            response.status.is_success(),
            "Bullet {path} returned HTTP {}",
            response.status.as_u16()
        );
        Ok(response.body.to_vec())
    }
    pub async fn request_instruments(
        &self,
        provider: &BulletInstrumentProviderConfig,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}/fapi/v1/exchangeInfo", self.base_url),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .context("Bullet exchangeInfo request failed")?;
        anyhow::ensure!(
            response.status.is_success(),
            "Bullet exchangeInfo returned HTTP {}",
            response.status.as_u16()
        );
        parse_instruments(&response.body, provider, ts_init)
    }
}

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<BulletMarket>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulletMarket {
    market_id: u16,
    symbol: String,
    contract_type: String,
    status: String,
    base_asset: String,
    quote_asset: String,
    margin_asset: String,
    price_precision: u8,
    quantity_precision: u8,
    filters: Vec<serde_json::Value>,
}
fn decimal(value: &serde_json::Value, name: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str_exact(
        value
            .get(name)
            .and_then(serde_json::Value::as_str)
            .with_context(|| format!("Bullet filter missing {name}"))?,
    )
    .context("invalid Bullet filter decimal")
}
fn currency(code: &str) -> anyhow::Result<Currency> {
    if code == "USD" {
        Ok(Currency::USD())
    } else {
        Currency::new_checked(
            code,
            8,
            0,
            code,
            nautilus_model::enums::CurrencyType::Crypto,
        )?;
        Ok(Currency::get_or_create_crypto(code))
    }
}
pub fn parse_instruments(
    bytes: &[u8],
    provider: &BulletInstrumentProviderConfig,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<InstrumentAny>> {
    if let Some(ids) = &provider.load_ids {
        anyhow::ensure!(
            ids.iter().all(|id| id.venue == *BULLET_VENUE),
            "Bullet provider has a foreign instrument ID"
        );
    }
    let response: ExchangeInfo =
        serde_json::from_slice(bytes).context("invalid Bullet exchangeInfo")?;
    let mut seen = HashSet::new();
    let mut instruments = Vec::new();
    for market in response.symbols {
        let id = instrument_id_from_market(&market.symbol)?;
        if !provider.includes(id) {
            continue;
        }
        // The live API uses CryptoPerp, not Binance's PERPETUAL literal.
        if market.contract_type != "CryptoPerp"
            || !matches!(
                market.status.as_str(),
                "TRADING" | "POST_ONLY" | "CANCEL_ONLY" | "HALT"
            )
        {
            continue;
        }
        anyhow::ensure!(seen.insert(id), "duplicate Bullet market {id}");
        let mut price_filter = None;
        let mut size_filter = None;
        for filter in &market.filters {
            match filter.get("filterType").and_then(serde_json::Value::as_str) {
                Some("PRICE_FILTER") => {
                    anyhow::ensure!(price_filter.is_none(), "duplicate Bullet price filter");
                    price_filter = Some(filter);
                }
                Some("LOT_SIZE") => {
                    anyhow::ensure!(size_filter.is_none(), "duplicate Bullet lot filter");
                    size_filter = Some(filter);
                }
                _ => {}
            }
        }
        let pf = price_filter.context("Bullet market missing PRICE_FILTER")?;
        let sf = size_filter.context("Bullet market missing LOT_SIZE")?;
        let tick = decimal(pf, "tickSize")?;
        let step = decimal(sf, "stepSize")?;
        anyhow::ensure!(
            tick > Decimal::ZERO && step > Decimal::ZERO,
            "Bullet increments must be positive"
        );
        let pp = market
            .price_precision
            .max(u8::try_from(tick.normalize().scale())?);
        let sp = u8::try_from(step.normalize().scale())?;
        let price = |d: Decimal| -> anyhow::Result<Price> {
            anyhow::ensure!(
                d.normalize().scale() <= u32::from(pp),
                "Bullet price limit precision exceeds instrument"
            );
            Ok(Price::from_decimal_dp(d, pp)?)
        };
        let quantity = |d: Decimal| -> anyhow::Result<Quantity> {
            anyhow::ensure!(
                d.normalize().scale() <= u32::from(sp),
                "Bullet size limit precision exceeds step precision"
            );
            Ok(Quantity::from_decimal_dp(d, sp)?)
        };
        let min_qty = decimal(sf, "minQty")?;
        let max_qty = decimal(sf, "maxQty")?;
        let min_price = decimal(pf, "minPrice")?;
        let max_price = decimal(pf, "maxPrice")?;
        anyhow::ensure!(
            [min_qty, max_qty, min_price, max_price]
                .iter()
                .all(|x| *x >= Decimal::ZERO),
            "negative Bullet filter limit"
        );
        anyhow::ensure!(
            max_qty.is_zero() || max_qty >= min_qty,
            "Bullet maxQty below minQty"
        );
        anyhow::ensure!(
            max_price.is_zero() || max_price >= min_price,
            "Bullet maxPrice below minPrice"
        );
        let mut instrument_info = serde_json::to_value(&market)?;
        instrument_info["funding_rate_contract"] = serde_json::json!({
            "kind": "settled_gross", "rate_basis_minutes": 60,
            "source_rate_basis_minutes": 480, "source": "/fapi/v1/fundingRate",
            "funding_spread_applied": false,
        });
        let instrument = CryptoPerpetual::builder()
            .instrument_id(id)
            .raw_symbol(Symbol::new_checked(&market.symbol)?)
            .base_currency(currency(&market.base_asset)?)
            .quote_currency(currency(&market.quote_asset)?)
            .settlement_currency(currency(&market.margin_asset)?)
            .is_inverse(false)
            .price_precision(pp)
            .size_precision(sp)
            .price_increment(price(tick)?)
            .size_increment(quantity(step)?)
            .maybe_min_quantity(
                (min_qty > Decimal::ZERO)
                    .then(|| quantity(min_qty))
                    .transpose()?,
            )
            .maybe_max_quantity(
                (max_qty > Decimal::ZERO)
                    .then(|| quantity(max_qty))
                    .transpose()?,
            )
            .maybe_min_price(
                (min_price > Decimal::ZERO)
                    .then(|| price(min_price))
                    .transpose()?,
            )
            .maybe_max_price(
                (max_price > Decimal::ZERO)
                    .then(|| price(max_price))
                    .transpose()?,
            )
            .info(serde_json::from_value(instrument_info)?)
            .ts_event(ts_init)
            .ts_init(ts_init)
            .build()
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        instruments.push(InstrumentAny::CryptoPerpetual(instrument));
    }
    if let Some(ids) = &provider.load_ids {
        let missing = ids
            .iter()
            .filter(|id| !seen.contains(id))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            missing.is_empty(),
            "Bullet requested instruments unavailable or unsupported: {missing:?}"
        );
    }
    anyhow::ensure!(
        !instruments.is_empty(),
        "Bullet returned no selected perpetual instruments"
    );
    Ok(instruments)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use nautilus_model::instruments::Instrument;
    const FIXTURE: &[u8] = include_bytes!("../test_data/http_exchange_info_mainnet.json");
    pub(crate) fn btc() -> InstrumentAny {
        parse_instruments(
            FIXTURE,
            &BulletInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec![instrument_id_from_market("BTC-USD").unwrap()]),
            },
            UnixNanos::from(1),
        )
        .unwrap()
        .remove(0)
    }
    #[test]
    fn captured_metadata_uses_step_instead_of_quantity_precision_and_keeps_usd() {
        let btc = btc();
        assert_eq!(btc.id().to_string(), "BTC-USD-PERP.BULLET");
        assert_eq!(btc.raw_symbol().as_str(), "BTC-USD");
        assert_eq!(btc.size_precision(), 4);
        assert_eq!(btc.price_precision(), 8);
        assert_eq!(btc.price_increment(), Price::from("1.00000000"));
        assert_eq!(btc.size_increment(), Quantity::from("0.0001"));
        let InstrumentAny::CryptoPerpetual(btc) = btc else {
            panic!()
        };
        assert_eq!(btc.settlement_currency, Currency::USD());
        assert_eq!(btc.min_quantity, Some(Quantity::from("0.0001")));
        assert!(btc.max_quantity.is_none());
        assert_eq!(btc.info.unwrap()["marketId"], serde_json::json!(1));
    }
    #[test]
    fn requested_metadata_must_be_complete_and_cannot_be_foreign() {
        for id in ["MISSING-USD-PERP.BULLET", "BTC-USD-PERP.EXTENDED"] {
            let provider = BulletInstrumentProviderConfig {
                load_all: false,
                load_ids: Some(vec![
                    instrument_id_from_market("BTC-USD").unwrap(),
                    id.into(),
                ]),
            };
            assert!(parse_instruments(FIXTURE, &provider, UnixNanos::from(1)).is_err());
        }
    }
    #[test]
    fn rejects_synthetic_missing_duplicate_or_invalid_filters() {
        let original: serde_json::Value = serde_json::from_slice(FIXTURE).unwrap();
        for kind in ["missing", "duplicate", "negative", "zero"] {
            let mut value = original.clone();
            let market = value["symbols"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|m| m["symbol"] == "BTC-USD")
                .unwrap();
            match kind {
                "missing" => market["filters"] = serde_json::json!([]),
                "duplicate" => {
                    let f = market["filters"][0].clone();
                    market["filters"].as_array_mut().unwrap().push(f);
                }
                "negative" => market["filters"][1]["stepSize"] = serde_json::json!("-1"),
                _ => market["filters"][0]["tickSize"] = serde_json::json!("0"),
            }
            assert!(
                parse_instruments(
                    &serde_json::to_vec(&value).unwrap(),
                    &BulletInstrumentProviderConfig {
                        load_all: false,
                        load_ids: Some(vec![instrument_id_from_market("BTC-USD").unwrap()])
                    },
                    UnixNanos::from(1)
                )
                .is_err(),
                "{kind}"
            );
        }
    }
}
