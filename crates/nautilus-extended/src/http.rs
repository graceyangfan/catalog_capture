// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::collections::HashMap;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::{
    create_standard_nautilus_headers, HttpClient, HttpRedirectPolicy, Method,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{
    common::instrument_id_from_market,
    config::{ExtendedDataClientConfig, ExtendedInstrumentProviderConfig},
};

const MARKETS_ENDPOINT: &str = "/info/markets";

#[derive(Debug, Clone)]
pub struct ExtendedHttpClient {
    base_url: String,
    client: HttpClient,
}

impl ExtendedHttpClient {
    pub fn new(config: &ExtendedDataClientConfig) -> anyhow::Result<Self> {
        let base_url = config.http_url().trim_end_matches('/').to_string();
        let headers = create_standard_nautilus_headers()
            .into_iter()
            .collect::<HashMap<_, _>>();
        let client = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(config.http_timeout_secs)
            .maybe_proxy_url(config.proxy_url.clone())
            .build()
            .context("failed to build Extended HTTP client")?;
        Ok(Self { base_url, client })
    }

    pub async fn request_instruments(
        &self,
        provider: &ExtendedInstrumentProviderConfig,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Vec<InstrumentAny>> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}{MARKETS_ENDPOINT}", self.base_url),
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .context("Extended markets request failed")?;
        anyhow::ensure!(
            response.status.is_success(),
            "Extended markets request returned HTTP {}",
            response.status.as_u16(),
        );

        let payload: ApiResponse<Vec<ExtendedMarket>> = serde_json::from_slice(&response.body)
            .context("failed to decode Extended markets response")?;
        anyhow::ensure!(
            payload.status == "OK",
            "Extended markets request failed: {}",
            payload
                .error
                .as_ref()
                .map_or("unknown venue error", |error| error.message.as_str()),
        );

        let mut instruments = Vec::new();
        for market in payload.data {
            if !market.is_supported_perpetual() {
                continue;
            }
            let instrument_id = instrument_id_from_market(&market.name)?;
            if !provider.includes(instrument_id) {
                continue;
            }
            let instrument = parse_market(&market, ts_init)?;
            instruments.push(instrument);
        }
        anyhow::ensure!(
            provider.load_all
                || provider.load_ids.as_ref().is_none_or(Vec::is_empty)
                || !instruments.is_empty(),
            "none of the configured Extended instruments were returned by the venue",
        );
        Ok(instruments)
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    status: String,
    data: T,
    error: Option<ApiError>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedMarket {
    name: String,
    #[serde(rename = "type")]
    market_type: String,
    asset_name: String,
    collateral_asset_name: String,
    active: bool,
    is_rfq: bool,
    status: String,
    trading_config: ExtendedTradingConfig,
}

impl ExtendedMarket {
    fn is_supported_perpetual(&self) -> bool {
        self.market_type == "PERPETUAL" && self.active && !self.is_rfq && self.status == "ACTIVE"
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtendedTradingConfig {
    min_order_size: Decimal,
    min_order_size_change: Decimal,
    min_price_change: Decimal,
}

fn parse_market(market: &ExtendedMarket, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
    let instrument_id = instrument_id_from_market(&market.name)?;
    let price_increment = market.trading_config.min_price_change.normalize();
    let size_increment = market.trading_config.min_order_size_change.normalize();
    let price_precision =
        u8::try_from(price_increment.scale()).context("Extended price precision exceeds u8")?;
    let size_precision =
        u8::try_from(size_increment.scale()).context("Extended size precision exceeds u8")?;

    let base_currency = Currency::get_or_create_crypto(market.asset_name.as_str());
    let quote_currency = Currency::get_or_create_crypto(market.collateral_asset_name.as_str());
    let instrument = CryptoPerpetual::builder()
        .instrument_id(instrument_id)
        .raw_symbol(Symbol::new_checked(&market.name)?)
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .settlement_currency(quote_currency)
        .is_inverse(false)
        .price_precision(price_precision)
        .size_precision(size_precision)
        .price_increment(Price::from_decimal_dp(price_increment, price_precision)?)
        .size_increment(Quantity::from_decimal_dp(size_increment, size_precision)?)
        .min_quantity(Quantity::from_decimal_dp(
            market.trading_config.min_order_size,
            size_precision,
        )?)
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    Ok(InstrumentAny::CryptoPerpetual(instrument))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::instruments::Instrument;

    #[test]
    fn parses_mainnet_btc_market_fixture() {
        let response: ApiResponse<Vec<ExtendedMarket>> =
            serde_json::from_str(include_str!("../test_data/http_markets_btc.json")).unwrap();
        assert_eq!(response.status, "OK");
        assert_eq!(response.data.len(), 1);
        let market = &response.data[0];
        assert!(market.is_supported_perpetual());

        let instrument = parse_market(market, UnixNanos::from(1)).unwrap();
        assert_eq!(instrument.id().to_string(), "BTC-USD-PERP.EXTENDED");
        assert_eq!(instrument.price_precision(), 0);
        assert_eq!(instrument.size_precision(), 5);
        assert_eq!(instrument.price_increment(), Price::from("1"));
        assert_eq!(instrument.size_increment(), Quantity::from("0.00001"));
        assert_eq!(instrument.min_quantity(), Some(Quantity::from("0.00010")));
        assert_eq!(instrument.raw_symbol().as_str(), "BTC-USD");
    }

    #[test]
    fn rejects_rfq_and_inactive_markets() {
        let market: ExtendedMarket = serde_json::from_str(
            r#"{
                "name":"BTC-USD","type":"PERPETUAL","assetName":"BTC",
                "collateralAssetName":"USD","active":true,"isRfq":true,"status":"ACTIVE",
                "tradingConfig":{"minOrderSize":"0.0001","minOrderSizeChange":"0.00001","minPriceChange":"0.1"}
            }"#,
        )
        .unwrap();
        assert!(!market.is_supported_perpetual());
    }

    #[tokio::test]
    #[ignore = "requires Extended mainnet network access"]
    async fn mainnet_btc_instrument_contract() {
        let config = ExtendedDataClientConfig::default();
        let client = ExtendedHttpClient::new(&config).unwrap();
        let provider = ExtendedInstrumentProviderConfig {
            load_all: false,
            load_ids: Some(vec![instrument_id_from_market("BTC-USD").unwrap()]),
        };
        let instruments = client
            .request_instruments(&provider, UnixNanos::from(1))
            .await
            .unwrap();
        assert_eq!(instruments.len(), 1);
        assert_eq!(instruments[0].id().to_string(), "BTC-USD-PERP.EXTENDED");
    }
}
