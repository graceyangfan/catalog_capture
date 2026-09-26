// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use nautilus_model::identifiers::InstrumentId;
use serde::Deserialize;
use serde_json::{json, Value};
use ustr::Ustr;

use crate::common::{instrument_id_from_market, market_from_instrument_id};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExtendedDataKind {
    OrderBook,
    Trades,
    MarkPrice,
    IndexPrice,
    FundingRate,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExtendedTopic {
    pub kind: ExtendedDataKind,
    pub market: Ustr,
}

impl ExtendedTopic {
    pub fn new(kind: ExtendedDataKind, instrument_id: InstrumentId) -> anyhow::Result<Self> {
        Ok(Self {
            kind,
            market: Ustr::from(&market_from_instrument_id(instrument_id)?),
        })
    }

    #[must_use]
    pub fn key(&self) -> String {
        match self.kind {
            ExtendedDataKind::OrderBook => format!("orderbooks.{}", self.market),
            ExtendedDataKind::Trades => format!("trades.{}", self.market),
            ExtendedDataKind::MarkPrice => format!("prices.mark.{}", self.market),
            ExtendedDataKind::IndexPrice => format!("prices.index.{}", self.market),
            ExtendedDataKind::FundingRate => format!("funding-rates.{}", self.market),
        }
    }

    #[must_use]
    pub fn subscribe_params(&self) -> Value {
        match self.kind {
            ExtendedDataKind::OrderBook => json!({
                "scope": "orderbooks",
                "selector": {"market": self.market, "depth": "full", "rfqOnly": false},
            }),
            ExtendedDataKind::Trades => json!({
                "scope": "trades",
                "selector": {"market": self.market},
            }),
            ExtendedDataKind::MarkPrice => json!({
                "scope": "prices",
                "selector": {"type": "mark", "market": self.market},
            }),
            ExtendedDataKind::IndexPrice => json!({
                "scope": "prices",
                "selector": {"type": "index", "market": self.market},
            }),
            ExtendedDataKind::FundingRate => json!({
                "scope": "funding-rates",
                "selector": {"market": self.market},
            }),
        }
    }

    pub fn instrument_id(&self) -> anyhow::Result<InstrumentId> {
        instrument_id_from_market(self.market.as_str())
    }
}

#[derive(Debug)]
pub enum ExtendedCommand {
    Subscribe(ExtendedTopic),
    Unsubscribe(ExtendedTopic),
}

#[derive(Debug, Deserialize)]
pub struct StreamEnvelope {
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Value,
    pub ts: u64,
    pub seq: u64,
    pub subscription: String,
}

#[derive(Debug, Deserialize)]
pub struct RpcResult {
    pub method: String,
    pub status: String,
    pub subscription: Option<String>,
    pub subscriptions: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::instrument_id_from_market;

    #[test]
    fn full_book_topic_matches_official_rpc_selector() {
        let topic = ExtendedTopic::new(
            ExtendedDataKind::OrderBook,
            instrument_id_from_market("BTC-USD").unwrap(),
        )
        .unwrap();
        assert_eq!(topic.key(), "orderbooks.BTC-USD");
        assert_eq!(
            topic.subscribe_params(),
            json!({
                "scope": "orderbooks",
                "selector": {"market": "BTC-USD", "depth": "full", "rfqOnly": false},
            }),
        );
    }

    #[test]
    fn price_topics_keep_price_kind_in_identity() {
        let id = instrument_id_from_market("BTC-USD").unwrap();
        let mark = ExtendedTopic::new(ExtendedDataKind::MarkPrice, id).unwrap();
        let index = ExtendedTopic::new(ExtendedDataKind::IndexPrice, id).unwrap();
        assert_eq!(mark.key(), "prices.mark.BTC-USD");
        assert_eq!(index.key(), "prices.index.BTC-USD");
        assert_ne!(mark, index);
    }

    #[test]
    fn mainnet_rpc_responses_match_expected_topics() {
        let responses: Vec<Value> =
            serde_json::from_str(include_str!("../../test_data/ws_rpc_responses_btc.json"))
                .unwrap();
        assert_eq!(responses.len(), 6);

        let result: RpcResult = serde_json::from_value(responses[5]["result"].clone()).unwrap();
        assert_eq!(result.method, "list-subscriptions");
        assert_eq!(result.status, "OK");
        assert_eq!(
            result.subscriptions.unwrap(),
            vec![
                "funding-rates.BTC-USD",
                "orderbooks.BTC-USD",
                "prices.index.BTC-USD",
                "prices.mark.BTC-USD",
                "trades.BTC-USD",
            ],
        );
    }
}
