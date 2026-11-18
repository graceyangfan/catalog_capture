// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Wire names follow the official Rust SDK; decimal tokens remain strings until conversion.

use anyhow::Context;
use serde::Deserialize;

/// Snapshot/update discriminator shared by the Bullet depth profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum BulletMessageType {
    #[serde(rename = "s")]
    Snapshot,
    #[serde(rename = "u")]
    Update,
}

/// One price level, encoded as exactly two decimal strings: `[price, quantity]`.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletPriceLevel(pub String, pub String);

/// A depth snapshot or absolute-quantity update for the subscribed depth profile.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletDepthUpdate {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "T")]
    pub transaction_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "U")]
    pub first_update_id: u64,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(rename = "pu")]
    pub prev_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<BulletPriceLevel>,
    #[serde(rename = "a")]
    pub asks: Vec<BulletPriceLevel>,
    #[serde(rename = "mt")]
    pub message_type: BulletMessageType,
}

/// Public aggregate trade. Deprecated fill/account placeholders are deliberately ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletAggTrade {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "f")]
    pub first_trade_id: u64,
    #[serde(rename = "l")]
    pub last_trade_id: u64,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
    #[serde(rename = "th")]
    pub tx_hash: String,
}

/// Mark/index publication; funding is retained without assuming settlement semantics.
#[derive(Debug, Clone, Deserialize)]
pub struct BulletMarkPrice {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
    #[serde(rename = "i")]
    pub index_price: String,
    #[serde(rename = "r")]
    pub funding_rate: String,
    #[serde(rename = "T")]
    pub next_funding_time: Option<u64>,
    #[serde(rename = "th")]
    pub tx_hash: Option<String>,
    pub mark_execution_timestamp_us: Option<u64>,
    pub mark_event_number: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulletMethodResult {
    pub id: Option<u64>,
    #[serde(rename = "E")]
    pub event_time: u64,
    pub result: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulletError {
    pub code: i64,
    pub msg: String,
    pub param: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulletErrorMessage {
    pub id: Option<u64>,
    #[serde(rename = "E")]
    pub event_time: u64,
    pub error: BulletError,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulletStatus {
    #[serde(rename = "E")]
    pub event_time: u64,
    pub status: String,
    pub client_id: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BulletPong {
    pub id: Option<u64>,
    #[serde(rename = "E")]
    pub event_time: u64,
}

/// Exact `e` dispatch prevents shape-compatible messages being routed to another family.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "e")]
pub enum BulletWsMessage {
    #[serde(rename = "depthUpdate")]
    Depth(BulletDepthUpdate),
    #[serde(rename = "aggTrade")]
    AggTrade(BulletAggTrade),
    #[serde(rename = "markPriceUpdate")]
    MarkPrice(BulletMarkPrice),
    #[serde(rename = "subscribe")]
    Subscribe(BulletMethodResult),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(BulletMethodResult),
    #[serde(rename = "status")]
    Status(BulletStatus),
    #[serde(rename = "pong")]
    Pong(BulletPong),
    #[serde(rename = "error")]
    Error(BulletErrorMessage),
}

/// Decode errors before success/data dispatch, including errors without an `e` tag.
pub fn decode_message(bytes: &[u8]) -> anyhow::Result<BulletWsMessage> {
    #[derive(Deserialize)]
    struct ErrorProbe {
        #[serde(default)]
        error: Option<serde_json::Value>,
    }
    let probe: ErrorProbe = serde_json::from_slice(bytes).context("invalid Bullet WS JSON")?;
    if probe.error.is_some() {
        return serde_json::from_slice(bytes)
            .map(BulletWsMessage::Error)
            .context("invalid Bullet WS error");
    }
    serde_json::from_slice(bytes).context("unsupported or malformed Bullet WS message")
}

/// Logical public family; mark and index share one physical topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BulletDataKind {
    OrderBook,
    Trades,
    MarkPrice,
    IndexPrice,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BulletTopic {
    pub kind: BulletDataKind,
    pub market: ustr::Ustr,
}
impl BulletTopic {
    pub fn new(
        kind: BulletDataKind,
        id: nautilus_model::identifiers::InstrumentId,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            kind,
            market: ustr::Ustr::from(&crate::common::market_from_instrument_id(id)?),
        })
    }
    pub fn key(&self) -> String {
        match self.kind {
            BulletDataKind::OrderBook => {
                format!("{}@depth{}", self.market, crate::common::BULLET_BOOK_DEPTH)
            }
            BulletDataKind::Trades => format!("{}@aggTrade", self.market),
            BulletDataKind::MarkPrice | BulletDataKind::IndexPrice => {
                format!("{}@markPrice", self.market)
            }
        }
    }
    pub fn instrument_id(&self) -> anyhow::Result<nautilus_model::identifiers::InstrumentId> {
        crate::common::instrument_id_from_market(self.market.as_str())
    }
    pub fn logical_key(&self) -> String {
        format!("{}@{:?}", self.market, self.kind)
    }
}
#[derive(Debug)]
pub enum BulletCommand {
    Subscribe(BulletTopic),
    Unsubscribe(BulletTopic),
}
