// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        BookOrder, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta,
        OrderBookDeltas, TradeTick,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::TradeId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::messages::StreamEnvelope;

const SNAPSHOT: &str = "ORDERBOOKS.SNAPSHOT";
const DELTA: &str = "ORDERBOOKS.DELTA";
const TRADES: &str = "TRADES";
const MARK_PRICE: &str = "PRICES.MP";
const INDEX_PRICE: &str = "PRICES.IP";
const FUNDING_RATE: &str = "FUNDING_RATES";

#[derive(Debug)]
pub enum ParsedMessage {
    Book(OrderBookDeltas),
    Trades(Vec<TradeTick>),
    MarkPrice(MarkPriceUpdate),
    IndexPrice(IndexPriceUpdate),
    FundingRate(FundingRateUpdate),
}

pub fn parse_stream_message(
    envelope: &StreamEnvelope,
    instrument: &InstrumentAny,
    book_sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<ParsedMessage>> {
    match envelope.message_type.as_str() {
        SNAPSHOT | DELTA => parse_book(envelope, instrument, book_sequence, ts_init)
            .map(|value| value.map(ParsedMessage::Book)),
        TRADES => parse_trades(envelope, instrument, ts_init)
            .map(|trades| Some(ParsedMessage::Trades(trades))),
        MARK_PRICE => {
            parse_price(envelope, instrument, ts_init, true).map(|value| Some(value.into()))
        }
        INDEX_PRICE => {
            parse_price(envelope, instrument, ts_init, false).map(|value| Some(value.into()))
        }
        FUNDING_RATE => parse_funding(envelope, instrument, ts_init)
            .map(|value| Some(ParsedMessage::FundingRate(value))),
        other => anyhow::bail!("unsupported Extended stream type `{other}`"),
    }
}

fn parse_book(
    envelope: &StreamEnvelope,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<OrderBookDeltas>> {
    let data: BookData = serde_json::from_value(envelope.data.clone())
        .context("invalid Extended order-book payload")?;
    validate_market(&data.market, instrument)?;
    anyhow::ensure!(data.depth == "f", "Extended order book is not full depth");
    let ts_event = millis_to_nanos(envelope.ts)?;
    let is_snapshot = envelope.message_type == SNAPSHOT;

    if !is_snapshot && data.bids.is_empty() && data.asks.is_empty() {
        return Ok(None);
    }

    let instrument_id = instrument.id();
    let mut deltas =
        Vec::with_capacity(data.bids.len() + data.asks.len() + usize::from(is_snapshot));
    if is_snapshot {
        deltas.push(OrderBookDelta::clear(
            instrument_id,
            sequence,
            ts_event,
            ts_init,
        ));
    }

    append_levels(
        &mut deltas,
        data.bids,
        OrderSide::Buy,
        is_snapshot,
        instrument,
        sequence,
        ts_event,
        ts_init,
    )?;
    append_levels(
        &mut deltas,
        data.asks,
        OrderSide::Sell,
        is_snapshot,
        instrument,
        sequence,
        ts_event,
        ts_init,
    )?;

    let last = deltas
        .last_mut()
        .context("Extended order-book frame produced no deltas")?;
    last.flags |= RecordFlag::F_LAST as u8;
    OrderBookDeltas::new_checked(instrument_id, deltas)
        .map(Some)
        .context("invalid Extended order-book batch")
}

#[expect(clippy::too_many_arguments)]
fn append_levels(
    deltas: &mut Vec<OrderBookDelta>,
    levels: Vec<BookLevel>,
    side: OrderSide,
    is_snapshot: bool,
    instrument: &InstrumentAny,
    sequence: u64,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<()> {
    for level in levels {
        let absolute = if is_snapshot {
            level.change
        } else {
            level
                .current
                .context("Extended delta level is missing absolute quantity `c`")?
        };
        anyhow::ensure!(
            absolute >= Decimal::ZERO,
            "negative Extended absolute quantity"
        );
        if is_snapshot {
            anyhow::ensure!(absolute > Decimal::ZERO, "zero Extended snapshot quantity");
        }

        let price = Price::from_decimal_dp(level.price, instrument.price_precision())
            .context("invalid Extended book price")?;
        let size = Quantity::from_decimal_dp(absolute, instrument.size_precision())
            .context("invalid Extended book quantity")?;
        let action = if is_snapshot {
            BookAction::Add
        } else if absolute.is_zero() {
            BookAction::Delete
        } else {
            BookAction::Update
        };
        let flags = if is_snapshot {
            RecordFlag::F_SNAPSHOT as u8
        } else {
            0
        };
        deltas.push(OrderBookDelta::new_checked(
            instrument.id(),
            action,
            BookOrder::new(side, price, size, 0),
            flags,
            sequence,
            ts_event,
            ts_init,
        )?);
    }
    Ok(())
}

fn parse_trades(
    envelope: &StreamEnvelope,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<Vec<TradeTick>> {
    let rows: Vec<TradeData> =
        serde_json::from_value(envelope.data.clone()).context("invalid Extended trades payload")?;
    rows.into_iter()
        .map(|row| {
            validate_market(&row.market, instrument)?;
            let aggressor_side = match row.side.as_str() {
                "BUY" => AggressorSide::Buy,
                "SELL" => AggressorSide::Sell,
                other => anyhow::bail!("invalid Extended trade side `{other}`"),
            };
            let price = Price::from_decimal_dp(row.price, instrument.price_precision())?;
            let quantity = Quantity::from_decimal_dp(row.quantity, instrument.size_precision())?;
            TradeTick::new_checked(
                instrument.id(),
                price,
                quantity,
                aggressor_side,
                TradeId::new_checked(row.trade_id.to_string())?,
                millis_to_nanos(row.timestamp)?,
                ts_init,
            )
            .context("invalid Extended trade tick")
        })
        .collect()
}

fn parse_price(
    envelope: &StreamEnvelope,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
    is_mark: bool,
) -> anyhow::Result<MarkOrIndex> {
    let data: PriceData =
        serde_json::from_value(envelope.data.clone()).context("invalid Extended price payload")?;
    validate_market(&data.market, instrument)?;
    let price = Price::from_decimal_dp(data.price, instrument.price_precision())?;
    let ts_event = millis_to_nanos(if data.timestamp > 0 {
        data.timestamp
    } else {
        envelope.ts
    })?;
    if is_mark {
        Ok(MarkOrIndex::Mark(MarkPriceUpdate::new(
            instrument.id(),
            price,
            ts_event,
            ts_init,
        )))
    } else {
        Ok(MarkOrIndex::Index(IndexPriceUpdate::new(
            instrument.id(),
            price,
            ts_event,
            ts_init,
        )))
    }
}

fn parse_funding(
    envelope: &StreamEnvelope,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<FundingRateUpdate> {
    let data: FundingData = serde_json::from_value(envelope.data.clone())
        .context("invalid Extended funding payload")?;
    validate_market(&data.market, instrument)?;
    Ok(FundingRateUpdate::new(
        instrument.id(),
        data.rate,
        Some(60),
        None,
        millis_to_nanos(data.timestamp)?,
        ts_init,
    ))
}

#[derive(Debug)]
enum MarkOrIndex {
    Mark(MarkPriceUpdate),
    Index(IndexPriceUpdate),
}

impl From<MarkOrIndex> for ParsedMessage {
    fn from(value: MarkOrIndex) -> Self {
        match value {
            MarkOrIndex::Mark(value) => Self::MarkPrice(value),
            MarkOrIndex::Index(value) => Self::IndexPrice(value),
        }
    }
}

fn validate_market(market: &str, instrument: &InstrumentAny) -> anyhow::Result<()> {
    anyhow::ensure!(
        market == instrument.raw_symbol().as_str(),
        "Extended payload market `{market}` does not match instrument {}",
        instrument.id(),
    );
    Ok(())
}

fn millis_to_nanos(value: u64) -> anyhow::Result<UnixNanos> {
    value
        .checked_mul(1_000_000)
        .map(UnixNanos::from)
        .context("Extended millisecond timestamp overflow")
}

#[derive(Debug, Deserialize)]
struct BookData {
    #[serde(rename = "m")]
    market: String,
    #[serde(rename = "b")]
    bids: Vec<BookLevel>,
    #[serde(rename = "a")]
    asks: Vec<BookLevel>,
    #[serde(rename = "d")]
    depth: String,
}

#[derive(Debug, Deserialize)]
struct BookLevel {
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "q")]
    change: Decimal,
    #[serde(rename = "c")]
    current: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
struct TradeData {
    #[serde(rename = "i")]
    trade_id: u64,
    #[serde(rename = "m")]
    market: String,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "T")]
    timestamp: u64,
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "q")]
    quantity: Decimal,
}

#[derive(Debug, Deserialize)]
struct PriceData {
    #[serde(rename = "m")]
    market: String,
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "ts")]
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct FundingData {
    #[serde(rename = "m")]
    market: String,
    #[serde(rename = "f")]
    rate: Decimal,
    #[serde(rename = "T")]
    timestamp: u64,
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{BookAction, RecordFlag},
        identifiers::{InstrumentId, Symbol, Venue},
        instruments::CryptoPerpetual,
        types::Currency,
    };

    use super::*;

    const BOOK_SNAPSHOT: &str = include_str!("../../test_data/ws_orderbook_snapshot_btc.json");
    const BOOK_DELTA: &str = include_str!("../../test_data/ws_orderbook_delta_btc.json");
    const TRADES_REPLAY: &str = include_str!("../../test_data/ws_trades_replay_btc.json");
    const MARK_PRICE: &str = include_str!("../../test_data/ws_mark_price_btc.json");
    const INDEX_PRICE: &str = include_str!("../../test_data/ws_index_price_btc.json");
    const FUNDING_RATE: &str = include_str!("../../test_data/ws_funding_rate_btc.json");

    fn instrument() -> InstrumentAny {
        InstrumentAny::CryptoPerpetual(
            CryptoPerpetual::builder()
                .instrument_id(InstrumentId::new(
                    Symbol::new("BTC-USD-PERP"),
                    Venue::new("EXTENDED"),
                ))
                .raw_symbol(Symbol::new("BTC-USD"))
                .base_currency(Currency::from("BTC"))
                .quote_currency(Currency::from("USD"))
                .settlement_currency(Currency::from("USD"))
                .is_inverse(false)
                .price_precision(0)
                .size_precision(5)
                .price_increment(Price::from("1"))
                .size_increment(Quantity::from("0.00001"))
                .ts_event(UnixNanos::from(0))
                .ts_init(UnixNanos::from(0))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn mainnet_snapshot_is_one_clear_add_batch() {
        let envelope: StreamEnvelope = serde_json::from_str(BOOK_SNAPSHOT).unwrap();
        let Some(ParsedMessage::Book(batch)) =
            parse_stream_message(&envelope, &instrument(), 42, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected book batch");
        };
        assert_eq!(envelope.seq, 0);
        assert_eq!(batch.deltas.len(), 6_036);
        assert!(batch.deltas.iter().all(|delta| delta.sequence == 42));
        assert_eq!(batch.deltas[0].action, BookAction::Clear);
        assert_eq!(batch.deltas[1].action, BookAction::Add);
        assert_eq!(batch.deltas[1].order.price, Price::from("84077"));
        assert_eq!(batch.deltas[1].order.size, Quantity::from("0.41547"));
        assert_ne!(
            batch.deltas.last().unwrap().flags & RecordFlag::F_LAST as u8,
            0,
        );
    }

    #[test]
    fn mainnet_delta_uses_absolute_current_quantity() {
        let envelope: StreamEnvelope = serde_json::from_str(BOOK_DELTA).unwrap();
        let Some(ParsedMessage::Book(batch)) =
            parse_stream_message(&envelope, &instrument(), 43, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected book batch");
        };
        assert_eq!(envelope.seq, 1);
        assert_eq!(batch.deltas.len(), 8);
        assert!(batch.deltas.iter().all(|delta| delta.sequence == 43));
        assert_eq!(batch.deltas[0].action, BookAction::Update);
        assert_eq!(batch.deltas[0].order.price, Price::from("84074"));
        assert_eq!(batch.deltas[0].order.size, Quantity::from("0.00118"));
    }

    #[test]
    fn mainnet_trade_replay_preserves_large_trade_ids() {
        let envelope: StreamEnvelope = serde_json::from_str(TRADES_REPLAY).unwrap();
        let Some(ParsedMessage::Trades(trades)) =
            parse_stream_message(&envelope, &instrument(), 0, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected trades");
        };
        assert_eq!(trades.len(), 50);
        assert_eq!(trades[0].trade_id.as_str(), "2103821900219158529");
        assert_eq!(trades[0].price, Price::from("84118"));
        assert_eq!(trades[0].size, Quantity::from("0.00237"));
        assert_eq!(
            trades[0].ts_event,
            UnixNanos::from(1_790_425_202_790_000_000)
        );
    }

    #[test]
    fn mainnet_mark_and_index_use_documented_timestamp_fallback() {
        let mark: StreamEnvelope = serde_json::from_str(MARK_PRICE).unwrap();
        let Some(ParsedMessage::MarkPrice(mark)) =
            parse_stream_message(&mark, &instrument(), 0, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected mark price");
        };
        assert_eq!(mark.value, Price::from("84058"));
        assert_eq!(mark.ts_event, UnixNanos::from(1_790_425_443_253_000_000));

        let index: StreamEnvelope = serde_json::from_str(INDEX_PRICE).unwrap();
        let Some(ParsedMessage::IndexPrice(index)) =
            parse_stream_message(&index, &instrument(), 0, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected index price");
        };
        assert_eq!(index.value, Price::from("84105"));
        assert_eq!(index.ts_event, UnixNanos::from(1_790_425_443_000_000_000));
    }

    #[test]
    fn mainnet_funding_rate_maps_to_standard_update() {
        let envelope: StreamEnvelope = serde_json::from_str(FUNDING_RATE).unwrap();
        let Some(ParsedMessage::FundingRate(funding)) =
            parse_stream_message(&envelope, &instrument(), 0, UnixNanos::from(2)).unwrap()
        else {
            panic!("expected funding rate");
        };
        assert_eq!(funding.rate, Decimal::new(13, 6));
        assert_eq!(funding.interval, Some(60));
        assert_eq!(funding.next_funding_ns, None);
        assert_eq!(funding.ts_event, UnixNanos::from(1_790_426_050_286_000_000));
    }

    #[test]
    fn delta_without_absolute_quantity_fails_closed() {
        let envelope: StreamEnvelope = serde_json::from_str(
            r#"{"type":"ORDERBOOKS.DELTA","data":{"m":"BTC-USD","b":[{"p":"100.0","q":"-1.0"}],"a":[],"d":"f"},"ts":1000,"seq":1,"subscription":"orderbooks.BTC-USD"}"#,
        )
        .unwrap();
        assert!(parse_stream_message(&envelope, &instrument(), 7, UnixNanos::from(2)).is_err());
    }
}
