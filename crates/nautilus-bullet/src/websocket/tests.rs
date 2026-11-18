// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use crate::websocket::{
    messages::{BulletWsMessage, decode_message},
    parse::{micros_to_nanos, parse_book, parse_prices, parse_trade},
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, Instrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use serde_json::{Value, json};

const BOOK: &[u8] = include_bytes!("../../test_data/ws_depth_snapshot.json");
const TRADE: &[u8] = include_bytes!("../../test_data/ws_agg_trade.json");
const MARK: &[u8] = include_bytes!("../../test_data/ws_mark_price.json");

// Synthetic instrument definitions; the metadata HTTP path is a separate implementation step.
fn instrument(base: &str, venue: &str) -> InstrumentAny {
    CryptoPerpetual::builder()
        .instrument_id(InstrumentId::from(
            format!("{base}-USD-PERP.{venue}").as_str(),
        ))
        .raw_symbol(Symbol::from(format!("{base}-USD").as_str()))
        .base_currency(Currency::get_or_create_crypto(base))
        .quote_currency(Currency::USD())
        .settlement_currency(Currency::USD())
        .is_inverse(false)
        .price_precision(2)
        .size_precision(3)
        .price_increment(Price::from("0.01"))
        .size_increment(Quantity::from("0.001"))
        .ts_event(UnixNanos::from(1))
        .ts_init(UnixNanos::from(1))
        .build()
        .unwrap()
        .into()
}

fn edited(bytes: &[u8], edit: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut payload: Value = serde_json::from_slice(bytes).unwrap();
    edit(&mut payload);
    serde_json::to_vec(&payload).unwrap()
}

#[test]
fn depth20_snapshot_admits_twenty_levels_and_rejects_a_deeper_profile() {
    for levels in [20, 21] {
        let bytes = edited(BOOK, |value| {
            value["b"] = json!(
                (0..levels)
                    .map(|i| [format!("{}", 50000 - i), "1".to_string()])
                    .collect::<Vec<_>>()
            );
        });
        let BulletWsMessage::Depth(message) = decode_message(&bytes).unwrap() else {
            panic!()
        };
        assert_eq!(
            parse_book(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1)).is_ok(),
            levels == 20
        );
    }
}

#[test]
fn documentation_snapshot_becomes_one_canonical_batch() {
    let BulletWsMessage::Depth(message) = decode_message(BOOK).unwrap() else {
        panic!()
    };
    let instrument = instrument("BTC", "BULLET");
    let batch = parse_book(&message, &instrument, UnixNanos::from(123))
        .unwrap()
        .unwrap();
    assert_eq!(batch.instrument_id, instrument.id());
    assert_eq!(batch.deltas.len(), 5);
    assert_eq!(batch.deltas[0].action, BookAction::Clear);
    assert_eq!(batch.deltas[1].order.side, Some(OrderSide::Buy));
    assert_eq!(batch.deltas[1].order.price, Price::from("50000.00"));
    assert_eq!(batch.deltas[1].order.size, Quantity::from("1.500"));
    assert_eq!(batch.deltas[3].order.side, Some(OrderSide::Sell));
    for (i, row) in batch.deltas.iter().enumerate() {
        assert_eq!(row.sequence, message.last_update_id);
        assert_eq!(row.ts_event.as_u64(), 1_706_745_600_000_000_000);
        assert_eq!(row.ts_init.as_u64(), 123);
        assert_ne!(row.flags & RecordFlag::F_SNAPSHOT as u8, 0);
        assert_eq!(row.flags & RecordFlag::F_LAST as u8 != 0, i == 4);
    }
}

#[test]
fn synthetic_delta_uses_absolute_size_and_chain_time() {
    let bytes = edited(BOOK, |v| {
        v["mt"] = json!("u");
        v["U"] = json!(1001);
        v["u"] = json!(1001);
        v["pu"] = json!(1000);
        v["E"] = json!(1706745600000999_u64);
        v["b"] = json!([["50000.00", "0"], ["49999.00", "0.250"]]);
        v["a"] = json!([]);
    });
    let BulletWsMessage::Depth(message) = decode_message(&bytes).unwrap() else {
        panic!()
    };
    let batch = parse_book(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1))
        .unwrap()
        .unwrap();
    assert_eq!(batch.deltas.len(), 2);
    assert_eq!(batch.deltas[0].action, BookAction::Delete);
    assert_eq!(batch.deltas[1].action, BookAction::Update);
    assert_eq!(batch.deltas[1].order.size, Quantity::from("0.250"));
    assert_eq!(batch.deltas[1].ts_event.as_u64(), 1_706_745_600_000_000_000);
    assert_eq!(batch.deltas[0].flags, 0);
    assert_eq!(batch.deltas[1].flags, RecordFlag::F_LAST as u8);
}

#[test]
fn synthetic_empty_update_is_not_a_clear_but_empty_snapshot_is() {
    for snapshot in [false, true] {
        let bytes = edited(BOOK, |v| {
            v["mt"] = json!(if snapshot { "s" } else { "u" });
            v["b"] = json!([]);
            v["a"] = json!([]);
        });
        let BulletWsMessage::Depth(message) = decode_message(&bytes).unwrap() else {
            panic!()
        };
        let batch = parse_book(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1)).unwrap();
        if snapshot {
            let batch = batch.unwrap();
            assert_eq!(batch.deltas.len(), 1);
            assert_eq!(batch.deltas[0].action, BookAction::Clear);
            assert_eq!(
                batch.deltas[0].flags,
                RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
            );
        } else {
            assert!(batch.is_none());
        }
    }
}

#[test]
fn rejects_synthetic_invalid_book_frames_without_returning_a_partial_batch() {
    let cases = [
        ("zero snapshot", json!([["50000.00", "0"]])),
        ("negative size", json!([["50000.00", "-1"]])),
        ("excess size precision", json!([["50000.00", "0.0001"]])),
        ("excess price precision", json!([["50000.001", "1"]])),
        ("bad second level", json!([["50000.00", "1"], ["bad", "1"]])),
    ];
    for (label, levels) in cases {
        let bytes = edited(BOOK, |v| v["b"] = levels);
        let BulletWsMessage::Depth(message) = decode_message(&bytes).unwrap() else {
            panic!()
        };
        assert!(
            parse_book(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1)).is_err(),
            "{label}"
        );
    }
    let bytes = edited(BOOK, |v| v["u"] = json!(1002));
    let BulletWsMessage::Depth(message) = decode_message(&bytes).unwrap() else {
        panic!()
    };
    assert!(parse_book(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1)).is_err());
}

#[test]
fn upstream_captured_trade_preserves_values_and_ignores_deprecated_side() {
    let BulletWsMessage::AggTrade(message) = decode_message(TRADE).unwrap() else {
        panic!()
    };
    let tick = parse_trade(&message, &instrument("SOL", "BULLET"), UnixNanos::from(9)).unwrap();
    assert_eq!(tick.trade_id.as_str(), "13205413");
    assert_eq!(tick.price, Price::from("85.65"));
    assert_eq!(tick.size, Quantity::from("0.010"));
    assert_eq!(tick.aggressor_side, AggressorSide::Sell);
    assert_eq!(tick.ts_event.as_u64(), 1_779_600_413_211_110_000);
    assert_eq!(tick.ts_init.as_u64(), 9);
}

#[test]
fn synthetic_trade_retains_max_u64_id_and_buyer_aggressor() {
    let bytes = edited(TRADE, |v| {
        v["a"] = json!(u64::MAX);
        v["m"] = json!(false);
        v["sd"] = json!("SELL");
    });
    let BulletWsMessage::AggTrade(message) = decode_message(&bytes).unwrap() else {
        panic!()
    };
    let tick = parse_trade(&message, &instrument("SOL", "BULLET"), UnixNanos::from(1)).unwrap();
    assert_eq!(tick.trade_id.as_str(), "18446744073709551615");
    assert_eq!(tick.aggressor_side, AggressorSide::Buy);
}

#[test]
fn rejects_synthetic_zero_negative_or_rounded_trade_quantities() {
    for quantity in ["0", "-0.1", "0.0001"] {
        let bytes = edited(TRADE, |v| v["q"] = json!(quantity));
        let BulletWsMessage::AggTrade(message) = decode_message(&bytes).unwrap() else {
            panic!()
        };
        assert!(parse_trade(&message, &instrument("SOL", "BULLET"), UnixNanos::from(1)).is_err());
    }
}

#[test]
fn mark_and_index_use_publication_time_not_next_funding_time() {
    let BulletWsMessage::MarkPrice(message) = decode_message(MARK).unwrap() else {
        panic!()
    };
    let prices = parse_prices(&message, &instrument("BTC", "BULLET"), UnixNanos::from(11)).unwrap();
    assert_eq!(prices.mark.value, Price::from("50000.50"));
    assert_eq!(prices.index.value, Price::from("50000.00"));
    assert_eq!(prices.mark.ts_event.as_u64(), 1_706_745_600_000_000_000);
    assert_eq!(prices.index.ts_event, prices.mark.ts_event);
    assert_eq!(prices.index.ts_init.as_u64(), 11);
    assert_eq!(message.funding_rate, "0.0001");
    assert_eq!(message.next_funding_time, Some(1706774400000000));
    assert!(message.mark_execution_timestamp_us.is_none());
    assert!(message.mark_event_number.is_none());
}

#[test]
fn synthetic_mark_execution_identity_survives_without_rewriting_publication_time() {
    let bytes = edited(MARK, |v| {
        v["mark_execution_timestamp_us"] = json!(1706745599999999_u64);
        v["mark_event_number"] = json!("18446744073709551615");
        v["th"] = json!("0xmark");
    });
    let BulletWsMessage::MarkPrice(message) = decode_message(&bytes).unwrap() else {
        panic!()
    };
    assert_eq!(
        message.mark_event_number.as_deref(),
        Some("18446744073709551615")
    );
    assert_eq!(message.mark_execution_timestamp_us, Some(1706745599999999));
    let prices = parse_prices(&message, &instrument("BTC", "BULLET"), UnixNanos::from(1)).unwrap();
    assert_eq!(prices.mark.ts_event.as_u64(), 1_706_745_600_000_000_000);
}

#[test]
fn rejects_wrong_symbol_or_venue_for_each_family() {
    let BulletWsMessage::Depth(book) = decode_message(BOOK).unwrap() else {
        panic!()
    };
    let BulletWsMessage::MarkPrice(mark) = decode_message(MARK).unwrap() else {
        panic!()
    };
    let BulletWsMessage::AggTrade(trade) = decode_message(TRADE).unwrap() else {
        panic!()
    };
    for wrong in [instrument("ETH", "BULLET"), instrument("BTC", "EXTENDED")] {
        assert!(parse_book(&book, &wrong, UnixNanos::from(1)).is_err());
        assert!(parse_prices(&mark, &wrong, UnixNanos::from(1)).is_err());
    }
    for wrong in [instrument("ETH", "BULLET"), instrument("SOL", "EXTENDED")] {
        assert!(parse_trade(&trade, &wrong, UnixNanos::from(1)).is_err());
    }
}

#[test]
fn rejects_synthetic_shape_compatible_wrong_event_and_malformed_levels() {
    for tag in ["bookTicker", "unknown", "aggTrade"] {
        assert!(decode_message(&edited(BOOK, |v| v["e"] = json!(tag))).is_err());
    }
    for level in [json!(["1"]), json!(["1", "2", "3"]), json!([1, "2"])] {
        assert!(decode_message(&edited(BOOK, |v| v["b"] = json!([level]))).is_err());
    }
    assert!(decode_message(&edited(BOOK, |v| v["mt"] = json!("invalid"))).is_err());
}

#[test]
fn error_envelopes_take_precedence_over_success_shape() {
    // Synthetic tagged and untagged errors, including a deliberately misleading success tag.
    for tag in [None, Some("error"), Some("subscribe")] {
        let mut value = json!({"E":1706745600000000_u64,"id":7,"result":"success","error":{"code":-1004,"msg":"invalid topic","param":"bad"}});
        if let Some(tag) = tag {
            value["e"] = json!(tag);
        }
        let BulletWsMessage::Error(error) =
            decode_message(&serde_json::to_vec(&value).unwrap()).unwrap()
        else {
            panic!()
        };
        assert_eq!(error.id, Some(7));
        assert_eq!(error.error.code, -1004);
    }
}

#[test]
fn official_upstream_subscribe_ack_has_a_result_not_a_topic_identity() {
    let bytes = br#"{"e":"subscribe","id":1,"E":1779600272876932,"result":"success"}"#;
    let BulletWsMessage::Subscribe(ack) = decode_message(bytes).unwrap() else {
        panic!()
    };
    assert_eq!(ack.id, Some(1));
    assert_eq!(ack.result, "success");
}

#[test]
fn timestamp_conversion_is_checked_and_exact() {
    assert_eq!(micros_to_nanos(1).unwrap().as_u64(), 1000);
    assert_eq!(
        micros_to_nanos(u64::MAX / 1000).unwrap().as_u64(),
        u64::MAX / 1000 * 1000
    );
    assert!(micros_to_nanos(u64::MAX / 1000 + 1).is_err());
}

#[test]
fn captured_mainnet_oracle_rounds_half_even_at_metadata_precision() {
    let events: Vec<Value> =
        serde_json::from_slice(include_bytes!("../../test_data/ws_mainnet_btc_probe.json"))
            .unwrap();
    let mark = events.iter().find(|v| v["e"] == "markPriceUpdate").unwrap();
    let BulletWsMessage::MarkPrice(message) =
        decode_message(&serde_json::to_vec(mark).unwrap()).unwrap()
    else {
        panic!()
    };
    let btc = crate::http::tests::btc();
    let result = parse_prices(&message, &btc, UnixNanos::from(7)).unwrap();
    let mark_decimal = rust_decimal::Decimal::from_str_exact(&message.mark_price)
        .unwrap()
        .round_dp(8);
    assert_eq!(
        result.mark.value,
        Price::from_decimal_dp(mark_decimal, 8).unwrap()
    );
    assert_eq!(result.mark.value.precision, 8);
}
