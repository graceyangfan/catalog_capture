// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Public WebSocket envelope, heartbeat, subscription, and topic handling.

use anyhow::Context;
use serde::Deserialize;
use serde_json::{json, value::RawValue};

pub(crate) enum PredictWebSocketMessage<'a> {
    Heartbeat(&'a RawValue),
    Market {
        topic: String,
        data: Option<&'a RawValue>,
    },
    Informational,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    frame_type: Option<String>,
    topic: Option<String>,
    #[serde(borrow, default, deserialize_with = "raw_data")]
    data: Option<&'a RawValue>,
}

fn raw_data<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

pub(crate) fn parse_message(text: &str) -> anyhow::Result<PredictWebSocketMessage<'_>> {
    let envelope: Envelope<'_> =
        serde_json::from_str(text).context("invalid Predict WebSocket JSON")?;
    if envelope.topic.as_deref() == Some("heartbeat") {
        return Ok(PredictWebSocketMessage::Heartbeat(
            envelope.data.context("Predict heartbeat is missing data")?,
        ));
    }
    if envelope.frame_type.as_deref() != Some("M") {
        return Ok(PredictWebSocketMessage::Informational);
    }
    Ok(PredictWebSocketMessage::Market {
        topic: envelope
            .topic
            .context("Predict market frame is missing topic")?,
        data: envelope.data,
    })
}

pub(crate) fn heartbeat_message(data: &RawValue) -> String {
    format!(r#"{{"method":"heartbeat","data":{}}}"#, data.get())
}

pub(crate) fn subscription_message(method: &str, request_id: u64, market_id: u64) -> String {
    json!({"method": method, "requestId": request_id, "params": [format!("predictOrderbook/{market_id}")]}).to_string()
}

pub(crate) fn parse_topic_market_id(topic: &str) -> anyhow::Result<u64> {
    let market_id = topic
        .strip_prefix("predictOrderbook/")
        .context("unexpected Predict topic; expected predictOrderbook/{marketId}")?;
    anyhow::ensure!(
        !market_id.is_empty() && !market_id.contains('/'),
        "invalid Predict orderbook topic `{topic}`"
    );
    market_id
        .parse()
        .with_context(|| format!("invalid Predict orderbook topic `{topic}`"))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{PredictWebSocketMessage, heartbeat_message, parse_message, parse_topic_market_id};

    #[test]
    fn only_accepts_canonical_orderbook_topics() {
        assert_eq!(
            parse_topic_market_id("predictOrderbook/2859346").unwrap(),
            2_859_346
        );
        for topic in ["predictOrderbook/", "predictOrderbook/1/extra", "trades/1"] {
            assert!(
                parse_topic_market_id(topic).is_err(),
                "expected {topic} to fail"
            );
        }
    }

    #[test]
    fn heartbeat_preserves_timestamp_value_and_takes_precedence_over_type() {
        for data in [
            json!(1_791_849_600_123u64),
            json!("1791849600123"),
            json!(null),
        ] {
            let text = json!({"topic": "heartbeat", "data": data}).to_string();
            let PredictWebSocketMessage::Heartbeat(echo) = parse_message(&text).unwrap() else {
                panic!("heartbeat must echo")
            };
            let reply: Value = serde_json::from_str(&heartbeat_message(echo)).unwrap();
            assert_eq!(reply, json!({"method": "heartbeat", "data": data}));
        }
        assert!(parse_message(r#"{"type":"M","topic":"heartbeat"}"#).is_err());
    }

    #[test]
    fn ack_and_unrelated_market_frames_do_not_require_snapshot_data() {
        assert!(matches!(
            parse_message(r#"{"type":"R","requestId":1}"#).unwrap(),
            PredictWebSocketMessage::Informational
        ));
        let PredictWebSocketMessage::Market { topic, data } =
            parse_message(r#"{"type":"M","topic":"other/1"}"#).unwrap()
        else {
            panic!("market envelope")
        };
        assert_eq!(topic, "other/1");
        assert!(
            data.is_none(),
            "dispatch filters topics before interpreting data"
        );
        assert!(parse_message(r#"{"type":"M","data":{}}"#).is_err());
    }
    #[test]
    fn numeric_snapshot_amounts_preserve_venue_decimal_tokens() {
        use crate::PredictOrderbookWire;
        let wire: PredictOrderbookWire = serde_json::from_str(
            r#"{"marketId":3628808,"updateTimestampMs":1792638971588,"orderCount":1,
            "bids":[[0.44,155.420350877193]],"asks":[[0.99,3900.9999999999995]],
            "lastOrderSettled":{"id":"x","price":0.1234567890123456789,"kind":"LIMIT","marketId":3628808,"side":"Ask","outcome":"No"},
            "settlementsPending":{"asks":[[0.99,1.2345678901234567]],"bids":[]}}"#,
        ).unwrap();
        assert_eq!(wire.asks[0].1.to_string(), "3900.9999999999995");
        assert_eq!(wire.bids[0].1.to_string(), "155.420350877193");
        assert_eq!(
            wire.last_order_settled.unwrap().price.to_string(),
            "0.1234567890123456789"
        );
        assert_eq!(
            wire.settlements_pending.unwrap().asks[0].1.to_string(),
            "1.2345678901234567"
        );
    }

    #[test]
    fn borrowed_payload_preserves_complete_recording_fields() {
        use crate::{PredictMarketContext, PredictOrderbookSnapshot, PredictOrderbookWire};
        let body = include_str!("../../test_data/orderbook_2859346.json");
        let text = format!(r#"{{"type":"M","topic":"predictOrderbook/2859346","data":{body}}}"#);
        let PredictWebSocketMessage::Market {
            topic,
            data: Some(raw),
        } = parse_message(&text).unwrap()
        else {
            panic!("snapshot")
        };
        assert_eq!(topic, "predictOrderbook/2859346");
        assert!(
            raw.get().as_ptr() >= text.as_ptr()
                && raw.get().as_ptr() < text.as_ptr().wrapping_add(text.len())
        );
        let context = PredictMarketContext {
            market_id: 2859346,
            yes_instrument_id: crate::instrument_id_from_outcome(2859346, 1).unwrap(),
            no_instrument_id: crate::instrument_id_from_outcome(2859346, 2).unwrap(),
            price_precision: 2,
            size_precision: nautilus_model::types::fixed::FIXED_PRECISION,
        };
        let direct: PredictOrderbookWire = serde_json::from_str(raw.get()).unwrap();
        let mut previous: Value = serde_json::from_str(&text).unwrap();
        let previous: PredictOrderbookWire =
            serde_json::from_value(previous["data"].take()).unwrap();
        let snapshot = |wire| {
            serde_json::to_value(
                PredictOrderbookSnapshot::from_wire(&context, wire, 42.into()).unwrap(),
            )
            .unwrap()
        };
        let expected = snapshot(previous);
        assert!(!expected["bids"].as_array().unwrap().is_empty());
        assert!(!expected["last_order_settled"].is_null());
        assert!(!expected["settlements_pending"].is_null());
        assert_eq!(snapshot(direct), expected);
    }
}
