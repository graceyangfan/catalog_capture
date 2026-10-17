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
use serde_json::{Value, json};

pub(crate) enum PredictWebSocketMessage {
    Heartbeat(Value),
    Market { topic: String, data: Option<Value> },
    Informational,
}

pub(crate) fn parse_message(text: &str) -> anyhow::Result<PredictWebSocketMessage> {
    let mut value: Value = serde_json::from_str(text).context("invalid Predict WebSocket JSON")?;
    if value.get("topic").and_then(Value::as_str) == Some("heartbeat") {
        return Ok(PredictWebSocketMessage::Heartbeat(
            value
                .get_mut("data")
                .map(Value::take)
                .context("Predict heartbeat is missing data")?,
        ));
    }
    if value.get("type").and_then(Value::as_str) != Some("M") {
        return Ok(PredictWebSocketMessage::Informational);
    }
    let topic = value
        .get_mut("topic")
        .and_then(|topic| match topic.take() {
            Value::String(topic) => Some(topic),
            _ => None,
        })
        .context("Predict market frame is missing topic")?;
    Ok(PredictWebSocketMessage::Market {
        topic,
        data: value.get_mut("data").map(Value::take),
    })
}

pub(crate) fn heartbeat_message(data: Value) -> String {
    json!({"method": "heartbeat", "data": data}).to_string()
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
}
