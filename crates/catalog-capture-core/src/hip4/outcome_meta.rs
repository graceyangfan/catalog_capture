// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use rust_decimal::Decimal;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHip4Market {
    pub question_id: u32,
    pub question_name: Option<String>,
    pub market_class: Option<String>,
    pub underlying: Option<String>,
    pub period: Option<String>,
    pub outcome_ids: Vec<u32>,
    pub instrument_ids: Vec<String>,
    pub expiration_ns: u64,
    pub start_price: Option<Decimal>,
    pub price_thresholds: Vec<Decimal>,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct ResolveHip4MarketOptions<'a> {
    pub underlying: &'a str,
    pub period: &'a str,
    pub market_class: &'a str,
    pub include_fallback: bool,
    pub now_ns: u64,
}

pub fn resolve_hip4_market(
    payload: &Value,
    options: &ResolveHip4MarketOptions<'_>,
) -> Result<ResolvedHip4Market> {
    let mut candidates = Vec::new();
    if let Some(questions) = payload.get("questions").and_then(Value::as_array) {
        candidates.extend(resolve_question_candidates(
            questions,
            options.underlying,
            options.period,
            options.market_class,
            options.include_fallback,
        )?);
    }
    if let Some(outcomes) = payload.get("outcomes").and_then(Value::as_array) {
        candidates.extend(resolve_outcome_candidates(
            outcomes,
            options.underlying,
            options.period,
            options.market_class,
        )?);
    }

    if candidates.is_empty() {
        bail!(
            "no HIP-4 market found for underlying={:?}, period={:?}, market_class={:?}",
            options.underlying,
            options.period,
            options.market_class
        );
    }

    let now_ns = options.now_ns;
    let future: Vec<_> = candidates
        .iter()
        .filter(|item| item.expiration_ns > 0 && item.expiration_ns >= now_ns)
        .cloned()
        .collect();
    if !future.is_empty() {
        return future
            .into_iter()
            .min_by_key(|item| (item.expiration_ns, item.question_id))
            .ok_or_else(|| anyhow::anyhow!("failed to select nearest future HIP-4 market"));
    }

    let with_expiry: Vec<_> = candidates
        .iter()
        .filter(|item| item.expiration_ns > 0)
        .cloned()
        .collect();
    if !with_expiry.is_empty() {
        return with_expiry
            .into_iter()
            .max_by_key(|item| (item.expiration_ns, item.question_id))
            .ok_or_else(|| anyhow::anyhow!("failed to select latest past HIP-4 market"));
    }

    candidates
        .into_iter()
        .max_by_key(|item| item.question_id)
        .ok_or_else(|| anyhow::anyhow!("failed to select HIP-4 market by question_id"))
}

fn resolve_question_candidates(
    questions: &[Value],
    underlying: &str,
    period: &str,
    market_class: &str,
    include_fallback: bool,
) -> Result<Vec<ResolvedHip4Market>> {
    let mut candidates = Vec::new();
    for raw in questions {
        let Some(question_id) = raw.get("question").and_then(Value::as_u64) else {
            continue;
        };
        let description = raw
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let fields = parse_description_fields(&description);
        if !matches_hip4_market_filter(&fields, underlying, period, market_class) {
            continue;
        }

        let named_outcomes = raw
            .get("namedOutcomes")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_u64)
                    .map(|value| value as u32)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if named_outcomes.is_empty() {
            continue;
        }

        let mut outcome_ids = named_outcomes;
        if include_fallback {
            if let Some(fallback) = raw.get("fallbackOutcome").and_then(Value::as_u64) {
                outcome_ids.push(fallback as u32);
            }
        }

        candidates.push(build_resolved_hip4_market(
            question_id as u32,
            description,
            &fields,
            outcome_ids,
            raw.get("name").and_then(Value::as_str).map(str::to_string),
        ));
    }
    Ok(candidates)
}

fn resolve_outcome_candidates(
    outcomes: &[Value],
    underlying: &str,
    period: &str,
    market_class: &str,
) -> Result<Vec<ResolvedHip4Market>> {
    let mut candidates = Vec::new();
    for raw in outcomes {
        let Some(outcome_id) = raw.get("outcome").and_then(Value::as_u64) else {
            continue;
        };
        let description = raw
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let fields = parse_description_fields(&description);
        if !matches_hip4_market_filter(&fields, underlying, period, market_class) {
            continue;
        }

        let outcome_ids = vec![outcome_id as u32];
        candidates.push(build_resolved_hip4_market(
            outcome_id as u32,
            description,
            &fields,
            outcome_ids,
            raw.get("name").and_then(Value::as_str).map(str::to_string),
        ));
    }
    Ok(candidates)
}

fn matches_hip4_market_filter(
    fields: &BTreeMap<String, String>,
    underlying: &str,
    period: &str,
    market_class: &str,
) -> bool {
    fields.get("underlying").map(String::as_str) == Some(underlying)
        && fields.get("period").map(String::as_str) == Some(period)
        && fields.get("class").map(String::as_str) == Some(market_class)
}

fn build_resolved_hip4_market(
    question_id: u32,
    description: String,
    fields: &BTreeMap<String, String>,
    outcome_ids: Vec<u32>,
    question_name: Option<String>,
) -> ResolvedHip4Market {
    ResolvedHip4Market {
        question_id,
        question_name,
        market_class: fields.get("class").cloned(),
        underlying: fields.get("underlying").cloned(),
        period: fields.get("period").cloned(),
        outcome_ids: outcome_ids.clone(),
        instrument_ids: instrument_ids_from_outcomes(&outcome_ids),
        expiration_ns: parse_expiry_to_ns(fields.get("expiry").map(String::as_str).unwrap_or(""))
            .unwrap_or(0),
        start_price: resolve_start_price(fields),
        price_thresholds: parse_price_thresholds(fields.get("priceThresholds").map(String::as_str)),
        description,
    }
}

pub fn instrument_ids_from_outcomes(outcome_ids: &[u32]) -> Vec<String> {
    outcome_ids
        .iter()
        .flat_map(|outcome_id| {
            ["YES", "NO"]
                .into_iter()
                .map(move |side| format!("{outcome_id}-{side}-OUTCOME.HYPERLIQUID"))
        })
        .collect()
}

pub fn hip4_perp_instrument_id(underlying: &str) -> String {
    format!("{underlying}-USD-PERP.HYPERLIQUID")
}

fn parse_description_fields(description: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for part in description.split('|') {
        let Some((key, value)) = part.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if !key.is_empty() {
            fields.insert(key.to_string(), value.to_string());
        }
    }
    fields
}

fn parse_price_thresholds(value: Option<&str>) -> Vec<Decimal> {
    let Some(value) = value else {
        return Vec::new();
    };
    value
        .split(',')
        .filter_map(|part| Decimal::from_str_exact(part.trim()).ok())
        .collect()
}

const DESCRIPTION_PRICE_KEYS: [&str; 5] = [
    "targetPrice",
    "startPrice",
    "startPx",
    "strike",
    "strikePrice",
];

fn resolve_start_price(fields: &BTreeMap<String, String>) -> Option<Decimal> {
    for key in DESCRIPTION_PRICE_KEYS {
        if let Some(value) = fields.get(key) {
            if let Ok(decimal) = Decimal::from_str_exact(value) {
                return Some(decimal);
            }
        }
    }
    None
}

/// Parse `expiry:YYYYMMDD-HHMM` as UTC wall clock (Hyperliquid HIP-4 convention).
pub fn parse_expiry_to_ns(value: &str) -> Option<u64> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    if raw.chars().all(|ch| ch.is_ascii_digit()) {
        return match raw.len() {
            len if len >= 16 => raw.parse().ok(),
            13 => raw.parse::<u64>().ok().map(|value| value * 1_000_000),
            10 => raw.parse::<u64>().ok().map(|value| value * 1_000_000_000),
            _ => None,
        };
    }

    if raw.len() != 13 || raw.as_bytes().get(8) != Some(&b'-') {
        return None;
    }
    let year = raw[0..4].parse::<i32>().ok()?;
    let month = raw[4..6].parse::<u32>().ok()?;
    let day = raw[6..8].parse::<u32>().ok()?;
    let hour = raw[9..11].parse::<u32>().ok()?;
    let minute = raw[11..13].parse::<u32>().ok()?;
    let datetime =
        chrono::NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, 0)?;
    let utc = datetime.and_utc();
    Some(utc.timestamp_nanos_opt()? as u64)
}

#[cfg(test)]
mod tests {
    use super::{
        instrument_ids_from_outcomes, parse_expiry_to_ns, resolve_hip4_market,
        ResolveHip4MarketOptions,
    };

    #[test]
    fn parse_expiry_wall_clock_utc() {
        let ns = parse_expiry_to_ns("20260614-0600").expect("expiry should parse");
        assert_eq!(ns, 1_781_416_800_000_000_000);
    }

    #[test]
    fn resolve_selects_nearest_future_question() {
        let payload = serde_json::json!({
            "questions": [
                {
                    "question": 55,
                    "name": "Recurring",
                    "description": "class:priceBinary|underlying:BTC|expiry:20260614-0600|period:1d",
                    "namedOutcomes": [326],
                    "fallbackOutcome": 325
                },
                {
                    "question": 56,
                    "name": "Recurring",
                    "description": "class:priceBinary|underlying:BTC|expiry:20260615-0600|period:1d",
                    "namedOutcomes": [330],
                    "fallbackOutcome": 329
                }
            ]
        });
        let before = resolve_hip4_market(
            &payload,
            &ResolveHip4MarketOptions {
                underlying: "BTC",
                period: "1d",
                market_class: "priceBinary",
                include_fallback: false,
                now_ns: parse_expiry_to_ns("20260614-0559").unwrap_or(0),
            },
        )
        .expect("market should resolve");
        let after = resolve_hip4_market(
            &payload,
            &ResolveHip4MarketOptions {
                underlying: "BTC",
                period: "1d",
                market_class: "priceBinary",
                include_fallback: false,
                now_ns: parse_expiry_to_ns("20260614-0600").unwrap_or(0) + 5_000_000_000,
            },
        )
        .expect("market should resolve");
        assert_eq!(before.question_id, 55);
        assert_eq!(after.question_id, 56);
    }

    #[test]
    fn instrument_ids_expand_yes_no_pairs() {
        assert_eq!(
            instrument_ids_from_outcomes(&[326]),
            vec![
                "326-YES-OUTCOME.HYPERLIQUID".to_string(),
                "326-NO-OUTCOME.HYPERLIQUID".to_string(),
            ]
        );
    }
}
