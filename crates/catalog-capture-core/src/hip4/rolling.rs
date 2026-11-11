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

//! Selection policy for Outcome.xyz BTC/ETH quarter-hour binary markets.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::parse_expiry_to_ns;

pub const ROLLING_INTERVAL_NS: u64 = 900_000_000_000;

/// Market-data families recorded for each rolling YES/NO pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Hip4RollingFamily {
    BookDeltas,
    Quotes,
    Trades,
}
impl Hip4RollingFamily {
    pub const fn catalog_prefix(self) -> &'static str {
        match self {
            Self::BookDeltas => "order_book_deltas",
            Self::Quotes => "quotes",
            Self::Trades => "trades",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hip4RollingConfig {
    pub venue_id: String,
    #[serde(skip)]
    pub families: Vec<Hip4RollingFamily>,
    #[serde(default = "assets")]
    pub underlyings: Vec<String>,
    #[serde(default = "refresh")]
    pub retry_secs: u64,
    /// Maximum time an expired predecessor may wait for a successor baseline.
    #[serde(default = "default_handover_grace_secs")]
    pub handover_grace_secs: u64,
}
fn assets() -> Vec<String> {
    vec!["BTC".into(), "ETH".into()]
}
const fn refresh() -> u64 {
    2
}
const fn default_handover_grace_secs() -> u64 {
    60
}
impl Hip4RollingConfig {
    pub fn captures(&self, family: Hip4RollingFamily) -> bool {
        self.families.contains(&family)
    }

    /// Prefer snapshots, then quotes, then trades for the durable handover baseline.
    pub fn baseline_family(&self) -> Hip4RollingFamily {
        *self
            .families
            .iter()
            .min()
            .expect("validated rolling families")
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.families.is_empty()
                && self.families.iter().collect::<BTreeSet<_>>().len() == self.families.len(),
            "HIP-4 rolling requires distinct selectors under capture.quotes, capture.trades or capture.book_deltas"
        );
        ensure!(
            !self.venue_id.trim().is_empty(),
            "HIP-4 rolling venue_id is required"
        );
        ensure!(
            !self.underlyings.is_empty()
                && self.underlyings.len() <= 2
                && self
                    .underlyings
                    .iter()
                    .all(|s| matches!(s.as_str(), "BTC" | "ETH"))
                && self.underlyings.iter().collect::<BTreeSet<_>>().len() == self.underlyings.len(),
            "HIP-4 rolling requires distinct BTC/ETH underlyings"
        );
        ensure!(
            (1..=60).contains(&self.retry_secs),
            "invalid HIP-4 discovery retry interval"
        );
        ensure!(
            self.handover_grace_secs < ROLLING_INTERVAL_NS / 1_000_000_000,
            "HIP-4 handover grace must be shorter than one market window"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hip4RollingMarket {
    pub outcome: u32,
    pub underlying: String,
    pub end_ns: u64,
    /// Unmodified venue metadata, including the deployer and fee fields.
    pub raw: Value,
}

/// Select only the currently running quarter hour. Never infer duration from TWAP seconds.
/// A missing market is normal during deployment after a boundary; the caller retries.
pub fn select_rolling_market(
    payload: &Value,
    underlying: &str,
    now_ns: u64,
) -> Result<Option<Hip4RollingMarket>> {
    ensure!(
        matches!(underlying, "BTC" | "ETH"),
        "unsupported HIP-4 rolling underlying"
    );
    let outcomes = payload
        .get("outcomes")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("HIP-4 outcomeMeta lacks outcomes"))?;
    let expected_end = (now_ns / ROLLING_INTERVAL_NS + 1)
        .checked_mul(ROLLING_INTERVAL_NS)
        .ok_or_else(|| anyhow::anyhow!("HIP-4 window overflow"))?;
    let mut selected = None;
    for raw in outcomes {
        if raw.get("venue").and_then(Value::as_str) != Some("out")
            || raw.get("name").and_then(Value::as_str) != Some("template:binaryPrice")
        {
            continue;
        }
        let description = raw
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let fields: BTreeMap<_, _> = description
            .split('|')
            .filter_map(|s| s.split_once(':'))
            .collect();
        if fields.get("perp").copied() != Some(underlying) {
            continue;
        }
        let end_ns = fields
            .get("time")
            .and_then(|s| parse_expiry_to_ns(s))
            .ok_or_else(|| anyhow::anyhow!("HIP-4 template lacks valid time"))?;
        if end_ns != expected_end {
            continue;
        }
        ensure!(
            fields.len() == description.split('|').count(),
            "duplicate HIP-4 template keyword"
        );
        ensure!(
            fields.get("seconds").copied() == Some("60")
                && fields.get("priceDescription").copied()
                    == Some(format!("{underlying}-USDC perp mark").as_str())
                && raw.get("quoteToken").and_then(Value::as_str) == Some("USDC"),
            "HIP-4 rolling settlement policy changed"
        );
        let threshold = fields
            .get("threshold")
            .and_then(|s| Decimal::from_str_exact(s).ok());
        ensure!(
            threshold.is_some_and(|v| v > Decimal::ZERO),
            "invalid HIP-4 threshold"
        );
        ensure!(
            raw.pointer("/sideSpecs/0/name").and_then(Value::as_str) == Some("template:Yes")
                && raw.pointer("/sideSpecs/1/name").and_then(Value::as_str) == Some("template:No")
                && raw
                    .get("sideSpecs")
                    .and_then(Value::as_array)
                    .is_some_and(|s| s.len() == 2),
            "HIP-4 rolling sides changed"
        );
        let outcome = raw
            .get("outcome")
            .and_then(Value::as_u64)
            .and_then(|id| u32::try_from(id).ok())
            .filter(|id| *id > 0)
            .ok_or_else(|| anyhow::anyhow!("invalid HIP-4 outcome ID"))?;
        ensure!(
            outcome
                .checked_mul(10)
                .and_then(|n| n.checked_add(100_000_001))
                .is_some(),
            "HIP-4 asset ID overflow"
        );
        ensure!(
            selected.is_none(),
            "ambiguous HIP-4 rolling market for {underlying}"
        );
        selected = Some(Hip4RollingMarket {
            outcome,
            underlying: underlying.into(),
            end_ns,
            raw: raw.clone(),
        });
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    fn row(id: u32, asset: &str, end: &str) -> Value {
        json!({"outcome":id,"venue":"out","name":"template:binaryPrice","quoteToken":"USDC",
            "description":format!("perp:{asset}|priceDescription:{asset}-USDC perp mark|seconds:60|threshold:100|time:{end}"),
            "sideSpecs":[{"name":"template:Yes"},{"name":"template:No"}],"deployerFeeScale":"1.0"})
    }
    #[test]
    fn current_window_ignores_future_expired_other_provider_and_other_asset() {
        let now = parse_expiry_to_ns("20261104-0744").unwrap();
        let mut wrong_provider = row(9, "BTC", "20261104-0745");
        wrong_provider["venue"] = json!("fox");
        let p = json!({"outcomes":[row(1,"BTC","20261104-0730"),row(2,"BTC","20261104-0745"),
            row(3,"BTC","20261104-0800"),row(4,"ETH","20261104-0745"),wrong_provider]});
        assert_eq!(
            select_rolling_market(&p, "BTC", now)
                .unwrap()
                .unwrap()
                .outcome,
            2
        );
        assert_eq!(
            select_rolling_market(&p, "ETH", now)
                .unwrap()
                .unwrap()
                .outcome,
            4
        );
        let next = select_rolling_market(&p, "BTC", parse_expiry_to_ns("20261104-0745").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(next.outcome, 3);
        assert_eq!(next.raw["deployerFeeScale"], "1.0");
        assert!(
            select_rolling_market(&json!({"outcomes":[]}), "BTC", now)
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn ambiguous_and_changed_rules_fail_closed() {
        let now = parse_expiry_to_ns("20261104-0744").unwrap();
        assert!(
            select_rolling_market(
                &json!({"outcomes":[row(1,"BTC","20261104-0745"),row(2,"BTC","20261104-0745")]}),
                "BTC",
                now
            )
            .is_err()
        );
        let mut changed = row(1, "BTC", "20261104-0745");
        changed["description"] = json!(
            "perp:BTC|priceDescription:BTC-USDC perp mark|seconds:900|threshold:100|time:20261104-0745"
        );
        assert!(select_rolling_market(&json!({"outcomes":[changed]}), "BTC", now).is_err());
        assert!(select_rolling_market(&json!({}), "BTC", now).is_err());
    }
}
