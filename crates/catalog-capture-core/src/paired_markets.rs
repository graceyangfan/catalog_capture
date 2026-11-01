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

//! Recording identities for matched sports outcomes; no venue transport or trading state.
use anyhow::Result;
use nautilus_model::identifiers::InstrumentId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MatchedSportsCaptureConfig {
    pub tag_ids: Vec<u64>,
    pub include_match_winners: bool,
    pub include_map_winners: bool,
    pub refresh_secs: u64,
    pub max_markets: usize,
    pub max_pages: usize,
    /// Bound statistics enrichment, independently of the complete matching universe.
    pub max_activity_candidates: usize,
    pub activity_scan_pages: usize,
    pub observation_timeout_secs: u64,
    /// At most one additional market is recorded while preparing a successor.
    pub preload_next: bool,
    pub preparation_timeout_secs: u64,
    pub preparation_retry_secs: u64,
    /// Historical lookback, never a startup waiting period.
    pub recent_trade_window_secs: u64,
    pub max_last_trade_age_secs: u64,
    pub min_recent_trade_times: usize,
    pub selection_candidates: usize,
    pub min_volume24h_usd: rust_decimal::Decimal,
    pub max_spread: rust_decimal::Decimal,
    pub depth_band: rust_decimal::Decimal,
    pub min_depth_shares: rust_decimal::Decimal,
}
impl Default for MatchedSportsCaptureConfig {
    fn default() -> Self {
        Self {
            tag_ids: vec![84],
            include_match_winners: true,
            include_map_winners: false,
            refresh_secs: 30,
            max_markets: 1,
            max_pages: 100,
            max_activity_candidates: 256,
            activity_scan_pages: 4,
            observation_timeout_secs: 30,
            preload_next: true,
            preparation_timeout_secs: 30,
            preparation_retry_secs: 30,
            recent_trade_window_secs: 300,
            max_last_trade_age_secs: 120,
            min_recent_trade_times: 2,
            selection_candidates: 5,
            min_volume24h_usd: rust_decimal::Decimal::from(100),
            max_spread: rust_decimal::Decimal::new(5, 2),
            depth_band: rust_decimal::Decimal::new(3, 2),
            min_depth_shares: rust_decimal::Decimal::from(100),
        }
    }
}
impl MatchedSportsCaptureConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.tag_ids.is_empty()
                && self.tag_ids.len() <= 8
                && self.tag_ids.iter().all(|v| *v > 0)
                && self
                    .tag_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == self.tag_ids.len(),
            "invalid or duplicate sports tag IDs"
        );
        anyhow::ensure!(
            (5..=300).contains(&self.refresh_secs),
            "MatchedSports refresh_secs must be 5..=300"
        );
        anyhow::ensure!(
            (1..=256).contains(&self.max_markets),
            "MatchedSports max_markets must be 1..=256"
        );
        anyhow::ensure!(
            (1..=1024).contains(&self.max_activity_candidates)
                && self.max_activity_candidates >= self.selection_candidates,
            "invalid activity candidate budget"
        );
        anyhow::ensure!(
            (1..=20).contains(&self.activity_scan_pages)
                && (self.include_match_winners || self.include_map_winners),
            "invalid activity scan page budget or empty sports market types"
        );
        anyhow::ensure!(
            (1..=100).contains(&self.max_pages),
            "MatchedSports max_pages must be 1..=100"
        );
        anyhow::ensure!(
            (1..=3600).contains(&self.observation_timeout_secs),
            "invalid observation timeout"
        );
        anyhow::ensure!(
            (5..=300).contains(&self.preparation_timeout_secs)
                && (5..=300).contains(&self.preparation_retry_secs),
            "invalid MatchedSports preparation timeout/retry"
        );
        anyhow::ensure!(
            (30..=900).contains(&self.recent_trade_window_secs)
                && (1..=self.recent_trade_window_secs).contains(&self.max_last_trade_age_secs)
                && (2..=50).contains(&self.min_recent_trade_times)
                && (self.max_markets + usize::from(self.preload_next)..=256)
                    .contains(&self.selection_candidates)
                && self.min_volume24h_usd > rust_decimal::Decimal::ZERO
                && self.max_spread > rust_decimal::Decimal::ZERO
                && self.max_spread < rust_decimal::Decimal::ONE
                && self.depth_band > rust_decimal::Decimal::ZERO
                && self.depth_band < rust_decimal::Decimal::ONE
                && self.min_depth_shares > rust_decimal::Decimal::ZERO,
            "invalid MatchedSports shortlist, volume, book or recent-trade controls"
        );
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedOutcome {
    pub predict_index_set: u8,
    pub predict_label: String,
    pub polymarket_label: String,
    pub predict_instrument: InstrumentId,
    pub polymarket_instrument: InstrumentId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedMarket {
    pub event_slug: String,
    pub predict_market_id: u64,
    pub polymarket_condition_id: String,
    pub game_number: Option<u32>,
    pub rules: String,
    pub outcomes: [MatchedOutcome; 2],
}
impl MatchedMarket {
    pub fn instrument_ids(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.outcomes
            .iter()
            .flat_map(|o| [o.predict_instrument, o.polymarket_instrument])
    }

    /// Market-data streams: native Predict YES and both native Polymarket outcomes.
    /// Both Predict definitions remain in `instrument_ids` for replay metadata.
    pub fn recording_instrument_ids(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.outcomes.iter().flat_map(|o| {
            std::iter::once(o.polymarket_instrument)
                .chain((o.predict_index_set == 1).then_some(o.predict_instrument))
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchRejection {
    pub predict_market_id: u64,
    pub reason: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MatchedUniverse {
    pub observed_ns: u64,
    pub markets: Vec<MatchedMarket>,
    /// Ordered new admissions. All validated incumbents remain in `markets`,
    /// including those below the activity floor. None supports direct fixed universes.
    pub admission_ids: Option<Vec<u64>>,
    pub rejected: Vec<MatchRejection>,
    pub metadata: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_policy_rejects_unbounded_or_invalid_controls() {
        assert!(MatchedSportsCaptureConfig::default().validate().is_ok());
        for config in [
            MatchedSportsCaptureConfig {
                max_activity_candidates: 1,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                include_match_winners: false,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                activity_scan_pages: 0,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                preparation_timeout_secs: 0,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                preparation_retry_secs: 0,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                min_volume24h_usd: rust_decimal::Decimal::ZERO,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                recent_trade_window_secs: 0,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                max_spread: rust_decimal::Decimal::ONE,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                max_markets: 6,
                ..Default::default()
            },
            MatchedSportsCaptureConfig {
                min_recent_trade_times: 51,
                ..Default::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
        assert!(
            MatchedSportsCaptureConfig {
                include_match_winners: false,
                include_map_winners: true,
                ..Default::default()
            }
            .validate()
            .is_ok()
        );
    }
}
