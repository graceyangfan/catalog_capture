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

use nautilus_model::{
    data::{OptionGreeks, QuoteTick},
    identifiers::InstrumentId,
};

use crate::dynamic_option_universe::DynamicOptionUniverseChange;

#[derive(Debug, Clone)]
pub struct OnlineOptionMetricsConfig {
    pub snapshot_interval_secs: u64,
    pub universes: Vec<OnlineOptionMetricsUniverseConfig>,
}

#[derive(Debug, Clone)]
pub struct OnlineOptionMetricsUniverseConfig {
    pub venue_id: String,
    pub underlying: String,
    pub expiry_iso8601: String,
    pub perp_instrument_id: InstrumentId,
    pub option_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone)]
pub struct OnlineOptionMetricsObserver {
    snapshot_interval_ns: u64,
    universes: Vec<UniverseState>,
    instrument_to_universes: BTreeMap<InstrumentId, Vec<usize>>,
}

#[derive(Debug, Clone)]
struct UniverseState {
    venue_id: String,
    underlying: String,
    expiry_iso8601: String,
    perp_instrument_id: InstrumentId,
    perp_quote_mid: Option<f64>,
    last_snapshot_ts_ns: Option<u64>,
    options: BTreeMap<InstrumentId, OptionState>,
}

#[derive(Debug, Clone)]
struct OptionState {
    strike: f64,
    option_type: char,
    mark_iv_raw: Option<f64>,
    mark_iv_decimal: Option<f64>,
    delta: Option<f64>,
    quote_mid: Option<f64>,
    quote_spread: Option<f64>,
}

#[derive(Debug, Clone)]
struct Snapshot {
    atm_strike: f64,
    atm_iv_decimal: Option<f64>,
    low_put_iv_decimal: Option<f64>,
    high_call_iv_decimal: Option<f64>,
    rough_risk_reversal_decimal: Option<f64>,
    rough_wing_richness_decimal: Option<f64>,
    greeks_ready: usize,
    quotes_ready: usize,
    option_count: usize,
}

impl OnlineOptionMetricsObserver {
    #[must_use]
    pub fn new(config: OnlineOptionMetricsConfig) -> Self {
        let snapshot_interval_ns = config.snapshot_interval_secs.max(1) * 1_000_000_000;
        let mut universes = Vec::with_capacity(config.universes.len());
        let mut instrument_to_universes = BTreeMap::<InstrumentId, Vec<usize>>::new();

        for universe_config in config.universes {
            let universe_index = universes.len();
            instrument_to_universes
                .entry(universe_config.perp_instrument_id)
                .or_default()
                .push(universe_index);

            let mut options = BTreeMap::new();
            for instrument_id in universe_config.option_instrument_ids {
                let descriptor = parse_option_id(&instrument_id.to_string()).unwrap_or((
                    String::new(),
                    0.0,
                    '?',
                ));
                instrument_to_universes
                    .entry(instrument_id)
                    .or_default()
                    .push(universe_index);
                options.insert(
                    instrument_id,
                    OptionState {
                        strike: descriptor.1,
                        option_type: descriptor.2,
                        mark_iv_raw: None,
                        mark_iv_decimal: None,
                        delta: None,
                        quote_mid: None,
                        quote_spread: None,
                    },
                );
            }

            universes.push(UniverseState {
                venue_id: universe_config.venue_id,
                underlying: universe_config.underlying,
                expiry_iso8601: universe_config.expiry_iso8601,
                perp_instrument_id: universe_config.perp_instrument_id,
                perp_quote_mid: None,
                last_snapshot_ts_ns: None,
                options,
            });
        }

        Self {
            snapshot_interval_ns,
            universes,
            instrument_to_universes,
        }
    }

    #[must_use]
    pub fn on_quote(&mut self, quote: &QuoteTick) -> Vec<String> {
        let Some(universe_indexes) = self
            .instrument_to_universes
            .get(&quote.instrument_id)
            .cloned()
        else {
            return Vec::new();
        };

        let bid = quote.bid_price.as_f64();
        let ask = quote.ask_price.as_f64();
        let quote_mid = (bid + ask) / 2.0;
        let quote_spread = ask - bid;
        let ts_ns = quote.ts_event.as_u64();

        let mut lines = Vec::new();
        for universe_index in universe_indexes {
            let universe = &mut self.universes[universe_index];
            if quote.instrument_id == universe.perp_instrument_id {
                universe.perp_quote_mid = Some(quote_mid);
            } else if let Some(option) = universe.options.get_mut(&quote.instrument_id) {
                option.quote_mid = Some(quote_mid);
                option.quote_spread = Some(quote_spread);
            }

            if let Some(snapshot) = universe.maybe_snapshot(ts_ns, self.snapshot_interval_ns) {
                lines.push(render_snapshot(universe, &snapshot));
            }
        }

        lines
    }

    #[must_use]
    pub fn on_option_greeks(&mut self, greeks: &OptionGreeks) -> Vec<String> {
        let Some(universe_indexes) = self
            .instrument_to_universes
            .get(&greeks.instrument_id)
            .cloned()
        else {
            return Vec::new();
        };

        let ts_ns = greeks.ts_event.as_u64();
        let mut lines = Vec::new();
        for universe_index in universe_indexes {
            let universe = &mut self.universes[universe_index];
            let Some(option) = universe.options.get_mut(&greeks.instrument_id) else {
                continue;
            };
            option.delta = Some(greeks.greeks.delta);
            if let Some(mark_iv) = greeks.mark_iv {
                option.mark_iv_raw = Some(mark_iv);
                option.mark_iv_decimal = Some(normalize_mark_iv(mark_iv));
            }

            if let Some(snapshot) = universe.maybe_snapshot(ts_ns, self.snapshot_interval_ns) {
                lines.push(render_snapshot(universe, &snapshot));
            }
        }

        lines
    }

    pub fn apply_universe_change(&mut self, change: &DynamicOptionUniverseChange) {
        let Some(universe_index) = self.universes.iter().position(|universe| {
            universe.venue_id == change.venue_id && universe.underlying == change.underlying
        }) else {
            return;
        };

        for instrument_id in &change.removed_instrument_ids {
            if let Some(indexes) = self.instrument_to_universes.get_mut(instrument_id) {
                indexes.retain(|index| *index != universe_index);
                if indexes.is_empty() {
                    self.instrument_to_universes.remove(instrument_id);
                }
            }
        }

        let universe = &mut self.universes[universe_index];
        universe.expiry_iso8601 = change.selected_expiry_iso8601.clone();
        if let Some(perp_instrument_id) = change.perp_instrument_id {
            if universe.perp_instrument_id != perp_instrument_id {
                if let Some(indexes) = self
                    .instrument_to_universes
                    .get_mut(&universe.perp_instrument_id)
                {
                    indexes.retain(|index| *index != universe_index);
                    if indexes.is_empty() {
                        self.instrument_to_universes
                            .remove(&universe.perp_instrument_id);
                    }
                }
                self.instrument_to_universes
                    .entry(perp_instrument_id)
                    .or_default()
                    .push(universe_index);
                universe.perp_instrument_id = perp_instrument_id;
            }
        }

        universe.options.clear();
        universe.perp_quote_mid = None;
        universe.last_snapshot_ts_ns = None;
        for instrument_id in &change.option_instrument_ids {
            let descriptor =
                parse_option_id(&instrument_id.to_string()).unwrap_or((String::new(), 0.0, '?'));
            self.instrument_to_universes
                .entry(*instrument_id)
                .or_default()
                .push(universe_index);
            universe.options.insert(
                *instrument_id,
                OptionState {
                    strike: descriptor.1,
                    option_type: descriptor.2,
                    mark_iv_raw: None,
                    mark_iv_decimal: None,
                    delta: None,
                    quote_mid: None,
                    quote_spread: None,
                },
            );
        }
    }
}

impl UniverseState {
    fn maybe_snapshot(&mut self, ts_ns: u64, interval_ns: u64) -> Option<Snapshot> {
        let should_emit = self
            .last_snapshot_ts_ns
            .is_none_or(|last| ts_ns.saturating_sub(last) >= interval_ns);
        if !should_emit {
            return None;
        }

        let snapshot = self.snapshot()?;
        self.last_snapshot_ts_ns = Some(ts_ns);
        Some(snapshot)
    }

    fn snapshot(&self) -> Option<Snapshot> {
        let perp_quote_mid = self.perp_quote_mid?;
        if self.options.is_empty() {
            return None;
        }

        let strikes = self
            .options
            .values()
            .map(|option| option.strike)
            .collect::<Vec<_>>();
        let atm_strike = choose_atm_strike(&strikes, perp_quote_mid)?;

        let atm_iv_decimal = average(
            self.options
                .values()
                .filter(|option| option.strike == atm_strike)
                .filter_map(|option| option.mark_iv_decimal)
                .collect(),
        )?;

        let low_strike = strikes.iter().copied().reduce(f64::min)?;
        let high_strike = strikes.iter().copied().reduce(f64::max)?;
        let low_put_iv_decimal = average(
            self.options
                .values()
                .filter(|option| option.strike == low_strike && option.option_type == 'P')
                .filter_map(|option| option.mark_iv_decimal)
                .collect(),
        );
        let high_call_iv_decimal = average(
            self.options
                .values()
                .filter(|option| option.strike == high_strike && option.option_type == 'C')
                .filter_map(|option| option.mark_iv_decimal)
                .collect(),
        );
        if low_put_iv_decimal.is_none() || high_call_iv_decimal.is_none() {
            return None;
        }

        let rough_risk_reversal_decimal = match (low_put_iv_decimal, high_call_iv_decimal) {
            (Some(low_put), Some(high_call)) => Some(high_call - low_put),
            _ => None,
        };

        let wing_average = average(
            [low_put_iv_decimal, high_call_iv_decimal]
                .into_iter()
                .flatten()
                .collect(),
        );
        let rough_wing_richness_decimal =
            wing_average.map(|wing_average| wing_average - atm_iv_decimal);

        let greeks_ready = self
            .options
            .values()
            .filter(|option| option.mark_iv_decimal.is_some() && option.delta.is_some())
            .count();
        if greeks_ready == 0 {
            return None;
        }
        let quotes_ready = self
            .options
            .values()
            .filter(|option| option.quote_mid.is_some())
            .count();

        Some(Snapshot {
            atm_strike,
            atm_iv_decimal: Some(atm_iv_decimal),
            low_put_iv_decimal,
            high_call_iv_decimal,
            rough_risk_reversal_decimal,
            rough_wing_richness_decimal,
            greeks_ready,
            quotes_ready,
            option_count: self.options.len(),
        })
    }
}

fn render_snapshot(universe: &UniverseState, snapshot: &Snapshot) -> String {
    format!(
        "[online-option-metrics] venue={} underlying={} expiry={} perp={} perp_mid={} atm_strike={} atm_iv={} low_put_iv={} high_call_iv={} rr={} wing={} greeks_ready={}/{} quotes_ready={}/{}",
        universe.venue_id,
        universe.underlying,
        universe.expiry_iso8601,
        universe.perp_instrument_id,
        format_optional(universe.perp_quote_mid, 2),
        format_float(snapshot.atm_strike, 0),
        format_optional(snapshot.atm_iv_decimal, 6),
        format_optional(snapshot.low_put_iv_decimal, 6),
        format_optional(snapshot.high_call_iv_decimal, 6),
        format_optional(snapshot.rough_risk_reversal_decimal, 6),
        format_optional(snapshot.rough_wing_richness_decimal, 6),
        snapshot.greeks_ready,
        snapshot.option_count,
        snapshot.quotes_ready,
        snapshot.option_count,
    )
}

fn choose_atm_strike(strikes: &[f64], perp_quote_mid: f64) -> Option<f64> {
    strikes.iter().copied().min_by(|left, right| {
        let left_key = ((left - perp_quote_mid).abs(), *left);
        let right_key = ((right - perp_quote_mid).abs(), *right);
        left_key
            .partial_cmp(&right_key)
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

fn average(values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn normalize_mark_iv(value: f64) -> f64 {
    if value.abs() > 3.0 {
        value / 100.0
    } else {
        value
    }
}

fn parse_option_id(instrument_id: &str) -> Option<(String, f64, char)> {
    let base = instrument_id.split('.').next()?;
    let tokens = base.split('-').collect::<Vec<_>>();
    for index in 0..tokens.len() {
        let token = tokens[index];
        if matches!(token, "C" | "P") && index >= 2 {
            let expiry = tokens[index - 2].to_string();
            let strike = tokens[index - 1].parse::<f64>().ok()?;
            let option_type = token.chars().next()?;
            return Some((expiry, strike, option_type));
        }
    }
    None
}

fn format_optional(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format_float(value, precision))
        .unwrap_or_else(|| "-".to_string())
}

fn format_float(value: f64, precision: usize) -> String {
    format!("{value:.precision$}")
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{OptionGreekValues, OptionGreeks, QuoteTick},
        enums::GreeksConvention,
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };
    use std::str::FromStr;

    use super::*;

    fn instrument_id(value: &str) -> InstrumentId {
        InstrumentId::from_str(value).expect("valid instrument id")
    }

    fn make_quote(value: &str, bid: &str, ask: &str, ts_ns: u64) -> QuoteTick {
        QuoteTick::new(
            instrument_id(value),
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1"),
            Quantity::from("1"),
            ts_ns.into(),
            ts_ns.into(),
        )
    }

    fn make_greeks(value: &str, delta: f64, mark_iv: f64, ts_ns: u64) -> OptionGreeks {
        OptionGreeks {
            instrument_id: instrument_id(value),
            convention: GreeksConvention::PriceAdjusted,
            greeks: OptionGreekValues {
                delta,
                gamma: 0.1,
                vega: 0.2,
                theta: -0.1,
                rho: 0.01,
            },
            bid_iv: None,
            ask_iv: None,
            mark_iv: Some(mark_iv),
            underlying_price: None,
            open_interest: None,
            ts_event: ts_ns.into(),
            ts_init: ts_ns.into(),
        }
    }

    #[test]
    fn parse_option_id_supports_deribit_bybit_and_okx() {
        assert_eq!(
            parse_option_id("BTC-20JUN26-62500-C.DERIBIT"),
            Some(("20JUN26".to_string(), 62500.0, 'C'))
        );
        assert_eq!(
            parse_option_id("BTC-20JUN26-62500-P-USDT-OPTION.BYBIT"),
            Some(("20JUN26".to_string(), 62500.0, 'P'))
        );
        assert_eq!(
            parse_option_id("BTC-USD-260620-62500-C.OKX"),
            Some(("260620".to_string(), 62500.0, 'C'))
        );
    }

    #[test]
    fn normalize_mark_iv_converts_percent_like_values() {
        assert!((normalize_mark_iv(35.62) - 0.3562).abs() < 1e-9);
        assert!((normalize_mark_iv(0.3562) - 0.3562).abs() < 1e-12);
    }

    #[test]
    fn observer_emits_decimal_snapshot() {
        let mut observer = OnlineOptionMetricsObserver::new(OnlineOptionMetricsConfig {
            snapshot_interval_secs: 1,
            universes: vec![OnlineOptionMetricsUniverseConfig {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                expiry_iso8601: "2026-06-20T08:00:00Z".to_string(),
                perp_instrument_id: instrument_id("BTC-PERPETUAL.DERIBIT"),
                option_instrument_ids: vec![
                    instrument_id("BTC-20JUN26-62000-C.DERIBIT"),
                    instrument_id("BTC-20JUN26-62000-P.DERIBIT"),
                    instrument_id("BTC-20JUN26-62500-C.DERIBIT"),
                    instrument_id("BTC-20JUN26-62500-P.DERIBIT"),
                    instrument_id("BTC-20JUN26-63000-C.DERIBIT"),
                    instrument_id("BTC-20JUN26-63000-P.DERIBIT"),
                ],
            }],
        });

        assert!(observer
            .on_quote(&make_quote(
                "BTC-PERPETUAL.DERIBIT",
                "62454.0",
                "62454.5",
                1
            ))
            .is_empty());
        assert!(observer
            .on_quote(&make_quote(
                "BTC-20JUN26-62500-C.DERIBIT",
                "0.0060",
                "0.0070",
                2
            ))
            .is_empty());

        let mut lines = Vec::new();
        lines.extend(observer.on_option_greeks(&make_greeks(
            "BTC-20JUN26-62000-P.DERIBIT",
            -0.34,
            40.0,
            1_000_000_001,
        )));
        lines.extend(observer.on_option_greeks(&make_greeks(
            "BTC-20JUN26-62500-C.DERIBIT",
            0.49,
            35.62,
            1_000_000_002,
        )));
        lines.extend(observer.on_option_greeks(&make_greeks(
            "BTC-20JUN26-62500-P.DERIBIT",
            -0.51,
            35.62,
            1_000_000_003,
        )));
        lines.extend(observer.on_option_greeks(&make_greeks(
            "BTC-20JUN26-63000-C.DERIBIT",
            0.29,
            32.19,
            2_000_000_004,
        )));

        let rendered = lines.join("\n");
        assert!(rendered.contains("atm_iv=0.356200"));
        assert!(rendered.contains("rr=-0.078100"));
    }
}
