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

use std::collections::BTreeSet;

use nautilus_model::{
    data::{BarType, DataType},
    enums::BookType,
    identifiers::InstrumentId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarCaptureSpec {
    pub bar_type: BarType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookDeltasCaptureSpec {
    pub instrument_id: InstrumentId,
    pub book_type: BookType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentStatusCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentCloseCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionGreeksCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

/// Subscribe-style custom data (`DataActor::subscribe_data` → live `on_data`).
///
/// Do **not** put request-only types here (e.g. `DeribitBookSummary`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomDataCaptureSpec {
    pub data_type: DataType,
}

/// How to treat a poll tick when a previous request is still in flight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RequestOverlapPolicy {
    /// Skip this tick (recommended; protects venue REST budget).
    #[default]
    Skip,
}

impl RequestOverlapPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
        }
    }
}

/// Request-style custom data (`DataActor::request_data` → `handle_data_response` /
/// `on_historical_data`).
///
/// Mirrors Nautilus: request is not a subscription. Capture only schedules polls
/// and sinks the response; HTTP is owned by the venue adapter client.
///
/// Do **not** put stream/subscribe types here (e.g. `DeribitVolatilityIndex`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomDataRequestCaptureSpec {
    pub data_type: DataType,
    /// Poll period in seconds. Hard minimum is 1; recommended default is 5.
    pub interval_secs: u64,
    /// Fire once at actor start before the first timer wait.
    pub fire_immediately: bool,
    pub overlap_policy: RequestOverlapPolicy,
    /// Soft in-flight timeout before a stuck request may be re-fired.
    pub request_timeout_secs: u64,
    /// Optional ClientId override (defaults are resolved by type name, e.g. DERIBIT).
    pub client_id: Option<String>,
}

/// Hard floor for `[[capture.custom_data_requests]].interval_secs`.
pub const MIN_CUSTOM_DATA_REQUEST_INTERVAL_SECS: u64 = 1;
/// Recommended production default for Deribit book-summary style polls.
pub const DEFAULT_CUSTOM_DATA_REQUEST_INTERVAL_SECS: u64 = 5;
/// Soft in-flight timeout when response correlation is incomplete.
pub const DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS: u64 = 15;
/// Aggregate request budget share (~10% of Deribit non-matching ~20 rps).
pub const DEFAULT_MAX_AGGREGATE_CUSTOM_DATA_REQUEST_RPS: f64 = 2.0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CaptureFamilyRuntimeFlags {
    pub instruments: bool,
    /// Subscribe-style custom data only (`subscribe_data` / `on_data`).
    pub custom_data: bool,
    /// Request-style custom data only (`request_data` / response handler).
    ///
    /// Shares the CustomData parquet sink with `custom_data` when both are set,
    /// but the Nautilus command path stays completely separate.
    pub custom_data_requests: bool,
    pub quotes: bool,
    pub trades: bool,
    pub bars: bool,
    pub book_deltas: bool,
    pub mark_prices: bool,
    pub index_prices: bool,
    pub funding_rates: bool,
    pub instrument_statuses: bool,
    pub instrument_closes: bool,
    pub option_greeks: bool,
}

impl CaptureFamilyRuntimeFlags {
    /// Whether a CustomData parquet writer runtime is required.
    ///
    /// Subscribe and request both deliver `CustomData` payloads to catalog, so
    /// they share one writer — not one Nautilus command path.
    #[must_use]
    pub const fn needs_custom_data_writer(&self) -> bool {
        self.custom_data || self.custom_data_requests
    }

    #[must_use]
    pub fn count_enabled(&self) -> usize {
        [
            self.instruments,
            // One shared CustomData writer for subscribe and/or request payloads.
            self.needs_custom_data_writer(),
            self.quotes,
            self.trades,
            self.bars,
            self.book_deltas,
            self.mark_prices,
            self.index_prices,
            self.funding_rates,
            self.instrument_statuses,
            self.instrument_closes,
            self.option_greeks,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapturePlan {
    pub instruments: Vec<InstrumentCaptureSpec>,
    pub quotes: Vec<QuoteCaptureSpec>,
    pub trades: Vec<TradeCaptureSpec>,
    pub bars: Vec<BarCaptureSpec>,
    pub book_deltas: Vec<BookDeltasCaptureSpec>,
    pub mark_prices: Vec<MarkPriceCaptureSpec>,
    pub index_prices: Vec<IndexPriceCaptureSpec>,
    pub funding_rates: Vec<FundingRateCaptureSpec>,
    pub instrument_statuses: Vec<InstrumentStatusCaptureSpec>,
    pub instrument_closes: Vec<InstrumentCloseCaptureSpec>,
    pub option_greeks: Vec<OptionGreeksCaptureSpec>,
    pub forward_prices: Vec<ForwardPriceCaptureSpec>,
    /// Subscribe-style only (`subscribe_data` → `on_data`).
    pub custom_data: Vec<CustomDataCaptureSpec>,
    /// Request-style only (`request_data` → `handle_data_response`).
    pub custom_data_requests: Vec<CustomDataRequestCaptureSpec>,
}

impl CapturePlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
            && self.quotes.is_empty()
            && self.trades.is_empty()
            && self.bars.is_empty()
            && self.book_deltas.is_empty()
            && self.mark_prices.is_empty()
            && self.index_prices.is_empty()
            && self.funding_rates.is_empty()
            && self.instrument_statuses.is_empty()
            && self.instrument_closes.is_empty()
            && self.option_greeks.is_empty()
            && self.forward_prices.is_empty()
            && self.custom_data.is_empty()
            && self.custom_data_requests.is_empty()
    }

    /// Union of every `instrument_id` referenced by any instrument-scoped family.
    ///
    /// `bars`, `custom_data`, and `custom_data_requests` are excluded because they
    /// are not keyed by `InstrumentId` alone.
    #[must_use]
    pub fn planned_instrument_ids(&self) -> Vec<InstrumentId> {
        let mut ids = BTreeSet::new();
        self.extend_planned_instrument_ids(&mut ids);
        ids.into_iter().collect()
    }

    /// Which per-family background worker runtimes are required for this plan.
    ///
    /// `instruments` is enabled when bootstrap will emit instrument metadata (any
    /// instrument-scoped family), not only when `capture.instruments` is explicit.
    ///
    /// Subscribe vs request custom data stay separate flags (Nautilus command paths
    /// differ). The CustomData parquet writer is enabled if either flag is set.
    #[must_use]
    pub fn family_runtime_flags(&self) -> CaptureFamilyRuntimeFlags {
        CaptureFamilyRuntimeFlags {
            instruments: !self.planned_instrument_ids().is_empty(),
            custom_data: !self.custom_data.is_empty(),
            custom_data_requests: !self.custom_data_requests.is_empty(),
            quotes: !self.quotes.is_empty(),
            trades: !self.trades.is_empty(),
            bars: !self.bars.is_empty(),
            book_deltas: !self.book_deltas.is_empty(),
            mark_prices: !self.mark_prices.is_empty(),
            index_prices: !self.index_prices.is_empty(),
            funding_rates: !self.funding_rates.is_empty(),
            instrument_statuses: !self.instrument_statuses.is_empty(),
            instrument_closes: !self.instrument_closes.is_empty(),
            option_greeks: !self.option_greeks.is_empty(),
        }
    }

    #[must_use]
    pub fn enabled_background_worker_count(&self) -> usize {
        self.family_runtime_flags().count_enabled()
    }

    fn extend_planned_instrument_ids(&self, ids: &mut BTreeSet<InstrumentId>) {
        ids.extend(self.instruments.iter().map(|spec| spec.instrument_id));
        ids.extend(self.quotes.iter().map(|spec| spec.instrument_id));
        ids.extend(self.trades.iter().map(|spec| spec.instrument_id));
        ids.extend(self.book_deltas.iter().map(|spec| spec.instrument_id));
        ids.extend(self.mark_prices.iter().map(|spec| spec.instrument_id));
        ids.extend(self.index_prices.iter().map(|spec| spec.instrument_id));
        ids.extend(self.funding_rates.iter().map(|spec| spec.instrument_id));
        ids.extend(
            self.instrument_statuses
                .iter()
                .map(|spec| spec.instrument_id),
        );
        ids.extend(self.instrument_closes.iter().map(|spec| spec.instrument_id));
        ids.extend(self.option_greeks.iter().map(|spec| spec.instrument_id));
        ids.extend(self.forward_prices.iter().map(|spec| spec.instrument_id));
    }
}

/// Entries present in `left` but not in `right`, compared per capture family.
#[must_use]
pub fn capture_plan_difference(left: &CapturePlan, right: &CapturePlan) -> CapturePlan {
    CapturePlan {
        instruments: capture_spec_difference(&left.instruments, &right.instruments),
        quotes: capture_spec_difference(&left.quotes, &right.quotes),
        trades: capture_spec_difference(&left.trades, &right.trades),
        bars: capture_spec_difference(&left.bars, &right.bars),
        book_deltas: capture_spec_difference(&left.book_deltas, &right.book_deltas),
        mark_prices: capture_spec_difference(&left.mark_prices, &right.mark_prices),
        index_prices: capture_spec_difference(&left.index_prices, &right.index_prices),
        funding_rates: capture_spec_difference(&left.funding_rates, &right.funding_rates),
        instrument_statuses: capture_spec_difference(
            &left.instrument_statuses,
            &right.instrument_statuses,
        ),
        instrument_closes: capture_spec_difference(
            &left.instrument_closes,
            &right.instrument_closes,
        ),
        option_greeks: capture_spec_difference(&left.option_greeks, &right.option_greeks),
        forward_prices: capture_spec_difference(&left.forward_prices, &right.forward_prices),
        custom_data: capture_spec_difference(&left.custom_data, &right.custom_data),
        custom_data_requests: capture_spec_difference(
            &left.custom_data_requests,
            &right.custom_data_requests,
        ),
    }
}

fn capture_spec_difference<T>(left: &[T], right: &[T]) -> Vec<T>
where
    T: Clone + PartialEq,
{
    left.iter()
        .filter(|spec| !right.contains(spec))
        .cloned()
        .collect()
}

/// Sorted unique instrument ids referenced by instrument-scoped capture families.
#[must_use]
pub fn plan_instrument_ids(plan: &CapturePlan) -> BTreeSet<InstrumentId> {
    plan.planned_instrument_ids().into_iter().collect()
}

#[must_use]
pub fn instrument_id_difference(
    left: &BTreeSet<InstrumentId>,
    right: &BTreeSet<InstrumentId>,
) -> Vec<InstrumentId> {
    left.difference(right).copied().collect()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use nautilus_model::data::DataType;

    use super::*;

    fn eth_perp() -> InstrumentId {
        InstrumentId::from_str("ETHUSDT-PERP.BINANCE").expect("valid instrument id")
    }

    fn btc_perp() -> InstrumentId {
        InstrumentId::from_str("BTCUSDT-PERP.BINANCE").expect("valid instrument id")
    }

    #[test]
    fn family_runtime_flags_enable_instruments_for_quote_only_plan() {
        let eth = eth_perp();
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            ..CapturePlan::default()
        };

        let flags = plan.family_runtime_flags();
        assert!(flags.instruments);
        assert!(flags.quotes);
        assert!(!flags.trades);
        assert_eq!(plan.enabled_background_worker_count(), 2);
    }

    #[test]
    fn family_runtime_flags_custom_data_only_has_single_worker() {
        let plan = CapturePlan {
            custom_data: vec![CustomDataCaptureSpec {
                data_type: DataType::new("DeribitVolatilityIndex", None, None),
            }],
            ..CapturePlan::default()
        };

        let flags = plan.family_runtime_flags();
        assert!(!flags.instruments);
        assert!(flags.custom_data);
        assert_eq!(plan.enabled_background_worker_count(), 1);
    }

    #[test]
    fn family_runtime_flags_keep_subscribe_and_request_custom_data_separate() {
        let request_only = CapturePlan {
            custom_data_requests: vec![CustomDataRequestCaptureSpec {
                data_type: DataType::new(
                    "DeribitBookSummary",
                    None,
                    Some("BTC:option".to_string()),
                ),
                interval_secs: 5,
                fire_immediately: true,
                overlap_policy: RequestOverlapPolicy::Skip,
                request_timeout_secs: 15,
                client_id: None,
            }],
            ..CapturePlan::default()
        };
        let request_flags = request_only.family_runtime_flags();
        assert!(!request_only.is_empty());
        assert!(!request_flags.custom_data);
        assert!(request_flags.custom_data_requests);
        assert!(request_flags.needs_custom_data_writer());
        assert_eq!(request_only.enabled_background_worker_count(), 1);

        let both = CapturePlan {
            custom_data: vec![CustomDataCaptureSpec {
                data_type: DataType::new("DeribitVolatilityIndex", None, None),
            }],
            custom_data_requests: request_only.custom_data_requests.clone(),
            ..CapturePlan::default()
        };
        let both_flags = both.family_runtime_flags();
        assert!(both_flags.custom_data);
        assert!(both_flags.custom_data_requests);
        // Still one CustomData parquet writer, not two workers.
        assert_eq!(both.enabled_background_worker_count(), 1);
    }

    #[test]
    fn planned_instrument_ids_dedupes_across_families() {
        let eth = eth_perp();
        let plan = CapturePlan {
            instruments: vec![InstrumentCaptureSpec { instrument_id: eth }],
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            mark_prices: vec![MarkPriceCaptureSpec { instrument_id: eth }],
            funding_rates: vec![FundingRateCaptureSpec { instrument_id: eth }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![eth]);
    }

    #[test]
    fn planned_instrument_ids_returns_sorted_unique_ids() {
        let eth = eth_perp();
        let btc = btc_perp();
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            trades: vec![TradeCaptureSpec { instrument_id: btc }],
            instrument_statuses: vec![InstrumentStatusCaptureSpec { instrument_id: eth }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![btc, eth]);
    }

    #[test]
    fn planned_instrument_ids_ignores_bars_and_custom_data() {
        let eth = eth_perp();
        let plan = CapturePlan {
            bars: vec![BarCaptureSpec {
                bar_type: BarType::from_str("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL").unwrap(),
            }],
            custom_data: vec![CustomDataCaptureSpec {
                data_type: DataType::new("SomeCustom", None, None),
            }],
            ..CapturePlan::default()
        };

        assert!(plan.planned_instrument_ids().is_empty());

        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            bars: vec![BarCaptureSpec {
                bar_type: BarType::from_str("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL").unwrap(),
            }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![eth]);
    }

    #[test]
    fn capture_plan_difference_tracks_added_and_removed_instruments() {
        let btc_perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let old_option = InstrumentId::from("BTC-20JUN26-62000-C.DERIBIT");
        let new_option = InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT");

        let previous = CapturePlan {
            instruments: vec![
                InstrumentCaptureSpec {
                    instrument_id: btc_perp,
                },
                InstrumentCaptureSpec {
                    instrument_id: old_option,
                },
            ],
            quotes: vec![
                QuoteCaptureSpec {
                    instrument_id: btc_perp,
                },
                QuoteCaptureSpec {
                    instrument_id: old_option,
                },
            ],
            index_prices: vec![IndexPriceCaptureSpec {
                instrument_id: btc_perp,
            }],
            funding_rates: vec![FundingRateCaptureSpec {
                instrument_id: btc_perp,
            }],
            ..CapturePlan::default()
        };
        let next = CapturePlan {
            instruments: vec![
                InstrumentCaptureSpec {
                    instrument_id: btc_perp,
                },
                InstrumentCaptureSpec {
                    instrument_id: new_option,
                },
            ],
            quotes: vec![
                QuoteCaptureSpec {
                    instrument_id: btc_perp,
                },
                QuoteCaptureSpec {
                    instrument_id: new_option,
                },
            ],
            index_prices: vec![IndexPriceCaptureSpec {
                instrument_id: btc_perp,
            }],
            funding_rates: vec![FundingRateCaptureSpec {
                instrument_id: btc_perp,
            }],
            ..CapturePlan::default()
        };

        let add = capture_plan_difference(&next, &previous);
        let remove = capture_plan_difference(&previous, &next);

        assert_eq!(plan_instrument_ids(&add), BTreeSet::from([new_option]));
        assert_eq!(plan_instrument_ids(&remove), BTreeSet::from([old_option]));
    }
}
