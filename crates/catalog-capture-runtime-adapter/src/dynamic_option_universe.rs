use std::{collections::BTreeSet, str::FromStr};

use anyhow::{Context, Result, bail};
use catalog_capture_core::{
    CapturePlan, MarkPriceCaptureSpec, OptionUniverseSpec, QuoteCaptureSpec,
    ResolvedOptionUniverse, expand_option_universe, merge_capture_plans, resolve_option_universe,
};
use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::PriceType,
    identifiers::{InstrumentId, Venue},
    instruments::InstrumentAny,
    types::Price,
};
use ustr::Ustr;

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseConfig {
    pub refresh_interval_secs: u64,
    pub static_plan: CapturePlan,
    pub initial_dynamic_plan: CapturePlan,
    pub universes: Vec<DynamicOptionUniverseEntryConfig>,
}

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseEntryConfig {
    pub venue: Venue,
    pub spec: OptionUniverseSpec,
    pub initial_plan: CapturePlan,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicOptionUniverseDelta {
    pub add: CapturePlan,
    pub remove: CapturePlan,
}

impl DynamicOptionUniverseDelta {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

#[derive(Debug, Clone)]
struct DynamicOptionUniverseState {
    venue: Venue,
    spec: OptionUniverseSpec,
    current_plan: CapturePlan,
}

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseManager {
    refresh_interval_secs: u64,
    current_dynamic_plan: CapturePlan,
    universes: Vec<DynamicOptionUniverseState>,
}

impl DynamicOptionUniverseManager {
    pub fn new(config: DynamicOptionUniverseConfig) -> Self {
        let universes = config
            .universes
            .into_iter()
            .map(|entry| DynamicOptionUniverseState {
                current_plan: entry.initial_plan,
                venue: entry.venue,
                spec: entry.spec,
            })
            .collect();

        Self {
            refresh_interval_secs: config.refresh_interval_secs,
            current_dynamic_plan: config.initial_dynamic_plan,
            universes,
        }
    }

    #[must_use]
    pub fn refresh_interval_secs(&self) -> u64 {
        self.refresh_interval_secs
    }

    pub fn refresh_from_cache(
        &mut self,
        cache: &Cache,
        now: UnixNanos,
    ) -> Result<DynamicOptionUniverseDelta> {
        let previous_dynamic_plan = self.current_dynamic_plan.clone();
        let mut next_dynamic_plan = CapturePlan::default();

        for state in &mut self.universes {
            match resolve_runtime_option_universe(cache, now, &state.spec, state.venue) {
                Ok(resolved) => {
                    let next_plan = expand_option_universe(&state.spec, &resolved);
                    let previous_ids = plan_instrument_ids(&state.current_plan);
                    let next_ids = plan_instrument_ids(&next_plan);
                    if next_ids != previous_ids {
                        println!(
                            "Refreshed option universe venue_id={} underlying={} instruments={} -> {}",
                            state.spec.venue_id,
                            state.spec.underlying,
                            previous_ids.len(),
                            next_ids.len(),
                        );
                    }
                    state.current_plan = next_plan.clone();
                    next_dynamic_plan = merge_capture_plans(&next_dynamic_plan, &next_plan);
                }
                Err(error) => {
                    eprintln!(
                        "Option universe refresh failed for venue_id={} underlying={}: {}",
                        state.spec.venue_id,
                        state.spec.underlying,
                        error,
                    );
                    next_dynamic_plan = merge_capture_plans(&next_dynamic_plan, &state.current_plan);
                }
            }
        }

        let delta = DynamicOptionUniverseDelta {
            add: capture_plan_difference(&next_dynamic_plan, &previous_dynamic_plan),
            remove: capture_plan_difference(&previous_dynamic_plan, &next_dynamic_plan),
        };
        self.current_dynamic_plan = next_dynamic_plan;
        Ok(delta)
    }
}

fn resolve_runtime_option_universe(
    cache: &Cache,
    now: UnixNanos,
    spec: &OptionUniverseSpec,
    venue: Venue,
) -> Result<ResolvedOptionUniverse> {
    if !venue.as_str().ends_with("DERIBIT") {
        bail!(
            "runtime option universe refresh currently supports Deribit only; got venue {}",
            venue
        );
    }

    let underlying = Ustr::from(spec.underlying.as_str());
    let option_instruments = cache
        .instruments(&venue, Some(&underlying))
        .into_iter()
        .cloned()
        .collect::<Vec<InstrumentAny>>();

    let reference_perp = derive_deribit_perpetual_id(&spec.underlying)?;
    let atm_reference = select_runtime_atm_reference(cache, reference_perp)
        .with_context(|| format!("failed to determine ATM reference from {}", reference_perp))?;
    let perp_instrument_id = spec.include_perp.then_some(reference_perp);

    Ok(resolve_option_universe(
        spec,
        &option_instruments,
        now,
        atm_reference,
        perp_instrument_id,
    )?)
}

fn select_runtime_atm_reference(cache: &Cache, instrument_id: InstrumentId) -> Result<Price> {
    if let Some(quote) = cache.quote(&instrument_id) {
        return Ok(quote.extract_price(PriceType::Mid));
    }
    if let Some(mark_price) = cache.mark_price(&instrument_id) {
        return Ok(mark_price.value);
    }
    if let Some(index_price) = cache.index_price(&instrument_id) {
        return Ok(index_price.value);
    }

    bail!("no quote/mark/index price available in cache")
}

fn derive_deribit_perpetual_id(underlying: &str) -> Result<InstrumentId> {
    Ok(InstrumentId::from_str(
        format!("{underlying}-PERPETUAL.DERIBIT").as_str(),
    )?)
}

fn capture_plan_difference(left: &CapturePlan, right: &CapturePlan) -> CapturePlan {
    CapturePlan {
        instruments: difference_by_instrument(&left.instruments, &right.instruments),
        quotes: difference_by_instrument(&left.quotes, &right.quotes),
        trades: difference_by_instrument(&left.trades, &right.trades),
        bars: left
            .bars
            .iter()
            .filter(|spec| !right.bars.contains(spec))
            .cloned()
            .collect(),
        book_deltas: left
            .book_deltas
            .iter()
            .filter(|spec| !right.book_deltas.contains(spec))
            .cloned()
            .collect(),
        mark_prices: difference_by_instrument(&left.mark_prices, &right.mark_prices),
        index_prices: difference_by_instrument(&left.index_prices, &right.index_prices),
        funding_rates: difference_by_instrument(&left.funding_rates, &right.funding_rates),
        instrument_statuses: difference_by_instrument(
            &left.instrument_statuses,
            &right.instrument_statuses,
        ),
        instrument_closes: difference_by_instrument(
            &left.instrument_closes,
            &right.instrument_closes,
        ),
        option_greeks: difference_by_instrument(&left.option_greeks, &right.option_greeks),
        custom_data: left
            .custom_data
            .iter()
            .filter(|spec| !right.custom_data.contains(spec))
            .cloned()
            .collect(),
    }
}

fn difference_by_instrument<T>(left: &[T], right: &[T]) -> Vec<T>
where
    T: Clone + PartialEq,
{
    left.iter()
        .filter(|spec| !right.contains(spec))
        .cloned()
        .collect()
}

pub fn plan_instrument_ids(plan: &CapturePlan) -> BTreeSet<InstrumentId> {
    plan.planned_instrument_ids().into_iter().collect()
}

pub fn plan_has_quotes(plan: &CapturePlan, instrument_id: InstrumentId) -> bool {
    plan.quotes
        .iter()
        .any(|spec: &QuoteCaptureSpec| spec.instrument_id == instrument_id)
}

pub fn plan_has_mark_prices(plan: &CapturePlan, instrument_id: InstrumentId) -> bool {
    plan.mark_prices
        .iter()
        .any(|spec: &MarkPriceCaptureSpec| spec.instrument_id == instrument_id)
}

pub fn plan_has_index_prices(plan: &CapturePlan, instrument_id: InstrumentId) -> bool {
    plan.index_prices
        .iter()
        .any(|spec| spec.instrument_id == instrument_id)
}

#[cfg(test)]
mod tests {
    use catalog_capture_core::{
        CapturePlan, ExpiryPolicy, FundingRateCaptureSpec, IndexPriceCaptureSpec,
        InstrumentCaptureSpec, OptionUniverseFamily, QuoteCaptureSpec, StrikePolicy,
    };

    use super::*;

    fn spec() -> OptionUniverseSpec {
        OptionUniverseSpec {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            settlement_currency: Some("BTC".to_string()),
            include_perp: true,
            families: vec![
                OptionUniverseFamily::Instruments,
                OptionUniverseFamily::Quotes,
                OptionUniverseFamily::OptionGreeks,
                OptionUniverseFamily::IndexPrices,
                OptionUniverseFamily::FundingRates,
            ],
            expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
            strike_policy: StrikePolicy::AtmRelative {
                strikes_above: 1,
                strikes_below: 1,
            },
        }
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

    #[test]
    fn plan_helpers_detect_reference_families() {
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: perp,
            }],
            mark_prices: vec![MarkPriceCaptureSpec {
                instrument_id: perp,
            }],
            index_prices: vec![IndexPriceCaptureSpec {
                instrument_id: perp,
            }],
            ..CapturePlan::default()
        };

        assert!(plan_has_quotes(&plan, perp));
        assert!(plan_has_mark_prices(&plan, perp));
        assert!(plan_has_index_prices(&plan, perp));
        assert_eq!(spec().underlying, "BTC");
    }
}
