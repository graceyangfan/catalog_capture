use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    derive_perp_instrument_id, expand_option_universe, merge_capture_plans, resolve_option_universe,
    CapturePlan, MarkPriceCaptureSpec, OptionUniverseSpec, OptionUniverseVenueKind,
    QuoteCaptureSpec, ResolvedOptionUniverse,
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
    pub venue_kind: OptionUniverseVenueKind,
    pub spec: OptionUniverseSpec,
    pub initial_plan: CapturePlan,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicOptionUniverseDelta {
    pub add: CapturePlan,
    pub remove: CapturePlan,
    pub changes: Vec<DynamicOptionUniverseChange>,
}

impl DynamicOptionUniverseDelta {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicOptionUniverseChange {
    pub venue_id: String,
    pub underlying: String,
    pub selected_expiry_iso8601: String,
    pub perp_instrument_id: Option<InstrumentId>,
    pub option_instrument_ids: Vec<InstrumentId>,
    pub previous_count: usize,
    pub next_count: usize,
    pub added_instrument_ids: Vec<InstrumentId>,
    pub removed_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone)]
struct DynamicOptionUniverseState {
    venue: Venue,
    venue_kind: OptionUniverseVenueKind,
    spec: OptionUniverseSpec,
    current_plan: CapturePlan,
}

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseManager {
    refresh_interval_secs: u64,
    static_plan: CapturePlan,
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
                venue_kind: entry.venue_kind,
                spec: entry.spec,
            })
            .collect();

        Self {
            refresh_interval_secs: config.refresh_interval_secs,
            static_plan: config.static_plan,
            current_dynamic_plan: config.initial_dynamic_plan,
            universes,
        }
    }

    #[must_use]
    pub fn refresh_interval_secs(&self) -> u64 {
        self.refresh_interval_secs
    }

    #[must_use]
    pub fn active_capture_plan(&self) -> CapturePlan {
        merge_capture_plans(&self.static_plan, &self.current_dynamic_plan)
    }

    pub fn refresh_from_cache(
        &mut self,
        cache: &Cache,
        now: UnixNanos,
    ) -> Result<DynamicOptionUniverseDelta> {
        let previous_dynamic_plan = self.current_dynamic_plan.clone();
        let mut next_dynamic_plan = CapturePlan::default();
        let mut changes = Vec::new();

        for state in &mut self.universes {
            match resolve_runtime_option_universe(
                cache,
                now,
                &state.spec,
                state.venue,
                state.venue_kind,
            ) {
                Ok(resolved) => {
                    let next_plan = expand_option_universe(&state.spec, &resolved);
                    let previous_ids = plan_instrument_ids(&state.current_plan);
                    let next_ids = plan_instrument_ids(&next_plan);
                    if next_ids != previous_ids {
                        changes.push(DynamicOptionUniverseChange {
                            venue_id: state.spec.venue_id.clone(),
                            underlying: state.spec.underlying.clone(),
                            selected_expiry_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
                                resolved.selected_expiry_ns,
                            ),
                            perp_instrument_id: resolved.perp_instrument_id,
                            option_instrument_ids: resolved.option_instrument_ids.clone(),
                            previous_count: previous_ids.len(),
                            next_count: next_ids.len(),
                            added_instrument_ids: instrument_id_difference(
                                &next_ids,
                                &previous_ids,
                            ),
                            removed_instrument_ids: instrument_id_difference(
                                &previous_ids,
                                &next_ids,
                            ),
                        });
                    }
                    state.current_plan = next_plan.clone();
                    next_dynamic_plan = merge_capture_plans(&next_dynamic_plan, &next_plan);
                }
                Err(error) => {
                    eprintln!(
                        "Option universe refresh failed for venue_id={} underlying={}: {}",
                        state.spec.venue_id, state.spec.underlying, error,
                    );
                    next_dynamic_plan =
                        merge_capture_plans(&next_dynamic_plan, &state.current_plan);
                }
            }
        }

        let delta = DynamicOptionUniverseDelta {
            add: capture_plan_difference(&next_dynamic_plan, &previous_dynamic_plan),
            remove: capture_plan_difference(&previous_dynamic_plan, &next_dynamic_plan),
            changes,
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
    venue_kind: OptionUniverseVenueKind,
) -> Result<ResolvedOptionUniverse> {
    if !venue_kind.supports_runtime_refresh() {
        bail!(
            "runtime option universe refresh is not supported for venue kind {:?} (venue_id={})",
            venue_kind,
            spec.venue_id
        );
    }

    let underlying = Ustr::from(spec.underlying.as_str());
    let option_instruments = cache
        .instruments(&venue, Some(&underlying))
        .into_iter()
        .cloned()
        .collect::<Vec<InstrumentAny>>();

    let reference_perp = derive_perp_instrument_id(spec, venue_kind).map_err(anyhow::Error::from)?;
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

fn instrument_id_difference(
    left: &BTreeSet<InstrumentId>,
    right: &BTreeSet<InstrumentId>,
) -> Vec<InstrumentId> {
    left.difference(right).copied().collect()
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
    use nautilus_common::cache::Cache;
    use nautilus_model::{
        data::QuoteTick,
        enums::OptionKind,
        identifiers::Symbol,
        instruments::{CryptoOption, CryptoPerpetual, InstrumentAny},
        types::{Currency, Money, Quantity},
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

    fn make_deribit_option(
        symbol: &str,
        strike: &str,
        kind: OptionKind,
        expiration_ns: u64,
    ) -> InstrumentAny {
        InstrumentAny::CryptoOption(CryptoOption::new(
            InstrumentId::from(format!("{symbol}.DERIBIT").as_str()),
            Symbol::from(symbol),
            Currency::from("BTC"),
            Currency::from("USD"),
            Currency::from("BTC"),
            false,
            kind,
            Price::from(strike),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            UnixNanos::from(expiration_ns),
            3,
            1,
            Price::from("0.001"),
            Quantity::from("0.1"),
            Some(Quantity::from(1)),
            Some(Quantity::from("0.1")),
            None,
            Some(Quantity::from("0.1")),
            None,
            Some(Money::new(10.0, Currency::from("USD"))),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.into(),
            0.into(),
        ))
    }

    fn make_btc_option_set() -> Vec<InstrumentAny> {
        vec![
            make_deribit_option(
                "BTC-26JUN26-64000-C",
                "64000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-64000-P",
                "64000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-65000-C",
                "65000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-65000-P",
                "65000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-66000-C",
                "66000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-66000-P",
                "66000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-67000-C",
                "67000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-67000-P",
                "67000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
        ]
    }

    fn make_deribit_perpetual() -> InstrumentAny {
        InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            Symbol::from("BTC-PERPETUAL"),
            Currency::from("BTC"),
            Currency::from("USD"),
            Currency::from("BTC"),
            false,
            1,
            1,
            Price::from("0.5"),
            Quantity::from("0.1"),
            None,
            None,
            None,
            None,
            Some(Money::from("10 USD")),
            Some(Money::from("1 USD")),
            Some(Price::from("1000000")),
            Some(Price::from("1")),
            None,
            None,
            None,
            None,
            None,
            None,
            UnixNanos::default(),
            UnixNanos::default(),
        ))
    }

    fn make_quote(instrument_id: InstrumentId, bid: &str, ask: &str) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::default(),
            UnixNanos::default(),
        )
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

    #[test]
    fn refresh_from_cache_reports_atm_rotation_change() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let mut cache = Cache::default();

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();

        let missing_reference_error = resolve_runtime_option_universe(
            &cache,
            now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap_err();
        assert!(missing_reference_error
            .to_string()
            .contains("failed to determine ATM reference"));

        cache.add_quote(make_quote(perp, "64990", "65010")).unwrap();

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();
        let initial_plan = expand_option_universe(&spec(), &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Deribit,
                spec: spec(),
                initial_plan,
            }],
        });

        cache.add_quote(make_quote(perp, "65990", "66010")).unwrap();
        let delta = manager.refresh_from_cache(&cache, now).unwrap();

        assert_eq!(delta.changes.len(), 1);
        let change = &delta.changes[0];
        assert_eq!(change.venue_id, "deribit_main");
        assert_eq!(change.underlying, "BTC");
        assert_eq!(change.previous_count, 7);
        assert_eq!(change.next_count, 7);
        assert_eq!(
            change.added_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-67000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-67000-P.DERIBIT"),
            ]
        );
        assert_eq!(
            change.removed_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
            ]
        );
        assert_eq!(change.option_instrument_ids.len(), 6);
        assert_eq!(
            plan_instrument_ids(&delta.add),
            BTreeSet::from([
                InstrumentId::from("BTC-26JUN26-67000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-67000-P.DERIBIT"),
            ]),
        );
        assert_eq!(
            plan_instrument_ids(&delta.remove),
            BTreeSet::from([
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
            ]),
        );
    }

    #[test]
    fn refresh_from_cache_discovers_new_expiry_instruments_added_later() {
        let initial_now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let later_now = UnixNanos::from(1_782_500_000_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let mut cache = Cache::default();

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();
        cache.add_quote(make_quote(perp, "64990", "65010")).unwrap();

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            initial_now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();
        let initial_plan = expand_option_universe(&spec(), &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Deribit,
                spec: spec(),
                initial_plan,
            }],
        });

        for instrument in [
            make_deribit_option(
                "BTC-03JUL26-64000-C",
                "64000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-64000-P",
                "64000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-65000-C",
                "65000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-65000-P",
                "65000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-66000-C",
                "66000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-66000-P",
                "66000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
        ] {
            cache.add_instrument(instrument).unwrap();
        }

        let delta = manager.refresh_from_cache(&cache, later_now).unwrap();

        assert_eq!(delta.changes.len(), 1);
        assert_eq!(
            delta.changes[0].added_instrument_ids,
            vec![
                InstrumentId::from("BTC-03JUL26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-64000-P.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-65000-P.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-66000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-66000-P.DERIBIT"),
            ],
        );
        assert_eq!(
            delta.changes[0].removed_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-66000-P.DERIBIT"),
            ],
        );
    }
}
