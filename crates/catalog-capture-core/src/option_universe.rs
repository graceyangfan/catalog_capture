use std::{collections::BTreeMap, str::FromStr};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OptionKind,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
use thiserror::Error;

use crate::plan::{
    CapturePlan, FundingRateCaptureSpec, IndexPriceCaptureSpec, InstrumentCaptureSpec,
    InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec, MarkPriceCaptureSpec,
    OptionGreeksCaptureSpec, QuoteCaptureSpec, TradeCaptureSpec,
};

const DAY_NS: u64 = 86_400_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseFamily {
    Instruments,
    Quotes,
    Trades,
    MarkPrices,
    IndexPrices,
    FundingRates,
    InstrumentStatuses,
    InstrumentCloses,
    OptionGreeks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpiryPolicy {
    Nearest { days_max: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrikePolicy {
    AtmRelative {
        strikes_above: usize,
        strikes_below: usize,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseVenueKind {
    Deribit,
    Bybit,
    Okx,
}

impl OptionUniverseVenueKind {
    #[must_use]
    pub const fn supports_runtime_refresh(self) -> bool {
        matches!(self, Self::Deribit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionUniverseSpec {
    pub venue_id: String,
    pub underlying: String,
    pub settlement_currency: Option<String>,
    pub include_perp: bool,
    pub families: Vec<OptionUniverseFamily>,
    pub expiry_policy: ExpiryPolicy,
    pub strike_policy: StrikePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedOptionUniverse {
    pub resolved_at_ns: UnixNanos,
    pub selected_expiry_ns: UnixNanos,
    pub atm_reference: Price,
    pub selected_strikes: Vec<Price>,
    pub perp_instrument_id: Option<InstrumentId>,
    pub option_instrument_ids: Vec<InstrumentId>,
    pub all_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OptionUniverseResolveError {
    #[error(
        "no matching option instruments found for venue_id={venue_id} underlying={underlying}"
    )]
    NoMatchingOptions {
        venue_id: String,
        underlying: String,
    },
    #[error("no expiry matched the configured expiry policy for venue_id={venue_id} underlying={underlying}")]
    NoMatchingExpiry {
        venue_id: String,
        underlying: String,
    },
    #[error("no call/put pairs remained after strike filtering for venue_id={venue_id} underlying={underlying}")]
    NoStrikePairs {
        venue_id: String,
        underlying: String,
    },
    #[error("include_perp=true requires a resolved perpetual instrument for venue_id={venue_id} underlying={underlying}")]
    MissingPerpetual {
        venue_id: String,
        underlying: String,
    },
    #[error("capture.option_universe venue_id={venue_id} requires settlement_currency to derive the hedge instrument")]
    MissingSettlementCurrency { venue_id: String },
    #[error("no option instrument matched the configured expiry policy for venue_id={venue_id} underlying={underlying}")]
    NoReferenceInstrument {
        venue_id: String,
        underlying: String,
    },
}

pub fn okx_instrument_family(spec: &OptionUniverseSpec) -> Result<String, OptionUniverseResolveError> {
    let Some(settlement_currency) = spec.settlement_currency.as_deref() else {
        return Err(OptionUniverseResolveError::MissingSettlementCurrency {
            venue_id: spec.venue_id.clone(),
        });
    };
    Ok(format!("{}-{settlement_currency}", spec.underlying))
}

pub fn derive_perp_instrument_id(
    spec: &OptionUniverseSpec,
    venue: OptionUniverseVenueKind,
) -> Result<InstrumentId, OptionUniverseResolveError> {
    let instrument_id = match venue {
        OptionUniverseVenueKind::Deribit => {
            format!("{}-PERPETUAL.DERIBIT", spec.underlying)
        }
        OptionUniverseVenueKind::Bybit => {
            let settlement_currency = spec.settlement_currency.as_deref().ok_or_else(|| {
                OptionUniverseResolveError::MissingSettlementCurrency {
                    venue_id: spec.venue_id.clone(),
                }
            })?;
            format!("{}{}-LINEAR.BYBIT", spec.underlying, settlement_currency)
        }
        OptionUniverseVenueKind::Okx => {
            format!("{}-SWAP.OKX", okx_instrument_family(spec)?)
        }
    };

    InstrumentId::from_str(instrument_id.as_str()).map_err(|_| {
        OptionUniverseResolveError::MissingPerpetual {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        }
    })
}

pub fn select_nearest_expiry_reference_instrument_id(
    spec: &OptionUniverseSpec,
    instruments: &[InstrumentAny],
    now: UnixNanos,
) -> Result<InstrumentId, OptionUniverseResolveError> {
    let matching_options = collect_matching_options(spec, instruments, now);
    if matching_options.is_empty() {
        return Err(OptionUniverseResolveError::NoMatchingOptions {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        });
    }

    let selected_expiry_ns = select_expiry(spec, &matching_options, now)?;
    matching_options
        .iter()
        .filter(|option| option.expiration_ns == selected_expiry_ns)
        .map(|option| option.instrument_id)
        .min()
        .ok_or_else(|| OptionUniverseResolveError::NoReferenceInstrument {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        })
}

pub fn resolve_option_universe(
    spec: &OptionUniverseSpec,
    instruments: &[InstrumentAny],
    now: UnixNanos,
    atm_reference: Price,
    perp_instrument_id: Option<InstrumentId>,
) -> Result<ResolvedOptionUniverse, OptionUniverseResolveError> {
    let matching_options = collect_matching_options(spec, instruments, now);
    if matching_options.is_empty() {
        return Err(OptionUniverseResolveError::NoMatchingOptions {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        });
    }

    let selected_expiry_ns = select_expiry(spec, &matching_options, now)?;
    let strike_pairs = collect_strike_pairs(&matching_options, selected_expiry_ns);
    let paired_strikes = strike_pairs
        .iter()
        .filter_map(|(strike, pair)| pair.as_complete_pair().map(|_| *strike))
        .collect::<Vec<_>>();
    if paired_strikes.is_empty() {
        return Err(OptionUniverseResolveError::NoStrikePairs {
            venue_id: spec.venue_id.clone(),
            underlying: spec.underlying.clone(),
        });
    }

    let selected_strikes = select_strikes(spec, &paired_strikes, atm_reference);
    let mut option_instrument_ids = Vec::new();
    for strike in &selected_strikes {
        let pair = strike_pairs
            .get(strike)
            .expect("selected strikes must exist in strike_pairs");
        let (call, put) = pair
            .as_complete_pair()
            .expect("selected strikes must have complete call/put pairs");
        option_instrument_ids.push(call);
        option_instrument_ids.push(put);
    }

    let perp_instrument_id = match (spec.include_perp, perp_instrument_id) {
        (true, Some(instrument_id)) => Some(instrument_id),
        (true, None) => {
            return Err(OptionUniverseResolveError::MissingPerpetual {
                venue_id: spec.venue_id.clone(),
                underlying: spec.underlying.clone(),
            });
        }
        (false, _) => None,
    };

    let mut all_instrument_ids = option_instrument_ids.clone();
    if let Some(instrument_id) = perp_instrument_id {
        all_instrument_ids.push(instrument_id);
    }
    all_instrument_ids.sort();
    all_instrument_ids.dedup();

    Ok(ResolvedOptionUniverse {
        resolved_at_ns: now,
        selected_expiry_ns,
        atm_reference,
        selected_strikes,
        perp_instrument_id,
        option_instrument_ids,
        all_instrument_ids,
    })
}

pub fn expand_option_universe(
    spec: &OptionUniverseSpec,
    resolved: &ResolvedOptionUniverse,
) -> CapturePlan {
    let mut plan = CapturePlan::default();

    for family in &spec.families {
        match family {
            OptionUniverseFamily::Instruments => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instruments.push(InstrumentCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::Quotes => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.quotes.push(QuoteCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::Trades => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.trades.push(TradeCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::MarkPrices => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.mark_prices.push(MarkPriceCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::IndexPrices => {
                if let Some(instrument_id) = resolved.perp_instrument_id {
                    plan.index_prices
                        .push(IndexPriceCaptureSpec { instrument_id });
                }
            }
            OptionUniverseFamily::FundingRates => {
                if let Some(instrument_id) = resolved.perp_instrument_id {
                    plan.funding_rates
                        .push(FundingRateCaptureSpec { instrument_id });
                }
            }
            OptionUniverseFamily::InstrumentStatuses => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instrument_statuses.push(InstrumentStatusCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::InstrumentCloses => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instrument_closes.push(InstrumentCloseCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            OptionUniverseFamily::OptionGreeks => {
                for instrument_id in &resolved.option_instrument_ids {
                    plan.option_greeks.push(OptionGreeksCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
        }
    }

    plan
}

pub fn merge_capture_plans(base: &CapturePlan, addition: &CapturePlan) -> CapturePlan {
    let mut merged = base.clone();
    extend_unique(
        &mut merged.instruments,
        addition.instruments.iter().cloned(),
    );
    extend_unique(&mut merged.quotes, addition.quotes.iter().cloned());
    extend_unique(&mut merged.trades, addition.trades.iter().cloned());
    extend_unique(&mut merged.bars, addition.bars.iter().cloned());
    extend_unique(
        &mut merged.book_deltas,
        addition.book_deltas.iter().cloned(),
    );
    extend_unique(
        &mut merged.mark_prices,
        addition.mark_prices.iter().cloned(),
    );
    extend_unique(
        &mut merged.index_prices,
        addition.index_prices.iter().cloned(),
    );
    extend_unique(
        &mut merged.funding_rates,
        addition.funding_rates.iter().cloned(),
    );
    extend_unique(
        &mut merged.instrument_statuses,
        addition.instrument_statuses.iter().cloned(),
    );
    extend_unique(
        &mut merged.instrument_closes,
        addition.instrument_closes.iter().cloned(),
    );
    extend_unique(
        &mut merged.option_greeks,
        addition.option_greeks.iter().cloned(),
    );
    extend_unique(
        &mut merged.custom_data,
        addition.custom_data.iter().cloned(),
    );
    merged
}

fn extend_unique<T>(target: &mut Vec<T>, items: impl IntoIterator<Item = T>)
where
    T: PartialEq,
{
    for item in items {
        if !target.contains(&item) {
            target.push(item);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct OptionInstrumentRef {
    instrument_id: InstrumentId,
    expiration_ns: UnixNanos,
    strike: Price,
    kind: OptionKind,
}

#[derive(Debug, Default, Clone, Copy)]
struct StrikePair {
    call: Option<InstrumentId>,
    put: Option<InstrumentId>,
}

impl StrikePair {
    fn as_complete_pair(&self) -> Option<(InstrumentId, InstrumentId)> {
        match (self.call, self.put) {
            (Some(call), Some(put)) => Some((call, put)),
            _ => None,
        }
    }
}

fn collect_matching_options(
    spec: &OptionUniverseSpec,
    instruments: &[InstrumentAny],
    now: UnixNanos,
) -> Vec<OptionInstrumentRef> {
    let settlement_currency = spec.settlement_currency.as_deref();

    instruments
        .iter()
        .filter_map(|instrument| {
            if !matches!(instrument, InstrumentAny::CryptoOption(_)) {
                return None;
            }

            let underlying = instrument.underlying()?;
            if underlying.as_str() != spec.underlying {
                return None;
            }

            if let Some(expected_settlement) = settlement_currency {
                if instrument.settlement_currency().code.as_str() != expected_settlement {
                    return None;
                }
            }

            let expiration_ns = instrument.expiration_ns()?;
            if expiration_ns <= now {
                return None;
            }

            Some(OptionInstrumentRef {
                instrument_id: instrument.id(),
                expiration_ns,
                strike: instrument.strike_price()?,
                kind: instrument.option_kind()?,
            })
        })
        .collect()
}

fn select_expiry(
    spec: &OptionUniverseSpec,
    options: &[OptionInstrumentRef],
    now: UnixNanos,
) -> Result<UnixNanos, OptionUniverseResolveError> {
    match spec.expiry_policy {
        ExpiryPolicy::Nearest { days_max } => {
            let max_delta_ns = u64::from(days_max) * DAY_NS;
            options
                .iter()
                .map(|instrument| instrument.expiration_ns)
                .filter(|expiry| expiry.as_u64().saturating_sub(now.as_u64()) <= max_delta_ns)
                .min()
                .ok_or_else(|| OptionUniverseResolveError::NoMatchingExpiry {
                    venue_id: spec.venue_id.clone(),
                    underlying: spec.underlying.clone(),
                })
        }
    }
}

fn collect_strike_pairs(
    options: &[OptionInstrumentRef],
    selected_expiry_ns: UnixNanos,
) -> BTreeMap<Price, StrikePair> {
    let mut strike_pairs: BTreeMap<Price, StrikePair> = BTreeMap::new();

    for option in options {
        if option.expiration_ns != selected_expiry_ns {
            continue;
        }

        let pair = strike_pairs.entry(option.strike).or_default();
        match option.kind {
            OptionKind::Call => pair.call = Some(option.instrument_id),
            OptionKind::Put => pair.put = Some(option.instrument_id),
        }
    }

    strike_pairs
}

fn select_strikes(
    spec: &OptionUniverseSpec,
    strikes: &[Price],
    atm_reference: Price,
) -> Vec<Price> {
    match spec.strike_policy {
        StrikePolicy::AtmRelative {
            strikes_above,
            strikes_below,
        } => {
            let atm = atm_reference.as_f64();
            let atm_index = strikes
                .iter()
                .enumerate()
                .min_by(|(_, left), (_, right)| {
                    let left_key = ((left.as_f64() - atm).abs(), left.as_f64());
                    let right_key = ((right.as_f64() - atm).abs(), right.as_f64());
                    left_key
                        .partial_cmp(&right_key)
                        .expect("finite strike distances")
                })
                .map(|(index, _)| index)
                .expect("at least one strike exists");

            let start = atm_index.saturating_sub(strikes_below);
            let end = (atm_index + strikes_above).min(strikes.len() - 1);
            strikes[start..=end].to_vec()
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::OptionKind,
        identifiers::{InstrumentId, Symbol},
        instruments::{CryptoOption, InstrumentAny},
        types::{Currency, Money, Price, Quantity},
    };

    use super::*;

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
        ]
    }

    fn make_spec() -> OptionUniverseSpec {
        OptionUniverseSpec {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            settlement_currency: Some("BTC".to_string()),
            include_perp: true,
            families: vec![
                OptionUniverseFamily::Instruments,
                OptionUniverseFamily::Quotes,
                OptionUniverseFamily::OptionGreeks,
                OptionUniverseFamily::FundingRates,
                OptionUniverseFamily::IndexPrices,
            ],
            expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
            strike_policy: StrikePolicy::AtmRelative {
                strikes_above: 1,
                strikes_below: 1,
            },
        }
    }

    #[test]
    fn resolve_option_universe_selects_nearest_expiry_and_atm_window() {
        let resolved = resolve_option_universe(
            &make_spec(),
            &make_btc_option_set(),
            UnixNanos::from(1_781_740_800_000_000_000u64),
            Price::from("65100"),
            Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
        )
        .expect("universe should resolve");

        assert_eq!(
            resolved.selected_expiry_ns,
            UnixNanos::from(1_782_432_000_000_000_000u64)
        );
        assert_eq!(
            resolved.selected_strikes,
            vec![
                Price::from("64000"),
                Price::from("65000"),
                Price::from("66000")
            ]
        );
        assert!(resolved
            .all_instrument_ids
            .contains(&InstrumentId::from("BTC-PERPETUAL.DERIBIT")));
        assert_eq!(resolved.option_instrument_ids.len(), 6);
    }

    #[test]
    fn resolve_option_universe_prefers_lower_strike_on_atm_tie() {
        let resolved = resolve_option_universe(
            &make_spec(),
            &make_btc_option_set(),
            UnixNanos::from(1_781_740_800_000_000_000u64),
            Price::from("64500"),
            Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
        )
        .expect("universe should resolve");

        assert_eq!(
            resolved.selected_strikes,
            vec![Price::from("64000"), Price::from("65000")]
        );
    }

    #[test]
    fn expand_option_universe_limits_perp_only_families_to_perp() {
        let resolved = resolve_option_universe(
            &make_spec(),
            &make_btc_option_set(),
            UnixNanos::from(1_781_740_800_000_000_000u64),
            Price::from("65100"),
            Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
        )
        .expect("universe should resolve");

        let expanded = expand_option_universe(&make_spec(), &resolved);

        assert_eq!(expanded.option_greeks.len(), 6);
        assert_eq!(
            expanded.index_prices,
            vec![IndexPriceCaptureSpec {
                instrument_id: InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            }]
        );
        assert_eq!(
            expanded.funding_rates,
            vec![FundingRateCaptureSpec {
                instrument_id: InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            }]
        );
    }

    #[test]
    fn derive_perp_instrument_id_builds_expected_symbols() {
        let spec = make_spec();
        assert_eq!(
            derive_perp_instrument_id(&spec, OptionUniverseVenueKind::Deribit).expect("deribit"),
            InstrumentId::from("BTC-PERPETUAL.DERIBIT")
        );

        let bybit_spec = OptionUniverseSpec {
            settlement_currency: Some("USDT".to_string()),
            ..make_spec()
        };
        assert_eq!(
            derive_perp_instrument_id(&bybit_spec, OptionUniverseVenueKind::Bybit).expect("bybit"),
            InstrumentId::from("BTCUSDT-LINEAR.BYBIT")
        );

        let okx_spec = OptionUniverseSpec {
            venue_id: "okx_main".to_string(),
            settlement_currency: Some("USD".to_string()),
            ..make_spec()
        };
        assert_eq!(
            derive_perp_instrument_id(&okx_spec, OptionUniverseVenueKind::Okx).expect("okx"),
            InstrumentId::from("BTC-USD-SWAP.OKX")
        );
    }

    #[test]
    fn select_nearest_expiry_reference_instrument_id_picks_nearest_expiry() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let reference = select_nearest_expiry_reference_instrument_id(
            &make_spec(),
            &make_btc_option_set(),
            now,
        )
        .expect("reference instrument should resolve");

        assert_eq!(
            reference,
            InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT")
        );
    }

    #[test]
    fn merge_capture_plans_dedupes_expanded_specs() {
        let base = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            }],
            ..CapturePlan::default()
        };
        let addition = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            }],
            option_greeks: vec![OptionGreeksCaptureSpec {
                instrument_id: InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
            }],
            ..CapturePlan::default()
        };

        let merged = merge_capture_plans(&base, &addition);

        assert_eq!(merged.quotes.len(), 1);
        assert_eq!(merged.option_greeks.len(), 1);
    }
}
