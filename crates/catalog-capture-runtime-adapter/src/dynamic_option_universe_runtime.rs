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

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    aggregate_open_interest_by_strike, derive_perp_instrument_id,
    option_instrument_ids_at_selected_expiry, resolve_option_universe,
    select_cache_perp_strike_fallback, AtmReferenceSource, CapturePlan, MarkPriceCaptureSpec,
    OptionUniverseSpec, OptionUniverseVenueKind, QuoteCaptureSpec, ResolvedOptionUniverse,
    StrikeOpenInterestByStrike,
};
use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::PriceType,
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::Price,
};
use ustr::Ustr;

pub fn resolve_runtime_option_universe(
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

    let option_instruments = cached_option_instruments(cache, venue, &spec.underlying);
    let (atm_reference, atm_reference_source) = select_runtime_strike_reference(
        cache, spec, venue, venue_kind, now,
    )
    .with_context(|| {
        format!(
            "failed to determine strike reference for venue_id={} underlying={}",
            spec.venue_id, spec.underlying
        )
    })?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_perp_instrument_id(spec, venue_kind).map_err(anyhow::Error::from))
        .transpose()?;

    let open_interest_by_strike = if spec.strike_policy.requires_open_interest() {
        Some(select_runtime_strike_open_interest(
            cache,
            spec,
            venue,
            now,
            &option_instruments,
        )?)
    } else {
        None
    };

    let mut resolved = resolve_option_universe(
        spec,
        &option_instruments,
        now,
        atm_reference,
        perp_instrument_id,
        open_interest_by_strike.as_ref(),
    )?;
    resolved.atm_reference_source = Some(atm_reference_source);
    Ok(resolved)
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

fn cached_option_instruments(cache: &Cache, venue: Venue, underlying: &str) -> Vec<InstrumentAny> {
    let underlying = Ustr::from(underlying);
    cache
        .instruments(&venue, Some(&underlying))
        .into_iter()
        .cloned()
        .collect()
}

fn select_runtime_strike_open_interest(
    cache: &Cache,
    spec: &OptionUniverseSpec,
    _venue: Venue,
    now: UnixNanos,
    option_instruments: &[InstrumentAny],
) -> Result<StrikeOpenInterestByStrike> {
    let (_, instrument_ids) =
        option_instrument_ids_at_selected_expiry(spec, option_instruments, now)
            .map_err(anyhow::Error::from)?;

    let mut entries = Vec::new();
    for instrument_id in instrument_ids {
        let Some(greeks) = cache.option_greeks(&instrument_id) else {
            continue;
        };
        let Some(open_interest) = greeks.open_interest else {
            continue;
        };
        if !open_interest.is_finite() || open_interest <= 0.0 {
            continue;
        }
        let Some(instrument) = option_instruments
            .iter()
            .find(|entry| entry.id() == instrument_id)
        else {
            continue;
        };
        let Some(strike) = instrument.strike_price() else {
            continue;
        };
        entries.push((strike, open_interest));
    }

    Ok(aggregate_open_interest_by_strike(entries))
}

fn select_runtime_strike_reference(
    cache: &Cache,
    spec: &OptionUniverseSpec,
    venue: Venue,
    venue_kind: OptionUniverseVenueKind,
    now: UnixNanos,
) -> Result<(Price, String)> {
    let option_instruments = cached_option_instruments(cache, venue, &spec.underlying);
    let (_, instrument_ids) =
        option_instrument_ids_at_selected_expiry(spec, &option_instruments, now)
            .map_err(anyhow::Error::from)?;

    for instrument_id in instrument_ids {
        let Some(greeks) = cache.option_greeks(&instrument_id) else {
            continue;
        };
        let Some(underlying_price) = greeks.underlying_price else {
            continue;
        };
        let price = Price::from(format!("{underlying_price}").as_str());
        return Ok((
            price,
            AtmReferenceSource::CacheGreeksUnderlyingPrice
                .as_str()
                .to_string(),
        ));
    }

    let reference_perp =
        derive_perp_instrument_id(spec, venue_kind).map_err(anyhow::Error::from)?;
    let quote_mid = cache
        .quote(&reference_perp)
        .map(|quote| quote.extract_price(PriceType::Mid));
    let mark = cache.mark_price(&reference_perp).map(|update| update.value);
    let index = cache
        .index_price(&reference_perp)
        .map(|update| update.value);
    if let Some((price, source)) = select_cache_perp_strike_fallback(mark, quote_mid, index) {
        return Ok((price, source.as_str().to_string()));
    }

    bail!(
        "no option greeks underlying_price or perp fallback reference available for venue_id={} underlying={}",
        spec.venue_id,
        spec.underlying
    )
}
