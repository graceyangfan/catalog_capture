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

use std::str::FromStr;

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    aggregate_open_interest_by_strike, derive_perp_instrument_id, okx_instrument_family,
    option_instrument_ids_at_selected_expiry, resolve_option_universe,
    select_nearest_expiry_reference_instrument_id, select_strike_reference_from_decimal_string,
    AtmReferenceSource, OptionUniverseSpec, OptionUniverseVenueKind, ResolvedOptionUniverse,
    StrikeOpenInterestByStrike,
};
use nautilus_bybit::common::enums::BybitProductType;
use nautilus_bybit::{
    common::{enums::BybitEnvironment, parse::extract_raw_symbol, urls::bybit_http_base_url},
    http::{client::BybitHttpClient, query::BybitTickersParams},
};
use nautilus_deribit::{
    common::enums::{DeribitCurrency, DeribitEnvironment},
    http::{client::DeribitHttpClient, models::DeribitProductType},
};
use nautilus_model::instruments::{Instrument, InstrumentAny};
use nautilus_model::{identifiers::InstrumentId, types::Price};
use nautilus_okx::{
    common::enums::{OKXEnvironment, OKXInstrumentType},
    http::client::OKXHttpClient,
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::config::VenueRuntimeConfig;

use super::validate::resolve_option_universe_venue;

pub async fn resolve_option_universe_spec(
    spec: &OptionUniverseSpec,
    venues: &[VenueRuntimeConfig],
) -> Result<ResolvedOptionUniverse> {
    let venue = resolve_option_universe_venue(spec, venues)?;
    dispatch_option_universe_resolution(spec, venue).await
}

async fn dispatch_option_universe_resolution(
    spec: &OptionUniverseSpec,
    venue: &VenueRuntimeConfig,
) -> Result<ResolvedOptionUniverse> {
    match venue {
        VenueRuntimeConfig::Deribit { environment, .. } => {
            resolve_deribit_option_universe(spec, *environment).await
        }
        VenueRuntimeConfig::Bybit { environment, .. } => {
            resolve_bybit_option_universe(spec, *environment).await
        }
        VenueRuntimeConfig::Okx { environment, .. } => {
            resolve_okx_option_universe(spec, *environment).await
        }
        _ => bail!(
            "capture.option_universe currently only supports Deribit/Bybit/OKX venues; got venue_id `{}`",
            spec.venue_id
        ),
    }
}

struct ResolvedVenueInputs {
    resolved_at_ns: nautilus_core::UnixNanos,
    spec_for_resolution: OptionUniverseSpec,
    option_instruments: Vec<InstrumentAny>,
    atm_reference: Price,
    atm_reference_source: String,
    perp_instrument_id: Option<InstrumentId>,
    open_interest_by_strike: Option<StrikeOpenInterestByStrike>,
}

fn finalize_option_universe_resolution(
    inputs: ResolvedVenueInputs,
) -> Result<ResolvedOptionUniverse> {
    let open_interest_by_strike = if inputs
        .spec_for_resolution
        .strike_policy
        .requires_open_interest()
    {
        Some(inputs.open_interest_by_strike.as_ref().with_context(|| {
            format!(
                "missing startup open interest map for venue_id={} underlying={}",
                inputs.spec_for_resolution.venue_id, inputs.spec_for_resolution.underlying
            )
        })?)
    } else {
        None
    };
    let mut resolved = resolve_option_universe(
        &inputs.spec_for_resolution,
        &inputs.option_instruments,
        inputs.resolved_at_ns,
        inputs.atm_reference,
        inputs.perp_instrument_id,
        open_interest_by_strike,
    )
    .map_err(anyhow::Error::from)?;
    resolved.atm_reference_source = Some(inputs.atm_reference_source);
    Ok(resolved)
}

fn normalized_resolution_spec(
    spec: &OptionUniverseSpec,
    clear_settlement_currency: bool,
) -> OptionUniverseSpec {
    let mut normalized = spec.clone();
    if clear_settlement_currency {
        normalized.settlement_currency = None;
    }
    normalized
}

async fn resolve_deribit_option_universe(
    spec: &OptionUniverseSpec,
    environment: DeribitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let currency = DeribitCurrency::from_str(&spec.underlying).with_context(|| {
        format!(
            "unsupported Deribit option underlying `{}`",
            spec.underlying
        )
    })?;
    let client = DeribitHttpClient::new(None, environment, 30, 3, 500, 5_000, None)
        .context("failed to create Deribit HTTP client for option universe resolution")?;

    let option_instruments = client
        .request_instruments(currency, Some(DeribitProductType::Option))
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit option instruments for underlying {}",
                spec.underlying
            )
        })?;

    let (atm_reference, atm_reference_source) =
        request_deribit_strike_reference(&client, spec, &option_instruments, resolved_at_ns)
            .await?;
    let open_interest_by_strike = maybe_fetch_deribit_strike_open_interest(
        &client,
        spec,
        &option_instruments,
        resolved_at_ns,
    )
    .await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| {
            derive_perp_instrument_id(spec, OptionUniverseVenueKind::Deribit)
                .map_err(anyhow::Error::from)
        })
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_resolution_spec(spec, false),
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
        open_interest_by_strike,
    })
}

async fn resolve_bybit_option_universe(
    spec: &OptionUniverseSpec,
    environment: BybitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let client = BybitHttpClient::new(
        Some(bybit_http_base_url(environment).to_string()),
        30,
        3,
        500,
        5_000,
        5_000,
        None,
    )
    .context("failed to create Bybit HTTP client for option universe resolution")?;

    let option_instruments = client
        .request_instruments(
            nautilus_bybit::common::enums::BybitProductType::Option,
            None,
            Some(Ustr::from(spec.underlying.as_str())),
        )
        .await
        .with_context(|| {
            format!(
                "failed to request Bybit option instruments for underlying {}",
                spec.underlying
            )
        })?;

    let (atm_reference, atm_reference_source) =
        request_bybit_strike_reference(&client, spec, &option_instruments, resolved_at_ns).await?;
    let open_interest_by_strike =
        maybe_fetch_bybit_strike_open_interest(&client, spec, &option_instruments, resolved_at_ns)
            .await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| {
            derive_perp_instrument_id(spec, OptionUniverseVenueKind::Bybit)
                .map_err(anyhow::Error::from)
        })
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_resolution_spec(spec, false),
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
        open_interest_by_strike,
    })
}

async fn resolve_okx_option_universe(
    spec: &OptionUniverseSpec,
    environment: OKXEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let client = OKXHttpClient::new(None, 30, 3, 500, 5_000, environment, None)
        .context("failed to create OKX HTTP client for option universe resolution")?;
    let instrument_family = okx_instrument_family(spec).map_err(anyhow::Error::from)?;
    // OKX uses the family suffix (for example `BTC-USD`) in config, but the parsed
    // instrument settlement currency is the base asset (for example `BTC`), so the
    // core settlement filter must be cleared before strike selection.
    let normalized_spec = normalized_resolution_spec(spec, true);
    let (option_instruments, _) = client
        .request_instruments(OKXInstrumentType::Option, Some(instrument_family.clone()))
        .await
        .with_context(|| {
            format!(
                "failed to request OKX option instruments for family {}",
                instrument_family
            )
        })?;
    for instrument in &option_instruments {
        client.cache_instrument(instrument.clone());
    }

    let (atm_reference, atm_reference_source) = request_okx_strike_reference(
        &client,
        spec,
        &normalized_spec,
        &option_instruments,
        resolved_at_ns,
    )
    .await?;
    let open_interest_by_strike = maybe_fetch_okx_strike_open_interest(
        &client,
        spec,
        &normalized_spec,
        &option_instruments,
        resolved_at_ns,
    )
    .await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| {
            derive_perp_instrument_id(spec, OptionUniverseVenueKind::Okx)
                .map_err(anyhow::Error::from)
        })
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_spec,
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
        open_interest_by_strike,
    })
}

async fn request_deribit_strike_reference(
    client: &DeribitHttpClient,
    spec: &OptionUniverseSpec,
    option_instruments: &[InstrumentAny],
    resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<(Price, String)> {
    let reference_id =
        select_nearest_expiry_reference_instrument_id(spec, option_instruments, resolved_at_ns)
            .map_err(anyhow::Error::from)?;
    let instrument_name = reference_id.symbol.as_str();
    let selected_expiry_ns = option_instruments
        .iter()
        .find(|instrument| instrument.id() == reference_id)
        .and_then(|instrument| instrument.expiration_ns())
        .with_context(|| {
            format!("reference instrument {reference_id} is missing expiry metadata")
        })?;

    let ticker = client
        .request_ticker(instrument_name)
        .await
        .with_context(|| {
            format!("failed to request Deribit option ticker for {instrument_name}")
        })?;
    if let Some((price, source)) = price_from_decimal(
        ticker.underlying_price,
        AtmReferenceSource::HttpOptionUnderlyingPrice,
    ) {
        return Ok((price, source.as_str().to_string()));
    }

    let summaries = client
        .request_book_summaries(&spec.underlying)
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit book summaries for underlying {}",
                spec.underlying
            )
        })?;
    for summary in summaries {
        let Some(instrument) = option_instruments
            .iter()
            .find(|entry| entry.id().symbol.as_str() == summary.instrument_name)
        else {
            continue;
        };
        if instrument.expiration_ns() != Some(selected_expiry_ns) {
            continue;
        }
        if let Some((price, source)) = price_from_decimal(
            summary.underlying_price,
            AtmReferenceSource::HttpBookSummaryUnderlyingPrice,
        ) {
            return Ok((price, source.as_str().to_string()));
        }
    }

    bail!(
        "no per-expiry forward (underlying_price) available for Deribit option universe {}",
        spec.underlying
    )
}

async fn request_bybit_strike_reference(
    client: &BybitHttpClient,
    spec: &OptionUniverseSpec,
    option_instruments: &[InstrumentAny],
    resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<(Price, String)> {
    let reference_id =
        select_nearest_expiry_reference_instrument_id(spec, option_instruments, resolved_at_ns)
            .map_err(anyhow::Error::from)?;
    let raw_symbol = extract_raw_symbol(reference_id.symbol.as_str()).to_string();
    let params = BybitTickersParams {
        category: BybitProductType::Option,
        symbol: Some(raw_symbol.clone()),
        base_coin: None,
        exp_date: None,
    };
    let tickers = client
        .request_option_tickers_raw_with_params(&params)
        .await
        .with_context(|| {
            format!(
                "failed to request Bybit option ticker for {} (raw={raw_symbol})",
                reference_id.symbol
            )
        })?;
    let Some(ticker) = tickers.first() else {
        bail!(
            "no Bybit option ticker returned for {} (raw={raw_symbol})",
            reference_id.symbol
        );
    };

    if let Some((price, source)) = select_strike_reference_from_decimal_string(
        ticker.underlying_price.as_str(),
        AtmReferenceSource::HttpOptionUnderlyingPrice,
    ) {
        return Ok((price, source.as_str().to_string()));
    }

    bail!(
        "no per-expiry forward (underlying_price) available for Bybit option universe {}",
        spec.underlying
    )
}

async fn request_okx_strike_reference(
    client: &OKXHttpClient,
    spec: &OptionUniverseSpec,
    normalized_spec: &OptionUniverseSpec,
    option_instruments: &[InstrumentAny],
    resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<(Price, String)> {
    let reference_instrument_id = select_nearest_expiry_reference_instrument_id(
        normalized_spec,
        option_instruments,
        resolved_at_ns,
    )
    .map_err(anyhow::Error::from)?;
    let forward_prices = client
        .request_forward_prices(&spec.underlying, Some(reference_instrument_id))
        .await
        .with_context(|| {
            format!(
                "failed to request OKX option forward prices for underlying {}",
                spec.underlying
            )
        })?;

    let price = select_okx_strike_reference(&forward_prices, &spec.underlying)?;
    Ok((
        price,
        AtmReferenceSource::HttpForwardPrice.as_str().to_string(),
    ))
}

fn select_okx_strike_reference(
    forward_prices: &[nautilus_model::data::ForwardPrice],
    underlying: &str,
) -> Result<Price> {
    let Some(forward_price) = forward_prices.first() else {
        bail!(
            "no OKX forward prices were returned for underlying {}",
            underlying
        );
    };

    Ok(Price::from(
        forward_price.forward_price.to_string().as_str(),
    ))
}

fn price_from_decimal(
    value: Option<Decimal>,
    source: AtmReferenceSource,
) -> Option<(Price, AtmReferenceSource)> {
    value.and_then(|decimal| {
        select_strike_reference_from_decimal_string(decimal.to_string().as_str(), source)
    })
}

fn open_interest_from_string(value: &str) -> Option<f64> {
    metric_from_string(value)
}

fn metric_from_string(value: &str) -> Option<f64> {
    value
        .parse::<f64>()
        .ok()
        .filter(|metric| metric.is_finite() && *metric > 0.0)
}

fn bybit_option_instrument_symbol_from_ticker(ticker_symbol: &str) -> String {
    format!("{ticker_symbol}-OPTION")
}

async fn maybe_fetch_deribit_strike_open_interest(
    _client: &DeribitHttpClient,
    spec: &OptionUniverseSpec,
    _option_instruments: &[InstrumentAny],
    _resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<Option<StrikeOpenInterestByStrike>> {
    if !spec.strike_policy.requires_open_interest() {
        return Ok(None);
    }

    bail!(
        "startup oi_ranked option universe resolution is not currently supported for venue_id={} \
         underlying={} because the Nautilus Deribit HTTP discovery path does not expose \
         per-contract option open interest; use atm_relative/all at startup or rely on \
         runtime refresh after option_greeks warmup",
        spec.venue_id,
        spec.underlying
    )
}

async fn maybe_fetch_bybit_strike_open_interest(
    client: &BybitHttpClient,
    spec: &OptionUniverseSpec,
    option_instruments: &[InstrumentAny],
    resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<Option<StrikeOpenInterestByStrike>> {
    if !spec.strike_policy.requires_open_interest() {
        return Ok(None);
    }

    let (selected_expiry_ns, _) =
        option_instrument_ids_at_selected_expiry(spec, option_instruments, resolved_at_ns)
            .map_err(anyhow::Error::from)?;
    let tickers = client
        .request_option_tickers_raw(&spec.underlying)
        .await
        .with_context(|| {
            format!(
                "failed to request Bybit option tickers for open interest on {}",
                spec.underlying
            )
        })?;

    let mut entries = Vec::new();
    for ticker in tickers {
        let instrument_symbol = bybit_option_instrument_symbol_from_ticker(ticker.symbol.as_str());
        let Some(instrument) = option_instruments
            .iter()
            .find(|entry| entry.id().symbol.as_str() == instrument_symbol)
        else {
            continue;
        };
        if instrument.expiration_ns() != Some(selected_expiry_ns) {
            continue;
        }
        let Some(strike) = instrument.strike_price() else {
            continue;
        };
        let Some(open_interest) = open_interest_from_string(ticker.open_interest.as_str()) else {
            continue;
        };
        entries.push((strike, open_interest));
    }

    Ok(Some(aggregate_open_interest_by_strike(entries)))
}

async fn maybe_fetch_okx_strike_open_interest(
    _client: &OKXHttpClient,
    spec: &OptionUniverseSpec,
    _normalized_spec: &OptionUniverseSpec,
    _option_instruments: &[InstrumentAny],
    _resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<Option<StrikeOpenInterestByStrike>> {
    if !spec.strike_policy.requires_open_interest() {
        return Ok(None);
    }

    bail!(
        "startup oi_ranked option universe resolution is not currently supported for venue_id={} \
         underlying={} because the Nautilus OKX HTTP discovery path does not expose \
         per-contract option open interest; use atm_relative/all at startup or rely on \
         runtime refresh after option_greeks warmup",
        spec.venue_id,
        spec.underlying
    )
}
