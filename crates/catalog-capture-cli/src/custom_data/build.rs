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

//! Build `DataType` values for request-style custom data from TOML metadata.

#[cfg(any(feature = "venue-deribit", feature = "venue-binance"))]
use anyhow::anyhow;
use anyhow::{Result, bail};
#[cfg(any(feature = "venue-deribit", feature = "venue-binance"))]
use nautilus_core::Params;
use nautilus_model::data::DataType;
#[cfg(any(feature = "venue-deribit", feature = "venue-binance"))]
use serde_json::Value as JsonValue;

use super::{KnownCustomDataType, supported_request_csv};

/// Builds a request `DataType` and optional default client id for config parse.
///
/// Returns an error if `type_name` is subscribe-only or unknown.
pub fn build_request_data_type(
    type_name: &str,
    _metadata: &std::collections::BTreeMap<String, String>,
    _identifier: Option<&str>,
) -> Result<(DataType, Option<String>)> {
    let Some(entry) = KnownCustomDataType::from_type_name(type_name) else {
        bail!(
            "unsupported capture.custom_data_requests.type_name `{type_name}`; \
             supported: {}",
            supported_request_csv()
        );
    };
    if !entry.is_request() {
        bail!(
            "custom_data_requests type_name `{type_name}` is subscribe-only; use [[capture.custom_data]] \
             (Nautilus subscribe_data), not [[capture.custom_data_requests]] (request_data)"
        );
    }

    match entry {
        #[cfg(feature = "venue-bullet")]
        KnownCustomDataType::BulletFundingEstimate => {
            let id: nautilus_model::identifiers::InstrumentId = _metadata
                .get("instrument_id")
                .ok_or_else(|| {
                    anyhow::anyhow!("BulletFundingEstimate requires metadata.instrument_id")
                })?
                .parse()?;
            anyhow::ensure!(
                _metadata.len() == 1,
                "BulletFundingEstimate only accepts instrument_id metadata"
            );
            super::ensure_identifier_matches(
                _identifier,
                &id.to_string(),
                "BulletFundingEstimate",
            )?;
            let data_type = nautilus_bullet::funding::estimate_data_type(id);
            nautilus_bullet::funding::estimate_instrument(&data_type)?;
            Ok((data_type, None))
        }

        #[cfg(feature = "venue-binance")]
        KnownCustomDataType::BinanceFuturesOpenInterest => {
            build_binance_open_interest(_metadata, _identifier)
        }
        #[cfg(feature = "venue-deribit")]
        KnownCustomDataType::DeribitBookSummary => {
            build_deribit_book_summary(_metadata, _identifier)
        }
        #[allow(unreachable_patterns)]
        other => bail!(
            "internal error: {} marked request but has no request builder",
            other.type_name()
        ),
    }
}

#[cfg(feature = "venue-deribit")]
fn build_deribit_book_summary(
    metadata: &std::collections::BTreeMap<String, String>,
    identifier: Option<&str>,
) -> Result<(DataType, Option<String>)> {
    let currency = metadata
        .get("currency")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "capture.custom_data_requests DeribitBookSummary requires metadata.currency \
                 (for example `BTC`)"
            )
        })?
        .to_ascii_uppercase();
    let kind = metadata
        .get("kind")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("option")
        .to_ascii_lowercase();
    let expected_id = format!("{currency}:{kind}");
    if let Some(identifier) = identifier {
        let identifier = identifier.trim();
        if !identifier.is_empty() && identifier != expected_id {
            bail!(
                "capture.custom_data_requests DeribitBookSummary identifier `{identifier}` \
                 must match `{expected_id}` (or be omitted)"
            );
        }
    }
    let params = Params::new(
        [
            ("currency".to_string(), JsonValue::String(currency)),
            ("kind".to_string(), JsonValue::String(kind)),
        ]
        .into(),
    );
    Ok((
        DataType::new("DeribitBookSummary", Some(params), Some(expected_id)),
        Some("DERIBIT".to_string()),
    ))
}

#[cfg(feature = "venue-binance")]
fn build_binance_open_interest(
    metadata: &std::collections::BTreeMap<String, String>,
    identifier: Option<&str>,
) -> Result<(DataType, Option<String>)> {
    use nautilus_model::identifiers::InstrumentId;
    use std::str::FromStr;

    let raw = metadata
        .get("instrument_id")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("BinanceFuturesOpenInterest requires metadata.instrument_id"))?;
    let id = InstrumentId::from_str(raw)?;
    ensure_binance_futures_instrument(id)?;
    super::ensure_identifier_matches(identifier, &id.to_string(), "BinanceFuturesOpenInterest")?;
    let params = Params::new(
        [(
            "instrument_id".to_string(),
            JsonValue::String(id.to_string()),
        )]
        .into(),
    );
    Ok((
        DataType::new(
            "BinanceFuturesOpenInterest",
            Some(params),
            Some(id.to_string()),
        ),
        None,
    ))
}

#[cfg(feature = "venue-binance")]
pub(super) fn ensure_binance_futures_instrument(
    id: nautilus_model::identifiers::InstrumentId,
) -> Result<()> {
    use nautilus_binance::common::{
        enums::BinanceProductType,
        symbol::{format_binance_symbol, format_instrument_id},
    };
    let symbol = ustr::Ustr::from(format_binance_symbol(&id).as_str());
    anyhow::ensure!(
        [BinanceProductType::UsdM, BinanceProductType::CoinM]
            .into_iter()
            .any(|product| format_instrument_id(&symbol, product) == id),
        "BinanceFuturesOpenInterest requires a canonical Binance Futures instrument, received {id}"
    );
    Ok(())
}

/// Resolve OI's instrument-scoped request through the configured Native product client.
#[cfg(feature = "venue-binance")]
pub fn resolve_open_interest_requests(
    plan: &mut catalog_capture_core::CapturePlan,
    venues: &[crate::config::VenueRuntimeConfig],
) -> Result<()> {
    use crate::config::VenueRuntimeConfig;
    use catalog_capture_core::plan::InstrumentCaptureSpec;
    use nautilus_binance::common::symbol::{format_binance_symbol, format_instrument_id};
    use nautilus_model::identifiers::InstrumentId;
    use std::str::FromStr;

    for spec in &mut plan.custom_data_requests {
        if spec.data_type.type_name() != "BinanceFuturesOpenInterest" {
            continue;
        }
        let id = InstrumentId::from_str(
            super::string_metadata(&spec.data_type, "instrument_id").ok_or_else(|| {
                anyhow!("BinanceFuturesOpenInterest requires metadata.instrument_id")
            })?,
        )?;
        let symbol = ustr::Ustr::from(format_binance_symbol(&id).as_str());
        let candidates = venues
            .iter()
            .filter_map(|venue| match venue {
                VenueRuntimeConfig::BinanceFutures {
                    id: client,
                    product_type,
                    ..
                } if format_instrument_id(&symbol, *product_type) == id
                    && spec
                        .client_id
                        .as_ref()
                        .is_none_or(|selected| selected == client) =>
                {
                    Some(client)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(
            candidates.len() == 1,
            "BinanceFuturesOpenInterest {id} must match exactly one configured Binance Futures client (client_id={:?}); found {}",
            spec.client_id,
            candidates.len()
        );
        spec.client_id = Some(candidates[0].clone());
        // Like other instrument-scoped families, record the definition needed for replay.
        if !plan
            .instruments
            .iter()
            .any(|entry| entry.instrument_id == id)
        {
            plan.instruments
                .push(InstrumentCaptureSpec { instrument_id: id });
        }
    }
    Ok(())
}

#[cfg(feature = "venue-bullet")]
pub fn resolve_bullet_funding_requests(
    plan: &mut catalog_capture_core::CapturePlan,
    venues: &[crate::config::VenueRuntimeConfig],
) -> Result<()> {
    for spec in &mut plan.custom_data_requests {
        if spec.data_type.type_name() != nautilus_bullet::funding::ESTIMATE_TYPE {
            continue;
        }
        let id = nautilus_bullet::funding::estimate_instrument(&spec.data_type)?;
        let candidates = venues
            .iter()
            .filter_map(|v| {
                if let crate::config::VenueRuntimeConfig::Bullet { id, .. } = v {
                    Some(id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let client = match &spec.client_id {
            Some(id) => {
                anyhow::ensure!(
                    candidates.contains(&id),
                    "BulletFundingEstimate client_id is not a configured Bullet client"
                );
                id.clone()
            }
            None => {
                anyhow::ensure!(
                    candidates.len() == 1,
                    "BulletFundingEstimate requires an unambiguous configured Bullet client"
                );
                candidates[0].clone()
            }
        };
        spec.client_id = Some(client);
        if !plan.instruments.iter().any(|s| s.instrument_id == id) {
            plan.instruments
                .push(catalog_capture_core::InstrumentCaptureSpec { instrument_id: id });
        }
    }
    Ok(())
}
