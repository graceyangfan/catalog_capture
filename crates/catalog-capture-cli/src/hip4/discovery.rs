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

#[cfg(feature = "venue-hyperliquid")]
use anyhow::Context;
use anyhow::{bail, Result};
#[cfg(feature = "venue-hyperliquid")]
use catalog_capture_core::{
    build_resolved_hip4_universe, resolve_hip4_market, ResolveHip4MarketOptions,
};
use catalog_capture_core::{Hip4UniverseSpec, ResolvedHip4Universe};
#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::http::client::HyperliquidRawHttpClient;

use crate::config::VenueRuntimeConfig;

pub async fn resolve_hip4_universe_spec(
    spec: &Hip4UniverseSpec,
    venues: &[VenueRuntimeConfig],
    http_timeout_secs: u64,
) -> Result<ResolvedHip4Universe> {
    #[cfg(not(feature = "venue-hyperliquid"))]
    {
        let _ = (venues, http_timeout_secs);
        bail!(
            "capture.hip4_universe requires cargo feature `venue-hyperliquid` \
             (rebuild with `--features venue-hyperliquid` or `--features all-venues`); \
             venue_id `{}`",
            spec.venue_id
        );
    }
    #[cfg(feature = "venue-hyperliquid")]
    {
        let venue = resolve_hip4_venue(spec, venues)?;
        let VenueRuntimeConfig::Hyperliquid { environment, .. } = venue else {
            bail!(
                "capture.hip4_universe currently only supports Hyperliquid venues; got venue_id `{}`",
                spec.venue_id
            );
        };

        let client = HyperliquidRawHttpClient::new(*environment, http_timeout_secs, None)
            .context("failed to create Hyperliquid HTTP client")?;
        let outcome_meta = client
            .get_outcome_meta()
            .await
            .context("failed to fetch Hyperliquid outcomeMeta")?;
        let payload = serde_json::to_value(outcome_meta)
            .context("failed to serialize Hyperliquid outcomeMeta payload")?;
        let now_ns = nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64();
        let market = resolve_hip4_market(
            &payload,
            &ResolveHip4MarketOptions {
                underlying: &spec.underlying,
                period: &spec.period,
                market_class: &spec.market_class,
                include_fallback: spec.include_fallback,
                now_ns,
            },
        )?;
        Ok(build_resolved_hip4_universe(spec, market, now_ns))
    }
}

#[cfg(feature = "venue-hyperliquid")]
fn resolve_hip4_venue<'a>(
    spec: &Hip4UniverseSpec,
    venues: &'a [VenueRuntimeConfig],
) -> Result<&'a VenueRuntimeConfig> {
    venues
        .iter()
        .find(|venue| venue.id() == spec.venue_id)
        .with_context(|| {
            format!(
                "capture.hip4_universe references unknown venue_id `{}`",
                spec.venue_id
            )
        })
}
