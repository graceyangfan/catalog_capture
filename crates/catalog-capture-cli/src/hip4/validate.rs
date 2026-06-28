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

use anyhow::{bail, Result};
use catalog_capture_core::Hip4UniverseSpec;

use crate::config::VenueRuntimeConfig;

pub fn validate_hip4_universes(
    specs: &[Hip4UniverseSpec],
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    for spec in specs {
        let venue = venues
            .iter()
            .find(|entry| entry.id() == spec.venue_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "capture.hip4_universe references unknown venue_id `{}`",
                    spec.venue_id
                )
            })?;
        if !matches!(venue, VenueRuntimeConfig::Hyperliquid { .. }) {
            bail!(
                "capture.hip4_universe venue_id `{}` must reference a hyperliquid venue",
                spec.venue_id
            );
        }
    }
    Ok(())
}
