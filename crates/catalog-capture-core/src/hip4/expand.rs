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

use crate::hip4::spec::{Hip4UniverseFamily, Hip4UniverseSpec, ResolvedHip4Universe};
use crate::plan::{CapturePlan, InstrumentCaptureSpec, MarkPriceCaptureSpec, QuoteCaptureSpec};

pub fn expand_hip4_universe(
    spec: &Hip4UniverseSpec,
    resolved: &ResolvedHip4Universe,
) -> CapturePlan {
    let mut plan = CapturePlan::default();

    for family in &spec.families {
        match family {
            Hip4UniverseFamily::Instruments => {
                for instrument_id in &resolved.all_instrument_ids {
                    plan.instruments.push(InstrumentCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            Hip4UniverseFamily::Quotes => {
                for instrument_id in &resolved.outcome_instrument_ids {
                    plan.quotes.push(QuoteCaptureSpec {
                        instrument_id: *instrument_id,
                    });
                }
            }
            Hip4UniverseFamily::MarkPrices => {
                if let Some(instrument_id) = resolved.perp_instrument_id {
                    plan.mark_prices
                        .push(MarkPriceCaptureSpec { instrument_id });
                }
            }
        }
    }

    plan
}
