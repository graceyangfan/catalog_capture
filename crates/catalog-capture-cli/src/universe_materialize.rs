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

use catalog_capture_core::{merge_capture_plans, CapturePlan};
use nautilus_model::identifiers::InstrumentId;

#[derive(Debug, Clone)]
pub struct UniverseMaterialization {
    pub plan: CapturePlan,
    pub planned_instrument_ids: BTreeSet<InstrumentId>,
}

impl UniverseMaterialization {
    #[must_use]
    pub fn new(base_plan: CapturePlan) -> Self {
        let planned_instrument_ids = base_plan
            .planned_instrument_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        Self {
            plan: base_plan,
            planned_instrument_ids,
        }
    }

    #[must_use]
    pub fn append_expanded_plan(&mut self, expanded: &CapturePlan) -> BTreeSet<InstrumentId> {
        let expanded_ids = expanded
            .planned_instrument_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        self.planned_instrument_ids
            .extend(expanded_ids.iter().copied());
        self.plan = merge_capture_plans(&self.plan, expanded);
        expanded_ids
    }
}
