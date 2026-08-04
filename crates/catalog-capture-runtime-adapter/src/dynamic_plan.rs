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

use catalog_capture_core::{capture_plan_difference, CapturePlan};

#[derive(Debug, Clone, Default)]
pub struct DynamicPlanDelta<TChange, TRecord> {
    pub add: CapturePlan,
    pub remove: CapturePlan,
    pub changes: Vec<TChange>,
    pub resolution_records: Vec<TRecord>,
}

impl<TChange, TRecord> DynamicPlanDelta<TChange, TRecord> {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

#[must_use]
pub fn build_dynamic_plan_delta<TChange, TRecord>(
    previous_dynamic_plan: &CapturePlan,
    next_dynamic_plan: &CapturePlan,
    changes: Vec<TChange>,
    resolution_records: Vec<TRecord>,
) -> DynamicPlanDelta<TChange, TRecord> {
    DynamicPlanDelta {
        add: capture_plan_difference(next_dynamic_plan, previous_dynamic_plan),
        remove: capture_plan_difference(previous_dynamic_plan, next_dynamic_plan),
        changes,
        resolution_records,
    }
}
