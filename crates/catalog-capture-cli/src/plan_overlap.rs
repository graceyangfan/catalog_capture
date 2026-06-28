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

use nautilus_model::identifiers::InstrumentId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanOverlap {
    pub overlapping_instrument_ids: Vec<String>,
    pub new_instrument_ids: Vec<String>,
}

pub fn plan_overlap(
    existing_instrument_ids: &BTreeSet<InstrumentId>,
    candidate_instrument_ids: &BTreeSet<InstrumentId>,
) -> PlanOverlap {
    let overlapping_instrument_ids = candidate_instrument_ids
        .iter()
        .filter(|instrument_id| existing_instrument_ids.contains(instrument_id))
        .map(ToString::to_string)
        .collect();
    let new_instrument_ids = candidate_instrument_ids
        .iter()
        .filter(|instrument_id| !existing_instrument_ids.contains(instrument_id))
        .map(ToString::to_string)
        .collect();

    PlanOverlap {
        overlapping_instrument_ids,
        new_instrument_ids,
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;

    use super::*;

    #[test]
    fn plan_overlap_splits_shared_and_new_ids() {
        let existing = BTreeSet::from([
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            InstrumentId::from("BTC-20JUN26-62000-C.DERIBIT"),
        ]);
        let candidate = BTreeSet::from([
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT"),
        ]);

        let overlap = plan_overlap(&existing, &candidate);

        assert_eq!(
            overlap.overlapping_instrument_ids,
            vec!["BTC-PERPETUAL.DERIBIT".to_string()]
        );
        assert_eq!(
            overlap.new_instrument_ids,
            vec!["BTC-27JUN26-62000-C.DERIBIT".to_string()]
        );
    }
}
