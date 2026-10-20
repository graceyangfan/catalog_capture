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

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{custom_data, data::CustomData, identifiers::InstrumentId};
use nautilus_serialization::arrow_custom_data;
use rust_decimal::Decimal;

use crate::common::open_interest_data_type;

/// Exact venue `bo_market_stats.open_interest`; no inferred multiplier or unit conversion.
#[arrow_custom_data]
#[custom_data]
pub struct LighterOpenInterest {
    pub instrument_id: InstrumentId,
    pub market_id: u64,
    pub open_interest: Decimal,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}
impl LighterOpenInterest {
    pub fn into_custom_data(self) -> CustomData {
        let data_type = open_interest_data_type(self.market_id);
        CustomData::new(Arc::new(self), data_type)
    }
}
pub fn register_lighter_ext_custom_data() {
    nautilus_serialization::ensure_custom_data_registered::<LighterOpenInterest>();
}
