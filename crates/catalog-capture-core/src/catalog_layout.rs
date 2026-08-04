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

//! Helpers for the Nautilus Trader **Rust** catalog path contract.
//!
//! Capture writes only Rust-canonical layout via `ParquetDataCatalog`.
//! Python legacy path mirrors are not supported.

use nautilus_model::instruments::{Instrument, InstrumentAny};

/// Instrument id string used in catalog partition keys.
#[must_use]
pub fn instrument_identifier(instrument: &InstrumentAny) -> String {
    Instrument::id(instrument).to_string()
}
