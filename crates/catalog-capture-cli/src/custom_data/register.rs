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

//! Register Nautilus custom data types with the runtime for known capture specs.

use catalog_capture_core::{CustomDataCaptureSpec, CustomDataRequestCaptureSpec};

use super::KnownCustomDataType;

#[cfg(feature = "venue-binance")]
use nautilus_binance::data_types::register_binance_custom_data;
#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::data_types::register_hyperliquid_custom_data;

pub fn register_subscribe_types(custom_data: &[CustomDataCaptureSpec]) {
    for spec in custom_data {
        if let Some(entry) = KnownCustomDataType::from_type_name(spec.data_type.type_name()) {
            if entry.is_subscribe() {
                entry.register();
            }
        }
    }
}

pub fn register_request_types(requests: &[CustomDataRequestCaptureSpec]) {
    for spec in requests {
        if let Some(entry) = KnownCustomDataType::from_type_name(spec.data_type.type_name()) {
            if entry.is_request() {
                entry.register();
            }
        }
    }
}

impl KnownCustomDataType {
    fn register(self) {
        match self {
            #[cfg(feature = "venue-binance")]
            Self::BinanceFuturesLiquidation | Self::BinanceFuturesTicker => {
                register_binance_custom_data();
            }
            #[cfg(feature = "venue-deribit")]
            Self::DeribitVolatilityIndex | Self::DeribitBookSummary => {
                nautilus_deribit::data_types::register_deribit_custom_data();
            }
            #[cfg(feature = "venue-hyperliquid")]
            Self::HyperliquidOpenInterest => {
                register_hyperliquid_custom_data();
            }
        }
    }
}
