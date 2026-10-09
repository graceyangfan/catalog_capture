// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! NautilusTrader-style Predict adapter primitives.

#![warn(rustc::all)]
#![deny(unsafe_code)]

pub mod common;
pub mod config;
pub mod data;
pub mod data_client;
pub mod factories;
pub mod http;
pub mod models;
pub mod providers;
pub mod universe;

pub use common::{
    PREDICT, PREDICT_VENUE, PredictMarketContext, instrument_id_from_outcome,
    predict_crypto_up_down_orderbook_data_type, predict_orderbook_data_type,
};
pub use config::PredictDataClientConfig;
pub use data::{
    PredictOrderbookLevel, PredictOrderbookSnapshot, PredictOrderbookWire,
    register_predict_custom_data,
};
pub use data_client::PredictDataClient;
pub use factories::PredictDataClientFactory;
pub use http::PredictHttpClient;
pub use providers::PredictInstrumentProvider;
pub use universe::{
    PredictCryptoUpDownSelector, SelectedCryptoUpDownMarket, discover_crypto_up_down_market,
};
