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

//! Predict market-data and execution adapter for NautilusTrader.
//!
//! # Feature Flags
//!
//! - `execution`: Private REST, wallet WebSocket and signing primitives; implies `live`.
//!   Covers JWT, read-only queries, lifecycle/settlement facts and offline order signing.
//!   Maps supported order commands and native execution reports; submission is explicitly opt-in.
//! - `high-precision` (default): Propagates Nautilus fixed-point precision features.
//! - `live` (default): Public market-data client, instrument discovery, and live transports.

#![warn(rustc::all)]
#![deny(unsafe_code)]

#[cfg(feature = "execution")]
pub mod blockchain;
pub mod common;
#[cfg(feature = "live")]
pub mod config;
pub mod data;
pub mod data_types;
#[cfg(feature = "execution")]
pub mod execution;
#[cfg(feature = "live")]
pub mod factories;
#[cfg(feature = "execution")]
pub mod fees;
pub mod http;
#[cfg(feature = "execution")]
pub mod latency;
#[cfg(feature = "live")]
pub mod providers;
#[cfg(feature = "execution")]
pub mod signing;
#[cfg(feature = "live")]
pub mod universe;
pub mod websocket;

#[cfg(feature = "execution")]
pub use common::environment::PredictEnvironment;
pub use common::{
    PREDICT, PREDICT_VENUE, PredictMarketContext, instrument_id_from_outcome,
    predict_crypto_up_down_orderbook_data_type, predict_orderbook_data_type,
};
#[cfg(feature = "live")]
pub use config::PredictDataClientConfig;
#[cfg(feature = "execution")]
pub use config::{
    PredictAccountStateMode, PredictChainClientConfig, PredictExecutionClientConfig,
    PredictPrivateClientConfig, PredictWalletWebSocketConfig,
};
#[cfg(feature = "live")]
pub use data::PredictDataClient;
pub use data_types::{
    PredictLastOrderSettled, PredictLastOrderSettledWire, PredictOrderbookLevel,
    PredictOrderbookSnapshot, PredictOrderbookWire, PredictSettlementPendingLevel,
    PredictSettlementsPending, PredictSettlementsPendingWire, register_predict_custom_data,
};
#[cfg(feature = "execution")]
pub use execution::PredictExecutionClient;
#[cfg(feature = "execution")]
pub use execution::observations::{
    PredictLimitOrderReceiptUpdate, PredictOrderNotAcceptedUpdate, PredictSettlementStatus,
    PredictSettlementUpdate, predict_limit_order_receipt_data_type,
    predict_order_not_accepted_data_type, predict_settlement_data_type,
};
#[cfg(feature = "live")]
pub use factories::PredictDataClientFactory;
#[cfg(feature = "execution")]
pub use factories::PredictExecutionClientFactory;
#[cfg(feature = "live")]
pub use http::PredictHttpClient;
#[cfg(feature = "execution")]
pub use http::command_models::{PredictLimitOrderOptions, PredictSelfTradePrevention};
#[cfg(feature = "live")]
pub use providers::PredictInstrumentProvider;
#[cfg(feature = "live")]
pub use universe::{
    PredictCryptoUpDownSelector, SelectedCryptoUpDownMarket, discover_crypto_up_down_market,
};
