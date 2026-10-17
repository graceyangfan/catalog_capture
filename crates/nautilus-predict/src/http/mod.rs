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

//! REST transport, authentication, and venue response models.

#[cfg(feature = "execution")]
pub mod account_models;
#[cfg(feature = "live")]
pub mod client;
#[cfg(feature = "execution")]
pub mod command_models;
#[cfg(feature = "execution")]
pub mod error;
#[cfg(feature = "execution")]
pub mod execution_models;
#[cfg(feature = "execution")]
pub mod market_models;
pub mod models;
#[cfg(feature = "execution")]
pub mod order_models;
#[cfg(feature = "execution")]
pub mod parse;
#[cfg(feature = "execution")]
pub mod private_client;
#[cfg(feature = "execution")]
pub mod query;
#[cfg(feature = "live")]
pub use client::PredictHttpClient;
#[cfg(feature = "execution")]
pub use private_client::{PredictAuthenticatedHttpClient, PredictPrivateHttpClient};
