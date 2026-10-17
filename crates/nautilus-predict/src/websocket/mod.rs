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

//! Predict WebSocket transport and protocol. Public payloads are available without live IO.

#[cfg(feature = "live")]
pub(crate) mod client;
pub mod messages;
#[cfg(feature = "live")]
pub(crate) mod parse;

#[cfg(feature = "execution")]
pub mod error;
#[cfg(feature = "execution")]
pub mod private_messages;
#[cfg(feature = "execution")]
mod private_parse;

#[cfg(feature = "execution")]
pub mod private_client;
#[cfg(feature = "execution")]
mod private_handler;
#[cfg(feature = "execution")]
pub use private_client::{PredictWalletSubscriptionState, PredictWalletWebSocketClient};
