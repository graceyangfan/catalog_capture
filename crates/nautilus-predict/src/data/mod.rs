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

//! Nautilus data client and its market-data runtime.

#[cfg(feature = "live")]
mod client;
#[cfg(feature = "live")]
mod depth;
#[cfg(feature = "live")]
mod discovery;
#[cfg(feature = "live")]
mod dispatch;
#[cfg(feature = "live")]
mod instruments;
#[cfg(feature = "live")]
mod lifecycle;
#[cfg(feature = "live")]
mod quotes;
#[cfg(feature = "live")]
mod requests;
#[cfg(feature = "live")]
mod runtime;
#[cfg(feature = "live")]
mod trades;

#[cfg(feature = "live")]
pub use self::client::PredictDataClient;
