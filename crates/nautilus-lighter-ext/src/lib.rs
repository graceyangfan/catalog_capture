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

//! Public binary option data alongside the unmodified upstream Lighter adapter.
#![deny(unsafe_code)]

pub mod common;
pub mod config;
pub mod data;
pub mod data_types;
pub mod factories;
pub mod http;
pub mod models;
pub mod websocket;

pub use nautilus_lighter as upstream;

pub use config::LighterBinaryDataClientConfig;
pub use data::LighterBinaryDataClient;
pub use data_types::{LighterOpenInterest, register_lighter_ext_custom_data};
pub use factories::LighterBinaryDataClientFactory;
