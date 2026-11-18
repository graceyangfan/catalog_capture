// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Bullet public market-data wire models and Nautilus conversions.
//!
//! Native read-only public data client, metadata bootstrap and epoch-owned WebSocket recovery.

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod factories;
pub mod funding;
pub mod http;
pub mod websocket;

pub use config::BulletDataClientConfig;
pub use data::BulletDataClient;
pub use factories::BulletDataClientFactory;
