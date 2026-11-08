// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Bitfinex public spot market-by-order data.

mod book;
mod data;
mod instruments;
mod websocket;

pub use data::{BitfinexDataClientConfig, BitfinexDataClientFactory};
pub use instruments::{BITFINEX, supported_instrument};

pub const DEFAULT_DEPTH: usize = 250;

pub fn validate_depth(depth: usize) -> anyhow::Result<()> {
    // CRC32 covers the top 25 orders per side. Use checksum-capable profiles.
    anyhow::ensure!(
        matches!(depth, 25 | 100 | 250),
        "Bitfinex R0 depth must be 25, 100 or 250 orders per side",
    );
    Ok(())
}
