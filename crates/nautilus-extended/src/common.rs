// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::sync::LazyLock;

use anyhow::Context;
use nautilus_model::identifiers::{ClientId, InstrumentId, Symbol, Venue};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

pub const EXTENDED: &str = "EXTENDED";

pub static EXTENDED_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(EXTENDED)));
pub static EXTENDED_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(EXTENDED)));

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtendedEnvironment {
    #[default]
    Mainnet,
    Testnet,
}

impl ExtendedEnvironment {
    #[must_use]
    pub const fn http_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.starknet.extended.exchange/api/v1",
            Self::Testnet => "https://api.starknet.sepolia.extended.exchange/api/v1",
        }
    }

    #[must_use]
    pub const fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc",
            Self::Testnet => {
                "wss://api.starknet.sepolia.extended.exchange/stream.extended.exchange/v2/rpc"
            }
        }
    }
}

pub fn instrument_id_from_market(market: &str) -> anyhow::Result<InstrumentId> {
    let symbol = Symbol::new_checked(format!("{market}-PERP"))
        .with_context(|| format!("invalid Extended market `{market}`"))?;
    Ok(InstrumentId::new(symbol, *EXTENDED_VENUE))
}

pub fn market_from_instrument_id(instrument_id: InstrumentId) -> anyhow::Result<String> {
    anyhow::ensure!(
        instrument_id.venue == *EXTENDED_VENUE,
        "instrument {instrument_id} does not belong to EXTENDED",
    );
    instrument_id
        .symbol
        .as_str()
        .strip_suffix("-PERP")
        .map(str::to_string)
        .with_context(|| format!("Extended instrument must end with -PERP: {instrument_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_identity_round_trip() {
        let instrument_id = instrument_id_from_market("BTC-USD").unwrap();
        assert_eq!(instrument_id.to_string(), "BTC-USD-PERP.EXTENDED");
        assert_eq!(market_from_instrument_id(instrument_id).unwrap(), "BTC-USD");
    }
}
