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

pub const BULLET: &str = "BULLET";
pub const BULLET_BOOK_DEPTH: usize = 20;

pub static BULLET_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(BULLET)));
pub static BULLET_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(BULLET)));

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulletEnvironment {
    #[default]
    Mainnet,
    Testnet,
}

impl BulletEnvironment {
    #[must_use]
    pub const fn http_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://tradingapi.bullet.xyz",
            Self::Testnet => "https://tradingapi.testnet.bullet.xyz",
        }
    }

    #[must_use]
    pub const fn ws_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://tradingapi.bullet.xyz/ws",
            Self::Testnet => "wss://tradingapi.testnet.bullet.xyz/ws",
        }
    }
}

pub fn instrument_id_from_market(market: &str) -> anyhow::Result<InstrumentId> {
    Symbol::new_checked(market)?;
    let symbol = Symbol::new_checked(format!("{market}-PERP"))
        .with_context(|| format!("invalid Bullet market `{market}`"))?;
    Ok(InstrumentId::new(symbol, *BULLET_VENUE))
}

pub fn market_from_instrument_id(instrument_id: InstrumentId) -> anyhow::Result<String> {
    anyhow::ensure!(
        instrument_id.venue == *BULLET_VENUE,
        "instrument {instrument_id} does not belong to BULLET",
    );
    instrument_id
        .symbol
        .as_str()
        .strip_suffix("-PERP")
        .map(str::to_string)
        .with_context(|| format!("Bullet instrument must end with -PERP: {instrument_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_identity_round_trip() {
        let instrument_id = instrument_id_from_market("BTC-USD").unwrap();
        assert_eq!(instrument_id.to_string(), "BTC-USD-PERP.BULLET");
        assert_eq!(market_from_instrument_id(instrument_id).unwrap(), "BTC-USD");
    }
}
