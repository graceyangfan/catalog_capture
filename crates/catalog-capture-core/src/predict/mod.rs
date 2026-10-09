// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

//! Predict venue-specific helpers which do not require a live client.

mod crypto_up_down;

pub use crypto_up_down::{
    CryptoUpDownMarketCandidate, CryptoUpDownMarketSelector, CryptoUpDownMarketWindow,
    CryptoUpDownSelectorKey, PREDICT_SELECTOR_INTERVAL_SECS, PREDICT_SELECTOR_PRICE_FEED_SYMBOL,
    PREDICT_SELECTOR_TITLE_ASSET, parse_crypto_up_down_title, predict_outcome_instrument_id,
    resolve_crypto_up_down_market,
};
