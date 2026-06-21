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

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use nautilus_core::string::conversions::to_snake_case;
use nautilus_model::instruments::{Instrument, InstrumentAny};

use crate::config::LayoutCompatibility;

#[must_use]
pub fn legacy_market_data_prefix(canonical_prefix: &str) -> &str {
    match canonical_prefix {
        "quotes" => "quote_tick",
        "trades" => "trade_tick",
        "mark_prices" => "mark_price_update",
        "bars" => "bar",
        "order_book_deltas" => "order_book_deltas",
        "index_prices" => "index_price_updates",
        "funding_rate_update" => "funding_rate_update",
        "instrument_status" => "instrument_status",
        "option_greeks" => "option_greeks",
        other => other,
    }
}

#[must_use]
pub fn python_legacy_instrument_prefix(instrument: &InstrumentAny) -> &'static str {
    match instrument {
        InstrumentAny::Betting(_) => "betting_instrument",
        InstrumentAny::BinaryOption(_) => "binary_option",
        InstrumentAny::Cfd(_) => "cfd",
        InstrumentAny::Commodity(_) => "commodity",
        InstrumentAny::CryptoFuture(_) => "crypto_future",
        InstrumentAny::CryptoFuturesSpread(_) => "crypto_futures_spread",
        InstrumentAny::CryptoOption(_) => "crypto_option",
        InstrumentAny::CryptoOptionSpread(_) => "crypto_option_spread",
        InstrumentAny::CryptoPerpetual(_) => "crypto_perpetual",
        InstrumentAny::CurrencyPair(_) => "currency_pair",
        InstrumentAny::Equity(_) => "equity",
        InstrumentAny::FuturesContract(_) => "futures_contract",
        InstrumentAny::FuturesSpread(_) => "futures_spread",
        InstrumentAny::IndexInstrument(_) => "index_instrument",
        InstrumentAny::OptionContract(_) => "option_contract",
        InstrumentAny::OptionSpread(_) => "option_spread",
        InstrumentAny::PerpetualContract(_) => "perpetual_contract",
        InstrumentAny::TokenizedAsset(_) => "tokenized_asset",
    }
}

pub fn link_or_copy(source: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        return Ok(());
    }

    fs::create_dir_all(
        destination
            .parent()
            .expect("destination file always has a parent directory"),
    )?;

    match fs::hard_link(source, destination) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) if should_fallback_to_copy(&err) => {
            fs::copy(source, destination)?;
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

fn should_fallback_to_copy(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::CrossesDevices {
        return true;
    }
    // EXDEV: hard link across filesystems.
    err.raw_os_error() == Some(18)
}

#[must_use]
pub fn resolve_local_source_path(local_root: &Path, original_path: &Path) -> PathBuf {
    if original_path.exists() {
        return original_path.to_path_buf();
    }

    if let Ok(stripped) = original_path.strip_prefix("/") {
        let candidate = local_root.join(stripped);
        if candidate.exists() {
            return candidate;
        }
    }

    local_root.join(original_path)
}

pub fn mirror_market_data_path(
    local_root: &Path,
    layout_compatibility: LayoutCompatibility,
    original_path: &Path,
    legacy_prefix: &str,
    identifier: &str,
) -> Result<()> {
    if layout_compatibility != LayoutCompatibility::RustCanonicalWithPythonLegacyMirror {
        return Ok(());
    }

    let filename = original_path
        .file_name()
        .expect("catalog write returns a file path");
    let legacy_dir = local_root.join("data").join(legacy_prefix).join(identifier);
    let legacy_path = legacy_dir.join(filename);
    let source_path = resolve_local_source_path(local_root, original_path);
    link_or_copy(&source_path, &legacy_path)
}

pub fn mirror_custom_data_path(
    local_root: &Path,
    layout_compatibility: LayoutCompatibility,
    original_path: &Path,
    type_name: &str,
    identifier: Option<&str>,
) -> Result<()> {
    if layout_compatibility != LayoutCompatibility::RustCanonicalWithPythonLegacyMirror {
        return Ok(());
    }

    let filename = original_path
        .file_name()
        .expect("catalog write returns a file path");
    let legacy_prefix = format!("custom_{}", to_snake_case(type_name));
    let mut legacy_dir = local_root.join("data").join(legacy_prefix);
    if let Some(identifier) = identifier {
        legacy_dir = legacy_dir.join(identifier);
    }
    let legacy_path = legacy_dir.join(filename);
    let source_path = resolve_local_source_path(local_root, original_path);
    link_or_copy(&source_path, &legacy_path)
}

#[must_use]
pub fn instrument_legacy_prefix(instrument: &InstrumentAny) -> String {
    python_legacy_instrument_prefix(instrument).to_string()
}

#[must_use]
pub fn instrument_identifier(instrument: &InstrumentAny) -> String {
    Instrument::id(instrument).to_string()
}
