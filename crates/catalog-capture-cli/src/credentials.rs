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

//! Venue credentials — two modes only:
//!
//! 1. **Public** (default): `api_key` / `api_secret` = `None`
//! 2. **Authenticated**: both key and secret set from env (complete pair)
//!
//! Secrets never come from TOML. Incomplete env (only key, only secret) → public.

use std::env;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeySecret {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OkxCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    })
}

/// Venue-id scoped name, e.g. `deribit_main` + `API_KEY` → `CAPTURE_VENUE_DERIBIT_MAIN_API_KEY`.
fn scoped(venue_id: &str, suffix: &str) -> String {
    let id = venue_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("CAPTURE_VENUE_{id}_{suffix}")
}

fn read_pair(venue_id: &str, kind: &str) -> ApiKeySecret {
    let kind = kind.to_ascii_uppercase();
    let key = env_nonempty(&scoped(venue_id, "API_KEY"))
        .or_else(|| env_nonempty(&format!("{kind}_API_KEY")));
    let secret = env_nonempty(&scoped(venue_id, "API_SECRET"))
        .or_else(|| env_nonempty(&format!("{kind}_API_SECRET")));
    match (key, secret) {
        (Some(api_key), Some(api_secret)) => ApiKeySecret {
            api_key: Some(api_key),
            api_secret: Some(api_secret),
        },
        _ => ApiKeySecret::default(), // public
    }
}

pub fn binance_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "BINANCE")
}

pub fn deribit_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "DERIBIT")
}

pub fn bybit_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "BYBIT")
}

pub fn okx_credentials(venue_id: &str) -> OkxCredentials {
    let key = env_nonempty(&scoped(venue_id, "API_KEY")).or_else(|| env_nonempty("OKX_API_KEY"));
    let secret =
        env_nonempty(&scoped(venue_id, "API_SECRET")).or_else(|| env_nonempty("OKX_API_SECRET"));
    let passphrase = env_nonempty(&scoped(venue_id, "API_PASSPHRASE"))
        .or_else(|| env_nonempty("OKX_API_PASSPHRASE"))
        .or_else(|| env_nonempty("OKX_PASSPHRASE"));
    match (key, secret, passphrase) {
        (Some(api_key), Some(api_secret), Some(api_passphrase)) => OkxCredentials {
            api_key: Some(api_key),
            api_secret: Some(api_secret),
            api_passphrase: Some(api_passphrase),
        },
        _ => OkxCredentials::default(), // public
    }
}

pub fn hyperliquid_private_key(venue_id: &str) -> Option<String> {
    env_nonempty(&scoped(venue_id, "PRIVATE_KEY"))
        .or_else(|| env_nonempty("HYPERLIQUID_PRIVATE_KEY"))
        .or_else(|| env_nonempty("HL_PRIVATE_KEY"))
}

pub fn api_key_secret_present(creds: &ApiKeySecret) -> bool {
    creds.api_key.is_some() && creds.api_secret.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear() {
        for k in [
            "BINANCE_API_KEY",
            "BINANCE_API_SECRET",
            "DERIBIT_API_KEY",
            "DERIBIT_API_SECRET",
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "OKX_API_KEY",
            "OKX_API_SECRET",
            "OKX_API_PASSPHRASE",
            "HYPERLIQUID_PRIVATE_KEY",
            "CAPTURE_VENUE_DERIBIT_MAIN_API_KEY",
            "CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET",
        ] {
            env::remove_var(k);
        }
    }

    #[test]
    fn no_env_is_public() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        assert_eq!(binance_credentials("x"), ApiKeySecret::default());
        assert!(!api_key_secret_present(&deribit_credentials(
            "deribit_main"
        )));
    }

    #[test]
    fn only_key_is_public() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        env::set_var("DERIBIT_API_KEY", "only-key");
        assert_eq!(deribit_credentials("deribit_main"), ApiKeySecret::default());
        clear();
    }

    #[test]
    fn both_key_and_secret_is_authenticated() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        env::set_var("DERIBIT_API_KEY", "k");
        env::set_var("DERIBIT_API_SECRET", "s");
        let c = deribit_credentials("deribit_main");
        assert_eq!(c.api_key.as_deref(), Some("k"));
        assert_eq!(c.api_secret.as_deref(), Some("s"));
        assert!(api_key_secret_present(&c));
        clear();
    }

    #[test]
    fn scoped_overrides_global() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        env::set_var("DERIBIT_API_KEY", "global-k");
        env::set_var("DERIBIT_API_SECRET", "global-s");
        env::set_var("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY", "scoped-k");
        env::set_var("CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET", "scoped-s");
        let c = deribit_credentials("deribit_main");
        assert_eq!(c.api_key.as_deref(), Some("scoped-k"));
        assert_eq!(c.api_secret.as_deref(), Some("scoped-s"));
        clear();
    }
}
