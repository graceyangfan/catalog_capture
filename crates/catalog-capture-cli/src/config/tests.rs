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

use super::*;
use crate::runner::validate_runtime;
use catalog_capture_core::{OptionUniverseFamily, StrikePolicy};
use std::path::{Path, PathBuf};

#[cfg(feature = "venue-hyperliquid")]
use nautilus_hyperliquid::common::enums::HyperliquidEnvironment;
#[cfg(feature = "venue-okx")]
use nautilus_okx::common::enums::OKXInstrumentType;

#[cfg(feature = "venue-hyperliquid")]
use super::venues::parse_hyperliquid_environment;
#[cfg(feature = "venue-okx")]
use super::venues::parse_okx_instrument_types;
use super::venues::{default_binance_product_type, parse_venue};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

#[cfg(feature = "venue-okx")]
#[test]
fn okx_default_instrument_types_is_swap_only() {
    let types = parse_okx_instrument_types(&[]).expect("defaults should parse");
    assert_eq!(types, vec![OKXInstrumentType::Swap]);
}

#[cfg(feature = "venue-okx")]
#[test]
fn okx_option_without_families_is_rejected() {
    let venue = VenueConfig {
        id: "okx_main".to_string(),
        kind: "okx".to_string(),
        environment: "live".to_string(),
        product_type: default_binance_product_type(),
        product_types: Vec::new(),
        instrument_types: vec!["option".to_string()],
        instrument_families: Vec::new(),
    };

    let err = parse_venue(venue).expect_err("option without families should fail");
    assert!(
        err.to_string().contains("instrument_families"),
        "unexpected error: {err}"
    );
}

#[cfg(feature = "venue-okx")]
#[test]
fn okx_option_with_families_is_accepted() {
    let venue = VenueConfig {
        id: "okx_main".to_string(),
        kind: "okx".to_string(),
        environment: "live".to_string(),
        product_type: default_binance_product_type(),
        product_types: Vec::new(),
        instrument_types: vec!["swap".to_string(), "option".to_string()],
        instrument_families: vec!["BTC-USD".to_string()],
    };

    let runtime = parse_venue(venue).expect("valid okx option venue");
    assert!(matches!(runtime, VenueRuntimeConfig::Okx { .. }));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_rejects_deribit_dvol_without_index_name() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "DeribitVolatilityIndex".to_string(),
                identifier: None,
                metadata: Default::default(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("missing index_name should fail");
    assert!(err.to_string().contains("metadata.index_name"));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_accepts_deribit_dvol_with_index_name() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("index_name".to_string(), "btc_usd".to_string());

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "DeribitVolatilityIndex".to_string(),
                identifier: None,
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("valid dvol config should pass");
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_accepts_binance_perp_trades_profile() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            trades: vec![InstrumentSelector {
                instrument_id: "ETHUSDT-PERP.BINANCE".to_string(),
            }],
            quotes: vec![InstrumentSelector {
                instrument_id: "ETHUSDT-PERP.BINANCE".to_string(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_futures_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("binance perp trades config should pass");
    assert_eq!(effective.plan.trades.len(), 1);
    assert_eq!(
        effective.plan.trades[0].instrument_id.to_string(),
        "ETHUSDT-PERP.BINANCE"
    );
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_accepts_binance_liquidation_custom_data() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "instrument_id".to_string(),
        "ETHUSDT-PERP.BINANCE".to_string(),
    );

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "BinanceFuturesLiquidation".to_string(),
                identifier: Some("ETHUSDT-PERP.BINANCE".to_string()),
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("liquidation should validate");
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_accepts_binance_liquidation_all_market_custom_data() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "BinanceFuturesLiquidation".to_string(),
                identifier: None,
                metadata: std::collections::BTreeMap::new(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("all-market liquidation should validate");
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_rejects_binance_liquidation_identifier_without_metadata() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "BinanceFuturesLiquidation".to_string(),
                identifier: Some("ETHUSDT-PERP.BINANCE".to_string()),
                metadata: std::collections::BTreeMap::new(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("identifier-only liquidation should fail");
    assert!(err
        .to_string()
        .contains("identifier requires metadata.instrument_id"));
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_accepts_binance_ticker_custom_data() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "instrument_id".to_string(),
        "ETHUSDT-PERP.BINANCE".to_string(),
    );

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "BinanceFuturesTicker".to_string(),
                identifier: Some("ETHUSDT-PERP.BINANCE".to_string()),
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("ticker should validate");
}

/// C3: request-only type in [[capture.custom_data]] must fail validate_runtime.
#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_rejects_request_type_in_subscribe_custom_data() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("currency".to_string(), "BTC".to_string());

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "DeribitBookSummary".to_string(),
                identifier: None,
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve (channel check is at runtime validate)");

    let err = validate_runtime(&effective).expect_err("request-only type in custom_data");
    let msg = err.to_string();
    assert!(
        msg.contains("request-only") && msg.contains("custom_data_requests"),
        "unexpected error: {msg}"
    );
}

/// C3: subscribe-only type in [[capture.custom_data_requests]] must fail at parse/resolve.
#[cfg(feature = "venue-deribit")]
#[test]
fn resolve_config_rejects_subscribe_type_in_custom_data_requests() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("index_name".to_string(), "btc_usd".to_string());

    let err = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data_requests: vec![CustomDataRequestSelector {
                type_name: "DeribitVolatilityIndex".to_string(),
                identifier: None,
                metadata,
                interval_secs: 5,
                fire_immediately: true,
                overlap_policy: "skip".to_string(),
                request_timeout_secs: 10,
                client_id: None,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect_err("subscribe-only type in custom_data_requests");

    let msg = err.to_string();
    assert!(
        msg.contains("subscribe-only") || msg.contains("[[capture.custom_data]]"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_rejects_binance_open_interest_without_request_path() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "BinanceFuturesOpenInterest".to_string(),
                identifier: Some("ETHUSDT-PERP.BINANCE".to_string()),
                metadata: std::collections::BTreeMap::new(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "binance_main".to_string(),
            kind: "binance_futures".to_string(),
            environment: "testnet".to_string(),
            product_type: "usd_m".to_string(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("open interest should fail early");
    let msg = err.to_string();
    assert!(
        msg.contains("request/poll")
            || msg.contains("custom_data_requests")
            || msg.contains("unknown custom_data"),
        "unexpected error: {msg}"
    );
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn hyperliquid_environment_aliases_parse() {
    assert_eq!(
        parse_hyperliquid_environment("live").expect("live should parse"),
        HyperliquidEnvironment::Mainnet
    );
    assert_eq!(
        parse_hyperliquid_environment("testnet").expect("testnet should parse"),
        HyperliquidEnvironment::Testnet
    );
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn parse_hyperliquid_venue_is_supported() {
    let venue = VenueConfig {
        id: "hl_main".to_string(),
        kind: "hyperliquid".to_string(),
        environment: "testnet".to_string(),
        product_type: default_binance_product_type(),
        product_types: Vec::new(),
        instrument_types: Vec::new(),
        instrument_families: Vec::new(),
    };

    let runtime = parse_venue(venue).expect("valid hyperliquid venue");
    assert!(matches!(runtime, VenueRuntimeConfig::Hyperliquid { .. }));
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn validate_runtime_accepts_hyperliquid_open_interest() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "instrument_id".to_string(),
        "ETH-USD-PERP.HYPERLIQUID".to_string(),
    );

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "HyperliquidOpenInterest".to_string(),
                identifier: Some("ETH-USD-PERP.HYPERLIQUID".to_string()),
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "hl_main".to_string(),
            kind: "hyperliquid".to_string(),
            environment: "testnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("valid hyperliquid OI config should pass");
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_rejects_unknown_custom_type() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "HyperliquidOpenInterset".to_string(),
                identifier: Some("ETH-USD-PERP.HYPERLIQUID".to_string()),
                metadata: Default::default(),
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "hl_main".to_string(),
            kind: "hyperliquid".to_string(),
            environment: "testnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("unknown custom type should fail");
    assert!(err.to_string().contains("unknown custom_data type_name"));
}

#[cfg(feature = "venue-binance")]
#[test]
fn validate_runtime_rejects_deribit_dvol_without_deribit_venue() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert("index_name".to_string(), "btc_usd".to_string());

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "DeribitVolatilityIndex".to_string(),
                identifier: None,
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "hl_main".to_string(),
            kind: "hyperliquid".to_string(),
            environment: "testnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("missing deribit venue should fail");
    assert!(err.to_string().contains("kind = \"deribit\""));
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn validate_runtime_rejects_hyperliquid_identifier_mismatch() {
    let mut metadata = std::collections::BTreeMap::new();
    metadata.insert(
        "instrument_id".to_string(),
        "ETH-USD-PERP.HYPERLIQUID".to_string(),
    );

    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            custom_data: vec![CustomDataSelector {
                type_name: "HyperliquidOpenInterest".to_string(),
                identifier: Some("BTC-USD-PERP.HYPERLIQUID".to_string()),
                metadata,
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "hl_main".to_string(),
            kind: "hyperliquid".to_string(),
            environment: "testnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("identifier mismatch should fail");
    assert!(err
        .to_string()
        .contains("must match metadata.instrument_id"));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn resolve_config_accepts_option_universe_without_explicit_capture_entries() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                    "index_prices".to_string(),
                    "funding_rates".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["future".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("option universe config should resolve");

    assert!(effective.plan.is_empty());
    assert_eq!(effective.option_universes.len(), 1);
    validate_runtime(&effective).expect("runtime validation should pass");
}

#[cfg(feature = "venue-deribit")]
#[test]
fn resolve_config_rejects_unknown_option_universe_family() {
    let err = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec!["books".to_string()],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["future".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect_err("unknown family should fail");

    assert!(err
        .to_string()
        .contains("unsupported capture.option_universe family"));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_rejects_option_universe_missing_future_product_type_for_perp() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective)
        .expect_err("include_perp without future product type should fail");
    assert!(err.to_string().contains("include \"future\""));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_rejects_option_universe_unknown_venue_id() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "deribit_missing".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["future".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective).expect_err("unknown venue id should fail");
    assert!(err.to_string().contains("unknown venue_id"));
}

#[cfg(feature = "venue-bybit")]
#[test]
fn validate_runtime_accepts_bybit_option_universe() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "bybit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("USDT".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                    "index_prices".to_string(),
                    "funding_rates".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "bybit_main".to_string(),
            kind: "bybit".to_string(),
            environment: "mainnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["linear".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("valid bybit option universe should pass");
}

#[cfg(feature = "venue-deribit")]
#[test]
fn validate_runtime_accepts_signal_only_capture_seconds() {
    let mut effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "deribit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("BTC".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "deribit_main".to_string(),
            kind: "deribit".to_string(),
            environment: "mainnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["future".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");
    effective.runtime.capture_seconds = 0;
    validate_runtime(&effective).expect("signal-only capture should validate");
}

#[cfg(feature = "venue-bybit")]
#[test]
fn validate_runtime_rejects_bybit_option_universe_without_settlement_currency() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "bybit_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: None,
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "bybit_main".to_string(),
            kind: "bybit".to_string(),
            environment: "mainnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: vec!["linear".to_string(), "option".to_string()],
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective)
        .expect_err("missing settlement_currency should fail for bybit");
    assert!(err.to_string().contains("requires settlement_currency"));
}

#[cfg(feature = "venue-okx")]
#[test]
fn validate_runtime_accepts_okx_option_universe() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "okx_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("USD".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                    "index_prices".to_string(),
                    "funding_rates".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "okx_main".to_string(),
            kind: "okx".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: vec!["swap".to_string(), "option".to_string()],
            instrument_families: vec!["BTC-USD".to_string()],
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    validate_runtime(&effective).expect("valid okx option universe should pass");
}

#[cfg(feature = "venue-okx")]
#[test]
fn validate_runtime_rejects_okx_option_universe_without_settlement_currency() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "okx_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: None,
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "okx_main".to_string(),
            kind: "okx".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: vec!["swap".to_string(), "option".to_string()],
            instrument_families: vec!["BTC-USD".to_string()],
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err =
        validate_runtime(&effective).expect_err("missing settlement_currency should fail for okx");
    assert!(err.to_string().contains("requires settlement_currency"));
}

#[cfg(feature = "venue-okx")]
#[test]
fn validate_runtime_rejects_okx_option_universe_without_matching_instrument_family() {
    let effective = resolve_config(CliConfigFile {
        capture: CaptureConfigFile {
            option_universe: vec![OptionUniverseSelector {
                venue_id: "okx_main".to_string(),
                underlying: "BTC".to_string(),
                settlement_currency: Some("USD".to_string()),
                include_perp: true,
                families: vec![
                    "instruments".to_string(),
                    "quotes".to_string(),
                    "option_greeks".to_string(),
                ],
                expiry_policy: ExpiryPolicySelector {
                    mode: "nearest".to_string(),
                    days_max: 45,
                },
                strike_policy: StrikePolicySelector {
                    mode: "atm_relative".to_string(),
                    strikes_above: 1,
                    strikes_below: 1,
                    top_n: None,
                },
            }],
            ..Default::default()
        },
        venues: vec![VenueConfig {
            id: "okx_main".to_string(),
            kind: "okx".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: vec!["swap".to_string(), "option".to_string()],
            instrument_families: vec!["ETH-USD".to_string()],
        }],
        ..Default::default()
    })
    .expect("config should resolve");

    let err = validate_runtime(&effective)
        .expect_err("missing matching instrument_family should fail for okx");
    assert!(err.to_string().contains("instrument_families"));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_dvol_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-dvol.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn example_hyperliquid_open_interest_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.hyperliquid-open-interest.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-binance")]
#[test]
fn example_binance_futures_liquidation_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.binance-futures-liquidation.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-binance")]
#[test]
fn example_binance_futures_ticker_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.binance-futures-ticker.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-binance")]
#[test]
fn example_binance_perp_bars_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.binance-perp-bars.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn example_hyperliquid_bars_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.hyperliquid-bars.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn example_hyperliquid_hip4_btc_daily_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.hyperliquid-hip4-btc-daily.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert_eq!(effective.hip4_universes.len(), 1);
    assert_eq!(effective.hip4_universes[0].market_class, "priceBinary");
    assert!(effective.runtime.hip4_universe_refresh.enabled);
    assert!(
        effective
            .runtime
            .hip4_universe_refresh
            .purge_removed_instruments
    );
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_book_deltas_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-book-deltas.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(effective
        .option_universes
        .iter()
        .any(|spec| spec.families.contains(&OptionUniverseFamily::BookDeltas)));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_oi_ranked_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-oi-ranked.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::OiRanked { top_n: 3 }
    ));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_autorefresh_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-autorefresh.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_oi_ranked_autorefresh_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-oi-ranked-autorefresh.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert_eq!(
        effective
            .runtime
            .option_universe_refresh
            .strike_change_confirmations,
        2
    );
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_research_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-research.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-bybit")]
#[test]
fn example_bybit_option_universe_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.bybit-btc-universe.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-okx")]
#[test]
fn example_okx_option_universe_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.okx-btc-universe.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-bybit")]
#[test]
fn example_bybit_option_universe_autorefresh_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.bybit-btc-universe-autorefresh.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-okx")]
#[test]
fn example_okx_option_universe_autorefresh_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.okx-btc-universe-autorefresh.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
}

#[cfg(feature = "venue-bybit")]
#[test]
fn example_bybit_option_universe_oi_ranked_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.bybit-btc-universe-oi-ranked.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::OiRanked { top_n: 3 }
    ));
}

#[cfg(feature = "venue-okx")]
#[test]
fn example_okx_option_universe_oi_ranked_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.okx-btc-universe-oi-ranked.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::OiRanked { top_n: 3 }
    ));
}

#[cfg(feature = "venue-deribit")]
#[test]
fn example_deribit_option_universe_all_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.deribit-btc-universe-all.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::AllStrikes
    ));
}

#[cfg(feature = "venue-bybit")]
#[test]
fn example_bybit_option_universe_all_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.bybit-btc-universe-all.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::AllStrikes
    ));
}

#[cfg(feature = "venue-okx")]
#[test]
fn example_okx_option_universe_all_config_loads_and_validates() {
    let path = repo_root().join("examples/capture.okx-btc-universe-all.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    validate_runtime(&effective).expect("example should validate");
    assert!(matches!(
        effective.option_universes[0].strike_policy,
        StrikePolicy::AllStrikes
    ));
}

#[cfg(feature = "venue-hyperliquid")]
#[test]
fn example_hyperliquid_perp_daily_segment_lifecycle_loads() {
    use catalog_capture_core::LifecycleMode;

    let path = repo_root().join("examples/capture.hyperliquid-perp-daily.toml");
    let loaded = load_config(&path).expect("example should load");
    let effective = resolve_config(loaded).expect("example should resolve");
    assert!(matches!(
        effective.capture.lifecycle.mode,
        LifecycleMode::Segment
    ));
    assert!(effective.capture.lifecycle.seal.enabled);
}
