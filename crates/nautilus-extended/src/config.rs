// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use nautilus_model::identifiers::InstrumentId;
use nautilus_network::websocket::TransportBackend;
use serde::{Deserialize, Serialize};

use crate::common::ExtendedEnvironment;

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct ExtendedInstrumentProviderConfig {
    #[builder(default = true)]
    pub load_all: bool,
    pub load_ids: Option<Vec<InstrumentId>>,
}

impl Default for ExtendedInstrumentProviderConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ExtendedInstrumentProviderConfig {
    #[must_use]
    pub fn includes(&self, instrument_id: InstrumentId) -> bool {
        self.load_all
            || self
                .load_ids
                .as_ref()
                .is_some_and(|ids| ids.contains(&instrument_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct ExtendedDataClientConfig {
    #[builder(default)]
    pub environment: ExtendedEnvironment,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub proxy_url: Option<String>,
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    #[builder(default = 10)]
    pub ws_timeout_secs: u64,
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
    #[builder(default)]
    pub instrument_provider: ExtendedInstrumentProviderConfig,
    #[builder(default)]
    pub transport_backend: TransportBackend,
}

impl Default for ExtendedDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ExtendedDataClientConfig {
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| self.environment.http_url().to_string())
    }

    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| self.environment.ws_url().to_string())
    }
}
