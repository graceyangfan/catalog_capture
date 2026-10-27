// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Runtime configuration for the Predict execution client.

use std::sync::Arc;

use nautilus_model::identifiers::AccountId;

use crate::signing::PredictAuthSigner;

use super::{PredictChainClientConfig, PredictWalletWebSocketConfig};
use crate::http::PredictAuthenticatedHttpClient;

/// Normal execution initializes reported cash; disabled mode is for transport diagnostics only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredictAccountStateMode {
    #[default]
    Reported,
    Disabled,
}

/// Execution configuration with an injected authenticated session.
/// Submission and REST removal require explicit opt-in.
#[derive(Clone, Debug)]
pub struct PredictExecutionClientConfig {
    /// None disables SubmitOrder. No credentials are implicitly acquired by the client.
    pub submission_signer: Option<Arc<PredictAuthSigner>>,
    /// Optional bounded telemetry. The host owns/drains the collector away from trading callbacks.
    pub latency: Option<crate::latency::PredictLatency>,
    /// Optional authentication authority, independent of permission to submit orders.
    pub auth_signer: Option<Arc<PredictAuthSigner>>,
    pub account_id: AccountId,
    pub session: PredictAuthenticatedHttpClient,
    pub websocket: PredictWalletWebSocketConfig,
    /// With a source configured, connect initializes a reported cash account through Portfolio.
    pub chain: Option<PredictChainClientConfig>,
    pub account_state_mode: PredictAccountStateMode,
    /// Explicit opt-in to REST orderbook removal, which does not invalidate a signature on-chain.
    /// Commands verify cached GTC/GTD limit orders; `BatchCancelOrders` groups eligible IDs
    /// in removal requests and resolves each result separately. No chain transactions are sent.
    pub orderbook_removal_enabled: bool,
    /// Cash/OPEN refresh interval in seconds, 1..=86400; default 30.
    /// Order events reproject local reservations. QueryAccount uses the same collector
    /// and permits authenticated reads during a wallet gap.
    pub account_refresh_interval_secs: u64,
    /// Maintenance interval after wallet transport changes; default one second.
    /// Refreshes cash and retires purged contexts. Native owns order/fill/position recovery.
    /// None requires explicit QueryAccount after a transport change; cleanup then runs
    /// at instrument, report and account-query boundaries.
    pub reconnect_reconciliation_interval_secs: Option<u64>,
    /// Market signing-metadata TTL in seconds, 1..=300; default 30.
    /// YES/NO share preparation at connection, definition changes and before expiry.
    /// Cold commands fail locally and request background preparation. Wallet reconnect
    /// preserves valid values. Contract nonces have independent, event-driven recovery.
    pub market_metadata_ttl_secs: u64,
    pub report_page_size: u32,
    pub max_report_pages: usize,
    /// Maximum duration of each asynchronous command, account or report request.
    /// The timer never wraps wallet callbacks or the synchronous Native command entry point.
    pub request_timeout_secs: u64,
}

impl PredictExecutionClientConfig {
    /// Creates private integration with strategy submission and orderbook removal disabled.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        session: PredictAuthenticatedHttpClient,
        websocket: PredictWalletWebSocketConfig,
    ) -> Self {
        Self {
            submission_signer: None,
            latency: None,
            auth_signer: None,
            account_id,
            session,
            websocket,
            chain: None,
            account_state_mode: PredictAccountStateMode::Reported,
            orderbook_removal_enabled: false,
            account_refresh_interval_secs: 30,
            reconnect_reconciliation_interval_secs: Some(1),
            market_metadata_ttl_secs: 30,
            report_page_size: 100,
            max_report_pages: 100,
            request_timeout_secs: 30,
        }
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Some(signer) = &self.auth_signer {
            anyhow::ensure!(
                signer.environment() == self.session.environment()
                    && signer.account_address() == self.session.account_address(),
                "Predict authentication signer differs from session"
            );
        }
        if let Some(signer) = &self.submission_signer {
            anyhow::ensure!(
                signer.environment() == self.session.environment()
                    && signer.account_address() == self.session.account_address(),
                "Predict submission signer differs from the authenticated account/environment"
            );
        }
        anyhow::ensure!(
            self.account_id.get_issuer() == *crate::common::PREDICT_VENUE,
            "Predict account ID must use the PREDICT issuer"
        );
        anyhow::ensure!(
            self.session.environment() == self.websocket.environment,
            "Predict execution transport environments differ"
        );
        self.websocket.validated_ws_url()?;
        if let Some(chain) = &self.chain {
            chain.validate()?;
            anyhow::ensure!(
                chain.environment == self.session.environment(),
                "Predict RPC and REST environments differ"
            );
        }
        anyhow::ensure!(
            (1..=300).contains(&self.market_metadata_ttl_secs),
            "Predict market metadata TTL must be 1..=300 seconds; zero is unsupported"
        );
        anyhow::ensure!(
            (1..=86_400).contains(&self.account_refresh_interval_secs),
            "Predict account refresh interval must be 1..=86400 seconds"
        );
        anyhow::ensure!(
            self.reconnect_reconciliation_interval_secs
                .is_none_or(|secs| (1..=300).contains(&secs))
                && self.report_page_size > 0
                && self.report_page_size <= i32::MAX as u32
                && self.max_report_pages > 0
                && self.max_report_pages <= 10000
                && self.request_timeout_secs > 0
                && self.request_timeout_secs <= 300,
            "invalid Predict request or report collection bounds"
        );
        Ok(())
    }
}
