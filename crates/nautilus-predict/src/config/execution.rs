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

//! Runtime-only configuration for incremental private execution integration.

use std::sync::Arc;

use nautilus_model::identifiers::AccountId;
use rust_decimal::Decimal;

use crate::signing::PredictAuthSigner;

/// Bounded BUY LIMIT admission and inventory-backed LIMIT/MARKET SELL.
#[derive(Clone, Debug)]
pub struct PredictSubmissionConfig {
    pub signer: Arc<PredictAuthSigner>,
    /// Per-BUY collateral cap; SELL is bounded by fresh unreserved token inventory.
    pub max_order_usdt: Decimal,
    /// Lifetime attempted BUY collateral, including unresolved submissions.
    pub max_total_usdt: Decimal,
    pub require_post_only: bool,
    /// SDK default quantity-based MARKET SELL slippage, in basis points.
    pub market_slippage_bps: u32,
}

use super::{PredictChainClientConfig, PredictWalletWebSocketConfig};
use crate::http::PredictAuthenticatedHttpClient;

/// Normal execution initializes reported cash; disabled mode is for transport diagnostics only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredictAccountStateMode {
    #[default]
    Reported,
    Disabled,
}

/// A pre-authenticated session is injected; no environment credentials are read implicitly.
/// Strategy submission and REST removal require explicit bounded opt-in.
#[derive(Clone, Debug)]
pub struct PredictExecutionClientConfig {
    /// None disables SubmitOrder. No credentials are implicitly acquired by the client.
    pub submission: Option<PredictSubmissionConfig>,
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
    /// Periodic refresh also covers deposits and withdrawals outside the wallet event stream.
    /// Order events and connected `QueryAccount` commands wake the same refresh loop;
    /// they do not create additional periodic timers.
    pub account_refresh_interval_secs: u64,
    /// Gap account-verification cadence (default one second); Native owns order/position
    /// application, including confirmed late fills found during verification.
    /// None requires the host to issue QueryAccount after native reconciliation settles.
    pub reconnect_reconciliation_interval_secs: Option<u64>,
    pub report_page_size: u32,
    pub max_report_pages: usize,
    pub report_timeout_secs: u64,
}

impl PredictExecutionClientConfig {
    /// Creates bounded private integration with strategy submission and orderbook removal disabled.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        session: PredictAuthenticatedHttpClient,
        websocket: PredictWalletWebSocketConfig,
    ) -> Self {
        Self {
            submission: None,
            auth_signer: None,
            account_id,
            session,
            websocket,
            chain: None,
            account_state_mode: PredictAccountStateMode::Reported,
            orderbook_removal_enabled: false,
            account_refresh_interval_secs: 30,
            reconnect_reconciliation_interval_secs: Some(1),
            report_page_size: 100,
            max_report_pages: 100,
            report_timeout_secs: 30,
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
        if let Some(submission) = &self.submission {
            anyhow::ensure!(
                submission.signer.environment() == self.session.environment()
                    && submission.signer.account_address() == self.session.account_address(),
                "Predict submission signer differs from the authenticated account/environment"
            );
            anyhow::ensure!(
                submission.market_slippage_bps < 10_000
                    && submission.max_order_usdt > Decimal::ZERO
                    && submission.max_total_usdt >= submission.max_order_usdt,
                "invalid Predict submission budget"
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
            self.reconnect_reconciliation_interval_secs
                .is_none_or(|secs| (1..=300).contains(&secs))
                && (1..=300).contains(&self.account_refresh_interval_secs)
                && self.report_page_size > 0
                && self.report_page_size <= i32::MAX as u32
                && self.max_report_pages > 0
                && self.max_report_pages <= 10000
                && self.report_timeout_secs > 0
                && self.report_timeout_secs <= 300,
            "invalid Predict report collection bounds"
        );
        Ok(())
    }
}
