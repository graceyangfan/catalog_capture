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

//! Core-thread account verification after native order and position reconciliation.
use std::{rc::Rc, sync::atomic::Ordering};

use nautilus_common::{
    messages::execution::{QueryAccount, TradingCommand},
    msgbus::{self, switchboard::MessagingSwitchboard},
    timer::TimeEventCallback,
};
use nautilus_core::{DurationNanos, UUID4};

use super::PredictExecutionClient;

impl PredictExecutionClient {
    pub(super) fn start_recovery_timer(&mut self) -> anyhow::Result<()> {
        let Some(secs) = self.config.reconnect_reconciliation_interval_secs else {
            return Ok(());
        };
        if self.config.account_state_mode != crate::config::PredictAccountStateMode::Reported {
            return Ok(());
        }
        let (trader_id, client_id, account_id) = (
            self.core.trader_id,
            self.core.client_id,
            self.core.account_id,
        );
        let auth = self
            .wallet_auth
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Predict recovery lacks wallet authentication"))?;
        let epoch = self
            .wallet_epoch
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Predict recovery lacks wallet epoch"))?;
        let verified = self.verified_epoch.clone();
        let required = self.reconciliation_required.clone();
        let checking = self.account_query_active.clone();
        let healthy = self.healthy.clone();
        let cancellation = self.tasks.cancellation_token();
        let clock = self.clock;
        let timer = self.recovery_clock.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "Predict automatic recovery requires the factory-provided Nautilus clock"
            )
        })?;
        timer.borrow_mut().set_timer_ns(
            &format!("predict-execution-recovery-{}", self.core.client_id),
            DurationNanos::from_secs(secs),
            None,
            None,
            Some(TimeEventCallback::RustLocal(Rc::new(move |_| {
                if cancellation.is_cancelled() || !healthy.load(Ordering::Acquire) {
                    return;
                }
                if !auth.is_authenticated()
                    || epoch.load(Ordering::Acquire) != verified.load(Ordering::Acquire)
                {
                    required.store(true, Ordering::Release);
                }
                if !required.load(Ordering::Acquire)
                    || !auth.is_authenticated()
                    || checking.load(Ordering::Acquire)
                {
                    return;
                }
                // LiveNode owns inflight/order/position checks. Only account readiness
                // needs a core-thread retry after the new wallet epoch and native fills apply.
                let endpoint = MessagingSwitchboard::exec_engine_queue_execute();
                msgbus::send_trading_command(
                    endpoint,
                    TradingCommand::QueryAccount(QueryAccount::new(
                        trader_id,
                        Some(client_id),
                        account_id,
                        UUID4::new(),
                        clock.get_time_ns(),
                        None,
                        None,
                    )),
                );
            }))),
            None,
            Some(false),
        )?;
        Ok(())
    }
}

impl PredictExecutionClient {
    pub(super) fn stop_recovery_timer(&self) {
        if let Some(clock) = &self.recovery_clock {
            let mut clock = clock.borrow_mut();
            let name = format!("predict-execution-recovery-{}", self.core.client_id);
            if clock.timer_exists(&name.as_str().into()) {
                clock.cancel_timer(&name);
            }
        }
    }
}
