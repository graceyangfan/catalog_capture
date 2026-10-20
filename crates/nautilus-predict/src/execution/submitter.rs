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

//! Admission, signing and one-shot private submission over owned worker snapshots.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use alloy::primitives::{Address, U256};
use nautilus_common::factories::OrderEventFactory;
use nautilus_core::{AtomicMap, time::AtomicTime};
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure};
use nautilus_model::{
    enums::{OrderSide, OrderType},
    identifiers::{InstrumentId, VenueOrderId},
    instruments::{BinaryOption, Instrument, InstrumentAny},
    orders::{Order, OrderAny},
};
use nautilus_network::websocket::AuthTracker;
use rust_decimal::Decimal;

use super::{
    order_builder::decimal_wei,
    orders::{PendingSubmissions, SubmissionGuard},
    types::classify_http_command_failure,
};
use crate::{
    blockchain::PredictChainClient,
    common::{
        enums::{PredictOrderSide, PredictSignatureType},
        parse::exact_wei,
    },
    config::{PredictExecutionClientConfig, PredictSubmissionConfig},
    http::{PredictAuthenticatedHttpClient, order_models::PredictOrder},
    signing::PredictOrderSigner,
};

pub(super) struct SubmitWorker {
    pub signer: Arc<PredictOrderSigner>,
    pub session: PredictAuthenticatedHttpClient,
    pub chain: PredictChainClient,
    pub config: PredictExecutionClientConfig,
    pub policy: PredictSubmissionConfig,
    pub order: Arc<OrderAny>,
    pub instrument: BinaryOption,
    pub price: U256,
    pub shares: U256,
    pub expiry: U256,
    pub salt: U256,
    pub cost: Decimal,
    pub free: Decimal,
    pub pending: PendingSubmissions,
    pub admission: Arc<tokio::sync::Mutex<()>>,
    pub orders: Arc<AtomicMap<VenueOrderId, Arc<OrderAny>>>,
    pub rest_ids: super::routing::RestOrderIds,
    pub reconciliation_required: Arc<AtomicBool>,
    pub account_sync: Arc<super::account::AccountSync>,
    pub instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    pub healthy: Arc<AtomicBool>,
    pub authentication: AuthTracker,
    pub epoch: Arc<AtomicU64>,
    pub admitted_epoch: u64,
    pub emitter: ExecutionEventEmitter,
    pub factory: OrderEventFactory,
    pub clock: &'static AtomicTime,
}

impl SubmitWorker {
    pub(super) fn available(&self) -> bool {
        !self.reconciliation_required.load(Ordering::Acquire)
            && self.healthy.load(Ordering::Acquire)
            && self.authentication.is_authenticated()
            && self.epoch.load(Ordering::Acquire) == self.admitted_epoch
    }

    async fn preflight(
        &self,
        market: crate::http::market_models::PredictExecutionMarket,
        rows: Vec<crate::http::account_models::PredictOrderData>,
    ) -> anyhow::Result<(
        crate::http::order_models::PredictSignedOrder,
        Option<crate::common::market::PredictMarketOrderAmounts>,
    )> {
        let market_id = super::parse::market_id_from_instrument(&self.instrument.id())?;
        let token: U256 = self
            .instrument
            .raw_symbol()
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("Predict token ID is invalid"))?;
        let outcome = if self.instrument.id().symbol.as_str().ends_with("-1") {
            1
        } else {
            2
        };
        anyhow::ensure!(
            market.trading_status == "OPEN",
            "Predict market is not open"
        );
        anyhow::ensure!(
            market.fee_rate_bps <= 10_000,
            "Predict market fee exceeds supported range"
        );
        // Reported cash and native position reconciliation account for BUY fees in shares.
        // Submission policy still enforces post-only when the operator requires it.
        anyhow::ensure!(
            market.decimal_precision == self.instrument.price_precision()
                && market.outcomes.len() == 2
                && market
                    .outcomes
                    .iter()
                    .filter(|o| o.index_set == outcome && o.on_chain_id == token)
                    .count()
                    == 1,
            "Predict market outcome/token/precision differs from the native instrument"
        );
        let side = if self.order.order_side() == OrderSide::Sell {
            PredictOrderSide::Sell
        } else {
            PredictOrderSide::Buy
        };
        let mut locked = Decimal::ZERO;
        let mut locked_shares = U256::ZERO;
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let hash = super::resources::open_order_hash(&row)?;
            anyhow::ensure!(
                seen.insert(hash),
                "Predict open-order admission pages contain duplicates"
            );
            locked_shares = locked_shares
                .checked_add(super::resources::signed_inventory(&row, token))
                .ok_or_else(|| anyhow::anyhow!("Predict SELL reservations overflow"))?;
            locked = locked
                .checked_add(super::resources::signed_admission_collateral(&row)?)
                .ok_or_else(|| anyhow::anyhow!("Predict aggregate reserve overflow"))?;
        }
        // OPEN can lag a transmitted order, including an unknown POST outcome. Union
        // protocol evidence with native snapshots by exchange hash, never sum both.
        // Admission stays serialized until the signed hash/resource claim is installed,
        // so another worker cannot transmit between this snapshot and registration.
        for (hash, order) in self.orders.load().iter() {
            if order.client_order_id() == self.order.client_order_id()
                || order.is_closed()
                || seen.contains(&hash.as_str().parse::<alloy::primitives::B256>()?)
            {
                continue;
            }
            if order.order_side() == OrderSide::Buy {
                locked = locked
                    .checked_add(super::resources::native_admission_collateral(order)?)
                    .ok_or_else(|| anyhow::anyhow!("Predict aggregate reserve overflow"))?;
            } else if order.order_side() == OrderSide::Sell {
                let instrument = self
                    .instruments
                    .get_cloned(&order.instrument_id())
                    .ok_or_else(|| anyhow::anyhow!("Predict pending SELL instrument is missing"))?;
                if instrument.raw_symbol().as_str().parse::<U256>()? == token {
                    locked_shares = locked_shares
                        .checked_add(decimal_wei(super::resources::native_inventory(order))?)
                        .ok_or_else(|| anyhow::anyhow!("Predict native SELL reserve overflow"))?;
                }
            }
        }
        let now = self.clock.get_time_ns().as_u64();
        let (nonce, timestamp) = if side == PredictOrderSide::Sell {
            let (balance, approved, nonce, timestamp) = self
                .chain
                .request_sell_admission(
                    self.session.account_address(),
                    market.exchange(),
                    token,
                    self.clock,
                )
                .await?;
            anyhow::ensure!(
                approved,
                "Predict exchange ERC1155 approval is missing; no approval was sent"
            );
            anyhow::ensure!(
                balance
                    .checked_sub(locked_shares)
                    .is_some_and(|free| free >= self.shares),
                "Predict fresh net inventory minus outstanding SELL reservations is insufficient"
            );
            (nonce, timestamp)
        } else {
            let (snapshot, nonce, timestamp) = self
                .chain
                .request_buy_admission(
                    self.session.account_address(),
                    market.exchange(),
                    self.clock,
                )
                .await?;
            let funds = exact_wei(snapshot.balance_wei)?
                .checked_sub(locked)
                .ok_or_else(|| anyhow::anyhow!("Predict free balance underflow"))?;
            anyhow::ensure!(
                self.cost <= funds.min(self.free),
                "Predict fresh collateral minus outstanding BUY reservations is insufficient"
            );
            let allowance = snapshot
                .allowances
                .iter()
                .find(|(exchange, _)| *exchange == market.exchange())
                .map(|(_, value)| *value)
                .unwrap_or_default();
            let cost = decimal_wei(self.cost)?;
            anyhow::ensure!(
                allowance >= cost,
                "Predict exchange USDT allowance is insufficient; no approval was sent"
            );
            (nonce, timestamp)
        };
        let latest_fill = self
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .latest_fill;
        let newest_fill = latest_fill;
        anyhow::ensure!(
            timestamp >= newest_fill,
            "Predict admission block predates confirmed native fills"
        );
        anyhow::ensure!(
            timestamp.as_u64() <= now.saturating_add(5_000_000_000)
                && now.saturating_sub(timestamp.as_u64()) <= 30_000_000_000,
            "Predict admission block is stale or in the future"
        );
        let amounts = if self.order.order_type() == OrderType::Market {
            let book = self.session.request_execution_orderbook(market_id).await?;
            let observed = self.clock.get_time_ns().as_u64();
            anyhow::ensure!(
                book.market_id == market_id
                    && book
                        .update_timestamp_ms
                        .checked_mul(1_000_000)
                        .and_then(|ts| observed.checked_sub(ts))
                        .is_some(),
                "Predict MARKET orderbook identity or freshness is invalid"
            );
            let levels = if outcome == 1 {
                book.bids
            } else {
                book.asks
                    .into_iter()
                    .map(|(px, qty)| (Decimal::ONE - px, qty))
                    .collect()
            };
            let levels = levels
                .into_iter()
                .map(|(px, qty)| Ok((decimal_wei(px)?, decimal_wei(qty)?)))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let amounts = crate::common::market::PredictMarketOrderAmounts::from_levels(
                side,
                self.shares,
                &levels,
                self.policy.market_slippage_bps,
            )?;
            anyhow::ensure!(
                amounts.taker_amount >= decimal_wei(Decimal::ONE)?,
                "Predict MARKET SELL is below the venue minimum of 1 USDT at its slippage floor"
            );
            Some(amounts)
        } else {
            None
        };
        let now = self.clock.get_time_ns().as_u64();
        anyhow::ensure!(
            now < self.instrument.expiration_ns.as_u64()
                && self.expiry > U256::from(now / 1_000_000_000),
            "Predict order expired during admission"
        );
        let (maker_amount, taker_amount) = match amounts {
            Some(amounts) => (amounts.maker_amount, amounts.taker_amount),
            None => {
                let collateral = decimal_wei(self.cost)?;
                if side == PredictOrderSide::Sell {
                    (self.shares, collateral)
                } else {
                    (collateral, self.shares)
                }
            }
        };
        let signed = self.signer.sign_order(
            &PredictOrder {
                salt: self.salt,
                maker: self.session.account_address(),
                signer: self.session.account_address(),
                taker: Address::ZERO,
                token_id: token,
                maker_amount,
                taker_amount,
                expiration: self.expiry,
                nonce,
                fee_rate_bps: U256::from(market.fee_rate_bps),
                side,
                signature_type: PredictSignatureType::Eoa,
            },
            market.exchange(),
        )?;
        Ok((signed, amounts))
    }

    pub(super) async fn run(&self, guard: &mut SubmissionGuard) -> Result<(), CommandFailure> {
        if !self.available() {
            return Err(CommandFailure::not_sent(
                "Predict admission lost wallet continuity",
            ));
        }
        let market_id = super::parse::market_id_from_instrument(&self.instrument.id())
            .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
        // Independent metadata and venue reservations do not hold the resource admission lock.
        let (market, rows) = tokio::try_join!(
            async {
                self.session
                    .request_execution_market(market_id)
                    .await
                    .map_err(anyhow::Error::from)
            },
            super::account::collect_open_orders(&self.config),
        )
        .map_err(|e| CommandFailure::NotSent(e.to_string()))?;
        // Under the lock, union current local claims with the venue rows, collect fresh
        // chain resources, then sign and register atomically. Concurrent attempts cannot
        // consume the same snapshot; external wallet orders remain part of admission.
        let admission = self.admission.lock().await;
        if !self.available() {
            return Err(CommandFailure::not_sent(
                "Predict admission lost wallet continuity",
            ));
        }
        let (signed, amounts) = self
            .preflight(market, rows)
            .await
            .map_err(|error| CommandFailure::NotSent(error.to_string()))?;
        if !self.available() {
            return Err(CommandFailure::NotSent(
                "Predict wallet continuity changed before transmission".into(),
            ));
        }
        let id = VenueOrderId::new(format!("{:#x}", signed.hash()));
        if self
            .orders
            .load()
            .get(&id)
            .is_some_and(|existing| existing.client_order_id() != self.order.client_order_id())
        {
            return Err(CommandFailure::NotSent(
                "Predict signed order hash already belongs to another native order".into(),
            ));
        }
        {
            let mut registry = self.pending.lock().expect("submission registry poisoned");
            let attempted = registry
                .attempted_usdt
                .checked_add(if self.order.order_side() == OrderSide::Buy {
                    self.cost
                } else {
                    Decimal::ZERO
                })
                .filter(|v| *v <= self.policy.max_total_usdt)
                .ok_or_else(|| {
                    CommandFailure::NotSent("Predict collateral cap is exhausted".into())
                })?;
            *registry
                .entries
                .get_mut(&self.order.client_order_id())
                .ok_or_else(|| {
                    CommandFailure::NotSent("Predict submission lost command ownership".into())
                })? = Some(id);
            registry.attempted_usdt = attempted;
            // Install correlation before POST. Only immutable snapshots are stored here.
            self.orders.insert(id, Arc::clone(&self.order));
            guard.transmitted = true;
        }
        // The signed hash now reserves this attempt through existing immutable native
        // context; a following preflight includes it even before OPEN/ACK publication.
        drop(admission);
        // Public correlation metadata survives an ambiguous reply without exposing signed
        // payloads. This is transmission intent, never a venue acceptance acknowledgement.
        log::info!(
            "Predict transmitting order: client_order_id={} order_hash={:#x}",
            self.order.client_order_id(),
            signed.hash(),
        );
        let receipt = if let Some(amounts) = amounts {
            self.session
                .request_create_market_order(&signed, &amounts)
                .await
        } else {
            self.session
                .request_create_limit_order(&signed, self.price, self.order.is_post_only())
                .await
        }
        .map_err(|error| classify_http_command_failure(&error))?;
        super::routing::register(&self.rest_ids, id, receipt.order_id).map_err(|error| {
            self.reconciliation_required.store(true, Ordering::Release);
            CommandFailure::Ambiguous(error.to_string())
        })?;
        // The receipt's opaque code is never interpreted as native acceptance.
        Ok(())
    }
}
