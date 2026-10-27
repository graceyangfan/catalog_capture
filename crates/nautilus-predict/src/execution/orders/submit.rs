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

//! Native terms, prepared signing inputs and one-shot order transmission.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy::primitives::{Address, U256};
use nautilus_core::time::AtomicTime;
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure};
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::VenueOrderId,
    instruments::{BinaryOption, Instrument},
    orders::{Order, OrderAny},
    types::Currency,
};
use nautilus_network::websocket::AuthTracker;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::execution::{account, context, metadata, observations, orders, parse};
use crate::{
    common::enums::{PredictOrderSide, PredictSignatureType},
    config::PredictExecutionClientConfig,
    execution::orders::{PendingSubmissions, SubmissionGuard, classify_http_command_failure},
    http::{command_models::PredictLimitOrderOptions, order_models::PredictOrder},
    signing::PredictOrderSigner,
};

#[derive(Clone, Copy, Debug)]
pub(in crate::execution) struct OrderTerms {
    pub cost: Decimal,
    pub price: U256,
    pub shares: U256,
    pub expiry: U256,
    pub market: Option<crate::common::market::PredictMarketOrderAmounts>,
}

pub(in crate::execution) fn decimal_wei(value: Decimal) -> anyhow::Result<U256> {
    let value = value.normalize();
    anyhow::ensure!(
        value > Decimal::ZERO && value.scale() <= 18,
        "Predict amount is nonpositive or exceeds wei precision"
    );
    crate::common::parse::decimal_to_wei(value)
}

/// Native command params carry venue-specific inputs without changing MarketOrder.
/// A decimal string keeps the strategy price exact; it is a reference before slippage.
pub(in crate::execution) fn market_price(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<Option<U256>> {
    anyhow::ensure!(
        params
            .and_then(|params| params.get("isFillOrKill"))
            .is_none(),
        "Predict FOK must use native TimeInForce::Fok, not params[isFillOrKill]"
    );
    let Some(value) = params.and_then(|params| params.get("market_price")) else {
        anyhow::ensure!(
            order.order_type() != OrderType::Market,
            "Predict MARKET requires strategy market_price"
        );
        return Ok(None);
    };
    anyhow::ensure!(
        order.order_type() == OrderType::Market,
        "Predict market_price requires MARKET"
    );
    let price = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Predict market_price must be a decimal string"))?;
    let price = Decimal::from_str_exact(price)?;
    Ok(Some(decimal_wei(price)?))
}

/// Match the Native Hyperliquid parameter name while rejecting lossy or ignored input.
/// Omission uses the official SDK's zero extra slippage; no client policy is applied.
pub(in crate::execution) fn market_order_slippage_bps(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<u32> {
    // Minimum-out changes signed BUY economics and the venue's target sizing.
    // Native currently supports the default model only; never ignore a caller's mode.
    for key in ["isMinAmountOut", "is_min_amount_out"] {
        anyhow::ensure!(
            params.and_then(|p| p.get(key)).is_none(),
            "Predict Native minimum-out MARKET model is unsupported; params[{key}] cannot select a signed amount model"
        );
    }
    anyhow::ensure!(
        params.and_then(|p| p.get("slippageBps")).is_none(),
        "Predict Native slippage uses params[market_order_slippage_bps], not HTTP slippageBps"
    );
    anyhow::ensure!(
        params.and_then(|p| p.get("market_slippage_bps")).is_none(),
        "Predict per-order slippage uses params[market_order_slippage_bps]"
    );
    let Some(value) = params.and_then(|p| p.get("market_order_slippage_bps")) else {
        return Ok(0);
    };
    anyhow::ensure!(
        order.order_type() == OrderType::Market,
        "Predict market_order_slippage_bps requires MARKET"
    );
    let bps = value.as_u64().filter(|bps| *bps < 10_000).ok_or_else(|| {
        anyhow::anyhow!("Predict market_order_slippage_bps must be an integer from 0 to 9999")
    })?;
    Ok(u32::try_from(bps)?)
}

/// STP is venue-specific; post-only remains on the Native order itself.
pub(in crate::execution) fn limit_options(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<crate::http::command_models::PredictLimitOrderOptions> {
    anyhow::ensure!(
        params.and_then(|params| params.get("isPostOnly")).is_none(),
        "Predict post-only must use the Native LIMIT is_post_only field, not params[isPostOnly]"
    );
    anyhow::ensure!(
        params
            .and_then(|params| params.get("selfTradePrevention"))
            .is_none(),
        "Predict Native STP uses params[self_trade_prevention], not the HTTP selfTradePrevention key"
    );
    let stp = params
        .and_then(|params| params.get("self_trade_prevention"))
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(
                "Predict self_trade_prevention must be CANCEL_MAKER/CANCEL_TAKER/CANCEL_BOTH"
            )
        })?;
    anyhow::ensure!(
        stp.is_none() || order.order_type() == OrderType::Limit,
        "Predict self_trade_prevention currently requires LIMIT"
    );
    Ok(crate::http::command_models::PredictLimitOrderOptions {
        is_post_only: order.is_post_only(),
        self_trade_prevention: stp,
    })
}

fn significant_digits(value: Decimal) -> usize {
    value
        .normalize()
        .mantissa()
        .unsigned_abs()
        .to_string()
        .trim_end_matches('0')
        .len()
}

pub(in crate::execution) fn terms(
    order: &OrderAny,
    instrument: &BinaryOption,
    market_order_slippage_bps: u32,
    now: u64,
    market_price: Option<U256>,
) -> anyhow::Result<OrderTerms> {
    // Native command admission owns the order-state check. This synchronous
    // conversion validates only venue-supported terms and exact signed amounts.
    anyhow::ensure!(
        matches!(order.order_type(), OrderType::Limit | OrderType::Market),
        "Predict order type is unsupported; use LIMIT or MARKET"
    );
    anyhow::ensure!(
        matches!(order.order_side(), OrderSide::Buy | OrderSide::Sell),
        "Predict order side is unsupported"
    );
    anyhow::ensure!(
        match order.order_type() {
            OrderType::Limit =>
                matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Gtd),
            OrderType::Market =>
                matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Fok),
            _ => false,
        },
        "Predict LIMIT requires GTC/GTD; MARKET requires GTC/FOK"
    );
    anyhow::ensure!(
        !order.is_reduce_only(),
        "Predict venue reduce-only is unsupported"
    );
    let quote_buy = order.is_quote_quantity();
    let original_budget = context::OrderView::quote_buy_budget(order);
    anyhow::ensure!(
        !quote_buy
            || (order.order_type() == OrderType::Market && order.order_side() == OrderSide::Buy),
        "Predict quote_quantity requires MARKET BUY"
    );
    anyhow::ensure!(
        (original_budget.is_none() && !quote_buy) || market_order_slippage_bps == 0,
        "Predict quote_quantity MARKET BUY requires market_order_slippage_bps=0 to preserve the quote budget"
    );
    anyhow::ensure!(
        !quote_buy || original_budget.is_some(),
        "Predict quote sizing requires original Native MARKET BUY quote intent"
    );
    let px = if order.order_type() == OrderType::Market {
        Decimal::ONE
    } else {
        order
            .price()
            .ok_or_else(|| anyhow::anyhow!("Predict limit price is missing"))?
            .as_decimal()
    };
    let qty = order.quantity().as_decimal();
    // MARKET's shared SDK amount builder checks its quantity/price bounds once.
    // LIMIT keeps exact native terms instead of silently applying SDK truncation.
    if order.order_type() == OrderType::Limit {
        anyhow::ensure!(
            px > Decimal::ZERO && px <= Decimal::ONE,
            "Predict limit price must be positive and at most one"
        );
        anyhow::ensure!(
            qty >= Decimal::new(1, 2),
            "Predict SDK quantity minimum is 0.01 shares"
        );
        anyhow::ensure!(
            significant_digits(px) <= 3 && significant_digits(qty) <= 5,
            "Predict SDK amount preparation would change original native terms"
        );
    }
    anyhow::ensure!(
        px % instrument.price_increment().as_decimal() == Decimal::ZERO
            && (quote_buy || qty % instrument.size_increment().as_decimal() == Decimal::ZERO),
        "Predict price or quantity does not match the native instrument increment"
    );
    let input = decimal_wei(qty)?;
    let market = if quote_buy {
        anyhow::ensure!(
            qty.round_dp(u32::from(Currency::USDT().precision)) == qty,
            "Predict quote amount requires exact native USDT precision"
        );
        Some(
            crate::common::market::PredictMarketOrderAmounts::from_value_price(
                input,
                market_price.ok_or_else(|| {
                    anyhow::anyhow!("Predict MARKET requires strategy market_price")
                })?,
            )?,
        )
    } else if order.order_type() == OrderType::Market {
        Some(
            crate::common::market::PredictMarketOrderAmounts::from_price(
                if order.order_side() == OrderSide::Buy {
                    crate::common::enums::PredictOrderSide::Buy
                } else {
                    crate::common::enums::PredictOrderSide::Sell
                },
                input,
                market_price.ok_or_else(|| {
                    anyhow::anyhow!("Predict MARKET requires strategy market_price")
                })?,
                market_order_slippage_bps,
            )?,
        )
    } else {
        None
    };
    let shares = market.map_or(input, |amounts| amounts.amount);
    if quote_buy {
        let derived = crate::common::parse::exact_wei(shares)?;
        parse::quantity(derived, instrument.size_precision())?;
        anyhow::ensure!(
            derived % instrument.size_increment().as_decimal() == Decimal::ZERO,
            "Predict derived shares do not match native instrument increment"
        );
    }
    let cost = if let Some(amounts) = market {
        crate::common::parse::exact_wei(if order.order_side() == OrderSide::Buy {
            amounts.maker_amount
        } else {
            amounts.taker_amount
        })?
    } else {
        px.checked_mul(qty)
            .ok_or_else(|| anyhow::anyhow!("Predict notional overflow"))?
    };
    anyhow::ensure!(
        original_budget.is_none_or(|budget| cost <= budget),
        "Predict signed collateral exceeds original Native quote budget"
    );
    anyhow::ensure!(
        instrument.quote_currency() == Currency::USDT()
            && (order.order_type() == OrderType::Market
                || cost.round_dp(u32::from(Currency::USDT().precision)) == cost),
        "Predict collateral requires exact native USDT representation"
    );
    let expiry = match order.time_in_force() {
        TimeInForce::Gtc | TimeInForce::Fok if order.order_type() == OrderType::Market => {
            now / 1_000_000_000 + 300
        }
        TimeInForce::Gtc => 4_102_444_800,
        TimeInForce::Gtd => {
            let expiry = order
                .expire_time()
                .ok_or_else(|| anyhow::anyhow!("Predict GTD expiry is missing"))?
                .as_u64();
            anyhow::ensure!(
                expiry % 1_000_000_000 == 0,
                "Predict expiry requires whole seconds"
            );
            expiry / 1_000_000_000
        }
        _ => unreachable!(),
    };
    anyhow::ensure!(
        expiry > now / 1_000_000_000 && instrument.expiration_ns.as_u64() > now,
        "Predict order or instrument has expired"
    );
    anyhow::ensure!(
        order.time_in_force() != TimeInForce::Gtd || expiry > now / 1_000_000_000 + 120,
        "Predict GTD expiry must be more than 2 minutes in the future"
    );
    anyhow::ensure!(
        cost >= Decimal::ONE,
        "Predict order is below the venue minimum of 1 USDT"
    );
    Ok(OrderTerms {
        cost,
        price: decimal_wei(px)?,
        shares,
        expiry: U256::from(expiry),
        market,
    })
}

pub(in crate::execution) struct SubmitWorker {
    pub signer: Arc<PredictOrderSigner>,
    pub command_started: Option<Instant>,
    pub config: PredictExecutionClientConfig,
    pub order: Arc<OrderAny>,
    pub definition: metadata::ExecutionInstrument,
    pub terms: OrderTerms,
    pub limit_options: PredictLimitOrderOptions,
    pub data_sender:
        Option<nautilus_common::live::sender::EventSender<nautilus_common::messages::DataEvent>>,
    pub salt: U256,
    pub pending: PendingSubmissions,
    pub orders: context::OrderSnapshots<VenueOrderId>,
    pub rest_ids: orders::RestOrderIds,
    pub account_refresh_required: Arc<AtomicBool>,
    pub account_sync: Arc<account::AccountSync>,
    pub metadata: Arc<metadata::ExecutionMetadata>,
    pub healthy: Arc<AtomicBool>,
    pub authentication: AuthTracker,
    pub epoch: Arc<AtomicU64>,
    pub admitted_epoch: u64,
    pub emitter: ExecutionEventEmitter,
    pub clock: &'static AtomicTime,
}

impl SubmitWorker {
    pub(in crate::execution) fn available(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
            && self.authentication.is_authenticated()
            && self.epoch.load(Ordering::Acquire) == self.admitted_epoch
    }

    fn current_definition(&self) -> bool {
        self.definition.is_current(&self.metadata.instruments)
    }

    fn execution_market(&self) -> anyhow::Result<metadata::MarketMetadata> {
        if let Some(value) = self.definition.cached_market(
            self.clock.get_time_ns(),
            self.config
                .market_metadata_ttl_secs
                .saturating_mul(1_000_000_000),
        )? {
            if let Some(probe) = &self.config.latency {
                probe.count(crate::latency::LatencyCounter::MetadataHit);
            }
            return Ok(value);
        }
        if let Some(probe) = &self.config.latency {
            probe.count(crate::latency::LatencyCounter::MetadataMiss);
        }
        self.metadata.notify.notify_one();
        anyhow::bail!("Predict market metadata is not ready; wait for background preparation")
    }

    fn prepare_order(
        &self,
        market: &crate::http::market_models::PredictExecutionMarket,
        nonce: U256,
    ) -> anyhow::Result<(
        crate::http::order_models::PredictSignedOrder,
        Option<crate::common::market::PredictMarketOrderAmounts>,
    )> {
        let token: U256 = self
            .definition
            .instrument
            .raw_symbol()
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("Predict token ID is invalid"))?;
        let side = if self.order.order_side() == OrderSide::Sell {
            PredictOrderSide::Sell
        } else {
            PredictOrderSide::Buy
        };
        let amounts = self.terms.market;
        let now = self.clock.get_time_ns().as_u64();
        anyhow::ensure!(
            self.definition
                .instrument
                .expiration_ns()
                .is_some_and(|expiry| now < expiry.as_u64())
                && self.terms.expiry > U256::from(now / 1_000_000_000),
            "Predict order expired during admission"
        );
        let (maker_amount, taker_amount) = match amounts {
            Some(amounts) => (amounts.maker_amount, amounts.taker_amount),
            None => {
                let collateral = decimal_wei(self.terms.cost)?;
                if side == PredictOrderSide::Sell {
                    (self.terms.shares, collateral)
                } else {
                    (collateral, self.terms.shares)
                }
            }
        };
        let signed = self.signer.sign_order(
            &PredictOrder {
                salt: self.salt,
                maker: self.config.session.account_address(),
                signer: self.config.session.account_address(),
                taker: Address::ZERO,
                token_id: token,
                maker_amount,
                taker_amount,
                expiration: self.terms.expiry,
                nonce,
                fee_rate_bps: U256::from(market.fee_rate_bps),
                side,
                signature_type: PredictSignatureType::Eoa,
            },
            market.exchange(),
        )?;
        Ok((signed, amounts))
    }

    /// Own one admitted command through cancellation, deadline and venue outcome.
    /// The TaskGroup owns spawning; no retry or independent lifecycle is added here.
    pub(in crate::execution) async fn run(
        self,
        mut guard: SubmissionGuard,
        cancellation: CancellationToken,
    ) {
        let deadline = Duration::from_secs(self.config.request_timeout_secs);
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            result = tokio::time::timeout(deadline, self.submit(&mut guard)) => result,
        };
        // A definitive HTTP result belongs to this command even if the wallet
        // reauthenticates while it is in flight. Only its task lifecycle ends delivery.
        if cancellation.is_cancelled() {
            return;
        }
        match result {
            Ok(Ok(())) => {}
            Ok(Err(CommandFailure::NotSent(reason) | CommandFailure::VenueRejected(reason))) => {
                // Applied or already queued Native acceptance wins over a late HTTP error.
                let known_outcome = guard.venue_id.is_some_and(|id| {
                    self.orders
                        .lock()
                        .expect("order snapshots poisoned")
                        .get(&id)
                        .is_some_and(|o| {
                            o.client_order_id() == self.order.client_order_id()
                                && (o.is_closed()
                                    || o.ts_accepted().is_some()
                                    || o.queued_acceptance().is_some())
                        })
                });
                if !known_outcome {
                    self.emitter.emit_order_rejected(
                        &self.order,
                        &reason,
                        self.clock.get_time_ns(),
                        false,
                    );
                }
            }
            Ok(Err(CommandFailure::Ambiguous(reason))) if guard.transmitted => {
                // `reason` comes only from sanitized typed private HTTP errors. Preserve
                // status/decode classification without logging response bodies or signatures.
                log::warn!(
                    "Predict submission outcome is unresolved; client_order_id={} reason={reason}; do not resubmit",
                    self.order.client_order_id(),
                );
            }
            Err(_) if guard.transmitted => {
                log::warn!(
                    "Predict submission outcome is unresolved; client_order_id={} reason=command deadline elapsed after transmission; do not resubmit",
                    self.order.client_order_id(),
                );
            }
            _ => {
                self.emitter.emit_order_rejected(
                    &self.order,
                    "Predict preflight did not complete; order was not transmitted",
                    self.clock.get_time_ns(),
                    false,
                );
            }
        }
    }

    async fn submit(&self, guard: &mut SubmissionGuard) -> Result<(), CommandFailure> {
        let preparation = self
            .config
            .latency
            .as_ref()
            .map(|probe| probe.timer(crate::latency::LatencyMetric::Preparation));
        if !self.available() || !self.current_definition() {
            return Err(CommandFailure::not_sent(
                "Predict admission lost wallet or instrument continuity",
            ));
        }
        let metadata = self
            .execution_market()
            .map_err(|error| CommandFailure::NotSent(error.to_string()))?;
        let nonce = self
            .metadata
            .nonce(metadata.market.exchange())
            .ok_or_else(|| {
                CommandFailure::not_sent("Predict account signing nonce is not prepared")
            })?;
        let (signed, amounts) = self
            .prepare_order(&metadata.market, nonce)
            .map_err(|error| CommandFailure::NotSent(error.to_string()))?;
        let id = VenueOrderId::new(format!("{:#x}", signed.hash()));
        // These values derive only from immutable command inputs. Prepare them
        // before serializing shared correlation and reservation registration.
        let snapshot = Arc::new(context::WorkerOrder::capture(&self.order));
        if let Some(preparation) = preparation {
            preparation.finish();
        }
        let registration_started = self.config.latency.as_ref().map(|_| Instant::now());
        let lock_acquired = {
            // Fence the prepared nonce, generation and definition with registration.
            // This short correlation lock never covers signing or network IO.
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            let lock_acquired = registration_started.map(|_| Instant::now());
            let nonces = self
                .metadata
                .nonces
                .lock()
                .expect("signing nonces poisoned");
            if nonces[metadata.market.exchange().cache_index()] != Some(nonce)
                || !self.available()
                || !self.current_definition()
                || !self
                    .clock
                    .get_time_ns()
                    .as_u64()
                    .checked_sub(metadata.observed.as_u64())
                    .is_some_and(|age| age < self.config.market_metadata_ttl_secs * 1_000_000_000)
            {
                return Err(CommandFailure::NotSent(
                    "Predict wallet, instrument or market metadata changed before transmission"
                        .into(),
                ));
            }
            if self
                .orders
                .lock()
                .expect("order snapshots poisoned")
                .get(&id)
                .is_some_and(|existing| existing.client_order_id() != self.order.client_order_id())
            {
                return Err(CommandFailure::NotSent(
                    "Predict signed order hash already belongs to another native order".into(),
                ));
            }
            {
                let mut registry = self.pending.lock().expect("submission registry poisoned");
                registry
                    .entries
                    .get_mut(&self.order.client_order_id())
                    .ok_or_else(|| {
                        CommandFailure::NotSent("Predict submission lost command ownership".into())
                    })?
                    .venue_id = Some(id);
                // Install correlation before POST. Only immutable snapshots are stored here.
                self.orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .insert(id, snapshot);
                sync.active_orders
                    .insert(self.order.client_order_id(), Some(id));
                guard.venue_id = Some(id);
                guard.transmitted = true;
            }
            lock_acquired
        };
        if let (Some(probe), Some(started), Some(acquired)) =
            (&self.config.latency, registration_started, lock_acquired)
        {
            // Publish after releasing every registration guard. Telemetry is not
            // part of the measured critical section and never waits for its collector.
            let registered = Instant::now();
            probe.elapsed(
                crate::latency::LatencyMetric::SubmissionAccountLockWait,
                started,
                acquired,
            );
            probe.elapsed(
                crate::latency::LatencyMetric::SubmissionRegistration,
                acquired,
                registered,
            );
        }
        // Correlation is installed before transmission; no funds ledger or admission
        // mutex is needed. Native risk and the venue enforce trading capacity.
        // Public correlation metadata survives an ambiguous reply without exposing signed
        // payloads. This is transmission intent, never a venue acceptance acknowledgement.
        log::info!(
            "Predict transmitting order: client_order_id={} order_hash={:#x}",
            self.order.client_order_id(),
            signed.hash(),
        );
        log::debug!(
            "Predict prepared order: client_order_id={} order_hash={:#x} instrument_id={} native_order_type={:?} native_tif={:?} initial_quote_quantity={} initial_quantity={} prepared_quantity={} wire_strategy={} is_post_only={:?} is_fill_or_kill={:?} slippage_bps={:?} price_per_share_wei={} maker_amount_wei={} taker_amount_wei={}",
            self.order.client_order_id(),
            signed.hash(),
            self.order.instrument_id(),
            self.order.order_type(),
            self.order.time_in_force(),
            self.order.init_event().quote_quantity,
            self.order.init_event().quantity,
            self.order.quantity(),
            if amounts.is_some() { "MARKET" } else { "LIMIT" },
            amounts.is_none().then_some(self.limit_options.is_post_only),
            (amounts.is_some() && self.order.time_in_force() == TimeInForce::Fok).then_some(true),
            amounts.map(|amounts| amounts.slippage_bps),
            amounts.map_or(self.terms.price, |amounts| amounts.price_per_share),
            signed.order().maker_amount,
            signed.order().taker_amount,
        );
        let timing = self.config.latency.as_ref().map(|probe| {
            if let Some(started) = self.command_started {
                probe.post(self.order.client_order_id(), started);
            }
            probe.count(crate::latency::LatencyCounter::CreateCalls);
            probe.timer(crate::latency::LatencyMetric::CreateRequest)
        });
        let receipt = if let Some(amounts) = amounts {
            if self.order.time_in_force() == TimeInForce::Fok {
                self.config
                    .session
                    .request_create_market_fok_order(&signed, &amounts)
                    .await
            } else {
                self.config
                    .session
                    .request_create_market_order(&signed, &amounts)
                    .await
            }
        } else {
            self.config
                .session
                .request_create_limit_order_with_options(
                    &signed,
                    self.terms.price,
                    self.limit_options,
                )
                .await
        };
        if let Some(timing) = timing {
            timing.finish();
        }
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                // Definitive venue rejection is published without account IO or retry.
                // A nonce failure invalidates its signing context and wakes the
                // background collector. Ordinary funding failures do not evict metadata.
                if matches!(
                    &error,
                    crate::http::error::Error::Api {
                        reason: crate::http::error::PredictApiErrorReason::InvalidNonce,
                        ..
                    }
                ) {
                    self.metadata
                        .invalidate_nonce(metadata.market.exchange(), nonce);
                }
                return Err(classify_http_command_failure(&error));
            }
        };
        orders::register(&self.rest_ids, id, receipt.order_id).map_err(|error| {
            self.account_refresh_required.store(true, Ordering::Release);
            CommandFailure::Ambiguous(error.to_string())
        })?;
        if self.terms.market.is_none() {
            let removal_locked_until = receipt.removal_lock_deadline().map_err(|_| {
                CommandFailure::ambiguous(
                    "Predict create receipt has invalid removal lock timestamp",
                )
            })?;
            // Publish receipt metadata through the Native data channel.
            // An old transport generation cannot publish into a renewed session.
            if self.available()
                && let Some(sender) = &self.data_sender
            {
                let now = self.clock.get_time_ns();
                let update = observations::PredictLimitOrderReceiptUpdate {
                    account_id: self.emitter.account_id(),
                    strategy_id: self.order.strategy_id(),
                    instrument_id: self.order.instrument_id(),
                    client_order_id: self.order.client_order_id(),
                    venue_order_id: id,
                    is_post_only: self.limit_options.is_post_only,
                    self_trade_prevention: self.limit_options.self_trade_prevention,
                    removal_locked_until,
                    connection_epoch: self.admitted_epoch,
                    ts_event: now,
                    ts_init: now,
                };
                if sender
                    .send(nautilus_common::messages::DataEvent::Data(
                        nautilus_model::data::Data::Custom(update.into_custom_data()),
                    ))
                    .is_err()
                {
                    // Optional metadata delivery does not change the remote order outcome.
                    log::warn!("Predict LIMIT receipt metadata channel closed");
                }
            }
        }
        // The receipt's opaque code is never interpreted as native acceptance.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wei_preserves_eighteen_decimals_and_normalizes_product_trailing_zeros() {
        let one_wei = Decimal::from_str_exact("0.000000000000000001").unwrap();
        assert_eq!(decimal_wei(one_wei).unwrap(), U256::from(1));
        let shares = Decimal::from_str_exact("196.0000000000000000").unwrap();
        let price = Decimal::from_str_exact("0.013").unwrap();
        assert_eq!(
            decimal_wei(shares * price).unwrap(),
            U256::from(2_548_000_000_000_000_000u128)
        );
        assert!(decimal_wei(Decimal::from_str_exact("0.0000000000000000001").unwrap()).is_err());
        assert!(decimal_wei(Decimal::ZERO).is_err());
        assert!(decimal_wei(-one_wei).is_err());
    }
}
