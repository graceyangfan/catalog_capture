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

//! Read-only worker projection of Native state. No event application or independent FSM.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, Weak},
};

use alloy::primitives::Address;
use indexmap::IndexMap;
use nautilus_core::UnixNanos;
use nautilus_live::execution::context::OrderContext;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{OrderEventAny, OrderFilled},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};

/// Immutable native context; no worker may mutate Cache or adopt an arbitrary venue order.
pub(super) struct ReportContext<O = OrderAny> {
    pub account_id: AccountId,
    pub wallet: Address,
    pub shares_allowed: bool,
    pub instruments: HashMap<InstrumentId, InstrumentAny>,
    pub orders: HashMap<VenueOrderId, O>,
}

impl<O> ReportContext<O> {
    pub(super) fn single_order(
        account_id: AccountId,
        wallet: Address,
        shares_allowed: bool,
        venue_id: VenueOrderId,
        order: O,
        instrument: InstrumentAny,
    ) -> Self {
        Self {
            account_id,
            wallet,
            shares_allowed,
            instruments: HashMap::from([(instrument.id(), instrument)]),
            orders: HashMap::from([(venue_id, order)]),
        }
    }
}

// Async workers hold immutable Native-derived views, never a parallel OMS.
pub(super) type OrderSnapshots<K> = Arc<Mutex<IndexMap<K, Arc<WorkerOrder>, ahash::RandomState>>>;

#[cfg(test)]
thread_local! {
    pub(super) static TRADE_ID_COPIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static TRADE_IDS_VISITED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static TRADE_ID_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static HISTORY_DROP_PROBE: std::cell::RefCell<Option<Box<dyn Fn()>>> = const { std::cell::RefCell::new(None) };
    pub(super) static APPLIED_MEMBERSHIP_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static APPLIED_FILL_CLONES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static CAPTURED_FILLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

// The only retained history is Native-applied fill evidence, indexed by trade ID.
// A cutoff makes older snapshots immutable even when Native applies a later fill.
#[derive(Debug, Default)]
struct AppliedFills {
    entries: IndexMap<TradeId, OrderFilled, ahash::RandomState>,
}

#[cfg(test)]
impl Drop for AppliedFills {
    fn drop(&mut self) {
        HISTORY_DROP_PROBE.with(|probe| {
            if let Some(probe) = probe.borrow().as_ref() {
                probe();
            }
        });
    }
}

// Weak identity cannot pin a retired Native history or confuse a new capture at
// the same fill count. Cutoffs refer only to the append-only applied-fill index.
#[derive(Clone, Debug)]
pub(super) struct NativeFillCursor {
    history: Weak<Mutex<AppliedFills>>,
    cutoff: usize,
}

impl NativeFillCursor {
    pub fn is_current(&self, order: &WorkerOrder) -> bool {
        self.cutoff == order.cutoff && Weak::ptr_eq(&self.history, &Arc::downgrade(&order.fills))
    }
}

#[derive(Debug, Clone)]
pub(super) struct WorkerOrder {
    context: OrderContext,
    source_event_id: nautilus_core::UUID4,
    account_id: Option<AccountId>,
    venue_order_id: Option<VenueOrderId>,
    filled_qty: Quantity,
    leaves_qty: Quantity,
    quote_buy_budget: Option<rust_decimal::Decimal>,
    submitted_quantity: Quantity,
    expire_time: Option<UnixNanos>,
    ts_submitted: Option<UnixNanos>,
    ts_accepted: Option<UnixNanos>,
    ts_last: UnixNanos,
    status: OrderStatus,
    is_open: bool,
    is_inflight: bool,
    is_synthetic: bool,
    latest_fill: UnixNanos,
    fills: Arc<Mutex<AppliedFills>>,
    cutoff: usize,
    // Emitted, not necessarily applied yet. Shared by snapshots until Native purge.
    acceptance: Arc<OnceLock<UnixNanos>>,
}
fn event_id(event: &OrderEventAny) -> nautilus_core::UUID4 {
    match event {
        OrderEventAny::Initialized(event) => event.event_id,
        OrderEventAny::Denied(event) => event.event_id,
        OrderEventAny::Emulated(event) => event.event_id,
        OrderEventAny::Released(event) => event.event_id,
        OrderEventAny::Submitted(event) => event.event_id,
        OrderEventAny::Accepted(event) => event.event_id,
        OrderEventAny::Rejected(event) => event.event_id,
        OrderEventAny::Canceled(event) => event.event_id,
        OrderEventAny::Expired(event) => event.event_id,
        OrderEventAny::Triggered(event) => event.event_id,
        OrderEventAny::PendingUpdate(event) => event.event_id,
        OrderEventAny::PendingCancel(event) => event.event_id,
        OrderEventAny::ModifyRejected(event) => event.event_id,
        OrderEventAny::CancelRejected(event) => event.event_id,
        OrderEventAny::Updated(event) => event.event_id,
        OrderEventAny::Filled(event) => event.event_id,
        OrderEventAny::FillVoided(event) => event.event_id,
    }
}

fn original_quote_buy_budget(order: &OrderAny) -> Option<rust_decimal::Decimal> {
    let init = order.init_event();
    (init.quote_quantity
        && init.order_type == OrderType::Market
        && init.order_side == OrderSide::Buy)
        .then(|| init.quantity.as_decimal())
}

// A terminal reconciliation can reduce the working quantity to confirmed fills.
// Signed REST/wallet terms still refer to the quantity actually submitted.
fn submitted_base_quantity(order: &OrderAny) -> Quantity {
    let init = order.init_event();
    if !init.quote_quantity {
        return init.quantity;
    }
    let mut quantity = Order::quantity(order);
    for event in order.events() {
        match event {
            OrderEventAny::Submitted(_) => break,
            OrderEventAny::Updated(update) if !update.is_quote_quantity => {
                quantity = update.quantity;
            }
            _ => {}
        }
    }
    quantity
}

impl WorkerOrder {
    /// Initial/recovery capture may read history once; normal observations copy only scalars.
    pub fn capture(order: &OrderAny) -> Self {
        let mut result = Self::scalars(
            order,
            Arc::default(),
            UnixNanos::default(),
            Arc::default(),
            submitted_base_quantity(order),
        );
        for event in order.events() {
            if let OrderEventAny::Filled(fill) = event {
                #[cfg(test)]
                CAPTURED_FILLS.with(|n| n.set(n.get() + 1));
                result.record_fill(fill);
            }
        }
        result
    }
    /// Reuse the Native observer's applied history for an in-service snapshot.
    /// Restored or not-yet-observed orders retain the cold Native capture path.
    pub fn snapshot(order: &OrderAny, previous: Option<&Arc<Self>>) -> Arc<Self> {
        let previous = previous
            .filter(|view| view.source_event_id == event_id(order.last_event()))
            .cloned();
        match previous {
            Some(previous) => previous,
            None => Arc::new(Self::capture(order)),
        }
    }
    pub fn observe(order: &OrderAny, event: &OrderEventAny, previous: Option<&Self>) -> Self {
        let Some(previous) = previous else {
            return Self::capture(order);
        };
        let mut result = Self::scalars(
            order,
            Arc::clone(&previous.fills),
            previous.latest_fill,
            Arc::clone(&previous.acceptance),
            if previous.ts_submitted.is_some() {
                previous.submitted_quantity
            } else {
                submitted_base_quantity(order)
            },
        );
        if let OrderEventAny::Filled(fill) = event {
            result.record_fill(fill);
        }
        result
    }
    fn scalars(
        order: &OrderAny,
        fills: Arc<Mutex<AppliedFills>>,
        latest_fill: UnixNanos,
        acceptance: Arc<OnceLock<UnixNanos>>,
        submitted_quantity: Quantity,
    ) -> Self {
        let cutoff = fills
            .lock()
            .expect("native fill view poisoned")
            .entries
            .len();
        Self {
            context: OrderContext::from(order),
            source_event_id: event_id(order.last_event()),
            account_id: Order::account_id(order),
            venue_order_id: Order::venue_order_id(order),
            filled_qty: Order::filled_qty(order),
            leaves_qty: Order::leaves_qty(order),
            quote_buy_budget: original_quote_buy_budget(order),
            submitted_quantity,
            expire_time: Order::expire_time(order),
            ts_submitted: Order::ts_submitted(order),
            ts_accepted: Order::ts_accepted(order),
            ts_last: Order::ts_last(order),
            status: Order::status(order),
            is_open: Order::is_open(order),
            is_inflight: Order::is_inflight(order),
            is_synthetic: OrderView::is_synthetic(order),
            latest_fill,
            fills,
            cutoff,
            acceptance,
        }
    }
    fn record_fill(&mut self, fill: &OrderFilled) {
        let mut fills = self.fills.lock().expect("native fill view poisoned");
        fills
            .entries
            .entry(fill.trade_id)
            .or_insert_with(|| fill.clone());
        self.cutoff = fills.entries.len();
        self.latest_fill = self.latest_fill.max(fill.ts_event);
    }
    pub fn strategy_id(&self) -> StrategyId {
        self.context.identity.strategy_id
    }
    pub fn instrument_id(&self) -> InstrumentId {
        self.context.identity.instrument_id
    }
    pub fn client_order_id(&self) -> ClientOrderId {
        self.context.identity.client_order_id
    }
    pub fn account_id(&self) -> Option<AccountId> {
        self.account_id
    }
    pub fn venue_order_id(&self) -> Option<VenueOrderId> {
        self.venue_order_id
    }
    pub fn order_side(&self) -> OrderSide {
        self.context.identity.order_side
    }
    pub fn order_type(&self) -> OrderType {
        self.context.identity.order_type
    }
    pub fn quantity(&self) -> Quantity {
        self.context.quantity
    }
    pub fn filled_qty(&self) -> Quantity {
        self.filled_qty
    }
    pub fn leaves_qty(&self) -> Quantity {
        self.leaves_qty
    }
    pub fn price(&self) -> Option<Price> {
        self.context.price
    }
    pub fn time_in_force(&self) -> TimeInForce {
        self.context.time_in_force
    }
    pub fn expire_time(&self) -> Option<UnixNanos> {
        self.expire_time
    }
    pub fn ts_submitted(&self) -> Option<UnixNanos> {
        self.ts_submitted
    }
    pub fn ts_accepted(&self) -> Option<UnixNanos> {
        self.ts_accepted
    }
    pub fn ts_last(&self) -> UnixNanos {
        self.ts_last
    }
    pub fn status(&self) -> OrderStatus {
        self.status
    }
    pub fn is_post_only(&self) -> bool {
        self.context.is_post_only
    }
    pub fn is_reduce_only(&self) -> bool {
        self.context.is_reduce_only
    }
    pub fn is_quote_quantity(&self) -> bool {
        self.context.is_quote_quantity
    }
    pub fn is_open(&self) -> bool {
        self.is_open
    }
    pub fn is_closed(&self) -> bool {
        self.status.is_closed()
    }
    pub fn is_inflight(&self) -> bool {
        self.is_inflight
    }
    pub fn is_synthetic(&self) -> bool {
        self.is_synthetic
    }
    pub fn latest_fill(&self) -> UnixNanos {
        self.latest_fill
    }

    pub fn accepted(
        &self,
        factory: &nautilus_common::factories::OrderEventFactory,
        id: VenueOrderId,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> OrderEventAny {
        OrderEventAny::Accepted(nautilus_model::events::OrderAccepted::new(
            factory.trader_id(),
            self.strategy_id(),
            self.instrument_id(),
            self.client_order_id(),
            id,
            factory.account_id(),
            nautilus_core::UUID4::new(),
            ts_event,
            ts_init,
            false,
        ))
    }
    pub fn canceled(
        &self,
        factory: &nautilus_common::factories::OrderEventFactory,
        id: Option<VenueOrderId>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> OrderEventAny {
        OrderEventAny::Canceled(nautilus_model::events::OrderCanceled::new(
            factory.trader_id(),
            self.strategy_id(),
            self.instrument_id(),
            self.client_order_id(),
            nautilus_core::UUID4::new(),
            ts_event,
            ts_init,
            false,
            id,
            Some(factory.account_id()),
            None,
        ))
    }
    pub fn expired(
        &self,
        factory: &nautilus_common::factories::OrderEventFactory,
        id: Option<VenueOrderId>,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> OrderEventAny {
        OrderEventAny::Expired(nautilus_model::events::OrderExpired::new(
            factory.trader_id(),
            self.strategy_id(),
            self.instrument_id(),
            self.client_order_id(),
            nautilus_core::UUID4::new(),
            ts_event,
            ts_init,
            false,
            id,
            Some(factory.account_id()),
        ))
    }
    pub fn rejected(
        &self,
        factory: &nautilus_common::factories::OrderEventFactory,
        reason: &str,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
        due_post_only: bool,
    ) -> OrderEventAny {
        OrderEventAny::Rejected(nautilus_model::events::OrderRejected::new(
            factory.trader_id(),
            self.strategy_id(),
            self.instrument_id(),
            self.client_order_id(),
            factory.account_id(),
            reason.into(),
            nautilus_core::UUID4::new(),
            ts_event,
            ts_init,
            false,
            due_post_only,
        ))
    }
    pub fn cancel_rejected(
        &self,
        factory: &nautilus_common::factories::OrderEventFactory,
        id: Option<VenueOrderId>,
        reason: &str,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> OrderEventAny {
        OrderEventAny::CancelRejected(nautilus_model::events::OrderCancelRejected::new(
            factory.trader_id(),
            self.strategy_id(),
            self.instrument_id(),
            self.client_order_id(),
            reason.into(),
            nautilus_core::UUID4::new(),
            ts_event,
            ts_init,
            false,
            id,
            Some(factory.account_id()),
        ))
    }
    pub fn queued_acceptance(&self) -> Option<UnixNanos> {
        self.acceptance.get().copied()
    }
    pub fn record_acceptance(&self, timestamp: UnixNanos) {
        let _ = self.acceptance.set(timestamp);
    }
    pub fn for_each_trade_id(
        &self,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.for_each_trade_id_since(None, visit).map(|_| ())
    }
    fn for_each_trade_id_since(
        &self,
        cursor: Option<&NativeFillCursor>,
        mut visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<NativeFillCursor>> {
        let history = Arc::downgrade(&self.fills);
        if cursor.is_some_and(|cursor| {
            !Weak::ptr_eq(&cursor.history, &history) || cursor.cutoff > self.cutoff
        }) {
            return Ok(None);
        }
        #[cfg(test)]
        TRADE_ID_READS.with(|reads| reads.set(reads.get() + 1));
        let fills = self.fills.lock().expect("native fill view poisoned");
        for (id, _) in fills
            .entries
            .get_range(cursor.map_or(0, |c| c.cutoff)..self.cutoff)
            .expect("immutable Native cutoff is within applied history")
        {
            #[cfg(test)]
            TRADE_IDS_VISITED.with(|reads| reads.set(reads.get() + 1));
            visit(*id)?;
        }
        Ok(Some(NativeFillCursor {
            history,
            cutoff: self.cutoff,
        }))
    }
    pub fn trade_ids(&self) -> Vec<TradeId> {
        #[cfg(test)]
        TRADE_ID_COPIES.with(|copies| copies.set(copies.get() + 1));
        let mut ids = Vec::new();
        self.for_each_trade_id(|id| {
            ids.push(id);
            Ok(())
        })
        .expect("collecting Native IDs cannot fail");
        ids
    }
    /// Inspect immutable Native evidence without copying a full event. The
    /// inspector must not re-enter this history or publish callbacks under its lock.
    pub fn with_applied_fill<R>(
        &self,
        id: TradeId,
        inspect: impl FnOnce(Option<&OrderFilled>) -> R,
    ) -> R {
        let fills = self.fills.lock().expect("native fill view poisoned");
        if let Some((_, _, fill)) = fills
            .entries
            .get_full(&id)
            .filter(|(index, _, _)| *index < self.cutoff)
        {
            inspect(Some(fill))
        } else {
            // New wallet fills usually have no applied event. Do not hold the
            // Native history lock while normalizing that incoming allocation.
            drop(fills);
            inspect(None)
        }
    }
    #[cfg(test)]
    pub fn applied_fill(&self, id: TradeId) -> Option<OrderFilled> {
        self.with_applied_fill(id, |fill| {
            fill.map(|fill| {
                APPLIED_FILL_CLONES.with(|clones| clones.set(clones.get() + 1));
                fill.clone()
            })
        })
    }
    pub fn has_applied_fill(&self, id: TradeId) -> bool {
        #[cfg(test)]
        APPLIED_MEMBERSHIP_READS.with(|n| n.set(n.get() + 1));
        self.fills
            .lock()
            .expect("native fill view poisoned")
            .entries
            .get_index_of(&id)
            .is_some_and(|index| index < self.cutoff)
    }
    pub fn applied_fill_cursor(
        &self,
        ids: impl Iterator<Item = TradeId>,
    ) -> Option<NativeFillCursor> {
        #[cfg(test)]
        APPLIED_MEMBERSHIP_READS.with(|n| n.set(n.get() + 1));
        let fills = self.fills.lock().expect("native fill view poisoned");
        let mut ids = ids;
        ids.all(|id| {
            fills
                .entries
                .get_index_of(&id)
                .is_some_and(|index| index < self.cutoff)
        })
        .then(|| NativeFillCursor {
            history: Arc::downgrade(&self.fills),
            cutoff: self.cutoff,
        })
    }
    pub fn fill_count(&self) -> usize {
        self.cutoff
    }
}
/// Private shared parser interface; full REST captures and compact live views use identical rules.
pub(super) trait OrderView {
    fn instrument_id(&self) -> InstrumentId;
    fn client_order_id(&self) -> ClientOrderId;
    fn account_id(&self) -> Option<AccountId>;
    fn venue_order_id(&self) -> Option<VenueOrderId>;
    fn order_side(&self) -> OrderSide;
    fn order_type(&self) -> OrderType;
    fn quantity(&self) -> Quantity;
    fn submitted_quantity(&self) -> Quantity;
    fn filled_qty(&self) -> Quantity;
    fn leaves_qty(&self) -> Quantity;
    fn price(&self) -> Option<Price>;
    fn time_in_force(&self) -> TimeInForce;
    fn expire_time(&self) -> Option<UnixNanos>;
    fn ts_submitted(&self) -> Option<UnixNanos>;
    fn ts_accepted(&self) -> Option<UnixNanos>;
    fn ts_last(&self) -> UnixNanos;
    fn status(&self) -> OrderStatus;
    fn is_post_only(&self) -> bool;
    fn is_reduce_only(&self) -> bool;
    fn is_quote_quantity(&self) -> bool;
    fn quote_buy_budget(&self) -> Option<rust_decimal::Decimal>;
    fn is_open(&self) -> bool;
    fn is_closed(&self) -> bool;
    fn is_synthetic(&self) -> bool;
    fn latest_fill(&self) -> UnixNanos;
    fn trade_ids(&self) -> Vec<TradeId>;
    fn for_each_trade_id(
        &self,
        mut visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        for id in self.trade_ids() {
            visit(id)?;
        }
        Ok(())
    }
    /// A mismatched cursor returns None without visiting. Full Native views do
    /// not expose a stable cursor and therefore retain full prefix validation.
    fn for_each_trade_id_since(
        &self,
        cursor: Option<&NativeFillCursor>,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<NativeFillCursor>> {
        if cursor.is_none() {
            self.for_each_trade_id(visit)?;
        }
        Ok(None)
    }
    fn with_applied_fill<R>(
        &self,
        id: TradeId,
        inspect: impl FnOnce(Option<&OrderFilled>) -> R,
    ) -> R;
    fn has_applied_fill(&self, id: TradeId) -> bool;
}
impl OrderView for OrderAny {
    fn instrument_id(&self) -> InstrumentId {
        Order::instrument_id(self)
    }
    fn client_order_id(&self) -> ClientOrderId {
        Order::client_order_id(self)
    }
    fn account_id(&self) -> Option<AccountId> {
        Order::account_id(self)
    }
    fn venue_order_id(&self) -> Option<VenueOrderId> {
        Order::venue_order_id(self)
    }
    fn order_side(&self) -> OrderSide {
        Order::order_side(self)
    }
    fn order_type(&self) -> OrderType {
        Order::order_type(self)
    }
    fn quantity(&self) -> Quantity {
        Order::quantity(self)
    }
    fn submitted_quantity(&self) -> Quantity {
        submitted_base_quantity(self)
    }
    fn filled_qty(&self) -> Quantity {
        Order::filled_qty(self)
    }
    fn leaves_qty(&self) -> Quantity {
        Order::leaves_qty(self)
    }
    fn price(&self) -> Option<Price> {
        Order::price(self)
    }
    fn time_in_force(&self) -> TimeInForce {
        Order::time_in_force(self)
    }
    fn expire_time(&self) -> Option<UnixNanos> {
        Order::expire_time(self)
    }
    fn ts_submitted(&self) -> Option<UnixNanos> {
        Order::ts_submitted(self)
    }
    fn ts_accepted(&self) -> Option<UnixNanos> {
        Order::ts_accepted(self)
    }
    fn ts_last(&self) -> UnixNanos {
        Order::ts_last(self)
    }
    fn status(&self) -> OrderStatus {
        Order::status(self)
    }
    fn is_post_only(&self) -> bool {
        Order::is_post_only(self)
    }
    fn is_reduce_only(&self) -> bool {
        Order::is_reduce_only(self)
    }
    fn is_quote_quantity(&self) -> bool {
        Order::is_quote_quantity(self)
    }
    fn quote_buy_budget(&self) -> Option<rust_decimal::Decimal> {
        original_quote_buy_budget(self)
    }
    fn is_open(&self) -> bool {
        Order::is_open(self)
    }
    fn is_closed(&self) -> bool {
        Order::is_closed(self)
    }
    fn is_synthetic(&self) -> bool {
        self.tags().is_some_and(|tags| {
            tags.iter().any(|tag| {
                tag.as_str() == nautilus_execution::reconciliation::RECONCILIATION_ORDER_TAG
            })
        })
    }
    fn latest_fill(&self) -> UnixNanos {
        self.events()
            .into_iter()
            .filter_map(|event| {
                if let OrderEventAny::Filled(fill) = event {
                    Some(fill.ts_event)
                } else {
                    None
                }
            })
            .max()
            .unwrap_or_default()
    }
    fn trade_ids(&self) -> Vec<TradeId> {
        Order::trade_ids(self).into_iter().copied().collect()
    }
    fn with_applied_fill<R>(
        &self,
        id: TradeId,
        inspect: impl FnOnce(Option<&OrderFilled>) -> R,
    ) -> R {
        inspect(self.events().into_iter().find_map(|event| {
            if let OrderEventAny::Filled(fill) = event {
                (fill.trade_id == id).then_some(fill)
            } else {
                None
            }
        }))
    }
    fn has_applied_fill(&self, id: TradeId) -> bool {
        Order::trade_ids(self).into_iter().any(|known| *known == id)
    }
}
impl OrderView for WorkerOrder {
    fn instrument_id(&self) -> InstrumentId {
        Self::instrument_id(self)
    }
    fn client_order_id(&self) -> ClientOrderId {
        Self::client_order_id(self)
    }
    fn account_id(&self) -> Option<AccountId> {
        Self::account_id(self)
    }
    fn venue_order_id(&self) -> Option<VenueOrderId> {
        Self::venue_order_id(self)
    }
    fn order_side(&self) -> OrderSide {
        Self::order_side(self)
    }
    fn order_type(&self) -> OrderType {
        Self::order_type(self)
    }
    fn quantity(&self) -> Quantity {
        Self::quantity(self)
    }
    fn submitted_quantity(&self) -> Quantity {
        self.submitted_quantity
    }
    fn filled_qty(&self) -> Quantity {
        Self::filled_qty(self)
    }
    fn leaves_qty(&self) -> Quantity {
        Self::leaves_qty(self)
    }
    fn price(&self) -> Option<Price> {
        Self::price(self)
    }
    fn time_in_force(&self) -> TimeInForce {
        Self::time_in_force(self)
    }
    fn expire_time(&self) -> Option<UnixNanos> {
        Self::expire_time(self)
    }
    fn ts_submitted(&self) -> Option<UnixNanos> {
        Self::ts_submitted(self)
    }
    fn ts_accepted(&self) -> Option<UnixNanos> {
        Self::ts_accepted(self)
    }
    fn ts_last(&self) -> UnixNanos {
        Self::ts_last(self)
    }
    fn status(&self) -> OrderStatus {
        Self::status(self)
    }
    fn is_post_only(&self) -> bool {
        Self::is_post_only(self)
    }
    fn is_reduce_only(&self) -> bool {
        Self::is_reduce_only(self)
    }
    fn is_quote_quantity(&self) -> bool {
        Self::is_quote_quantity(self)
    }
    fn quote_buy_budget(&self) -> Option<rust_decimal::Decimal> {
        self.quote_buy_budget
    }
    fn is_open(&self) -> bool {
        Self::is_open(self)
    }
    fn is_closed(&self) -> bool {
        Self::is_closed(self)
    }
    fn is_synthetic(&self) -> bool {
        Self::is_synthetic(self)
    }
    fn latest_fill(&self) -> UnixNanos {
        Self::latest_fill(self)
    }
    fn trade_ids(&self) -> Vec<TradeId> {
        Self::trade_ids(self)
    }
    fn for_each_trade_id(
        &self,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        Self::for_each_trade_id(self, visit)
    }
    fn for_each_trade_id_since(
        &self,
        cursor: Option<&NativeFillCursor>,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<NativeFillCursor>> {
        Self::for_each_trade_id_since(self, cursor, visit)
    }
    fn with_applied_fill<R>(
        &self,
        id: TradeId,
        inspect: impl FnOnce(Option<&OrderFilled>) -> R,
    ) -> R {
        Self::with_applied_fill(self, id, inspect)
    }
    fn has_applied_fill(&self, id: TradeId) -> bool {
        Self::has_applied_fill(self, id)
    }
}
impl<T: OrderView> OrderView for Arc<T> {
    fn instrument_id(&self) -> InstrumentId {
        self.as_ref().instrument_id()
    }
    fn client_order_id(&self) -> ClientOrderId {
        self.as_ref().client_order_id()
    }
    fn account_id(&self) -> Option<AccountId> {
        self.as_ref().account_id()
    }
    fn venue_order_id(&self) -> Option<VenueOrderId> {
        self.as_ref().venue_order_id()
    }
    fn order_side(&self) -> OrderSide {
        self.as_ref().order_side()
    }
    fn order_type(&self) -> OrderType {
        self.as_ref().order_type()
    }
    fn quantity(&self) -> Quantity {
        self.as_ref().quantity()
    }
    fn submitted_quantity(&self) -> Quantity {
        self.as_ref().submitted_quantity()
    }
    fn filled_qty(&self) -> Quantity {
        self.as_ref().filled_qty()
    }
    fn leaves_qty(&self) -> Quantity {
        self.as_ref().leaves_qty()
    }
    fn price(&self) -> Option<Price> {
        self.as_ref().price()
    }
    fn time_in_force(&self) -> TimeInForce {
        self.as_ref().time_in_force()
    }
    fn expire_time(&self) -> Option<UnixNanos> {
        self.as_ref().expire_time()
    }
    fn ts_submitted(&self) -> Option<UnixNanos> {
        self.as_ref().ts_submitted()
    }
    fn ts_accepted(&self) -> Option<UnixNanos> {
        self.as_ref().ts_accepted()
    }
    fn ts_last(&self) -> UnixNanos {
        self.as_ref().ts_last()
    }
    fn status(&self) -> OrderStatus {
        self.as_ref().status()
    }
    fn is_post_only(&self) -> bool {
        self.as_ref().is_post_only()
    }
    fn is_reduce_only(&self) -> bool {
        self.as_ref().is_reduce_only()
    }
    fn is_quote_quantity(&self) -> bool {
        self.as_ref().is_quote_quantity()
    }
    fn quote_buy_budget(&self) -> Option<rust_decimal::Decimal> {
        self.as_ref().quote_buy_budget()
    }
    fn is_open(&self) -> bool {
        self.as_ref().is_open()
    }
    fn is_closed(&self) -> bool {
        self.as_ref().is_closed()
    }
    fn is_synthetic(&self) -> bool {
        self.as_ref().is_synthetic()
    }
    fn latest_fill(&self) -> UnixNanos {
        self.as_ref().latest_fill()
    }
    fn trade_ids(&self) -> Vec<TradeId> {
        self.as_ref().trade_ids()
    }
    fn for_each_trade_id(
        &self,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.as_ref().for_each_trade_id(visit)
    }
    fn for_each_trade_id_since(
        &self,
        cursor: Option<&NativeFillCursor>,
        visit: impl FnMut(TradeId) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<NativeFillCursor>> {
        self.as_ref().for_each_trade_id_since(cursor, visit)
    }
    fn with_applied_fill<R>(
        &self,
        id: TradeId,
        inspect: impl FnOnce(Option<&OrderFilled>) -> R,
    ) -> R {
        self.as_ref().with_applied_fill(id, inspect)
    }
    fn has_applied_fill(&self, id: TradeId) -> bool {
        self.as_ref().has_applied_fill(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unapplied_inspection_releases_native_history_before_normalization() {
        let native = nautilus_model::orders::builder::OrderTestBuilder::new(OrderType::Limit)
            .instrument_id("42-1.PREDICT".into())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.130"))
            .build();
        let order = WorkerOrder::capture(&native);
        order.with_applied_fill("NEW-TRADE".into(), |fill| {
            assert!(fill.is_none());
            assert!(
                order.fills.try_lock().is_ok(),
                "new-fill normalization must not hold the Native history lock"
            );
        });
    }
}
