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

//! Test strategy intent only. Native Cache and startup reconciliation own economic state.
use indexmap::IndexMap;
use nautilus_model::enums::{OrderStatus, OrderType, PositionSide};

use super::*;

const STATE_KEY: &str = "predict_exit_intent";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    version: u8,
    instrument: InstrumentId,
    entry: ClientOrderId,
    maker: Option<ClientOrderId>,
    taker: Option<ClientOrderId>,
    stop: bool,
    stop_attempts: u32,
}

impl ExitStrategy {
    /// Explicit cold-start claim for a single-entry exit test, not generic strategy recovery.
    /// Native reports reconstruct execution state before on_start; historical fills are not signals.
    pub fn with_exit_only_recovery(mut self) -> Self {
        self.cold_exit_only = true;
        self.native_entry = None;
        self.entry_sent = true;
        self.entry_modify_sent = true;
        self.entry_cancel_sent = true;
        self.cancel_orders_on_stop = false;
        self.core.config.use_uuid_client_order_ids = true;
        self.core.config.external_order_instrument_ids = Some(vec![self.instrument]);
        self
    }

    fn adopt_exit_only_state(&mut self) -> anyhow::Result<()> {
        let (orders, positions) = {
            let cache = self.cache();
            (
                cache.orders(
                    None,
                    Some(&self.instrument),
                    self.strategy_id().as_ref(),
                    None,
                    None,
                ),
                cache.positions_open(
                    None,
                    Some(&self.instrument),
                    self.strategy_id().as_ref(),
                    None,
                    None,
                ),
            )
        };
        anyhow::ensure!(
            orders
                .iter()
                .all(|o| o.account_id() == Some(self.account_id))
                && positions
                    .iter()
                    .all(|p| p.account_id == self.account_id && p.side == PositionSide::Long),
            "exit-only ownership mismatch"
        );
        if positions.is_empty() {
            anyhow::ensure!(
                orders.iter().all(|o| o.is_closed()),
                "flat account has an active exit"
            );
            self.journal.borrow_mut().push(
                json!({"type":"ExitOnlyAdopted","netPosition":"0","maker":null,"noEntry":true}),
            );
            return Ok(());
        }
        let buys: Vec<_> = orders
            .iter()
            .filter(|o| o.order_side() == OrderSide::Buy)
            .collect();
        anyhow::ensure!(
            buys.len() == 1 && buys[0].is_closed(),
            "exit-only test requires one terminal entry"
        );
        self.entry = buys[0].client_order_id();
        let mut gross = Decimal::ZERO;
        let mut cost = Decimal::ZERO;
        let mut shares_fee = Decimal::ZERO;
        let mut proceeds = Decimal::ZERO;
        for order in &orders {
            for event in order.events() {
                if let OrderEventAny::Filled(fill) = event {
                    let value = fill.last_qty.as_decimal() * fill.last_px.as_decimal();
                    let fee = fill.commission.map_or(Decimal::ZERO, |v| v.as_decimal());
                    if order.order_side() == OrderSide::Buy {
                        gross += fill.last_qty.as_decimal();
                        cost += value;
                        #[cfg(feature = "native-quantity-commission")]
                        {
                            shares_fee += fill.quantity_commission.unwrap_or(Decimal::ZERO);
                        }
                        #[cfg(not(feature = "native-quantity-commission"))]
                        {
                            shares_fee += fee / fill.last_px.as_decimal();
                        }
                    } else {
                        proceeds += value - fee;
                    }
                }
            }
        }
        anyhow::ensure!(
            gross > Decimal::ZERO && gross == buys[0].filled_qty().as_decimal(),
            "entry history incomplete"
        );
        let active: Vec<_> = orders.iter().filter(|o| !o.is_closed()).collect();
        anyhow::ensure!(
            active.len() <= 1,
            "ambiguous exit orders; no automatic role inference"
        );
        let net: Decimal = positions.iter().map(|p| p.quantity.as_decimal()).sum();
        if let Some(order) = active.first() {
            // REST does not expose post-only intent. Adopt the existing limit without inventing it.
            anyhow::ensure!(
                order.order_side() == OrderSide::Sell
                    && order.order_type() == OrderType::Limit
                    && matches!(
                        order.status(),
                        OrderStatus::Accepted | OrderStatus::PartiallyFilled
                    )
                    && order.leaves_qty().as_decimal() <= net,
                "unsupported or oversized existing exit"
            );
            self.maker = Some(order.client_order_id());
        }
        self.entry_qty = gross;
        self.entry_fee_shares = shares_fee;
        self.entry_cost = cost;
        self.realized_proceeds = proceeds;
        self.journal.borrow_mut().push(json!({"type":"ExitOnlyAdopted","entry":self.entry.to_string(),
            "maker":self.maker.map(|id|id.to_string()),"entryQuantity":gross.to_string(),"entrySharesFee":shares_fee.to_string(),
            "entryCost":cost.to_string(),"netPosition":net.to_string(),"realizedProceeds":proceeds.to_string(),"noEntry":true}));
        Ok(())
    }

    pub fn with_restart_recovery(mut self) -> Self {
        // Native UUID mode avoids the timestamp/counter ID restarting within the same second.
        self.core.config.use_uuid_client_order_ids = true;
        self.restart_recovery = true;
        self
    }

    pub(super) fn save_recovery_state(&self) -> anyhow::Result<IndexMap<String, Vec<u8>>> {
        if !self.restart_recovery {
            return Ok(IndexMap::new());
        }
        let state = Intent {
            version: 1,
            instrument: self.instrument,
            entry: self.entry,
            maker: self.maker,
            taker: self.taker,
            stop: self.stop,
            stop_attempts: self.stop_attempts,
        };
        self.journal.borrow_mut().push(json!({"type":"StrategyStateSaved","maker":self.maker.map(|id|id.to_string()),"stop":self.stop}));
        Ok(IndexMap::from([(
            STATE_KEY.into(),
            serde_json::to_vec(&state)?,
        )]))
    }

    pub(super) fn load_recovery_state(
        &mut self,
        state: IndexMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.restart_recovery,
            "exit strategy restart recovery is disabled"
        );
        let state: Intent = serde_json::from_slice(
            state
                .get(STATE_KEY)
                .ok_or_else(|| anyhow::anyhow!("missing exit strategy intent"))?,
        )?;
        anyhow::ensure!(
            state.version == 1,
            "unsupported exit strategy intent version"
        );
        anyhow::ensure!(
            state.instrument == self.instrument && state.entry == self.entry,
            "exit strategy intent identity differs from configuration"
        );
        anyhow::ensure!(
            state.stop_attempts <= self.max_stop_attempts,
            "exit strategy stop attempt limit differs from persisted intent"
        );
        self.maker = state.maker;
        self.taker = state.taker;
        self.stop = state.stop;
        self.stop_attempts = state.stop_attempts;
        self.restored = true;
        self.journal.borrow_mut().push(json!({"type":"StrategyStateLoaded","maker":self.maker.map(|id|id.to_string()),"stop":self.stop}));
        Ok(())
    }

    pub(super) fn adopt_reconciled_state(&mut self) -> anyhow::Result<()> {
        if self.cold_exit_only {
            return self.adopt_exit_only_state();
        }
        if !self.restart_recovery {
            return Ok(());
        }
        let (orders, positions) = {
            let cache = self.cache();
            (
                cache.orders(
                    None,
                    Some(&self.instrument),
                    self.strategy_id().as_ref(),
                    None,
                    None,
                ),
                cache.positions_open(
                    None,
                    Some(&self.instrument),
                    self.strategy_id().as_ref(),
                    None,
                    None,
                ),
            )
        };
        if orders.is_empty() && positions.is_empty() && !self.restored {
            return Ok(());
        }
        anyhow::ensure!(
            self.restored,
            "cached execution exists but strategy intent is missing"
        );
        let account = self.account_id;
        anyhow::ensure!(
            orders.iter().all(|o| o.account_id() == Some(account))
                && positions
                    .iter()
                    .all(|p| p.account_id == account && p.side == PositionSide::Long),
            "exit strategy account or position ownership differs"
        );
        let entry = orders
            .iter()
            .find(|o| o.client_order_id() == self.entry)
            .ok_or_else(|| anyhow::anyhow!("persisted entry is absent from native cache"))?;
        anyhow::ensure!(
            entry.is_closed()
                && entry.order_side() == OrderSide::Buy
                && entry.is_post_only()
                && entry.order_type() == OrderType::Limit,
            "restart requires a terminal maker BUY entry"
        );
        let mut entry_qty = Decimal::ZERO;
        let mut entry_cost = Decimal::ZERO;
        let mut proceeds = Decimal::ZERO;
        for order in &orders {
            for event in order.events() {
                if let OrderEventAny::Filled(fill) = event {
                    let value = fill.last_qty.as_decimal() * fill.last_px.as_decimal();
                    let fee = fill.commission.map_or(Decimal::ZERO, |v| v.as_decimal());
                    if order.client_order_id() == self.entry {
                        anyhow::ensure!(
                            fee.is_zero(),
                            "restart entry fee must have known maker semantics"
                        );
                        entry_qty += fill.last_qty.as_decimal();
                        entry_cost += value;
                    } else if order.order_side() == OrderSide::Sell {
                        proceeds += value - fee;
                    } else {
                        anyhow::bail!("unexpected additional entry in strategy ownership");
                    }
                }
            }
        }
        anyhow::ensure!(
            entry_qty == entry.filled_qty().as_decimal() && entry_qty > Decimal::ZERO,
            "persisted entry lacks complete native fills"
        );
        for (id, maker) in [(self.maker, true), (self.taker, false)] {
            if let Some(id) = id {
                let order = orders
                    .iter()
                    .find(|o| o.client_order_id() == id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("persisted exit role has no owned native order")
                    })?;
                anyhow::ensure!(
                    order.order_side() == OrderSide::Sell
                        && order.order_type()
                            == if maker {
                                OrderType::Limit
                            } else {
                                OrderType::Market
                            }
                        && order.is_post_only() == maker,
                    "persisted exit role differs from native order"
                );
                if order.is_closed() {
                    if maker {
                        self.maker = None;
                    } else {
                        self.taker = None;
                    }
                } else {
                    anyhow::ensure!(
                        matches!(
                            order.status(),
                            OrderStatus::Accepted | OrderStatus::PartiallyFilled
                        ),
                        "restart exit order remains unresolved"
                    );
                }
            }
        }
        let net: Decimal = positions.iter().map(|p| p.quantity.as_decimal()).sum();
        let mut reserved = Decimal::ZERO;
        for order in orders.iter().filter(|o| !o.is_closed()) {
            anyhow::ensure!(
                Some(order.client_order_id()) == self.maker
                    || Some(order.client_order_id()) == self.taker,
                "unclaimed active order in exit strategy ownership"
            );
            reserved += order.leaves_qty().as_decimal();
        }
        anyhow::ensure!(
            self.maker.is_none() || self.taker.is_none(),
            "overlapping active exit roles"
        );
        anyhow::ensure!(
            reserved <= net + Decimal::new(1, 12),
            "active exit exceeds native net position"
        );
        self.entry_qty = entry_qty;
        self.entry_cost = entry_cost;
        self.entry_fee_shares = Decimal::ZERO;
        self.realized_proceeds = proceeds;
        self.entry_sent = true;
        self.entry_cancel_sent = true;
        self.entry_modify_sent = true;
        self.cancel_sent = false;
        self.journal.borrow_mut().push(json!({"type":"RestartAdopted","maker":self.maker.map(|id|id.to_string()),"taker":self.taker.map(|id|id.to_string()),"entryQuantity":entry_qty.to_string(),"entryCost":entry_cost.to_string(),"netPosition":net.to_string(),"realizedProceeds":proceeds.to_string(),"reservedQuantity":reserved.to_string()}));
        Ok(())
    }
}
