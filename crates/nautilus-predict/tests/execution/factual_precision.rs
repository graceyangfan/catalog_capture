// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Synthetic loopback execution facts independent of quote increments and fee denomination.

use nautilus_common::msgbus::TypedHandler;
use nautilus_model::events::OrderEventAny;

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn atomic_zero_fee_and_collateral_fills_reach_subscribers_without_history_queries() {
    for (amount, fee_type, fee, price) in [
        (1u128, "SHARES", 0u128, 620_000_000_000_000_000u64),
        (1, "COLLATERAL", 0, 620_000_000_000_000_000),
        (
            17_000_000_000_000_000_099,
            "COLLATERAL",
            10_000_000_000_000_000,
            619_900_000_000_000_000,
        ),
    ] {
        let private = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&private);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(instrument(1).into())
            .unwrap();
        let own = order(1);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        cache
            .borrow_mut()
            .add_venue_order_id(
                &own.client_order_id(),
                &own.venue_order_id().unwrap(),
                false,
            )
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let expected = Quantity::from_u256(U256::from(amount), 18).unwrap();
        let expected_price =
            Price::from_decimal(Decimal::from_i128_with_scale(i128::from(price), 18)).unwrap();
        let observed = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let cache = cache.clone();
            let observed = observed.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if let OrderEventAny::Filled(fill) = event
                    && fill.client_order_id == id
                {
                    assert_eq!(fill.last_qty, expected);
                    assert_eq!(fill.last_px, expected_price);
                    let state = cache.borrow();
                    let current = state.order(&id).unwrap();
                    assert_eq!(current.filled_qty(), expected);
                    assert_eq!(current.status(), OrderStatus::PartiallyFilled);
                    assert_eq!(
                        state.positions(None, Some(&instrument_id(1)), None, None, None)[0]
                            .quantity,
                        expected
                    );
                    observed.borrow_mut().push(fill.trade_id);
                }
            }
        });
        let topic = format!("events.order.{}", own.strategy_id()).into();
        msgbus::subscribe_order_events(topic, handler.clone(), None);
        let mut wire = fill(1);
        wire["isMaker"] = json!(false);
        wire["fee"] = json!({"type":fee_type,"amountWei":fee.to_string()});
        wire["details"]["quantityFilled"] = json!(expected.as_decimal().to_string());
        wire["fill"] = json!({"executedSizeWei":amount.to_string(),
            "executedPriceWei":price.to_string(),
            "executedValueWei":(U256::from(amount)*U256::from(price)/U256::from(1_000_000_000_000_000_000u64)).to_string()});
        let mut engine = cancellations::engine(&cache);
        private.wallet(wire.clone());
        private.wallet(wire.clone());
        let (control_hash, control) = wallet_lifecycle::control(&mut engine, &wire);
        private.wallet(control);
        loop {
            match next(&mut rx).await {
                ExecutionEvent::Report(report @ ExecutionReport::Fill(_)) => {
                    engine.reconcile_execution_report(&report);
                }
                ExecutionEvent::Order(event @ OrderEventAny::Canceled(_))
                    if event.venue_order_id() == Some(control_hash) =>
                {
                    engine.process(&event);
                    break;
                }
                ExecutionEvent::Account(state) => msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    &state,
                ),
                other => panic!("unexpected precision feedback {other:?}"),
            }
        }
        assert_eq!(observed.borrow().len(), 1);
        assert_eq!(
            cache
                .borrow()
                .positions(None, Some(&instrument_id(1)), None, None, None)[0]
                .events
                .len(),
            1
        );
        assert!(private.state.order_requests.lock().unwrap().is_empty());
        assert!(private.state.match_requests.lock().unwrap().is_empty());
        msgbus::unsubscribe_order_events(topic, &handler);
        client.disconnect().await.unwrap();
    }
}
