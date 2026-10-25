// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Opt-in Native inventory-fee prototype: real report ingestion, not external adjustments.
use nautilus_common::msgbus::TypedHandler;
use nautilus_model::{
    enums::LiquiditySide,
    events::{OrderEventAny, PositionEvent},
    instruments::Instrument,
    position::Position,
    reports::FillReport,
};

use super::*;

fn charged_fill(
    sequence: u64,
    quantity: &str,
    price: &str,
    fee: &str,
) -> nautilus_model::events::OrderFilled {
    let mut event = super::native_fee_paths::fill(
        &InstrumentAny::BinaryOption(instrument(1)),
        sequence,
        OrderSide::Buy,
        quantity,
        price,
        Money::zero(Currency::USDT()),
    );
    event.quantity_commission = Some(Quantity::from(fee));
    event
}

#[test]
fn quantity_commission_multiple_prices_replay_and_close_include_inventory_cost() {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    let first = charged_fill(1, "17", "0.009", "0.34");
    let second = charged_fill(2, "15", "0.010", "0.30");
    let mut position = Position::new_checked(&instrument, first.clone()).unwrap();
    assert_eq!(position.quantity.as_decimal(), Decimal::new(1666, 2));
    position.try_apply(&second).unwrap();
    assert!((position.quantity.as_decimal() - Decimal::new(3136, 2)).abs() < Decimal::new(1, 12));
    let encoded = serde_json::to_string(&position).unwrap();
    let mut restored: Position = serde_json::from_str(&encoded).unwrap();
    restored.try_apply(&second).unwrap();
    assert_eq!(serde_json::to_string(&restored).unwrap(), encoded);
    let close = super::native_fee_paths::fill(
        &instrument,
        3,
        OrderSide::Sell,
        "31.36",
        "0.010",
        Money::zero(Currency::USDT()),
    );
    restored.try_apply(&close).unwrap();
    assert!(restored.is_closed());
    assert!(restored.ts_closed.is_some());
    assert!(
        (restored.realized_pnl.unwrap().as_decimal() - Decimal::new(106, 4)).abs()
            <= Decimal::new(1, 8)
    );
    // A flat cycle has an independent cash oracle. Native PnL already includes removed
    // inventory cost; subtracting an execution-price fee estimate would charge it twice.
    let cash = close.last_qty.as_decimal() * close.last_px.as_decimal()
        - first.last_qty.as_decimal() * first.last_px.as_decimal()
        - second.last_qty.as_decimal() * second.last_px.as_decimal();
    assert_eq!(cash, Decimal::new(106, 4));
    let execution_price_fee_value = first.quantity_commission.unwrap().as_decimal()
        * first.last_px.as_decimal()
        + second.quantity_commission.unwrap().as_decimal() * second.last_px.as_decimal();
    assert_eq!(execution_price_fee_value, Decimal::new(606, 5));
    assert_ne!(
        restored.realized_pnl.unwrap().as_decimal() - execution_price_fee_value,
        cash
    );
    assert!((restored.realized_pnl.unwrap().as_decimal() - cash).abs() <= Decimal::new(1, 8));
    assert!(restored.adjustments.iter().all(|a| a.pnl_change.is_some()));
    assert!(restored.adjustments.is_empty());
    assert!(
        matches!(&restored.replay_events[0], nautilus_model::position::PositionReplayEvent::Filled(fill) if fill.quantity_commission == first.quantity_commission)
    );
}

#[test]
fn quantity_commission_invalid_inputs_leave_position_unchanged() {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    let mut position =
        Position::new_checked(&instrument, charged_fill(1, "17", "0.009", "0.34")).unwrap();
    for case in ["zero", "excessive", "duplicate_fee", "oversell"] {
        let mut invalid = charged_fill(2, "15", "0.010", "0.30");
        match case {
            "zero" => invalid.quantity_commission = Some(Quantity::zero(0)),
            "excessive" => invalid.quantity_commission = Some(Quantity::from(15)),
            "duplicate_fee" => invalid.commission = Some(Money::from("0.003 USDT")),
            "oversell" => {
                invalid.order_side = OrderSide::Sell;
                invalid.last_qty = Quantity::from("16.66");
            }
            _ => unreachable!(),
        }
        let before = serde_json::to_string(&position).unwrap();
        assert!(position.try_apply(&invalid).is_err(), "{case}");
        assert_eq!(serde_json::to_string(&position).unwrap(), before, "{case}");
    }
}

#[test]
fn quantity_commission_zero_fee_maker_and_quantity_fee_sell() {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    let first = charged_fill(1, "17", "0.009", "0.34");
    let mut position = Position::new_checked(&instrument, first).unwrap();
    let mut maker = super::native_fee_paths::fill(
        &instrument,
        2,
        OrderSide::Buy,
        "15",
        "0.010",
        Money::zero(Currency::USDT()),
    );
    maker.liquidity_side = LiquiditySide::Maker;
    position.try_apply(&maker).unwrap();
    let sell = super::native_fee_paths::fill(
        &instrument,
        3,
        OrderSide::Sell,
        "31.66",
        "0.010",
        Money::zero(Currency::USDT()),
    );
    position.try_apply(&sell).unwrap();
    assert!(position.is_closed());
    assert_eq!(position.realized_pnl.unwrap(), Money::from("0.0136 USDT"));

    let instrument = nautilus_model::instruments::stubs::currency_pair_btcusdt().into();
    let buy = super::native_fee_paths::fill(
        &instrument,
        1,
        OrderSide::Buy,
        "1",
        "100",
        Money::zero(Currency::USDT()),
    );
    let mut position = Position::new_checked(&instrument, buy).unwrap();
    let mut sell = super::native_fee_paths::fill(
        &instrument,
        2,
        OrderSide::Sell,
        "0.99",
        "110",
        Money::zero(Currency::USDT()),
    );
    sell.quantity_commission = Some(Quantity::from_decimal(Decimal::new(1, 2)).unwrap());
    let before = serde_json::to_string(&position).unwrap();
    assert!(position.try_apply(&sell).is_err());
    assert_eq!(serde_json::to_string(&position).unwrap(), before);
}

#[test]
fn quantity_commission_arrow_roundtrip_retains_fee_without_info() {
    use nautilus_model::events::OrderFilled;
    use nautilus_serialization::arrow::{DecodeTypedFromRecordBatch, EncodeToRecordBatch};
    for gross in ["32", "32.000000000000000099"] {
        let event = charged_fill(1, gross, "0.04", "0.64");
        assert!(event.info.is_none());
        let batch =
            OrderFilled::encode_batch(&event.metadata(), std::slice::from_ref(&event)).unwrap();
        let decoded = OrderFilled::decode_typed_batch(batch.schema().metadata(), batch).unwrap();
        assert_eq!(decoded.as_slice(), std::slice::from_ref(&event));
        let position = Position::new_checked(
            &InstrumentAny::BinaryOption(instrument(1)),
            decoded[0].clone(),
        )
        .unwrap();
        let snapshot = nautilus_model::events::PositionSnapshot::from_replay_state(&position, None);
        let batch = nautilus_model::events::PositionSnapshot::encode_batch(
            &snapshot.metadata(),
            std::slice::from_ref(&snapshot),
        )
        .unwrap();
        let restored = nautilus_model::events::PositionSnapshot::decode_typed_batch(
            batch.schema().metadata(),
            batch,
        )
        .unwrap();
        assert_eq!(restored[0].replay_state, snapshot.replay_state);
        let mut recovered: Position =
            serde_json::from_value(restored[0].replay_state.clone().unwrap()).unwrap();
        assert_eq!(
            super::native_fee_paths::quantity_fees(&recovered),
            super::native_fee_paths::quantity_fees(&position)
        );
        assert_eq!(recovered.quantity, position.quantity);
        assert_eq!(recovered.realized_pnl, position.realized_pnl);
        let before = serde_json::to_string(&recovered).unwrap();
        recovered.try_apply(&event).unwrap();
        assert_eq!(serde_json::to_string(&recovered).unwrap(), before);
        recovered
            .try_apply(&charged_fill(2, "1", "0.04", "0.02"))
            .unwrap();
        assert_eq!(
            recovered.quantity.as_decimal(),
            Decimal::from_str_exact(gross).unwrap() + Decimal::new(34, 2)
        );
        assert_eq!(
            super::native_fee_paths::quantity_fees(&recovered).unwrap(),
            Decimal::new(66, 2)
        );
        assert_eq!(recovered.realized_pnl.unwrap(), Money::from("-0.0264 USDT"));
    }
}

#[tokio::test]
async fn quantity_commission_reports_preserve_gross_and_publish_net_once() {
    for submitted in [32, 100] {
        run_reports(submitted).await;
    }
}

async fn run_reports(submitted: u32) {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let _portfolio = portfolio(&cache);
    connect_reported(&mut client, &mut rx).await;
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .quantity(Quantity::from(submitted))
        .price(Price::from("0.010"))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(1u64)))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            VenueOrderId::new(HASH),
            UnixNanos::from(2u64),
            UnixNanos::from(2u64),
        ))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache.clone(),
        None,
    );
    let observed = Rc::new(RefCell::new(Vec::new()));
    let target = observed.clone();
    let callback_cache = cache.clone();
    let handler = TypedHandler::from(move |event: &PositionEvent| {
        let cache = callback_cache.borrow();
        let position = cache.positions(None, Some(&instrument_id(1)), None, None, None)[0].clone();
        target.borrow_mut().push(json!({"event":event,"net":position.quantity.as_decimal().to_string(),"pnl":position.realized_pnl.map(|v|v.to_string())}));
    });
    let topic = format!("events.position.{}", order.strategy_id()).into();
    msgbus::subscribe_position_events(topic, handler.clone(), None);
    for (i, quantity, price, fee) in [(1, "17", "0.009", "0.34"), (2, "15", "0.010", "0.30")] {
        let mut report = FillReport::new(
            account_id(),
            instrument_id(1),
            VenueOrderId::new(HASH),
            format!("QF-{i}").into(),
            OrderSide::Buy,
            Quantity::from(quantity),
            Price::from(price),
            Money::zero(Currency::USDT()),
            LiquiditySide::Taker,
            Some(order.client_order_id()),
            None,
            UnixNanos::from(i * 1000u64),
            UnixNanos::from(OBSERVED),
            None,
        );
        report.quantity_commission = Some(Quantity::from(fee));
        let decoded: FillReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(decoded.clone())));
        engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(decoded)));
        let cache = cache.borrow();
        let native = cache.order(&order.client_order_id()).unwrap();
        assert_eq!(
            native.status(),
            if i == 1 || submitted == 100 {
                OrderStatus::PartiallyFilled
            } else {
                OrderStatus::Filled
            }
        );
        let fill = native
            .events()
            .into_iter()
            .find_map(|event| match event {
                OrderEventAny::Filled(f) if f.trade_id == report.trade_id => Some(f),
                _ => None,
            })
            .unwrap();
        assert_eq!(fill.quantity_commission, report.quantity_commission);
        assert_eq!(
            super::native_fee_paths::inventory_fees(native.events()),
            if i == 1 {
                Decimal::new(34, 2)
            } else {
                Decimal::new(64, 2)
            }
        );
    }
    if submitted == 100 {
        let cached = cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        engine.process(&factory.generate_order_canceled(
            &cached,
            Some(VenueOrderId::new(HASH)),
            UnixNanos::from(OBSERVED),
            UnixNanos::from(OBSERVED),
        ));
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Canceled
        );
    }
    assert_eq!(observed.borrow().len(), 2);
    assert_eq!(observed.borrow()[0]["pnl"], "-0.00306000 USDT");
    assert_eq!(observed.borrow()[1]["pnl"], "-0.00606000 USDT");
    assert_eq!(
        Decimal::from_str_exact(observed.borrow()[0]["net"].as_str().unwrap()).unwrap(),
        Decimal::new(1666, 2)
    );
    assert_eq!(
        Decimal::from_str_exact(observed.borrow()[1]["net"].as_str().unwrap()).unwrap(),
        Decimal::new(3136, 2)
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::from(32)
    );
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_total(Some(Currency::USDT()))
            .unwrap(),
        Money::from("1000 USDT")
    );
    msgbus::unsubscribe_position_events(topic, &handler);
    client.disconnect().await.unwrap();
}

fn mixed_price_cycle() -> Value {
    let instrument = InstrumentAny::BinaryOption(instrument(1));
    let mut fills = vec![
        charged_fill(1, "17", "0.009", "0.340000000000000001"),
        charged_fill(2, "15", "0.010", "0.299999999999999998"),
    ];
    // These are two partial executions of one order, so the adapter's cumulative gross
    // projection allocates 17 then 15 while retaining the 18-decimal source quantities.
    fills[1].client_order_id = fills[0].client_order_id;
    let mut position = Position::new_checked(&instrument, fills[0].clone()).unwrap();

    let first = json!({"nativeQuantity":position.quantity.as_decimal(),"fees":super::native_fee_paths::quantity_fees(&position)});
    position.try_apply(&fills[1]).unwrap();

    assert_eq!(
        position.quantity.as_decimal(),
        Decimal::from_str_exact("31.360000000000000001").unwrap()
    );
    let total = super::native_fee_paths::quantity_fees(&position).unwrap();
    assert_eq!(
        total,
        Decimal::from_str_exact("0.639999999999999999").unwrap()
    );
    assert_eq!(
        super::native_fee_paths::inventory_fee_residual(total, 16),
        Decimal::new(1, 18)
    );
    let mut cash = -Decimal::new(303, 3);
    let mut states = vec![first];
    for (sequence, side, quantity, price, fee) in [
        (3, OrderSide::Sell, "10", "0.011", "0.0022 USDT"),
        (4, OrderSide::Buy, "5", "0.008", "0 USDT"),
        (
            5,
            OrderSide::Sell,
            "26.360000000000000001",
            "0.012",
            "0.0063264 USDT",
        ),
    ] {
        let fill = super::native_fee_paths::fill(
            &instrument,
            sequence,
            side,
            quantity,
            price,
            Money::from(fee),
        );
        let value = fill.last_qty.as_decimal() * fill.last_px.as_decimal();
        cash += if side == OrderSide::Buy {
            -value
        } else {
            value
        } - fill.commission.unwrap().as_decimal();
        let encoded = serde_json::to_string(&position).unwrap();
        position = serde_json::from_str(&encoded).unwrap();
        let restored = serde_json::to_string(&position).unwrap();
        // Old events must remain duplicates even when replayed out of sequence.
        for previous in fills.iter().rev() {
            position.try_apply(previous).unwrap();
        }
        assert_eq!(serde_json::to_string(&position).unwrap(), restored);
        position.try_apply(&fill).unwrap();
        let equity = cash + position.quantity.as_decimal() * fill.last_px.as_decimal();
        assert!(
            (position.total_pnl(fill.last_px).unwrap().as_decimal() - equity).abs()
                <= Decimal::new(1, 8)
        );
        fills.push(fill);
        states.push(json!({"quantity":position.quantity.as_decimal(),"fees":super::native_fee_paths::quantity_fees(&position),
            "pnl":position.realized_pnl,"commissions":position.commissions()}));
    }
    assert!(position.is_closed());
    assert_eq!(
        cash,
        Decimal::from_str_exact("0.074793600000000000012").unwrap()
    );
    assert!((position.realized_pnl.unwrap().as_decimal() - cash).abs() <= Decimal::new(1, 8));
    assert!(position.adjustments.iter().all(|a| a.pnl_change.is_some()));
    assert_eq!(
        super::native_fee_paths::quantity_fees(&position),
        Some(total)
    );
    assert_eq!(position.commissions()[0], Money::from("0.0085264 USDT"));
    let snapshot = nautilus_model::events::PositionSnapshot::from_replay_state(&position, None);
    let restored: Position =
        serde_json::from_value(snapshot.replay_state.clone().unwrap()).unwrap();
    assert_eq!(
        super::native_fee_paths::quantity_fees(&restored),
        Some(total)
    );
    json!({"source":"synthetic Native Position mixed-price BUY/SELL accounting; not a strategy or venue execution",
        "rawBuyQuantities":["17","15"],
        "rawQuantitySum":"32","fills":fills,"states":states,"snapshot":snapshot,"cashPnl":cash.to_string(),
        "inventoryFeeCost":super::native_fee_paths::quantity_fee_value(&position).unwrap().to_string(),"nativePnlMinusCash":(position.realized_pnl.unwrap().as_decimal()-cash).to_string(),"feeSummary":total,"quantityResidual":super::native_fee_paths::inventory_fee_residual(total, 16).to_string(),"nativePnl":position.realized_pnl,
        "duplicateReplayUnchanged":true})
}

#[test]
fn quantity_commission_mixed_prices_buy_sell_restore_and_replay() {
    mixed_price_cycle();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline Rust Strategy + LiveNode precision and mixed-cycle capture; create_new only"]
async fn record_quantity_commission_residual_validation_json() {
    use super::live_node::strategy::Phase;
    let wallet =
        super::live_node::run_replay_variant(Phase::Fill, 1, true, Some(100), true, false, false)
            .await;
    let multiple_prices =
        super::live_node::run_replay_variant(Phase::Fill, 1, true, Some(32), true, true, true)
            .await;
    let round_trip = super::recorded_startup::run_trade_flow().await;
    let result = json!({"source":"offline Rust Strategy + LiveNode and Native model validation; no real order writes",
        "mainnetReads":0,"mainnetWrites":0,"nativeQuantityPrecision":16,"rawEvidencePrecision":18,
        "walletPrecision":wallet,"multiplePrices":multiple_prices,"recordedEconomicsRoundTrip":round_trip,
        "mixedNativeCycle":mixed_price_cycle()});
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/private_api/native_quantity_commission_quantity_only_live_node_20261017.json",
    );
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &result).unwrap();
}

// Preserve legacy base-fee behavior while explicit quantity fees include inventory cost.
fn accounting_naturality_audit() -> Value {
    let spot: InstrumentAny = nautilus_model::instruments::stubs::currency_pair_btcusdt().into();
    let base = spot.base_currency().unwrap();
    let legacy_buy = super::native_fee_paths::fill(
        &spot,
        1,
        OrderSide::Buy,
        "1",
        "100",
        Money::from_decimal(Decimal::new(1, 2), base).unwrap(),
    );
    let mut quantity_buy = legacy_buy.clone();
    quantity_buy.commission = Some(Money::zero(Currency::USDT()));
    quantity_buy.quantity_commission = Some(Quantity::from_decimal(Decimal::new(1, 2)).unwrap());
    let mut legacy = Position::new_checked(&spot, legacy_buy).unwrap();
    let mut quantity = Position::new_checked(&spot, quantity_buy).unwrap();
    assert_eq!(legacy.quantity, quantity.quantity);
    assert_eq!(legacy.avg_px_open, quantity.avg_px_open);
    // Position marks price movement on net inventory; it does not capitalize the
    // base fee into avg_px_open. Even at entry price, liquidatable value is below cost.
    let entry_mark = Price::from("100");
    assert_eq!(
        legacy.total_pnl(entry_mark).unwrap(),
        Money::zero(Currency::USDT())
    );
    assert_eq!(
        quantity.total_pnl(entry_mark).unwrap(),
        Money::from("-1 USDT")
    );
    let cash_cost = Decimal::from(100);
    assert_eq!(
        quantity.quantity.as_decimal() * entry_mark.as_decimal() - cash_cost,
        -Decimal::ONE
    );
    let small_gain_mark = Price::from("101");
    assert_eq!(
        quantity.total_pnl(small_gain_mark).unwrap(),
        Money::from("-0.01 USDT")
    );
    assert_eq!(
        quantity.quantity.as_decimal() * small_gain_mark.as_decimal() - cash_cost,
        Decimal::new(-1, 2)
    );
    let same_net = quantity.quantity.to_string();
    let close = super::native_fee_paths::fill(
        &spot,
        2,
        OrderSide::Sell,
        "0.99",
        "110",
        Money::zero(Currency::USDT()),
    );
    legacy.try_apply(&close).unwrap();
    quantity.try_apply(&close).unwrap();
    assert!(legacy.is_closed() && quantity.is_closed());
    assert_eq!(legacy.realized_pnl.unwrap(), Money::from("9.9 USDT"));
    assert_eq!(quantity.realized_pnl.unwrap(), Money::from("8.9 USDT"));

    // A late duplicate from the previous episode should be recognized before checking
    // whether its SELL quantity can fit the new, much smaller current inventory.
    let binary: InstrumentAny = instrument(1).into();
    let first = charged_fill(1, "1", "0.10", "0.01");
    let mut reopened = Position::new_checked(&binary, first).unwrap();
    let historical_close = super::native_fee_paths::fill(
        &binary,
        2,
        OrderSide::Sell,
        "0.99",
        "0.11",
        Money::zero(Currency::USDT()),
    );
    reopened.try_apply(&historical_close).unwrap();
    reopened
        .try_apply(&charged_fill(3, "0.1", "0.10", "0.001"))
        .unwrap();
    let before = serde_json::to_string(&reopened).unwrap();
    let result = reopened.try_apply(&historical_close);
    result.unwrap();
    assert_eq!(serde_json::to_string(&reopened).unwrap(), before);
    json!({
        "source":"direct Rust Native model diagnostic, synthetic; no new LiveNode or venue evidence",
        "nativeDevelopBase":"7b3ed05d980a344c1f698661d542ebcbae2c8972",
        "implementation":"Native fill-price inventory fee expense and exact persisted fee facts",
        "legacyBaseCommissionCompatibility":{
            "grossBuy":"1", "buyPrice":"100", "inventoryFee":"0.01", "net":same_net,
            "sellQty":"0.99", "sellPrice":"110", "cashDifference":"8.90 USDT",
            "legacyRealized":legacy.realized_pnl, "legacyCommissions":legacy.commissions,
            "quantityRealized":quantity.realized_pnl,
            "quantityCommissions":super::native_fee_paths::quantity_fees(&quantity),
            "meaning":"Legacy base commission behavior is unchanged; the explicit quantity fee path also writes off removed inventory cost."
        },
        "priorEpisodeDuplicate":{
            "newEpisodeNet":reopened.quantity,
            "historicalCloseQty":"0.99", "error":null,
            "stateUnchanged":true,
            "contractSatisfied":true,
            "finding":"Native durable identity filtering ignores the late historical duplicate before inventory preflight."
        }
    })
}

#[test]
fn quantity_commission_architecture_audit_preserves_cost_convention_and_replay_idempotency() {
    accounting_naturality_audit();
}

#[test]
#[ignore = "explicit synthetic Native architecture audit capture; create_new only"]
fn record_quantity_commission_architecture_audit_json() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/quantity_commission_base_parity_audit_20261017.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file, &accounting_naturality_audit()).unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline Native core review and Rust Strategy + LiveNode JSON; create_new only"]
async fn record_quantity_commission_core_review_json() {
    let fixture_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/private_api");
    let probes: Value = serde_json::from_slice(
        &std::fs::read(fixture_dir.join("quantity_commission_core_necessity_probes_20261017.json"))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(probes["sourcesRestoredExactly"], true);
    let partial_exit = super::recorded_startup::run_trade_flow_variant(true).await;
    assert_eq!(partial_exit["finalOpenOrders"], 0);
    assert_eq!(partial_exit["finalPositions"], 0);
    let value = json!({
        "source":"offline controlled Native core review and synthetic Rust Strategy + LiveNode replay; no new venue evidence",
        "mainnetReads":0,"mainnetWrites":0,
        "nativeDevelopBase":"7b3ed05d980a344c1f698661d542ebcbae2c8972",
        "coreNecessityProbes":probes,
        "baseCommissionParity":accounting_naturality_audit(),
        "mixedCycle":mixed_price_cycle(),
        "partialEntryMakerCancelTakerExit":partial_exit
    });
    let output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(fixture_dir.join("quantity_commission_core_review_livenode_20261017.json"))
        .unwrap();
    serde_json::to_writer_pretty(output, &value).unwrap();
}

#[test]
fn fill_price_expense_and_net_basis_are_distinct_from_gross_vwap() {
    let instrument: InstrumentAny = instrument(1).into();
    let first = charged_fill(1, "10", "0.100", "1");
    let mut position = Position::new_checked(&instrument, first).unwrap();
    let second = super::native_fee_paths::fill(
        &instrument,
        2,
        OrderSide::Buy,
        "10",
        "0.200",
        Money::zero(Currency::USDT()),
    );
    position.try_apply(&second).unwrap();
    assert_eq!(position.quantity.as_decimal(), Decimal::from(19));
    assert_eq!(position.cost_open, Decimal::new(290, 2));
    assert_eq!(position.realized_pnl.unwrap(), Money::from("-0.1 USDT"));
    assert_eq!(
        position.avg_px_open,
        Decimal::new(290, 2) / Decimal::from(19)
    );
    assert_ne!(position.avg_px_open, Decimal::new(15, 2));
    let close = super::native_fee_paths::fill(
        &instrument,
        3,
        OrderSide::Sell,
        "19",
        "0.200",
        Money::zero(Currency::USDT()),
    );
    position.try_apply(&close).unwrap();
    assert!(position.is_closed());
    assert_eq!(position.realized_pnl.unwrap(), Money::from("0.8 USDT"));
}
