// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! LiveNode DataEngine -> recorder -> Native BacktestEngine, without venue or order writes.
use anyhow::Result;
use catalog_capture_core::{
    CaptureConfig, CapturePlan,
    plan::{BookDeltasCaptureSpec, BookDepthCaptureSpec, TradeCaptureSpec},
    replay::{read_book_replay_windows, read_book_replay_windows_in_range, read_replay_windows},
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_backtest::{
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    engine::BacktestEngine,
};
use nautilus_common::{
    actor::{DataActor, drain_callbacks, registry::try_get_actor_unchecked},
    clock::VirtualClock,
    enums::Environment,
    timer::TimeEvent,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::models::fee::{FeeModelAny, MakerTakerFeeModel};
use nautilus_live::node::LiveNode;
use nautilus_model::{
    data::{
        BookOrder, Data, HasTsInit, OrderBookDelta, OrderBookDeltas, OrderBookDepth, TradeTick,
    },
    enums::{AccountType, AggressorSide, BookAction, BookType, OmsType, OrderSide, RecordFlag},
    identifiers::{InstrumentId, StrategyId, TradeId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orderbook::OrderBook,
    types::{Money, Price, Quantity},
};
use nautilus_trading::{StrategyConfig, StrategyCore, nautilus_strategy};
use std::{cell::RefCell, rc::Rc};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    payload: Payload,
    decision: OrderSide,
    ordinal: usize,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Payload {
    Book(OrderBookDeltas),
    Trade(TradeTick),
    Depth(Box<OrderBookDepth>),
}
#[derive(Debug)]
struct Trace {
    core: StrategyCore,
    id: InstrumentId,
    book: OrderBook,
    seen: usize,
    output: Rc<RefCell<Vec<Observation>>>,
}
nautilus_strategy!(Trace);
impl Trace {
    fn new(id: InstrumentId, output: Rc<RefCell<Vec<Observation>>>) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("TRACE-001")),
                order_id_tag: Some("001".into()),
                ..Default::default()
            }),
            id,
            book: OrderBook::new(id, BookType::L2_MBP),
            seen: 0,
            output,
        }
    }
    fn observe(&mut self, payload: Payload) {
        let bids = self.book.bids_as_map(None);
        let asks = self.book.asks_as_map(None);
        let decision = if bids.values().next() > asks.values().next() {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        self.output.borrow_mut().push(Observation {
            payload,
            decision,
            ordinal: self.seen,
        });
        self.seen += 1;
    }
}
impl DataActor for Trace {
    fn on_start(&mut self) -> Result<()> {
        self.subscribe_book_deltas(self.id, BookType::L2_MBP, None, None, false, None);
        self.subscribe_trades(self.id, None, None);
        self.subscribe_book_depth(self.id, BookType::L2_MBP, None, None, false, None);
        Ok(())
    }
    fn on_book_deltas(&mut self, frame: &OrderBookDeltas) -> Result<()> {
        self.book.apply_deltas(frame)?;
        self.observe(Payload::Book(frame.clone()));
        Ok(())
    }
    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> Result<()> {
        self.book.apply_depth(depth)?;
        self.observe(Payload::Depth(Box::new(depth.clone())));
        Ok(())
    }
    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        self.observe(Payload::Trade(*trade));
        Ok(())
    }
}
fn source(id: InstrumentId) -> Vec<Data> {
    let start = 1_800_000_000_000_000_000_u64;
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    book.add(
        BookOrder::new(
            OrderSide::Buy,
            Price::from("100.00"),
            Quantity::from("5.000"),
            0,
        ),
        0,
        1,
        start.into(),
    );
    book.add(
        BookOrder::new(
            OrderSide::Sell,
            Price::from("101.00"),
            Quantity::from("7.000"),
            0,
        ),
        0,
        1,
        start.into(),
    );
    let snapshot = book.to_deltas(start.into(), start.into());
    let update = |ts: u64, size: &str| {
        OrderBookDeltas::new(
            id,
            vec![OrderBookDelta::new(
                id,
                BookAction::Update,
                BookOrder::new(
                    OrderSide::Buy,
                    Price::from("100.00"),
                    Quantity::from(size),
                    0,
                ),
                RecordFlag::F_LAST as u8,
                ts,
                ts.into(),
                ts.into(),
            )],
        )
    };
    let mut data = vec![
        snapshot.into(),
        update(start + 10, "6.000").into(),
        update(start + 10, "9.000").into(),
        TradeTick::new(
            id,
            Price::from("100.00"),
            Quantity::from("1.000"),
            AggressorSide::Buy,
            TradeId::from("T1"),
            (start + 20).into(),
            (start + 20).into(),
        )
        .into(),
        update(start + 30, "4.000").into(),
    ];
    for (index, item) in data.iter_mut().enumerate() {
        if let Data::BookDeltas(frame) = item {
            for row in &mut frame.deltas {
                row.sequence = 1 + index as u64;
            }
            **frame = OrderBookDeltas::new(id, std::mem::take(&mut frame.deltas));
        }
    }
    data
}

fn depth_source(id: InstrumentId) -> Vec<Data> {
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    source(id)
        .into_iter()
        .map(|data| match data {
            Data::BookDeltas(frame) => {
                book.apply_deltas(&frame).unwrap();
                if RecordFlag::F_SNAPSHOT.matches(frame.flags) {
                    // Exercise Native depth beyond the ten inline levels.
                    for index in 0..15 {
                        book.add(
                            BookOrder::new(
                                OrderSide::Buy,
                                Price::new(90.0 - f64::from(index), 2),
                                Quantity::from("1.000"),
                                0,
                            ),
                            0,
                            frame.sequence,
                            frame.ts_event,
                        );
                    }
                }
                let bids: Vec<_> = book
                    .bids(None)
                    .flat_map(|level| level.iter().copied())
                    .collect();
                let asks: Vec<_> = book
                    .asks(None)
                    .flat_map(|level| level.iter().copied())
                    .collect();
                let bid_counts = vec![0; bids.len()];
                let ask_counts = vec![0; asks.len()];
                OrderBookDepth::new(
                    id,
                    bids,
                    asks,
                    bid_counts,
                    ask_counts,
                    RecordFlag::F_SNAPSHOT as u8,
                    frame.sequence,
                    frame.ts_event,
                    frame.ts_init,
                )
                .into()
            }
            other => other,
        })
        .collect()
}

#[test]
fn live_strategy_and_backtest_observe_identical_frames_and_decisions_across_seal() {
    assert_live_replay_parity(false, false);
}

#[test]
fn live_depth_and_trade_strategy_matches_native_backtest_across_seal() {
    assert_live_replay_parity(true, false);
}

#[test]
fn delayed_rest_baseline_records_losslessly_and_replays_after_explicit_warmup() {
    assert_live_replay_parity(false, true);
}

const RESEARCH_START: u64 = 1_800_000_000_000_000_025;

fn assert_live_replay_parity(use_depth: bool, delayed_snapshot: bool) {
    let root = std::env::temp_dir().join(format!("live-replay-{}", UUID4::new()));
    std::fs::create_dir_all(&root).unwrap();
    let live_root = root.clone();
    let (expected, source_count) = std::thread::spawn(move || {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let mut data = if use_depth {
            depth_source(id)
        } else {
            source(id)
        };
        if delayed_snapshot {
            let Data::BookDeltas(frame) = &mut data[0] else {
                unreachable!()
            };
            for row in &mut frame.deltas {
                row.ts_init = RESEARCH_START.into();
            }
            **frame = OrderBookDeltas::new(id, std::mem::take(&mut frame.deltas));
            let Data::Trade(trade) = &mut data[3] else {
                unreachable!()
            };
            trade.ts_init = (RESEARCH_START + 1).into();
            trade.ts_event = trade.ts_init;
        }
        let journal = Rc::new(RefCell::new(Vec::new()));
        let mut node = LiveNode::builder(TraderId::from("LIVE-001"), Environment::Live)
            .unwrap()
            .with_name("FRAME-PARITY")
            .with_clock_factory(|| Rc::new(RefCell::new(VirtualClock::new())))
            .build()
            .unwrap();
        node.kernel()
            .cache
            .borrow_mut()
            .add_instrument(instrument)
            .unwrap();
        node.kernel()
            .cache
            .borrow_mut()
            .add_order_book(OrderBook::new(id, BookType::L2_MBP))
            .unwrap();
        let capture = CatalogCaptureActor::new(CatalogCaptureActorConfig::new(
            CaptureConfig {
                catalog_uri: format!("file://{}", live_root.display()),
                ..Default::default()
            },
            CapturePlan {
                book_deltas: if use_depth {
                    Vec::new()
                } else {
                    vec![BookDeltasCaptureSpec {
                        instrument_id: id,
                        book_type: BookType::L2_MBP,
                        depth: None,
                    }]
                },
                book_depth: if use_depth {
                    vec![BookDepthCaptureSpec {
                        instrument_id: id,
                        book_type: BookType::L2_MBP,
                        depth: None,
                    }]
                } else {
                    Vec::new()
                },
                trades: vec![TradeCaptureSpec { instrument_id: id }],
                ..Default::default()
            },
        ))
        .unwrap();
        let capture_id = capture.actor_id();
        node.add_actor(capture).unwrap();
        node.add_strategy(Trace::new(id, Rc::clone(&journal)))
            .unwrap();
        node.kernel().trader.borrow_mut().initialize().unwrap();
        node.kernel_mut().start_trader().unwrap();
        for (index, item) in data.iter().enumerate() {
            if let Data::BookDeltas(frame) = item {
                node.kernel()
                    .cache
                    .borrow_mut()
                    .order_book_mut(&id)
                    .unwrap()
                    .apply_deltas(frame)
                    .unwrap();
            }
            if let Data::BookDepth(depth) = item {
                node.kernel()
                    .cache
                    .borrow_mut()
                    .order_book_mut(&id)
                    .unwrap()
                    .apply_depth(depth)
                    .unwrap();
            }
            node.kernel()
                .data_engine
                .borrow_mut()
                .process_data(item.clone());
            drain_callbacks(10_000).unwrap();
            if index == 1 {
                let mut capture =
                    try_get_actor_unchecked::<CatalogCaptureActor>(&capture_id.inner()).unwrap();
                capture
                    .on_time_event(&TimeEvent::new(
                        "SEGMENT_SEAL".into(),
                        UUID4::new(),
                        UnixNanos::default(),
                        UnixNanos::default(),
                    ))
                    .unwrap();
            }
        }
        node.kernel_mut().stop_trader();
        let output = journal.borrow().clone();
        assert_eq!(output.len(), data.len());
        (output, data.len())
    })
    .join()
    .unwrap();
    let replay_root = root.clone();
    let actual = std::thread::spawn(move || {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let mut data: Vec<Data> = if use_depth {
            let depths = read_replay_windows::<OrderBookDepth>(&replay_root, id).unwrap();
            assert_eq!(depths.len(), 1);
            assert_eq!(depths[0].data[0].bids.len(), 16);
            depths
                .into_iter()
                .flat_map(|window| window.data)
                .map(Data::from)
                .collect()
        } else {
            let books = if delayed_snapshot {
                let raw = read_replay_windows::<OrderBookDelta>(&replay_root, id).unwrap();
                assert_eq!(raw.len(), 1);
                let frames =
                    catalog_capture_core::group_book_deltas(id, raw[0].data.clone()).unwrap();
                assert_eq!(frames.len(), 4);
                assert_eq!(frames[0].ts_init.as_u64(), RESEARCH_START);
                assert!(frames[1].ts_init < frames[0].ts_init);
                assert_eq!(
                    frames
                        .iter()
                        .map(|frame| frame.sequence)
                        .collect::<Vec<_>>(),
                    vec![1, 2, 3, 5]
                );
                assert!(read_book_replay_windows(&replay_root, id).is_err());
                read_book_replay_windows_in_range(
                    &replay_root,
                    id,
                    RESEARCH_START.into(),
                    (RESEARCH_START + 100).into(),
                )
                .unwrap()
            } else {
                read_book_replay_windows(&replay_root, id).unwrap()
            };
            assert_eq!(books.len(), 1);
            assert!(books[0].has_book_baseline());
            books
                .into_iter()
                .flat_map(|window| window.data)
                .map(Data::from)
                .collect()
        };
        let trades = read_replay_windows::<TradeTick>(&replay_root, id).unwrap();
        assert_eq!(trades.len(), 1);
        data.extend(
            trades
                .into_iter()
                .flat_map(|window| window.data)
                .map(Data::from),
        );
        data.sort_by_key(HasTsInit::ts_init);
        let expected_count = if delayed_snapshot { 3 } else { source_count };
        assert_eq!(data.len(), expected_count);
        let journal = Rc::new(RefCell::new(Vec::new()));
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
        engine
            .add_venue(
                SimulatedVenueConfig::builder()
                    .venue(Venue::from("BINANCE"))
                    .oms_type(OmsType::Netting)
                    .account_type(AccountType::Margin)
                    .book_type(BookType::L2_MBP)
                    .starting_balances(vec![Money::from("1_000_000 USDT")])
                    .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        engine.add_instrument(&instrument).unwrap();
        engine
            .add_strategy(Trace::new(id, Rc::clone(&journal)))
            .unwrap();
        engine.add_data(data, None, true, true).unwrap();
        engine.run(None, None, None, false).unwrap();
        assert_eq!(engine.get_result().iterations, expected_count);
        journal.borrow().clone()
    })
    .join()
    .unwrap();
    if delayed_snapshot {
        // Book initialization is new; no claim of restoring pre-start strategy state.
        assert_eq!(actual.len(), 3);
        assert_eq!(expected.len(), 5);
        for (replay, live) in actual[1..].iter().zip(&expected[3..]) {
            assert_eq!(replay.payload, live.payload);
            assert_eq!(replay.decision, live.decision);
        }
        assert_eq!(actual[1].ordinal, 1);
        assert_eq!(expected[3].ordinal, 3);
    } else {
        assert_eq!(actual, expected);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn gap_windows_start_fresh_backtest_engines_instead_of_carrying_strategy_state() {
    use catalog_capture_core::{
        PartitionKey,
        sink::{BookDeltaCatalogSink, CaptureSink},
    };
    let root = std::env::temp_dir().join(format!("independent-replays-{}", UUID4::new()));
    std::fs::create_dir_all(&root).unwrap();
    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let id = instrument.id();
    let input = source(id);
    let books: Vec<_> = input
        .into_iter()
        .filter_map(|data| {
            if let Data::BookDeltas(frame) = data {
                Some(*frame)
            } else {
                None
            }
        })
        .collect();
    let key = PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key();
    let mut sink = BookDeltaCatalogSink::from_config(&CaptureConfig {
        catalog_uri: format!("file://{}", root.display()),
        ..Default::default()
    })
    .unwrap();
    sink.write_batch(&key, books[..2].to_vec()).unwrap();
    sink.break_continuity(Some(&key)).unwrap();
    let mut recovered = OrderBook::new(id, BookType::L2_MBP);
    for frame in &books[..3] {
        recovered.apply_deltas(frame).unwrap();
    }
    let baseline = recovered.to_deltas(books[2].ts_event, books[2].ts_init);
    sink.write_batch(&key, vec![baseline, books[3].clone()])
        .unwrap();
    sink.seal_all_for_shutdown().unwrap();
    let windows = read_book_replay_windows(&root, id).unwrap();
    assert_eq!(windows.len(), 2);
    let mut first_decisions = Vec::new();
    for window in windows {
        assert!(window.has_book_baseline());
        assert_eq!(window.data.len(), 2);
        let instrument = instrument.clone();
        let observations = std::thread::spawn(move || {
            let journal = Rc::new(RefCell::new(Vec::new()));
            let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
            engine
                .add_venue(
                    SimulatedVenueConfig::builder()
                        .venue(Venue::from("BINANCE"))
                        .oms_type(OmsType::Netting)
                        .account_type(AccountType::Margin)
                        .book_type(BookType::L2_MBP)
                        .starting_balances(vec![Money::from("1_000_000 USDT")])
                        .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into())
                        .build()
                        .unwrap(),
                )
                .unwrap();
            engine.add_instrument(&instrument).unwrap();
            engine
                .add_strategy(Trace::new(id, Rc::clone(&journal)))
                .unwrap();
            engine
                .add_data(
                    window.data.into_iter().map(Data::from).collect(),
                    None,
                    true,
                    true,
                )
                .unwrap();
            engine.run(None, None, None, false).unwrap();
            assert_eq!(engine.get_result().iterations, 2);
            journal.borrow().clone()
        })
        .join()
        .unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].ordinal, 0);
        assert_eq!(observations[1].ordinal, 1);
        first_decisions.push(observations[0].decision);
    }
    assert_eq!(first_decisions, vec![OrderSide::Sell, OrderSide::Buy]);
    std::fs::remove_dir_all(root).unwrap();
}

/// Opt-in readback of the public Futures capture; never opens a venue connection or sends orders.
#[test]
#[ignore = "requires FUTURES_RECOVERY_CATALOG and FUTURES_RECOVERY_START_NS"]
fn recorded_futures_recovery_matches_native_backtest_after_explicit_start() {
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    let root = std::path::PathBuf::from(std::env::var("FUTURES_RECOVERY_CATALOG").unwrap());
    let start: UnixNanos = std::env::var("FUTURES_RECOVERY_START_NS")
        .unwrap()
        .parse::<u64>()
        .unwrap()
        .into();
    let mut reports = Vec::new();
    for symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"] {
        let root = root.clone();
        let report = std::thread::spawn(move || {
            let id = InstrumentId::from(format!("{symbol}-PERP.BINANCE").as_str());
            let raw = read_replay_windows::<OrderBookDelta>(&root, id).unwrap();
            assert_eq!(raw.len(), 1, "probe needs one uninterrupted source epoch per symbol");
            let frames = catalog_capture_core::group_book_deltas(id, raw[0].data.clone()).unwrap();
            let decreases = frames.windows(2).filter(|pair| pair[0].ts_init > pair[1].ts_init).count();
            let end = raw[0].end;
            let mut expected = OrderBook::new(id, BookType::L2_MBP);
            for frame in &frames { expected.apply_deltas(frame).unwrap(); }
            let prepared = read_book_replay_windows_in_range(&root, id, start, end).unwrap();
            assert_eq!(prepared.len(), 1);
            assert!(prepared[0].has_book_baseline());
            assert_eq!(prepared[0].start, start);
            let retained_frames = prepared[0].data.len();
            let trades = read_replay_windows::<TradeTick>(&root, id).unwrap();
            let mut data: Vec<Data> = prepared.into_iter().flat_map(|window| window.data).map(Data::from)
                .chain(trades.into_iter().flat_map(|window| window.data)
                    .filter(|trade| trade.ts_init >= start && trade.ts_init <= end).map(Data::from)).collect();
            data.sort_by_key(HasTsInit::ts_init);
            let count = data.len();
            let instrument = ParquetDataCatalog::new(&root, None, None, None, None)
                .query_instruments(Some(&[id.to_string()])).unwrap().into_iter().last().unwrap();
            let journal = Rc::new(RefCell::new(Vec::new()));
            let mut engine = BacktestEngine::new(BacktestEngineConfig::default()).unwrap();
            engine.add_venue(SimulatedVenueConfig::builder()
                .venue(id.venue).oms_type(OmsType::Netting).account_type(AccountType::Margin)
                .book_type(BookType::L2_MBP).starting_balances(vec![Money::from("1_000_000 USDT")])
                .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into()).build().unwrap()).unwrap();
            engine.add_instrument(&instrument).unwrap();
            engine.add_strategy(Trace::new(id, Rc::clone(&journal))).unwrap();
            engine.add_data(data, None, true, true).unwrap();
            engine.run(None, None, None, false).unwrap();
            assert_eq!(engine.get_result().iterations, count);
            let observations = journal.borrow();
            assert_eq!(observations.len(), count);
            let mut replayed = OrderBook::new(id, BookType::L2_MBP);
            let mut actual_frames = 0;
            for observation in observations.iter() {
                if let Payload::Book(frame) = &observation.payload {
                    replayed.apply_deltas(frame).unwrap();
                    actual_frames += 1;
                }
            }
            assert_eq!(actual_frames, retained_frames);
            assert_eq!(replayed.bids_as_map(None), expected.bids_as_map(None));
            assert_eq!(replayed.asks_as_map(None), expected.asks_as_map(None));
            assert_eq!(replayed.sequence, expected.sequence);
            serde_json::json!({"instrument":id.to_string(),"raw_rows":raw[0].data.len(),
                "raw_frames":frames.len(),"time_decreases":decreases,"replay_frames":retained_frames,
                "strategy_callbacks":count,"start_ns":start.as_u64(),"end_ns":end.as_u64()})
        }).join().unwrap();
        reports.push(report);
    }
    assert!(
        reports
            .iter()
            .any(|report| report["time_decreases"].as_u64().unwrap() > 0),
        "probe must observe buffered recovery timestamp reordering"
    );
    if let Ok(path) = std::env::var("FUTURES_RECOVERY_REPORT") {
        std::fs::write(path, serde_json::to_string_pretty(&reports).unwrap()).unwrap();
    }
    eprintln!("{}", serde_json::to_string(&reports).unwrap());
}
