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
    replay::{read_book_replay_windows, read_replay_windows},
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
    assert_live_replay_parity(false);
}

#[test]
fn live_depth_and_trade_strategy_matches_native_backtest_across_seal() {
    assert_live_replay_parity(true);
}

fn assert_live_replay_parity(use_depth: bool) {
    let root = std::env::temp_dir().join(format!("live-replay-{}", UUID4::new()));
    std::fs::create_dir_all(&root).unwrap();
    let live_root = root.clone();
    let (expected, source_count) = std::thread::spawn(move || {
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let data = if use_depth {
            depth_source(id)
        } else {
            source(id)
        };
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
            let books = read_book_replay_windows(&replay_root, id).unwrap();
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
        assert_eq!(data.len(), source_count);
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
        assert_eq!(engine.get_result().iterations, source_count);
        journal.borrow().clone()
    })
    .join()
    .unwrap();
    assert_eq!(actual, expected);
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
