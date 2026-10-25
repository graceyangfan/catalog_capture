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

//! Opt-in mainnet MARKET probe through actual Strategy/Risk/Execution/Cache.
use nautilus_common::actor::DataActor;
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    events::OrderEventAny,
    identifiers::{ClientOrderId, InstrumentId},
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_network::websocket::TransportBackend;
use nautilus_predict::PredictDataClientConfig;
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::*;

#[path = "market_probe_roundtrip.rs"]
mod roundtrip;
use roundtrip::{OfiWindow, RoundTrip};

#[derive(Clone, Debug)]
struct ProbeCase {
    name: &'static str,
    bps: u32,
    fok: bool,
    quote: bool,
}

fn probe_cases(matrix: bool) -> Vec<ProbeCase> {
    let mut cases = vec![ProbeCase {
        name: "base_fok_zero",
        bps: 0,
        fok: true,
        quote: false,
    }];
    if matrix {
        cases.extend([
            ProbeCase {
                name: "base_fok_100bps",
                bps: 100,
                fok: true,
                quote: false,
            },
            ProbeCase {
                name: "base_fok_200bps",
                bps: 200,
                fok: true,
                quote: false,
            },
            ProbeCase {
                name: "base_gtc_100bps",
                bps: 100,
                fok: false,
                quote: false,
            },
            ProbeCase {
                name: "quote_fok_zero",
                bps: 0,
                fok: true,
                quote: true,
            },
        ]);
    } else {
        cases.push(ProbeCase {
            name: "base_gtc_zero",
            bps: 0,
            fok: false,
            quote: false,
        });
    }
    cases
}

#[derive(Debug)]
struct MarketProbe {
    core: StrategyCore,
    instrument: InstrumentId,
    journal: Rc<RefCell<Vec<Value>>>,
    next: usize,
    active: Option<ClientOrderId>,
    submitted_ns: u64,
    accepted_ns: Option<u64>,
    cancel_sent: bool,
    cases: Vec<ProbeCase>,
    // Present only for the separately supervised single-order partial-fill run.
    terms: Option<(String, Quantity, OrderSide)>,
    watch_book: bool,
    allow_micro: bool,
    output: Option<PathBuf>,
    round_trip: Option<RoundTrip>,
    ofi: OfiWindow,
    entry: Option<ClientOrderId>,
    entry_deadline: Option<u64>,
    entry_budget: Decimal,
    max_loss: Decimal,
    last_query_ns: u64,
}

impl MarketProbe {
    fn cancel_active(&mut self) -> anyhow::Result<()> {
        if let Some(id) = self.active
            && !self.cancel_sent
            && self.cache().order(&id).is_some_and(|o| !o.is_closed())
        {
            self.cancel_sent = true;
            self.journal
                .borrow_mut()
                .push(json!({"type":"NativeCancelRequested","clientOrderId":id.to_string(),"observedNs":self.clock().timestamp_ns().as_u64()}));
            self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
}

impl MarketProbe {
    fn advance_probe(
        &mut self,
        trigger: Option<nautilus_model::data::QuoteTick>,
    ) -> anyhow::Result<()> {
        if self.round_trip.as_ref().is_some_and(|state| state.done) {
            return Ok(());
        }
        if let Some(id) = self.active {
            let cache = self.cache();
            let Some(order) = cache.order(&id) else {
                return Ok(());
            };
            anyhow::ensure!(
                self.terms.is_some() || order.filled_qty().is_zero(),
                "unexpected fill; stop probe"
            );
            if !order.is_closed() {
                if self
                    .clock()
                    .timestamp_ns()
                    .as_u64()
                    .saturating_sub(if self.terms.is_some() {
                        self.accepted_ns.unwrap_or(self.submitted_ns)
                    } else {
                        self.submitted_ns
                    })
                    > if self.terms.is_some() && self.accepted_ns.is_some() {
                        2_000_000_000
                    } else {
                        4_000_000_000
                    }
                {
                    self.cancel_active()?;
                    let now = self.clock().timestamp_ns().as_u64();
                    if self.terms.is_some()
                        && now.saturating_sub(self.last_query_ns) >= 1_000_000_000
                    {
                        let order = self.cache().order(&id);
                        if let Some(order) = order {
                            self.last_query_ns = now;
                            self.journal.borrow_mut().push(json!({"type":"NativeRecoveryQueryRequested","clientOrderId":id.to_string(),"observedNs":now}));
                            self.query_order(&order, Some(ClientId::new("PREDICT")), None)?;
                        }
                    }
                }
                return Ok(());
            }
            self.active = None;
        }
        if self.next >= self.cases.len() {
            return self.drive_round_trip();
        }
        let Some(quote) = trigger.or_else(|| self.cache().quote(&self.instrument)) else {
            return Ok(());
        };
        if self
            .entry_deadline
            .is_some_and(|deadline| self.clock().timestamp_ns().as_u64() >= deadline)
        {
            return self.finish_round_trip("entry_window_expired_without_buy", Decimal::ZERO);
        }
        let ofi = self.ofi.evidence(self.clock().timestamp_ns().as_u64());
        if self.round_trip.is_some() && (!self.watch_book || ofi["eligible"] != true) {
            return Ok(());
        }
        let case = self.cases[self.next].clone();
        let signed_bound =
            Decimal::new(80, 2) * (Decimal::ONE + Decimal::new(i64::from(case.bps), 4));
        let gap = quote.ask_price.as_decimal() - signed_bound;
        let dynamic_terms = if self.watch_book {
            let ask = quote.ask_price.as_decimal();
            if ask <= Decimal::ZERO || ask > Decimal::new(95, 2) {
                return Ok(());
            }
            let qty = (self.entry_budget / ask)
                .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);
            Some((
                ask.to_string(),
                Quantity::from(qty.to_string().as_str()),
                OrderSide::Buy,
            ))
        } else {
            None
        };
        let (price, quantity, side) = if let Some((price, quantity, side)) =
            dynamic_terms.as_ref().or(self.terms.as_ref())
        {
            let reference = Decimal::from_str_exact(price)?;
            let age = self
                .clock()
                .timestamp_ns()
                .as_u64()
                .saturating_sub(quote.ts_init.as_u64());
            anyhow::ensure!(age <= 2_000_000_000, "partial probe quote is stale");
            if *side == OrderSide::Buy {
                let ask = quote.ask_price.as_decimal();
                let available = quote.ask_size.as_decimal();
                if !(ask <= reference && reference <= ask * Decimal::new(103, 2)) {
                    return Ok(());
                }
                // Wait on Native's current BBO instead of submitting after the
                // transient thin level has already been replenished. Multi-level
                // candidates are checked independently by the relay's fresh book.
                let micro = self.allow_micro
                    && (Decimal::new(2, 2)..=Decimal::new(10, 2)).contains(&(ask * available))
                    && quote.bid_price.as_decimal()
                        * quantity.as_decimal()
                        * Decimal::new(98, 2)
                        * Decimal::new(97, 2)
                        >= Decimal::new(105, 2);
                if ask == reference
                    && !((Decimal::new(110, 2)..=self.entry_budget * Decimal::new(95, 2))
                        .contains(&(ask * available))
                        && quantity.as_decimal() >= available * Decimal::new(105, 2)
                        && quote.bid_price.as_decimal()
                            * available
                            * Decimal::new(98, 2)
                            * Decimal::new(97, 2)
                            >= Decimal::new(103, 2)
                        && quote.bid_size.as_decimal() >= quantity.as_decimal() * Decimal::from(3))
                    && !(micro
                        && quantity.as_decimal() >= available * Decimal::new(105, 2)
                        && quote.bid_size.as_decimal() >= quantity.as_decimal() * Decimal::from(3))
                {
                    return Ok(());
                }
            } else if quote.bid_price.as_decimal() < reference {
                return Ok(());
            }
            (price.clone(), *quantity, *side)
        } else {
            anyhow::ensure!(
                (Decimal::new(2, 2)..=Decimal::new(10, 2)).contains(&gap),
                "ask moved outside 2-10 cent signed-price probe gap"
            );
            (
                "0.80".to_owned(),
                Quantity::from(if case.quote { "1.00" } else { "1.25" }),
                OrderSide::Buy,
            )
        };
        if self.round_trip.is_some()
            && Decimal::from_str_exact(&price)? * quantity.as_decimal()
                - quote.bid_price.as_decimal() * quantity.as_decimal() * Decimal::new(9604, 4)
                > self.max_loss
        {
            return Ok(());
        }
        let signed_bound = Decimal::from_str_exact(&price)?
            * (Decimal::ONE
                + Decimal::new(i64::from(case.bps), 4)
                    * if side == OrderSide::Buy {
                        Decimal::ONE
                    } else {
                        -Decimal::ONE
                    });
        let tif = if case.fok {
            TimeInForce::Fok
        } else {
            TimeInForce::Gtc
        };
        let own = self.order().market(
            self.instrument,
            side,
            quantity,
            Some(tif),
            None,
            Some(case.quote),
            None,
            None,
            None,
            None,
        );
        self.active = Some(own.client_order_id());
        self.entry = self.active;
        self.cancel_sent = false;
        let submitted_ns = self.clock().timestamp_ns().as_u64();
        self.submitted_ns = submitted_ns;
        self.accepted_ns = None;
        let intent = json!({"type":"NativeSubmitIntent","case":case.name,"clientOrderId":own.client_order_id().to_string(),"orderType":"MARKET","timeInForce":tif.to_string(),"quantity":quantity.to_string(),"quoteQuantity":case.quote,"market_price":price,"market_order_slippage_bps":case.bps,"signedBound":signed_bound.to_string(),"observedBid":quote.bid_price.to_string(),"observedBidSize":quote.bid_size.to_string(),"observedAsk":quote.ask_price.to_string(),"observedAskSize":quote.ask_size.to_string(),"quoteTsEvent":quote.ts_event.as_u64(),"quoteTsInit":quote.ts_init.as_u64(),"observedNs":submitted_ns,"ofi":ofi});
        if let Some(output) = &self.output {
            // Persist the exact trigger frame before signing/transmission. The
            // relay verifies SDK economics locally without another book round trip.
            fill_session::checkpoint(&output.join("quote_intent.json"), &intent)?;
        }
        self.journal.borrow_mut().push(intent);
        self.next += 1;
        self.submit_order(
            own,
            None,
            Some(ClientId::new("PREDICT")),
            Some(nautilus_core::Params::new(
                [
                    ("market_price".into(), json!(price)),
                    ("market_order_slippage_bps".into(), json!(case.bps)),
                ]
                .into(),
            )),
        )
    }
}

impl DataActor for MarketProbe {
    fn on_quote(&mut self, quote: &nautilus_model::data::QuoteTick) -> anyhow::Result<()> {
        let now = self.clock().timestamp_ns().as_u64();
        self.ofi.observe(quote, now);
        if self.terms.is_some() {
            let observed_ns = self.clock().timestamp_ns().as_u64();
            self.journal.borrow_mut().push(json!({"type":"NativeQuoteObserved","bid":quote.bid_price.to_string(),"ask":quote.ask_price.to_string(),"bidSize":quote.bid_size.to_string(),"askSize":quote.ask_size.to_string(),"tsEvent":quote.ts_event.as_u64(),"tsInit":quote.ts_init.as_u64(),"observedNs":observed_ns}));
        }
        if self.watch_book {
            self.advance_probe(Some(*quote))?;
        }
        Ok(())
    }

    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.round_trip.is_some() {
            let now = self.clock().timestamp_ns().as_u64();
            self.entry_deadline = Some(now + 90_000_000_000);
        }
        self.subscribe_quotes(self.instrument, Some(ClientId::new("PREDICT")), None);
        self.clock().set_timer(
            "MARKET-PROBE",
            std::time::Duration::from_millis(100),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }

    fn on_time_event(&mut self, _: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        self.advance_probe(None)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timer("MARKET-PROBE");
        self.cancel_active()?;
        self.cancel_exit()
    }
}

nautilus_strategy!(MarketProbe, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        if let OrderEventAny::Filled(fill) = &event
            && self.entry == Some(fill.client_order_id)
        {
            let now = self.clock().timestamp_ns().as_u64();
            if let Some(state) = self.round_trip.as_mut() {
                state.observe_entry_fill(now);
            }
        }
        self.journal
            .borrow_mut()
            .push(json!({"type":"NativeOrderEvent","event":event,"observedNs":self.clock().timestamp_ns().as_u64()}));
    }
    fn on_order_accepted(&mut self, event: nautilus_model::events::OrderAccepted) {
        let accepted_ns = self.clock().timestamp_ns().as_u64();
        if self.entry == Some(event.client_order_id) {
            self.accepted_ns = Some(accepted_ns);
        }
        self.journal.borrow_mut().push(json!({"type":"StrategyOrderAccepted","clientOrderId":event.client_order_id.to_string(),"observedNs":self.clock().timestamp_ns().as_u64()}));
        if self.terms.is_none()
            && let Err(error) = self.cancel_active()
        {
            self.journal
                .borrow_mut()
                .push(json!({"type":"StrategyCancelError","reason":error.to_string()}));
        }
    }
});

#[tokio::test(flavor = "current_thread")]
#[ignore = "MAINNET: bounded supervisor only; zero-fill controls or one <=2 USDT partial-fill probe"]
async fn mainnet_native_market_parameters() -> anyhow::Result<()> {
    let settings =
        PredictExecutionSettings::load(&root().join("env/predict-native-market-probe.toml"))?;
    let manifest: Value = serde_json::from_slice(&std::fs::read(
        root().join("logs/predict-native-market-probe.json"),
    )?)?;
    let partial = manifest["partialProbe"] == true;
    let sell = partial && manifest["side"] == "SELL";
    anyhow::ensure!(
        settings.test.allow_order_writes
            && !settings.test.require_post_only
            && settings.test.max_order_usdt
                <= if partial {
                    Decimal::new(2, 0)
                } else {
                    Decimal::new(102, 2)
                }
            && settings.test.max_total_usdt
                <= if partial {
                    Decimal::new(2, 0)
                } else {
                    Decimal::new(510, 2)
                },
        "explicit bounded MARKET profile required"
    );
    if let Some(target) = manifest["targetPrincipal"].as_str() {
        anyhow::ensure!(
            Decimal::from_str_exact(target)? <= Decimal::new(2, 0),
            "target exceeds fixed budget"
        );
    }
    let market_id = manifest["marketId"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("market ID missing"))?;
    let cases = if partial {
        vec![ProbeCase {
            name: "bounded_partial",
            bps: if sell { 300 } else { 0 },
            fok: sell,
            quote: false,
        }]
    } else {
        probe_cases(manifest["parameterMatrix"] == true)
    };
    let case_count = cases.len();
    let output = PathBuf::from(
        manifest["output"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("output missing"))?,
    );
    anyhow::ensure!(
        !output.join("native.json").exists(),
        "refusing to overwrite probe evidence"
    );
    let origin = manifest["origin"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("relay missing"))?;
    anyhow::ensure!(
        origin.starts_with("http://127.0.0.1:"),
        "bounded supervisor relay required"
    );
    let mut config = settings.private_config()?;
    config.base_url_http = Some(origin.to_owned());
    let signer = settings.signer()?;
    let session = nautilus_predict::http::PredictPrivateHttpClient::new(config)?
        .authenticate(&signer)
        .await?;
    let market = session.request_execution_market(market_id).await?;
    let book = session.request_execution_orderbook(market_id).await?;
    anyhow::ensure!(
        book.market_id == market_id
            && (partial
                || book
                    .asks
                    .first()
                    .is_some_and(|r| (Decimal::new(83, 2)..=Decimal::new(90, 2)).contains(&r.0))),
        "market/book admission mismatch"
    );
    let account = AccountId::new("PREDICT-native-market-probe");
    let mut execution =
        PredictExecutionClientConfig::new(account, session, settings.websocket_config()?);
    execution.auth_signer = Some(signer.clone());
    execution.chain = Some(settings.chain_config());
    execution.orderbook_removal_enabled = true;
    // Deliberately differs from per-order zero: detect an ignored override in the relay oracle.
    execution.submission_signer = Some(signer);
    let mut live = node::build_node_with_engine_and_stop_delay(
        TraderId::new("NATIVE-MARKET-001"),
        PredictDataClientConfig {
            api_key: settings
                .private_config()?
                .api_key
                .ok_or_else(|| anyhow::anyhow!("API key required"))?,
            market_ids: vec![market_id],
            base_url_http: None,
            base_url_ws: None,
            http_timeout_secs: 15,
            ws_timeout_secs: 15,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        execution,
        nautilus_live::node::config::LiveExecutionEngineConfig {
            reconciliation: false,
            inflight_check_interval_ms: 0,
            open_check_interval_secs: None,
            position_check_interval_secs: None,
            ..Default::default()
        },
        3,
    )?;
    let outcome = if partial {
        manifest["outcome"]
            .as_u64()
            .filter(|x| [1, 2].contains(x))
            .ok_or_else(|| anyhow::anyhow!("outcome missing"))?
    } else {
        1
    };
    let instrument = InstrumentId::from_as_ref(format!("{market_id}-{outcome}.PREDICT"))?;
    let journal = Rc::new(RefCell::new(Vec::new()));
    live.add_strategy(MarketProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some("MARKET-PROBE-001".into()),
            ..Default::default()
        }),
        instrument,
        journal: journal.clone(),
        next: 0,
        active: None,
        submitted_ns: 0,
        accepted_ns: None,
        cancel_sent: false,
        cases,
        watch_book: partial && manifest["watchLiveBook"] == true,
        allow_micro: partial && manifest["allowMicroRemainder"] == true,
        output: partial.then(|| output.clone()),
        round_trip: (partial && manifest["ofiRoundTrip"] == true).then(RoundTrip::default),
        ofi: OfiWindow::default(),
        entry: None,
        entry_deadline: None,
        last_query_ns: 0,
        max_loss: Decimal::from_str_exact(manifest["maxLossUSDT"].as_str().unwrap_or("0.12"))?,
        entry_budget: Decimal::from_str_exact(
            manifest["targetPrincipal"].as_str().unwrap_or("1.4"),
        )?,
        terms: if partial {
            Some((
                manifest["referencePrice"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("price missing"))?
                    .to_owned(),
                Quantity::from(
                    manifest["quantity"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("quantity missing"))?,
                ),
                if sell {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
            ))
        } else {
            None
        },
    })?;
    let cache = live.kernel().cache.clone();
    let handle = live.handle();
    let snapshot = || {
        let cache = cache.borrow();
        let orders = cache.orders(None, Some(&instrument), None, Some(&account), None);
        json!({"marketId":market_id,"marketFlags":{"isNegRisk":market.is_neg_risk,"isYieldBearing":market.is_yield_bearing},
            "journal":*journal.borrow(),"orders":orders.iter().map(|o|json!({"clientOrderId":o.client_order_id().to_string(),"venueOrderId":o.venue_order_id().map(|v|v.to_string()),"status":o.status().to_string(),"quantity":o.quantity().to_string(),"filled":o.filled_qty().to_string(),"events":o.events()})).collect::<Vec<_>>(),
            "complete":if manifest["ofiRoundTrip"] == true { journal.borrow().iter().any(|e|e["type"]=="RoundTripComplete") && orders.iter().all(|o|o.is_closed()) } else { orders.len()==case_count && orders.iter().all(|o|o.is_closed()) },
            "positions":cache.positions_open(None,Some(&instrument),None,Some(&account),None).iter().map(|p|json!({"quantity":p.quantity.to_string(),"strategyId":p.strategy_id.to_string()})).collect::<Vec<_>>(),"hasFill":orders.iter().any(|o|!o.filled_qty().is_zero())})
    };
    let monitor = async {
        for _ in 0..if manifest["watchLiveBook"] == true {
            750
        } else {
            150
        } {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let value = snapshot();
            fill_session::checkpoint(&output.join("native_checkpoint.json"), &value)?;
            if value["complete"] == true || (!partial && value["hasFill"] == true) {
                break;
            }
        }
        handle.stop();
        Ok::<(), anyhow::Error>(())
    };
    let (result, observed) = tokio::join!(live.run(), monitor);
    let value = snapshot();
    std::fs::write(
        output.join("native.json"),
        serde_json::to_vec_pretty(&value)?,
    )?;
    result?;
    observed?;
    anyhow::ensure!(
        value["complete"] == true && (partial || value["hasFill"] == false),
        "probe incomplete or unexpected fill; supervisor must verify cleanup"
    );
    Ok(())
}
