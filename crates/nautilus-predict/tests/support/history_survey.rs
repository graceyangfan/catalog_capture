// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Bounded real historical observations; no submission, removal or chain capability.

use std::{collections::BTreeSet, path::Path};

use alloy::primitives::B256;
use nautilus_predict::{
    common::contracts::PredictExchangeType,
    http::{
        PredictPrivateHttpClient,
        query::{PredictMatchesQuery, PredictOrderStatusFilter, PredictOrdersQuery},
    },
    signing::PredictOrderHasher,
};
use serde_json::{Value, json};

const FIRST: u32 = 8;
const MAX_PAGES: usize = 2;
const MAX_ORDERS: usize = 24;

fn save(output: &Path, feedback: &Value) -> anyhow::Result<()> {
    std::fs::write(output, serde_json::to_vec_pretty(feedback)?)?;
    Ok(())
}

fn phase(path: &Path, value: &str) -> anyhow::Result<()> {
    std::fs::write(path, value)?;
    Ok(())
}

pub async fn run() -> anyhow::Result<()> {
    let origin = std::env::var("PREDICT_HISTORY_READONLY_HTTP_ORIGIN")?;
    anyhow::ensure!(
        origin.starts_with("http://127.0.0.1:"),
        "write-blocking relay required"
    );
    let output = std::env::var("PREDICT_HISTORY_READONLY_FEEDBACK")?;
    let output = Path::new(&output);
    let phase_path = std::env::var("PREDICT_HISTORY_READONLY_PHASE")?;
    let phase_path = Path::new(&phase_path);
    let settings = super::operator_settings()?;
    let mut config = settings.private_config()?;
    config.base_url_http = Some(origin);
    let http = PredictPrivateHttpClient::new(config)?;
    let signer = settings.signer()?;
    let session = http.authenticate(&signer).await?;
    session.request_account().await?;
    let own = session.account_address();
    let hasher = PredictOrderHasher::new(session.environment());
    let mut feedback = json!({
        "source":"Actual own-crate read-only lifecycle survey; raw responses recorded by independent write-blocking relay",
        "orderWrites":0,"chainWrites":0,"limits":{"first":FIRST,"maxPagesPerQuery":MAX_PAGES,"maxOrderLookups":MAX_ORDERS,"maxMarkets":8},
        "pages":[],"orders":[],"markets":[],
        "limitations":["Later snapshots cannot reconstruct wallet messages or cancellation initiator", "A page cap is not complete history", "Missing match-leg hashes are not invented", "List-query filter enum is narrower than returned order status enum"]
    });
    save(output, &feedback)?;
    let mut targets = Vec::new();
    // A real historical canceled LIMIT and a second previously observed FILLED
    // atomic remainder. These are known lookups, never submitted test orders.
    let known: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/cold_start_readonly_20261016T133220Z.json"
    ))?;
    for row in known["ordersByHash"].as_array().unwrap() {
        if let Some(hash) = row["order"]["hash"].as_str() {
            targets.push(hash.parse::<B256>()?);
        }
    }
    let mut pages = Vec::new();
    for role in [None, Some(true), Some(false)] {
        let label = match role {
            None => "all",
            Some(true) => "maker",
            Some(false) => "taker",
        };
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        for index in 0..MAX_PAGES {
            phase(phase_path, &format!("matches-{label}-{index}"))?;
            let result = session
                .request_matches(&PredictMatchesQuery {
                    first: Some(FIRST),
                    after: cursor.as_deref(),
                    is_signer_maker: role,
                    ..Default::default()
                })
                .await;
            let page = match result {
                Ok(page) => page,
                Err(error) => {
                    pages.push(json!({"endpoint":"matches","role":label,"page":index,"error":error.to_string()}));
                    break;
                }
            };
            let mut legs = Vec::new();
            for event in &page.data {
                for (leg, maker) in std::iter::once((&event.taker, false))
                    .chain(event.makers.iter().map(|leg| (leg, true)))
                {
                    if leg.signer != own {
                        continue;
                    }
                    if let Some(hash) = leg.hash {
                        targets.push(hash);
                    }
                    legs.push(json!({"hash":leg.hash,"isMaker":maker,"amount":leg.amount,"price":leg.price,
                        "feeAmountWei":leg.fee.amount.to_string(),"feeType":format!("{:?}",leg.fee.fee_type),
                        "transactionHash":event.transaction_hash,"settlementId":event.settlement_id}));
                }
            }
            let stop = if page.cursor.is_none() {
                "end"
            } else if index + 1 == MAX_PAGES {
                "page_cap"
            } else if !cursors.insert(page.cursor.clone().unwrap()) {
                "repeated_cursor"
            } else {
                "continue"
            };
            pages.push(
                json!({"endpoint":"matches","role":label,"page":index,"rows":page.data.len(),
                "requestedAfter":cursor,"nextCursor":page.cursor,"stop":stop,"ownedLegs":legs}),
            );
            cursor = page.cursor;
            feedback["pages"] = json!(pages);
            save(output, &feedback)?;
            if stop != "continue" {
                break;
            }
        }
    }
    for (label, status) in [
        ("default", None),
        ("open", Some(PredictOrderStatusFilter::Open)),
        ("filled", Some(PredictOrderStatusFilter::Filled)),
    ] {
        let mut cursor = None;
        let mut cursors = BTreeSet::new();
        for index in 0..MAX_PAGES {
            phase(phase_path, &format!("orders-{label}-{index}"))?;
            let result = session
                .request_orders(&PredictOrdersQuery {
                    first: Some(FIRST),
                    after: cursor.as_deref(),
                    status,
                })
                .await;
            let page = match result {
                Ok(page) => page,
                Err(error) => {
                    pages.push(json!({"endpoint":"orders","filter":label,"page":index,"error":error.to_string()}));
                    break;
                }
            };
            for row in &page.data {
                let exchange =
                    PredictExchangeType::from_market_flags(row.is_neg_risk, row.is_yield_bearing);
                let hash = hasher.order_hash(&row.order.fields, exchange);
                anyhow::ensure!(
                    row.order.hash.is_none_or(|value| value == hash),
                    "list signed hash mismatch"
                );
                targets.push(hash);
            }
            let stop = if page.cursor.is_none() {
                "end"
            } else if index + 1 == MAX_PAGES {
                "page_cap"
            } else if !cursors.insert(page.cursor.clone().unwrap()) {
                "repeated_cursor"
            } else {
                "continue"
            };
            pages.push(json!({"endpoint":"orders","filter":label,"page":index,"rows":page.data.len(),
                "requestedAfter":cursor,"nextCursor":page.cursor,"stop":stop,"nativeHashesVerified":true}));
            cursor = page.cursor;
            feedback["pages"] = json!(pages);
            save(output, &feedback)?;
            if stop != "continue" {
                break;
            }
        }
    }
    feedback["pages"] = json!(pages);
    let mut seen = BTreeSet::new();
    targets.retain(|hash| seen.insert(*hash));
    feedback["distinctDiscoveredOrders"] = json!(targets.len());
    feedback["orderLookupCapReached"] = json!(targets.len() > MAX_ORDERS);
    let mut markets = BTreeSet::new();
    for hash in targets.into_iter().take(MAX_ORDERS) {
        phase(phase_path, "order-by-hash")?;
        let observation = match session.request_order(hash).await {
            Ok(row) => {
                let exchange =
                    PredictExchangeType::from_market_flags(row.is_neg_risk, row.is_yield_bearing);
                anyhow::ensure!(
                    hasher.order_hash(&row.order.fields, exchange) == hash,
                    "detail signed hash mismatch"
                );
                markets.insert(row.market_id);
                json!({"hash":hash,"status":format!("{:?}",row.status),"strategy":format!("{:?}",row.strategy),
                    "side":format!("{:?}",row.order.fields.side),"amount":row.amount,"amountFilled":row.amount_filled,
                    "expiration":row.order.fields.expiration.to_string(),"marketId":row.market_id,"nativeHashVerified":true})
            }
            Err(error) => {
                json!({"hash":hash,"queryError":error.to_string(),"interpretation":"No inferred outcome from lookup failure"})
            }
        };
        feedback["orders"].as_array_mut().unwrap().push(observation);
        save(output, &feedback)?;
    }
    for market in markets.into_iter().take(8) {
        phase(phase_path, "market-lifecycle")?;
        let observation = match session.request_execution_market(market).await {
            Ok(row) => {
                json!({"id":market,"tradingStatus":row.trading_status,"feeRateBps":row.fee_rate_bps})
            }
            Err(error) => json!({"id":market,"queryError":error.to_string()}),
        };
        feedback["markets"]
            .as_array_mut()
            .unwrap()
            .push(observation);
        save(output, &feedback)?;
    }
    Ok(())
}
