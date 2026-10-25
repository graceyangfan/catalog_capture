// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Public chain evidence, decoded independently of Python; no credentials or network.
#![cfg(feature = "execution")]

use alloy::{
    primitives::{Address, B256, Bytes, U256},
    sol,
    sol_types::{SolCall, SolEvent},
};
use nautilus_predict::{
    common::{
        contracts::PredictExchangeType,
        enums::{PredictOrderSide, PredictSignatureType},
        environment::PredictEnvironment,
    },
    http::order_models::PredictOrder,
    signing::PredictOrderHasher,
};
use serde_json::Value;

sol! {
    // Canonical signed Order layout from official predict-sdk 0.0.22 ABI.
    struct ChainOrder {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
        bytes signature;
    }
    // Decode only the first two argument slots; this does not assert the router's
    // function selector or unknown trailing fee/authorization argument types.
    function decodeOrdersPrefix(ChainOrder takerOrder, ChainOrder[] makerOrders);
    event OrderFilled(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        uint256 makerAssetId,
        uint256 takerAssetId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee
    );
    event TransferSingle(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256 id,
        uint256 value
    );
}

fn recording() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/private_api/live_chain_atomic_rounding_20261025.json"
    ))
    .unwrap()
}

fn hash(order: &ChainOrder) -> B256 {
    let fields = PredictOrder {
        salt: order.salt,
        maker: order.maker,
        signer: order.signer,
        taker: order.taker,
        token_id: order.tokenId,
        maker_amount: order.makerAmount,
        taker_amount: order.takerAmount,
        expiration: order.expiration,
        nonce: order.nonce,
        fee_rate_bps: order.feeRateBps,
        side: PredictOrderSide::try_from(order.side).unwrap(),
        signature_type: PredictSignatureType::try_from(order.signatureType).unwrap(),
    };
    PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(&fields, PredictExchangeType::Standard)
}

fn decoded(tx: &Value) -> (decodeOrdersPrefixCall, Vec<OrderFilled>) {
    let input: Bytes = tx["transaction"]["input"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let orders = decodeOrdersPrefixCall::abi_decode_raw(&input[4..]).unwrap();
    let exchange = PredictExchangeType::Standard.contract(PredictEnvironment::BnbMainnet);
    let fills = tx["receipt"]["logs"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|raw| {
            let address: Address = raw["address"].as_str().unwrap().parse().unwrap();
            let topics = raw["topics"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap().parse::<B256>().unwrap())
                .collect::<Vec<_>>();
            if address != exchange || topics.first() != Some(&OrderFilled::SIGNATURE_HASH) {
                return None;
            }
            let data: Bytes = raw["data"].as_str().unwrap().parse().unwrap();
            Some(OrderFilled::decode_raw_log(topics, &data).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(
        fills.len(),
        2,
        "one counterparty and one taker fill; summary is not an extra fill"
    );
    assert_eq!(tx["receipt"]["status"], "0x1");
    assert_eq!(tx["receipt"]["transactionHash"], tx["hash"]);
    (orders, fills)
}

#[test]
fn public_counterparty_splits_reproduce_the_one_wei_share_shortfall() {
    let saved = recording();
    let rows = saved["rpcCapture"]["transactions"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let mut gross = U256::ZERO;
    let mut cash = U256::ZERO;
    let mut fees = U256::ZERO;
    let mut maker_cash = U256::ZERO;
    let mut fractions = U256::ZERO;
    let requested = U256::from(7_650_000_000_000_000_000u64);
    for row in rows {
        let (orders, fills) = decoded(row);
        assert_eq!(orders.makerOrders.len(), 1);
        let maker = &orders.makerOrders[0];
        let taker = &orders.takerOrder;
        let ours = fills.iter().find(|f| f.orderHash == hash(taker)).unwrap();
        let theirs = fills.iter().find(|f| f.orderHash == hash(maker)).unwrap();
        assert_eq!(taker.takerAmount, requested);
        assert_eq!(maker.side, 0);
        assert_eq!(taker.side, 0);
        assert_eq!(
            maker.makerAmount * U256::from(100),
            maker.takerAmount * U256::from(74)
        );
        assert_eq!(theirs.makerAssetId, U256::ZERO);
        assert_eq!(ours.makerAssetId, U256::ZERO);
        assert_ne!(theirs.takerAssetId, ours.takerAssetId);
        assert_eq!(theirs.takerAmountFilled, ours.takerAmountFilled);
        assert_eq!(
            theirs.makerAmountFilled + ours.makerAmountFilled,
            ours.takerAmountFilled
        );
        let shares = theirs.makerAmountFilled * maker.takerAmount / maker.makerAmount;
        assert_eq!(shares, theirs.takerAmountFilled);
        let numerator = theirs.makerAmountFilled * U256::from(50);
        assert_eq!(numerator / U256::from(37), shares);
        fractions += numerator % U256::from(37);
        assert_eq!(
            ours.fee,
            ours.takerAmountFilled * U256::from(200) / U256::from(10_000)
        );
        gross += ours.takerAmountFilled;
        cash += ours.makerAmountFilled;
        fees += ours.fee;
        maker_cash += theirs.makerAmountFilled;
    }
    assert_eq!(fractions, U256::from(37));
    assert_eq!(maker_cash * U256::from(50) / U256::from(37), requested);
    assert_eq!(gross, requested - U256::from(1));
    assert_eq!(
        cash,
        U256::from(1_989_000_000_000_000_000u64) - U256::from(1)
    );
    assert_eq!(fees, U256::from(152_999_999_999_999_999u64));
    assert_eq!(gross - fees, U256::from(7_497_000_000_000_000_000u64));
}

#[test]
fn chain_remaining_is_maker_asset_and_cannot_override_clob_filled() {
    let saved = recording();
    let first = &saved["rpcCapture"]["transactions"][0];
    let (orders, fills) = decoded(first);
    let snapshots = saved["statusSnapshots"]["calls"].as_array().unwrap();
    let latest = |hash: B256| {
        snapshots
            .iter()
            .find(|row| row["orderHash"] == format!("{hash:#x}") && row["params"][1] == "latest")
            .unwrap()
    };
    let maker = &orders.makerOrders[0];
    let theirs = fills
        .iter()
        .find(|fill| fill.orderHash == hash(maker))
        .unwrap();
    let snapshot = latest(hash(maker));
    let result: Bytes = snapshot["response"]["result"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(result.len(), 64);
    let remaining = U256::from_be_slice(&result[32..]);
    assert_eq!(remaining, maker.makerAmount - theirs.makerAmountFilled);
    assert_ne!(remaining, maker.takerAmount - theirs.takerAmountFilled);
    assert_eq!(U256::from_be_slice(&result[..32]), U256::ZERO);

    let ours = latest(hash(&orders.takerOrder));
    let result: Bytes = ours["response"]["result"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(result.len(), 64);
    assert_eq!(U256::from_be_slice(&result[..32]), U256::ZERO);
    assert_eq!(U256::from_be_slice(&result[32..]), U256::from(1));
    let history: Value = serde_json::from_str(include_str!(
        "fixtures/private_api/live_market_history_readonly_20261025.json"
    ))
    .unwrap();
    let rest = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| {
            event["responseBody"]["data"]["order"]["hash"]
                == format!("{:#x}", hash(&orders.takerOrder))
        })
        .unwrap();
    assert_eq!(rest["responseBody"]["data"]["status"], "FILLED");
    assert_eq!(
        rest["responseBody"]["data"]["amountFilled"],
        "7649999999999999999"
    );
    assert_eq!(saved["writes"], 0);
}

#[test]
fn counterparty_protocol_fee_refunds_are_not_actual_inventory_commission() {
    let saved = recording();
    let ctf = PredictExchangeType::Standard.conditional_tokens(PredictEnvironment::BnbMainnet);
    for tx in saved["rpcCapture"]["transactions"].as_array().unwrap() {
        let (orders, fills) = decoded(tx);
        for order in [&orders.takerOrder, &orders.makerOrders[0]] {
            let fill = fills
                .iter()
                .find(|fill| fill.orderHash == hash(order))
                .unwrap();
            let mut incoming = U256::ZERO;
            let mut outgoing = U256::ZERO;
            let mut inflows = Vec::new();
            for raw in tx["receipt"]["logs"].as_array().unwrap() {
                let address: Address = raw["address"].as_str().unwrap().parse().unwrap();
                let topics = raw["topics"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().parse::<B256>().unwrap())
                    .collect::<Vec<_>>();
                if address != ctf || topics.first() != Some(&TransferSingle::SIGNATURE_HASH) {
                    continue;
                }
                let data: Bytes = raw["data"].as_str().unwrap().parse().unwrap();
                let transfer = TransferSingle::decode_raw_log(topics, &data).unwrap();
                if transfer.id != order.tokenId {
                    continue;
                }
                if transfer.to == order.maker {
                    incoming += transfer.value;
                    inflows.push(transfer.value);
                }
                if transfer.from == order.maker {
                    outgoing += transfer.value;
                }
            }
            assert!(fill.fee > U256::ZERO);
            let net = incoming - outgoing;
            if hash(order) == hash(&orders.takerOrder) {
                assert_eq!(net, fill.takerAmountFilled - fill.fee);
                assert_eq!(inflows.len(), 1);
            } else {
                assert_eq!(net, fill.takerAmountFilled);
                assert_eq!(inflows.len(), 2);
                assert_eq!(
                    inflows[1], fill.fee,
                    "later transfer returns the protocol fee"
                );
            }
        }
    }
}
