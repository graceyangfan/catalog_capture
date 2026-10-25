// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Opt-in validation capture through this crate's signer, DTOs and private HTTP transport.
//! Invalid outer options use the private transport only here; production admission is unchanged.

use std::{path::Path, sync::Arc, time::Duration};

use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    common::{
        contracts::PredictExchangeType,
        enums::{PredictOrderSide, PredictSignatureType},
        market::PredictMarketOrderAmounts,
    },
    http::order_models::PredictOrder,
    signing::PredictOrderSigner,
    websocket::{
        private_client::PredictWalletWebSocketClient, private_messages::PredictWalletStreamMessage,
    },
};

const SCALE: u64 = 1_000_000_000_000_000_000;

fn cases() -> Vec<(&'static str, bool, &'static str, Value)> {
    vec![
        ("limit_fok", false, "isFillOrKill", json!(true)),
        ("market_post_only", true, "isPostOnly", json!(true)),
        ("limit_slippage", false, "slippageBps", json!("100")),
        (
            "market_slippage_out_of_range",
            true,
            "slippageBps",
            json!("10000"),
        ),
        (
            "limit_amounts_mismatch",
            false,
            "pricePerShare",
            json!("20000000000000000"),
        ),
        ("market_amounts_mismatch", true, "slippageBps", json!("100")),
    ]
}

fn amounts() -> PredictMarketOrderAmounts {
    PredictMarketOrderAmounts::from_price(
        PredictOrderSide::Buy,
        U256::from(100) * U256::from(SCALE),
        U256::from(SCALE / 100),
        0,
    )
    .unwrap()
}

fn expired_guard(body: &Value, signed: &PredictSignedOrder) -> anyhow::Result<()> {
    anyhow::ensure!(
        signed.order().expiration == U256::from(1)
            && signed.order().side == PredictOrderSide::Buy
            && signed.order().maker_amount == U256::from(SCALE)
            && signed.order().taker_amount == U256::from(100) * U256::from(SCALE)
            && body["data"]["order"] == serde_json::to_value(signed.contract_order()?)?,
        "probe requires unchanged, signed expiration=1 and fixed BUY economics"
    );
    Ok(())
}

fn body(signed: &PredictSignedOrder, market: bool) -> anyhow::Result<Value> {
    let request = if market {
        PredictCreateOrderRequest::market(signed, &amounts())?
    } else {
        PredictCreateOrderRequest::limit(signed, U256::from(SCALE / 100), false)?
    };
    Ok(serde_json::to_value(request)?)
}

fn read_private<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let metadata = std::fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "operator TOML permissions must be 600"
        );
    }
    anyhow::ensure!(metadata.is_file(), "operator TOML must be a file");
    let content: SecretString = std::fs::read_to_string(path)?.into();
    toml::from_str(content.expose_secret())
        .map_err(|_| anyhow::anyhow!("invalid operator TOML; source withheld"))
}

fn redact(value: Value, secrets: &[String]) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter_map(|(key, item)| {
                    if matches!(
                        key.to_ascii_lowercase().as_str(),
                        "token" | "jwt" | "private_key"
                    ) {
                        return None;
                    }
                    let item = if key.eq_ignore_ascii_case("signature") {
                        json!("[redacted-signature]")
                    } else {
                        redact(item, secrets)
                    };
                    Some((key, item))
                })
                .collect(),
        ),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|v| redact(v, secrets)).collect())
        }
        Value::String(mut text) => {
            for secret in secrets {
                if !secret.is_empty() {
                    text = text.replace(secret, "[redacted]");
                }
            }
            Value::String(text)
        }
        other => other,
    }
}

fn save(path: &Path, evidence: &Value, secrets: &[String]) -> anyhow::Result<()> {
    use std::io::Write;
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(&redact(
        evidence.clone(),
        secrets,
    ))?)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    Ok(())
}

#[test]
fn parameter_probe_cannot_change_signed_terms_or_remove_expiration() {
    let wallet = Arc::new(
        PredictAuthSigner::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
            PredictEnvironment::BnbMainnet,
            None,
        )
        .unwrap(),
    );
    let order = PredictOrder {
        salt: U256::from(1),
        maker: wallet.account_address(),
        signer: wallet.account_address(),
        taker: Address::ZERO,
        token_id: U256::from(11),
        maker_amount: U256::from(SCALE),
        taker_amount: U256::from(100) * U256::from(SCALE),
        expiration: U256::from(1),
        nonce: U256::ZERO,
        fee_rate_bps: U256::from(200),
        side: PredictOrderSide::Buy,
        signature_type: PredictSignatureType::Eoa,
    };
    let signer = PredictOrderSigner::new(wallet);
    let signed = signer
        .sign_order(&order, PredictExchangeType::Standard)
        .unwrap();
    for (_, market, key, value) in cases() {
        let mut request = body(&signed, market).unwrap();
        request["data"][key] = value;
        expired_guard(&request, &signed).unwrap();
        for (field, value) in [
            ("expiration", json!(0)),
            ("expiration", json!(4_102_444_800u64)),
            ("makerAmount", json!("1")),
            ("signature", json!("0x00")),
        ] {
            let mut changed = request.clone();
            changed["data"]["order"][field] = value;
            assert!(expired_guard(&changed, &signed).is_err());
        }
    }
    assert!(PredictCreateOrderRequest::limit(&signed, U256::from(SCALE / 50), false).is_err());
    let mut invalid = amounts();
    invalid.slippage_bps = 100;
    assert!(PredictCreateOrderRequest::market(&signed, &invalid).is_err());
    for expiry in [0, 4_102_444_800u64] {
        let mut changed = order;
        changed.expiration = U256::from(expiry);
        let changed = signer
            .sign_order(&changed, PredictExchangeType::Standard)
            .unwrap();
        let request = body(&changed, false).unwrap();
        assert!(expired_guard(&request, &changed).is_err());
    }
}

#[test]
fn parameter_probe_redacts_signatures_and_embedded_operator_values() {
    let secret = "operator-secret-sentinel".to_owned();
    let saved = redact(
        json!({
            "data":{"order":{"signature":"never-persist-this-signature","expiration":1}},
            "message":format!("echo {secret}"),"token":"never-persist-this-token",
        }),
        std::slice::from_ref(&secret),
    );
    let encoded = saved.to_string();
    for forbidden in [
        secret.as_str(),
        "never-persist-this-signature",
        "never-persist-this-token",
    ] {
        assert!(!encoded.contains(forbidden));
    }
    assert_eq!(saved["data"]["order"]["expiration"], 1);
}

#[tokio::test]
#[ignore = "explicit operator authorization only: mainnet parameter negatives with immutable signed expiration=1"]
async fn mainnet_signed_expired_parameter_feedback() -> anyhow::Result<()> {
    anyhow::ensure!(
        std::env::var("PREDICT_RECORD_EXPIRED_PARAMETERS").as_deref() == Ok("1"),
        "explicit expired-only probe gate required"
    );
    #[derive(Deserialize)]
    struct Settings {
        network: Network,
        privy: Privy,
    }
    #[derive(Deserialize)]
    struct Network {
        environment: String,
        api_key_file: std::path::PathBuf,
        timeout_secs: u64,
    }
    #[derive(Deserialize)]
    struct Privy {
        mode: String,
        private_key: SecretString,
        predict_account: String,
        owner_address: String,
    }
    #[derive(Deserialize)]
    struct Key {
        api_key: SecretString,
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let settings: Settings = read_private(&root.join("env/predict-execution.toml"))?;
    anyhow::ensure!(
        settings.network.environment == "mainnet" && settings.privy.mode == "predict_account",
        "expected mainnet Predict Account settings"
    );
    let key_path = if settings.network.api_key_file.is_absolute() {
        settings.network.api_key_file
    } else {
        root.join("env").join(settings.network.api_key_file)
    };
    let key: Key = read_private(&key_path)?;
    let wallet = Arc::new(PredictAuthSigner::new(
        settings.privy.private_key.clone(),
        PredictEnvironment::BnbMainnet,
        Some(&settings.privy.predict_account),
    )?);
    anyhow::ensure!(
        wallet.owner_address() == settings.privy.owner_address.parse::<Address>()?,
        "owner guard mismatch"
    );
    let transport = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        environment: PredictEnvironment::BnbMainnet,
        api_key: Some(key.api_key.clone()),
        base_url_http: None,
        http_timeout_secs: settings.network.timeout_secs.min(15),
    })?;
    let session = transport.authenticate(&wallet).await?;
    session.request_account().await?;
    let mut secrets = vec![
        settings.privy.private_key.expose_secret().to_owned(),
        key.api_key.expose_secret().to_owned(),
        session.token().expose_secret().to_owned(),
        settings.privy.predict_account.clone(),
        settings.privy.owner_address.clone(),
        wallet.account_address().to_string(),
        wallet.owner_address().to_string(),
    ];
    let response = session
        .get(
            "/v1/markets",
            "/v1/markets",
            Some(&[("first", "100"), ("status", "OPEN")]),
        )
        .await?;
    let listing: Value = serde_json::from_slice(&response.body)?;
    let market = listing["data"]
        .as_array()
        .and_then(|items| {
            items.iter().find(|m| {
                m["tradingStatus"] == "OPEN"
                    && ["bitcoin", "ethereum", "bnb"]
                        .iter()
                        .any(|s| m.to_string().to_ascii_lowercase().contains(s))
            })
        })
        .ok_or_else(|| anyhow::anyhow!("no open crypto candidate; no orders sent"))?;
    let token = market["outcomes"]
        .as_array()
        .and_then(|items| items.iter().find(|o| o["indexSet"] == 1))
        .and_then(|o| o["onChainId"].as_str())
        .ok_or_else(|| anyhow::anyhow!("outcome token unavailable"))?
        .parse::<U256>()?;
    let exchange = PredictExchangeType::from_market_flags(
        market["isNegRisk"]
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("missing market domain"))?,
        market["isYieldBearing"]
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("missing market domain"))?,
    );
    let book = session
        .get(
            "/v1/markets/{id}/orderbook",
            &format!("/v1/markets/{}/orderbook", market["id"]),
            None::<&PredictOrdersQuery<'_>>,
        )
        .await?;
    let book: Value = serde_json::from_slice(&book.body)?;
    let signer = PredictOrderSigner::new(wallet.clone());
    let cancel = CancellationToken::new();
    let _cancel_on_error = cancel.clone().drop_guard();
    let mut ws = PredictWalletWebSocketClient::connect(
        crate::PredictWalletWebSocketConfig {
            environment: PredictEnvironment::BnbMainnet,
            api_key: Some(key.api_key),
            ..Default::default()
        },
        &session,
        cancel.clone(),
    )
    .await?;
    ws.wait_until_subscribed().await?;
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader_events = observed.clone();
    let reader = tokio::spawn(async move {
        while let Some(message) = ws.next_message().await? {
            if let PredictWalletStreamMessage::Wallet { event, .. } = message {
                reader_events
                    .lock()
                    .unwrap()
                    .push((event.order.order_hash, format!("{event:?}")));
            }
        }
        ws.close().await
    });
    let output = root.join("data/predict-validation").join(format!(
        "crate_signed_parameters_{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%S%fZ")
    ));
    std::fs::create_dir(&output)?;
    let path = output.join("capture.json");
    let mut evidence = json!({"source":"Actual mainnet: crate signer/DTO/private HTTP parameter probe; signed expiration=1",
        "strictZeroFill":true,"signedExpiration":1,"nativeStrategySubmission":false,"testOnlyOuterOptionInjection":true,
        "market":market,"bookBefore":book,"cases":[],"walletEvents":[],"restByHash":[],
        "limitations":["Expired signed orders cannot establish live matching behavior; expiry may mask target parameter checks.",
                        "Wallet observation is a parsed diagnostic projection; HTTP responses retain original JSON fields."]});
    save(&path, &evidence, &secrets)?;
    let mut hashes = Vec::new();
    let seed = nautilus_core::time::get_atomic_clock_realtime()
        .get_time_ns()
        .as_u64();
    let mut masked_count = 0;
    for (index, (name, is_market, field, value)) in cases().into_iter().enumerate() {
        let order = PredictOrder {
            salt: U256::from(seed) + U256::from(index),
            maker: wallet.account_address(),
            signer: wallet.account_address(),
            taker: Address::ZERO,
            token_id: token,
            maker_amount: U256::from(SCALE),
            taker_amount: U256::from(100) * U256::from(SCALE),
            expiration: U256::from(1),
            nonce: U256::ZERO,
            fee_rate_bps: U256::from(
                market["feeRateBps"]
                    .as_u64()
                    .ok_or_else(|| anyhow::anyhow!("missing fee"))?,
            ),
            side: PredictOrderSide::Buy,
            signature_type: PredictSignatureType::Eoa,
        };
        let signed = signer.sign_order(&order, exchange)?;
        secrets.push(signed.signature().expose_secret().to_owned());
        let mut request = body(&signed, is_market)?;
        request["data"][field] = value;
        expired_guard(&request, &signed)?;
        hashes.push(signed.hash());
        evidence["cases"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":name,"request":request,"transmissionIntent":true}));
        save(&path, &evidence, &secrets)?;
        // Test-only malformed outer options. This is exactly the crate's production
        // secret-body HTTP POST transport, with no retry and no exported bypass API.
        let response = session.post(ORDERS, &request).await?;
        let payload: Value = serde_json::from_slice(&response.body)?;
        let masked = matches!(
            payload["error"].as_str(),
            Some(
                "create_order_expiry_too_soon"
                    | "create_order_contract_order_expired"
                    | "create_order_invalid_expiration_timestamp"
            )
        );
        evidence["cases"][index]["httpStatus"] = json!(response.status.as_u16());
        evidence["cases"][index]["response"] = payload.clone();
        evidence["cases"][index]["targetMaskedByExpiry"] = json!(masked);
        let decoded = decode_response::<PredictCreateOrderReceipt>(ORDERS, &response);
        evidence["cases"][index]["crateResult"] =
            json!(decoded.as_ref().err().map(ToString::to_string));
        save(&path, &evidence, &secrets)?;
        if response.status.as_u16() != 400 || payload["success"] != false {
            if let Some(id) = payload["data"]["orderId"].as_str() {
                let cleanup = session.request_remove_orders(&[id.parse()?]).await;
                evidence["cleanup"] = match cleanup {
                    Ok(receipt) => {
                        json!({"removed":receipt.removed,"noop":receipt.noop,"rejected":receipt.rejected})
                    }
                    Err(error) => json!({"error":error.to_string()}),
                };
            }
            evidence["unexpectedResponse"] = json!(true);
            save(&path, &evidence, &secrets)?;
            break;
        }
        masked_count = if masked { masked_count + 1 } else { 0 };
        if masked_count >= 2 {
            evidence["stoppedBecause"] = json!(
                "two consecutive targets masked by expiry; no change to signed safety boundary"
            );
            break;
        }
    }
    for hash in &hashes {
        let response = session
            .get(
                ORDER,
                &format!("/v1/orders/{hash:#x}"),
                None::<&PredictOrdersQuery<'_>>,
            )
            .await?;
        evidence["restByHash"].as_array_mut().unwrap().push(json!({"hash":hash,"httpStatus":response.status.as_u16(),"response":serde_json::from_slice::<Value>(&response.body)?}));
        save(&path, &evidence, &secrets)?;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    cancel.cancel();
    reader.await??;
    evidence["walletEvents"] = json!(
        observed
            .lock()
            .unwrap()
            .iter()
            .filter(|(hash, _)| hashes.contains(hash))
            .map(|(hash, diagnostic)| json!({"orderHash":hash,"parsedDiagnostic":diagnostic}))
            .collect::<Vec<_>>()
    );
    evidence["submittedRequests"] = json!(hashes.len());
    evidence["completedAt"] = json!(chrono::Utc::now().to_rfc3339());
    save(&path, &evidence, &secrets)?;
    println!("Expired-only crate capture: {}", path.display());
    anyhow::ensure!(
        evidence.get("unexpectedResponse").is_none(),
        "unexpected response; see recorded evidence"
    );
    anyhow::ensure!(
        evidence["walletEvents"].as_array().unwrap().is_empty(),
        "unexpected wallet lifecycle; inspect capture"
    );
    anyhow::ensure!(
        evidence["restByHash"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["httpStatus"] == 404),
        "order hash did not return not-found"
    );
    Ok(())
}
