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

//! Authentication, private queries and explicit order requests using Nautilus HTTP transport.
//!
//! Account-scoped sessions share token rotation across clones and isolate independent accounts.
//! A 401/403 is returned to the caller; token refresh and operation retries are explicit.
//! Requests run in the caller's future without adapter background tasks. Dropping the future
//! stops waiting; an already transmitted request cannot be recalled.

use std::{collections::HashMap, fmt};

use alloy::primitives::{Address, B256, U256};
use nautilus_core::string::secret::SecretString;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, HttpResponse, Method, create_standard_nautilus_headers,
};
use serde::Serialize;

use super::{
    account_models::{PredictMatchData, PredictOrderData, PredictPage, PredictPositionData},
    command_models::{
        PredictCreateOrderReceipt, PredictCreateOrderRequest, PredictRemoveOrdersReceipt,
    },
    error::{
        Error, PredictDecodeErrorKind, Result, check_response_status, decode_page, decode_response,
    },
    execution_models::{
        PredictAccount, PredictAuthMessage, PredictAuthRequest, PredictAuthTokenData,
    },
    order_models::PredictSignedOrder,
    query::{
        PredictAccountMatchesQuery, PredictMatchesQuery, PredictOrdersQuery, PredictPositionsQuery,
        validate_first,
    },
};
use crate::{
    common::{credential::PredictAuthToken, environment::PredictEnvironment},
    config::PredictPrivateClientConfig,
    signing::PredictAuthSigner,
};

const AUTH_MESSAGE: &str = "/v1/auth/message";
const AUTH: &str = "/v1/auth";
const ACCOUNT: &str = "/v1/account";
const ORDERS: &str = "/v1/orders";
const ORDER: &str = "/v1/orders/{hash}";
const POSITIONS: &str = "/v1/positions";
const MATCHES: &str = "/v1/orders/matches";
const REMOVE_ORDERS: &str = "/v1/orders/remove";

#[cfg(test)]
#[path = "../../tests/unit/parameter_probe.rs"]
mod parameter_probe;

/// Private REST transport, independent of the public recording client.
#[derive(Clone)]
pub struct PredictPrivateHttpClient {
    environment: PredictEnvironment,
    base_url: String,
    api_key: Option<SecretString>,
    client: HttpClient,
}

impl PredictPrivateHttpClient {
    /// Creates private transport bound to an explicit environment.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration is invalid or the HTTP client cannot be built.
    pub fn new(config: PredictPrivateClientConfig) -> Result<Self> {
        let base_url = config.validated_http_url()?;
        let client = HttpClient::builder()
            .headers(create_standard_nautilus_headers().into_iter().collect())
            .header_keys(vec!["retry-after".into()])
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(config.http_timeout_secs)
            .build()
            .map_err(|e| Error::from_http_client("HTTP client initialization", e))?;
        Ok(Self {
            environment: config.environment,
            base_url,
            api_key: config.api_key,
            client,
        })
    }

    /// Fetches the exact dynamic challenge text from the venue.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response has no valid challenge.
    pub async fn request_auth_message(&self) -> Result<String> {
        let response = self
            .client
            .request(
                Method::GET,
                format!("{}{AUTH_MESSAGE}", self.base_url),
                None,
                Some(self.headers(None)),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| Error::from_http_client(AUTH_MESSAGE, e))?;
        let data: PredictAuthMessage = decode_response(AUTH_MESSAGE, &response)?;
        if data.message.is_empty() {
            return Err(Error::decode(
                AUTH_MESSAGE,
                PredictDecodeErrorKind::InvalidChallenge,
            ));
        }
        Ok(data.message)
    }

    /// Exchanges a signed challenge. Also usable with an externally managed wallet signer.
    ///
    /// # Errors
    ///
    /// Returns an error if the signed request is invalid, rejected, or returns an unusable token.
    pub async fn request_auth_token(
        &self,
        request: &PredictAuthRequest,
    ) -> Result<PredictAuthToken> {
        if request.signer.is_zero()
            || request.message.is_empty()
            || request.signature.expose_secret().is_empty()
        {
            return Err(Error::BadRequest(
                "auth request requires signer, message and signature",
            ));
        }
        let body: SecretString = serde_json::to_string(request)
            .map_err(|_| Error::BadRequest("failed to encode auth request"))?
            .into();
        let mut headers = self.headers(None);
        headers.insert("Content-Type".into(), "application/json".into());
        let response = self
            .client
            .request_with_secret_body(
                Method::POST,
                format!("{}{AUTH}", self.base_url),
                None,
                Some(headers),
                body,
                None,
                None,
            )
            .await
            .map_err(|e| Error::from_http_client(AUTH, e))?;
        let data: PredictAuthTokenData = decode_response(AUTH, &response)?;
        PredictAuthToken::new(data.token)
            .map_err(|_| Error::decode(AUTH, PredictDecodeErrorKind::InvalidToken))
    }

    /// Fetches the dynamic challenge, signs its exact text, and creates a private session.
    ///
    /// # Errors
    ///
    /// Returns an error if signer and transport environments differ, signing fails, or authentication fails.
    pub async fn authenticate(
        &self,
        signer: &PredictAuthSigner,
    ) -> Result<PredictAuthenticatedHttpClient> {
        if signer.environment() != self.environment {
            return Err(Error::Configuration(
                "signing environment differs from private transport",
            ));
        }
        let message = self.request_auth_message().await?;
        let signature = signer.sign_auth_message(&message)?;
        let request = PredictAuthRequest {
            signer: signer.account_address(),
            message,
            signature,
        };
        let token = self.request_auth_token(&request).await?;
        self.authenticated(token, signer.account_address())
    }

    /// Binds an externally acquired token to the expected wallet identity.
    /// `request_account` verifies that the venue returns the same address.
    ///
    /// # Errors
    ///
    /// Returns an error if the expected account address is zero.
    pub fn authenticated(
        &self,
        token: PredictAuthToken,
        account_address: Address,
    ) -> Result<PredictAuthenticatedHttpClient> {
        if account_address.is_zero() {
            return Err(Error::BadRequest(
                "authenticated account address cannot be zero",
            ));
        }
        Ok(PredictAuthenticatedHttpClient {
            transport: self.clone(),
            token: std::sync::Arc::new(tokio::sync::watch::Sender::new(token)),
            account_address,
            identity_verified: Default::default(),
        })
    }

    fn headers(&self, token: Option<&PredictAuthToken>) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(key) = &self.api_key {
            headers.insert("x-api-key".into(), key.expose_secret().to_string());
        }
        if let Some(token) = token {
            headers.insert(
                "Authorization".into(),
                format!("Bearer {}", token.expose_secret()),
            );
        }
        headers
    }
}

impl fmt::Debug for PredictPrivateHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredictPrivateHttpClient")
            .field("environment", &self.environment)
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key)
            .finish_non_exhaustive()
    }
}

/// Account-scoped bearer session with shared token rotation across clones.
#[derive(Clone, Debug)]
pub struct PredictAuthenticatedHttpClient {
    transport: PredictPrivateHttpClient,
    token: std::sync::Arc<tokio::sync::watch::Sender<PredictAuthToken>>,
    account_address: Address,
    // Session identity is immutable. Replacement JWTs are verified before publication.
    identity_verified: std::sync::Arc<tokio::sync::OnceCell<()>>,
}

impl PredictAuthenticatedHttpClient {
    /// Reads lifecycle metadata without loading an instrument into Native.
    pub(crate) async fn request_market_definition(
        &self,
        market_id: u64,
    ) -> Result<super::models::PredictMarket> {
        const ENDPOINT: &str = "/v1/markets/{id}";
        let response = self
            .get(ENDPOINT, &format!("/v1/markets/{market_id}"), None::<&()>)
            .await?;
        let market: super::models::PredictMarket = decode_response(ENDPOINT, &response)?;
        if market.id != market_id {
            return Err(Error::BadRequest(
                "market lifecycle identity differs from request",
            ));
        }
        Ok(market)
    }

    /// Fetches required signing metadata, including fees and exchange flags.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identity, fees, metadata or request failure.
    pub async fn request_execution_market(
        &self,
        market_id: u64,
    ) -> Result<super::market_models::PredictExecutionMarket> {
        const ENDPOINT: &str = "/v1/markets/{id}";
        if market_id == 0 || market_id > i64::MAX as u64 {
            return Err(Error::BadRequest("invalid execution market ID"));
        }
        let response = self
            .get(ENDPOINT, &format!("/v1/markets/{market_id}"), None::<&()>)
            .await?;
        let market: super::market_models::PredictExecutionMarket =
            decode_response(ENDPOINT, &response)?;
        if market.id != market_id || market.fee_rate_bps > 10_000 {
            return Err(Error::BadRequest(
                "execution market identity or fee is invalid",
            ));
        }
        Ok(market)
    }

    /// Fetches the REST YES orderbook for the requested market.
    ///
    /// # Errors
    ///
    /// Rejects malformed books and non-success envelopes; callers validate market and timestamp.
    pub async fn request_execution_orderbook(
        &self,
        market_id: u64,
    ) -> Result<super::execution_models::PredictExecutionOrderbook> {
        const ENDPOINT: &str = "/v1/markets/{id}/orderbook";
        if market_id == 0 || market_id > i64::MAX as u64 {
            return Err(Error::BadRequest("invalid execution market ID"));
        }
        let response = self
            .get(
                ENDPOINT,
                &format!("/v1/markets/{market_id}/orderbook"),
                None::<&()>,
            )
            .await?;
        decode_response(ENDPOINT, &response)
    }

    /// Sends one immutable signed LIMIT request exactly once. A receipt is not a fill or a
    /// Nautilus acceptance event. A timeout, server error or malformed acknowledgement leaves
    /// the submission outcome unknown; reconcile by the locally computed hash before retrying.
    ///
    /// # Errors
    ///
    /// Invalid local context, HTTP failures and inconsistent acknowledgement hashes return errors.
    pub async fn request_create_limit_order(
        &self,
        signed: &PredictSignedOrder,
        price_per_share_wei: U256,
        is_post_only: bool,
    ) -> Result<PredictCreateOrderReceipt> {
        self.request_create_limit_order_with_options(
            signed,
            price_per_share_wei,
            super::command_models::PredictLimitOrderOptions {
                is_post_only,
                self_trade_prevention: None,
            },
        )
        .await
    }

    /// Sends one LIMIT request with typed venue options. No automatic POST retry.
    ///
    /// # Errors
    ///
    /// Invalid terms, context, HTTP responses or acknowledgement hashes return errors.
    pub async fn request_create_limit_order_with_options(
        &self,
        signed: &PredictSignedOrder,
        price_per_share_wei: U256,
        options: super::command_models::PredictLimitOrderOptions,
    ) -> Result<PredictCreateOrderReceipt> {
        let request =
            PredictCreateOrderRequest::limit_with_options(signed, price_per_share_wei, options)?;
        self.request_create_order(signed, &request).await
    }

    /// Sends one MARKET FOK request, without retrying.
    /// A receipt proves neither FOK enforcement nor zero execution. Reconcile by hash.
    /// Native submission selects this path from TimeInForce::Fok, never command params.
    ///
    /// # Errors
    ///
    /// Invalid terms, mismatched environments and HTTP/receipt failures return errors.
    pub async fn request_create_market_fok_order(
        &self,
        signed: &PredictSignedOrder,
        amounts: &crate::common::market::PredictMarketOrderAmounts,
    ) -> Result<PredictCreateOrderReceipt> {
        let request = PredictCreateOrderRequest::market_fok(signed, amounts)?;
        self.request_create_order(signed, &request).await
    }

    /// Sends the quantity-based MARKET envelope once, preserving signed amounts.
    /// Reconcile ambiguous outcomes by order hash; this method does not retry POSTs.
    ///
    /// # Errors
    ///
    /// Invalid signed context or metadata, transport failures and mismatched receipts fail.
    /// Ambiguous results must be reconciled by hash, never retried blindly.
    pub async fn request_create_market_order(
        &self,
        signed: &PredictSignedOrder,
        amounts: &crate::common::market::PredictMarketOrderAmounts,
    ) -> Result<PredictCreateOrderReceipt> {
        let request = PredictCreateOrderRequest::market(signed, amounts)?;
        self.request_create_order(signed, &request).await
    }

    async fn request_create_order(
        &self,
        signed: &PredictSignedOrder,
        request: &PredictCreateOrderRequest<'_>,
    ) -> Result<PredictCreateOrderReceipt> {
        if signed.environment() != self.environment() {
            return Err(Error::BadRequest(
                "signed order and REST environments differ",
            ));
        }
        if signed.order().maker != self.account_address
            || signed.order().signer != self.account_address
        {
            return Err(Error::BadRequest(
                "signed order and REST wallet identities differ",
            ));
        }
        let response = self.post(ORDERS, request).await?;
        let receipt: PredictCreateOrderReceipt = decode_response(ORDERS, &response)?;
        if response.status.as_u16() != 201
            || receipt.order_hash != signed.hash()
            || receipt.code.trim().is_empty()
        {
            return Err(
                Error::decode(ORDERS, PredictDecodeErrorKind::InvalidAcknowledgement)
                    .with_response_status(response.status.as_u16()),
            );
        }
        Ok(receipt)
    }

    /// Removes a bounded set of decimal order IDs from the venue orderbook exactly once.
    /// This REST operation does not invalidate the signed orders on-chain. Unlike the hash
    /// removal endpoint, this endpoint documents an explicit removed/noop/rejected partition.
    ///
    /// # Errors
    ///
    /// Empty, oversized or duplicate requests and incomplete/foreign acknowledgements are rejected.
    pub async fn request_remove_orders(&self, ids: &[U256]) -> Result<PredictRemoveOrdersReceipt> {
        if ids.is_empty()
            || ids.len() > 100
            || ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len()
        {
            return Err(Error::BadRequest(
                "order removal requires 1..=100 unique IDs",
            ));
        }
        #[derive(Serialize)]
        struct Data {
            ids: Vec<String>,
        }
        #[derive(Serialize)]
        struct Request {
            data: Data,
        }
        let request = Request {
            data: Data {
                ids: ids.iter().map(ToString::to_string).collect(),
            },
        };
        let response = self.post(REMOVE_ORDERS, &request).await?;
        check_response_status(REMOVE_ORDERS, &response)?;
        let receipt: PredictRemoveOrdersReceipt =
            serde_json::from_slice(&response.body).map_err(|e| {
                Error::from_json(REMOVE_ORDERS, &e).with_response_status(response.status.as_u16())
            })?;
        if response.status.as_u16() != 200 {
            return Err(Error::decode(
                REMOVE_ORDERS,
                PredictDecodeErrorKind::InvalidAcknowledgement,
            )
            .with_response_status(response.status.as_u16()));
        }
        receipt
            .validate(REMOVE_ORDERS, ids)
            .map_err(|error| error.with_response_status(response.status.as_u16()))?;
        Ok(receipt)
    }

    /// Returns the explicit signing and transport environment of this session.
    #[must_use]
    pub fn environment(&self) -> PredictEnvironment {
        self.transport.environment
    }

    /// Returns the session token for authenticated transports.
    #[must_use]
    pub fn token(&self) -> PredictAuthToken {
        self.token.borrow().clone()
    }

    pub(crate) fn token_updates(&self) -> tokio::sync::watch::Receiver<PredictAuthToken> {
        self.token.subscribe()
    }

    /// Re-authenticates without retrying any trading operation, validates identity, then publishes.
    ///
    /// # Errors
    ///
    /// Returns an error for mismatched identity, failed authentication or unusable expiration.
    pub async fn renew(&self, signer: &PredictAuthSigner) -> Result<()> {
        if signer.account_address() != self.account_address
            || signer.environment() != self.environment()
        {
            return Err(Error::Configuration(
                "renewal signer differs from session identity",
            ));
        }
        let replacement = self.transport.authenticate(signer).await?;
        replacement.request_account().await?;
        let token = replacement.token();
        let now = nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64()
            / 1_000_000_000;
        if token.expires_at()?.is_none_or(|expiry| expiry <= now) {
            return Err(Error::BadRequest("renewed JWT has no usable expiration"));
        }
        self.token.send_replace(token);
        let _ = self.identity_verified.set(());
        Ok(())
    }

    /// Returns the wallet identity bound to this session.
    #[must_use]
    pub fn account_address(&self) -> Address {
        self.account_address
    }

    /// Returns venue identity/referral/points, never a cash balance or `AccountState`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the response is invalid, or the account identity differs.
    pub async fn request_account(&self) -> Result<PredictAccount> {
        let account = self.fetch_account().await?;
        let _ = self.identity_verified.set(());
        Ok(account)
    }

    /// Validate externally bound tokens once; connected/replaced sessions do no extra GET.
    pub(crate) async fn ensure_account_identity(&self) -> Result<()> {
        self.identity_verified
            .get_or_try_init(|| async { self.fetch_account().await.map(|_| ()) })
            .await?;
        Ok(())
    }

    async fn fetch_account(&self) -> Result<PredictAccount> {
        let response = self.get(ACCOUNT, ACCOUNT, None::<&()>).await?;
        let account: PredictAccount = decode_response(ACCOUNT, &response)?;
        if account.address != self.account_address {
            return Err(Error::AccountMismatch);
        }
        Ok(account)
    }

    /// Requests one page of this wallet's orders, preserving the venue's next cursor.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid parameters, HTTP/decode failure or another wallet's order.
    pub async fn request_orders(
        &self,
        query: &PredictOrdersQuery<'_>,
    ) -> Result<PredictPage<PredictOrderData>> {
        validate_first(query.first)?;
        let response = self.get(ORDERS, ORDERS, Some(query)).await?;
        let page: PredictPage<PredictOrderData> = decode_page(ORDERS, &response, true)?;
        for order in &page.data {
            self.check_order_identity(order)?;
        }
        Ok(page)
    }

    /// Requests an order by its exchange EIP-712 hash, never by its decimal order ID.
    ///
    /// # Errors
    ///
    /// Returns an error on HTTP/decode failure, a foreign order or a different returned hash.
    pub async fn request_order(&self, hash: B256) -> Result<PredictOrderData> {
        let path = format!("{ORDERS}/{hash:#x}");
        let response = self.get(ORDER, &path, None::<&()>).await?;
        let order: PredictOrderData = decode_response(ORDER, &response)?;
        self.check_order_identity(&order)?;
        if order.order.hash.is_some_and(|returned| returned != hash) {
            return Err(Error::decode(
                ORDER,
                PredictDecodeErrorKind::InvalidOrderHash,
            ));
        }
        Ok(order)
    }

    /// Requests one page of wallet holdings. Omitted/lost outcomes are not inferred to be flat.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid parameters or HTTP/decode failure.
    pub async fn request_positions(
        &self,
        query: &PredictPositionsQuery<'_>,
    ) -> Result<PredictPage<PredictPositionData>> {
        validate_first(query.first)?;
        if query
            .after
            .is_some_and(|value| value.chars().count() > 1024)
            || query
                .category_id
                .is_some_and(|value| value.chars().count() > 256)
            || query.market_id.is_some_and(|value| value > i64::MAX as u64)
        {
            return Err(Error::BadRequest(
                "position filters exceed the API schema limits",
            ));
        }
        let response = self.get(POSITIONS, POSITIONS, Some(query)).await?;
        // PositionsResponse makes cursor optional, unlike orders and matches.
        decode_page(POSITIONS, &response, false)
    }

    /// Requests public match events with a mandatory filter for this session's signer.
    /// A match contains multiple participant legs; it is not itself a single FillReport.
    ///
    /// # Errors
    ///
    /// Returns an error on invalid parameters, HTTP/decode failure or an unrelated match.
    pub async fn request_matches(
        &self,
        query: &PredictMatchesQuery<'_>,
    ) -> Result<PredictPage<PredictMatchData>> {
        validate_first(query.first)?;
        let params = PredictAccountMatchesQuery {
            query,
            signer_address: self.account_address,
        };
        let response = self.get(MATCHES, MATCHES, Some(&params)).await?;
        let page: PredictPage<PredictMatchData> = decode_page(MATCHES, &response, true)?;
        for event in &page.data {
            let is_taker = event.taker.signer == self.account_address;
            let is_maker = event
                .makers
                .iter()
                .any(|leg| leg.signer == self.account_address);
            let matches_filter = match query.is_signer_maker {
                Some(true) => is_maker,
                Some(false) => is_taker,
                None => is_taker || is_maker,
            };
            if !matches_filter {
                return Err(Error::AccountMismatch);
            }
        }
        Ok(page)
    }

    fn check_order_identity(&self, order: &PredictOrderData) -> Result<()> {
        if order.order.fields.signer != self.account_address
            || order.order.fields.maker != self.account_address
        {
            return Err(Error::AccountMismatch);
        }
        Ok(())
    }

    async fn get<P: Serialize>(
        &self,
        endpoint: &'static str,
        path: &str,
        params: Option<&P>,
    ) -> Result<HttpResponse> {
        self.transport
            .client
            .request_with_params(
                Method::GET,
                format!("{}{path}", self.transport.base_url),
                params,
                Some(self.transport.headers(Some(&self.token()))),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| Error::from_http_client(endpoint, e))
    }

    async fn post<P: Serialize>(
        &self,
        endpoint: &'static str,
        request: &P,
    ) -> Result<HttpResponse> {
        let body: SecretString = serde_json::to_string(request)
            .map_err(|_| Error::BadRequest("failed to encode order command"))?
            .into();
        let mut headers = self.transport.headers(Some(&self.token()));
        headers.insert("Content-Type".into(), "application/json".into());
        self.transport
            .client
            .request_with_secret_body(
                Method::POST,
                format!("{}{endpoint}", self.transport.base_url),
                None,
                Some(headers),
                body,
                None,
                None,
            )
            .await
            .map_err(|e| Error::from_http_client(endpoint, e))
    }
}
