// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! In-process private HTTP, wallet WebSocket, and chain RPC venue.

use super::*;

pub(super) type CreateReply = (u16, u64, Vec<String>);

#[derive(Clone)]
pub(super) struct VenueState {
    pub(super) pages: Arc<Mutex<VecDeque<Value>>>,
    pub(super) match_pages: Arc<Mutex<VecDeque<(u16, Value, u64)>>>,
    pub(super) match_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    pub(super) match_requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
    pub(super) match_response_log: Arc<Mutex<Vec<Value>>>,
    pub(super) position_response_log: Arc<Mutex<Vec<Value>>>,
    pub(super) match_snapshot: Arc<Mutex<Option<Vec<Value>>>>,
    pub(super) position_snapshot: Arc<Mutex<Option<Vec<Value>>>>,
    pub(super) orders: Arc<Mutex<VecDeque<Value>>>,
    pub(super) order_replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
    pub(super) order_requests: Arc<Mutex<Vec<String>>>,
    pub(super) order_response_log: Arc<Mutex<Vec<Value>>>,
    pub(super) order_snapshot: Arc<Mutex<Option<Value>>>,
    pub(super) orders_by_hash: Arc<Mutex<HashMap<String, Value>>>,
    pub(super) order_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    pub(super) rpc_accounts: Arc<Mutex<Vec<String>>>,
    pub(super) requests: Arc<Mutex<Vec<HashMap<String, String>>>>,
    pub(super) sockets: Arc<Mutex<Vec<mpsc::UnboundedSender<Message>>>>,
    pub(super) ws_requests: Arc<Mutex<Vec<Value>>>,
    pub(super) open_orders: Arc<Mutex<Vec<Value>>>,
    pub(super) open_pages: Arc<Mutex<VecDeque<Value>>>,
    pub(super) order_list_log: Arc<Mutex<Vec<Value>>>,
    pub(super) rpc_frame: Arc<Mutex<(u128, u64, u64)>>,
    pub(super) rpc_methods: Arc<Mutex<Vec<String>>>,
    pub(super) rpc_delay_ms: Arc<Mutex<u64>>,
    pub(super) rpc_failure: Arc<Mutex<bool>>,
    pub(super) remove_replies: Arc<Mutex<VecDeque<(u16, Value, u64)>>>,
    pub(super) removals: Arc<Mutex<Vec<Value>>>,
    pub(super) removal_responses: Arc<Mutex<Vec<Value>>>,
    pub(super) remove_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    pub(super) creations: Arc<Mutex<Vec<Value>>>,
    pub(super) create_replies: Arc<Mutex<VecDeque<CreateReply>>>,
    pub(super) create_order_ids: Arc<Mutex<VecDeque<String>>>,
    pub(super) create_gate: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    pub(super) create_body: Arc<Mutex<Option<Value>>>,
    pub(super) market_fee: Arc<Mutex<u32>>,
    pub(super) market_yield_bearing: Arc<Mutex<bool>>,
    pub(super) admission_allowance: Arc<Mutex<U256>>,
    pub(super) sell_inventory: Arc<Mutex<U256>>,
    pub(super) sell_approved: Arc<Mutex<bool>>,
    pub(super) execution_book: Arc<Mutex<Value>>,
}

pub(super) struct Server {
    pub(super) http: String,
    pub(super) ws: String,
    pub(super) state: VenueState,
    pub(super) task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) fn json_response(value: Value) -> Response {
    Response::builder()
        .header("content-type", "application/json")
        .body(axum::body::Body::from(value.to_string()))
        .unwrap()
}

pub(super) fn auth(headers: &HeaderMap) {
    assert_eq!(headers["x-api-key"], KEY);
    assert_eq!(headers["authorization"], format!("Bearer {JWT}"));
}

pub(super) async fn account(headers: HeaderMap) -> Response {
    auth(&headers);
    json_response(
        json!({"success":true, "data":{"name":"fixture", "address":WALLET,
        "referral":{"code":null,"status":"LOCKED"}, "points":{"total":123}}}),
    )
}

pub(super) async fn positions(
    State(state): State<VenueState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    let url = nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
    let query = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    state.requests.lock().unwrap().push(query);
    let queued = state.pages.lock().unwrap().pop_front();
    let reply=queued.unwrap_or_else(|| json!({"success":true,"cursor":null,"data":state.position_snapshot.lock().unwrap().clone().expect("unexpected position request")}));
    state
        .position_response_log
        .lock()
        .unwrap()
        .push(reply.clone());
    json_response(reply)
}

pub(super) async fn matches_history(
    State(state): State<VenueState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    let url = nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
    state.match_requests.lock().unwrap().push(
        url.query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect(),
    );
    let gate = state.match_gate.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.notified().await;
    }
    let (status, body, delay) = state
        .match_pages
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| match state.match_snapshot.lock().unwrap().clone() {
            Some(mut rows) => {
                if let Some(market) = url
                    .query_pairs()
                    .find_map(|(key, value)| (key == "marketId").then(|| value.into_owned()))
                {
                    let market: u64 = market
                        .parse()
                        .expect("fixture market query must be numeric");
                    rows.retain(|row| row["market"]["id"].as_u64() == Some(market));
                }
                (200, json!({"success":true,"cursor":null,"data":rows}), 0)
            }
            None => (
                503,
                json!({"success":false,"message":"fixture history unavailable"}),
                0,
            ),
        });
    tokio::time::sleep(Duration::from_millis(delay)).await;
    state
        .match_response_log
        .lock()
        .unwrap()
        .push(json!({"status":status,"body":body}));
    let mut response = json_response(body);
    *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
    response
}

pub(super) async fn rest_order(
    State(state): State<VenueState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    assert!(uri.path().starts_with("/v1/orders/0x"));
    state
        .order_requests
        .lock()
        .unwrap()
        .push(uri.path().to_string());
    let gate = state.order_gate.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.notified().await;
    }
    let reply = state.order_replies.lock().unwrap().pop_front();
    let (status, body) = reply.unwrap_or_else(|| {
        (
            200,
            state
                .orders
                .lock()
                .unwrap()
                .pop_front()
                .or_else(|| {
                    state
                        .orders_by_hash
                        .lock()
                        .unwrap()
                        .get(uri.path().trim_start_matches("/v1/orders/"))
                        .cloned()
                })
                .or_else(|| state.order_snapshot.lock().unwrap().clone())
                .unwrap_or_else(|| panic!("unexpected order request: {}", uri.path())),
        )
    });
    state
        .order_response_log
        .lock()
        .unwrap()
        .push(json!({"path":uri.path(),"status":status,"body":body}));
    let mut response = json_response(body);
    *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
    response
}

pub(super) async fn open_orders(
    State(state): State<VenueState>,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    let url = nautilus_network::http::Url::parse(&format!("http://localhost{uri}")).unwrap();
    let query: HashMap<_, _> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(query.get("status").is_none_or(|status| status == "OPEN"));
    state.requests.lock().unwrap().push(query.clone());
    let reply = state
        .open_pages
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| {
            let rows = if query.contains_key("status") {
                state.open_orders.lock().unwrap().clone()
            } else {
                let by_hash = state.orders_by_hash.lock().unwrap();
                if by_hash.is_empty() {
                    state.open_orders.lock().unwrap().clone()
                } else {
                    by_hash.values().map(|v| v["data"].clone()).collect()
                }
            };
            json!({"success":true,"cursor":null,"data":rows})
        });
    state
        .order_list_log
        .lock()
        .unwrap()
        .push(json!({"query":query,"response":reply}));
    json_response(reply)
}

pub(super) async fn remove_orders(
    State(state): State<VenueState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    auth(&headers);
    let request: Value = serde_json::from_slice(&body).unwrap();
    state.removals.lock().unwrap().push(request.clone());
    let gate = state.remove_gate.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.notified().await;
    }
    let (status, mut reply, delay) = state
        .remove_replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected removal request");
    if reply["echoMixedTerminal"] == true {
        let ids = request["data"]["ids"].as_array().unwrap();
        reply = json!({"success":true,"removed":[ids[0].clone()],"noop":ids[1..].to_vec()});
        // Venue fixture: a concurrent external cancellation explains the noop leg.
        for row in state.orders_by_hash.lock().unwrap().values_mut() {
            if reply["noop"]
                .as_array()
                .unwrap()
                .contains(&row["data"]["id"])
            {
                row["data"]["status"] = json!("CANCELLED");
            }
        }
        state
            .open_orders
            .lock()
            .unwrap()
            .retain(|row| !ids.contains(&row["id"]));
    }
    if reply["echoRemoved"] == true {
        reply = json!({"success":true,"removed":request["data"]["ids"],"noop":[]});
    }
    if status == 200
        && let Some(removed) = reply["removed"].as_array()
    {
        state
            .open_orders
            .lock()
            .unwrap()
            .retain(|row| !removed.contains(&row["id"]));
    }
    state
        .removal_responses
        .lock()
        .unwrap()
        .push(json!({"status":status,"body":reply,"request":request}));
    tokio::time::sleep(Duration::from_millis(delay)).await;
    let mut response = json_response(reply);
    *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
    response
}

pub(super) async fn collateral_rpc(
    State(state): State<VenueState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    assert!(!headers.contains_key("authorization"));
    assert!(!headers.contains_key("x-api-key"));
    let request: Value = serde_json::from_slice(&body).unwrap();
    state
        .rpc_methods
        .lock()
        .unwrap()
        .push(request["method"].as_str().unwrap().to_string());
    let delay = *state.rpc_delay_ms.lock().unwrap();
    if delay != 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    if *state.rpc_failure.lock().unwrap() {
        return json_response(
            json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"private-provider-fixture"}}),
        );
    }
    let (balance, number, timestamp) = *state.rpc_frame.lock().unwrap();
    let result = match request["method"].as_str().unwrap() {
        "eth_chainId" => json!("0x38"),
        "eth_getBlockByNumber" => {
            json!({"number":format!("0x{number:x}"),"hash":format!("0x{number:064x}"),"timestamp":format!("0x{timestamp:x}")})
        }
        "eth_call" => {
            let data = request["params"][0]["data"].as_str().unwrap();
            let amount = if data == "0x313ce567" {
                U256::from(18)
            } else if data.starts_with("0x70a08231") {
                state
                    .rpc_accounts
                    .lock()
                    .unwrap()
                    .push(format!("0x{}", &data[34..74]));
                U256::from(balance)
            } else if data.starts_with("0xdd62ed3e") {
                *state.admission_allowance.lock().unwrap()
            } else if data.starts_with("0x00fdd58e") {
                *state.sell_inventory.lock().unwrap()
            } else if data.starts_with("0xe985e9c5") {
                U256::from(u8::from(*state.sell_approved.lock().unwrap()))
            } else if data.starts_with("0x7ecebe00") {
                U256::from(7)
            } else {
                U256::ZERO
            };
            json!(format!("0x{amount:064x}"))
        }
        _ => panic!("unexpected RPC method"),
    };
    json_response(json!({"jsonrpc":"2.0","id":request["id"],"result":result}))
}

pub(super) async fn websocket(
    ws: WebSocketUpgrade,
    State(state): State<VenueState>,
    headers: HeaderMap,
) -> Response {
    assert_eq!(headers["x-api-key"], KEY);
    ws.on_upgrade(move |socket| serve_socket(socket, state))
}

pub(super) async fn serve_socket(socket: WebSocket, state: VenueState) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    state.sockets.lock().unwrap().push(tx);
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            command = rx.recv() => {
                let Some(command) = command else { break; };
                if sink.send(command).await.is_err() { break; }
            }
            message = stream.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    let request: Value = serde_json::from_str(&text).unwrap();
                    state.ws_requests.lock().unwrap().push(request.clone());
                    if request["method"] == "subscribe" {
                        assert_eq!(request["params"], json!([format!("predictWalletEvents/{JWT}")]));
                        if sink.send(Message::Text(json!({"type":"R","requestId":request["requestId"],"success":true}).to_string().into())).await.is_err() { break; }
                    }
                }
                Some(Ok(Message::Ping(_)|Message::Pong(_))) => {},
                _ => break,
            }
        }
    }
}

pub(super) async fn server(pages: Vec<Value>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = VenueState {
        match_response_log: Default::default(),
        position_response_log: Default::default(),
        pages: Arc::new(Mutex::new(pages.into())),
        match_pages: Arc::new(Mutex::new(VecDeque::new())),
        match_gate: Default::default(),
        match_requests: Arc::new(Mutex::new(vec![])),
        match_snapshot: Default::default(),
        position_snapshot: Default::default(),
        orders: Arc::new(Mutex::new(VecDeque::new())),
        order_replies: Default::default(),
        order_requests: Default::default(),
        order_response_log: Default::default(),
        order_snapshot: Default::default(),
        orders_by_hash: Default::default(),
        order_gate: Default::default(),
        rpc_accounts: Arc::new(Mutex::new(vec![])),
        requests: Arc::new(Mutex::new(vec![])),
        sockets: Arc::new(Mutex::new(vec![])),
        ws_requests: Default::default(),
        open_orders: Arc::new(Mutex::new(vec![])),
        open_pages: Arc::new(Mutex::new(VecDeque::new())),
        order_list_log: Default::default(),
        rpc_frame: Arc::new(Mutex::new((1_000_000_000_000_000_000_000u128, 1, 2))),
        rpc_methods: Arc::new(Mutex::new(vec![])),
        rpc_delay_ms: Arc::new(Mutex::new(0)),
        rpc_failure: Arc::new(Mutex::new(false)),
        remove_replies: Arc::new(Mutex::new(VecDeque::new())),
        removals: Arc::new(Mutex::new(vec![])),
        removal_responses: Arc::new(Mutex::new(vec![])),
        remove_gate: Arc::new(Mutex::new(None)),
        creations: Arc::new(Mutex::new(vec![])),
        create_replies: Arc::new(Mutex::new(VecDeque::new())),
        create_order_ids: Default::default(),
        create_gate: Arc::new(Mutex::new(None)),
        create_body: Arc::new(Mutex::new(None)),
        market_fee: Arc::new(Mutex::new(0)),
        market_yield_bearing: Arc::new(Mutex::new(false)),
        admission_allowance: Arc::new(Mutex::new(U256::MAX)),
        sell_inventory: Arc::new(Mutex::new(U256::ZERO)),
        sell_approved: Arc::new(Mutex::new(false)),
        execution_book: Arc::new(Mutex::new(Value::Null)),
    };
    let router = Router::new()
        .route("/v1/account", get(account))
        .route("/v1/positions", get(positions))
        .route("/v1/orders/matches", get(matches_history))
        .route("/v1/orders/{hash}", get(rest_order))
        .route("/v1/orders", get(open_orders).post(create_order))
        .route("/v1/markets/{id}", get(execution_market))
        .route("/v1/markets/{id}/orderbook", get(execution_book))
        .route("/v1/orders/remove", post(remove_orders))
        .route("/rpc", post(collateral_rpc))
        .route("/ws", get(websocket))
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server {
        http: format!("http://{addr}"),
        ws: format!("ws://{addr}/ws"),
        state,
        task,
    }
}

impl Server {
    pub(super) fn wallet(&self, data: Value) {
        self.state
            .sockets
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(Message::Text(
                json!({"type":"M", "topic":format!("predictWalletEvents/{JWT}"),"data":data})
                    .to_string()
                    .into(),
            ))
            .unwrap();
    }
}

pub(super) async fn execution_market(
    State(state): State<VenueState>,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    json_response(
        json!({"success":true,"data":{"id":42,"tradingStatus":"OPEN",
        "isNegRisk":false,"isYieldBearing":*state.market_yield_bearing.lock().unwrap(),"feeRateBps":*state.market_fee.lock().unwrap(),
        "decimalPrecision":3,"outcomes":[{"indexSet":1,"onChainId":"11"},{"indexSet":2,"onChainId":"22"}]}}),
    )
}

pub(super) async fn create_order(
    State(state): State<VenueState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    auth(&headers);
    let request: Value = serde_json::from_slice(&body).unwrap();
    let order_id = state
        .create_order_ids
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| "12345".into());
    state.creations.lock().unwrap().push(request.clone());
    let gate = state.create_gate.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.notified().await;
    }
    let (status, delay, events) =
        state
            .create_replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or((201, 0, vec![]));
    let index = if request["data"]["order"]["tokenId"] == "11" {
        1
    } else {
        2
    };
    for kind in events {
        let mut payload = if kind == "orderTransactionSuccess" {
            fill(index)
        } else {
            event(&kind, index)
        };
        payload["orderId"] = json!(order_id);
        payload["orderHash"] = request["data"]["order"]["hash"].clone();
        let price_wei =
            Decimal::from_str_exact(request["data"]["pricePerShare"].as_str().unwrap()).unwrap();
        let price = price_wei / Decimal::from(1_000_000_000_000_000_000u64);
        let quantity =
            Decimal::from_str_exact(request["data"]["order"]["takerAmount"].as_str().unwrap())
                .unwrap()
                / Decimal::from(1_000_000_000_000_000_000u64);
        payload["details"]["quantity"] = json!(quantity.to_string());
        payload["details"]["price"] = json!(price.to_string());
        payload["details"]["value"] = json!((price * quantity).to_string());
        payload["timestamp"] = json!(2);
        if kind == "orderTransactionSuccess" {
            payload["isMaker"] = json!(true);
            payload["fee"]["amountWei"] = json!("0");
            payload["fill"]["executedPriceWei"] = json!(price_wei.to_string());
            payload["fill"]["executedValueWei"] =
                json!((price_wei * Decimal::from(17)).to_string());
        }
        if kind == "orderCancelled" {
            payload["details"]["quantityFilled"] = json!("17");
        }
        state
            .sockets
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .send(Message::Text(
                json!({"type":"M","topic":format!("predictWalletEvents/{JWT}"),"data":payload})
                    .to_string()
                    .into(),
            ))
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(delay)).await;
    let payload = if let Some(body) = state.create_body.lock().unwrap().clone() {
        body
    } else if status == 201 {
        json!({"success":true,"data":{"code":"opaque-code-not-a-status","orderId":order_id,
            "orderHash":request["data"]["order"]["hash"]}})
    } else if status == 400 {
        // Synthetic business rejection in the documented error envelope.
        json!({"success":false,"code":400,"error":"bad_request","message":"invalid signature"})
    } else {
        json!({"success":false,"message":"fixture rejected"})
    };
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(payload.to_string()))
        .unwrap()
}

pub(super) async fn execution_book(
    State(state): State<VenueState>,
    headers: HeaderMap,
) -> Response {
    auth(&headers);
    json_response(json!({"success":true,"data":state.execution_book.lock().unwrap().clone()}))
}
