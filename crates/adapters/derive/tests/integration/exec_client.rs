// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
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

//! Integration tests for `DeriveExecutionClient` against local REST and WS mocks.
//!
//! Covers the lifecycle (connect, private channel subscription, disconnect),
//! the order operations (submit / cancel / modify / batch-cancel / query),
//! report generation (open / history / fill / position), and the private
//! WS dispatch loop. Uses minimal axum mocks that record the incoming
//! request bodies and let tests inject responses or push WS frames.

use std::{
    cell::RefCell,
    collections::HashMap,
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use nautilus_common::{
    cache::{Cache, ORDER_NOT_FOUND},
    clients::ExecutionClient,
    live::runner::{replace_exec_event_sender, replace_system_event_sender},
    messages::{
        ExecutionEvent, SystemEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, ExecutionReport, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
        },
        system::SocketState,
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_derive::{
    common::{
        consts::{DERIVE_VENUE, MIN_SIGNATURE_TTL, TRIGGER_ORDER_SIGNATURE_TTL},
        enums::DeriveEnvironment,
        parse::parse_derive_instrument_any,
    },
    config::DeriveExecutionClientConfig,
    execution::DeriveExecutionClient,
    http::models::DeriveInstrument,
};
use nautilus_live::{ExecutionClientCore, SocketReconnectRegistry, SocketReconnectRequestOutcome};
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    data::QuoteTick,
    enums::{
        AccountType, OmsType, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce,
        TriggerType,
    },
    events::{AccountState, OrderAccepted, OrderEventAny, OrderInitialized, OrderUpdated},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TradeId,
        TraderId, VenueOrderId,
    },
    orders::{Order, OrderAny, OrderList, OrderTestBuilder},
    reports::{OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Money, Price, Quantity},
};
use nautilus_network::{http::HttpClient, websocket::TransportBackend};
use rstest::rstest;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use ustr::Ustr;

const TEST_WALLET: &str = "0x000000000000000000000000000000000000aaaa";
const TEST_SESSION_KEY: &str = "0x2ae8be44db8a590d20bffbe3b6872df9b569147d3bf6801a35a28281a4816bbd";
const TEST_SUBACCOUNT: u64 = 30769;
const TEST_DOMAIN_SEPARATOR: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";
const TEST_ACTION_TYPEHASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const TEST_TRADE_MODULE_ADDRESS: &str = "0x000000000000000000000000000000000000bbbb";

#[derive(Clone, Default)]
struct RestState {
    get_subaccount_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    get_order_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    open_orders_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    trigger_orders_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    order_history_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    trade_history_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    positions_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    ticker_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    get_instrument_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    subaccount_response: Arc<tokio::sync::Mutex<Value>>,
    fail_subaccount_requests: Arc<AtomicBool>,
    open_orders_response: Arc<tokio::sync::Mutex<Value>>,
    trigger_orders_response: Arc<tokio::sync::Mutex<Value>>,
    order_history_response: Arc<tokio::sync::Mutex<Value>>,
    order_history_pages: Arc<tokio::sync::Mutex<Vec<Value>>>,
    trade_history_response: Arc<tokio::sync::Mutex<Value>>,
    trade_history_pages: Arc<tokio::sync::Mutex<Vec<Value>>>,
    positions_response: Arc<tokio::sync::Mutex<Value>>,
    ticker_response: Arc<tokio::sync::Mutex<Value>>,
    get_order_response: Arc<tokio::sync::Mutex<Value>>,
    get_instrument_response: Arc<tokio::sync::Mutex<Value>>,
    submitted_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancelled_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    replaced_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    ambiguous_submit_response: Arc<AtomicBool>,
    /// Injected JSON-RPC error envelope (without `id`) for `private/order`,
    /// e.g. `json!({"error": {"code": -32603, "message": "Internal error"}})`.
    submit_error_response: Arc<tokio::sync::Mutex<Option<Value>>>,
    /// When set, `private/cancel` answers HTTP 503 so the write outcome is
    /// ambiguous (the venue may have cancelled before the response failed).
    fail_cancel_requests: Arc<AtomicBool>,
    /// `public/get_ticker` bodies, separately from the bulk
    /// `public/get_tickers` map, so a test can serve an empty bulk map (the
    /// outage condition) alongside a valid direct response.
    direct_ticker_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    direct_ticker_response: Arc<tokio::sync::Mutex<Value>>,
    /// Delay applied to `private/get_order_history`, so a reconciliation's
    /// history traversal can outlast the configured account-state age.
    order_history_delay_ms: Arc<AtomicUsize>,
    /// When set, `private/order` answers HTTP 503: the write outcome is
    /// ambiguous and, in these tests, the order never reached the venue.
    fail_submit_requests: Arc<AtomicBool>,
    /// When set, `private/replace` answers HTTP 503.
    fail_replace_requests: Arc<AtomicBool>,
    /// Delay applied to `private/order` before it answers, so a write can be
    /// held in flight at the venue across several reconciliations.
    submit_delay_ms: Arc<AtomicUsize>,
    /// When non-zero, `private/order` answers with this HTTP status instead of
    /// a JSON-RPC body.
    submit_http_status: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct WsState {
    connection_count: Arc<AtomicUsize>,
    initial_handshake_failures: Arc<AtomicUsize>,
    disconnect_now: Arc<AtomicBool>,
    login_delay_ms: Arc<AtomicUsize>,
    login_delay_started: Arc<AtomicBool>,
    subscribe_delay_ms: Arc<AtomicUsize>,
    delayed_subscribe_acknowledged: Arc<AtomicBool>,
    login_frames: Arc<tokio::sync::Mutex<Vec<Value>>>,
    subscribe_frames: Arc<tokio::sync::Mutex<Vec<Value>>>,
    subscribe_status: Arc<tokio::sync::Mutex<Option<HashMap<String, String>>>>,
    login_failures_after_first: Arc<AtomicUsize>,
    disconnect_after_subscribe: Arc<AtomicBool>,
    // Order entry now flows over the WebSocket Trading API. Each vector holds
    // the `params` object of a captured `private/*` frame so assertions read
    // the signed body fields directly (`body["instrument_name"]`, etc.).
    submitted_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    submitted_order_received_at_secs: Arc<tokio::sync::Mutex<Vec<u64>>>,
    submitted_trigger_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancelled_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancelled_trigger_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancelled_labels: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancel_by_instrument_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    cancel_all_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    replace_orders: Arc<tokio::sync::Mutex<Vec<Value>>>,
    // Injected JSON-RPC reply body (without `id`) per private method. When set,
    // the mock merges the request `id` and returns it instead of the default
    // success result, e.g. `json!({"error": {"code": -32602, "message": "x"}})`.
    order_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    trigger_order_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    cancel_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    cancel_trigger_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    cancel_by_instrument_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    cancel_by_label_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    replace_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    replace_notification_before_reply: Arc<tokio::sync::Mutex<Option<Value>>>,
    notification_tx: tokio::sync::mpsc::UnboundedSender<Value>,
    notification_rx: Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<Value>>>>,
}

impl Default for WsState {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
        Self {
            connection_count: Arc::new(AtomicUsize::new(0)),
            initial_handshake_failures: Arc::new(AtomicUsize::new(0)),
            disconnect_now: Arc::new(AtomicBool::new(false)),
            login_delay_ms: Arc::new(AtomicUsize::new(0)),
            login_delay_started: Arc::new(AtomicBool::new(false)),
            subscribe_delay_ms: Arc::new(AtomicUsize::new(0)),
            delayed_subscribe_acknowledged: Arc::new(AtomicBool::new(false)),
            login_frames: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            subscribe_frames: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            subscribe_status: Arc::new(tokio::sync::Mutex::new(None)),
            login_failures_after_first: Arc::new(AtomicUsize::new(0)),
            disconnect_after_subscribe: Arc::new(AtomicBool::new(false)),
            submitted_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            submitted_order_received_at_secs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            submitted_trigger_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            cancelled_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            cancelled_trigger_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            cancelled_labels: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            cancel_by_instrument_calls: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            cancel_all_calls: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            replace_orders: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            order_reply: Arc::new(tokio::sync::Mutex::new(None)),
            trigger_order_reply: Arc::new(tokio::sync::Mutex::new(None)),
            cancel_reply: Arc::new(tokio::sync::Mutex::new(None)),
            cancel_trigger_reply: Arc::new(tokio::sync::Mutex::new(None)),
            cancel_by_instrument_reply: Arc::new(tokio::sync::Mutex::new(None)),
            cancel_by_label_reply: Arc::new(tokio::sync::Mutex::new(None)),
            replace_reply: Arc::new(tokio::sync::Mutex::new(None)),
            replace_notification_before_reply: Arc::new(tokio::sync::Mutex::new(None)),
            notification_tx: tx,
            notification_rx: Arc::new(tokio::sync::Mutex::new(Some(rx))),
        }
    }
}

impl WsState {
    fn push_notification(&self, frame: Value) {
        self.notification_tx
            .send(frame)
            .expect("notification queue closed");
    }
}

async fn handle_rest_health() -> impl IntoResponse {
    StatusCode::OK
}

async fn wait_for_http_health(addr: SocketAddr) {
    let health_url = format!("http://{addr}/health");
    let http_client = HttpClient::builder().build().unwrap();
    wait_until_async(
        || {
            let url = health_url.clone();
            let client = http_client.clone();
            async move { client.get(url, None, None, Some(1), None).await.is_ok() }
        },
        Duration::from_secs(5),
    )
    .await;
}

async fn handle_get_subaccount(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.get_subaccount_calls.lock().await.push(parsed);
    if state.fail_subaccount_requests.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "temporary outage").into_response();
    }
    let response = state.subaccount_response.lock().await.clone();
    let body = if response.is_null() {
        json!({"id": 1, "result": sample_subaccount_json()})
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_order(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.get_order_calls.lock().await.push(parsed);
    let response = state.get_order_response.lock().await.clone();
    let body = if response.is_null() {
        json!({"id": 1, "result": sample_order_json()})
    } else if response.get("error").is_some() {
        response
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_submit_order(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.submitted_orders.lock().await.push(parsed.clone());
    let delay_ms = state.submit_delay_ms.load(Ordering::SeqCst);
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
    }
    let status = state.submit_http_status.load(Ordering::SeqCst);
    if status > 0 {
        let status = StatusCode::from_u16(status as u16).expect("valid status");
        return (status, "venue rejected the request").into_response();
    }
    if state.fail_submit_requests.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "temporary outage").into_response();
    }
    if let Some(error) = state.submit_error_response.lock().await.clone() {
        let mut body = error;
        body["id"] = json!(1);
        return (StatusCode::OK, Json(body)).into_response();
    }
    if state.ambiguous_submit_response.load(Ordering::SeqCst) {
        return (StatusCode::OK, Json(json!({"id": 1, "result": {}}))).into_response();
    }
    let order = order_json_with(
        "ord-rest-submit-1",
        parsed
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("STRAT-REST-SUBMIT"),
        parsed
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or("buy"),
        parsed
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or("ETH-PERP"),
        1_700_000_001_000_i64,
        "open",
    );
    (
        StatusCode::OK,
        Json(json!({"id": 1, "result": {"order": order}})),
    )
        .into_response()
}

async fn handle_cancel_order(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.cancelled_orders.lock().await.push(parsed);
    if state.fail_cancel_requests.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "temporary outage").into_response();
    }
    (StatusCode::OK, Json(json!({"id": 1, "result": {}}))).into_response()
}

async fn handle_replace_order(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.replaced_orders.lock().await.push(parsed.clone());
    if state.fail_replace_requests.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "temporary outage").into_response();
    }
    let order = order_json_with(
        "ord-rest-replace-1",
        parsed
            .get("label")
            .and_then(Value::as_str)
            .unwrap_or("STRAT-REST-REPLACE"),
        parsed
            .get("direction")
            .and_then(Value::as_str)
            .unwrap_or("buy"),
        parsed
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or("ETH-PERP"),
        1_700_000_001_000_i64,
        "open",
    );
    (
        StatusCode::OK,
        Json(json!({"id": 1, "result": {"order": order}})),
    )
        .into_response()
}

async fn handle_get_open_orders(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.open_orders_calls.lock().await.push(parsed);
    let response = state.open_orders_response.lock().await.clone();
    let body = if response.is_null() {
        json!({"id": 1, "result": {"orders": [sample_order_json()], "subaccount_id": TEST_SUBACCOUNT}})
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_trigger_orders(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.trigger_orders_calls.lock().await.push(parsed);
    let response = state.trigger_orders_response.lock().await.clone();
    let body = if response.is_null() {
        json!({"id": 1, "result": {"orders": [], "subaccount_id": TEST_SUBACCOUNT}})
    } else if response.get("error").is_some() {
        response
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_order_history(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.order_history_calls.lock().await.push(parsed);

    let delay_ms = state.order_history_delay_ms.load(Ordering::SeqCst);
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
    }

    let mut pages = state.order_history_pages.lock().await;
    if !pages.is_empty() {
        let page = pages.remove(0);
        return (StatusCode::OK, Json(json!({"id": 1, "result": page}))).into_response();
    }
    drop(pages);

    let response = state.order_history_response.lock().await.clone();
    let body = if response.is_null() {
        // Default: empty page so by-label fallbacks terminate.
        json!({
            "id": 1,
            "result": {
                "orders": [],
                "pagination": {"count": 0, "num_pages": 0},
                "subaccount_id": TEST_SUBACCOUNT,
            }
        })
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_trade_history(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.trade_history_calls.lock().await.push(parsed);

    // `trade_history_pages` lets pagination tests sequence one response per
    // call; when empty, fall back to the single canned response.
    let mut pages = state.trade_history_pages.lock().await;
    if !pages.is_empty() {
        let page = pages.remove(0);
        return (StatusCode::OK, Json(json!({"id": 1, "result": page}))).into_response();
    }
    drop(pages);

    let response = state.trade_history_response.lock().await.clone();
    let body = if response.is_null() {
        json!({
            "id": 1,
            "result": {
                "trades": [],
                "pagination": {"count": 0, "num_pages": 0},
                "subaccount_id": TEST_SUBACCOUNT,
            }
        })
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_positions(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.positions_calls.lock().await.push(parsed);
    let response = state.positions_response.lock().await.clone();
    let body = if response.is_null() {
        json!({
            "id": 1,
            "result": {"positions": [], "subaccount_id": TEST_SUBACCOUNT}
        })
    } else {
        json!({"id": 1, "result": response})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_tickers(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.ticker_calls.lock().await.push(parsed);

    let response = state.ticker_response.lock().await.clone();
    let response = if response.is_null() {
        sample_ticker_json("ETH-PERP", 1_700_000_000_013_i64)
    } else {
        response
    };
    let body = if response.get("error").is_some() {
        response
    } else if response.get("tickers").is_some() {
        json!({"id": 1, "result": response})
    } else {
        let instrument_name = response
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or("ETH-PERP");
        json!({"id": 1, "result": {"tickers": {instrument_name: response}}})
    };
    (StatusCode::OK, Json(body)).into_response()
}

/// Serves the instrument-specific `public/get_ticker` endpoint.
///
/// Calls are also recorded in `ticker_calls` so assertions about "the quote
/// refresh happened" stay endpoint-agnostic. `direct_ticker_response` takes
/// precedence; otherwise the bulk `ticker_response` body is reused so a test
/// that only cares about ticker content needs no extra setup.
async fn handle_get_ticker(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.direct_ticker_calls.lock().await.push(parsed.clone());
    state.ticker_calls.lock().await.push(parsed.clone());

    let requested = parsed
        .get("instrument_name")
        .and_then(Value::as_str)
        .unwrap_or("ETH-PERP")
        .to_string();

    let direct = state.direct_ticker_response.lock().await.clone();
    let response = if direct.is_null() {
        state.ticker_response.lock().await.clone()
    } else {
        direct
    };
    let mut response = if response.is_null() {
        sample_ticker_json(&requested, 1_700_000_000_013_i64)
    } else {
        response
    };

    if response.get("error").is_some() {
        return (StatusCode::OK, Json(response)).into_response();
    }
    if let Some(tickers) = response.get("tickers").cloned() {
        let Some(ticker) = tickers.get(&requested).cloned() else {
            return (
                StatusCode::OK,
                Json(json!({
                    "id": 1,
                    "error": {"code": -32602, "message": "instrument not found"},
                })),
            )
                .into_response();
        };
        response = ticker;
    }
    response["instrument_name"] = Value::String(requested);
    (StatusCode::OK, Json(json!({"id": 1, "result": response}))).into_response()
}

async fn handle_get_instrument(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.get_instrument_calls.lock().await.push(parsed.clone());
    let response = state.get_instrument_response.lock().await.clone();
    let mut result = if response.is_null() {
        sample_instrument_json()
    } else {
        response
    };
    // Echo the requested name so a fetch for any instrument returns a
    // definition whose name matches the order that triggered it.
    if let Some(requested) = parsed.get("instrument_name").and_then(Value::as_str) {
        result["instrument_name"] = Value::String(requested.to_string());
    }
    (StatusCode::OK, Json(json!({"id": 1, "result": result}))).into_response()
}

async fn start_rest_server(state: RestState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/health", get(handle_rest_health))
        .route("/private/get_subaccount", post(handle_get_subaccount))
        .route("/private/get_order", post(handle_get_order))
        .route("/private/order", post(handle_submit_order))
        .route("/private/cancel", post(handle_cancel_order))
        .route("/private/replace", post(handle_replace_order))
        .route("/private/get_open_orders", post(handle_get_open_orders))
        .route(
            "/private/get_trigger_orders",
            post(handle_get_trigger_orders),
        )
        .route("/private/get_order_history", post(handle_get_order_history))
        .route("/private/get_trade_history", post(handle_get_trade_history))
        .route("/private/get_positions", post(handle_get_positions))
        .route("/public/get_tickers", post(handle_get_tickers))
        .route("/public/get_ticker", post(handle_get_ticker))
        .route("/public/get_instrument", post(handle_get_instrument))
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    wait_for_http_health(addr).await;
    addr
}

async fn handle_ws_upgrade(ws: WebSocketUpgrade, State(state): State<WsState>) -> Response {
    if state
        .initial_handshake_failures
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "temporary websocket outage",
        )
            .into_response();
    }
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: WsState) {
    state.connection_count.fetch_add(1, Ordering::SeqCst);

    // Take the notification receiver on connect; the test's `push_notification`
    // sends Values that get forwarded to the client as subscription frames.
    let mut notification_rx = state.notification_rx.lock().await.take();

    loop {
        tokio::select! {
            biased;
            () = tokio::time::sleep(Duration::from_millis(5)) => {
                if state.disconnect_now.swap(false, Ordering::SeqCst) {
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
            }
            frame = socket.next() => {
                let Some(Ok(frame)) = frame else { break };
                match frame {
                    Message::Text(text) => {
                        let Ok(payload) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        let id = payload.get("id").and_then(Value::as_u64).unwrap_or(0);
                        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");

                        let params = payload.get("params").cloned().unwrap_or(Value::Null);
                        let reply = match method {
                            "public/login" => {
                                let login_count = {
                                    let mut frames = state.login_frames.lock().await;
                                    frames.push(payload.clone());
                                    frames.len()
                                };
                                let login_delay_ms = state.login_delay_ms.load(Ordering::SeqCst);
                                if login_count > 1 && login_delay_ms > 0 {
                                    state.login_delay_started.store(true, Ordering::SeqCst);
                                    tokio::time::sleep(Duration::from_millis(login_delay_ms as u64)).await;
                                }
                                let reject_reconnect = login_count > 1
                                    && state
                                        .login_failures_after_first
                                        .try_update(
                                            Ordering::SeqCst,
                                            Ordering::SeqCst,
                                            |remaining| remaining.checked_sub(1),
                                        )
                                        .is_ok();

                                if reject_reconnect {
                                    json!({
                                        "id": id,
                                        "error": {"code": -32602, "message": "bad signature"},
                                    })
                                } else {
                                    json!({"id": id, "result": {"success": true}})
                                }
                            }
                            "subscribe" => {
                                state.subscribe_frames.lock().await.push(payload.clone());
                                let subscribe_delay_ms =
                                    state.subscribe_delay_ms.load(Ordering::SeqCst);
                                if subscribe_delay_ms > 0 {
                                    state
                                        .delayed_subscribe_acknowledged
                                        .store(false, Ordering::SeqCst);
                                    tokio::time::sleep(Duration::from_millis(
                                        subscribe_delay_ms as u64,
                                    ))
                                    .await;
                                    state
                                        .delayed_subscribe_acknowledged
                                        .store(true, Ordering::SeqCst);
                                }
                                let channels = payload
                                    .get("params")
                                    .and_then(|p| p.get("channels"))
                                    .and_then(Value::as_array)
                                    .cloned()
                                    .unwrap_or_default();

                                if let Some(status) = state.subscribe_status.lock().await.clone() {
                                    let current_subscriptions = channels
                                        .iter()
                                        .filter(|channel| {
                                            channel
                                                .as_str()
                                                .and_then(|channel| status.get(channel))
                                                .is_some_and(|status| status == "ok")
                                        })
                                        .cloned()
                                        .collect::<Vec<_>>();
                                    json!({
                                        "id": id,
                                        "result": {
                                            "current_subscriptions": current_subscriptions,
                                            "status": status,
                                        },
                                    })
                                } else {
                                    json!({"id": id, "result": {"channels": channels}})
                                }
                            }
                            "private/order" => {
                                let received_at_secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("system time is after unix epoch")
                                    .as_secs();
                                state.submitted_orders.lock().await.push(params);
                                state
                                    .submitted_order_received_at_secs
                                    .lock()
                                    .await
                                    .push(received_at_secs);
                                ws_reply(id, &state.order_reply, || {
                                    json!({"result": {"order": sample_order_json()}})
                                })
                                .await
                            }
                            "private/trigger_order" => {
                                state
                                    .submitted_trigger_orders
                                    .lock()
                                    .await
                                    .push(params.clone());
                                ws_reply(id, &state.trigger_order_reply, || {
                                    json!({
                                        "result": {
                                            "order": trigger_order_json_with(
                                                "trig-mock-1",
                                                params
                                                    .get("label")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("STRAT-TRIGGER-1"),
                                                params
                                                    .get("direction")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("buy"),
                                                params
                                                    .get("instrument_name")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("ETH-PERP"),
                                                1_700_000_001_000_i64,
                                                params
                                                    .get("order_type")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("market"),
                                                "untriggered",
                                                params
                                                    .get("limit_price")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("3500"),
                                                params
                                                    .get("trigger_price")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("3450"),
                                                params
                                                    .get("trigger_price_type")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("mark"),
                                                params
                                                    .get("trigger_type")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or("stoploss"),
                                            ),
                                        }
                                    })
                                })
                                .await
                            }
                            "private/replace" => {
                                state.replace_orders.lock().await.push(params.clone());
                                ws_reply(id, &state.replace_reply, || {
                                    let label = params
                                        .get("label")
                                        .and_then(Value::as_str)
                                        .unwrap_or("STRAT-O-1");
                                    let order_id_to_cancel = params
                                        .get("order_id_to_cancel")
                                        .and_then(Value::as_str)
                                        .unwrap_or("ord-stale-1");
                                    json!({
                                        "result": {
                                            "order": order_json_with(
                                                "ord-replaced-1",
                                                label,
                                                "buy",
                                                "ETH-PERP",
                                                1_700_000_001_000_i64,
                                                "open",
                                            ),
                                            "cancelled_order": order_json_with(
                                                order_id_to_cancel,
                                                label,
                                                "buy",
                                                "ETH-PERP",
                                                1_700_000_000_000_i64,
                                                "cancelled",
                                            ),
                                        }
                                    })
                                })
                                .await
                            }
                            "private/cancel" => {
                                state.cancelled_orders.lock().await.push(params);
                                ws_reply(id, &state.cancel_reply, || json!({"result": {}})).await
                            }
                            "private/cancel_trigger_order" => {
                                state
                                    .cancelled_trigger_orders
                                    .lock()
                                    .await
                                    .push(params.clone());
                                ws_reply(id, &state.cancel_trigger_reply, || {
                                    json!({
                                        "result": trigger_order_json_with(
                                            params
                                                .get("order_id")
                                                .and_then(Value::as_str)
                                                .unwrap_or("trig-mock-1"),
                                            "STRAT-TRIGGER-1",
                                            "buy",
                                            "ETH-PERP",
                                            1_700_000_002_000_i64,
                                            "market",
                                            "cancelled",
                                            "3500",
                                            "3450",
                                            "mark",
                                            "stoploss",
                                        )
                                    })
                                })
                                .await
                            }
                            "private/cancel_by_instrument" => {
                                state
                                    .cancel_by_instrument_calls
                                    .lock()
                                    .await
                                    .push(params);
                                ws_reply(id, &state.cancel_by_instrument_reply, || {
                                    json!({"result": {"cancelled_orders": 1}})
                                })
                                .await
                            }
                            "private/cancel_by_label" => {
                                state.cancelled_labels.lock().await.push(params);
                                ws_reply(id, &state.cancel_by_label_reply, || {
                                    json!({"result": {"cancelled_orders": 1}})
                                })
                                .await
                            }
                            "private/cancel_all" => {
                                state.cancel_all_calls.lock().await.push(params);
                                json!({"id": id, "result": {}})
                            }
                            _ => json!({"id": id, "result": {}}),
                        };

                        if method == "private/replace"
                            && let Some(notification) = state
                                .replace_notification_before_reply
                                .lock()
                                .await
                                .take()
                        {
                            if socket
                                .send(Message::Text(notification.to_string().into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }

                        if socket
                            .send(Message::Text(reply.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }

                        if method == "subscribe"
                            && state.disconnect_after_subscribe.swap(false, Ordering::SeqCst)
                        {
                            let _ = socket.send(Message::Close(None)).await;
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            notif = recv_notification(&mut notification_rx) => {
                let Some(notif) = notif else { continue };
                if socket
                    .send(Message::Text(notif.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    state.connection_count.fetch_sub(1, Ordering::SeqCst);
}

async fn recv_notification(
    rx: &mut Option<tokio::sync::mpsc::UnboundedReceiver<Value>>,
) -> Option<Value> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Builds a JSON-RPC reply for a captured `private/*` frame: the injected reply
/// body merged with `id` when set, otherwise the default success result.
async fn ws_reply(
    id: u64,
    injected: &Arc<tokio::sync::Mutex<Option<Value>>>,
    default: impl FnOnce() -> Value,
) -> Value {
    let mut reply = injected.lock().await.clone().unwrap_or_else(default);
    if let Value::Object(map) = &mut reply {
        map.insert("id".to_string(), json!(id));
    }
    reply
}

async fn start_ws_server(state: WsState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/ws", get(handle_ws_upgrade))
        .route("/health", get(handle_rest_health))
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    wait_for_http_health(addr).await;
    addr
}

fn rest_url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

fn ws_url(addr: SocketAddr) -> String {
    format!("ws://{addr}/ws")
}

fn sample_instrument_json() -> Value {
    json!({
        "amount_step": "0.001",
        "base_asset_address": "0x000000000000000000000000000000000000abcd",
        "base_asset_sub_id": "42",
        "base_currency": "ETH",
        "base_fee": "0",
        "instrument_name": "ETH-PERP",
        "instrument_type": "perp",
        "is_active": true,
        "maker_fee_rate": "0.0001",
        "mark_price_fee_rate_cap": null,
        "maximum_amount": "1000",
        "minimum_amount": "0.001",
        "option_details": null,
        "perp_details": {
            "aggregate_funding": "0",
            "funding_rate": "0",
            "index": "ETH-USD",
            "max_rate_per_hour": "0.01",
            "min_rate_per_hour": "-0.01",
            "static_interest_rate": "0",
        },
        "quote_currency": "USDC",
        "scheduled_activation": 0,
        "scheduled_deactivation": 32503680000000_i64,
        "taker_fee_rate": "0.0005",
        "tick_size": "0.01",
    })
}

fn sample_ticker_json(instrument_name: &str, timestamp_ms: i64) -> Value {
    json!({
        "instrument_name": instrument_name,
        "best_ask_amount": "1.0",
        "best_ask_price": "3501.00",
        "best_bid_amount": "1.0",
        "best_bid_price": "3500.00",
        "funding_rate": "0",
        "index_price": "3500",
        "mark_price": "3500",
        "max_price": "5000",
        "min_price": "1",
        "timestamp": timestamp_ms,
    })
}

fn option_instrument_json(instrument_name: &str, option_type: &str, strike: &str) -> Value {
    json!({
        "amount_step": "0.01",
        "base_asset_address": "0x0000000000000000000000000000000000000001",
        "base_asset_sub_id": "12345",
        "base_currency": "ETH",
        "base_fee": "1",
        "instrument_name": instrument_name,
        "instrument_type": "option",
        "is_active": true,
        "maker_fee_rate": "0",
        "mark_price_fee_rate_cap": null,
        "maximum_amount": "100",
        "minimum_amount": "0.01",
        "option_details": {
            "expiry": 1_782_000_000_i64,
            "index": "ETH-USD",
            "option_type": option_type,
            "settlement_price": null,
            "strike": strike,
        },
        "perp_details": null,
        "quote_currency": "USDC",
        "scheduled_activation": 1_700_000_000_000_i64,
        "scheduled_deactivation": 32503680000000_i64,
        "taker_fee_rate": "0.001",
        "tick_size": "1",
    })
}

fn spot_instrument_json(instrument_name: &str) -> Value {
    json!({
        "amount_step": "0.01",
        "base_asset_address": "0x41675b7746AE0E464f2594d258CF399c392A179C",
        "base_asset_sub_id": "0",
        "base_currency": "ETH",
        "base_fee": "0",
        "instrument_name": instrument_name,
        "instrument_type": "erc20",
        "is_active": true,
        "maker_fee_rate": "0",
        "mark_price_fee_rate_cap": null,
        "maximum_amount": "10000",
        "minimum_amount": "0.1",
        "option_details": null,
        "perp_details": null,
        "quote_currency": "USDC",
        "scheduled_activation": 0,
        "scheduled_deactivation": 32503680000000_i64,
        "taker_fee_rate": "0",
        "tick_size": "0.1",
    })
}

fn sample_order_json() -> Value {
    json!({
        "amount": "1",
        "average_price": "3500",
        "cancel_reason": "",
        "creation_timestamp": 1_700_000_000_000_i64,
        "direction": "buy",
        "filled_amount": "0",
        "instrument_name": "ETH-PERP",
        "is_transfer": false,
        "label": "STRAT-O-1",
        "last_update_timestamp": 1_700_000_001_000_i64,
        "limit_price": "3500",
        "max_fee": "1",
        "mmp": false,
        "nonce": 1,
        "order_fee": "0",
        "order_id": "ord-mock-1",
        "order_status": "open",
        "order_type": "limit",
        "signature": "0x00",
        "signature_expiry_sec": 1_700_000_900,
        "signer": "0xsigner",
        "subaccount_id": TEST_SUBACCOUNT,
        "time_in_force": "gtc",
    })
}

fn order_json_with(
    order_id: &str,
    label: &str,
    direction: &str,
    instrument_name: &str,
    last_update_ms: i64,
    status: &str,
) -> Value {
    json!({
        "amount": "1",
        "average_price": "3500",
        "cancel_reason": "",
        "creation_timestamp": 1_700_000_000_000_i64,
        "direction": direction,
        "filled_amount": "0",
        "instrument_name": instrument_name,
        "is_transfer": false,
        "label": label,
        "last_update_timestamp": last_update_ms,
        "limit_price": "3500",
        "max_fee": "1",
        "mmp": false,
        "nonce": 1,
        "order_fee": "0",
        "order_id": order_id,
        "order_status": status,
        "order_type": "limit",
        "signature": "0x00",
        "signature_expiry_sec": 1_700_000_900,
        "signer": "0xsigner",
        "subaccount_id": TEST_SUBACCOUNT,
        "time_in_force": "gtc",
    })
}

#[expect(clippy::too_many_arguments)]
fn trigger_order_json_with(
    order_id: &str,
    label: &str,
    direction: &str,
    instrument_name: &str,
    last_update_ms: i64,
    order_type: &str,
    status: &str,
    limit_price: &str,
    trigger_price: &str,
    trigger_price_type: &str,
    trigger_type: &str,
) -> Value {
    json!({
        "amount": "1",
        "average_price": "0",
        "cancel_reason": "",
        "creation_timestamp": 1_700_000_000_000_i64,
        "direction": direction,
        "filled_amount": "0",
        "instrument_name": instrument_name,
        "is_transfer": false,
        "label": label,
        "last_update_timestamp": last_update_ms,
        "limit_price": limit_price,
        "max_fee": "1",
        "mmp": false,
        "nonce": 1,
        "order_fee": "0",
        "order_id": order_id,
        "order_status": status,
        "order_type": order_type,
        "signature": "0x00",
        "signature_expiry_sec": 1_702_678_400,
        "signer": "0xsigner",
        "subaccount_id": TEST_SUBACCOUNT,
        "time_in_force": "gtc",
        "trigger_price": trigger_price,
        "trigger_price_type": trigger_price_type,
        "trigger_type": trigger_type,
    })
}

fn sample_trade_json(trade_id: &str, order_id: &str, instrument_name: &str) -> Value {
    trade_json_with_label(trade_id, order_id, instrument_name, "STRAT-O-1")
}

fn trade_json_with_label(
    trade_id: &str,
    order_id: &str,
    instrument_name: &str,
    label: &str,
) -> Value {
    json!({
        "direction": "buy",
        "index_price": "3500",
        "instrument_name": instrument_name,
        "is_transfer": false,
        "label": label,
        "liquidity_role": "taker",
        "mark_price": "3500",
        "order_id": order_id,
        "quote_id": null,
        "realized_pnl": "0",
        "subaccount_id": TEST_SUBACCOUNT,
        "timestamp": 1_700_000_002_000_i64,
        "trade_amount": "1",
        "trade_fee": "0.5",
        "trade_id": trade_id,
        "trade_price": "3505",
        "tx_hash": "0xabc",
        "tx_status": "settled",
        "wallet": "0xwallet",
    })
}

fn sample_position_json(instrument_name: &str, amount: &str) -> Value {
    json!({
        "amount": amount,
        "average_price": "3500",
        "creation_timestamp": 1_700_000_000_000_i64,
        "cumulative_funding": "0",
        "delta": "1",
        "gamma": "0",
        "index_price": "3500",
        "initial_margin": "100",
        "instrument_name": instrument_name,
        "instrument_type": "perp",
        "leverage": null,
        "liquidation_price": null,
        "maintenance_margin": "50",
        "mark_price": "3500",
        "mark_value": "3500",
        "net_settlements": "0",
        "open_orders_margin": "0",
        "pending_funding": "0",
        "realized_pnl": "0",
        "theta": "0",
        "unrealized_pnl": "0",
        "vega": "0",
    })
}

fn sample_subaccount_json() -> Value {
    json!({
        "collaterals": [{
            "amount": "1000",
            "asset_name": "USDC",
            "asset_type": "erc20",
            "cumulative_interest": "0",
            "currency": "USDC",
            "initial_margin": "100",
            "maintenance_margin": "50",
            "mark_price": "1",
            "mark_value": "1000",
            "pending_interest": "0",
        }],
        "collaterals_initial_margin": "100",
        "collaterals_maintenance_margin": "50",
        "collaterals_value": "1000",
        "currency": "USDC",
        "initial_margin": "100",
        "is_under_liquidation": false,
        "maintenance_margin": "50",
        "margin_type": "SM",
        "open_orders": [],
        "open_orders_margin": "0",
        "positions": [],
        "positions_initial_margin": "0",
        "positions_maintenance_margin": "0",
        "positions_value": "0",
        "subaccount_id": TEST_SUBACCOUNT,
        "subaccount_value": "1000",
    })
}

fn test_config(rest: SocketAddr, ws: SocketAddr) -> DeriveExecutionClientConfig {
    DeriveExecutionClientConfig {
        account_id: AccountId::from("DERIVE-001"),
        wallet_address: Some(TEST_WALLET.to_string()),
        session_key: Some(TEST_SESSION_KEY.into()),
        subaccount_id: Some(TEST_SUBACCOUNT),
        base_url_rest: Some(rest_url(rest)),
        base_url_ws: Some(ws_url(ws)),
        proxy_url: None,
        environment: DeriveEnvironment::Testnet,
        http_timeout_secs: 5,
        max_retries: 1,
        retry_delay_initial_ms: 50,
        retry_delay_max_ms: 500,
        ws_timeout_secs: Some(30),
        max_fee_per_contract: Some(dec!(1000)),
        transport_backend: TransportBackend::default(),
        domain_separator: Some(TEST_DOMAIN_SEPARATOR.to_string()),
        action_typehash: Some(TEST_ACTION_TYPEHASH.to_string()),
        trade_module_address: Some(TEST_TRADE_MODULE_ADDRESS.to_string()),
        signature_expiry_secs: 600,
        market_order_slippage_bps: 50,
        max_matching_requests_per_second: None,
        max_per_instrument_matching_requests_per_second: None,
        rest_fallback_enabled: false,
        private_state_poll_interval_secs: Some(10),
        max_account_state_age_secs: Some(30),
        max_quote_age_secs: Some(30),
    }
}

#[rstest]
#[tokio::test]
async fn test_submit_order_uses_single_rest_write_after_ws_authentication_is_lost() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;

    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    add_fresh_quote_to_cache(&tc.cache, InstrumentId::from("ETH-PERP.DERIVE"));

    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-SUBMIT"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "REST submit request",
    )
    .await;

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted from REST submit response",
    )
    .await;
    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = event {
        assert_eq!(accepted.client_order_id, order.client_order_id());
        assert_eq!(
            accepted.venue_order_id,
            VenueOrderId::from("ord-rest-submit-1")
        );
    }

    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert!(ws_state.submitted_orders.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_reduce_only_submit_uses_rest_after_fresh_private_reconciliation() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    let order = build_reduce_only_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-REDUCE-ONLY"),
        OrderSide::Sell,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("reduce-only submit command accepted");

    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "REST reduce-only submit request",
    )
    .await;
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert_eq!(
        rest_state.submitted_orders.lock().await[0]["reduce_only"],
        true,
    );
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_rest_submit_blocks_a_second_risk_increasing_order() {
    let rest_state = RestState::default();
    rest_state
        .ambiguous_submit_response
        .store(true, Ordering::SeqCst);
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);

    let first = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-AMBIGUOUS-1"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    let second = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-AMBIGUOUS-2"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    for order in [&first, &second] {
        tc.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("cache insert");
    }

    tc.client
        .submit_order(submit_cmd(&first))
        .expect("first submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous REST submit request",
    )
    .await;
    let resolved_to_cancel = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-RESOLVED-CANCEL-WHILE-UNRESOLVED"),
        OrderSide::Buy,
        Price::from("3490.00"),
        Quantity::from("1.000"),
    );
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            resolved_to_cancel,
            VenueOrderId::from("ord-rest-resolved-cancel-while-unresolved"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.client
        .cancel_order(CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            ClientOrderId::from("STRAT-REST-RESOLVED-CANCEL-WHILE-UNRESOLVED"),
            Some(VenueOrderId::from(
                "ord-rest-resolved-cancel-while-unresolved",
            )),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("resolved cancellation is accepted despite unrelated unresolved write");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "REST cancel for a resolved order while another order is unresolved",
    )
    .await;
    tc.client
        .submit_order(submit_cmd(&second))
        .expect("second submit is handled as a denied command");
    drain_denied_without_submitted(&mut tc.rx, &second.client_order_id()).await;

    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

/// Builds a REST-fallback client already in REST execution mode.
async fn rest_fallback_client(
    rest_state: &RestState,
    ws_state: &WsState,
    configure: impl FnOnce(DeriveExecutionClientConfig) -> DeriveExecutionClientConfig,
) -> TestClient {
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            configure(config)
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, rest_state).await;
    tc
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_websocket_write_blocks_the_next_rest_entry() {
    // The routing decision is taken before the write. When the socket dies in
    // between, the possibly-live WebSocket order must still register as
    // unresolved, or the next command — now routed through REST — passes the
    // entry gate while that order may be working.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal error"},
    }));
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let ambiguous = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-AMBIGUOUS"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(ambiguous.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&ambiguous))
        .expect("ambiguous WebSocket submit accepted");
    wait_until(
        || {
            let ws = ws_state.clone();
            async move { !ws.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous WebSocket submit request",
    )
    .await;
    assert_no_terminal_rejection(&mut tc.rx, &ambiguous.client_order_id()).await;

    // Now lose the socket so the next command routes through REST.
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { !rest.order_history_calls.lock().await.is_empty() }
        },
        "REST reconciliation after the socket was lost",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let blocked = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-AMBIGUOUS-NEXT"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(blocked.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&blocked))
        .expect("next entry command handled");
    drain_denied_without_submitted(&mut tc.rx, &blocked.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_startup_login_failure_enters_rest_fallback_instead_of_failing_connect() {
    // The design's trigger is "disconnected **or** unauthenticated": a socket
    // that upgrades but cannot authenticate must start in REST fallback.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    *ws_state.subscribe_status.lock().await = Some(HashMap::from([(
        format!("{TEST_SUBACCOUNT}.orders"),
        "unauthorized".to_string(),
    )]));
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;

    tc.client
        .connect()
        .await
        .expect("REST fallback allows startup when the session cannot authenticate");
    assert!(tc.client.is_connected());
    wait_until(
        || {
            let rest = rest_state.clone();
            async move {
                !rest.order_history_calls.lock().await.is_empty()
                    && !rest.positions_calls.lock().await.is_empty()
            }
        },
        "private REST reconciliation after an unauthenticated startup",
    )
    .await;
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_unresolvable_write_halts_entries_forever_but_websocket_routing_recovers() {
    // A write whose response was lost has no positive evidence to find, and no
    // elapsed time proves a signed request cannot still land. So it is never
    // retried and never retired: risk-increasing entries stay halted on *both*
    // transports until the process restarts. Routing, however, must still
    // return to the WebSocket, or the client would be stranded in REST mode.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config.max_account_state_age_secs = Some(30);
            config.max_quote_age_secs = Some(30);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");

    // Lose the session so writes route over REST.
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { !rest.order_history_calls.lock().await.is_empty() }
        },
        "REST reconciliation after the session was lost",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    rest_state
        .fail_submit_requests
        .store(true, Ordering::SeqCst);
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let lost = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-LOST-WRITE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(lost.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&lost))
        .expect("lost submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous REST submit request",
    )
    .await;
    assert_no_terminal_rejection(&mut tc.rx, &lost.client_order_id()).await;

    rest_state
        .fail_submit_requests
        .store(false, Ordering::SeqCst);

    // Several reconciliations pass with no trace of the write. Nothing may
    // retire it, and nothing may be re-sent.
    let rest_writes_after_loss = rest_state.submitted_orders.lock().await.len();
    let polls_after_loss = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { rest.order_history_calls.lock().await.len() >= polls_after_loss + 3 }
        },
        "complete reconciliations with no trace of the lost write",
    )
    .await;
    assert_eq!(
        rest_state.submitted_orders.lock().await.len(),
        rest_writes_after_loss,
        "the ambiguous write must never be retried",
    );

    // The venue's session comes back and the supervisor redials it.
    let connections_before_recovery = ws_state.connection_count.load(Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(0, Ordering::SeqCst);
    wait_until_async(
        || {
            let ws = ws_state.clone();
            async move { ws.connection_count.load(Ordering::SeqCst) > connections_before_recovery }
        },
        Duration::from_secs(20),
    )
    .await;

    // The unresolved write halts risk-increasing entries, and keeps halting
    // them once the WebSocket is back. The refused command is also what tells
    // the client the reconciliation has been applied, which is the point at
    // which routing may return to the WebSocket.
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let blocked = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-ENTRY-AFTER-LOSS"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(blocked.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&blocked))
        .expect("entry command handled");
    let reason = denied_reason(&mut tc.rx, &blocked.client_order_id()).await;
    assert!(
        reason.contains("unknown venue outcome"),
        "unexpected denial reason: {reason}",
    );

    // Routing recovered: an exposure-reducing order now goes over the
    // WebSocket, which is only reachable once fallback has exited.
    let exit = build_reduce_only_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-REDUCING-AFTER-LOSS"),
        OrderSide::Sell,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(exit.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&exit))
        .expect("reduce-only command accepted");
    wait_until_async(
        || {
            let ws = ws_state.clone();
            async move { !ws.submitted_orders.lock().await.is_empty() }
        },
        Duration::from_secs(15),
    )
    .await;

    assert_eq!(
        rest_state.submitted_orders.lock().await.len(),
        rest_writes_after_loss,
        "nothing was re-sent over REST",
    );
    assert_eq!(ws_state.submitted_orders.lock().await.len(), 1);
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_replace_resolves_but_entries_wait_for_the_replacement_to_be_applied() {
    // Two properties in one run. The ambiguous replace is resolved by the stale
    // venue order id going terminal, even though the replacement is left open
    // and the history legs have lost their shared label. And the entry gate
    // stays closed until the engine has actually applied that replacement to
    // the cache: until then the cached order still carries the old venue id and
    // quantity, so sizing a new entry would use pre-reconciliation state.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let label = "STRAT-REST-AMBIGUOUS-REPLACE";
    let client_order_id = ClientOrderId::from(label);
    let old_venue_order_id = VenueOrderId::from("ord-rest-replace-ambiguous-a");
    let new_venue_order_id = VenueOrderId::from("ord-rest-replace-ambiguous-b");

    // The venue reports the order about to be replaced, so the generation
    // agrees with the cache and the replace is admitted.
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [order_json_with(
            old_venue_order_id.as_str(),
            label,
            "buy",
            "ETH-PERP",
            1_700_000_001_000_i64,
            "open",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    rest_state
        .fail_replace_requests
        .store(true, Ordering::SeqCst);

    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.private_state_poll_interval_secs = Some(1);
        config.max_account_state_age_secs = Some(30);
        config.max_quote_age_secs = Some(30);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            old_venue_order_id,
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            client_order_id,
            Some(old_venue_order_id),
            Some(Quantity::from("2.000")),
            Some(Price::from("3505.00")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("replace command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.replaced_orders.lock().await.is_empty() }
        },
        "ambiguous REST replace request",
    )
    .await;

    // Venue state after the replace: the old leg is cancelled and the
    // replacement is open at the raised quantity. Neither history leg carries
    // the label, because two venue orders share it without a replace link, so
    // the targeted venue order id is the only usable evidence.
    let old_leg = order_json_with(
        old_venue_order_id.as_str(),
        label,
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "cancelled",
    );
    let mut replacement = order_json_with(
        new_venue_order_id.as_str(),
        label,
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "open",
    );
    replacement["amount"] = json!("2");
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [replacement.clone()],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [old_leg, replacement],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });

    let polls_after_replace = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.order_history_calls.lock().await.len() >= polls_after_replace + 2 }
        },
        "reconciliations observing the replaced legs",
    )
    .await;

    // The replace is resolved, but the cache has not been updated: the entry
    // must still be refused.
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let premature = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-ENTRY-BEFORE-APPLY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(premature.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&premature))
        .expect("premature entry command handled");
    drain_denied_without_submitted(&mut tc.rx, &premature.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());

    // Now apply the replacement the way the engine would.
    tc.cache
        .borrow_mut()
        .update_order(&OrderEventAny::Updated(OrderUpdated::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-1"),
            instrument_id,
            client_order_id,
            Quantity::from("2.000"),
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            false,
            Some(new_venue_order_id),
            Some(AccountId::from("DERIVE-001")),
            Some(Price::from("3505.00")),
            None,
            None,
            false,
        )))
        .expect("apply replacement to the cache");

    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let entry = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-ENTRY-AFTER-APPLY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("entry command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "entry admitted once the replacement was applied",
    )
    .await;
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_in_flight_write_is_never_released_by_empty_reconciliations() {
    // A request held at the venue can still land. Complete reads that show no
    // trace of it prove nothing, so it must keep blocking entries until its own
    // response arrives — and then be applied exactly once.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.private_state_poll_interval_secs = Some(1);
        config.max_account_state_age_secs = Some(30);
        config.max_quote_age_secs = Some(30);
        // The request must outlive several reconciliations without the client
        // timing it out, so the test observes a genuinely in-flight write.
        config.http_timeout_secs = 30;
        config
    })
    .await;
    rest_state.submit_delay_ms.store(5_000, Ordering::SeqCst);

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let pending = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-PENDING-WRITE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(pending.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&pending))
        .expect("pending submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "held REST submit request",
    )
    .await;

    // Several complete reconciliations run while the request is still in flight.
    let polls_before = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.order_history_calls.lock().await.len() >= polls_before + 3 }
        },
        "complete empty reconciliations while the write is in flight",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let blocked = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-PENDING-WRITE-BLOCKED"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(blocked.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&blocked))
        .expect("second entry command handled");
    let reason = denied_reason(&mut tc.rx, &blocked.client_order_id()).await;
    assert!(
        reason.contains("unknown venue outcome"),
        "unexpected denial reason: {reason}",
    );
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);

    // The held request finally lands and is applied once.
    let accepted = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted once the held write returns",
    )
    .await;
    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted {
        assert_eq!(accepted.client_order_id, pending.client_order_id());
    }
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_cancel_of_a_live_target_is_never_released_by_its_continued_presence() {
    // Seeing the cancellation's target still open is not evidence that the
    // cancellation never happened, and no amount of elapsed time makes it so.
    let rest_state = RestState::default();
    let label = "STRAT-REST-LIVE-TARGET-CANCEL";
    let mut current_leg = order_json_with(
        "ord-rest-leg-b",
        label,
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "open",
    );
    current_leg["replaced_order_id"] = json!("ord-rest-leg-a");
    let historical_leg = order_json_with(
        "ord-rest-leg-a",
        label,
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "cancelled",
    );
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [current_leg.clone()],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [historical_leg, current_leg],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    rest_state
        .fail_cancel_requests
        .store(true, Ordering::SeqCst);

    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.private_state_poll_interval_secs = Some(1);
        config.max_account_state_age_secs = Some(30);
        config.max_quote_age_secs = Some(30);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from(label);
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("ord-rest-leg-b"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.client
        .cancel_order(CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::from("ord-rest-leg-b")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("cancel command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "ambiguous REST cancel request",
    )
    .await;

    // Many reconciliations, every one of them showing the target explicitly
    // open and the historical leg terminal.
    let polls_after_cancel = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.order_history_calls.lock().await.len() >= polls_after_cancel + 4 }
        },
        "reconciliations showing the cancellation target still open",
    )
    .await;

    // A fresh quote and fresh, applied private state, so the only possible
    // reason to refuse is the unresolved cancellation itself.
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let entry = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-LIVE-TARGET-ENTRY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("entry command handled");
    let reason = denied_reason(&mut tc.rx, &entry.client_order_id()).await;
    assert!(
        reason.contains("unknown venue outcome"),
        "the refusal must be the unresolved cancellation, not a masked freshness failure: {reason}",
    );
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_incomplete_reports_keep_fallback_active_through_websocket_recovery() {
    // An incomplete reconciliation may not hand order routing back to the
    // WebSocket, where the REST admission gates would be skipped entirely.
    let rest_state = RestState::default();
    let mut unconvertible_fill = sample_trade_json("trade-f3-1", "ord-mock-1", "ETH-PERP");
    unconvertible_fill["trade_fee"] = json!("79228162514264337593543950335");
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [unconvertible_fill],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let ws_state = WsState::default();
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config.max_account_state_age_secs = Some(30);
            config.max_quote_age_secs = Some(30);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");

    // Lose and immediately regain the socket: recovery succeeds, but every
    // reconciliation drops a fill row.
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let ws = ws_state.clone();
            let rest = rest_state.clone();
            async move {
                ws.login_frames.lock().await.len() >= 2
                    && rest.order_history_calls.lock().await.len() >= 2
            }
        },
        "WebSocket recovery with incomplete REST reconciliation",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let order = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-F3-INCOMPLETE-ON-RECOVERY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(
        rest_state.submitted_orders.lock().await.is_empty(),
        "no REST write",
    );
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "and no WebSocket write either",
    );
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_write_on_a_healthy_socket_halts_the_next_websocket_entry() {
    // A JSON-RPC -32603 leaves the socket authenticated, so nothing about the
    // transport changes. The uncertainty must still halt the next entry even
    // though it would be routed straight over that healthy WebSocket.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal error"},
    }));
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config.max_account_state_age_secs = Some(30);
            config.max_quote_age_secs = Some(30);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let ambiguous = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-HEALTHY-AMBIGUOUS"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(ambiguous.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&ambiguous))
        .expect("ambiguous WebSocket submit accepted");
    wait_until(
        || {
            let ws = ws_state.clone();
            async move { !ws.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous WebSocket submit request",
    )
    .await;
    assert_no_terminal_rejection(&mut tc.rx, &ambiguous.client_order_id()).await;

    // The socket never dropped, so routing is still the WebSocket.
    *ws_state.order_reply.lock().await = None;
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let blocked = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-WS-HEALTHY-AMBIGUOUS-NEXT"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(blocked.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&blocked))
        .expect("next entry command handled");
    let reason = denied_reason(&mut tc.rx, &blocked.client_order_id()).await;
    assert!(
        reason.contains("unknown venue outcome"),
        "unexpected denial reason: {reason}",
    );
    assert_eq!(
        ws_state.submitted_orders.lock().await.len(),
        1,
        "no second WebSocket write",
    );
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    // The ambiguity also scheduled reconciliation, which is the only way
    // evidence about it can ever arrive.
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { !rest.order_history_calls.lock().await.is_empty() }
        },
        "reconciliation scheduled by the ambiguous WebSocket write",
    )
    .await;
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_healthy_start_then_exhausted_recovery_is_redialled_by_the_supervisor() {
    // One supervisor covers both startup routes. A healthy start parks it; a
    // later terminal session failure must still get a fresh dial, or the client
    // sits in REST mode for the rest of the process while reporting connected.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config.max_account_state_age_secs = Some(30);
            config.max_quote_age_secs = Some(30);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");
    // Exhaust the transport's own recovery.
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { !rest.order_history_calls.lock().await.is_empty() }
        },
        "REST fallback after the session recovery was exhausted",
    )
    .await;

    // The venue comes back; the supervisor must dial again and reconcile.
    let connections_before_recovery = ws_state.connection_count.load(Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(0, Ordering::SeqCst);
    wait_until_async(
        || {
            let ws = ws_state.clone();
            async move { ws.connection_count.load(Ordering::SeqCst) > connections_before_recovery }
        },
        Duration::from_secs(20),
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let order = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-SUPERVISOR-REDIAL-ENTRY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command accepted");
    wait_until_async(
        || {
            let ws = ws_state.clone();
            async move { !ws.submitted_orders.lock().await.is_empty() }
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_ambiguous_submit_is_resolved_by_a_fill_report_alone() {
    // A landed submit whose order report was dropped or had its label detached
    // still shows up as a fill, and that fill is positive evidence the write
    // reached the venue.
    let rest_state = RestState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    rest_state
        .fail_submit_requests
        .store(true, Ordering::SeqCst);
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.private_state_poll_interval_secs = Some(1);
        config.max_account_state_age_secs = Some(30);
        config.max_quote_age_secs = Some(30);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let label = "STRAT-REST-FILL-EVIDENCE";
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let ambiguous = build_limit_order(
        instrument_id,
        ClientOrderId::from(label),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(ambiguous.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&ambiguous))
        .expect("ambiguous submit accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous REST submit request",
    )
    .await;

    // No order report mentions the label; only a fill does.
    rest_state
        .fail_submit_requests
        .store(false, Ordering::SeqCst);
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [trade_json_with_label(
            "trade-fill-evidence-1",
            "ord-fill-evidence-1",
            "ETH-PERP",
            label,
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });

    let polls_before = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.order_history_calls.lock().await.len() >= polls_before + 2 }
        },
        "reconciliation carrying the fill evidence",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let entry = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-FILL-EVIDENCE-ENTRY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("entry command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.submitted_orders.lock().await.len() >= 2 }
        },
        "entry admitted after the fill resolved the ambiguous submit",
    )
    .await;
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_client_error_submit_is_definitive_and_does_not_halt_the_next_entry() {
    // A 4xx is thrown out before the matching engine sees it, so the write
    // definitively did not happen: reject it and leave the gate open.
    let rest_state = RestState::default();
    rest_state.submit_http_status.store(400, Ordering::SeqCst);
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.private_state_poll_interval_secs = Some(1);
        config.max_account_state_age_secs = Some(30);
        config.max_quote_age_secs = Some(30);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let rejected = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-CLIENT-ERROR"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(rejected.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&rejected))
        .expect("submit command accepted");
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected for a definitive 4xx",
    )
    .await;
    if let ExecutionEvent::Order(OrderEventAny::Rejected(event)) = event {
        assert_eq!(event.client_order_id, rejected.client_order_id());
    }

    // The gate is not halted: the next entry is admitted.
    rest_state.submit_http_status.store(0, Ordering::SeqCst);
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let entry = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-AFTER-CLIENT-ERROR"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("entry command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.submitted_orders.lock().await.len() >= 2 }
        },
        "entry admitted after a definitive rejection",
    )
    .await;
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_limit_entry_without_a_quote_is_denied_during_rest_fallback() {
    // Fresh private state is not enough: a risk-increasing limit order priced
    // with no current quote is exactly the fail-open case the gate must close.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |config| config).await;

    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-NO-QUOTE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_expired_quote_denies_a_limit_entry_during_rest_fallback() {
    // The quote bound is deliberately much tighter than the account-state
    // bound, so this proves the gate honours its own dedicated quote bound
    // rather than the (generous) private-state one.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.max_account_state_age_secs = Some(60);
        config.max_quote_age_secs = Some(2);
        config.private_state_poll_interval_secs = Some(1);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    // Let the quote expire while private reconciliation keeps polling, so the
    // only stale input is the quote.
    tokio::time::sleep(Duration::from_millis(2_200)).await;

    let order = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-STALE-QUOTE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_quote_gate_blocks_an_increasing_replace_but_not_a_reducing_one() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |config| config).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let increasing = ClientOrderId::from("STRAT-REST-REPLACE-UP");
    let reducing = ClientOrderId::from("STRAT-REST-REPLACE-DOWN");
    for (client_order_id, venue_order_id) in [
        (increasing, "ord-rest-replace-up"),
        (reducing, "ord-rest-replace-down"),
    ] {
        add_order_to_cache(
            &tc.cache,
            accepted_order(
                build_limit_order(
                    instrument_id,
                    client_order_id,
                    OrderSide::Buy,
                    Price::from("3500.00"),
                    Quantity::from("1.000"),
                ),
                VenueOrderId::from(venue_order_id),
                AccountId::from("DERIVE-001"),
            ),
            Some(ClientId::from("DERIVE")),
        );
    }

    // No quote in the cache: raising the buy price is risk-increasing and must
    // be refused, while shrinking the size cannot increase exposure.
    tc.client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            increasing,
            Some(VenueOrderId::from("ord-rest-replace-up")),
            Some(Quantity::from("1.000")),
            Some(Price::from("3510.00")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("increasing replace command handled");
    drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))
            )
        },
        "ModifyRejected for a price-increasing replace without a quote",
    )
    .await;
    assert!(rest_state.replaced_orders.lock().await.is_empty());

    tc.client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            reducing,
            Some(VenueOrderId::from("ord-rest-replace-down")),
            Some(Quantity::from("0.500")),
            Some(Price::from("3490.00")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("reducing replace command handled");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.replaced_orders.lock().await.is_empty() }
        },
        "REST replace for an exposure-reducing modify",
    )
    .await;
    assert_eq!(rest_state.replaced_orders.lock().await.len(), 1);
    assert!(ws_state.replace_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_partial_private_reports_keep_the_entry_gate_closed() {
    // A fill the venue sent but the adapter could not convert leaves exposure
    // unaccounted for, so the reconciliation is not a complete picture.
    let rest_state = RestState::default();
    let mut unconvertible_fill = sample_trade_json("trade-partial-1", "ord-mock-1", "ETH-PERP");
    unconvertible_fill["trade_fee"] = json!("79228162514264337593543950335");
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [unconvertible_fill],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |config| config).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let order = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-PARTIAL-REPORTS"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_slow_history_traversal_does_not_refresh_an_expired_account_snapshot() {
    // The account snapshot is fetched first and the history traversal can take
    // longer than the whole freshness budget; age must be measured from when
    // the snapshot was observed, not from when the traversal finished.
    let rest_state = RestState::default();
    rest_state
        .order_history_delay_ms
        .store(1_500, Ordering::SeqCst);
    let ws_state = WsState::default();
    let mut tc = rest_fallback_client(&rest_state, &ws_state, |mut config| {
        config.max_account_state_age_secs = Some(1);
        config.private_state_poll_interval_secs = Some(1);
        config
    })
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let order = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-SLOW-HISTORY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_market_orders_use_the_direct_ticker_when_the_bulk_map_is_empty() {
    // The outage this failover exists for returns an empty `public/get_tickers`
    // map, so a quote refresh built on the bulk endpoint rejects every market
    // order — entries and risk-reducing exits alike.
    let rest_state = RestState::default();
    *rest_state.ticker_response.lock().await = json!({"tickers": {}});
    *rest_state.direct_ticker_response.lock().await = fresh_ticker_json("ETH-PERP");
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);

    let entry = build_market_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-MARKET-ENTRY"),
        OrderSide::Buy,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("market entry command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "REST market entry write",
    )
    .await;
    drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted from REST market entry",
    )
    .await;

    let exit = build_reduce_only_market_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-MARKET-EXIT"),
        OrderSide::Sell,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(exit.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&exit))
        .expect("market exit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.submitted_orders.lock().await.len() >= 2 }
        },
        "REST market exit write",
    )
    .await;

    let posts = rest_state.submitted_orders.lock().await;
    assert_eq!(posts.len(), 2, "each market order is written exactly once");
    assert_eq!(posts[0]["order_type"].as_str(), Some("market"));
    assert_eq!(posts[1]["reduce_only"], true);
    drop(posts);
    assert_eq!(rest_state.direct_ticker_calls.lock().await.len(), 2);
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_stale_direct_ticker_denies_a_market_order_without_writing() {
    let rest_state = RestState::default();
    // A valid-looking but long-expired direct ticker must not be signed against.
    *rest_state.direct_ticker_response.lock().await =
        sample_ticker_json("ETH-PERP", 1_700_000_000_013_i64);
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let order = build_market_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-MARKET-STALE"),
        OrderSide::Buy,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("market command handled");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected for a stale direct ticker",
    )
    .await;
    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, order.client_order_id());
        assert!(
            rejected
                .reason
                .contains("market-order quote refresh failed"),
            "unexpected reject reason: {}",
            rejected.reason,
        );
    }
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_older_replacement_leg_does_not_resolve_the_current_legs_ambiguous_cancel() {
    // After replacing A with B both venue orders legitimately keep the same
    // label, so a terminal report for A must not resolve an ambiguous cancel
    // of B: only evidence about B's own venue order id can.
    let rest_state = RestState::default();
    let label = "STRAT-REST-REPLACED-LEG-CANCEL";
    let mut current_leg = order_json_with(
        "ord-rest-leg-b",
        label,
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "open",
    );
    current_leg["replaced_order_id"] = json!("ord-rest-leg-a");
    let historical_leg = order_json_with(
        "ord-rest-leg-a",
        label,
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "cancelled",
    );
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [current_leg.clone()],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [historical_leg, current_leg],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    rest_state
        .fail_cancel_requests
        .store(true, Ordering::SeqCst);

    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from(label);
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("ord-rest-leg-b"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.client
        .cancel_order(CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::from("ord-rest-leg-b")),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("cancel command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "ambiguous REST cancel request",
    )
    .await;

    let polls_after_cancel = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.order_history_calls.lock().await.len() >= polls_after_cancel + 2 }
        },
        "reconciliations observing both replacement legs after the ambiguous cancel",
    )
    .await;

    // A fresh quote, so the refusal cannot be a masked quote-gate failure.
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let entry = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-REPLACED-LEG-ENTRY"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(entry.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&entry))
        .expect("entry command handled");

    let reason = denied_reason(&mut tc.rx, &entry.client_order_id()).await;
    assert!(
        reason.contains("unknown venue outcome"),
        "the historical leg must not resolve the current leg's cancellation: {reason}",
    );
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_internal_error_rest_submit_keeps_identity_and_blocks_the_next_entry() {
    // JSON-RPC -32603 is the venue's internal error: the matching engine may
    // have accepted the order before the response failed, so the write outcome
    // is ambiguous and must not produce a terminal rejection.
    let rest_state = RestState::default();
    *rest_state.submit_error_response.lock().await =
        Some(json!({"error": {"code": -32603, "message": "Internal error"}}));
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);

    let first = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-INTERNAL-ERROR-1"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    let second = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-REST-INTERNAL-ERROR-2"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    for order in [&first, &second] {
        tc.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("cache insert");
    }

    tc.client
        .submit_order(submit_cmd(&first))
        .expect("first submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "internal-error REST submit request",
    )
    .await;
    assert_no_terminal_rejection(&mut tc.rx, &first.client_order_id()).await;

    tc.client
        .submit_order(submit_cmd(&second))
        .expect("second submit is handled as a denied command");
    drain_denied_without_submitted(&mut tc.rx, &second.client_order_id()).await;

    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_rest_fallback_stays_active_until_reconnect_subscriptions_and_reconciliation_finish() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");

    let subscriptions_before_reconnect = ws_state.subscribe_frames.lock().await.len();
    let polls_before_reconnect = rest_state.order_history_calls.lock().await.len();
    ws_state.subscribe_delay_ms.store(3_000, Ordering::SeqCst);
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let ws = ws_state.clone();
            async move {
                ws.login_frames.lock().await.len() >= 2
                    && ws.subscribe_frames.lock().await.len() > subscriptions_before_reconnect
            }
        },
        "authenticated reconnect subscription request before its delayed acknowledgement",
    )
    .await;
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { rest.order_history_calls.lock().await.len() >= polls_before_reconnect + 2 }
        },
        "two private reconciliations while reconnect subscription is pending",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, InstrumentId::from("ETH-PERP.DERIVE"));
    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-SUBSCRIPTION-GATE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit during reconnect is handled");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "REST submit while the recovered subscription is unconfirmed",
    )
    .await;

    assert!(ws_state.submitted_orders.lock().await.is_empty());
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert!(
        !ws_state
            .delayed_subscribe_acknowledged
            .load(Ordering::SeqCst),
        "REST submit must complete before the subscription acknowledgement"
    );
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_unresolved_rest_write_keeps_entries_blocked_after_websocket_recovery() {
    let rest_state = RestState::default();
    rest_state
        .ambiguous_submit_response
        .store(true, Ordering::SeqCst);
    let ws_state = WsState::default();
    ws_state.login_delay_ms.store(2_500, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    tc.client.connect().await.expect("initial connect succeeds");
    ws_state.disconnect_now.store(true, Ordering::SeqCst);
    wait_until(
        || {
            let ws = ws_state.clone();
            let rest = rest_state.clone();
            async move {
                ws.login_delay_started.load(Ordering::SeqCst)
                    && ws.login_frames.lock().await.len() >= 2
                    && !rest.order_history_calls.lock().await.is_empty()
            }
        },
        "REST reconciliation during delayed WebSocket recovery",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, InstrumentId::from("ETH-PERP.DERIVE"));
    let first = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-AMBIGUOUS-RECOVERY-1"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    let second = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-AMBIGUOUS-RECOVERY-2"),
        OrderSide::Buy,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    for order in [&first, &second] {
        tc.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("cache insert");
    }
    tc.client
        .submit_order(submit_cmd(&first))
        .expect("ambiguous submit command accepted");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "ambiguous REST submit request",
    )
    .await;

    ws_state.login_delay_ms.store(0, Ordering::SeqCst);
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { rest.order_history_calls.lock().await.len() >= 2 }
        },
        "reconnect reconciliation after ambiguous REST submit",
    )
    .await;

    tc.client
        .submit_order(submit_cmd(&second))
        .expect("second submit command handled");
    drain_denied_without_submitted(&mut tc.rx, &second.client_order_id()).await;
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_stale_private_rest_reconciliation_denies_new_entry() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.max_account_state_age_secs = Some(1);
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-STALE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("stale entry command is denied");

    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_failed_private_reconciliation_closes_entry_gate_immediately() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let calls_before_outage = rest_state.get_subaccount_calls.lock().await.len();
    rest_state
        .fail_subaccount_requests
        .store(true, Ordering::SeqCst);
    wait_until(
        || {
            let state = rest_state.clone();
            async move { state.get_subaccount_calls.lock().await.len() >= calls_before_outage + 2 }
        },
        "failed REST account reconciliation",
    )
    .await;

    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-FAILED-RECONCILIATION"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("entry command handled");

    drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;
    assert!(rest_state.submitted_orders.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_initial_websocket_503_starts_rest_mode_and_retries_connection() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .initial_handshake_failures
        .store(1, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;

    tc.client
        .connect()
        .await
        .expect("REST fallback allows startup during initial WebSocket outage");
    assert!(tc.client.is_connected());
    wait_until(
        || {
            let ws = ws_state.clone();
            let rest = rest_state.clone();
            async move {
                ws.connection_count.load(Ordering::SeqCst) > 0
                    && !ws.login_frames.lock().await.is_empty()
                    && !rest.order_history_calls.lock().await.is_empty()
            }
        },
        "WebSocket retry after REST-mode startup",
    )
    .await;

    assert_eq!(
        ws_state.initial_handshake_failures.load(Ordering::SeqCst),
        0
    );
    assert!(!rest_state.order_history_calls.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_startup_retry_keeps_rest_mode_until_private_subscriptions_are_confirmed() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .initial_handshake_failures
        .store(1, Ordering::SeqCst);
    ws_state.subscribe_delay_ms.store(3_000, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config.private_state_poll_interval_secs = Some(1);
            config
        })
        .await;
    tc.client
        .connect()
        .await
        .expect("REST fallback allows startup during initial WebSocket outage");
    let polls_before_retry = rest_state.order_history_calls.lock().await.len();
    wait_until(
        || {
            let ws = ws_state.clone();
            async move {
                !ws.login_frames.lock().await.is_empty()
                    && !ws.subscribe_frames.lock().await.is_empty()
            }
        },
        "startup WebSocket login and delayed private subscription",
    )
    .await;
    wait_until(
        || {
            let rest = rest_state.clone();
            async move { rest.order_history_calls.lock().await.len() > polls_before_retry }
        },
        "private reconciliation before startup subscription acknowledgement",
    )
    .await;

    add_fresh_quote_to_cache(&tc.cache, InstrumentId::from("ETH-PERP.DERIVE"));
    let order = build_limit_order(
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-REST-STARTUP-SUBSCRIPTION-GATE"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit during startup retry is handled");
    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "REST submit while startup subscription is unconfirmed",
    )
    .await;

    assert!(ws_state.submitted_orders.lock().await.is_empty());
    assert_eq!(rest_state.submitted_orders.lock().await.len(), 1);
    tc.client.disconnect().await.expect("disconnect succeeds");
}

async fn wait_for_rest_execution_mode(tc: &mut TestClient, rest_state: &RestState) {
    tc.client.connect().await.expect("initial connect succeeds");
    wait_until(
        || {
            let state = rest_state.clone();
            async move {
                state.get_subaccount_calls.lock().await.len() > 1
                    && !state.order_history_calls.lock().await.is_empty()
                    && !state.trade_history_calls.lock().await.is_empty()
                    && !state.positions_calls.lock().await.is_empty()
            }
        },
        "private REST reconciliation",
    )
    .await;
    drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Report(ExecutionReport::MassStatus(_))
            )
        },
        "initial REST private-state reconciliation report",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_uses_rest_only_during_fallback_for_resolved_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-REST-CANCEL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            order,
            VenueOrderId::from("ord-rest-cancel-1"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-rest-cancel-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client
        .cancel_order(cancel)
        .expect("cancel command accepted");

    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "REST cancel request",
    )
    .await;
    let posts = rest_state.cancelled_orders.lock().await;
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["order_id"].as_str(), Some("ord-rest-cancel-1"));
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    drop(posts);
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_uses_rest_only_during_fallback_for_resolved_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    add_fresh_quote_to_cache(&tc.cache, instrument_id);
    let client_order_id = ClientOrderId::from("STRAT-REST-REPLACE");
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("ord-rest-replace-old"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.client
        .modify_order(ModifyOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            client_order_id,
            Some(VenueOrderId::from("ord-rest-replace-old")),
            Some(Quantity::from("2.000")),
            Some(Price::from("3505.00")),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("modify command accepted");

    wait_until(
        || {
            let state = rest_state.clone();
            async move { !state.replaced_orders.lock().await.is_empty() }
        },
        "REST replace request",
    )
    .await;
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Updated(_))),
        "OrderUpdated from REST replace",
    )
    .await;
    if let ExecutionEvent::Order(OrderEventAny::Updated(updated)) = event {
        assert_eq!(updated.client_order_id, client_order_id);
        assert_eq!(
            updated.venue_order_id,
            Some(VenueOrderId::from("ord-rest-replace-1"))
        );
    }
    assert!(ws_state.replace_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_cancel_trigger_order_is_denied_during_rest_fallback() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-REST-TRIGGER-CANCEL");
    let order = build_stop_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3400.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .expect("cache insert");
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("trig-rest-cancel-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client
        .cancel_order(cancel)
        .expect("cancel command handled");

    assert!(rest_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_is_denied_without_writes_during_rest_fallback() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    let mut tc =
        build_client_with_config(rest_state.clone(), ws_state.clone(), None, |mut config| {
            config.rest_fallback_enabled = true;
            config
        })
        .await;
    wait_for_rest_execution_mode(&mut tc, &rest_state).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-REST-CANCEL-ALL");
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                instrument_id,
                client_order_id,
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("ord-rest-cancel-all-1"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.cache.borrow_mut().build_index();
    tc.client
        .cancel_all_orders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            instrument_id,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("unsupported cancel-all command is handled");

    let event = drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
            )
        },
        "CancelRejected for unsupported cancel-all",
    )
    .await;
    assert!(matches!(
        event,
        ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
    ));
    assert!(rest_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());
    assert!(ws_state.cancel_by_instrument_calls.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect succeeds");
}

fn build_core(cache: Rc<RefCell<Cache>>) -> ExecutionClientCore {
    ExecutionClientCore::new(
        TraderId::from("TRADER-001"),
        ClientId::from("DERIVE"),
        *DERIVE_VENUE,
        OmsType::Netting,
        AccountId::from("DERIVE-001"),
        AccountType::Margin,
        None,
        cache,
    )
}

struct TestClient {
    client: DeriveExecutionClient,
    cache: Rc<RefCell<Cache>>,
    rx: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
}

async fn build_client(rest_state: RestState, ws_state: WsState) -> TestClient {
    build_client_with_config(rest_state, ws_state, None, |config| config).await
}

async fn build_client_with_config(
    rest_state: RestState,
    ws_state: WsState,
    registry: Option<&SocketReconnectRegistry>,
    configure: impl FnOnce(DeriveExecutionClientConfig) -> DeriveExecutionClientConfig,
) -> TestClient {
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
    replace_exec_event_sender(tx);

    let cache = Rc::new(RefCell::new(Cache::default()));
    // Pre-register the account so `connect()`'s `await_account_registered`
    // gate resolves immediately; the live runner populates the cache from
    // `refresh_account_state`'s `AccountState` event, but tests drive the
    // emitter directly.
    register_test_account(&cache, AccountId::from("DERIVE-001"));

    let config = configure(test_config(rest_addr, ws_addr));
    let client = || DeriveExecutionClient::new(build_core(cache.clone()), config);
    let mut client = match registry {
        Some(registry) => registry.scope(client),
        None => client(),
    }
    .expect("client creation succeeds");
    // start() installs the freshly-replaced event sender on the emitter, so
    // tests that drain the receiver must call it before any emit_*.
    client.start().expect("start succeeds");
    TestClient { client, cache, rx }
}

fn register_test_account(cache: &Rc<RefCell<Cache>>, account_id: AccountId) {
    let account_state = AccountState::new(
        account_id,
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from("10000.0 USDC"),
            Money::from("0 USDC"),
            Money::from("10000.0 USDC"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    let account = AccountAny::Margin(MarginAccount::new(account_state, true));
    cache.borrow_mut().add_account(account).unwrap();
}

async fn wait_until<F, Fut>(predicate: F, _label: &str)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    wait_until_async(predicate, Duration::from_secs(5)).await;
}

async fn drain_until<F>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    predicate: F,
    label: &str,
) -> ExecutionEvent
where
    F: Fn(&ExecutionEvent) -> bool,
{
    let deadline = Duration::from_secs(5);
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            let event = rx.recv().await?;
            if predicate(&event) {
                return Some(event);
            }
        }
    })
    .await
    .unwrap_or(None);

    match outcome {
        Some(event) => event,
        None => panic!("timeout waiting for: {label}"),
    }
}

/// Drains until `OrderDenied` for `client_order_id`, failing if `OrderSubmitted`
/// for the same order arrives first.
async fn drain_denied_without_submitted(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    client_order_id: &ClientOrderId,
) -> ExecutionEvent {
    let deadline = Duration::from_secs(5);
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            let event = rx.recv().await?;

            if let ExecutionEvent::Order(OrderEventAny::Submitted(submitted)) = &event
                && submitted.client_order_id == *client_order_id
            {
                panic!("OrderSubmitted emitted for {client_order_id} before OrderDenied");
            }

            if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = &event
                && denied.client_order_id == *client_order_id
            {
                return Some(event);
            }
        }
    })
    .await
    .unwrap_or(None);

    match outcome {
        Some(event) => event,
        None => panic!("timeout waiting for OrderDenied for {client_order_id}"),
    }
}

/// Seeds a current top-of-book quote, which a risk-increasing write in REST
/// fallback requires before it may be sent.
fn add_fresh_quote_to_cache(cache: &Rc<RefCell<Cache>>, instrument_id: InstrumentId) {
    let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let quote = QuoteTick::new(
        instrument_id,
        Price::from("3500.00"),
        Price::from("3501.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        now,
        now,
    );
    cache.borrow_mut().add_quote(quote).expect("quote insert");
}

/// `public/get_ticker` body timestamped now, so a REST-fallback market order
/// passes the maximum quote age.
fn fresh_ticker_json(instrument_name: &str) -> Value {
    let now_ms = nautilus_core::time::get_atomic_clock_realtime()
        .get_time_ms()
        .try_into()
        .expect("clock fits i64");
    sample_ticker_json(instrument_name, now_ms)
}

/// Drains until `OrderDenied` for `client_order_id` and returns its reason,
/// failing if `OrderSubmitted` for the same order arrives first.
async fn denied_reason(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    client_order_id: &ClientOrderId,
) -> String {
    let event = drain_denied_without_submitted(rx, client_order_id).await;
    match event {
        ExecutionEvent::Order(OrderEventAny::Denied(denied)) => denied.reason.to_string(),
        other => panic!("expected OrderDenied, got {other:?}"),
    }
}

/// Drains for `window` and fails if any terminal rejection for
/// `client_order_id` arrives: an ambiguous write must stay unresolved.
async fn assert_no_terminal_rejection(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    client_order_id: &ClientOrderId,
) {
    let window = Duration::from_millis(500);
    let _ = tokio::time::timeout(window, async {
        loop {
            let Some(event) = rx.recv().await else {
                return;
            };
            match &event {
                ExecutionEvent::Order(OrderEventAny::Rejected(rejected))
                    if rejected.client_order_id == *client_order_id =>
                {
                    panic!("OrderRejected emitted for ambiguous write {client_order_id}")
                }
                ExecutionEvent::Order(OrderEventAny::Denied(denied))
                    if denied.client_order_id == *client_order_id =>
                {
                    panic!("OrderDenied emitted for ambiguous write {client_order_id}")
                }
                _ => {}
            }
        }
    })
    .await;
}

fn build_limit_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    price: Price,
    quantity: Quantity,
) -> OrderAny {
    build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        side,
        price,
        quantity,
        TimeInForce::Gtc,
        false,
    )
}

fn build_limit_order_with_time_in_force(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    price: Price,
    quantity: Quantity,
    time_in_force: TimeInForce,
    post_only: bool,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .price(price)
        .time_in_force(time_in_force)
        .post_only(post_only)
        .build()
}

fn build_reduce_only_limit_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    price: Price,
    quantity: Quantity,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .price(price)
        .reduce_only(true)
        .build()
}

fn build_market_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    quantity: Quantity,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .build()
}

fn build_reduce_only_market_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    quantity: Quantity,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .reduce_only(true)
        .build()
}

fn build_stop_market_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    trigger_price: Price,
    quantity: Quantity,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::StopMarket)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .trigger_price(trigger_price)
        .trigger_type(TriggerType::MarkPrice)
        .build()
}

fn accepted_order(
    mut order: OrderAny,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
) -> OrderAny {
    order
        .apply(OrderEventAny::Accepted(OrderAccepted::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            venue_order_id,
            account_id,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(1),
            false,
        )))
        .expect("accept order");
    order
}

fn add_order_to_cache(cache: &Rc<RefCell<Cache>>, order: OrderAny, client_id: Option<ClientId>) {
    cache
        .borrow_mut()
        .add_order(order, None, client_id, false)
        .expect("cache insert");
}

fn build_limit_if_touched_order(
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    side: OrderSide,
    price: Price,
    trigger_price: Price,
    quantity: Quantity,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::LimitIfTouched)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(side)
        .quantity(quantity)
        .price(price)
        .trigger_price(trigger_price)
        .trigger_type(TriggerType::MarkPrice)
        .build()
}

fn submit_cmd(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::from_order(
        order,
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        None,
        UUID4::new(),
        UnixNanos::default(),
    )
}

fn make_subscription_frame(channel: &str, data: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": channel,
            "data": data,
        }
    })
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_subscribes_private_channels() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let (system_tx, mut system_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_system_event_sender(system_tx);
    let registry = SocketReconnectRegistry::default();
    let mut tc =
        build_client_with_config(rest_state, ws_state.clone(), Some(&registry), |config| {
            config
        })
        .await;

    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe frame received",
    )
    .await;

    let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    let endpoint = Ustr::from("derive-user-streams");
    let client_id = ClientId::from("DERIVE");
    let handle = registry.handle(client_id, endpoint).unwrap();

    assert_eq!(change.client_id, client_id);
    assert_eq!(change.venue, Some(*DERIVE_VENUE));
    assert_eq!(change.endpoint, endpoint);
    assert_eq!(change.state, SocketState::Connected);
    assert_eq!(
        handle.request_reconnect(),
        SocketReconnectRequestOutcome::Accepted
    );
    let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    assert_eq!(change.endpoint, endpoint);
    assert_eq!(change.state, SocketState::Disconnected);

    let frames = ws_state.subscribe_frames.lock().await.clone();
    let channels: Vec<String> = frames
        .iter()
        .flat_map(|f| {
            f["params"]["channels"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        })
        .filter_map(|c| c.as_str().map(str::to_string))
        .collect();
    assert!(channels.contains(&format!("{TEST_SUBACCOUNT}.orders")));
    assert!(channels.contains(&format!("{TEST_SUBACCOUNT}.trades")));
    assert!(channels.contains(&format!("{TEST_SUBACCOUNT}.balances")));

    tc.client.disconnect().await.expect("disconnect succeeds");
    assert!(registry.handle(client_id, endpoint).is_none());
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_fails_when_private_channel_is_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.subscribe_status.lock().await = Some(HashMap::from([
        (format!("{TEST_SUBACCOUNT}.orders"), "ok".to_string()),
        (
            format!("{TEST_SUBACCOUNT}.trades"),
            "unauthorized".to_string(),
        ),
        (format!("{TEST_SUBACCOUNT}.balances"), "ok".to_string()),
    ]));
    let mut tc = build_client(rest_state, ws_state.clone()).await;

    let err = tc
        .client
        .connect()
        .await
        .expect_err("private subscribe rejection must fail connect");

    let error_chain = format!("{err:#}");
    assert!(error_chain.contains("private WS subscriptions"));
    assert!(error_chain.contains("unauthorized"));
    assert!(!tc.client.is_connected());
    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.connection_count.load(Ordering::SeqCst) == 0 }
        },
        "failed connect tears down WS transport",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_exec_client_reconnect_refreshes_account_and_submits_mass_status() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;

    tc.client.connect().await.expect("connect succeeds");
    let event = drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Report(ExecutionReport::MassStatus(_))
            )
        },
        "post-reconnect mass status",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::MassStatus(status)) = event {
        assert_eq!(status.client_id, ClientId::from("DERIVE"));
        assert_eq!(status.account_id, AccountId::from("DERIVE-001"));
    } else {
        unreachable!();
    }
    assert!(rest_state.get_subaccount_calls.lock().await.len() >= 2);
    assert!(!rest_state.open_orders_calls.lock().await.is_empty());
    assert!(!rest_state.trigger_orders_calls.lock().await.is_empty());
    assert!(!rest_state.order_history_calls.lock().await.is_empty());
    assert!(!rest_state.trade_history_calls.lock().await.is_empty());
    assert!(!rest_state.positions_calls.lock().await.is_empty());
    assert_eq!(ws_state.login_frames.lock().await.len(), 2);
    assert_eq!(ws_state.subscribe_frames.lock().await.len(), 2);
    assert!(tc.client.is_connected());

    tc.client.disconnect().await.expect("disconnect succeeds");
}

#[rstest]
#[tokio::test]
async fn test_exec_client_marks_disconnected_after_reconnect_auth_exhaustion() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .login_failures_after_first
        .store(10, Ordering::SeqCst);
    ws_state
        .disconnect_after_subscribe
        .store(true, Ordering::SeqCst);
    let mut tc = build_client(rest_state, ws_state.clone()).await;

    tc.client.connect().await.expect("initial connect succeeds");
    tokio::time::timeout(Duration::from_secs(5), async {
        while tc.client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("execution client remained connected after auth exhaustion");

    assert!(!tc.client.is_connected());
    assert_eq!(ws_state.login_frames.lock().await.len(), 4);
    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.connection_count.load(Ordering::SeqCst) == 0 }
        },
        "failed session recovery closes transport",
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_submit_order_limit_posts_signed_payload() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-LIMIT-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;

    let posts = ws_state.submitted_orders.lock().await;
    let body = &posts[0];
    assert_eq!(body["instrument_name"].as_str(), Some("ETH-PERP"));
    assert_eq!(body["direction"].as_str(), Some("buy"));
    assert_eq!(body["order_type"].as_str(), Some("limit"));
    assert_eq!(body["time_in_force"].as_str(), Some("gtc"));
    assert_eq!(body["label"].as_str(), Some("STRAT-LIMIT-1"));
    assert_eq!(body["limit_price"].as_str(), Some("3500.00"));
    assert_eq!(body["amount"].as_str(), Some("1.000"));
    assert_eq!(body["subaccount_id"].as_u64(), Some(TEST_SUBACCOUNT));
    assert!(body["signature"].as_str().unwrap().starts_with("0x"));
    assert!(body["nonce"].as_u64().unwrap() > 0);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_accepts_signature_ttl_above_minimum() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client_with_config(rest_state, ws_state.clone(), None, |mut config| {
        config.signature_expiry_secs = MIN_SIGNATURE_TTL.as_secs() + 1;
        config
    })
    .await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OK-TTL-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    let start_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time is after unix epoch")
        .as_secs() as i64;
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;

    let posts = ws_state.submitted_orders.lock().await;
    let body = &posts[0];
    let expiry = body["signature_expiry_sec"]
        .as_i64()
        .expect("payload has signature expiry");
    let expected_ttl = (MIN_SIGNATURE_TTL.as_secs() + 1) as i64;
    assert!(
        expiry >= start_secs + expected_ttl - 2 && expiry <= start_secs + expected_ttl + 5,
        "signature expiry must use the configured TTL above the minimum, was {expiry}",
    );
    assert_eq!(body["label"].as_str(), Some("STRAT-OK-TTL-1"));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_deeply_paced_submit_builds_signature_after_matching_quota_wait() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // The fixed window is aligned to client construction, so measure from
    // before the build to bound the reset wait.
    let started = std::time::Instant::now();
    let mut tc = build_client_with_config(rest_state, ws_state.clone(), None, |mut config| {
        config.signature_expiry_secs = MIN_SIGNATURE_TTL.as_secs() + 1;
        config.max_matching_requests_per_second = Some(1);
        config
    })
    .await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    for sequence in 0..7 {
        let order = build_limit_order(
            instrument_id,
            ClientOrderId::from(format!("STRAT-PACED-{sequence}")),
            OrderSide::Buy,
            Price::from("3500.00"),
            Quantity::from("1.000"),
        );
        tc.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("cache insert");
        tc.client
            .submit_order(submit_cmd(&order))
            .expect("submit Ok");
    }

    // The fixed-window reset departs the last two writes at the ~5s boundary;
    // bound the wait from order submission so it cannot race the reset.
    wait_until_async(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() == 7 }
        },
        Duration::from_secs(15),
    )
    .await;
    let elapsed = started.elapsed();
    let posts = ws_state.submitted_orders.lock().await;
    let received_at_secs = ws_state.submitted_order_received_at_secs.lock().await;

    assert_eq!(posts.len(), 7);
    assert_eq!(received_at_secs.len(), 7);
    assert!(
        elapsed >= Duration::from_secs(4),
        "writes past the five-request burst must wait for the discrete window \
         reset (~5s), elapsed {elapsed:?}",
    );

    for (body, received_at_secs) in posts.iter().zip(received_at_secs.iter()) {
        let expiry_secs = body["signature_expiry_sec"]
            .as_i64()
            .expect("payload has signature expiry");
        let remaining_secs = i128::from(expiry_secs) - i128::from(*received_at_secs);
        assert!(
            remaining_secs >= i128::from(MIN_SIGNATURE_TTL.as_secs()),
            "signature for {} must retain at least the venue minimum after pacing, remaining {remaining_secs} s",
            body["label"],
        );
    }
    drop(received_at_secs);
    drop(posts);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_global_matching_allowance_gates_distinct_instrument_until_window_reset() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // The fixed window is aligned to client construction, so measure from
    // before the build to bound the reset wait.
    let started = std::time::Instant::now();
    let started_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after unix epoch")
        .as_secs();
    let mut tc =
        build_client_with_config(rest_state, ws_state.clone(), None, |config| config).await;
    tc.client.connect().await.expect("connect succeeds");

    // Five ETH-PERP writes exhaust the Trader account-wide matching window
    // (and ETH-PERP's own per-instrument window) without touching BTC-PERP's.
    for sequence in 0..5 {
        let order = build_limit_order(
            InstrumentId::from("ETH-PERP.DERIVE"),
            ClientOrderId::from(format!("STRAT-GLOBAL-ETH-{sequence}")),
            OrderSide::Buy,
            Price::from("3500.00"),
            Quantity::from("1.000"),
        );
        tc.cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .expect("cache insert");
        tc.client
            .submit_order(submit_cmd(&order))
            .expect("submit Ok");
    }
    wait_until_async(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() == 5 }
        },
        Duration::from_secs(10),
    )
    .await;

    // A BTC-PERP write has a fresh per-instrument allowance, but the global
    // bucket is drained: it must wait for the discrete window reset.
    let btc_order = build_limit_order(
        InstrumentId::from("BTC-PERP.DERIVE"),
        ClientOrderId::from("STRAT-GLOBAL-BTC-0"),
        OrderSide::Buy,
        Price::from("50000.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(btc_order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&btc_order))
        .expect("submit Ok");

    wait_until_async(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() == 6 }
        },
        Duration::from_secs(15),
    )
    .await;

    let posts = ws_state.submitted_orders.lock().await;
    let received_at_secs = ws_state.submitted_order_received_at_secs.lock().await;
    assert_eq!(posts.len(), 6);
    assert_eq!(posts[5]["instrument_name"].as_str(), Some("BTC-PERP"));

    let btc_received_secs = received_at_secs[5];
    let eth_received_secs = received_at_secs[..5].to_vec();
    assert!(
        btc_received_secs >= started_secs + 4,
        "global bucket must gate the BTC-PERP write until the ~5s window reset, \
         started {started_secs}, BTC-PERP received {btc_received_secs}",
    );
    assert!(
        eth_received_secs
            .iter()
            .all(|&secs| secs <= btc_received_secs),
        "the five ETH-PERP writes must depart within the first window",
    );
    drop(received_at_secs);
    drop(posts);
    drop(eth_received_secs);

    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(12),
        "smoke bound: the reset wait must be one window, elapsed {elapsed:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(MIN_SIGNATURE_TTL.as_secs(), "must be greater than the Derive minimum")]
#[case(MIN_SIGNATURE_TTL.as_secs() - 1, "must be greater than the Derive minimum")]
#[tokio::test]
async fn test_submit_order_rejects_signature_ttl_minimum_or_lower_before_posting(
    #[case] signature_expiry_secs: u64,
    #[case] reason_fragment: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client_with_config(rest_state, ws_state.clone(), None, |mut config| {
        config.signature_expiry_secs = signature_expiry_secs;
        config
    })
    .await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-BAD-TTL-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted event",
    )
    .await;
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, order.client_order_id());
        assert!(!rejected.due_post_only);
        assert!(
            rejected.reason.contains("order expiry validation failed")
                && rejected.reason.contains(reason_fragment),
            "unexpected reject reason: {}",
            rejected.reason,
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "invalid signature TTL must not post private/order",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_stop_market_posts_trigger_order_and_emits_accepted() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-STOP-1");
    let order = build_stop_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("3600.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    let start_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time is after unix epoch")
        .as_secs() as i64;
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_trigger_orders.lock().await.is_empty() }
        },
        "private/trigger_order posted",
    )
    .await;

    let posts = ws_state.submitted_trigger_orders.lock().await;
    let body = &posts[0];
    assert_eq!(body["instrument_name"].as_str(), Some("ETH-PERP"));
    assert_eq!(body["direction"].as_str(), Some("sell"));
    assert_eq!(body["order_type"].as_str(), Some("market"));
    assert_eq!(body["time_in_force"].as_str(), Some("gtc"));
    assert_eq!(body["label"].as_str(), Some("STRAT-STOP-1"));
    assert_eq!(body["limit_price"].as_str(), Some("3582.00"));
    assert_eq!(body["amount"].as_str(), Some("1.000"));
    assert_eq!(body["trigger_price"].as_str(), Some("3600.00"));
    assert_eq!(body["trigger_price_type"].as_str(), Some("mark"));
    assert_eq!(body["trigger_type"].as_str(), Some("stoploss"));
    assert_eq!(body["subaccount_id"].as_u64(), Some(TEST_SUBACCOUNT));
    assert!(body["signature"].as_str().unwrap().starts_with("0x"));
    assert!(
        body["conn_id"].as_str().is_some_and(|v| !v.is_empty()),
        "conn_id must be present",
    );
    assert!(
        body["order_id"].as_str().is_some_and(|v| !v.is_empty()),
        "trigger order_id must be present",
    );
    let expiry = body["signature_expiry_sec"]
        .as_i64()
        .expect("trigger payload has signature expiry");
    let expected_ttl = TRIGGER_ORDER_SIGNATURE_TTL.as_secs() as i64;
    assert!(
        expiry >= start_secs + expected_ttl - 2 && expiry <= start_secs + expected_ttl + 5,
        "trigger expiry must be about 31 days from submit time, was {expiry}",
    );
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "trigger submit must not post private/order",
    );
    drop(posts);

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = event {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.venue_order_id.as_str(), "trig-mock-1");
        assert_eq!(accepted.strategy_id, StrategyId::from("S-1"));
        assert_eq!(accepted.instrument_id, instrument_id);
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(TimeInForce::Gtc, false, "gtc")]
#[case(TimeInForce::Ioc, false, "ioc")]
#[case(TimeInForce::Fok, false, "fok")]
#[case(TimeInForce::Gtc, true, "post_only")]
#[tokio::test]
async fn test_submit_order_posts_supported_time_in_force(
    #[case] time_in_force: TimeInForce,
    #[case] post_only: bool,
    #[case] expected: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let post_only_suffix = if post_only { "POST" } else { "NORM" };
    let client_order_id =
        ClientOrderId::from(format!("STRAT-TIF-{time_in_force:?}-{post_only_suffix}"));
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
        time_in_force,
        post_only,
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;

    let posts = ws_state.submitted_orders.lock().await;
    assert_eq!(posts[0]["time_in_force"].as_str(), Some(expected));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(TimeInForce::Day, false, "unsupported time in force")]
#[case(TimeInForce::Day, true, "unsupported time in force")]
#[case(TimeInForce::Ioc, true, "post-only Derive orders only support GTC")]
#[case(TimeInForce::Fok, true, "post-only Derive orders only support GTC")]
#[tokio::test]
async fn test_submit_order_denies_unsupported_time_in_force_before_posting(
    #[case] time_in_force: TimeInForce,
    #[case] post_only: bool,
    #[case] reason_fragment: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let post_only_suffix = if post_only { "POST" } else { "NORM" };
    let client_order_id = ClientOrderId::from(format!(
        "STRAT-BAD-TIF-{time_in_force:?}-{post_only_suffix}"
    ));
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
        time_in_force,
        post_only,
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;

    if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
        assert_eq!(denied.client_order_id, order.client_order_id());
        assert!(
            denied.reason.contains(reason_fragment),
            "unexpected deny reason: {}",
            denied.reason,
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "invalid TIF must not post to the venue",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_submit_order_denies_unsupported_order_type_before_posting() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-BAD-TYPE");
    let order = OrderTestBuilder::new(OrderType::MarketToLimit)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.000"))
        .price(Price::from("3500.00"))
        .build();
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;

    if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
        assert_eq!(denied.client_order_id, order.client_order_id());
        assert!(
            denied.reason.contains("unsupported order type"),
            "unexpected deny reason: {}",
            denied.reason,
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "unsupported order type must not post to the venue",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn test_submit_order_denies_unsupported_trigger_price_type_before_posting() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-BAD-TRIGGER-TYPE");
    let order = OrderTestBuilder::new(OrderType::StopMarket)
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("S-1"))
        .instrument_id(instrument_id)
        .client_order_id(client_order_id)
        .side(OrderSide::Sell)
        .quantity(Quantity::from("1.000"))
        .trigger_price(Price::from("3400.00"))
        .trigger_type(TriggerType::IndexPrice)
        .build();
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_denied_without_submitted(&mut tc.rx, &order.client_order_id()).await;

    if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
        assert_eq!(denied.client_order_id, order.client_order_id());
        assert!(
            denied.reason.contains("unsupported trigger price type"),
            "unexpected deny reason: {}",
            denied.reason,
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.submitted_orders.lock().await.is_empty(),
        "unsupported trigger price type must not post to the venue",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_market_with_quote_uses_rounded_slippage_bound() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut refreshed_ticker = sample_ticker_json("ETH-PERP", 1_700_000_000_013_i64);
    refreshed_ticker["best_ask_price"] = json!("3501.00");
    *rest_state.ticker_response.lock().await = refreshed_ticker;
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MARKET-1");

    let quote = QuoteTick::new(
        instrument_id,
        Price::from("3100.00"),
        Price::from("3101.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    tc.cache
        .borrow_mut()
        .add_quote(quote)
        .expect("quote insert");
    let order = build_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted for market",
    )
    .await;

    let posts = ws_state.submitted_orders.lock().await;
    let body = &posts[0];
    assert_eq!(body["order_type"].as_str(), Some("market"));
    // Refreshed REST ask, not stale cache: 3501 * 1.005 -> 3518.51
    assert_eq!(body["limit_price"].as_str(), Some("3518.51"));
    assert_eq!(rest_state.ticker_calls.lock().await.len(), 1);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_market_without_quote_is_denied() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MARKET-2");
    let order = build_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))),
        "OrderDenied event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
        assert!(denied.reason.contains("no cached quote"));
    } else {
        unreachable!();
    }
    assert!(ws_state.submitted_orders.lock().await.is_empty());
    assert!(rest_state.ticker_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_market_rejects_when_quote_refresh_fails_without_posting() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = json!({
        "id": 1,
        "error": {"code": -32000, "message": "ticker unavailable"},
    });
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MARKET-REFRESH-FAIL");
    let quote = QuoteTick::new(
        instrument_id,
        Price::from("3500.00"),
        Price::from("3501.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    tc.cache
        .borrow_mut()
        .add_quote(quote)
        .expect("quote insert");
    let order = build_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("0.500"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted event",
    )
    .await;
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, order.client_order_id());
        assert!(
            rejected
                .reason
                .contains("market-order quote refresh failed"),
            "unexpected reject reason: {}",
            rejected.reason,
        );
    } else {
        unreachable!();
    }
    assert!(!rest_state.ticker_calls.lock().await.is_empty());
    assert!(ws_state.submitted_orders.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_jsonrpc_rejection_emits_order_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Mock returns a structured JSON-RPC error envelope.
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32602, "message": "Invalid params"}
    }));
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-REJECT-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        let reason = rejected.reason.as_str();
        assert!(!rejected.due_post_only);
        assert!(
            reason.contains("-32602") && reason.contains("Invalid params"),
            "unexpected reject reason: {reason}",
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_post_only_cross_jsonrpc_sets_due_post_only() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {
            "code": 11008,
            "message": "Post only order cannot cross the market"
        }
    }));
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-POST-ONLY-CROSS");
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
        TimeInForce::Gtc,
        true,
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected post-only cross",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        let reason = rejected.reason.as_str();
        assert!(rejected.due_post_only);
        assert!(
            reason.contains("11008") && reason.contains("Post only order cannot cross the market"),
            "unexpected reject reason: {reason}",
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_jsonrpc_ambiguous_does_not_emit_order_rejected() {
    // JSON-RPC `-32603` (generic internal error) is the only code currently in
    // the write-outcome-ambiguous set: the venue's own process is known to
    // have run for some unknown distance before failing, so the order may
    // have been accepted before the failure response. `send_private_once`
    // does not replay; emitting OrderRejected on those would let the engine
    // treat a live order as rejected. Mirrors the cancel/modify policy.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal venue error"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-SUBMIT-RETRY");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    // OrderSubmitted lands synchronously; drain it so the timeout below
    // only watches for a stray Rejected emission.
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Rejected(_))) => {
                    return Some("unexpected OrderRejected on retryable code");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "ambiguous JSON-RPC code must not emit OrderRejected, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_rate_limit_jsonrpc_emits_order_rejected() {
    // Observed venue behavior: Derive returns `-32000 Rate limit exceeded`
    // for throttled requests. The code sits in the JSON-RPC server-error
    // range and is HTTP-retryable, but the matching engine never saw the
    // request: the gateway threw it out. This is a *definitive* rejection
    // for the write outcome, so the adapter must emit OrderRejected to
    // clear the engine's PendingSubmit. The narrower ambiguous classifier
    // `is_write_outcome_ambiguous_jsonrpc` exists so codes like this are
    // not silently swallowed.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32000, "message": "Rate limit exceeded: 0xwallet-nonMatching"}
    }));
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-RATE-LIMIT");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected event for rate limit",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        let reason = rejected.reason.as_str();
        assert!(!rejected.due_post_only);
        assert!(
            reason.contains("-32000") && reason.contains("Rate limit"),
            "unexpected reject reason: {reason}",
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_list_delegates_per_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let order_a = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-LIST-A"),
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    let order_b = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-LIST-B"),
        OrderSide::Sell,
        Price::from("3501.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order_a.clone(), None, None, false)
        .expect("insert A");
    tc.cache
        .borrow_mut()
        .add_order(order_b.clone(), None, None, false)
        .expect("insert B");

    let order_list = OrderList::new(
        OrderListId::from("OL-1"),
        instrument_id,
        StrategyId::from("S-1"),
        vec![order_a.client_order_id(), order_b.client_order_id()],
        UnixNanos::default(),
    );

    let cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        order_list,
        vec![
            OrderInitialized::from(&order_a),
            OrderInitialized::from(&order_b),
        ],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );
    tc.client
        .submit_order_list(cmd)
        .expect("submit_order_list Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() >= 2 }
        },
        "two submit posts",
    )
    .await;
    let posts = ws_state.submitted_orders.lock().await;
    let labels: Vec<&str> = posts
        .iter()
        .map(|b| b["label"].as_str().unwrap_or(""))
        .collect();
    assert!(labels.contains(&"STRAT-LIST-A"));
    assert!(labels.contains(&"STRAT-LIST-B"));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_calls_private_cancel() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("O-1"),
        Some(VenueOrderId::from("ord-mock-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "cancel posted",
    )
    .await;

    let posts = ws_state.cancelled_orders.lock().await;
    let body = &posts[0];
    assert_eq!(body["subaccount_id"].as_u64(), Some(TEST_SUBACCOUNT));
    assert_eq!(body["instrument_name"].as_str(), Some("ETH-PERP"));
    assert_eq!(body["order_id"].as_str(), Some("ord-mock-1"));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_trigger_order_calls_private_cancel_trigger_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-CXL-TRIGGER");
    let order = build_stop_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3400.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .expect("cache insert");

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("trig-cancel-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_trigger_orders.lock().await.is_empty() }
        },
        "private/cancel_trigger_order posted",
    )
    .await;

    let posts = ws_state.cancelled_trigger_orders.lock().await;
    let body = &posts[0];
    assert_eq!(body["subaccount_id"].as_u64(), Some(TEST_SUBACCOUNT));
    assert_eq!(body["order_id"].as_str(), Some("trig-cancel-1"));
    assert!(
        body.get("instrument_name").is_none(),
        "trigger cancel params must not include instrument_name",
    );
    assert!(
        ws_state.cancelled_orders.lock().await.is_empty(),
        "trigger cancel must not post private/cancel",
    );
    drop(posts);

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled from trigger cancel response",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert_eq!(canceled.instrument_id, instrument_id);
        assert_eq!(
            canceled.venue_order_id.map(|id| id.to_string()),
            Some("trig-cancel-1".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_trigger_order_without_venue_id_resolves_label() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-CXL-TRIGGER-BY-LABEL");
    *rest_state.trigger_orders_response.lock().await = json!({
        "orders": [trigger_order_json_with(
            "trig-resolved-by-label",
            client_order_id.as_str(),
            "buy",
            "ETH-PERP",
            1_700_000_001_000,
            "market",
            "untriggered",
            "3417",
            "3400",
            "mark",
            "stoploss",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let order = build_stop_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3400.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .expect("cache insert");
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled from trigger resolved by label",
    )
    .await;
    let posts = ws_state.cancelled_trigger_orders.lock().await;

    assert_eq!(rest_state.trigger_orders_calls.lock().await.len(), 1);
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0]["order_id"].as_str(),
        Some("trig-resolved-by-label")
    );
    assert!(ws_state.cancelled_labels.lock().await.is_empty());

    if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert_eq!(
            canceled.venue_order_id.map(|id| id.to_string()),
            Some("trig-resolved-by-label".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(
    json!({"orders": [], "subaccount_id": TEST_SUBACCOUNT}),
    "trigger order not found for client_order_id",
    1
)]
#[case(
    json!({"id": 1, "error": {"code": -32000, "message": "trigger lookup unavailable"}}),
    "failed to resolve trigger order by label",
    2
)]
#[tokio::test]
async fn test_cancel_trigger_order_without_venue_id_rejects_lookup_failure(
    #[case] trigger_orders_response: Value,
    #[case] expected_reason: &str,
    #[case] expected_calls: usize,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trigger_orders_response.lock().await = trigger_orders_response;
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-CXL-TRIGGER-LOOKUP-FAIL");
    let order = build_stop_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3400.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .expect("cache insert");
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
            )
        },
        "OrderCancelRejected from trigger lookup",
    )
    .await;
    tc.client.disconnect().await.expect("disconnect");

    assert_eq!(
        rest_state.trigger_orders_calls.lock().await.len(),
        expected_calls
    );
    assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_labels.lock().await.is_empty());

    if let ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert!(rejected.venue_order_id.is_none());
        assert!(rejected.reason.contains(expected_reason));
    } else {
        unreachable!();
    }
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_without_venue_id_calls_private_cancel_by_label() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_label_reply.lock().await = Some(
        serde_json::from_str(include_str!(
            "../../test_data/common/ws_cancel_by_label_nonzero.json"
        ))
        .expect("nonzero cancel-by-label fixture is valid JSON"),
    );
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-CXL-BY-LABEL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_labels.lock().await.is_empty() }
        },
        "private/cancel_by_label posted",
    )
    .await;

    let posts = ws_state.cancelled_labels.lock().await;
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["subaccount_id"].as_u64(), Some(TEST_SUBACCOUNT));
    assert_eq!(posts[0]["label"].as_str(), Some(client_order_id.as_str()));
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    drop(posts);

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Canceled(_))) => {
                    return Some("OrderCanceled before venue notification");
                }
                Some(ExecutionEvent::Order(OrderEventAny::CancelRejected(_))) => {
                    return Some("OrderCancelRejected for nonzero count");
                }
                Some(_) => {}
                None => return Some("execution event channel closed"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "nonzero cancel-by-label must wait for venue notification, was {outcome:?}",
    );

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let canceled_frame = json!([order_json_with(
        "ord-canceled-by-label",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "cancelled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &canceled_frame));
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled after cancel_by_label",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert_eq!(
            canceled.venue_order_id.map(|id| id.to_string()),
            Some("ord-canceled-by-label".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_by_label_zero_count_emits_cancel_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_label_reply.lock().await = Some(
        serde_json::from_str(include_str!(
            "../../test_data/common/ws_cancel_by_label_zero.json"
        ))
        .expect("zero cancel-by-label fixture is valid JSON"),
    );
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let client_order_id = ClientOrderId::from("STRAT-CXL-BY-LABEL-ZERO");
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
            )
        },
        "OrderCancelRejected for zero cancel-by-label count",
    )
    .await;

    assert_eq!(ws_state.cancelled_labels.lock().await.len(), 1);
    if let ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert!(rejected.venue_order_id.is_none());
        assert_eq!(
            rejected.reason,
            "no open order matched the client_order_id label"
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_by_label_jsonrpc_ambiguous_does_not_emit_cancel_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_label_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal venue error"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-CXL-BY-LABEL-AMBIGUOUS");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_labels.lock().await.is_empty() }
        },
        "private/cancel_by_label posted",
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::CancelRejected(_))) => {
                    return Some("OrderCancelRejected for ambiguous outcome");
                }
                Some(_) => {}
                None => return Some("execution event channel closed"),
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "ambiguous cancel-by-label must not emit a terminal rejection, was {outcome:?}",
    );

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let canceled_frame = json!([order_json_with(
        "ord-canceled-after-ambiguous-label",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "cancelled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &canceled_frame));
    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled after ambiguous cancel-by-label",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert_eq!(
            canceled.venue_order_id.map(|id| id.to_string()),
            Some("ord-canceled-after-ambiguous-label".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_by_label_rejection_emits_cancel_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_label_reply.lock().await = Some(json!({
        "error": {"code": -32602, "message": "No order with label"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let client_order_id = ClientOrderId::from("STRAT-CXL-BY-LABEL-REJECT");
    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        client_order_id,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| {
            matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::CancelRejected(_))
            )
        },
        "OrderCancelRejected from cancel_by_label",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert!(rejected.venue_order_id.is_none());
        assert!(rejected.reason.contains("No order with label"));
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_without_side_sends_cancel_by_instrument_for_empty_book() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_instrument_reply.lock().await =
        Some(json!({"result": {"cancelled_orders": 0}}));
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancel_by_instrument_calls.lock().await.is_empty() }
        },
        "cancel_by_instrument posted",
    )
    .await;

    let posts = ws_state.cancel_by_instrument_calls.lock().await;
    assert_eq!(
        posts.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "instrument_name": "ETH-PERP",
        })],
    );
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_without_side_cancels_matching_triggers() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let account_id = AccountId::from("DERIVE-001");
    let client_id = Some(ClientId::from("DERIVE"));
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_stop_market_order(
                InstrumentId::from("ETH-PERP.DERIVE"),
                ClientOrderId::from("TRIGGER-ETH"),
                OrderSide::Buy,
                Price::from("3450.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("trig-eth"),
            account_id,
        ),
        client_id,
    );
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_stop_market_order(
                InstrumentId::from("BTC-PERP.DERIVE"),
                ClientOrderId::from("TRIGGER-BTC"),
                OrderSide::Sell,
                Price::from("66000.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("trig-btc"),
            account_id,
        ),
        client_id,
    );
    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_limit_order(
                InstrumentId::from("ETH-PERP.DERIVE"),
                ClientOrderId::from("REGULAR-ETH"),
                OrderSide::Buy,
                Price::from("3500.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("regular-eth"),
            account_id,
        ),
        client_id,
    );
    tc.cache.borrow_mut().build_index();

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move {
                !state.cancel_by_instrument_calls.lock().await.is_empty()
                    && !state.cancelled_trigger_orders.lock().await.is_empty()
            }
        },
        "trigger and instrument cancels posted",
    )
    .await;

    assert_eq!(
        ws_state.cancelled_trigger_orders.lock().await.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "order_id": "trig-eth",
        })],
    );
    assert_eq!(
        ws_state.cancel_by_instrument_calls.lock().await.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "instrument_name": "ETH-PERP",
        })],
    );
    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_trigger_failure_still_sends_cancel_by_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_trigger_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal venue error"}
    }));
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    add_order_to_cache(
        &tc.cache,
        accepted_order(
            build_stop_market_order(
                InstrumentId::from("ETH-PERP.DERIVE"),
                ClientOrderId::from("TRIGGER-ETH"),
                OrderSide::Buy,
                Price::from("3450.00"),
                Quantity::from("1.000"),
            ),
            VenueOrderId::from("trig-eth"),
            AccountId::from("DERIVE-001"),
        ),
        Some(ClientId::from("DERIVE")),
    );
    tc.cache.borrow_mut().build_index();

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move {
                !state.cancelled_trigger_orders.lock().await.is_empty()
                    && !state.cancel_by_instrument_calls.lock().await.is_empty()
            }
        },
        "trigger and instrument cancels posted",
    )
    .await;

    assert_eq!(
        ws_state.cancelled_trigger_orders.lock().await.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "order_id": "trig-eth",
        })],
    );
    assert_eq!(
        ws_state.cancel_by_instrument_calls.lock().await.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "instrument_name": "ETH-PERP",
        })],
    );
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(event)) => return Some(event),
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "cancel-all failure must not emit a per-order event, was {outcome:?}",
    );
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(OrderSide::Buy, "buy-eth", "trig-buy-eth")]
#[case(OrderSide::Sell, "sell-eth", "trig-sell-eth")]
#[tokio::test]
async fn test_cancel_all_orders_side_filter_iterates_matching_open_orders(
    #[case] side: OrderSide,
    #[case] regular_order_id: &str,
    #[case] trigger_order_id: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let other_instrument_id = InstrumentId::from("BTC-PERP.DERIVE");
    let account_id = AccountId::from("DERIVE-001");
    let other_account_id = AccountId::from("DERIVE-OTHER");
    let client_id = Some(ClientId::from("DERIVE"));
    let other_client_id = Some(ClientId::from("DERIVE-OTHER"));
    let regular_orders = [
        (
            "L-BUY-ETH",
            "buy-eth",
            OrderSide::Buy,
            instrument_id,
            account_id,
            client_id,
        ),
        (
            "L-SELL-ETH",
            "sell-eth",
            OrderSide::Sell,
            instrument_id,
            account_id,
            client_id,
        ),
        (
            "L-BUY-BTC",
            "buy-btc",
            OrderSide::Buy,
            other_instrument_id,
            account_id,
            client_id,
        ),
        (
            "L-BUY-OTHER-ACCOUNT",
            "buy-other-account",
            OrderSide::Buy,
            instrument_id,
            other_account_id,
            client_id,
        ),
        (
            "L-SELL-OTHER-ACCOUNT",
            "sell-other-account",
            OrderSide::Sell,
            instrument_id,
            other_account_id,
            client_id,
        ),
        (
            "L-BUY-OTHER-CLIENT",
            "buy-other-client",
            OrderSide::Buy,
            instrument_id,
            account_id,
            other_client_id,
        ),
        (
            "L-SELL-UNCLAIMED",
            "sell-unclaimed",
            OrderSide::Sell,
            instrument_id,
            account_id,
            None,
        ),
    ];

    for (order_id, venue_order_id, order_side, instrument, account, owner) in regular_orders {
        add_order_to_cache(
            &tc.cache,
            accepted_order(
                build_limit_order(
                    instrument,
                    ClientOrderId::from(order_id),
                    order_side,
                    Price::from("3500.00"),
                    Quantity::from("1.000"),
                ),
                VenueOrderId::from(venue_order_id),
                account,
            ),
            owner,
        );
    }

    for (order_id, venue_order_id, order_side) in [
        ("T-BUY-ETH", "trig-buy-eth", OrderSide::Buy),
        ("T-SELL-ETH", "trig-sell-eth", OrderSide::Sell),
    ] {
        add_order_to_cache(
            &tc.cache,
            accepted_order(
                build_stop_market_order(
                    instrument_id,
                    ClientOrderId::from(order_id),
                    order_side,
                    Price::from("3450.00"),
                    Quantity::from("1.000"),
                ),
                VenueOrderId::from(venue_order_id),
                account_id,
            ),
            client_id,
        );
    }
    tc.cache.borrow_mut().build_index();

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        Some(side),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move {
                !state.cancelled_orders.lock().await.is_empty()
                    && !state.cancelled_trigger_orders.lock().await.is_empty()
            }
        },
        "filtered cancels posted",
    )
    .await;

    let posts = ws_state.cancelled_orders.lock().await;
    assert_eq!(posts.len(), 1, "expected exactly one filtered cancel");
    let body = &posts[0];
    assert_eq!(body["order_id"].as_str(), Some(regular_order_id));
    assert_eq!(body["instrument_name"].as_str(), Some("ETH-PERP"));
    drop(posts);
    assert_eq!(
        ws_state.cancelled_trigger_orders.lock().await.as_slice(),
        &[json!({
            "subaccount_id": TEST_SUBACCOUNT,
            "order_id": trigger_order_id,
        })],
    );
    assert!(ws_state.cancel_by_instrument_calls.lock().await.is_empty(),);
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(Some(OrderSide::Buy), OrderType::Limit)]
#[case(None, OrderType::StopMarket)]
#[tokio::test]
async fn test_cancel_all_orders_missing_cached_venue_id_fails_closed(
    #[case] order_side: Option<OrderSide>,
    #[case] order_type: OrderType,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let build_order = |client_order_id| match order_type {
        OrderType::Limit => build_limit_order(
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            Price::from("3500.00"),
            Quantity::from("1.000"),
        ),
        OrderType::StopMarket => build_stop_market_order(
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            Price::from("3450.00"),
            Quantity::from("1.000"),
        ),
        _ => unreachable!(),
    };
    let account_id = AccountId::from("DERIVE-001");
    let client_id = Some(ClientId::from("DERIVE"));
    let valid = accepted_order(
        build_order(ClientOrderId::from("VALID")),
        VenueOrderId::from("valid-venue-id"),
        account_id,
    );
    let mut missing = accepted_order(
        build_order(ClientOrderId::from("MISSING")),
        VenueOrderId::from("removed-venue-id"),
        account_id,
    );

    match &mut missing {
        OrderAny::Limit(order) => order.venue_order_id = None,
        OrderAny::StopMarket(order) => order.venue_order_id = None,
        _ => unreachable!(),
    }
    add_order_to_cache(&tc.cache, valid, client_id);
    add_order_to_cache(&tc.cache, missing, client_id);
    tc.cache.borrow_mut().build_index();

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        order_side,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(ws_state.cancelled_orders.lock().await.is_empty());
    assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    assert!(ws_state.cancel_by_instrument_calls.lock().await.is_empty());
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_posts_replace_and_emits_order_updated() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    // Drain the initial account-state event emitted at connect.
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-1")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Updated(_))),
        "OrderUpdated event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Updated(updated)) = event {
        assert_eq!(updated.client_order_id, client_order_id);
        assert_eq!(updated.quantity, Quantity::from("2.000"));
        assert_eq!(updated.price, Some(Price::from("3505.00")));
        // Mock response carries `order.order_id = ord-replaced-1`.
        assert_eq!(
            updated.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-replaced-1".to_string()),
        );
    } else {
        unreachable!();
    }

    // Exactly one replace request was sent, with the stale id in the cancel
    // clause and the new quantity/price in the signed envelope.
    let replaces = ws_state.replace_orders.lock().await;
    assert_eq!(replaces.len(), 1, "expected exactly one replace request");
    let body = &replaces[0];
    assert_eq!(body["order_id_to_cancel"].as_str(), Some("ord-stale-1"));
    assert_eq!(body["instrument_name"].as_str(), Some("ETH-PERP"));
    assert_eq!(body["direction"].as_str(), Some("buy"));
    assert_eq!(body["amount"].as_str(), Some("2.000"));
    assert_eq!(body["limit_price"].as_str(), Some("3505.00"));
    assert_eq!(body["label"].as_str(), Some("STRAT-MOD-1"));
    assert!(body["signature"].as_str().unwrap().starts_with("0x"));
    // The legacy cancel-only fallback must not fire any more.
    assert!(ws_state.cancelled_orders.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_rejects_missing_cached_order_with_canonical_reason() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state).await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-MISSING");
    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-missing-cache")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
        "OrderModifyRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert_eq!(rejected.reason, ORDER_NOT_FOUND);
        assert_eq!(
            rejected.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-missing-cache".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.stop().expect("stop");
}

#[rstest]
#[case(
    MIN_SIGNATURE_TTL.as_secs(),
    "must be greater than the Derive minimum"
)]
#[case(
    MIN_SIGNATURE_TTL.as_secs() - 1,
    "must be greater than the Derive minimum"
)]
#[case(i64::MAX as u64, "overflows Derive signature_expiry_sec")]
#[tokio::test]
async fn test_modify_order_rejects_invalid_signature_ttl_before_posting_replace(
    #[case] signature_expiry_secs: u64,
    #[case] reason_fragment: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client_with_config(rest_state, ws_state.clone(), None, |mut config| {
        config.signature_expiry_secs = signature_expiry_secs;
        config
    })
    .await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-BAD-TTL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-overflow")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
        "OrderModifyRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
        let reason = rejected.reason.as_str();
        assert!(
            reason.contains("replace expiry validation failed") && reason.contains(reason_fragment),
            "unexpected reject reason: {reason}",
        );
        assert_eq!(
            rejected.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-stale-overflow".to_string()),
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.replace_orders.lock().await.is_empty(),
        "invalid signature TTL must not post private/replace",
    );
    assert!(
        ws_state.cancelled_orders.lock().await.is_empty(),
        "invalid signature TTL must not fall back to private/cancel",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_suppresses_replace_cancel_leg() {
    // Regression: Derive's `private/replace` cancels the old order and opens a
    // new one under the same label. The `.orders` channel pushes the old
    // order's cancellation, which must NOT terminate the order: `modify_order`
    // rebinds it to the replacement via OrderUpdated, and the cancel-of-old leg
    // is suppressed. Mirrors the Hyperliquid GH-3827 cancel-replace handling.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-SUPPRESS");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    // Submit to register the tracked identity, then accept via an `.orders`
    // Open frame so the order binds to venue_order_id `ord-stale-1`.
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-stale-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted",
    )
    .await;

    // Modify: the default replace reply rebinds to `ord-replaced-1`.
    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-1")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");
    let updated = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Updated(_))),
        "OrderUpdated",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Updated(updated)) = updated {
        assert_eq!(
            updated.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-replaced-1".to_string()),
        );
    } else {
        unreachable!();
    }

    // The venue now pushes the replace's cancel-of-old leg on the `.orders`
    // channel. With the order rebound to `ord-replaced-1`, this stale cancel of
    // `ord-stale-1` must be suppressed (no OrderCanceled).
    let cancel_frame = json!([order_json_with(
        "ord-stale-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "cancelled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &cancel_frame));

    let canceled = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Canceled(_))) => return true,
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await;
    assert!(
        canceled.is_err(),
        "the replace's cancel-of-old leg must not emit OrderCanceled",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_accepts_replacement_rejection_before_rpc_response() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.replace_reply.lock().await = Some(json!({
        "error": {"code": 11008, "message": "Post only order cannot cross the market"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-EARLY-REJECT");
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
        TimeInForce::Gtc,
        true,
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-before-replace",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted",
    )
    .await;

    let mut rejected_order = order_json_with(
        "ord-replacement-rejected",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "rejected",
    );
    rejected_order["cancel_reason"] = json!("Post only order cannot cross the market");
    rejected_order["time_in_force"] = json!("post_only");
    *ws_state.replace_notification_before_reply.lock().await = Some(make_subscription_frame(
        &orders_channel,
        &json!([rejected_order]),
    ));

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-before-replace")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "replacement OrderRejected before private/replace response",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert!(rejected.due_post_only);
        assert_eq!(rejected.reason, "Post only order cannot cross the market");
    } else {
        unreachable!();
    }

    let late_terminal = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(
                    OrderEventAny::Updated(_) | OrderEventAny::ModifyRejected(_),
                )) => return true,
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await;
    assert!(
        late_terminal.is_err(),
        "definitive replace response must not emit after a terminal replacement frame",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_accepts_replacement_open_before_rpc_response() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.replace_reply.lock().await = Some(json!({
        "result": {
            "order": order_json_with(
                "ord-replaced-1",
                "STRAT-O-1",
                "buy",
                "ETH-PERP",
                1_700_000_003_000_i64,
                "open",
            ),
            "cancelled_order": order_json_with(
                "ord-before-replace",
                "STRAT-O-1",
                "buy",
                "ETH-PERP",
                1_700_000_002_000_i64,
                "cancelled",
            ),
        },
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-O-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-before-replace",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted",
    )
    .await;

    let replacement_frame = json!([order_json_with(
        "ord-replaced-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "open",
    )]);
    *ws_state.replace_notification_before_reply.lock().await =
        Some(make_subscription_frame(&orders_channel, &replacement_frame));
    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-before-replace")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let mut duplicate_accepted = false;
    let updated = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Accepted(_))) => {
                    duplicate_accepted = true;
                }
                Some(ExecutionEvent::Order(OrderEventAny::Updated(updated))) => return updated,
                Some(_) => {}
                None => panic!("event channel closed before OrderUpdated"),
            }
        }
    })
    .await
    .expect("OrderUpdated after replacement Open frame");

    assert!(
        !duplicate_accepted,
        "replacement emitted a second OrderAccepted"
    );
    assert_eq!(
        updated.venue_order_id,
        Some(VenueOrderId::from("ord-replaced-1"))
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_unexpected_response_shape_does_not_emit_updated() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Venue returned `result: {}` with no coherent replace outcome. The typed
    // handle rejects the inconsistent fields as a `Serde` error. A response the
    // client cannot trust leaves the replace outcome ambiguous (the venue may
    // have applied it), so the adapter emits no terminal event and lets
    // reconciliation settle the order rather than rebinding to a stale VOI or
    // falsely rejecting a live order.
    *ws_state.replace_reply.lock().await = Some(json!({"result": {}}));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-AMBIG");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-ambig")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3501.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    // Replace must still post even though the response shape is unexpected.
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.replace_orders.lock().await.is_empty() }
        },
        "replace posted",
    )
    .await;

    // No OrderUpdated (would rebind a stale VOI) and no ModifyRejected (would
    // falsely reject a possibly-live order): the ambiguous outcome is silent.
    let terminal = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(
                    OrderEventAny::Updated(_) | OrderEventAny::ModifyRejected(_),
                )) => return true,
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await;
    assert!(
        terminal.is_err(),
        "malformed replace result must not emit a terminal modify event",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_partial_replace_failure_emits_cancelled_once() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.replace_reply.lock().await = Some(json!({
        "result": {
            "order": null,
            "cancelled_order": order_json_with(
                "ord-partial-old",
                "STRAT-MOD-PARTIAL",
                "buy",
                "ETH-PERP",
                1_700_000_002_000_i64,
                "cancelled",
            ),
            "create_order_error": {
                "code": 10001,
                "message": "insufficient margin",
            },
        },
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-PARTIAL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-partial-old",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted",
    )
    .await;

    let cancel_frame = json!([order_json_with(
        "ord-partial-old",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "cancelled",
    )]);
    *ws_state.replace_notification_before_reply.lock().await =
        Some(make_subscription_frame(&orders_channel, &cancel_frame));

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-partial-old")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3505.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled after partial replace",
    )
    .await;
    let ExecutionEvent::Order(OrderEventAny::Canceled(cancelled)) = event else {
        unreachable!();
    };
    assert_eq!(cancelled.client_order_id, client_order_id);
    assert_eq!(
        cancelled.venue_order_id,
        Some(VenueOrderId::from("ord-partial-old")),
    );

    let duplicate_terminal = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(
                    OrderEventAny::Canceled(_)
                    | OrderEventAny::Updated(_)
                    | OrderEventAny::ModifyRejected(_),
                )) => return true,
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await;
    assert!(
        duplicate_terminal.is_err(),
        "partial replace must emit exactly one terminal order event",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_jsonrpc_rejection_emits_modify_rejected() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Venue surfaces a structured JSON-RPC error envelope.
    *ws_state.replace_reply.lock().await = Some(json!({
        "error": {"code": -32602, "message": "Invalid params"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-REJ");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-rej")),
        Some(Quantity::from("0.500")),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
        "OrderModifyRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
        let reason = rejected.reason.as_str();
        assert!(
            reason.contains("-32602") && reason.contains("Invalid params"),
            "unexpected reject reason: {reason}",
        );
        assert_eq!(
            rejected.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-stale-rej".to_string()),
        );
    } else {
        unreachable!();
    }
    // One replace request, no OrderUpdated should land.
    let replaces = ws_state.replace_orders.lock().await;
    assert_eq!(replaces.len(), 1, "expected exactly one replace request");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_jsonrpc_ambiguous_does_not_emit_modify_rejected() {
    // JSON-RPC `-32603` (generic internal error) leaves the replace outcome
    // ambiguous: the venue may have processed it and merely failed to
    // respond. Emitting OrderModifyRejected on those would let the engine
    // revert a successfully-replaced order; the adapter must stay silent
    // and rely on WS reconciliation. Mirrors the submit/cancel policy.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.replace_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal venue error"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-RETRY");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("ord-stale-retry")),
        Some(Quantity::from("0.500")),
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.replace_orders.lock().await.is_empty() }
        },
        "replace posted",
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))) => {
                    return Some("unexpected OrderModifyRejected on retryable code");
                }
                Some(ExecutionEvent::Order(OrderEventAny::Updated(_))) => {
                    return Some("unexpected OrderUpdated on retryable code");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "retryable JSON-RPC code must not emit a terminal modify event, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::no_venue_order_id(None, true, "venue_order_id is required")]
#[case::order_not_in_cache(Some(VenueOrderId::from("ord-x")), false, "order not found in cache")]
#[tokio::test]
async fn test_modify_order_rejects_invalid_command(
    #[case] venue_order_id: Option<VenueOrderId>,
    #[case] pre_insert_order: bool,
    #[case] reason_fragment: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-INVALID");

    if pre_insert_order {
        let order = build_limit_order(
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            Price::from("3500.00"),
            Quantity::from("1.000"),
        );
        tc.cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .expect("cache insert");
    }
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        venue_order_id,
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
        "OrderModifyRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
        assert!(
            rejected.reason.contains(reason_fragment),
            "expected reason to contain `{reason_fragment}`, was `{}`",
            rejected.reason.as_str(),
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.replace_orders.lock().await.is_empty(),
        "validation failure must not post to the venue",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_modify_order_rejects_trigger_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MOD-TRIGGER");
    let order = build_limit_if_touched_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3700.00"),
        Price::from("3600.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .expect("cache insert");

    let cmd = ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(VenueOrderId::from("trig-mod-1")),
        Some(Quantity::from("2.000")),
        Some(Price::from("3710.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.modify_order(cmd).expect("modify_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::ModifyRejected(_))),
        "OrderModifyRejected event",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
        assert_eq!(
            rejected.reason,
            "Derive trigger orders cannot be modified; cancel and resubmit",
        );
        assert_eq!(
            rejected.venue_order_id.map(|v| v.as_str().to_string()),
            Some("trig-mod-1".to_string()),
        );
    } else {
        unreachable!();
    }
    assert!(
        ws_state.replace_orders.lock().await.is_empty(),
        "trigger modify must not post private/replace",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_orders_fans_out_per_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let inner = |voi: &str| {
        CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            InstrumentId::from("ETH-PERP.DERIVE"),
            ClientOrderId::from(voi),
            Some(VenueOrderId::from(voi)),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    };
    let cmd = BatchCancelOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        vec![inner("ord-A"), inner("ord-B")],
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.batch_cancel_orders(cmd).expect("batch_cancel Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.cancelled_orders.lock().await.len() >= 2 }
        },
        "two cancels posted",
    )
    .await;
    let posts = ws_state.cancelled_orders.lock().await;
    let ids: Vec<&str> = posts
        .iter()
        .map(|b| b["order_id"].as_str().unwrap_or(""))
        .collect();
    assert!(ids.contains(&"ord-A") && ids.contains(&"ord-B"));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::venue_id(Some(VenueOrderId::from("ord-mock-1")))]
#[case::client_label(None)]
#[tokio::test]
async fn test_query_order_emits_order_status_report(#[case] venue_order_id: Option<VenueOrderId>) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = QueryOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-O-1"),
        venue_order_id,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.query_order(cmd).expect("query_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "OrderStatusReport event",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Order(report)) = event {
        let mut expected = OrderStatusReport::new(
            AccountId::from("DERIVE-001"),
            InstrumentId::from("ETH-PERP.DERIVE"),
            None,
            VenueOrderId::from("ord-mock-1"),
            Some(OrderSide::Buy),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from("1"),
            Quantity::from("0"),
            UnixNanos::from(1_700_000_000_000_000_000),
            UnixNanos::from(1_700_000_001_000_000_000),
            report.ts_init,
            Some(report.report_id),
        )
        .with_client_order_id(ClientOrderId::from("STRAT-O-1"))
        .with_price(Price::from("3500"));
        expected.avg_px = Some(dec!(3500));
        assert_eq!(*report, expected);
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::missing_label("UNKNOWN", "ETH-PERP.DERIVE", false)]
#[case::instrument_mismatch("STRAT-O-1", "BTC-PERP.DERIVE", false)]
#[case::malformed_response("STRAT-O-1", "ETH-PERP.DERIVE", true)]
#[tokio::test]
async fn test_query_order_by_label_does_not_emit_invalid_report(
    #[case] label: &str,
    #[case] instrument: &str,
    #[case] malformed: bool,
) {
    let rest_state = RestState::default();
    if malformed {
        *rest_state.open_orders_response.lock().await = json!({});
    }

    let mut tc = build_client(rest_state.clone(), WsState::default()).await;
    tc.client.connect().await.expect("connect succeeds");
    tc.client
        .query_order(QueryOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            InstrumentId::from(instrument),
            ClientOrderId::from(label),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .expect("query_order succeeds");

    wait_until_async(
        || async { !rest_state.open_orders_calls.lock().await.is_empty() },
        Duration::from_secs(5),
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if let Some(ExecutionEvent::Report(ExecutionReport::Order(report))) = tc.rx.recv().await
            {
                return report;
            }
        }
    })
    .await;

    assert!(outcome.is_err(), "unexpected report: {outcome:?}");
    assert_eq!(rest_state.open_orders_calls.lock().await.len(), 1);
    assert!(rest_state.get_order_calls.lock().await.is_empty());
    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_query_account_emits_account_state_event() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");
    // Drain the initial account-state event emitted at connect time so the
    // explicit query_account event below is the one we inspect.
    let _initial = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let cmd = QueryAccount::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        AccountId::from("DERIVE-001"),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.query_account(cmd).expect("query_account Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "AccountState event",
    )
    .await;

    if let ExecutionEvent::Account(state) = event {
        // sample subaccount carries 1000 USDC with no requirements; the
        // 100/50 net health values travel in `info`, not as margins
        assert_eq!(state.balances.len(), 1);
        assert_eq!(state.balances[0].total.as_decimal(), dec!(1000));
        assert_eq!(state.balances[0].locked.as_decimal(), dec!(0));
        assert_eq!(state.balances[0].free.as_decimal(), dec!(1000));
        assert_eq!(state.margins.len(), 1);
        assert_eq!(state.margins[0].initial.as_decimal(), dec!(0));
        assert_eq!(state.margins[0].maintenance.as_decimal(), dec!(0));
        let info = state.info.expect("account state carries risk info");
        assert_eq!(info.get("net_initial_margin"), Some(&json!("100")));
        assert_eq!(info.get("net_maintenance_margin"), Some(&json!("50")));
    } else {
        unreachable!();
    }

    let calls = rest_state.get_subaccount_calls.lock().await;
    // At least one call (connect refresh) plus the explicit query.
    assert!(calls.len() >= 2);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_balance_subscription_refreshes_authoritative_account_state() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");
    let _ = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let mut updated_subaccount = sample_subaccount_json();
    updated_subaccount["collaterals"][0]["amount"] = json!("1250");
    updated_subaccount["collaterals"][0]["mark_value"] = json!("1250");
    updated_subaccount["collaterals_value"] = json!("1250");
    updated_subaccount["subaccount_value"] = json!("1250");
    *rest_state.subaccount_response.lock().await = updated_subaccount;

    ws_state.push_notification(make_subscription_frame(
        &format!("{TEST_SUBACCOUNT}.balances"),
        &json!([{
            "name": "USDC",
            "new_balance": "1250",
            "previous_balance": "1000",
            "update_type": "asset_deposit",
        }]),
    ));

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Account(_)),
        "balance refresh AccountState",
    )
    .await;

    if let ExecutionEvent::Account(state) = event {
        assert_eq!(state.balances.len(), 1);
        assert_eq!(state.balances[0].total.as_decimal(), dec!(1250));
        assert_eq!(state.balances[0].locked.as_decimal(), dec!(0));
        assert_eq!(state.balances[0].free.as_decimal(), dec!(1250));
    } else {
        unreachable!();
    }
    assert!(rest_state.get_subaccount_calls.lock().await.len() >= 2);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_open_only_includes_trigger_orders() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Distinct payloads so the routing branch is observable.
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [order_json_with(
            "from-open", "L-OPEN", "buy", "ETH-PERP", 1_700_000_001_000, "open",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trigger_orders_response.lock().await = json!({
        "orders": [trigger_order_json_with(
            "from-trigger",
            "L-TRIGGER",
            "sell",
            "ETH-PERP",
            1_700_000_001_500,
            "market",
            "untriggered",
            "3582",
            "3600",
            "mark",
            "stoploss",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "from-history", "L-HIST", "buy", "ETH-PERP", 1_700_000_001_000, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        true,
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("reports");
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].venue_order_id.as_str(), "from-open");
    assert_eq!(reports[1].venue_order_id.as_str(), "from-trigger");
    assert_eq!(reports[1].order_type, OrderType::StopMarket);
    assert_eq!(reports[1].order_status, OrderStatus::Accepted);
    assert_eq!(reports[1].trigger_price, Some(Price::from("3600")));
    assert!(!rest_state.open_orders_calls.lock().await.is_empty());
    assert!(!rest_state.trigger_orders_calls.lock().await.is_empty());
    assert!(rest_state.order_history_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_history_path_when_not_open_only() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [order_json_with(
            "from-open", "L-OPEN", "buy", "ETH-PERP", 1_700_000_001_000, "open",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "from-history", "L-HIST", "buy", "ETH-PERP", 1_700_000_001_000, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        false,
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("reports");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].venue_order_id.as_str(), "from-history");
    let calls = rest_state.order_history_calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["page_size"].as_u64(), Some(500));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_paginates_across_multiple_pages() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.order_history_pages.lock().await = vec![
        json!({
            "orders": [order_json_with(
                "order-page-1", "L-PAGE-1", "buy", "ETH-PERP", 1_700_000_000_500, "filled",
            )],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
        json!({
            "orders": [order_json_with(
                "order-page-2", "L-PAGE-2", "sell", "ETH-PERP", 1_700_000_001_500, "filled",
            )],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
    ];
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        false,
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        Some(UnixNanos::from(1_700_000_000_000_123_456_u64)),
        Some(UnixNanos::from(1_700_000_002_000_999_999_u64)),
        None,
        None,
    );
    let reports = tc
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("reports");

    let mut venue_order_ids: Vec<&str> = reports
        .iter()
        .map(|report| report.venue_order_id.as_str())
        .collect();
    venue_order_ids.sort_unstable();
    assert_eq!(venue_order_ids, vec!["order-page-1", "order-page-2"]);

    let calls = rest_state.order_history_calls.lock().await;
    assert_eq!(calls.len(), 2, "must request both pages");
    assert_eq!(calls[0]["page"].as_u64(), Some(1));
    assert_eq!(calls[0]["page_size"].as_u64(), Some(500));
    assert_eq!(calls[0]["from_timestamp"].as_i64(), Some(1_700_000_000_000));
    assert_eq!(calls[0]["to_timestamp"].as_i64(), Some(1_700_000_002_000));
    assert_eq!(calls[1]["page"].as_u64(), Some(2));
    assert_eq!(calls[1]["page_size"].as_u64(), Some(500));
    assert_eq!(calls[1]["from_timestamp"].as_i64(), Some(1_700_000_000_000));
    assert_eq!(calls[1]["to_timestamp"].as_i64(), Some(1_700_000_002_000));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_open_only_ignores_time_window() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [
            order_json_with("early", "E", "buy", "ETH-PERP", 100, "open"),
            order_json_with("middle", "M", "buy", "ETH-PERP", 200, "open"),
            order_json_with("late", "L", "buy", "ETH-PERP", 300, "open"),
        ],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        true,
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        Some(UnixNanos::from(150_000_000_u64)), // 150 ms
        Some(UnixNanos::from(250_000_000_u64)), // 250 ms
        None,
        None,
    );
    let reports = tc
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("reports");
    assert_eq!(
        reports
            .iter()
            .map(|report| report.venue_order_id.as_str())
            .collect::<Vec<_>>(),
        vec!["early", "middle", "late"],
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::status_report(false)]
#[case::query_order(true)]
#[tokio::test]
async fn test_generate_order_status_report_falls_back_to_history_by_label(
    #[case] query_order: bool,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });

    let mut filled_order = order_json_with(
        "ord-hist-1",
        "STRAT-LABEL",
        "sell",
        "ETH-PERP",
        1_700_000_001_000,
        "filled",
    );
    filled_order["amount"] = json!("1.25");
    filled_order["filled_amount"] = json!("1.25");
    *rest_state.order_history_pages.lock().await = vec![
        json!({
            "orders": [order_json_with(
                "ord-unrelated", "OTHER-LABEL", "buy", "ETH-PERP", 1_700_000_000_500, "filled",
            )],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
        json!({
            "orders": [filled_order],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
    ];
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        Some(ClientOrderId::from("STRAT-LABEL")),
        None,
        None,
        None,
    );

    let report = if query_order {
        query_order_report(&mut tc, &cmd).await
    } else {
        tc.client
            .generate_order_status_report(&cmd)
            .await
            .expect("report")
            .expect("some")
    };

    let mut expected = OrderStatusReport::new(
        AccountId::from("DERIVE-001"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        VenueOrderId::from("ord-hist-1"),
        Some(OrderSide::Sell),
        OrderType::Limit,
        TimeInForce::Gtc,
        OrderStatus::Filled,
        Quantity::from("1.25"),
        Quantity::from("1.25"),
        UnixNanos::from(1_700_000_000_000_000_000),
        UnixNanos::from(1_700_000_001_000_000_000),
        report.ts_init,
        Some(report.report_id),
    )
    .with_client_order_id(ClientOrderId::from("STRAT-LABEL"))
    .with_price(Price::from("3500"));
    expected.avg_px = Some(dec!(3500));
    assert_eq!(report, expected);
    assert!(rest_state.get_order_calls.lock().await.is_empty());
    assert_eq!(rest_state.open_orders_calls.lock().await.len(), 1);
    assert_eq!(rest_state.trigger_orders_calls.lock().await.len(), 1);
    assert_eq!(
        *rest_state.order_history_calls.lock().await,
        vec![
            json!({"subaccount_id": TEST_SUBACCOUNT, "page": 1, "page_size": 500, "instrument_name": "ETH-PERP"}),
            json!({"subaccount_id": TEST_SUBACCOUNT, "page": 2, "page_size": 500, "instrument_name": "ETH-PERP"}),
        ],
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::status_report(false)]
#[case::query_order(true)]
#[tokio::test]
async fn test_generate_order_status_report_finds_trigger_order_by_label_before_history(
    #[case] query_order: bool,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trigger_orders_response.lock().await = json!({
        "orders": [trigger_order_json_with(
            "trig-label-1",
            "STRAT-TRIG-LABEL",
            "sell",
            "ETH-PERP",
            1_700_000_001_000,
            "limit",
            "untriggered",
            "3700",
            "3800",
            "mark",
            "takeprofit",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-hist-1", "STRAT-TRIG-LABEL", "buy", "ETH-PERP", 1, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        Some(ClientOrderId::from("STRAT-TRIG-LABEL")),
        None,
        None,
        None,
    );

    let report = if query_order {
        query_order_report(&mut tc, &cmd).await
    } else {
        tc.client
            .generate_order_status_report(&cmd)
            .await
            .expect("report")
            .expect("some")
    };

    let expected = OrderStatusReport::new(
        AccountId::from("DERIVE-001"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        VenueOrderId::from("trig-label-1"),
        Some(OrderSide::Sell),
        OrderType::LimitIfTouched,
        TimeInForce::Gtc,
        OrderStatus::Accepted,
        Quantity::from("1"),
        Quantity::from("0"),
        UnixNanos::from(1_700_000_000_000_000_000),
        UnixNanos::from(1_700_000_001_000_000_000),
        report.ts_init,
        Some(report.report_id),
    )
    .with_client_order_id(ClientOrderId::from("STRAT-TRIG-LABEL"))
    .with_price(Price::from("3700"))
    .with_trigger_price(Price::from("3800"))
    .with_trigger_type(TriggerType::MarkPrice);
    assert_eq!(report, expected);
    assert!(!rest_state.open_orders_calls.lock().await.is_empty());
    assert!(!rest_state.trigger_orders_calls.lock().await.is_empty());
    assert!(
        rest_state.order_history_calls.lock().await.is_empty(),
        "trigger match must skip order history",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_report_returns_none_on_instrument_mismatch() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Default get_order response has instrument_name = "ETH-PERP"; ask for BTC.
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("BTC-PERP.DERIVE")),
        None,
        Some(VenueOrderId::from("ord-mock-1")),
        None,
        None,
    );
    let report = tc
        .client
        .generate_order_status_report(&cmd)
        .await
        .expect("report");
    assert!(report.is_none());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_filters_by_venue_order_id() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [
            sample_trade_json("trade-a", "ord-1", "ETH-PERP"),
            sample_trade_json("trade-b", "ord-2", "ETH-PERP"),
        ],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        Some(VenueOrderId::from("ord-2")),
        None,
        None,
        None,
        None,
    );
    let reports = tc.client.generate_fill_reports(cmd).await.expect("fills");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].trade_id.as_str(), "trade-b");
    assert_eq!(reports[0].venue_order_id.as_str(), "ord-2");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_filters_by_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.positions_response.lock().await = json!({
        "positions": [
            sample_position_json("ETH-PERP", "3"),
            sample_position_json("BTC-PERP", "-1"),
        ],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_position_status_reports(&cmd)
        .await
        .expect("positions");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id.symbol.as_str(), "ETH-PERP");
    assert_eq!(reports[0].signed_decimal_qty.to_string(), "3");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_builds_startup_snapshot_from_http_reports() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-open-1", "L-OPEN", "buy", "ETH-PERP", 1_700_000_001_000, "open",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-filled-1", "L-FILLED", "sell", "ETH-PERP", 1_700_000_002_000, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [sample_trade_json("trade-fill-1", "ord-filled-1", "ETH-PERP")],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.positions_response.lock().await = json!({
        "positions": [sample_position_json("ETH-PERP", "0.3")],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let mass_status = tc
        .client
        .generate_mass_status(Some(10_000_000))
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");

    let order_reports = mass_status.order_reports();
    let fill_reports = mass_status.fill_reports();
    let position_reports = mass_status.position_reports();
    let eth_position_reports = position_reports
        .get(&InstrumentId::from("ETH-PERP.DERIVE"))
        .expect("ETH-PERP position report");

    assert_eq!(mass_status.client_id, ClientId::from("DERIVE"));
    assert_eq!(mass_status.account_id, AccountId::from("DERIVE-001"));
    assert_eq!(mass_status.venue, *DERIVE_VENUE);
    assert_eq!(order_reports.len(), 2);
    assert!(order_reports.contains_key(&VenueOrderId::from("ord-open-1")));
    assert!(order_reports.contains_key(&VenueOrderId::from("ord-filled-1")));
    assert_eq!(fill_reports.len(), 1);
    assert!(fill_reports.contains_key(&VenueOrderId::from("ord-filled-1")));
    assert_eq!(eth_position_reports.len(), 1);
    assert_eq!(eth_position_reports[0].signed_decimal_qty, dec!(0.3));

    let open_order_calls = rest_state.open_orders_calls.lock().await;
    let order_history_calls = rest_state.order_history_calls.lock().await;
    let trade_history_calls = rest_state.trade_history_calls.lock().await;
    let position_calls = rest_state.positions_calls.lock().await;

    assert_eq!(open_order_calls.len(), 1);
    assert_eq!(order_history_calls.len(), 1);
    assert_eq!(trade_history_calls.len(), 1);
    assert_eq!(position_calls.len(), 1);
    assert!(open_order_calls[0].get("from_timestamp").is_none());
    assert!(
        order_history_calls[0]
            .get("from_timestamp")
            .and_then(Value::as_i64)
            .is_some()
    );
    assert!(
        trade_history_calls[0]
            .get("from_timestamp")
            .and_then(Value::as_i64)
            .is_some()
    );
    assert!(position_calls[0].get("from_timestamp").is_none());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_normalizes_duplicate_labels() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let old_order = order_json_with(
        "ord-replace-old",
        "L-REPLACE",
        "buy",
        "ETH-PERP",
        1_700_000_001_000,
        "cancelled",
    );
    let mut replacement_order = order_json_with(
        "ord-replace-new",
        "L-REPLACE",
        "buy",
        "ETH-PERP",
        1_700_000_002_000,
        "open",
    );
    replacement_order["replaced_order_id"] = json!("ord-replace-old");
    let reused_order_1 = order_json_with(
        "ord-reused-1",
        "L-REUSED",
        "sell",
        "ETH-PERP",
        1_700_000_003_000,
        "filled",
    );
    let reused_order_2 = order_json_with(
        "ord-reused-2",
        "L-REUSED",
        "sell",
        "ETH-PERP",
        1_700_000_004_000,
        "cancelled",
    );
    let mixed_old_order = order_json_with(
        "ord-mixed-old",
        "L-MIXED",
        "buy",
        "ETH-PERP",
        1_700_000_005_000,
        "cancelled",
    );
    let mut mixed_replacement_order = order_json_with(
        "ord-mixed-new",
        "L-MIXED",
        "buy",
        "ETH-PERP",
        1_700_000_006_000,
        "filled",
    );
    mixed_replacement_order["replaced_order_id"] = json!("ord-mixed-old");
    let mixed_unrelated_order = order_json_with(
        "ord-mixed-unrelated",
        "L-MIXED",
        "sell",
        "ETH-PERP",
        1_700_000_007_000,
        "cancelled",
    );
    let active_history_order = order_json_with(
        "ord-active",
        "L-ACTIVE",
        "buy",
        "ETH-PERP",
        1_700_000_008_000,
        "open",
    );
    let active_label_reuse = order_json_with(
        "ord-active-reuse",
        "L-ACTIVE",
        "sell",
        "ETH-PERP",
        1_700_000_009_000,
        "filled",
    );
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [active_history_order.clone()],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [
            old_order,
            replacement_order,
            reused_order_1,
            reused_order_2,
            mixed_old_order,
            mixed_replacement_order,
            mixed_unrelated_order,
            active_history_order,
            active_label_reuse,
        ],
        "pagination": {"count": 9, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [trade_json_with_label(
            "trade-reused-1",
            "ord-reused-1",
            "ETH-PERP",
            "L-REUSED",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let history_cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        false,
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
    );
    let history_reports = tc
        .client
        .generate_order_status_reports(&history_cmd)
        .await
        .expect("history reports");
    assert_eq!(history_reports.len(), 9);
    for report in &history_reports {
        let expected = match report.venue_order_id.as_str() {
            "ord-replace-old" | "ord-replace-new" => ClientOrderId::from("L-REPLACE"),
            "ord-reused-1" | "ord-reused-2" => ClientOrderId::from("L-REUSED"),
            "ord-mixed-old" | "ord-mixed-new" | "ord-mixed-unrelated" => {
                ClientOrderId::from("L-MIXED")
            }
            "ord-active" | "ord-active-reuse" => ClientOrderId::from("L-ACTIVE"),
            venue_order_id => panic!("unexpected venue order ID {venue_order_id}"),
        };
        assert_eq!(report.client_order_id, Some(expected));
    }

    let mass_status = tc
        .client
        .generate_mass_status(Some(10_000_000))
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");
    let reports = mass_status.order_reports();
    let old_report = reports
        .get(&VenueOrderId::from("ord-replace-old"))
        .expect("superseded order report");
    let replacement_report = reports
        .get(&VenueOrderId::from("ord-replace-new"))
        .expect("replacement order report");

    assert_eq!(
        old_report.client_order_id,
        Some(ClientOrderId::from("L-REPLACE")),
    );
    assert_eq!(
        replacement_report.client_order_id,
        Some(ClientOrderId::from("L-REPLACE")),
    );

    for venue_order_id in ["ord-reused-1", "ord-reused-2"] {
        let report = reports
            .get(&VenueOrderId::from(venue_order_id))
            .expect("reused-label order report");
        assert!(report.client_order_id.is_none());
    }

    for venue_order_id in ["ord-mixed-old", "ord-mixed-new", "ord-mixed-unrelated"] {
        let report = reports
            .get(&VenueOrderId::from(venue_order_id))
            .expect("mixed-label order report");
        assert!(report.client_order_id.is_none());
    }
    assert_eq!(
        reports
            .get(&VenueOrderId::from("ord-active"))
            .expect("open order report")
            .client_order_id,
        Some(ClientOrderId::from("L-ACTIVE")),
    );
    assert!(
        reports
            .get(&VenueOrderId::from("ord-active-reuse"))
            .expect("reused active-label order report")
            .client_order_id
            .is_none(),
    );
    let fills = mass_status.fill_reports();
    let reused_fills = fills
        .get(&VenueOrderId::from("ord-reused-1"))
        .expect("reused-label fill reports");
    assert_eq!(reused_fills.len(), 1);
    assert_eq!(reused_fills[0].trade_id, TradeId::from("trade-reused-1"));
    assert!(reused_fills[0].client_order_id.is_none());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_adds_flat_position_without_current_position() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-filled-flat", "L-FILLED-FLAT", "buy", "ETH-PERP", 1_700_000_002_000, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [sample_trade_json("trade-flat-1", "ord-filled-flat", "ETH-PERP")],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.positions_response.lock().await = json!({
        "positions": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let mass_status = tc
        .client
        .generate_mass_status(Some(10_000_000))
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");

    let position_reports = mass_status.position_reports();
    let eth_reports = position_reports
        .get(&InstrumentId::from("ETH-PERP.DERIVE"))
        .expect("ETH-PERP flat position report");

    assert_eq!(eth_reports.len(), 1);
    assert_eq!(eth_reports[0].position_side, PositionSide::Flat);
    assert_eq!(eth_reports[0].signed_decimal_qty, dec!(0));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_does_not_flatten_unconverted_position() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [
            order_json_with(
                "ord-held-eth", "L-HELD-ETH", "buy", "ETH-PERP", 1_700_000_003_000, "filled",
            ),
            order_json_with(
                "ord-flat-btc", "L-FLAT-BTC", "sell", "BTC-PERP", 1_700_000_004_000, "filled",
            ),
        ],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [],
        "pagination": {"count": 0, "num_pages": 0},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.positions_response.lock().await = json!({
        "positions": [
            sample_position_json("ETH-PERP", "0.1234567890123456789012345678912345"),
            sample_position_json("SOL-PERP", "2.5"),
        ],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let mass_status = tc
        .client
        .generate_mass_status(Some(10_000_000))
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");

    let position_reports = mass_status.position_reports();
    assert!(
        !position_reports.contains_key(&InstrumentId::from("ETH-PERP.DERIVE")),
        "unconverted held position must not be reported as flat",
    );

    let sol_reports = position_reports
        .get(&InstrumentId::from("SOL-PERP.DERIVE"))
        .expect("valid SOL-PERP position report");
    assert_eq!(sol_reports.len(), 1);
    assert_eq!(sol_reports[0].position_side, PositionSide::Long);
    assert_eq!(sol_reports[0].signed_decimal_qty, dec!(2.5));

    let btc_reports = position_reports
        .get(&InstrumentId::from("BTC-PERP.DERIVE"))
        .expect("genuinely absent BTC-PERP position has a flat report");
    assert_eq!(btc_reports.len(), 1);
    assert_eq!(btc_reports[0].position_side, PositionSide::Flat);
    assert_eq!(btc_reports[0].signed_decimal_qty, dec!(0));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_without_lookback_omits_time_window() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [],
        "pagination": {"count": 0, "num_pages": 0},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [],
        "pagination": {"count": 0, "num_pages": 0},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.positions_response.lock().await = json!({
        "positions": [],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let mass_status = tc
        .client
        .generate_mass_status(None)
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");

    let open_order_calls = rest_state.open_orders_calls.lock().await;
    let order_history_calls = rest_state.order_history_calls.lock().await;
    let trade_history_calls = rest_state.trade_history_calls.lock().await;
    let position_calls = rest_state.positions_calls.lock().await;

    assert!(mass_status.order_reports().is_empty());
    assert!(mass_status.fill_reports().is_empty());
    assert!(mass_status.position_reports().is_empty());
    assert_eq!(open_order_calls.len(), 1);
    assert_eq!(order_history_calls.len(), 1);
    assert_eq!(trade_history_calls.len(), 1);
    assert_eq!(position_calls.len(), 1);
    assert!(open_order_calls[0].get("from_timestamp").is_none());
    assert!(order_history_calls[0].get("from_timestamp").is_none());
    assert!(trade_history_calls[0].get("from_timestamp").is_none());
    assert!(position_calls[0].get("from_timestamp").is_none());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_mass_status_prefers_open_order_snapshot_on_overlap() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-overlap-1", "L-OPEN", "buy", "ETH-PERP", 1_700_000_003_000, "open",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    *rest_state.order_history_response.lock().await = json!({
        "orders": [order_json_with(
            "ord-overlap-1", "L-HISTORY", "buy", "ETH-PERP", 1_700_000_002_000, "filled",
        )],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let mass_status = tc
        .client
        .generate_mass_status(Some(10_000_000))
        .await
        .expect("mass status request succeeds")
        .expect("Derive returns mass status");
    let order_reports = mass_status.order_reports();
    let report = order_reports
        .get(&VenueOrderId::from("ord-overlap-1"))
        .expect("overlapping order report");

    assert_eq!(order_reports.len(), 1);
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(
        report.client_order_id.map(|id| id.as_str().to_string()),
        Some("L-OPEN".to_string()),
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_report_by_venue_id_uses_get_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        Some(VenueOrderId::from("ord-mock-1")),
        None,
        None,
    );
    let report = tc
        .client
        .generate_order_status_report(&cmd)
        .await
        .expect("report")
        .expect("some");
    assert_eq!(report.venue_order_id.as_str(), "ord-mock-1");
    let calls = rest_state.get_order_calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["order_id"].as_str(), Some("ord-mock-1"));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_report_by_venue_id_falls_back_to_trigger_orders() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_order_response.lock().await = json!({
        "jsonrpc": "2.0",
        "error": {"code": -32602, "message": "Order not found"},
    });
    *rest_state.trigger_orders_response.lock().await = json!({
        "orders": [trigger_order_json_with(
            "trig-venue-1",
            "STRAT-TRIG-VENUE",
            "buy",
            "ETH-PERP",
            1_700_000_001_000,
            "market",
            "untriggered",
            "3417",
            "3400",
            "mark",
            "stoploss",
        )],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        Some(VenueOrderId::from("trig-venue-1")),
        None,
        None,
    );
    let report = tc
        .client
        .generate_order_status_report(&cmd)
        .await
        .expect("report")
        .expect("some");
    assert_eq!(report.venue_order_id.as_str(), "trig-venue-1");
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(
        report.client_order_id,
        Some(ClientOrderId::from("STRAT-TRIG-VENUE"))
    );
    assert_eq!(rest_state.get_order_calls.lock().await.len(), 1);
    assert_eq!(rest_state.trigger_orders_calls.lock().await.len(), 1);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_orders_notification_emits_order_status_report() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    // Wait for the connect-time subscribe to land before pushing a
    // notification so the order of operations matches the live venue.
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;

    // Drain the initial account-state event emitted at connect.
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let data = json!([sample_order_json()]);
    let frame = make_subscription_frame(&channel, &data);
    ws_state.push_notification(frame);

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "OrderStatusReport from WS",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Order(report)) = event {
        assert_eq!(report.venue_order_id.as_str(), "ord-mock-1");
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_trades_notification_emits_fill_report() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.trades");
    let data = json!([sample_trade_json("trade-ws-1", "ord-ws-1", "ETH-PERP")]);
    let frame = make_subscription_frame(&channel, &data);
    ws_state.push_notification(frame);

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Fill(_))),
        "FillReport from WS",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Fill(report)) = event {
        assert_eq!(report.trade_id.as_str(), "trade-ws-1");
        assert_eq!(report.venue_order_id.as_str(), "ord-ws-1");
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_trades_dedup_suppresses_repeated_trade_id() {
    // The same trade arriving twice on the WS .trades channel (typical
    // immediately after a reconnect replay) must emit only one FillReport.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.trades");
    let data = json!([sample_trade_json("trade-dup-1", "ord-dup-1", "ETH-PERP")]);
    ws_state.push_notification(make_subscription_frame(&channel, &data));
    ws_state.push_notification(make_subscription_frame(&channel, &data));

    let first = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Fill(_))),
        "first FillReport from WS",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Fill(report)) = first {
        assert_eq!(report.trade_id.as_str(), "trade-dup-1");
    } else {
        unreachable!();
    }

    // The second frame must be suppressed. Give the dispatch loop enough
    // headroom to process it; if dedup is wired correctly nothing arrives.
    let second = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Report(ExecutionReport::Fill(_))) => return true,
                Some(_) => {}
                None => return false,
            }
        }
    })
    .await;
    assert!(
        second.is_err(),
        "duplicate trade_id must not produce a second FillReport",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cross_source_dedup_skips_ws_trade_in_generate_fill_reports() {
    // WS dispatches a fill first; a subsequent HTTP reconciliation pull whose
    // window overlaps the live stream returns the same trade_id. The HTTP
    // path must drop the duplicate so the reconciler does not re-apply a
    // fill the live engine has already processed.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Reconciliation response carries one trade with the same trade_id the
    // WS will have already emitted, plus one fresh trade that should pass.
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [
            sample_trade_json("trade-shared-1", "ord-1", "ETH-PERP"),
            sample_trade_json("trade-fresh-1", "ord-2", "ETH-PERP"),
        ],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    // Push the shared trade through WS first so it enters the dedup set.
    let channel = format!("{TEST_SUBACCOUNT}.trades");
    let data = json!([sample_trade_json("trade-shared-1", "ord-1", "ETH-PERP")]);
    ws_state.push_notification(make_subscription_frame(&channel, &data));
    let ws_event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Fill(_))),
        "WS FillReport for shared trade",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Fill(report)) = ws_event {
        assert_eq!(report.trade_id.as_str(), "trade-shared-1");
    } else {
        unreachable!();
    }

    // HTTP reconciliation now returns the same trade plus a fresh one; only
    // the fresh one should survive dedup.
    let cmd = GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
        None,
    );
    let reports = tc.client.generate_fill_reports(cmd).await.expect("fills");
    assert_eq!(reports.len(), 1, "shared trade must be deduplicated");
    assert_eq!(reports[0].trade_id.as_str(), "trade-fresh-1");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_does_not_mark_unconsumed_trades_emitted() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [sample_trade_json("trade-retry-1", "ord-1", "ETH-PERP")],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let generate = || {
        GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("ETH-PERP.DERIVE")),
            None,
            None,
            None,
            None,
            None,
        )
    };
    let first = tc
        .client
        .generate_fill_reports(generate())
        .await
        .expect("first fill generation succeeds");
    let retry = tc
        .client
        .generate_fill_reports(generate())
        .await
        .expect("retry fill generation succeeds");

    assert_eq!(first.len(), 1);
    assert_eq!(retry.len(), 1);
    assert_eq!(first[0].trade_id.as_str(), "trade-retry-1");
    assert_eq!(retry[0].trade_id.as_str(), "trade-retry-1");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_trades_failed_commission_conversion_does_not_record_dedup() {
    // The failed construction must not poison dedup for the same trade_id
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.trades");
    let mut bad_fee = sample_trade_json("trade-fee-ws-1", "ord-fee-ws-1", "ETH-PERP");
    bad_fee["trade_fee"] = json!("79228162514264337593543950335");
    ws_state.push_notification(make_subscription_frame(&channel, &json!([bad_fee])));
    ws_state.push_notification(make_subscription_frame(
        &channel,
        &json!([sample_trade_json(
            "trade-fee-ws-1",
            "ord-fee-ws-1",
            "ETH-PERP"
        )]),
    ));

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Fill(_))),
        "FillReport after failed commission conversion",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Fill(report)) = event {
        assert_eq!(report.trade_id.as_str(), "trade-fee-ws-1");
        assert_eq!(report.commission.as_decimal(), dec!(0.5));
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_skips_unrepresentable_commission_and_retries() {
    // The failed row is skipped, not marked processed, so a later poll
    // re-fetches it
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut bad_fee = sample_trade_json("trade-fee-rest-1", "ord-1", "ETH-PERP");
    bad_fee["trade_fee"] = json!("79228162514264337593543950335");
    *rest_state.trade_history_response.lock().await = json!({
        "trades": [
            bad_fee,
            sample_trade_json("trade-fee-rest-2", "ord-2", "ETH-PERP"),
        ],
        "pagination": {"count": 2, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let generate = || {
        GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from("ETH-PERP.DERIVE")),
            None,
            None,
            None,
            None,
            None,
        )
    };
    let reports = tc
        .client
        .generate_fill_reports(generate())
        .await
        .expect("fill generation survives the unrepresentable fee row");
    assert_eq!(reports.len(), 1, "failed row must be skipped");
    assert_eq!(reports[0].trade_id.as_str(), "trade-fee-rest-2");

    *rest_state.trade_history_response.lock().await = json!({
        "trades": [sample_trade_json("trade-fee-rest-1", "ord-1", "ETH-PERP")],
        "pagination": {"count": 1, "num_pages": 1},
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let retried = tc
        .client
        .generate_fill_reports(generate())
        .await
        .expect("retry fill generation succeeds");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].trade_id.as_str(), "trade-fee-rest-1");

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_tracked_order_open_emits_order_accepted_once() {
    // Submit an order so its identity is registered, then push the venue's
    // `.orders` Open notice twice (the second simulates a reconnect replay).
    // The dispatch must route the first frame to a proper `OrderAccepted`
    // event and suppress the duplicate on the second.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-TRACKED-OPEN");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    // OrderSubmitted fires synchronously from `submit_order`.
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let frame = json!([order_json_with(
        "ord-tracked-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on first Open",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = event {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.venue_order_id.as_str(), "ord-tracked-1");
        // Identity fields captured at submit must propagate to the event.
        assert_eq!(accepted.strategy_id, StrategyId::from("S-1"));
        assert_eq!(accepted.instrument_id, instrument_id);
    } else {
        unreachable!();
    }

    // Replay the same Open frame. The dispatch must suppress the duplicate
    // Accepted and must not emit an OrderStatusReport fallback.
    ws_state.push_notification(make_subscription_frame(&channel, &frame));
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Accepted(_))) => {
                    return Some("duplicate Accepted");
                }
                Some(ExecutionEvent::Report(ExecutionReport::Order(_))) => {
                    return Some("fallback OrderStatusReport");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "tracked replay must not emit further events, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_tracked_fill_emits_order_filled_and_dedupes_by_trade_id() {
    // Submit an order, then push a `.trades` frame whose label matches the
    // tracked order. The dispatch must synthesize Accepted (since no Open
    // came first), emit OrderFilled (not FillReport), and drop a replayed
    // trade with the same trade_id.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-TRACKED-FILL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.trades");
    let frame = json!([trade_json_with_label(
        "trade-tracked-1",
        "ord-tracked-1",
        "ETH-PERP",
        client_order_id.as_str(),
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    // Synthesized Accepted lands before the Filled, in lifecycle order.
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "synthesized OrderAccepted",
    )
    .await;
    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on tracked trade",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = event {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.trade_id.as_str(), "trade-tracked-1");
    } else {
        unreachable!();
    }

    // Replay the same trade. Dedup must drop it: no further Filled, no
    // fallback FillReport.
    ws_state.push_notification(make_subscription_frame(&channel, &frame));
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Filled(_))) => {
                    return Some("duplicate Filled");
                }
                Some(ExecutionEvent::Report(ExecutionReport::Fill(_))) => {
                    return Some("fallback FillReport");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "replayed trade must be deduped, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_orders_filled_before_trades_still_emits_tracked_fill() {
    // Venue split-channel ordering: the `.orders` Filled notice can arrive
    // before the matching `.trades` record. The dispatch must keep the
    // tracked identity alive across that gap so the trade still emits
    // `OrderFilled` instead of falling through to `FillReport`.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-SPLIT-CHAN");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    // `.orders` Filled arrives first.
    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let orders_frame = json!([order_json_with(
        "ord-split-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_005_000_i64,
        "filled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &orders_frame));
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "synthesized OrderAccepted from terminal `.orders` Filled",
    )
    .await;

    // Now the matching `.trades` frame lands. Must take the tracked path
    // and emit `OrderFilled`, not a `FillReport`.
    let trades_channel = format!("{TEST_SUBACCOUNT}.trades");
    let trades_frame = json!([trade_json_with_label(
        "trade-split-1",
        "ord-split-1",
        "ETH-PERP",
        client_order_id.as_str(),
    )]);
    ws_state.push_notification(make_subscription_frame(&trades_channel, &trades_frame));

    let event = drain_until(
        &mut tc.rx,
        |e| {
            matches!(
                e,
                ExecutionEvent::Order(OrderEventAny::Filled(_))
                    | ExecutionEvent::Report(ExecutionReport::Fill(_))
            )
        },
        "fill emission",
    )
    .await;

    match event {
        ExecutionEvent::Order(OrderEventAny::Filled(filled)) => {
            assert_eq!(filled.client_order_id, client_order_id);
            assert_eq!(filled.trade_id.as_str(), "trade-split-1");
        }
        ExecutionEvent::Report(_) => {
            panic!(
                "tracked fill must not fall back to FillReport when `.orders` Filled came first"
            );
        }
        _ => unreachable!(),
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_external_order_falls_back_to_status_report() {
    // A `.orders` frame whose label has no registered identity (external or
    // pre-existing order) must take the report path so the reconciler can
    // ingest the state without misrouted lifecycle events.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let frame = json!([order_json_with(
        "ord-external-1",
        "EXTERNAL-LABEL",
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "OrderStatusReport for external order",
    )
    .await;

    if let ExecutionEvent::Report(ExecutionReport::Order(report)) = event {
        assert_eq!(report.venue_order_id.as_str(), "ord-external-1");
        assert_eq!(
            report.client_order_id.map(|c| c.as_str().to_string()),
            Some("EXTERNAL-LABEL".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case::canceled("cancelled", "canceled")]
#[case::expired("expired", "expired")]
#[tokio::test]
async fn test_ws_dispatch_tracked_terminal_status_emits_proper_event_and_forgets_identity(
    #[case] status: &str,
    #[case] expected: &str,
) {
    // Tracked Canceled / Expired must:
    //   1. Synthesize OrderAccepted (carrying ts_accepted from the report)
    //   2. Emit the terminal event (carrying ts_last)
    //   3. Forget identity, so a replayed `.orders` frame for the same CID
    //      falls back to OrderStatusReport
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-TERMINAL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    // Distinct creation vs. last_update lets us verify which one each event
    // carries: the synthesized Accepted must use creation_timestamp, the
    // terminal event must use last_update_timestamp.
    let creation_ms: i64 = 1_700_000_000_000;
    let last_update_ms: i64 = 1_700_000_005_000;
    let expected_accepted_ns = UnixNanos::from((creation_ms as u64) * 1_000_000);
    let expected_terminal_ns = UnixNanos::from((last_update_ms as u64) * 1_000_000);

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let frame = json!([order_json_with(
        "ord-terminal-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        last_update_ms,
        status,
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    let accepted_event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "synthesized OrderAccepted",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted_event {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.venue_order_id.as_str(), "ord-terminal-1");
        assert_eq!(
            accepted.ts_event, expected_accepted_ns,
            "synthesized Accepted must carry ts_accepted (creation_timestamp)",
        );
    } else {
        unreachable!();
    }

    match expected {
        "canceled" => {
            let event = drain_until(
                &mut tc.rx,
                |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
                "OrderCanceled",
            )
            .await;

            if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
                assert_eq!(canceled.client_order_id, client_order_id);
                assert_eq!(canceled.venue_order_id.unwrap().as_str(), "ord-terminal-1");
                assert_eq!(canceled.ts_event, expected_terminal_ns);
            } else {
                unreachable!();
            }
        }
        "expired" => {
            let event = drain_until(
                &mut tc.rx,
                |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Expired(_))),
                "OrderExpired",
            )
            .await;

            if let ExecutionEvent::Order(OrderEventAny::Expired(expired)) = event {
                assert_eq!(expired.client_order_id, client_order_id);
                assert_eq!(expired.venue_order_id.unwrap().as_str(), "ord-terminal-1");
                assert_eq!(expired.ts_event, expected_terminal_ns);
            } else {
                unreachable!();
            }
        }
        _ => unreachable!("unexpected variant marker {expected}"),
    }

    // Replay the same frame. Identity was forgotten, so the dispatch must
    // fall back to the report path.
    ws_state.push_notification(make_subscription_frame(&channel, &frame));
    let event = drain_until(
        &mut tc.rx,
        |e| {
            matches!(
                e,
                ExecutionEvent::Report(ExecutionReport::Order(_))
                    | ExecutionEvent::Order(
                        OrderEventAny::Canceled(_)
                            | OrderEventAny::Expired(_)
                            | OrderEventAny::Accepted(_),
                    )
            )
        },
        "post-terminal replay",
    )
    .await;
    assert!(
        matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "after terminal, replayed frame must fall back to OrderStatusReport, was {event:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_tracked_rejected_emits_rejected_without_synthesized_accepted() {
    // Rejected is deliberately asymmetric with Canceled/Expired: a venue-side
    // rejection can precede any Accepted notice, so the dispatch must NOT
    // synthesize an Accepted before the Rejected event.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-REJECTED-WS");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let frame = json!([order_json_with(
        "ord-rej-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "rejected",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    // The first lifecycle event that lands must be the Rejected itself, with
    // no synthesized Accepted preceding it.
    let event = drain_until(
        &mut tc.rx,
        |e| {
            matches!(
                e,
                ExecutionEvent::Order(OrderEventAny::Accepted(_) | OrderEventAny::Rejected(_),)
            )
        },
        "Rejected (without prior Accepted)",
    )
    .await;

    match event {
        ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) => {
            assert_eq!(rejected.client_order_id, client_order_id);
            assert_eq!(rejected.reason, "Order rejected by Derive");
            assert!(!rejected.due_post_only);
        }
        ExecutionEvent::Order(OrderEventAny::Accepted(_)) => {
            panic!("Rejected path must not synthesize OrderAccepted");
        }
        _ => unreachable!(),
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_post_only_cross_rejected_sets_due_post_only() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-POST-ONLY-WS");
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
        TimeInForce::Gtc,
        true,
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let mut order_update = order_json_with(
        "ord-post-only-rej-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_002_000_i64,
        "rejected",
    );
    order_update["cancel_reason"] = json!("Post only order cannot cross the market");
    order_update["time_in_force"] = json!("post_only");
    let frame = json!([order_update]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    let event = drain_until(
        &mut tc.rx,
        |e| {
            matches!(
                e,
                ExecutionEvent::Order(OrderEventAny::Accepted(_) | OrderEventAny::Rejected(_),)
            )
        },
        "post-only Rejected",
    )
    .await;

    match event {
        ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) => {
            assert_eq!(rejected.client_order_id, client_order_id);
            assert_eq!(rejected.reason, "Post only order cannot cross the market");
            assert!(rejected.due_post_only);
        }
        ExecutionEvent::Order(OrderEventAny::Accepted(_)) => {
            panic!("Rejected path must not synthesize OrderAccepted");
        }
        _ => unreachable!(),
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_order_jsonrpc_rejection_forgets_identity() {
    // Regression for the submit_order forget-on-rejection paths: after a
    // JSON-RPC rejection, the registered identity must be cleared so a later
    // `.orders` frame for the same CID falls back to the untracked report
    // path (the engine has already terminated the order locally).
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.order_reply.lock().await = Some(json!({
        "error": {"code": -32602, "message": "Invalid params"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-FORGET-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected after JSON-RPC error",
    )
    .await;

    // Now push a `.orders` Open frame reusing the same label. With the
    // identity forgotten, this must take the untracked report path; before
    // the fix, it would have re-synthesized OrderAccepted for a CID the
    // local engine had already rejected.
    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let frame = json!([order_json_with(
        "ord-stale-after-reject",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_006_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &frame));

    let event = drain_until(
        &mut tc.rx,
        |e| {
            matches!(
                e,
                ExecutionEvent::Report(ExecutionReport::Order(_))
                    | ExecutionEvent::Order(OrderEventAny::Accepted(_))
            )
        },
        "post-reject .orders frame outcome",
    )
    .await;
    assert!(
        matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "identity must be forgotten after rejection; got {event:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_ws_dispatch_filled_then_replayed_open_is_suppressed() {
    // After a tracked Filled, a replayed `.orders` Open (typical reconnect
    // replay window) must not re-emit OrderAccepted. The `contains_filled`
    // guard short-circuits the Accepted path.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-FILLED-REPLAY");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;

    let channel = format!("{TEST_SUBACCOUNT}.orders");
    let filled_frame = json!([order_json_with(
        "ord-filled-replay",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_007_000_i64,
        "filled",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &filled_frame));
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "synthesized OrderAccepted from Filled",
    )
    .await;

    // Replay an Open frame for the same CID. Must not re-emit OrderAccepted.
    let open_frame = json!([order_json_with(
        "ord-filled-replay",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_008_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&channel, &open_frame));
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Accepted(_))) => {
                    return Some("duplicate Accepted after Filled");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "replayed Open after Filled must be suppressed, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_market_buy_full_lifecycle_emits_accepted_then_filled() {
    // TC-E01: market BUY end-to-end. Walks the dispatch path from submit
    // (OrderSubmitted + REST POST), through `.orders` Open (OrderAccepted),
    // `.trades` (OrderFilled with venue trade fields), and a trailing
    // `.orders` Filled which must be a no-op (Accepted is already marked
    // and tracked Filled emits only from the trade path).
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MKT-BUY-E01");

    // Market orders need top-of-book to compute the slippage bound; see
    // `test_submit_order_market_with_quote_uses_rounded_slippage_bound`.
    let quote = QuoteTick::new(
        instrument_id,
        Price::from("3500.00"),
        Price::from("3501.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    tc.cache
        .borrow_mut()
        .add_quote(quote)
        .expect("quote insert");

    let order = build_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-mkt-buy-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));

    let accepted = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on .orders Open",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.venue_order_id.as_str(), "ord-mkt-buy-1");
        assert_eq!(accepted.instrument_id, instrument_id);
    } else {
        unreachable!();
    }

    let trades_channel = format!("{TEST_SUBACCOUNT}.trades");
    let trade_frame = json!([trade_json_with_label(
        "trade-mkt-buy-1",
        "ord-mkt-buy-1",
        "ETH-PERP",
        client_order_id.as_str(),
    )]);
    ws_state.push_notification(make_subscription_frame(&trades_channel, &trade_frame));

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.venue_order_id.as_str(), "ord-mkt-buy-1");
        assert_eq!(filled.trade_id.as_str(), "trade-mkt-buy-1");
        assert_eq!(filled.order_side, OrderSide::Buy);
        assert_eq!(filled.last_qty.as_decimal(), dec!(1));
        assert_eq!(filled.last_px.as_decimal(), dec!(3505));
    } else {
        unreachable!();
    }

    let filled_frame = json!([order_json_with(
        "ord-mkt-buy-1",
        client_order_id.as_str(),
        "buy",
        "ETH-PERP",
        1_700_000_003_000_i64,
        "filled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &filled_frame));
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Accepted(_))) => {
                    return Some("duplicate Accepted after fill");
                }
                Some(ExecutionEvent::Order(OrderEventAny::Filled(_))) => {
                    return Some("duplicate Filled after fill");
                }
                Some(ExecutionEvent::Report(_)) => {
                    return Some("fallback report after tracked fill");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "trailing .orders Filled must be a no-op, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_jsonrpc_definitive_rejection_emits_order_cancel_rejected() {
    // TC-E40: a venue cancel for a definitive rejection (invalid params,
    // unknown order) must translate to OrderCancelRejected so the engine
    // clears the PendingCancel state. Uses `-32602` (invalid params), a
    // non-retryable JSON-RPC code per `is_retryable_jsonrpc_code`.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_reply.lock().await = Some(json!({
        "error": {"code": -32602, "message": "Order already canceled"}
    }));
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-CXL-E40"),
        Some(VenueOrderId::from("ord-already-canceled")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::CancelRejected(_))),
        "OrderCancelRejected on definitive JSON-RPC error",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) = event {
        assert_eq!(
            rejected.client_order_id,
            ClientOrderId::from("STRAT-CXL-E40")
        );
        assert_eq!(
            rejected.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-already-canceled".to_string()),
        );
        let reason = rejected.reason.as_str();
        assert!(
            reason.contains("-32602") && reason.contains("already canceled"),
            "unexpected cancel-reject reason: {reason}",
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_jsonrpc_ambiguous_does_not_emit_cancel_rejected() {
    // JSON-RPC `-32603` (generic internal error) leaves the cancel outcome
    // ambiguous because `send_private_once` does not replay and the venue
    // may have processed the cancel before the failure response. The
    // adapter must NOT emit OrderCancelRejected; WS reconciliation drives
    // the terminal state.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_reply.lock().await = Some(json!({
        "error": {"code": -32603, "message": "Internal venue error"}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-CXL-RETRY"),
        Some(VenueOrderId::from("ord-retry-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "cancel posted",
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::CancelRejected(_))) => {
                    return Some("unexpected OrderCancelRejected on retryable code");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "retryable JSON-RPC code must not emit OrderCancelRejected, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[case(-32602, "Invalid params")]
#[case(-32603, "Internal venue error")]
#[tokio::test]
async fn test_cancel_all_orders_bulk_failures_emit_no_order_events(
    #[case] code: i64,
    #[case] message: &str,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *ws_state.cancel_by_instrument_reply.lock().await = Some(json!({
        "error": {"code": code, "message": message}
    }));
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancel_by_instrument_calls.lock().await.is_empty() }
        },
        "cancel_by_instrument posted",
    )
    .await;

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(event)) => return Some(event),
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "bulk failure has no per-order outcome to emit, was {outcome:?}",
    );
    assert!(ws_state.cancel_all_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_buy_side_with_no_open_orders_is_noop() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        Some(OrderSide::Buy),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_all_orders(cmd).expect("cancel_all Ok");

    // Intentional quiet window: `wait_until_async` cannot prove absence of a future cancel.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let cancels = ws_state.cancelled_orders.lock().await;
    assert!(
        cancels.is_empty(),
        "no cancels should be sent when open_orders is empty, saw {}",
        cancels.len(),
    );
    assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    assert!(ws_state.cancel_by_instrument_calls.lock().await.is_empty(),);
    assert!(rest_state.open_orders_calls.lock().await.is_empty());
    assert!(rest_state.trigger_orders_calls.lock().await.is_empty());
    let cancel_all = ws_state.cancel_all_calls.lock().await;
    assert!(
        cancel_all.is_empty(),
        "private/cancel_all must not be invoked for side-filtered command, saw {}",
        cancel_all.len(),
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_query_order_unparsable_response_does_not_emit_report() {
    // query_order swallows deserialize failures so callers do not get a
    // partial / invalid OrderStatusReport. Use a response that the
    // DeriveOrder serde shape cannot parse.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_order_response.lock().await = json!({});
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = QueryOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        InstrumentId::from("ETH-PERP.DERIVE"),
        ClientOrderId::from("STRAT-Q-UNK"),
        Some(VenueOrderId::from("ord-unknown-1")),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.query_order(cmd).expect("query_order Ok");

    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Report(ExecutionReport::Order(_))) => {
                    return Some("unexpected OrderStatusReport");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "unparsable get_order response must not emit a report, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_open_no_filter_returns_all_instruments() {
    // TC-E84: with `open_only=true` and no instrument filter, the
    // reconciler must see every open order the venue returns, regardless of
    // instrument. Caller-side filtering is the only knob for narrowing.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.open_orders_response.lock().await = json!({
        "orders": [
            order_json_with("ord-eth-1", "L-ETH-1", "buy", "ETH-PERP", 100, "open"),
            order_json_with("ord-eth-2", "L-ETH-2", "sell", "ETH-PERP", 101, "open"),
            order_json_with("ord-btc-1", "L-BTC-1", "buy", "BTC-PERP", 102, "open"),
        ],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        true,
        None,
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_order_status_reports(&cmd)
        .await
        .expect("reports");
    assert_eq!(reports.len(), 3);

    let mut by_voi: std::collections::HashMap<&str, &OrderStatusReport> =
        std::collections::HashMap::new();

    for r in &reports {
        by_voi.insert(r.venue_order_id.as_str(), r);
    }

    let eth1 = by_voi.get("ord-eth-1").expect("ord-eth-1 present");
    assert_eq!(
        eth1.client_order_id.map(|c| c.as_str().to_string()),
        Some("L-ETH-1".to_string()),
    );
    assert_eq!(eth1.instrument_id.symbol.as_str(), "ETH-PERP");
    assert_eq!(eth1.order_side, Some(OrderSide::Buy));

    let eth2 = by_voi.get("ord-eth-2").expect("ord-eth-2 present");
    assert_eq!(eth2.order_side, Some(OrderSide::Sell));

    let btc1 = by_voi.get("ord-btc-1").expect("ord-btc-1 present");
    assert_eq!(btc1.instrument_id.symbol.as_str(), "BTC-PERP");

    // History endpoint must NOT be touched: open_only routes via get_open_orders.
    assert!(rest_state.order_history_calls.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_returns_long_short_and_flat() {
    // TC-E85: positions with mixed signs must round-trip with the correct
    // `position_side`. Flats are preserved (the reconciler decides how to
    // treat them; the adapter does not pre-filter).
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.positions_response.lock().await = json!({
        "positions": [
            sample_position_json("ETH-PERP", "3"),
            sample_position_json("BTC-PERP", "-1.5"),
            sample_position_json("SOL-PERP", "0"),
        ],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_position_status_reports(&cmd)
        .await
        .expect("positions");
    assert_eq!(reports.len(), 3);

    let by_symbol: std::collections::HashMap<&str, &PositionStatusReport> = reports
        .iter()
        .map(|r| (r.instrument_id.symbol.as_str(), r))
        .collect();

    let eth = by_symbol.get("ETH-PERP").expect("ETH-PERP present");
    assert_eq!(eth.position_side, PositionSide::Long);
    assert_eq!(eth.signed_decimal_qty, dec!(3));

    let btc = by_symbol.get("BTC-PERP").expect("BTC-PERP present");
    assert_eq!(btc.position_side, PositionSide::Short);
    assert_eq!(btc.signed_decimal_qty, dec!(-1.5));

    let sol = by_symbol.get("SOL-PERP").expect("SOL-PERP present");
    assert_eq!(sol.position_side, PositionSide::Flat);
    assert_eq!(sol.signed_decimal_qty, dec!(0));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_paginates_across_multiple_pages() {
    // TC-E86: the adapter walks `pagination.num_pages` and merges trades
    // across calls. Two pages with one trade each must produce two reports
    // and two GET calls.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![
        json!({
            "trades": [sample_trade_json("trade-page-1", "ord-A", "ETH-PERP")],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
        json!({
            "trades": [sample_trade_json("trade-page-2", "ord-B", "ETH-PERP")],
            "pagination": {"count": 2, "num_pages": 2},
            "subaccount_id": TEST_SUBACCOUNT,
        }),
    ];
    let mut tc = build_client(rest_state.clone(), ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-PERP.DERIVE")),
        None,
        None,
        None,
        None,
        None,
    );
    let reports = tc.client.generate_fill_reports(cmd).await.expect("fills");

    let mut trade_ids: Vec<&str> = reports.iter().map(|r| r.trade_id.as_str()).collect();
    trade_ids.sort_unstable();
    assert_eq!(trade_ids, vec!["trade-page-1", "trade-page-2"]);

    let calls = rest_state.trade_history_calls.lock().await;
    assert_eq!(calls.len(), 2, "must request both pages");
    assert_eq!(calls[0]["page"].as_u64(), Some(1));
    assert_eq!(calls[0]["page_size"].as_u64(), Some(500));
    assert_eq!(calls[1]["page"].as_u64(), Some(2));
    assert_eq!(calls[1]["page_size"].as_u64(), Some(500));

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_option_call_buy_limit_full_lifecycle() {
    // Group 10 / Option call buy: exercises the adapter against an
    // `instrument_type=option` instrument shape. The dispatch must accept
    // option instrument_id strings (`ETH-20260626-3500-C.DERIVE`) and walk
    // the same lifecycle as perps.
    let (mut tc, ws_state) =
        build_connected_option_client("ETH-20260626-3500-C", "C", "3500").await;
    drain_initial_account_state(&mut tc).await;

    let instrument_id = InstrumentId::from("ETH-20260626-3500-C.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OPT-CALL-BUY");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.00"),
    );
    submit_cached_order(&tc, &order);

    drain_order_submitted(&mut tc).await;
    wait_for_private_order_post(&ws_state).await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(
            posts[0]["instrument_name"].as_str(),
            Some("ETH-20260626-3500-C"),
        );
        assert_eq!(posts[0]["direction"].as_str(), Some("buy"));
        assert_eq!(posts[0]["order_type"].as_str(), Some("limit"));
    }

    let open_frame = json!([order_json_with(
        "ord-opt-call-1",
        client_order_id.as_str(),
        "buy",
        "ETH-20260626-3500-C",
        1_700_000_001_000_i64,
        "open",
    )]);
    push_orders_update(&ws_state, &open_frame);

    let accepted = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on option Open",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.instrument_id, instrument_id);
    } else {
        unreachable!();
    }

    let trade_frame = json!([trade_json_with_label(
        "trade-opt-call-1",
        "ord-opt-call-1",
        "ETH-20260626-3500-C",
        client_order_id.as_str(),
    )]);
    push_trades_update(&ws_state, &trade_frame);

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on option .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.trade_id.as_str(), "trade-opt-call-1");
        assert_eq!(filled.order_side, OrderSide::Buy);
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_option_put_sell_limit_full_lifecycle() {
    // Group 10 / Option put sell: mirror of the call-buy test on a put
    // instrument (`-P` suffix, `option_type=P`).
    let (mut tc, ws_state) =
        build_connected_option_client("ETH-20260626-3500-P", "P", "3500").await;
    drain_initial_account_state(&mut tc).await;

    let instrument_id = InstrumentId::from("ETH-20260626-3500-P.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OPT-PUT-SELL");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("80"),
        Quantity::from("0.50"),
    );
    submit_cached_order(&tc, &order);

    drain_order_submitted(&mut tc).await;
    wait_for_private_order_post(&ws_state).await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(
            posts[0]["instrument_name"].as_str(),
            Some("ETH-20260626-3500-P"),
        );
        assert_eq!(posts[0]["direction"].as_str(), Some("sell"));
    }

    let open_frame = json!([order_json_with(
        "ord-opt-put-1",
        client_order_id.as_str(),
        "sell",
        "ETH-20260626-3500-P",
        1_700_000_001_000_i64,
        "open",
    )]);
    push_orders_update(&ws_state, &open_frame);

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on option put Open",
    )
    .await;

    let trade_frame = json!([{
        "direction": "sell",
        "index_price": "3500",
        "instrument_name": "ETH-20260626-3500-P",
        "is_transfer": false,
        "label": client_order_id.as_str(),
        "liquidity_role": "taker",
        "mark_price": "80",
        "order_id": "ord-opt-put-1",
        "quote_id": null,
        "realized_pnl": "0",
        "subaccount_id": TEST_SUBACCOUNT,
        "timestamp": 1_700_000_002_000_i64,
        "trade_amount": "0.5",
        "trade_fee": "0.1",
        "trade_id": "trade-opt-put-1",
        "trade_price": "80",
        "tx_hash": "0xabc",
        "tx_status": "settled",
        "wallet": "0xwallet",
    }]);
    push_trades_update(&ws_state, &trade_frame);

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on option put .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.trade_id.as_str(), "trade-opt-put-1");
        assert_eq!(filled.order_side, OrderSide::Sell);
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_option_order_resolves_option_instrument_for_signing() {
    // Group 10 / signing: the adapter resolves the option-specific
    // instrument record (option_details, base_asset_sub_id, tick_size=1)
    // when submitting an option order. We assert the get_instrument
    // request carries the option name (so the right asset is used for the
    // EIP-712 trade-module signing payload) and that the POST body uses
    // the same name.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_instrument_response.lock().await =
        option_instrument_json("ETH-20260626-3500-C", "C", "3500");
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");
    wait_for_private_subscription(&ws_state).await;

    let instrument_id = InstrumentId::from("ETH-20260626-3500-C.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OPT-SIGN");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.00"),
    );
    submit_cached_order(&tc, &order);

    wait_for_private_order_post(&ws_state).await;

    let instrument_calls = rest_state.get_instrument_calls.lock().await;
    assert_eq!(instrument_calls.len(), 1);
    assert_eq!(
        instrument_calls[0]["instrument_name"].as_str(),
        Some("ETH-20260626-3500-C"),
    );
    drop(instrument_calls);

    let posts = ws_state.submitted_orders.lock().await;
    let body = &posts[0];
    assert_eq!(
        body["instrument_name"].as_str(),
        Some("ETH-20260626-3500-C"),
    );
    assert_eq!(body["direction"].as_str(), Some("buy"));
    assert_eq!(body["order_type"].as_str(), Some("limit"));
    assert_eq!(body["label"].as_str(), Some("STRAT-OPT-SIGN"));
    // The signed payload carries a non-empty signature and a nonce; the
    // venue-side verification would fail if asset_address / sub_id from
    // the option record were not used.
    assert!(body["signature"].as_str().unwrap().starts_with("0x"));
    assert!(body["nonce"].as_u64().unwrap() > 0);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_option_fok_limit_posts_fok_and_emits_filled() {
    // TC-E99: option FOK limit orders use the ordinary option signing path
    // but must preserve the FOK instruction sent to the venue.
    let (mut tc, ws_state) =
        build_connected_option_client("ETH-20260626-3500-C", "C", "3500").await;
    drain_initial_account_state(&mut tc).await;

    let instrument_id = InstrumentId::from("ETH-20260626-3500-C.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OPT-FOK");
    let order = build_limit_order_with_time_in_force(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.00"),
        TimeInForce::Fok,
        false,
    );
    submit_cached_order(&tc, &order);

    drain_order_submitted(&mut tc).await;
    wait_for_private_order_post(&ws_state).await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(
            posts[0]["instrument_name"].as_str(),
            Some("ETH-20260626-3500-C"),
        );
        assert_eq!(posts[0]["time_in_force"].as_str(), Some("fok"));
        assert_eq!(posts[0]["order_type"].as_str(), Some("limit"));
    }

    let open_frame = json!([order_json_with(
        "ord-opt-fok-1",
        client_order_id.as_str(),
        "buy",
        "ETH-20260626-3500-C",
        1_700_000_001_000_i64,
        "open",
    )]);
    push_orders_update(&ws_state, &open_frame);

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on option FOK Open",
    )
    .await;

    let trade_frame = json!([trade_json_with_label(
        "trade-opt-fok-1",
        "ord-opt-fok-1",
        "ETH-20260626-3500-C",
        client_order_id.as_str(),
    )]);
    push_trades_update(&ws_state, &trade_frame);

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on option FOK .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.venue_order_id.as_str(), "ord-opt-fok-1");
        assert_eq!(filled.trade_id.as_str(), "trade-opt-fok-1");
        assert_eq!(filled.order_side, OrderSide::Buy);
        assert_eq!(filled.last_qty.as_decimal(), dec!(1));
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_cancel_option_order_posts_private_cancel_and_emits_canceled() {
    // TC-E100: option cancels must keep the option symbol on private/cancel
    // and route the terminal `.orders` update back to the tracked order.
    let (mut tc, ws_state) =
        build_connected_option_client("ETH-20260626-3500-C", "C", "3500").await;
    drain_initial_account_state(&mut tc).await;

    let instrument_id = InstrumentId::from("ETH-20260626-3500-C.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-OPT-CANCEL");
    let venue_order_id = VenueOrderId::from("ord-opt-cancel-1");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.00"),
    );
    submit_cached_order(&tc, &order);

    drain_order_submitted(&mut tc).await;
    wait_for_private_order_post(&ws_state).await;

    let open_frame = json!([order_json_with(
        venue_order_id.as_str(),
        client_order_id.as_str(),
        "buy",
        "ETH-20260626-3500-C",
        1_700_000_001_000_i64,
        "open",
    )]);
    push_orders_update(&ws_state, &open_frame);

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on option Open",
    )
    .await;

    let cancel = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("DERIVE")),
        StrategyId::from("S-1"),
        instrument_id,
        client_order_id,
        Some(venue_order_id),
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    tc.client.cancel_order(cancel).expect("cancel_order Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.cancelled_orders.lock().await.is_empty() }
        },
        "private/cancel posted",
    )
    .await;
    {
        let posts = ws_state.cancelled_orders.lock().await;
        assert_eq!(
            posts[0]["instrument_name"].as_str(),
            Some("ETH-20260626-3500-C"),
        );
        assert_eq!(posts[0]["order_id"].as_str(), Some("ord-opt-cancel-1"));
        assert!(ws_state.cancelled_trigger_orders.lock().await.is_empty());
    }

    let cancel_frame = json!([order_json_with(
        "ord-opt-cancel-1",
        client_order_id.as_str(),
        "buy",
        "ETH-20260626-3500-C",
        1_700_000_002_000_i64,
        "cancelled",
    )]);
    push_orders_update(&ws_state, &cancel_frame);

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Canceled(_))),
        "OrderCanceled on option cancel",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Canceled(canceled)) = event {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert_eq!(
            canceled.venue_order_id.map(|v| v.as_str().to_string()),
            Some("ord-opt-cancel-1".to_string()),
        );
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_option_position() {
    // TC-E101: option positions use the same reconciliation report path, but
    // the instrument id must preserve the full option symbol.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut option_position = sample_position_json("ETH-20260626-3500-C", "-2");
    option_position["instrument_type"] = json!("option");
    option_position["average_price"] = json!("80");
    *rest_state.positions_response.lock().await = json!({
        "positions": [option_position],
        "subaccount_id": TEST_SUBACCOUNT,
    });
    let mut tc = build_client(rest_state, ws_state).await;
    tc.client.connect().await.expect("connect succeeds");

    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(InstrumentId::from("ETH-20260626-3500-C.DERIVE")),
        None,
        None,
        None,
        None,
    );
    let reports = tc
        .client
        .generate_position_status_reports(&cmd)
        .await
        .expect("positions");

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0].instrument_id.symbol.as_str(),
        "ETH-20260626-3500-C"
    );
    assert_eq!(reports[0].position_side, PositionSide::Short);
    assert_eq!(reports[0].signed_decimal_qty, dec!(-2));
    assert_eq!(reports[0].avg_px_open, Some(dec!(80)));

    tc.client.disconnect().await.expect("disconnect");
}

async fn build_connected_option_client(
    instrument_name: &str,
    option_type: &str,
    strike: &str,
) -> (TestClient, WsState) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_instrument_response.lock().await =
        option_instrument_json(instrument_name, option_type, strike);

    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");
    wait_for_private_subscription(&ws_state).await;

    (tc, ws_state)
}

async fn wait_for_private_subscription(ws_state: &WsState) {
    wait_until(
        || {
            let state = (*ws_state).clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
}

async fn drain_initial_account_state(tc: &mut TestClient) {
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;
}

fn submit_cached_order(tc: &TestClient, order: &OrderAny) {
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(order))
        .expect("submit Ok");
}

async fn drain_order_submitted(tc: &mut TestClient) {
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
}

async fn wait_for_private_order_post(ws_state: &WsState) {
    wait_until(
        || {
            let state = (*ws_state).clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;
}

fn push_orders_update(ws_state: &WsState, data: &Value) {
    let channel = format!("{TEST_SUBACCOUNT}.orders");
    ws_state.push_notification(make_subscription_frame(&channel, data));
}

fn push_trades_update(ws_state: &WsState, data: &Value) {
    let channel = format!("{TEST_SUBACCOUNT}.trades");
    ws_state.push_notification(make_subscription_frame(&channel, data));
}

#[rstest]
#[tokio::test]
async fn test_second_submit_resolves_instrument_from_cache_without_refetch() {
    // The instrument cache is keyed by InstrumentId over an AtomicMap: once an
    // order resolves an instrument via public/get_instrument, a later order for
    // the same instrument must be served from the cache rather than re-fetched.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state.clone(), ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    let first = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-CACHE-1"),
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.0"),
    );
    tc.cache
        .borrow_mut()
        .add_order(first.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&first))
        .expect("submit Ok");

    // Insert precedes the signed POST, so once the first order is on the wire
    // the cache is populated and the second submit cannot race the fetch.
    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() == 1 }
        },
        "first private/order posted",
    )
    .await;

    let second = build_limit_order(
        instrument_id,
        ClientOrderId::from("STRAT-CACHE-2"),
        OrderSide::Buy,
        Price::from("100"),
        Quantity::from("1.0"),
    );
    tc.cache
        .borrow_mut()
        .add_order(second.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&second))
        .expect("submit Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { state.submitted_orders.lock().await.len() == 2 }
        },
        "second private/order posted",
    )
    .await;

    assert_eq!(rest_state.get_instrument_calls.lock().await.len(), 1);

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_market_sell_full_lifecycle_emits_accepted_then_filled() {
    // TC-E02: market SELL mirror of TC-E01. Verifies the dispatch path is
    // side-agnostic and that `OrderFilled.order_side` is taken from the
    // tracked identity (Sell) rather than the trade frame.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-MKT-SELL-E02");

    let quote = QuoteTick::new(
        instrument_id,
        Price::from("3500.00"),
        Price::from("3501.00"),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        UnixNanos::default(),
        UnixNanos::default(),
    );
    tc.cache
        .borrow_mut()
        .add_quote(quote)
        .expect("quote insert");

    let order = build_market_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");

    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(posts[0]["direction"].as_str(), Some("sell"));
        assert_eq!(posts[0]["order_type"].as_str(), Some("market"));
    }

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-mkt-sell-1",
        client_order_id.as_str(),
        "sell",
        "ETH-PERP",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));

    let accepted = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on .orders Open",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.venue_order_id.as_str(), "ord-mkt-sell-1");
        assert_eq!(accepted.instrument_id, instrument_id);
    } else {
        unreachable!();
    }

    // Inline rather than `trade_json_with_label` so the frame's direction
    // matches the order side. Identity drives OrderFilled.order_side, but
    // keeping the frame realistic avoids confusion in regression diffs.
    let trades_channel = format!("{TEST_SUBACCOUNT}.trades");
    let trade_frame = json!([{
        "direction": "sell",
        "index_price": "3500",
        "instrument_name": "ETH-PERP",
        "is_transfer": false,
        "label": client_order_id.as_str(),
        "liquidity_role": "taker",
        "mark_price": "3500",
        "order_id": "ord-mkt-sell-1",
        "quote_id": null,
        "realized_pnl": "0",
        "subaccount_id": TEST_SUBACCOUNT,
        "timestamp": 1_700_000_002_000_i64,
        "trade_amount": "1",
        "trade_fee": "0.5",
        "trade_id": "trade-mkt-sell-1",
        "trade_price": "3495",
        "tx_hash": "0xabc",
        "tx_status": "settled",
        "wallet": "0xwallet",
    }]);
    ws_state.push_notification(make_subscription_frame(&trades_channel, &trade_frame));

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.venue_order_id.as_str(), "ord-mkt-sell-1");
        assert_eq!(filled.trade_id.as_str(), "trade-mkt-sell-1");
        assert_eq!(filled.order_side, OrderSide::Sell);
        assert_eq!(filled.last_qty.as_decimal(), dec!(1));
        assert_eq!(filled.last_px.as_decimal(), dec!(3495));
    } else {
        unreachable!();
    }

    let filled_frame = json!([order_json_with(
        "ord-mkt-sell-1",
        client_order_id.as_str(),
        "sell",
        "ETH-PERP",
        1_700_000_003_000_i64,
        "filled",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &filled_frame));
    let outcome = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Accepted(_))) => {
                    return Some("duplicate Accepted after fill");
                }
                Some(ExecutionEvent::Order(OrderEventAny::Filled(_))) => {
                    return Some("duplicate Filled after fill");
                }
                Some(ExecutionEvent::Report(_)) => {
                    return Some("fallback report after tracked fill");
                }
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        outcome.is_err(),
        "trailing .orders Filled must be a no-op, was {outcome:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_spot_buy_limit_full_lifecycle() {
    // Spot (ERC-20) buy: exercises the adapter against an
    // `instrument_type=erc20` instrument (`ETH-USDC`). Spot reuses the Trade
    // module signing path, so submit/open/fill must walk the same lifecycle
    // as perps and options with no execution-side branch.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_instrument_response.lock().await = spot_instrument_json("ETH-USDC");
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.subscribe_frames.lock().await.is_empty() }
        },
        "subscribe acknowledged",
    )
    .await;
    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-USDC.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-SPOT-BUY");
    let order = build_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Buy,
        Price::from("2000.0"),
        Quantity::from("0.10"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Submitted(_))),
        "OrderSubmitted",
    )
    .await;
    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted",
    )
    .await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(posts[0]["instrument_name"].as_str(), Some("ETH-USDC"));
        assert_eq!(posts[0]["direction"].as_str(), Some("buy"));
        assert_eq!(posts[0]["order_type"].as_str(), Some("limit"));
        // reduce_only must be absent: this is a plain spot open.
        assert!(posts[0].get("reduce_only").is_none());
    }

    let orders_channel = format!("{TEST_SUBACCOUNT}.orders");
    let open_frame = json!([order_json_with(
        "ord-spot-1",
        client_order_id.as_str(),
        "buy",
        "ETH-USDC",
        1_700_000_001_000_i64,
        "open",
    )]);
    ws_state.push_notification(make_subscription_frame(&orders_channel, &open_frame));

    let accepted = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Accepted(_))),
        "OrderAccepted on spot Open",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Accepted(accepted)) = accepted {
        assert_eq!(accepted.client_order_id, client_order_id);
        assert_eq!(accepted.instrument_id, instrument_id);
        assert_eq!(accepted.venue_order_id.as_str(), "ord-spot-1");
    } else {
        unreachable!();
    }

    let trades_channel = format!("{TEST_SUBACCOUNT}.trades");
    let trade_frame = json!([{
        "direction": "buy",
        "index_price": "2000",
        "instrument_name": "ETH-USDC",
        "is_transfer": false,
        "label": client_order_id.as_str(),
        "liquidity_role": "taker",
        "mark_price": "2000",
        "order_id": "ord-spot-1",
        "quote_id": null,
        "realized_pnl": "0",
        "subaccount_id": TEST_SUBACCOUNT,
        "timestamp": 1_700_000_002_000_i64,
        "trade_amount": "0.1",
        "trade_fee": "0",
        "trade_id": "trade-spot-1",
        "trade_price": "2000",
        "tx_hash": "0xabc",
        "tx_status": "settled",
        "wallet": "0xwallet",
    }]);
    ws_state.push_notification(make_subscription_frame(&trades_channel, &trade_frame));

    let filled = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Filled(_))),
        "OrderFilled on spot .trades",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Filled(filled)) = filled {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.trade_id.as_str(), "trade-spot-1");
        assert_eq!(filled.order_side, OrderSide::Buy);
        assert_eq!(filled.last_qty.as_decimal(), dec!(0.1));
        assert_eq!(filled.last_px.as_decimal(), dec!(2000));
    } else {
        unreachable!();
    }

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_spot_reduce_only_is_denied_locally() {
    // Derive spot has no position concept, so reduce-only can never reduce
    // anything; the venue rejects it unconditionally (11025). The adapter
    // short-circuits that deterministic outcome with a local OrderDenied and
    // never posts to the venue. Perp/option reduce-only is untouched.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    // The guard reads the engine cache to classify the instrument as spot
    // (CurrencyPair), so register the parsed ETH-USDC record first.
    let derive_instrument: DeriveInstrument =
        serde_json::from_value(spot_instrument_json("ETH-USDC")).expect("spot instrument parses");
    let instrument = parse_derive_instrument_any(&derive_instrument, UnixNanos::default())
        .expect("parse succeeds")
        .expect("spot instrument produced");
    tc.cache
        .borrow_mut()
        .add_instrument(instrument)
        .expect("instrument insert");

    let instrument_id = InstrumentId::from("ETH-USDC.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-SPOT-RO");
    let order = build_reduce_only_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("2000.0"),
        Quantity::from("0.10"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Denied(_))),
        "OrderDenied for spot reduce-only",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
        assert_eq!(denied.client_order_id, client_order_id);
        assert!(
            denied.reason.contains("reduce-only"),
            "unexpected deny reason: {}",
            denied.reason,
        );
    } else {
        unreachable!();
    }

    // The order must never reach the venue.
    assert!(ws_state.submitted_orders.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_perp_reduce_only_reaches_venue() {
    // The other half of the spot guard's invariant: reduce-only on a
    // derivative (perp) must NOT be blocked locally. The venue's perp
    // reduce-only rejection is conditional on position state, so the order
    // must reach `/private/order` with `reduce_only: true` and emit no local
    // Denied/Rejected. Guards against the guard over-matching all instruments.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Default get_instrument returns the ETH-PERP perp record.
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-PERP-RO");
    let order = build_reduce_only_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("3500.00"),
        Quantity::from("1.000"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    wait_until(
        || {
            let state = ws_state.clone();
            async move { !state.submitted_orders.lock().await.is_empty() }
        },
        "private/order posted for reduce-only perp",
    )
    .await;
    {
        let posts = ws_state.submitted_orders.lock().await;
        assert_eq!(posts[0]["instrument_name"].as_str(), Some("ETH-PERP"));
        assert_eq!(posts[0]["reduce_only"].as_bool(), Some(true));
    }

    // No local Denied/Rejected should have been emitted for the perp.
    let blocked = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            match tc.rx.recv().await {
                Some(ExecutionEvent::Order(OrderEventAny::Denied(_))) => return Some("denied"),
                Some(ExecutionEvent::Order(OrderEventAny::Rejected(_))) => return Some("rejected"),
                Some(_) => {}
                None => return None,
            }
        }
    })
    .await;
    assert!(
        blocked.is_err(),
        "reduce-only perp must not be blocked locally, was {blocked:?}",
    );

    tc.client.disconnect().await.expect("disconnect");
}

#[rstest]
#[tokio::test]
async fn test_submit_spot_reduce_only_lazy_resolution_is_rejected() {
    // Same invariant as the deny test, but via the lazy instrument path: the
    // core cache is empty at submit time, so the synchronous deny is skipped
    // and the order resolves through `public/get_instrument`. The in-task net
    // must still keep the reduce-only spot order off the venue. OrderSubmitted
    // has already fired, so this surfaces as OrderRejected rather than Denied.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.get_instrument_response.lock().await = spot_instrument_json("ETH-USDC");
    let mut tc = build_client(rest_state, ws_state.clone()).await;
    tc.client.connect().await.expect("connect");

    let _ = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Account(_)),
        "initial AccountState",
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-USDC.DERIVE");
    let client_order_id = ClientOrderId::from("STRAT-SPOT-RO-LAZY");
    let order = build_reduce_only_limit_order(
        instrument_id,
        client_order_id,
        OrderSide::Sell,
        Price::from("2000.0"),
        Quantity::from("0.10"),
    );
    tc.cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .expect("cache insert");
    tc.client
        .submit_order(submit_cmd(&order))
        .expect("submit Ok");

    let event = drain_until(
        &mut tc.rx,
        |e| matches!(e, ExecutionEvent::Order(OrderEventAny::Rejected(_))),
        "OrderRejected for lazily-resolved spot reduce-only",
    )
    .await;

    if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert!(
            rejected.reason.contains("reduce-only"),
            "unexpected reject reason: {}",
            rejected.reason,
        );
    } else {
        unreachable!();
    }

    // The order must never reach the venue.
    assert!(ws_state.submitted_orders.lock().await.is_empty());

    tc.client.disconnect().await.expect("disconnect");
}

async fn query_order_report(
    tc: &mut TestClient,
    cmd: &GenerateOrderStatusReport,
) -> OrderStatusReport {
    tc.client
        .query_order(QueryOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("DERIVE")),
            StrategyId::from("S-1"),
            cmd.instrument_id.unwrap(),
            cmd.client_order_id.unwrap(),
            None,
            cmd.command_id,
            cmd.ts_init,
            None,
            None,
        ))
        .expect("query_order succeeds");

    let event = drain_until(
        &mut tc.rx,
        |event| matches!(event, ExecutionEvent::Report(ExecutionReport::Order(_))),
        "OrderStatusReport event",
    )
    .await;

    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        unreachable!();
    };

    *report
}
