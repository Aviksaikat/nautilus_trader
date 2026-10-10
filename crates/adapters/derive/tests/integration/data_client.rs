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

//! Integration tests for `DeriveDataClient` against local REST and WS mocks.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
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
use futures_util::{SinkExt, StreamExt};
use jiff::Timestamp;
use nautilus_common::{
    clients::DataClient,
    live::runner::{replace_data_event_sender, replace_system_event_sender},
    messages::{
        DataEvent, SystemEvent,
        data::{
            DataResponse, RequestBars, RequestFundingRates, RequestInstrument, RequestInstruments,
            RequestOptionChainReferencePrice, RequestQuotes, RequestTrades, SubscribeBookDeltas,
            SubscribeBookDepth, SubscribeQuotes, SubscribeTrades, UnsubscribeBookDeltas,
            UnsubscribeBookDepth, UnsubscribeQuotes, UnsubscribeTrades,
        },
        system::SocketState,
    },
    testing::wait_until_async,
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_derive::{
    common::{
        consts::{DERIVE_CLIENT_ID, DERIVE_VENUE},
        enums::DeriveEnvironment,
    },
    config::DeriveDataClientConfig,
    data::DeriveDataClient,
};
use nautilus_live::{SocketReconnectRegistry, SocketReconnectRequestOutcome};
use nautilus_model::{
    data::{BarType, Data},
    enums::{AggressorSide, BookType},
    identifiers::{InstrumentId, OptionSeriesId, TradeId},
    instruments::Instrument,
    types::{Price, Quantity},
};
use nautilus_network::{http::HttpClient, websocket::TransportBackend};
use rstest::rstest;
use serde_json::{Value, json};
use ustr::Ustr;

#[derive(Clone, Default)]
struct RestRequest {
    body: Value,
}

#[derive(Clone, Default)]
struct RestState {
    requests: Arc<tokio::sync::Mutex<Vec<RestRequest>>>,
    trade_history_pages: Arc<tokio::sync::Mutex<Vec<Value>>>,
    trade_history_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    funding_rate_history_response: Arc<tokio::sync::Mutex<Value>>,
    funding_rate_history_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    candles_response: Arc<tokio::sync::Mutex<Value>>,
    candles_pages: Arc<tokio::sync::Mutex<HashMap<i64, Value>>>,
    candles_generated_per_call: Arc<tokio::sync::Mutex<Option<usize>>>,
    candles_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    ticker_response: Arc<tokio::sync::Mutex<Value>>,
    ticker_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
    ticker_response_delay_ms: Arc<AtomicUsize>,
    instruments_response: Arc<tokio::sync::Mutex<Option<Value>>>,
    instrument_response: Arc<tokio::sync::Mutex<Value>>,
    instrument_calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
}

impl RestState {
    async fn requests(&self) -> Vec<RestRequest> {
        self.requests.lock().await.clone()
    }

    async fn trade_history_calls(&self) -> Vec<Value> {
        self.trade_history_calls.lock().await.clone()
    }

    async fn funding_rate_history_calls(&self) -> Vec<Value> {
        self.funding_rate_history_calls.lock().await.clone()
    }

    async fn candles_calls(&self) -> Vec<Value> {
        self.candles_calls.lock().await.clone()
    }

    async fn ticker_calls(&self) -> Vec<Value> {
        self.ticker_calls.lock().await.clone()
    }

    async fn instrument_calls(&self) -> Vec<Value> {
        self.instrument_calls.lock().await.clone()
    }
}

#[derive(Clone)]
struct WsState {
    connection_count: Arc<AtomicUsize>,
    upgrade_attempts: Arc<AtomicUsize>,
    upgrade_failures: Arc<AtomicUsize>,
    subscribe_frames: Arc<tokio::sync::Mutex<Vec<Value>>>,
    unsubscribe_frames: Arc<tokio::sync::Mutex<Vec<Value>>>,
    subscribe_failures: Arc<AtomicUsize>,
    unsubscribe_failures: Arc<AtomicUsize>,
    deferred_subscribe_responses: Arc<AtomicUsize>,
    subscribe_response_gate: Arc<tokio::sync::Semaphore>,
    subscribe_response_count: Arc<AtomicUsize>,
    unsubscribe_before_subscribe_response: Arc<AtomicBool>,
    subscription_notifications: Arc<tokio::sync::Mutex<HashMap<String, Vec<Value>>>>,
    notification_delay_ms: Arc<AtomicUsize>,
    send_all_notifications: Arc<AtomicBool>,
    notification_before_subscribe_response: Arc<AtomicBool>,
    close_signal: Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<()>>>>,
}

impl Default for WsState {
    fn default() -> Self {
        Self {
            connection_count: Arc::new(AtomicUsize::new(0)),
            upgrade_attempts: Arc::new(AtomicUsize::new(0)),
            upgrade_failures: Arc::new(AtomicUsize::new(0)),
            subscribe_frames: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            unsubscribe_frames: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            subscribe_failures: Arc::new(AtomicUsize::new(0)),
            unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
            deferred_subscribe_responses: Arc::new(AtomicUsize::new(0)),
            subscribe_response_gate: Arc::new(tokio::sync::Semaphore::new(0)),
            subscribe_response_count: Arc::new(AtomicUsize::new(0)),
            unsubscribe_before_subscribe_response: Arc::new(AtomicBool::new(false)),
            subscription_notifications: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            notification_delay_ms: Arc::new(AtomicUsize::new(0)),
            send_all_notifications: Arc::new(AtomicBool::new(false)),
            notification_before_subscribe_response: Arc::new(AtomicBool::new(false)),
            close_signal: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

impl WsState {
    async fn subscribes(&self) -> Vec<Value> {
        self.subscribe_frames.lock().await.clone()
    }

    async fn unsubscribes(&self) -> Vec<Value> {
        self.unsubscribe_frames.lock().await.clone()
    }

    /// Forces the current server-side WS connection closed, simulating an
    /// ordinary transport drop (as opposed to a clean client disconnect).
    /// The underlying generic transport then auto-reconnects against the
    /// same mock server.
    async fn force_disconnect(&self) {
        if let Some(tx) = self.close_signal.lock().await.take() {
            let _ = tx.send(());
        }
    }
}

async fn handle_get_instruments(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed_body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state
        .requests
        .lock()
        .await
        .push(RestRequest { body: parsed_body });

    let response = state
        .instruments_response
        .lock()
        .await
        .clone()
        .unwrap_or_else(|| load_json("common/http_get_instruments_eth_all.json"));
    (StatusCode::OK, Json(response)).into_response()
}

async fn handle_get_trade_history(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let page = parsed.get("page").and_then(Value::as_u64).unwrap_or(1) as usize;
    state.trade_history_calls.lock().await.push(parsed);

    let pages = state.trade_history_pages.lock().await;
    let result = pages
        .get(page.saturating_sub(1))
        .cloned()
        .unwrap_or_else(|| json!({"trades": [], "pagination": {"count": 0, "num_pages": 0}}));

    (StatusCode::OK, Json(json!({"id": 1, "result": result}))).into_response()
}

async fn handle_get_funding_rate_history(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.funding_rate_history_calls.lock().await.push(parsed);

    let result = state.funding_rate_history_response.lock().await.clone();
    (StatusCode::OK, Json(json!({"id": 1, "result": result}))).into_response()
}

async fn handle_get_tradingview_chart_data(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let end_ts = parsed.get("end_timestamp").and_then(Value::as_i64);
    let period = parsed.get("period").and_then(Value::as_i64);
    state.candles_calls.lock().await.push(parsed);

    if let Some(per_call) = *state.candles_generated_per_call.lock().await
        && let (Some(end_ts), Some(period)) = (end_ts, period)
    {
        let result = synth_candles_page(end_ts, period, per_call);
        return (StatusCode::OK, Json(json!({"id": 1, "result": result}))).into_response();
    }

    {
        let pages = state.candles_pages.lock().await;
        if !pages.is_empty() {
            let result = end_ts
                .and_then(|ts| pages.get(&ts).cloned())
                .unwrap_or_else(|| json!([]));
            return (StatusCode::OK, Json(json!({"id": 1, "result": result}))).into_response();
        }
    }

    let response_value = state.candles_response.lock().await.clone();
    // A configured `error` envelope is returned verbatim so tests can drive
    // the REST-error path; any other value is wrapped as a `result` envelope.
    let body = if response_value.get("error").is_some() {
        response_value
    } else {
        json!({"id": 1, "result": response_value})
    };
    (StatusCode::OK, Json(body)).into_response()
}

fn synth_candles_page(end_ts: i64, period: i64, per_call: usize) -> Value {
    let mut bars = Vec::with_capacity(per_call);
    for i in 0..per_call {
        let bucket = end_ts - period * i as i64;

        bars.push(json!({
            "open_price": "100.0",
            "high_price": "101.0",
            "low_price": "99.0",
            "close_price": "100.5",
            "volume_usd": "0",
            "volume_contracts": "1.0",
            "timestamp": bucket,
            "timestamp_bucket": bucket,
        }));
    }
    Value::Array(bars)
}

fn candle_json(bucket: i64) -> Value {
    json!({
        "open_price": "100.0",
        "high_price": "101.0",
        "low_price": "99.0",
        "close_price": "100.5",
        "volume_usd": "0",
        "volume_contracts": "1.0",
        "timestamp": bucket,
        "timestamp_bucket": bucket,
    })
}

fn datetime_from_secs(secs: i64) -> Timestamp {
    Timestamp::from_second(secs).unwrap()
}

async fn handle_get_tickers(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.ticker_calls.lock().await.push(parsed);

    let response_value = state.ticker_response.lock().await.clone();
    // A configured `error` envelope is returned verbatim so tests can drive
    // the REST-error path; any other value is wrapped as a `result` envelope.
    let body = if response_value.get("error").is_some() {
        response_value
    } else if response_value.get("tickers").is_some() {
        json!({"id": 1, "result": response_value})
    } else {
        let instrument_name = response_value
            .get("instrument_name")
            .and_then(Value::as_str)
            .unwrap_or("ETH-PERP");
        json!({"id": 1, "result": {"tickers": {instrument_name: response_value}}})
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn handle_get_ticker(State(state): State<RestState>, body: axum::body::Bytes) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.ticker_calls.lock().await.push(parsed);

    let delay_ms = state.ticker_response_delay_ms.load(Ordering::SeqCst);
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
    }

    let response_value = state.ticker_response.lock().await.clone();
    (
        StatusCode::OK,
        Json(json!({"id": 1, "result": response_value})),
    )
        .into_response()
}

async fn handle_get_instrument(
    State(state): State<RestState>,
    body: axum::body::Bytes,
) -> Response {
    let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    state.instrument_calls.lock().await.push(parsed);

    let response_value = state.instrument_response.lock().await.clone();
    let body = if response_value.get("error").is_some() {
        response_value
    } else {
        json!({"id": 1, "result": response_value})
    };
    (StatusCode::OK, Json(body)).into_response()
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

async fn start_rest_server(state: RestState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/public/get_instruments", post(handle_get_instruments))
        .route("/public/get_instrument", post(handle_get_instrument))
        .route("/public/get_trade_history", post(handle_get_trade_history))
        .route(
            "/public/get_funding_rate_history",
            post(handle_get_funding_rate_history),
        )
        .route(
            "/public/get_tradingview_chart_data",
            post(handle_get_tradingview_chart_data),
        )
        .route("/public/get_tickers", post(handle_get_tickers))
        .route("/public/get_ticker", post(handle_get_ticker))
        .route("/health", get(handle_rest_health))
        .with_state(state);

    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    wait_for_http_health(addr).await;

    addr
}

async fn handle_ws_upgrade(ws: WebSocketUpgrade, State(state): State<WsState>) -> Response {
    state.upgrade_attempts.fetch_add(1, Ordering::SeqCst);
    if state
        .upgrade_failures
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
            remaining.checked_sub(1)
        })
        .is_ok()
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: WsState) {
    state.connection_count.fetch_add(1, Ordering::SeqCst);
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    *state.close_signal.lock().await = Some(close_tx);

    loop {
        let frame = tokio::select! {
            _ = close_rx.recv() => break,
            frame = receiver.next() => frame,
        };
        let Some(frame) = frame else { break };
        let Ok(frame) = frame else { break };
        match frame {
            Message::Text(text) => {
                let Ok(payload) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let id = payload.get("id").and_then(Value::as_u64).unwrap_or(0);
                let method = payload.get("method").and_then(Value::as_str).unwrap_or("");

                match method {
                    "subscribe" => {
                        state.subscribe_frames.lock().await.push(payload.clone());
                        let channels = payload
                            .get("params")
                            .and_then(|p| p.get("channels"))
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        let reject = state
                            .subscribe_failures
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                remaining.checked_sub(1)
                            })
                            .is_ok();
                        let reply = if reject {
                            json!({"id": id, "error": {"code": -32603, "message": "subscribe denied"}})
                        } else {
                            json!({"id": id, "result": {"channels": channels}})
                        };
                        let mut notifications = Vec::new();

                        if !reject {
                            let mut configured = state.subscription_notifications.lock().await;
                            for channel in channels.iter().filter_map(Value::as_str) {
                                if state.send_all_notifications.load(Ordering::SeqCst) {
                                    if let Some(values) = configured.get_mut(channel) {
                                        notifications.append(values);
                                    }
                                } else {
                                    let notification = configured
                                        .get_mut(channel)
                                        .and_then(|values| {
                                            (!values.is_empty()).then(|| values.remove(0))
                                        })
                                        .or_else(|| subscription_notification(channel));

                                    if let Some(notification) = notification {
                                        notifications.push(notification);
                                    }
                                }
                            }
                        }
                        let deferred = state
                            .deferred_subscribe_responses
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                remaining.checked_sub(1)
                            })
                            .is_ok();

                        if deferred
                            && state
                                .notification_before_subscribe_response
                                .load(Ordering::SeqCst)
                        {
                            for notification in notifications.drain(..) {
                                if !send_ws_message(&sender, notification).await {
                                    break;
                                }
                            }
                        }

                        if deferred {
                            let sender = Arc::clone(&sender);
                            let gate = Arc::clone(&state.subscribe_response_gate);
                            let response_count = Arc::clone(&state.subscribe_response_count);

                            tokio::spawn(async move {
                                let permit = gate.acquire().await.expect("gate must remain open");
                                permit.forget();

                                if send_ws_message(&sender, reply).await {
                                    response_count.fetch_add(1, Ordering::SeqCst);

                                    for notification in notifications {
                                        if !send_ws_message(&sender, notification).await {
                                            break;
                                        }
                                    }
                                }
                            });
                        } else {
                            if state
                                .notification_before_subscribe_response
                                .load(Ordering::SeqCst)
                            {
                                for notification in notifications.drain(..) {
                                    if !send_ws_message(&sender, notification).await {
                                        break;
                                    }
                                }
                            }
                            if !send_ws_message(&sender, reply).await {
                                break;
                            }
                            state
                                .subscribe_response_count
                                .fetch_add(1, Ordering::SeqCst);

                            for notification in notifications {
                                if !send_ws_message(&sender, notification).await {
                                    break;
                                }
                                let delay_ms = state.notification_delay_ms.load(Ordering::SeqCst);
                                if delay_ms > 0 {
                                    tokio::time::sleep(Duration::from_millis(delay_ms as u64))
                                        .await;
                                }
                            }
                        }
                    }
                    "unsubscribe" => {
                        state.unsubscribe_frames.lock().await.push(payload.clone());
                        if state.subscribe_response_count.load(Ordering::SeqCst) == 0 {
                            state
                                .unsubscribe_before_subscribe_response
                                .store(true, Ordering::SeqCst);
                        }
                        let reject = state
                            .unsubscribe_failures
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                remaining.checked_sub(1)
                            })
                            .is_ok();
                        let reply = if reject {
                            json!({"id": id, "error": {"code": -32603, "message": "unsubscribe denied"}})
                        } else {
                            json!({"id": id, "result": {"success": true}})
                        };

                        if !send_ws_message(&sender, reply).await {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    state.connection_count.fetch_sub(1, Ordering::SeqCst);
}

async fn send_ws_message(
    sender: &Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocket, Message>>>,
    value: Value,
) -> bool {
    sender
        .lock()
        .await
        .send(Message::Text(value.to_string().into()))
        .await
        .is_ok()
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

fn data_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

fn load_json(filename: &str) -> Value {
    let content = std::fs::read_to_string(data_path().join(filename))
        .unwrap_or_else(|_| panic!("failed to read {filename}"));
    serde_json::from_str(&content).expect("invalid json")
}

fn subscription_notification(channel: &str) -> Option<Value> {
    let data = if channel.starts_with("ticker_slim.") {
        load_json("perps/ws_ticker_slim_eth.json")
    } else if channel.starts_with("ticker.") {
        json!({
            "timestamp": 1_700_000_000_010_i64,
            "instrument_ticker": load_json("perps/ws_ticker_eth.json"),
        })
    } else if channel.starts_with("orderbook.") {
        load_json("perps/ws_orderbook_eth.json")
    } else if channel == "trades.option.ETH" {
        json!([load_json("options/ws_trade_eth.json")])
    } else if channel == "trades.perp.ETH" {
        json!([load_json("perps/ws_trade_eth.json")])
    } else {
        return None;
    };

    Some(json!({
        "jsonrpc": "2.0",
        "method": "subscription",
        "params": {
            "channel": channel,
            "data": data,
        }
    }))
}

fn config(rest_addr: SocketAddr, ws_addr: SocketAddr) -> DeriveDataClientConfig {
    DeriveDataClientConfig {
        base_url_rest: Some(rest_url(rest_addr)),
        base_url_ws: Some(ws_url(ws_addr)),
        proxy_url: None,
        environment: DeriveEnvironment::Mainnet,
        http_timeout_secs: 5,
        ws_timeout_secs: Some(5),
        update_instruments_interval_mins: 60,
        currencies: Vec::new(),
        include_expired: false,
        auto_load_missing_instruments: true,
        rest_market_data_failover_enabled: false,
        rest_market_data_poll_interval_ms: None,
        rest_market_data_max_quote_age_secs: None,
        transport_backend: TransportBackend::default(),
    }
}

fn params(values: &[(&str, Value)]) -> Params {
    let mut params = Params::new();
    for (key, value) in values {
        params.insert((*key).to_string(), value.clone());
    }
    params
}

fn subscribe_quotes(instrument_id: InstrumentId, params: Option<Params>) -> SubscribeQuotes {
    SubscribeQuotes::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        params,
    )
}

fn unsubscribe_quotes(instrument_id: InstrumentId, params: Option<Params>) -> UnsubscribeQuotes {
    UnsubscribeQuotes::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        params,
    )
}

fn subscribe_book_deltas(
    instrument_id: InstrumentId,
    depth: Option<usize>,
    params: Option<Params>,
) -> SubscribeBookDeltas {
    SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        depth.and_then(NonZeroUsize::new),
        false,
        None,
        params,
    )
}

fn subscribe_book_depth(instrument_id: InstrumentId, params: Option<Params>) -> SubscribeBookDepth {
    SubscribeBookDepth::new(
        instrument_id,
        BookType::L2_MBP,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        NonZeroUsize::new(10),
        false,
        None,
        params,
    )
}

fn unsubscribe_book_deltas(
    instrument_id: InstrumentId,
    params: Option<Params>,
) -> UnsubscribeBookDeltas {
    UnsubscribeBookDeltas::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        params,
    )
}

fn unsubscribe_book_depth(instrument_id: InstrumentId) -> UnsubscribeBookDepth {
    UnsubscribeBookDepth::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn subscribe_trades(instrument_id: InstrumentId) -> SubscribeTrades {
    SubscribeTrades::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

fn unsubscribe_trades(instrument_id: InstrumentId) -> UnsubscribeTrades {
    UnsubscribeTrades::new(
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    )
}

async fn wait_for_subscribe(state: &WsState, channel: &str) {
    wait_until_async(
        || {
            let state = state.clone();
            let channel = channel.to_string();
            async move {
                state.subscribes().await.iter().any(|frame| {
                    frame["params"]["channels"]
                        .as_array()
                        .is_some_and(|channels| channels.iter().any(|c| c == &channel))
                })
            }
        },
        Duration::from_secs(5),
    )
    .await;
}

async fn wait_for_subscribe_count(state: &WsState, channel: &str, expected: usize) {
    wait_until_async(
        || {
            let state = state.clone();
            let channel = channel.to_string();
            async move {
                state
                    .subscribes()
                    .await
                    .iter()
                    .filter(|frame| {
                        frame["params"]["channels"]
                            .as_array()
                            .is_some_and(|channels| channels.iter().any(|value| value == &channel))
                    })
                    .count()
                    >= expected
            }
        },
        Duration::from_secs(5),
    )
    .await;
}

async fn wait_for_unsubscribe(state: &WsState, channel: &str) {
    wait_until_async(
        || {
            let state = state.clone();
            let channel = channel.to_string();
            async move {
                state.unsubscribes().await.iter().any(|frame| {
                    frame["params"]["channels"]
                        .as_array()
                        .is_some_and(|channels| channels.iter().any(|c| c == &channel))
                })
            }
        },
        Duration::from_secs(5),
    )
    .await;
}

async fn recv_data(rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> Data {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timeout waiting for data event")
        .expect("data event channel closed")
    {
        DataEvent::Data(data) => data,
        other => panic!("expected data event, was {other:?}"),
    }
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribes_dispatches_and_unsubscribes_exact_channels() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let option_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(
            instrument_id,
            Some(params(&[("interval", json!("100"))])),
        ))
        .unwrap();
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.100").await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => {
            assert_eq!(quote.instrument_id, instrument_id);
            assert_eq!(quote.bid_price, Price::from("1992.36"));
            assert_eq!(quote.ask_price, Price::from("1992.37"));
            assert_eq!(quote.bid_size, Quantity::from("1.505"));
            assert_eq!(quote.ask_size, Quantity::from("1.505"));
        }
        other => panic!("expected quote data, was {other:?}"),
    }

    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_unsubscribe(&ws_state, "ticker_slim.ETH-PERP.100").await;

    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id, Some(20), None))
        .unwrap();
    wait_for_subscribe(&ws_state, "orderbook.ETH-PERP.1.20").await;

    match recv_data(&mut rx).await {
        Data::BookDeltas(deltas) => {
            assert_eq!(deltas.instrument_id, instrument_id);
            assert_eq!(deltas.deltas.len(), 3);
            assert_eq!(deltas.deltas[1].order.price, Price::from("3500.00"));
        }
        other => panic!("expected deltas data, was {other:?}"),
    }

    client
        .unsubscribe_book_deltas(&unsubscribe_book_deltas(instrument_id, None))
        .unwrap();
    wait_for_unsubscribe(&ws_state, "orderbook.ETH-PERP.1.20").await;

    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();
    wait_for_subscribe(&ws_state, "trades.option.ETH").await;

    match recv_data(&mut rx).await {
        Data::Trade(trade) => {
            assert_eq!(trade.instrument_id, option_id);
            assert_eq!(trade.trade_id.to_string(), "option-trade-1");
        }
        other => panic!("expected trade data, was {other:?}"),
    }

    client
        .unsubscribe_trades(&unsubscribe_trades(option_id))
        .unwrap();
    wait_for_unsubscribe(&ws_state, "trades.option.ETH").await;

    let requests = rest_state.requests().await;
    assert_eq!(requests.len(), 3);
    let bodies: HashSet<String> = requests.iter().map(|r| r.body.to_string()).collect();
    assert!(bodies.contains(
        &json!({"currency": "ETH", "instrument_type": "perp", "expired": false}).to_string()
    ));
    assert!(bodies.contains(
        &json!({"currency": "ETH", "instrument_type": "option", "expired": false}).to_string()
    ));
    assert!(bodies.contains(
        &json!({"currency": "ETH", "instrument_type": "erc20", "expired": false}).to_string()
    ));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_quote_subscription_publishes_fresh_rest_ticker_when_ws_is_silent() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert("ticker_slim.ETH-PERP.1000".to_string(), vec![Value::Null]);
    let timestamp = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": timestamp,
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.1000").await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => {
            assert_eq!(quote.instrument_id, instrument_id);
            assert_eq!(quote.bid_price, Price::from("3499.50"));
            assert_eq!(quote.ask_price, Price::from("3501.00"));
            assert_eq!(quote.bid_size, Quantity::from("0.800"));
            assert_eq!(quote.ask_size, Quantity::from("1.200"));
            assert_eq!(quote.ts_event.as_u64(), timestamp as u64 * 1_000_000);
        }
        other => panic!("expected quote data, was {other:?}"),
    }

    let calls = rest_state.ticker_calls().await;
    assert_eq!(calls[0]["instrument_name"], "ETH-PERP");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "the same REST ticker timestamp must not publish twice",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_rest_failover_starts_after_initial_websocket_upgrade_failure() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state.upgrade_failures.store(1, Ordering::SeqCst);
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": now_ms,
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_until_async(
        || {
            let rest_state = rest_state.clone();
            async move { !rest_state.ticker_calls().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.bid_price, Price::from("3499.50")),
        other => panic!("expected REST quote during WebSocket retry, was {other:?}"),
    }
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.1000").await;
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_rest_failover_reports_connected_during_sustained_websocket_outage() {
    // A sustained 503 upgrade failure must not leave `is_connected()` false
    // forever: `DataEngine`/`LiveNode` startup polls exactly that seam
    // (`check_connected` -> `DataClient::is_connected`) and aborts at a
    // deadline if it never reports true. REST-active failover must satisfy
    // that readiness contract without claiming the WebSocket is healthy.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .upgrade_failures
        .store(usize::MAX, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();

    assert!(
        client.is_connected(),
        "client must report connected while REST failover is the active path, \
         even though the WebSocket upgrade keeps failing",
    );

    // The WebSocket keeps retrying and keeps failing (sustained outage);
    // readiness must still hold well past a single retry cycle.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        client.is_connected(),
        "client must remain connected through a sustained WebSocket outage",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_repeated_connect_during_initial_retry_does_not_race_event_receiver() {
    // A second `connect()` call while the initial-reconnect retry task is
    // still trying must be treated as an existing connection attempt, not
    // race the retry task for the WebSocket client's sole event receiver
    // (which could otherwise tear down a session the retry task just
    // recovered). The retry task sleeps 250ms between attempts, so a
    // second `connect()` issued immediately after the first one returns
    // finds the WebSocket mutex free; without the fix it would acquire it
    // and perform its own upgrade attempt synchronously.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .upgrade_failures
        .store(usize::MAX, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();

    client.connect().await.unwrap();
    let attempts_after_first_connect = ws_state.upgrade_attempts.load(Ordering::SeqCst);
    assert_eq!(attempts_after_first_connect, 1);

    client.connect().await.unwrap();

    assert_eq!(
        ws_state.upgrade_attempts.load(Ordering::SeqCst),
        attempts_after_first_connect,
        "a connect() call made while the initial retry task owns the WebSocket \
         lifecycle must not itself attempt a new upgrade",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_initial_retry_task_keeps_reconnecting_after_terminal_stream_failure() {
    // The initial-reconnect retry task must keep owning WebSocket recovery
    // for the rest of the session. A later *terminal* recovery failure (the
    // venue rejects every resubscribe attempt until the session-recovery
    // budget is exhausted) closes the WebSocket message stream the retry
    // task is reading from; the task must dial again rather than abandoning
    // WebSocket recovery for good and leaving REST as the permanent path.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state.upgrade_failures.store(1, Ordering::SeqCst);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert("ticker_slim.ETH-PERP.1000".to_string(), vec![Value::Null]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();

    // Initial upgrade fails once; the retry task redials and succeeds.
    client.connect().await.unwrap();
    wait_until_async(
        || {
            let ws_state = ws_state.clone();
            async move { ws_state.connection_count.load(Ordering::SeqCst) >= 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.1000").await;
    wait_until_async(
        || {
            let client_ref = &client;
            async move { client_ref.is_connected() }
        },
        Duration::from_secs(2),
    )
    .await;
    assert!(client.is_connected());

    // `upgrade_attempts` is a monotonic counter (unlike `connection_count`,
    // a live-connection gauge that nets back to the same value across a
    // disconnect/redial cycle), so it is what actually proves a *new* dial
    // happened. Two attempts have occurred so far: the initial failure and
    // the retry task's first successful connect.
    let attempts_before_terminal_failure = ws_state.upgrade_attempts.load(Ordering::SeqCst);
    assert_eq!(attempts_before_terminal_failure, 2);

    // Every resubscribe attempt during recovery is rejected, so the venue
    // session recovery budget exhausts and the stream closes. That alone
    // costs one more dial (the generic transport reconnecting at the socket
    // level before session recovery can even attempt a resubscribe).
    ws_state
        .subscribe_failures
        .store(usize::MAX, Ordering::SeqCst);
    ws_state.force_disconnect().await;

    // The retry task observes the closed stream, falls back to its outer
    // loop, and dials a fresh WebSocket connection again — a second attempt
    // beyond the transport's own reconnect.
    wait_until_async(
        || {
            let ws_state = ws_state.clone();
            async move {
                ws_state.upgrade_attempts.load(Ordering::SeqCst)
                    >= attempts_before_terminal_failure + 2
            }
        },
        Duration::from_secs(10),
    )
    .await;
    assert!(
        ws_state.upgrade_attempts.load(Ordering::SeqCst) >= attempts_before_terminal_failure + 2,
        "the initial retry task must dial again after a terminal recovery \
         failure closes its WebSocket stream, instead of abandoning \
         WebSocket recovery for the rest of the session",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_initial_retry_task_restores_quote_subscription_after_terminal_stream_failure() {
    // Companion to `test_initial_retry_task_keeps_reconnecting_after_terminal_stream_failure`,
    // which only proves the retry task dials again. A fresh `connect()`
    // tears down the old transport's desired-subscription set entirely
    // (unlike an in-place reconnect, which replays it automatically), so
    // dialling again is not enough on its own: without replaying active
    // channels from the data client's own subscription registry, the socket
    // reconnects with no ticker subscription and WS quote data never
    // resumes for the rest of the process.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    ws_state.upgrade_failures.store(1, Ordering::SeqCst);
    let initial_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut initial_notification = subscription_notification(channel).unwrap();
    initial_notification["params"]["data"]["instrument_ticker"]["t"] = json!(initial_ms);
    initial_notification["params"]["data"]["timestamp"] = json!(initial_ms);
    let restored_ms = initial_ms + 1_000;
    let mut restored_notification = subscription_notification(channel).unwrap();
    restored_notification["params"]["data"]["instrument_ticker"]["t"] = json!(restored_ms);
    restored_notification["params"]["data"]["timestamp"] = json!(restored_ms);
    ws_state.subscription_notifications.lock().await.insert(
        channel.to_string(),
        vec![initial_notification, restored_notification],
    );
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();

    // Initial upgrade fails once; the retry task redials and succeeds.
    client.connect().await.unwrap();
    wait_until_async(
        || {
            let ws_state = ws_state.clone();
            async move { ws_state.connection_count.load(Ordering::SeqCst) >= 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;
    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), initial_ms as u64 * 1_000_000),
        other => panic!("expected initial WebSocket quote, was {other:?}"),
    }

    // Reject exactly `MAX_SESSION_RECOVERY_ATTEMPTS` (3) resubscribe
    // attempts so in-place session recovery exhausts deterministically,
    // then the counter naturally reaches zero and rejects nothing further
    // -- the redial's own resubscribe is therefore the 5th subscribe frame
    // for this channel (1 initial + 3 rejected recovery attempts + 1
    // restored), not a racy manual reset against the production task.
    ws_state.subscribe_failures.store(3, Ordering::SeqCst);
    let attempts_before_terminal_failure = ws_state.upgrade_attempts.load(Ordering::SeqCst);
    ws_state.force_disconnect().await;

    wait_until_async(
        || {
            let ws_state = ws_state.clone();
            async move {
                ws_state.upgrade_attempts.load(Ordering::SeqCst)
                    >= attempts_before_terminal_failure + 2
            }
        },
        Duration::from_secs(10),
    )
    .await;
    assert!(
        ws_state.upgrade_attempts.load(Ordering::SeqCst) >= attempts_before_terminal_failure + 2,
        "the initial retry task must dial again after a terminal recovery failure",
    );

    wait_for_subscribe_count(&ws_state, channel, 5).await;
    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), restored_ms as u64 * 1_000_000),
        other => panic!(
            "expected the quote subscription to be restored and a fresh WebSocket quote to \
             flow after the redial, was {other:?}"
        ),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_rest_polling_resumes_immediately_after_transport_disconnect_despite_fresh_quote() {
    // REST polling must react to the transport actually being down, not
    // only to quote staleness: `is_connected` is cleared on a recovery
    // *failure*, not on an ordinary transport drop, so a poller gated only
    // by `is_connected` plus quote age would keep trusting a still-"fresh"
    // last WS quote for the whole configured max-age window even though
    // the socket just dropped.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let channel = "ticker_slim.ETH-PERP.1000";
    let mut ws_notification = subscription_notification(channel).unwrap();
    ws_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms);
    ws_notification["params"]["data"]["timestamp"] = json!(now_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![ws_notification]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    // Deliberately generous: long enough that the fix must be reacting to
    // the transport drop itself, not merely waiting the quote out.
    cfg.rest_market_data_max_quote_age_secs = Some(30);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), now_ms as u64 * 1_000_000),
        other => panic!("expected fresh WebSocket quote, was {other:?}"),
    }

    // Sanity: once the fresh WS quote is confirmed, REST must go idle (a
    // poll or two racing the notification on startup is expected and fine).
    let calls_after_ws_quote = rest_state.ticker_calls().await.len();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        rest_state.ticker_calls().await.len(),
        calls_after_ws_quote,
        "REST must not keep polling once the current WebSocket quote is confirmed fresh",
    );

    let fresh_now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": fresh_now_ms,
    });
    // Every resubscribe attempt during recovery is rejected, so session
    // recovery cannot complete (no `Reconnected`) and, within this test's
    // tight assertion window, cannot yet have exhausted its retry budget
    // either (no `SessionRecoveryFailed`, so `is_connected` stays true).
    // Any resumed polling observed below is therefore attributable only to
    // the transport epoch changing underneath a still-"connected" client.
    ws_state
        .subscribe_failures
        .store(usize::MAX, Ordering::SeqCst);
    let calls_before_disconnect = rest_state.ticker_calls().await.len();
    ws_state.force_disconnect().await;

    // Comfortably inside the ~1s it takes the 3-attempt session-recovery
    // budget (250ms/500ms backoff) to exhaust, so `is_connected` cannot
    // have been cleared by a recovery failure yet.
    wait_until_async(
        || {
            let rest_state = rest_state.clone();
            async move { rest_state.ticker_calls().await.len() > calls_before_disconnect }
        },
        Duration::from_millis(600),
    )
    .await;
    assert!(
        rest_state.ticker_calls().await.len() > calls_before_disconnect,
        "REST polling must resume promptly after an ordinary transport \
         disconnect, even though the last WebSocket quote is still within \
         the configured max age and is_connected has not yet been cleared",
    );
    assert!(
        client.is_connected(),
        "is_connected must still be true at this point (recovery is still \
         retrying, not yet exhausted) -- the resumed polling above cannot \
         be explained by is_connected alone",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_rest_polling_starts_during_sustained_upgrade_outage_despite_fresh_quote() {
    // A sustained upgrade outage (every reconnect attempt 503s, mirroring
    // the motivating production incident) never installs a new transport,
    // so the low-level connection epoch never changes -- only the
    // transport's own connection mode reflects the outage. A poller gated
    // on epoch alone would keep trusting the last WS quote as "current"
    // until it ages out, even though the socket has been down the whole
    // time. This is a distinct scenario from an ordinary disconnect that
    // successfully redials (which changes the epoch and was already
    // covered): here the epoch is fixed throughout, so only `is_active()`
    // can detect the outage.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let channel = "ticker_slim.ETH-PERP.1000";
    let mut ws_notification = subscription_notification(channel).unwrap();
    ws_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms);
    ws_notification["params"]["data"]["timestamp"] = json!(now_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![ws_notification]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    // Deliberately generous: long enough that the fix must be reacting to
    // the transport being down, not merely waiting the quote out.
    cfg.rest_market_data_max_quote_age_secs = Some(30);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), now_ms as u64 * 1_000_000),
        other => panic!("expected fresh WebSocket quote, was {other:?}"),
    }

    let fresh_now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": fresh_now_ms,
    });

    // Every upgrade attempt from here on 503s, so the transport can never
    // leave `Reconnect`/`Disconnect` and the connection epoch never moves.
    ws_state
        .upgrade_failures
        .store(usize::MAX, Ordering::SeqCst);
    let calls_before_disconnect = rest_state.ticker_calls().await.len();
    ws_state.force_disconnect().await;

    wait_until_async(
        || {
            let rest_state = rest_state.clone();
            async move { rest_state.ticker_calls().await.len() > calls_before_disconnect }
        },
        Duration::from_secs(2),
    )
    .await;
    assert!(
        rest_state.ticker_calls().await.len() > calls_before_disconnect,
        "REST polling must start promptly during a sustained upgrade outage, \
         even though the last WebSocket quote is still within the \
         configured max age and the connection epoch has not changed",
    );

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(
            quote.bid_price,
            Price::from("3499.50"),
            "the post-request recheck must use the same transport-health \
             signal as the pre-request check, so the REST quote is \
             actually published rather than suppressed as \"WS current\"",
        ),
        other => panic!("expected a published REST quote, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_quote_subscription_keeps_rest_failover_disabled_by_default() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert("ticker_slim.ETH-PERP.1000".to_string(), vec![Value::Null]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.1000").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(rest_state.ticker_calls().await.is_empty());
    assert!(rx.try_recv().is_err());
    client.disconnect().await.unwrap();
}

#[rstest]
#[case::missing_interval(None, Some(1))]
#[case::zero_interval(Some(0), Some(1))]
#[case::missing_max_age(Some(1), None)]
#[case::zero_max_age(Some(1), Some(0))]
#[case::unsupported_max_age(Some(1), Some(u64::MAX))]
fn test_rest_failover_requires_valid_explicit_bounds(
    #[case] poll_interval_ms: Option<u64>,
    #[case] max_quote_age_secs: Option<u64>,
) {
    let mut cfg = config(
        SocketAddr::from(([127, 0, 0, 1], 1)),
        SocketAddr::from(([127, 0, 0, 1], 1)),
    );
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = poll_interval_ms;
    cfg.rest_market_data_max_quote_age_secs = max_quote_age_secs;

    assert!(DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).is_err());
}

#[rstest]
#[case::wrong_instrument("wrong_instrument")]
#[case::missing_bid("missing_bid")]
#[case::crossed_book("crossed_book")]
#[case::negative_prices("negative_prices")]
#[case::zero_bid_price("zero_bid_price")]
#[case::zero_ask_price("zero_ask_price")]
#[case::zero_size("zero_size")]
#[case::stale("stale")]
#[case::future("future")]
#[tokio::test]
async fn test_quote_subscription_rejects_invalid_rest_tickers(#[case] failure: &str) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert("ticker_slim.ETH-PERP.1000".to_string(), vec![Value::Null]);
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut ticker = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": now_ms,
    });
    match failure {
        "wrong_instrument" => ticker["instrument_name"] = json!("BTC-PERP"),
        "missing_bid" => {
            ticker.as_object_mut().unwrap().remove("best_bid_price");
        }
        "crossed_book" => ticker["best_ask_price"] = json!("3499.0"),
        "negative_prices" => {
            ticker["best_bid_price"] = json!("-2.0");
            ticker["best_ask_price"] = json!("-1.0");
        }
        "zero_bid_price" => ticker["best_bid_price"] = json!("0"),
        "zero_ask_price" => ticker["best_ask_price"] = json!("0"),
        "zero_size" => ticker["best_bid_amount"] = json!("0"),
        "stale" => ticker["timestamp"] = json!(now_ms - 5_000),
        "future" => ticker["timestamp"] = json!(now_ms + 5_000),
        _ => unreachable!(),
    }
    *rest_state.ticker_response.lock().await = ticker;
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(1);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, "ticker_slim.ETH-PERP.1000").await;
    wait_until_async(
        || {
            let rest_state = rest_state.clone();
            async move { !rest_state.ticker_calls().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "invalid REST ticker {failure} must not publish a quote",
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_quote_subscription_returns_to_current_websocket_updates() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let channel = "ticker_slim.ETH-PERP.1000";
    let mut ws_notification = subscription_notification(channel).unwrap();
    ws_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms);
    ws_notification["params"]["data"]["timestamp"] = json!(now_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![Value::Null, ws_notification]);
    ws_state.notification_delay_ms.store(150, Ordering::SeqCst);
    ws_state
        .send_all_notifications
        .store(true, Ordering::SeqCst);
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": now_ms - 300,
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(1);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.bid_price, Price::from("3499.50")),
        other => panic!("expected REST quote, was {other:?}"),
    }
    match recv_data(&mut rx).await {
        Data::Quote(quote) => {
            assert_eq!(quote.bid_price, Price::from("1992.36"));
            assert_eq!(quote.ts_event.as_u64(), now_ms as u64 * 1_000_000);
        }
        other => panic!("expected WebSocket quote, was {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let calls_after_recovery = rest_state.ticker_calls().await.len();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(rest_state.ticker_calls().await.len(), calls_after_recovery);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "current WebSocket data must suppress further REST quote publications",
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_inflight_rest_quote_is_discarded_after_current_websocket_quote() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let channel = "ticker_slim.ETH-PERP.1000";
    let mut ws_notification = subscription_notification(channel).unwrap();
    ws_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms - 100);
    ws_notification["params"]["data"]["timestamp"] = json!(now_ms - 100);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![ws_notification]);
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": now_ms,
    });
    rest_state
        .ticker_response_delay_ms
        .store(200, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(1);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_until_async(
        || {
            let rest_state = rest_state.clone();
            async move { !rest_state.ticker_calls().await.is_empty() }
        },
        Duration::from_secs(5),
    )
    .await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.bid_price, Price::from("1992.36")),
        other => panic!("expected current WebSocket quote, was {other:?}"),
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        rx.try_recv().is_err(),
        "in-flight REST quote must be discarded"
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_rest_quote_polling_stops_after_failed_subscription_and_resubscribe() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state.subscribe_failures.store(1, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(40);
    cfg.rest_market_data_max_quote_age_secs = Some(1);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, "ticker_slim.ETH-PERP.1000", 1).await;
    tokio::time::sleep(Duration::from_millis(180)).await;
    let calls_after_failed_subscription = rest_state.ticker_calls().await.len();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        rest_state.ticker_calls().await.len(),
        calls_after_failed_subscription,
        "failed quote subscription must stop its REST poller",
    );
    assert!(rx.try_recv().is_err());

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, "ticker_slim.ETH-PERP.1000", 2).await;
    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(180)).await;
    let calls_after_resubscribe = rest_state.ticker_calls().await.len();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        rest_state.ticker_calls().await.len() - calls_after_resubscribe <= 4,
        "resubscribe must run one poller at the configured cadence",
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_unsubscribe_resubscribe_interleaved_with_reconnect_settles_cleanly() {
    // The explicit subscription-confirmation write in `run_channel_subscribe`
    // checks the owner's generation and the session generation and performs
    // the write atomically under one lock acquisition shared with the
    // `Reconnected` handler. Interleaving an unsubscribe/resubscribe cycle
    // with a concurrent, unrelated transport reconnect must not corrupt that
    // state: the client must settle back into a single, correctly confirmed
    // subscription with WebSocket data flowing normally, never a stuck or
    // duplicated poller.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![Value::Null, Value::Null]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, channel, 1).await;

    // Rapid-fire unsubscribe/resubscribe, with an unrelated transport
    // disconnect landing in the middle of it -- no synchronization between
    // the two, deliberately.
    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    ws_state.force_disconnect().await;

    // The client must settle: exactly one owner ends up subscribed and
    // confirmed, and a fresh WebSocket ticker is accepted and published
    // through the normal (non-REST) path.
    wait_until_async(
        || {
            let client_ref = &client;
            async move { client_ref.is_connected() }
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(client.is_connected());

    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut fresh_notification = subscription_notification(channel).unwrap();
    fresh_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms);
    fresh_notification["params"]["data"]["timestamp"] = json!(now_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![fresh_notification]);
    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.instrument_id, instrument_id),
        other => panic!("expected a published quote after settling, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_pre_ack_websocket_quote_does_not_recover_rest_failover() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    let ws_timestamp = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut pre_ack_notification = subscription_notification(channel).unwrap();
    pre_ack_notification["params"]["data"]["instrument_ticker"]["t"] = json!(ws_timestamp);
    pre_ack_notification["params"]["data"]["timestamp"] = json!(ws_timestamp);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![pre_ack_notification]);
    ws_state
        .notification_before_subscribe_response
        .store(true, Ordering::SeqCst);
    ws_state
        .deferred_subscribe_responses
        .store(1, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": now_ms,
    });

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.bid_price, Price::from("3499.50")),
        other => panic!("expected REST quote before subscribe acknowledgement, was {other:?}"),
    }
    ws_state.subscribe_response_gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rx.try_recv().is_err(),
        "pre-ack WebSocket quote must not be published"
    );
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_reconnect_before_ack_does_not_publish_prior_session_confirmation() {
    // The prior session's subscription confirmation must not survive a
    // reconnect: a ticker the venue replays *before* the post-reconnect
    // resubscribe is acknowledged must not be published using the stale
    // confirmation left over from before the transport dropped (the same
    // discipline already applied to the very first subscribe's pre-ack
    // case, now also fenced across a reconnect by transport epoch).
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    let now_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut initial_notification = subscription_notification(channel).unwrap();
    initial_notification["params"]["data"]["instrument_ticker"]["t"] = json!(now_ms);
    initial_notification["params"]["data"]["timestamp"] = json!(now_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![initial_notification]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(30);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), now_ms as u64 * 1_000_000),
        other => panic!("expected initial WebSocket quote, was {other:?}"),
    }

    // Gate the *resubscribe* ack during recovery: the venue sends a fresh,
    // monotonic replay ticker before that ack response is released, so a
    // rejection below can only be explained by the confirmation fence, not
    // by staleness or non-monotonicity. The timestamp is "now" (not a fixed
    // offset): it must predate whenever the message actually gets
    // processed, after the reconnect dance below, or the pre-existing
    // future-dated check would reject it for an unrelated reason.
    let mut replay_notification = subscription_notification(channel).unwrap();
    let replay_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    replay_notification["params"]["data"]["instrument_ticker"]["t"] = json!(replay_ms);
    replay_notification["params"]["data"]["timestamp"] = json!(replay_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![replay_notification]);
    ws_state
        .notification_before_subscribe_response
        .store(true, Ordering::SeqCst);
    ws_state
        .deferred_subscribe_responses
        .store(1, Ordering::SeqCst);

    ws_state.force_disconnect().await;
    wait_for_subscribe_count(&ws_state, channel, 2).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rx.try_recv().is_err(),
        "a pre-ack replay ticker on a reconnected session must not be \
         published using the prior session's now-stale confirmation",
    );

    ws_state.subscribe_response_gate.add_permits(1);
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_disconnect_connect_pre_ack_does_not_reuse_stale_confirmation() {
    // A full disconnect()/connect() rebuilds the transport from scratch, so
    // the new transport's low-level epoch restarts at zero exactly like the
    // torn-down transport's did. The prior session's subscription
    // confirmation must not survive that rebuild: a ticker the venue
    // replays before the fresh subscribe is acknowledged must not be
    // published, and REST polling must not stop, even though the old
    // (now-stale) confirmation shares the rebuilt transport's epoch value.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    let initial_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut initial_notification = subscription_notification(channel).unwrap();
    initial_notification["params"]["data"]["instrument_ticker"]["t"] = json!(initial_ms);
    initial_notification["params"]["data"]["timestamp"] = json!(initial_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![initial_notification]);
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.rest_market_data_failover_enabled = true;
    cfg.rest_market_data_poll_interval_ms = Some(20);
    cfg.rest_market_data_max_quote_age_secs = Some(5);
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ts_event.as_u64(), initial_ms as u64 * 1_000_000),
        other => panic!("expected initial WebSocket quote, was {other:?}"),
    }

    // Full teardown: drop the first transport entirely instead of merely
    // forcing a low-level reconnect on the same one.
    client.disconnect().await.unwrap();
    while rx.try_recv().is_ok() {}

    // Queue a fresh pre-ack notification and gate the rebuilt transport's
    // subscribe acknowledgement, exactly as the single-transport reconnect
    // variant above does.
    let replay_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    let mut replay_notification = subscription_notification(channel).unwrap();
    replay_notification["params"]["data"]["instrument_ticker"]["t"] = json!(replay_ms);
    replay_notification["params"]["data"]["timestamp"] = json!(replay_ms);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![replay_notification]);
    ws_state
        .notification_before_subscribe_response
        .store(true, Ordering::SeqCst);
    ws_state
        .deferred_subscribe_responses
        .store(1, Ordering::SeqCst);

    client.connect().await.unwrap();
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, channel, 2).await;

    let rest_ms = get_atomic_clock_realtime().get_time_ms() as i64;
    *rest_state.ticker_response.lock().await = json!({
        "instrument_name": "ETH-PERP",
        "best_ask_amount": "1.2",
        "best_ask_price": "3501.0",
        "best_bid_amount": "0.8",
        "best_bid_price": "3499.5",
        "index_price": "3500.0",
        "mark_price": "3500.5",
        "max_price": "5000",
        "min_price": "100",
        "timestamp": rest_ms,
    });

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(
            quote.bid_price,
            Price::from("3499.50"),
            "REST polling must still cover the instrument: a pre-ack replay \
             on the rebuilt transport must not be mistaken for a confirmed \
             WebSocket subscription",
        ),
        other => panic!(
            "expected a REST quote before the rebuilt transport's subscribe ack, was {other:?}"
        ),
    }

    ws_state.subscribe_response_gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rx.try_recv().is_err(),
        "pre-ack WebSocket quote on the rebuilt transport must not be published \
         using the torn-down transport's stale confirmation"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_subscribe_book_depth_emits_depth_snapshot() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    client
        .subscribe_book_depth(subscribe_book_depth(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, "orderbook.ETH-PERP.1.10").await;

    match recv_data(&mut rx).await {
        Data::BookDepth(depth) => {
            assert_eq!(depth.instrument_id, instrument_id);
            assert_eq!(depth.bids[0].price, Price::from("3500.00"));
            assert_eq!(depth.bids[0].size, Quantity::from("1.000"));
            assert_eq!(depth.asks[0].price, Price::from("3501.00"));
            assert_eq!(depth.asks[0].size, Quantity::from("2.000"));
            assert_eq!(depth.bid_counts[0], 1);
            assert_eq!(depth.ask_counts[0], 1);
        }
        other => panic!("expected depth data, was {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        rx.try_recv().is_err(),
        "book depth subscription must not emit extra data",
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_shared_orderbook_channel_unsubscribes_after_last_owner() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let channel = "orderbook.ETH-PERP.1.10";

    client
        .subscribe_book_deltas(subscribe_book_deltas(instrument_id, Some(10), None))
        .unwrap();
    client
        .subscribe_book_depth(subscribe_book_depth(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    client
        .unsubscribe_book_deltas(&unsubscribe_book_deltas(instrument_id, None))
        .unwrap();
    assert!(ws_state.unsubscribes().await.is_empty());

    client
        .unsubscribe_book_depth(&unsubscribe_book_depth(instrument_id))
        .unwrap();
    wait_for_unsubscribe(&ws_state, channel).await;

    let subscribe_count = ws_state
        .subscribes()
        .await
        .iter()
        .filter(|frame| frame["params"]["channels"][0] == channel)
        .count();
    assert_eq!(subscribe_count, 1);
    assert_eq!(ws_state.unsubscribes().await.len(), 1);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_shared_trade_channel_survives_creator_subscribe_failure() {
    let rest_state = RestState::default();
    let mut response = load_json("common/http_get_instruments_eth_all.json");
    let instruments = response["result"].as_array_mut().unwrap();
    let mut second_option = instruments[1].clone();
    second_option["instrument_name"] = json!("ETH-20260627-3600-C");
    second_option["base_asset_sub_id"] = json!("43");
    second_option["option_details"]["strike"] = json!("3600");
    instruments.push(second_option);
    *rest_state.instruments_response.lock().await = Some(response);

    let ws_state = WsState::default();
    ws_state.subscribe_failures.store(1, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let first_option = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    let second_option = InstrumentId::from("ETH-20260627-3600-C.DERIVE");
    let channel = "trades.option.ETH";

    client
        .subscribe_trades(subscribe_trades(first_option))
        .unwrap();
    client
        .subscribe_trades(subscribe_trades(second_option))
        .unwrap();
    wait_for_subscribe_count(&ws_state, channel, 2).await;
    assert_eq!(ws_state.unsubscribes().await.len(), 1);

    let subscribe_count = ws_state
        .subscribes()
        .await
        .iter()
        .filter(|frame| frame["params"]["channels"][0] == channel)
        .count();
    assert_eq!(subscribe_count, 2);

    client
        .unsubscribe_trades(&unsubscribe_trades(first_option))
        .unwrap();
    client
        .unsubscribe_trades(&unsubscribe_trades(second_option))
        .unwrap();
    wait_until_async(
        || {
            let state = ws_state.clone();
            async move { state.unsubscribes().await.len() == 2 }
        },
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(ws_state.unsubscribes().await.len(), 2);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_unsubscribe_waits_for_inflight_subscribe_before_venue_cleanup() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state
        .deferred_subscribe_responses
        .store(1, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let channel = "ticker_slim.ETH-PERP.1000";

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;
    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(ws_state.unsubscribes().await.is_empty());

    ws_state.subscribe_response_gate.add_permits(1);
    wait_for_unsubscribe(&ws_state, channel).await;

    assert_eq!(ws_state.subscribe_response_count.load(Ordering::SeqCst), 1);
    assert!(
        !ws_state
            .unsubscribe_before_subscribe_response
            .load(Ordering::SeqCst)
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_failed_unsubscribe_allows_fresh_subscription() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    ws_state.unsubscribe_failures.store(1, Ordering::SeqCst);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
    let channel = "ticker_slim.ETH-PERP.1000";

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;
    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_unsubscribe(&ws_state, channel).await;
    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, channel, 2).await;

    assert_eq!(ws_state.unsubscribes().await.len(), 1);
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_quote_resubscribe_does_not_merge_pre_unsubscribe_cache() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "ticker_slim.ETH-PERP.1000";
    let full = subscription_notification(channel).unwrap();
    let mut partial = full.clone();
    partial["params"]["data"]["instrument_ticker"]["a"] = json!("0");
    partial["params"]["data"]["instrument_ticker"]["A"] = json!("0");
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![full, partial]);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;

    while rx.try_recv().is_ok() {}
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Quote(quote) => assert_eq!(quote.ask_price, Price::from("1992.37")),
        other => panic!("expected quote data, was {other:?}"),
    }

    client
        .unsubscribe_quotes(&unsubscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_unsubscribe(&ws_state, channel).await;

    while rx.try_recv().is_ok() {}

    client
        .subscribe_quotes(subscribe_quotes(instrument_id, None))
        .unwrap();
    wait_for_subscribe_count(&ws_state, channel, 2).await;

    let event = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        event.is_err(),
        "partial quote must not use cache from the prior subscription: {event:?}"
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_request_instruments_returns_err_for_empty_currencies() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut cfg = config(rest_addr, ws_addr);
    cfg.currencies = Vec::new();
    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();

    let request = RequestInstruments::new(
        None,
        None,
        Some(*DERIVE_CLIENT_ID),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    let err = client
        .request_instruments(request)
        .expect_err("must reject empty currencies");
    let msg = err.to_string();
    assert!(
        msg.contains("requires at least one configured currency"),
        "{msg}"
    );
}

fn request_trades(instrument_id: InstrumentId, limit: Option<usize>) -> RequestTrades {
    RequestTrades::new(
        instrument_id,
        None,
        None,
        limit.and_then(NonZeroUsize::new),
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_funding_rates(instrument_id: InstrumentId, limit: Option<usize>) -> RequestFundingRates {
    RequestFundingRates::new(
        instrument_id,
        None,
        None,
        limit.and_then(NonZeroUsize::new),
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_bars(bar_type: BarType, limit: Option<usize>) -> RequestBars {
    RequestBars::new(
        bar_type,
        None,
        None,
        limit.and_then(NonZeroUsize::new),
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_bars_window(
    bar_type: BarType,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    limit: Option<usize>,
) -> RequestBars {
    RequestBars::new(
        bar_type,
        start,
        end,
        limit.and_then(NonZeroUsize::new),
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_option_chain_reference_price(
    instrument_id: InstrumentId,
) -> RequestOptionChainReferencePrice {
    RequestOptionChainReferencePrice::new(
        OptionSeriesId::new(
            *DERIVE_VENUE,
            Ustr::from("ETH"),
            Ustr::from("USDC"),
            UnixNanos::from(1),
        ),
        instrument_id,
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

async fn recv_response(rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> DataResponse {
    recv_response_within(rx, Duration::from_secs(5)).await
}

async fn recv_response_within(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    within: Duration,
) -> DataResponse {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let event = tokio::time::timeout_at(deadline, rx.recv())
            .await
            .expect("timeout waiting for response event")
            .expect("data event channel closed");
        if let DataEvent::Response(response) = event {
            return response;
        }
    }
}

fn page_trade(trade_id: &str) -> Value {
    page_trade_at(trade_id, 1_700_000_000_000)
}

fn page_trade_at(trade_id: &str, timestamp: i64) -> Value {
    json!({
        "direction": "buy",
        "index_price": "3500",
        "instrument_name": "ETH-PERP",
        "liquidity_role": "taker",
        "mark_price": "3500",
        "realized_pnl": "0",
        "subaccount_id": 1,
        "timestamp": timestamp,
        "trade_amount": "0.25",
        "trade_fee": "0.01",
        "trade_id": trade_id,
        "trade_price": "3500",
        "tx_hash": "0xhash",
        "tx_status": "settled",
        "wallet": "0xwallet"
    })
}

fn trade_page(num_pages: usize, count: usize, id_prefix: &str) -> Value {
    let trades: Vec<Value> = (0..count)
        .map(|i| page_trade(&format!("{id_prefix}-{i}")))
        .collect();
    json!({
        "trades": trades,
        "pagination": {"count": (num_pages * count) as i64, "num_pages": num_pages as i64},
    })
}

async fn connect_with_eth_currency(rest_addr: SocketAddr, ws_addr: SocketAddr) -> DeriveDataClient {
    let mut cfg = config(rest_addr, ws_addr);
    cfg.currencies = vec!["ETH".to_string()];
    let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, cfg).unwrap();
    client.connect().await.unwrap();
    client
}

#[rstest]
#[tokio::test]
async fn test_ws_trades_emit_direction_as_aggressor_side() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let channel = "trades.perp.ETH";
    let mut notification = subscription_notification(channel).unwrap();
    let row = notification["params"]["data"][0].clone();
    let mut buy = row.clone();
    buy["direction"] = json!("buy");
    buy["trade_id"] = json!("perp-trade-buy");
    let mut sell = row;
    sell["direction"] = json!("sell");
    sell["trade_id"] = json!("perp-trade-sell");
    notification["params"]["data"] = json!([buy, sell]);
    ws_state
        .subscription_notifications
        .lock()
        .await
        .insert(channel.to_string(), vec![notification]);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    while rx.try_recv().is_ok() {}

    client
        .subscribe_trades(subscribe_trades(instrument_id))
        .unwrap();
    wait_for_subscribe(&ws_state, channel).await;

    match recv_data(&mut rx).await {
        Data::Trade(trade) => {
            assert_eq!(trade.instrument_id, instrument_id);
            assert_eq!(trade.aggressor_side, AggressorSide::Buy);
            assert_eq!(trade.trade_id, TradeId::from("perp-trade-buy"));
            assert_eq!(trade.price, Price::from("3500.00"));
            assert_eq!(trade.size, Quantity::from("1.000"));
        }
        other => panic!("expected trade data, was {other:?}"),
    }

    match recv_data(&mut rx).await {
        Data::Trade(trade) => {
            assert_eq!(trade.instrument_id, instrument_id);
            assert_eq!(trade.aggressor_side, AggressorSide::Sell);
            assert_eq!(trade.trade_id, TradeId::from("perp-trade-sell"));
        }
        other => panic!("expected trade data, was {other:?}"),
    }

    client
        .unsubscribe_trades(&unsubscribe_trades(instrument_id))
        .unwrap();
    wait_for_unsubscribe(&ws_state, channel).await;
    client.disconnect().await.unwrap();
}
#[rstest]
#[tokio::test]
async fn test_request_trades_paginates_with_constant_page_size() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![
        trade_page(3, 10, "p1"),
        trade_page(3, 10, "p2"),
        trade_page(3, 10, "p3"),
    ];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(
            InstrumentId::from("ETH-PERP.DERIVE"),
            Some(25),
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };
    assert_eq!(trades.data.len(), 25);

    let calls = rest_state.trade_history_calls().await;
    assert_eq!(calls.len(), 3, "must walk all three pages to reach 25");

    let pages: Vec<u64> = calls
        .iter()
        .map(|body| body.get("page").and_then(Value::as_u64).unwrap())
        .collect();
    assert_eq!(pages, vec![1, 2, 3]);

    let page_sizes: Vec<u64> = calls
        .iter()
        .map(|body| body.get("page_size").and_then(Value::as_u64).unwrap())
        .collect();
    assert!(
        page_sizes.windows(2).all(|w| w[0] == w[1]),
        "page_size must be constant across paginated calls, was {page_sizes:?}",
    );
    assert_eq!(page_sizes[0], 25, "page_size should equal capped limit");
}

#[rstest]
#[tokio::test]
async fn test_request_trades_returns_newest_unique_records_in_chronological_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![
        json!({
            "trades": [
                page_trade_at("newest", 1_700_000_000_300),
                page_trade_at("duplicate", 1_700_000_000_200),
                page_trade_at("duplicate", 1_700_000_000_200),
            ],
            "pagination": {"count": 5, "num_pages": 2},
        }),
        json!({
            "trades": [
                page_trade_at("duplicate", 1_700_000_000_200),
                page_trade_at("older", 1_700_000_000_100),
                page_trade_at("oldest", 1_700_000_000_000),
            ],
            "pagination": {"count": 6, "num_pages": 2},
        }),
    ];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(
            InstrumentId::from("ETH-PERP.DERIVE"),
            Some(3),
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };
    let trade_ids: Vec<&str> = trades
        .data
        .iter()
        .map(|trade| trade.trade_id.as_str())
        .collect();
    let calls = rest_state.trade_history_calls().await;
    let to_timestamps: Vec<i64> = calls
        .iter()
        .map(|body| body["to_timestamp"].as_i64().expect("to_timestamp"))
        .collect();

    assert_eq!(trade_ids, vec!["older", "duplicate", "newest"]);
    assert_eq!(calls.len(), 2, "duplicates must not count toward the limit");
    assert!(
        to_timestamps
            .windows(2)
            .all(|window| window[0] == window[1]),
        "pagination must use one fixed end bound: {to_timestamps:?}",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_trades_normalizes_paired_maker_first_rows_to_single_taker_tick() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![load_json(
        "perps/http_public_trades_result_eth_paired_maker_first.json",
    )];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(InstrumentId::from("ETH-PERP.DERIVE"), None))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };

    assert_eq!(trades.data.len(), 1, "paired rows must emit one tick");
    let tick = &trades.data[0];
    assert_eq!(tick.aggressor_side, AggressorSide::Buy);
    assert_eq!(tick.trade_id, TradeId::from("pub-pair-1"));
    assert_eq!(tick.price, Price::from("3500.00"));
    assert_eq!(tick.size, Quantity::from("0.250"));
    assert_eq!(tick.ts_event, UnixNanos::from(1_700_000_000_000_000_000));
}

#[rstest]
#[tokio::test]
async fn test_request_trades_normalizes_paired_taker_first_rows_to_identical_tick() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![load_json(
        "perps/http_public_trades_result_eth_paired_taker_first.json",
    )];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(InstrumentId::from("ETH-PERP.DERIVE"), None))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };

    assert_eq!(trades.data.len(), 1, "paired rows must emit one tick");
    let tick = &trades.data[0];
    assert_eq!(tick.aggressor_side, AggressorSide::Buy);
    assert_eq!(tick.trade_id, TradeId::from("pub-pair-1"));
    assert_eq!(tick.price, Price::from("3500.00"));
    assert_eq!(tick.size, Quantity::from("0.250"));
    assert_eq!(tick.ts_event, UnixNanos::from(1_700_000_000_000_000_000));
}

#[rstest]
#[tokio::test]
async fn test_request_trades_terminates_on_num_pages() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![trade_page(1, 5, "single")];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(InstrumentId::from("ETH-PERP.DERIVE"), None))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };
    assert_eq!(trades.data.len(), 5);
    assert_eq!(rest_state.trade_history_calls().await.len(), 1);
}

#[rstest]
#[tokio::test]
async fn test_request_trades_terminates_on_empty_page() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Declare 5 pages but page 2 returns empty. Loop must break without panicking
    // and emit the page-1 trades.
    *rest_state.trade_history_pages.lock().await = vec![
        trade_page(5, 4, "p1"),
        json!({"trades": [], "pagination": {"count": 4, "num_pages": 5}}),
    ];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_trades(request_trades(InstrumentId::from("ETH-PERP.DERIVE"), None))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Trades(trades) = response else {
        panic!("expected trades response");
    };
    assert_eq!(trades.data.len(), 4);
    assert_eq!(rest_state.trade_history_calls().await.len(), 2);
}

#[rstest]
#[tokio::test]
async fn test_request_trades_returns_err_for_missing_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();

    let err = client
        .request_trades(request_trades(InstrumentId::from("ETH-PERP.DERIVE"), None))
        .expect_err("must reject missing instrument");
    assert!(err.to_string().contains("not found in cache"), "{err}",);
}

#[rstest]
#[tokio::test]
async fn test_request_funding_rates_emits_response_with_records() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.funding_rate_history_response.lock().await =
        load_json("perps/http_public_funding_rate_history_eth.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_funding_rates(request_funding_rates(
            InstrumentId::from("ETH-PERP.DERIVE"),
            None,
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::FundingRates(rates) = response else {
        panic!("expected funding rates response");
    };
    assert_eq!(rates.data.len(), 3);
    assert_eq!(rates.data[0].rate.to_string(), "0.00012");
    assert_eq!(
        rates.data[0].ts_event,
        UnixNanos::from(1_700_000_000_000_000_000)
    );
    assert_eq!(rates.data[2].rate.to_string(), "0.00011");
    assert_eq!(
        rates.data[2].ts_event,
        UnixNanos::from(1_700_007_200_000_000_000)
    );

    let calls = rest_state.funding_rate_history_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["instrument_name"], "ETH-PERP");
}

#[rstest]
#[tokio::test]
async fn test_request_funding_rates_returns_newest_limit_in_chronological_order() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut response = load_json("perps/http_public_funding_rate_history_eth.json");
    response["funding_rate_history"]
        .as_array_mut()
        .unwrap()
        .reverse();
    response["funding_rate_history"]
        .as_array_mut()
        .unwrap()
        .insert(
            0,
            json!({
                "funding_rate": "0.00013",
                "timestamp": -1,
            }),
        );
    *rest_state.funding_rate_history_response.lock().await = response;
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    client
        .request_funding_rates(request_funding_rates(
            InstrumentId::from("ETH-PERP.DERIVE"),
            Some(2),
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::FundingRates(rates) = response else {
        panic!("expected funding rates response");
    };
    let timestamps: Vec<UnixNanos> = rates.data.iter().map(|rate| rate.ts_event).collect();

    assert_eq!(
        timestamps,
        vec![
            UnixNanos::from(1_700_003_600_000_000_000),
            UnixNanos::from(1_700_007_200_000_000_000),
        ],
    );
}

#[rstest]
#[tokio::test]
async fn test_request_funding_rates_returns_err_for_non_perp() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;

    let err = client
        .request_funding_rates(request_funding_rates(
            InstrumentId::from("ETH-20260627-3500-C.DERIVE"),
            None,
        ))
        .expect_err("must reject non-perp instrument");
    assert!(
        err.to_string()
            .contains("only available for Derive perpetual"),
        "{err}",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_funding_rates_returns_err_for_missing_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();

    let err = client
        .request_funding_rates(request_funding_rates(
            InstrumentId::from("ETH-PERP.DERIVE"),
            None,
        ))
        .expect_err("must reject missing instrument");
    assert!(err.to_string().contains("not found in cache"), "{err}",);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_emits_response_with_records() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.candles_response.lock().await = load_json("perps/http_public_candles_eth.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-15-MINUTE-LAST-EXTERNAL");

    client.request_bars(request_bars(bar_type, None)).unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };
    assert_eq!(bars.bar_type, bar_type);
    assert_eq!(bars.data.len(), 3);
    assert_eq!(bars.data[0].open, Price::from("3500.00"));
    assert_eq!(bars.data[0].high, Price::from("3501.50"));
    assert_eq!(bars.data[0].low, Price::from("3499.00"));
    assert_eq!(bars.data[0].close, Price::from("3501.00"));
    // Volume must match `volume_contracts` (3.527), not `volume_usd` (12345.6)
    assert_eq!(bars.data[0].volume, Quantity::from("3.527"));
    assert_eq!(
        bars.data[0].ts_event,
        UnixNanos::from(1_700_000_900_000_000_000),
    );
    assert_eq!(
        bars.data[2].ts_event,
        UnixNanos::from(1_700_002_700_000_000_000),
    );

    let calls = rest_state.candles_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["instrument_name"], "ETH-PERP");
    // Derive's `period` enum is bucket size in seconds: 15 MINUTE -> 900
    assert_eq!(calls[0]["period"], 900);

    // Default window when start/end are None: end_ts = now, start_ts =
    // end_ts - period * DERIVE_CANDLES_DEFAULT_LIMIT. Pin the span so any
    // mutation of the default_span math is caught.
    let start_ts = calls[0]["start_timestamp"].as_i64().unwrap();
    let end_ts = calls[0]["end_timestamp"].as_i64().unwrap();
    assert!(start_ts < end_ts, "start_ts={start_ts} end_ts={end_ts}");
    assert_eq!(end_ts - start_ts, 900 * 1000);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_excludes_forming_bucket() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let period = 60;
    let now_secs = Timestamp::now().as_second();
    *rest_state.candles_response.lock().await =
        Value::Array(vec![candle_json(now_secs - period), candle_json(now_secs)]);
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    client.request_bars(request_bars(bar_type, None)).unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };

    assert_eq!(bars.data.len(), 1);
    assert_eq!(
        bars.data[0].ts_event,
        UnixNanos::from((now_secs as u64) * 1_000_000_000),
    );
}

#[rstest]
#[tokio::test]
async fn test_request_bars_honors_limit() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.candles_response.lock().await = load_json("perps/http_public_candles_eth.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-15-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(request_bars(bar_type, Some(2)))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };
    assert_eq!(bars.data.len(), 2);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_returns_err_for_missing_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    let err = client
        .request_bars(request_bars(bar_type, None))
        .expect_err("must reject missing instrument");
    assert!(err.to_string().contains("not found in cache"), "{err}",);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_returns_err_for_non_external_source() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-INTERNAL");

    let err = client
        .request_bars(request_bars(bar_type, None))
        .expect_err("must reject INTERNAL aggregation source");
    assert!(
        err.to_string().contains("EXTERNAL aggregation source"),
        "{err}",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_bars_returns_err_for_non_last_price_type() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-MID-EXTERNAL");

    let err = client
        .request_bars(request_bars(bar_type, None))
        .expect_err("must reject non-LAST price type");
    assert!(err.to_string().contains("PriceType::Last"), "{err}",);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_returns_err_for_unsupported_spec() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    // Derive's enum tops out at WEEK1; SECOND-aggregated bars are unsupported
    let bar_type = BarType::from("ETH-PERP.DERIVE-30-SECOND-LAST-EXTERNAL");

    let err = client
        .request_bars(request_bars(bar_type, None))
        .expect_err("must reject unsupported bar spec");
    assert!(
        err.to_string().contains("unsupported Derive bar spec"),
        "{err}",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_bars_drops_response_on_rest_error() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    // Configure the mock so `get_candles` returns an Err. The production
    // contract under test is "any REST error must drop the BarsResponse",
    // independent of which DeriveHttpError variant the handler surfaced.
    *rest_state.candles_response.lock().await = json!({
        "id": 1,
        "error": {"code": -32602, "message": "boom"},
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-15-MINUTE-LAST-EXTERNAL");

    client.request_bars(request_bars(bar_type, None)).unwrap();

    // Wait until the REST call lands on the mock so the test is robust
    // against scheduling delays between runtimes.
    {
        let state = rest_state.clone();
        wait_until_async(
            move || {
                let state = state.clone();
                async move { !state.candles_calls.lock().await.is_empty() }
            },
            Duration::from_secs(5),
        )
        .await;
    }

    // After the failed REST call, the handler logs and returns without
    // emitting a BarsResponse. Verify nothing arrives within a window
    // generous enough to survive scheduling delays on slow CI runners.
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
    if let Ok(Some(DataEvent::Response(DataResponse::Bars(_)))) = received {
        panic!("BarsResponse must be dropped when get_candles fails");
    }
}

#[rstest]
#[tokio::test]
async fn test_request_bars_walks_multiple_pages_to_start() {
    // Three pages of three 1-minute bars; the mock keys responses by the
    // request's `end_timestamp` so the test fails fast if the backwards-walk
    // math changes (next end_ts must equal earliest_bucket - 1).
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let period: i64 = 60;
    let start_secs: i64 = 1_700_000_000;
    let end_secs: i64 = start_secs + period * 8;

    {
        let mut pages = rest_state.candles_pages.lock().await;
        pages.insert(
            end_secs,
            Value::Array(vec![
                candle_json(start_secs + period * 6),
                candle_json(start_secs + period * 7),
                candle_json(start_secs + period * 8),
            ]),
        );
        pages.insert(
            start_secs + period * 6 - 1,
            Value::Array(vec![
                candle_json(start_secs + period * 3),
                candle_json(start_secs + period * 4),
                candle_json(start_secs + period * 5),
            ]),
        );
        pages.insert(
            start_secs + period * 3 - 1,
            Value::Array(vec![
                candle_json(start_secs),
                candle_json(start_secs + period),
                candle_json(start_secs + period * 2),
            ]),
        );
    }

    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(request_bars_window(
            bar_type,
            Some(datetime_from_secs(start_secs)),
            Some(datetime_from_secs(end_secs)),
            None,
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };

    assert_eq!(bars.data.len(), 9);

    for (i, bar) in bars.data.iter().enumerate() {
        let expected_secs = start_secs + period * (i as i64 + 1);

        assert_eq!(
            bar.ts_event,
            UnixNanos::from((expected_secs as u64) * 1_000_000_000),
            "bar {i} ts_event mismatch",
        );
    }

    let calls = rest_state.candles_calls().await;
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0]["end_timestamp"], end_secs);
    assert_eq!(calls[1]["end_timestamp"], start_secs + period * 6 - 1);
    assert_eq!(calls[2]["end_timestamp"], start_secs + period * 3 - 1);
    assert!(
        calls.iter().all(|c| c["start_timestamp"] == start_secs),
        "start_timestamp must remain pinned across pages",
    );
}

#[rstest]
#[tokio::test]
async fn test_request_bars_honors_limit_across_pages() {
    // limit=5 must cap the walk: page 1 yields 3, page 2 yields 3, total
    // 6 >= 5 triggers loop exit, then the leading bar is dropped so only
    // the 5 most recent survive.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let period: i64 = 60;
    let start_secs: i64 = 1_700_000_000;
    let end_secs: i64 = start_secs + period * 8;

    {
        let mut pages = rest_state.candles_pages.lock().await;
        pages.insert(
            end_secs,
            Value::Array(vec![
                candle_json(start_secs + period * 6),
                candle_json(start_secs + period * 7),
                candle_json(start_secs + period * 8),
            ]),
        );
        pages.insert(
            start_secs + period * 6 - 1,
            Value::Array(vec![
                candle_json(start_secs + period * 3),
                candle_json(start_secs + period * 4),
                candle_json(start_secs + period * 5),
            ]),
        );
        pages.insert(
            start_secs + period * 3 - 1,
            Value::Array(vec![
                candle_json(start_secs),
                candle_json(start_secs + period),
                candle_json(start_secs + period * 2),
            ]),
        );
    }

    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(request_bars_window(
            bar_type,
            Some(datetime_from_secs(start_secs)),
            Some(datetime_from_secs(end_secs)),
            Some(5),
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };

    assert_eq!(bars.data.len(), 5);
    // Most recent 5 bars survive after dropping the oldest from page 2.
    assert_eq!(
        bars.data[0].ts_event,
        UnixNanos::from(((start_secs + period * 5) as u64) * 1_000_000_000),
    );
    assert_eq!(
        bars.data[4].ts_event,
        UnixNanos::from(((start_secs + period * 9) as u64) * 1_000_000_000),
    );

    let calls = rest_state.candles_calls().await;
    assert_eq!(calls.len(), 2);
}

#[rstest]
#[tokio::test]
async fn test_request_bars_dedups_overlapping_pages() {
    // The venue is contractually expected to return non-overlapping windows,
    // but the dedup AHashSet defends against any future regression. Page 2
    // deliberately repeats page 1's earliest bucket.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let period: i64 = 60;
    let start_secs: i64 = 1_700_000_000;
    let end_secs: i64 = start_secs + period * 5;

    {
        let mut pages = rest_state.candles_pages.lock().await;
        pages.insert(
            end_secs,
            Value::Array(vec![
                candle_json(start_secs + period * 3),
                candle_json(start_secs + period * 4),
                candle_json(start_secs + period * 5),
            ]),
        );
        pages.insert(
            start_secs + period * 3 - 1,
            Value::Array(vec![
                candle_json(start_secs),
                candle_json(start_secs + period),
                candle_json(start_secs + period * 2),
                // Overlap with page 1's earliest bucket; must be deduped.
                candle_json(start_secs + period * 3),
            ]),
        );
    }

    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(request_bars_window(
            bar_type,
            Some(datetime_from_secs(start_secs)),
            Some(datetime_from_secs(end_secs)),
            None,
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };

    assert_eq!(bars.data.len(), 6, "dedup must drop the repeated bucket");

    let mut seen: HashSet<UnixNanos> = HashSet::new();

    for bar in &bars.data {
        assert!(
            seen.insert(bar.ts_event),
            "duplicate ts_event {:?}",
            bar.ts_event
        );
    }
}

#[rstest]
#[tokio::test]
async fn test_request_bars_terminates_at_safety_cap() {
    // The synthetic mock returns one bar per call with timestamp = end_ts,
    // so each iteration advances `current_end` by exactly `period`. With
    // start_ts far enough in the past, the walk would run forever without
    // the safety cap; assert it stops at 100 pages.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let period: i64 = 60;
    let end_secs: i64 = 1_700_000_000 + period * 200;
    let start_secs: i64 = 1_700_000_000;
    *rest_state.candles_generated_per_call.lock().await = Some(1);

    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let bar_type = BarType::from("ETH-PERP.DERIVE-1-MINUTE-LAST-EXTERNAL");

    client
        .request_bars(request_bars_window(
            bar_type,
            Some(datetime_from_secs(start_secs)),
            Some(datetime_from_secs(end_secs)),
            None,
        ))
        .unwrap();

    // The 100-page walk is paced by the non-matching REST quota (5/s after
    // the 25-request burst), so it takes ~15s; allow well beyond that.
    let response = recv_response_within(&mut rx, Duration::from_secs(25)).await;
    let DataResponse::Bars(bars) = response else {
        panic!("expected bars response");
    };

    assert_eq!(bars.data.len(), 100);
    let calls = rest_state.candles_calls().await;
    assert_eq!(calls.len(), 100);
}

#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_emits_price() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = load_json("options/http_ticker_eth_snapshot.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");

    let clock = get_atomic_clock_realtime();
    let before_ns = clock.get_time_ns();
    client
        .request_option_chain_reference_price(request_option_chain_reference_price(instrument_id))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let after_ns = clock.get_time_ns();
    let DataResponse::OptionChainReferencePrice(reference) = response else {
        panic!("expected option-chain reference price response");
    };
    assert_eq!(reference.series_id.venue, *DERIVE_VENUE);
    assert_eq!(reference.series_id.underlying, Ustr::from("ETH"));
    assert_eq!(reference.series_id.settlement_currency, Ustr::from("USDC"));
    assert_eq!(reference.price, Some(Price::from("3505")));
    assert!(
        reference.ts_init >= before_ns && reference.ts_init <= after_ns,
        "ts_init must come from the local realtime clock"
    );

    let calls = rest_state.ticker_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["instrument_type"], "option");
    assert_eq!(calls[0]["currency"], "ETH");
    assert_eq!(calls[0]["expiry_date"], "20260627");
}

#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_rejects_non_option() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let perp_id = InstrumentId::from("ETH-PERP.DERIVE");

    let err = client
        .request_option_chain_reference_price(request_option_chain_reference_price(perp_id))
        .expect_err("must reject non-option instrument");
    assert!(
        err.to_string().contains("require an option instrument"),
        "{err}",
    );
}

#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_is_none_on_rest_error() {
    // The engine waits for this response before creating the OptionChainManager.
    // On REST failure we must still emit an (empty) OptionChainReferencePriceResponse so the
    // engine can fall back to live-tick bootstrap.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = json!({
        "id": 1,
        "error": {"code": -32602, "message": "boom"},
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");

    client
        .request_option_chain_reference_price(request_option_chain_reference_price(instrument_id))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::OptionChainReferencePrice(reference) = response else {
        panic!("expected option-chain reference price response");
    };
    assert_eq!(reference.series_id.venue, *DERIVE_VENUE);
    assert_eq!(reference.price, None);
}

#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_is_none_without_option_pricing() {
    // Non-option ticker (perp snapshot fixture) has no option_pricing. The
    // engine must still get a response to unblock the OptionChainManager.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = load_json("perps/http_ticker_eth_snapshot.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");

    client
        .request_option_chain_reference_price(request_option_chain_reference_price(instrument_id))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::OptionChainReferencePrice(reference) = response else {
        panic!("expected option-chain reference price response");
    };
    assert_eq!(reference.price, None);
}

#[rstest]
#[case::zero("0")]
#[case::negative("-1")]
#[case::overflow("79228162514264337593543950335")]
#[tokio::test]
async fn test_option_chain_reference_price_rejects_invalid_value(#[case] value: &str) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let mut ticker = load_json("options/http_ticker_eth_snapshot.json");
    ticker["option_pricing"]["forward_price"] = json!(value);
    *rest_state.ticker_response.lock().await = ticker;
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    let request = request_option_chain_reference_price(instrument_id);
    let request_id = request.request_id;
    let series_id = request.series_id;

    client
        .request_option_chain_reference_price(request)
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::OptionChainReferencePrice(reference) = response else {
        panic!("expected option-chain reference price response");
    };
    assert_eq!(reference.correlation_id, request_id);
    assert_eq!(reference.series_id, series_id);
    assert_eq!(reference.price, None);
}

#[rstest]
#[tokio::test]
async fn test_option_chain_reference_price_rejects_missing_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");

    let err = client
        .request_option_chain_reference_price(request_option_chain_reference_price(instrument_id))
        .expect_err("must reject missing instrument");
    assert!(err.to_string().contains("not found in cache"), "{err}",);
}

#[rstest]
#[tokio::test]
async fn test_data_client_connect_disconnect() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);
    let (system_tx, mut system_rx) = tokio::sync::mpsc::unbounded_channel();
    replace_system_event_sender(system_tx);

    let registry = SocketReconnectRegistry::default();
    let mut client = registry
        .scope(|| DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)))
        .unwrap();
    assert!(!client.is_connected());

    client.connect().await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), system_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let SystemEvent::SocketState(change) = event;
    let endpoint = Ustr::from("derive-data-streams");
    let handle = registry.handle(*DERIVE_CLIENT_ID, endpoint).unwrap();

    assert!(client.is_connected());
    assert_eq!(change.client_id, *DERIVE_CLIENT_ID);
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

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    assert!(registry.handle(*DERIVE_CLIENT_ID, endpoint).is_none());
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_reconnect_after_disconnect_resumes_stream() {
    // After disconnect() cancels the token, a fresh connect() must refresh
    // it. Otherwise the new WS consumption loop spawns onto a cancelled
    // token and exits immediately: outbound subscribe frames still go out
    // (they bypass the consumption loop), but no inbound data reaches the
    // data_sender. This test pins both halves of the lifecycle by waiting
    // for a Data::Trade event on the reconnected session, which only
    // arrives if the consumption loop is alive.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());

    while rx.try_recv().is_ok() {}

    client.connect().await.unwrap();
    assert!(client.is_connected());

    while rx.try_recv().is_ok() {}

    let option_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();
    wait_for_subscribe(&ws_state, "trades.option.ETH").await;

    // The mock pushes a synthetic trade notification on every subscribe.
    // If the consumption loop is dead (regressed token-refresh fix), the
    // notification is silently dropped and recv_data times out.
    match recv_data(&mut rx).await {
        Data::Trade(trade) => assert_eq!(trade.instrument_id, option_id),
        other => panic!("expected trade data, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_disconnect_stops_event_flow() {
    // disconnect() must abort tracked request tasks so no late responses
    // arrive after the call returns. Use request_trades because it pages
    // and would otherwise keep firing after the WS is torn down.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.trade_history_pages.lock().await = vec![trade_page(3, 10, "p1")];
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_trades(request_trades(instrument_id, None))
        .unwrap();

    // Tear down before the request can possibly drain all 3 pages
    let mut client = client;
    let disconnect_deadline = Duration::from_secs(5);
    tokio::time::timeout(disconnect_deadline, client.disconnect())
        .await
        .expect("disconnect must complete promptly even with in-flight requests")
        .unwrap();
    assert!(!client.is_connected());

    while rx.try_recv().is_ok() {}

    let quiet_window = Duration::from_millis(500);
    let maybe_event = tokio::time::timeout(quiet_window, rx.recv()).await;
    assert!(
        maybe_event.is_err(),
        "no data events should arrive after disconnect, was: {maybe_event:?}",
    );
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_reset_after_subscribe_clears_state() {
    // reset() on a live client with the WS stream up and a tracked
    // subscribe task must succeed without panic, and the client must
    // accept a fresh connect() that resumes data flow. Exercises the
    // `abort_pending_tasks()` and `ws_stream_handle.take().abort()`
    // branches added in reset(), plus the connect()-time WS teardown
    // that rebuilds out_rx after a reset.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let option_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();

    client.reset().unwrap();
    assert!(!client.is_connected());

    while rx.try_recv().is_ok() {}

    client.connect().await.unwrap();
    assert!(client.is_connected());

    while rx.try_recv().is_ok() {}

    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();
    wait_for_subscribe(&ws_state, "trades.option.ETH").await;

    match recv_data(&mut rx).await {
        Data::Trade(trade) => assert_eq!(trade.instrument_id, option_id),
        other => panic!("expected trade data, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn test_data_client_stop_then_connect_clears_stale_subscriptions() {
    // Regression: stop() cancels the token without clearing local sub state,
    // so a follow-up connect() must clear the active_* maps when it tears
    // down the WS client. Otherwise a resubscribe hits the early-return on
    // the stale entry and no new WS frame is sent on the fresh socket.
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let mut client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let option_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();
    wait_for_subscribe(&ws_state, "trades.option.ETH").await;

    client.stop().unwrap();
    assert!(!client.is_connected());

    while rx.try_recv().is_ok() {}
    ws_state.subscribe_frames.lock().await.clear();

    client.connect().await.unwrap();
    assert!(client.is_connected());

    while rx.try_recv().is_ok() {}

    // A fresh subscribe must reach the WS server, proving the active_*
    // entry from before stop() did not silently suppress the new call.
    client
        .subscribe_trades(subscribe_trades(option_id))
        .unwrap();
    wait_for_subscribe(&ws_state, "trades.option.ETH").await;

    match recv_data(&mut rx).await {
        Data::Trade(trade) => assert_eq!(trade.instrument_id, option_id),
        other => panic!("expected trade data, was {other:?}"),
    }

    client.disconnect().await.unwrap();
}

fn request_instrument(instrument_id: InstrumentId) -> RequestInstrument {
    RequestInstrument::new(
        instrument_id,
        None,
        None,
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_quotes(instrument_id: InstrumentId) -> RequestQuotes {
    RequestQuotes::new(
        instrument_id,
        None,
        None,
        None,
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

fn request_quotes_window(
    instrument_id: InstrumentId,
    start: Option<Timestamp>,
    end: Option<Timestamp>,
) -> RequestQuotes {
    RequestQuotes::new(
        instrument_id,
        start,
        end,
        None,
        Some(*DERIVE_CLIENT_ID),
        UUID4::new(),
        UnixNanos::default(),
        None,
    )
}

#[rstest]
#[tokio::test]
async fn test_request_instrument_emits_response_and_caches() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let snapshot = load_json("perps/http_get_instrument_eth.json");
    let payload = snapshot
        .get("result")
        .cloned()
        .expect("fixture missing `result`");
    *rest_state.instrument_response.lock().await = payload;
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_instrument(request_instrument(instrument_id))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Instrument(boxed) = response else {
        panic!("expected instrument response");
    };
    assert_eq!(boxed.instrument_id, instrument_id);
    assert_eq!(boxed.data.id(), instrument_id);
    assert_eq!(boxed.data.price_precision(), 2);
    assert_eq!(boxed.data.size_precision(), 3);
    assert_eq!(boxed.data.price_increment(), Price::from("0.01"));
    assert_eq!(boxed.data.size_increment(), Quantity::from("0.001"));

    let calls = rest_state.instrument_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["instrument_name"], "ETH-PERP");

    // request_quotes returns Err when the instrument is not in the cache, so
    // accepting the call proves the handler populated `self.instruments`.
    client
        .request_quotes(request_quotes(instrument_id))
        .expect("instrument must be cached after request_instrument success");
}

#[rstest]
#[tokio::test]
async fn test_request_instrument_drops_response_on_rest_error() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.instrument_response.lock().await = json!({
        "id": 1,
        "error": {"code": -32602, "message": "boom"},
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_instrument(request_instrument(instrument_id))
        .unwrap();

    {
        let state = rest_state.clone();
        wait_until_async(
            move || {
                let state = state.clone();
                async move { !state.instrument_calls.lock().await.is_empty() }
            },
            Duration::from_secs(5),
        )
        .await;
    }

    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await;
    if let Ok(Some(DataEvent::Response(DataResponse::Instrument(_)))) = received {
        panic!("InstrumentResponse must be dropped when get_instrument fails");
    }
}

#[rstest]
#[tokio::test]
async fn test_request_instrument_returns_err_for_non_derive_venue() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();
    let foreign_id = InstrumentId::from("ETH-PERP.OTHER");

    let err = client
        .request_instrument(request_instrument(foreign_id))
        .expect_err("must reject non-Derive venue");
    assert!(err.to_string().contains("not for venue"), "{err}");
}

#[rstest]
#[tokio::test]
async fn test_request_quotes_emits_response_with_single_tick() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = load_json("perps/http_ticker_eth_snapshot.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_quotes(request_quotes(instrument_id))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Quotes(quotes) = response else {
        panic!("expected quotes response");
    };
    assert_eq!(quotes.instrument_id, instrument_id);
    assert_eq!(quotes.data.len(), 1);
    assert_eq!(quotes.data[0].bid_price, Price::from("3499.50"));
    assert_eq!(quotes.data[0].ask_price, Price::from("3501.00"));
    assert_eq!(quotes.data[0].bid_size, Quantity::from("0.800"));
    assert_eq!(quotes.data[0].ask_size, Quantity::from("1.200"));

    let calls = rest_state.ticker_calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["instrument_type"], "perp");
    assert_eq!(calls[0]["currency"], "ETH");
    assert!(calls[0].get("expiry_date").is_none());
}

#[rstest]
#[tokio::test]
async fn test_request_quotes_drops_response_on_rest_error() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = json!({
        "id": 1,
        "error": {"code": -32602, "message": "boom"},
    });
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    // Drain connect-time Instrument events so the quiet-window check below
    // cannot consume one in place of a missing QuotesResponse.
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_quotes(request_quotes(instrument_id))
        .unwrap();

    {
        let state = rest_state.clone();
        wait_until_async(
            move || {
                let state = state.clone();
                async move { !state.ticker_calls.lock().await.is_empty() }
            },
            Duration::from_secs(5),
        )
        .await;
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(DataEvent::Response(DataResponse::Quotes(_)))) => {
                panic!("QuotesResponse must be dropped when get_ticker fails")
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => break,
        }
    }
}

// Fixture `http_ticker_eth_perp_snapshot.json` has `timestamp = 1_700_000_000_000`
// ms; offsets below are seconds-since-epoch and snapshot_secs is 1_700_000_000.
#[rstest]
#[case::unbounded(None, None, 1)]
#[case::straddle(Some(1_699_999_999), Some(1_700_000_001), 1)]
#[case::inclusive_lower(Some(1_700_000_000), None, 1)]
#[case::inclusive_upper(None, Some(1_700_000_000), 1)]
#[case::start_after(Some(1_700_000_001), None, 0)]
#[case::end_before(None, Some(1_699_999_999), 0)]
#[case::both_in_past(Some(1_699_000_000), Some(1_699_999_999), 0)]
#[case::both_in_future(Some(1_700_000_001), Some(1_700_001_000), 0)]
#[tokio::test]
async fn test_request_quotes_window_filter(
    #[case] start_secs: Option<i64>,
    #[case] end_secs: Option<i64>,
    #[case] expected_count: usize,
) {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    *rest_state.ticker_response.lock().await = load_json("perps/http_ticker_eth_snapshot.json");
    let rest_addr = start_rest_server(rest_state.clone()).await;
    let ws_addr = start_ws_server(ws_state.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = connect_with_eth_currency(rest_addr, ws_addr).await;
    let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");

    client
        .request_quotes(request_quotes_window(
            instrument_id,
            start_secs.map(datetime_from_secs),
            end_secs.map(datetime_from_secs),
        ))
        .unwrap();

    let response = recv_response(&mut rx).await;
    let DataResponse::Quotes(quotes) = response else {
        panic!("expected quotes response");
    };
    assert_eq!(quotes.data.len(), expected_count);
}

#[rstest]
#[tokio::test]
async fn test_request_quotes_returns_err_for_missing_instrument() {
    let rest_state = RestState::default();
    let ws_state = WsState::default();
    let rest_addr = start_rest_server(rest_state).await;
    let ws_addr = start_ws_server(ws_state).await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    replace_data_event_sender(tx);

    let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config(rest_addr, ws_addr)).unwrap();

    let err = client
        .request_quotes(request_quotes(InstrumentId::from("ETH-PERP.DERIVE")))
        .expect_err("must reject missing instrument");
    assert!(err.to_string().contains("not found in cache"), "{err}");
}
