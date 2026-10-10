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

//! Live market data client implementation for the Derive adapter.

use std::{
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use dashmap::DashMap;
use nautilus_common::{
    cache::{InstrumentLookupError, quote::QuoteCache},
    clients::DataClient,
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, FundingRatesResponse, InstrumentResponse,
            InstrumentsResponse, OptionChainReferencePriceResponse, QuotesResponse, RequestBars,
            RequestFundingRates, RequestInstrument, RequestInstruments,
            RequestOptionChainReferencePrice, RequestQuotes, RequestTrades, SubscribeBookDeltas,
            SubscribeBookDepth, SubscribeFundingRates, SubscribeIndexPrices, SubscribeMarkPrices,
            SubscribeOptionGreeks, SubscribeQuotes, SubscribeTrades, TradesResponse,
            UnsubscribeBookDeltas, UnsubscribeBookDepth, UnsubscribeFundingRates,
            UnsubscribeIndexPrices, UnsubscribeMarkPrices, UnsubscribeOptionGreeks,
            UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
    providers::InstrumentProvider,
};
use nautilus_core::{
    AtomicMap, AtomicSet, Params, UnixNanos,
    datetime::{NANOSECONDS_IN_SECOND, datetime_to_unix_nanos},
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    SocketControl,
    task::{TaskGroup, TaskGroupGuard},
};
use nautilus_model::{
    data::{Bar, Data, QuoteTick},
    enums::{AggregationSource, BookType, PriceType},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::{
            DERIVE_CANDLES_DEFAULT_LIMIT, DERIVE_CANDLES_MAX_PAGES, DERIVE_TRADES_PAGE_SIZE,
            DERIVE_VENUE, RECONNECT_BACKOFF_FACTOR, RECONNECT_BASE_BACKOFF, RECONNECT_MAX_BACKOFF,
        },
        enums::{
            DeriveInstrumentType, DeriveOrderbookDepth, DeriveOrderbookGroup, DeriveTickerInterval,
        },
        parse::{format_instrument_id, format_venue_symbol, parse_derive_instrument_any},
        rate_limit::DERIVE_NON_MATCHING_TPS,
    },
    config::DeriveDataClientConfig,
    http::DeriveHttpClient,
    providers::{
        DeriveInstrumentProvider, fetch_instrument_definitions, parse_instrument_definitions,
    },
    websocket::client::DeriveWebSocketShutdownHandle,
    websocket::{
        DEFAULT_ORDERBOOK_DEPTH, DEFAULT_ORDERBOOK_GROUP, DEFAULT_TICKER_INTERVAL,
        DerivePublicWsData, DeriveTickerMsg, DeriveWebSocketClient,
        DeriveWebSocketSubscriptionHandle, DeriveWsError, DeriveWsMessage, QuoteFeedState,
        SessionEpoch, WsMessageContext, bar_spec_to_derive_period, orderbook_channel,
        parse_candle_record, parse_funding_rate, parse_funding_rate_history_record,
        parse_index_price, parse_mark_price, parse_option_greeks, parse_orderbook_deltas,
        parse_orderbook_depth, parse_public_ws_data, parse_ticker_quote,
        parse_ticker_quote_from_rest, parse_trade_tick, parse_trade_tick_from_rest, ticker_channel,
        trades_channel,
    },
};

/// Derive live data client.
#[derive(Debug)]
pub struct DeriveDataClient {
    client_id: ClientId,
    config: DeriveDataClientConfig,
    http_client: DeriveHttpClient,
    provider: DeriveInstrumentProvider,
    ws_client: Arc<tokio::sync::Mutex<DeriveWebSocketClient>>,
    ws_handle: DeriveWebSocketSubscriptionHandle,
    ws_shutdown: DeriveWebSocketShutdownHandle,
    is_connected: Arc<AtomicBool>,
    /// Set while REST market-data failover is the active data path because the
    /// WebSocket is unavailable. Lets [`DataClient::is_connected`] report
    /// readiness for live-node startup without claiming the WebSocket itself
    /// is healthy; internal transport-health decisions (REST poll cadence,
    /// quote acceptance) continue to read `is_connected` directly.
    ///
    /// Latched for the whole outage (cleared only by `disconnect`/`stop`/
    /// `reset`), this also makes `connect()`'s top-of-function fast path
    /// ("already connected, return") apply for the entire time the spawned
    /// initial-reconnect retry task owns the WebSocket lifecycle, so a
    /// repeated `connect()` call never races that task for the WebSocket
    /// client's sole event receiver.
    rest_failover_active: Arc<AtomicBool>,
    ws_session_generation: Arc<AtomicU64>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    pending_tasks: TaskGroup,
    shutdown_errors: Vec<String>,
    data_sender: EventSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    active_book_delta_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_book_depth_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_ticker_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_quote_subs: Arc<AtomicSet<InstrumentId>>,
    quote_poll_subs: Arc<Mutex<AHashMap<InstrumentId, u64>>>,
    active_trade_subs: Arc<AtomicSet<InstrumentId>>,
    active_mark_subs: Arc<AtomicSet<InstrumentId>>,
    active_index_subs: Arc<AtomicSet<InstrumentId>>,
    active_funding_subs: Arc<AtomicSet<InstrumentId>>,
    active_greeks_subs: Arc<AtomicSet<InstrumentId>>,
    channel_subscriptions: Arc<ChannelSubscriptionRegistry>,
    subscription_lock: Arc<Mutex<()>>,
    quote_cache: Arc<Mutex<QuoteCache>>,
    quote_feed_state: Arc<Mutex<AHashMap<InstrumentId, QuoteFeedState>>>,
    clock: &'static AtomicTime,
}

impl DeriveDataClient {
    /// Creates a new [`DeriveDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be initialized.
    pub fn new(client_id: ClientId, config: DeriveDataClientConfig) -> anyhow::Result<Self> {
        config.validate()?;

        let clock = get_atomic_clock_realtime();
        let data_sender = get_data_event_sender();
        let proxy_url = config
            .proxy_url
            .as_ref()
            .map(|value| value.expose_secret().to_owned());
        let http_client = DeriveHttpClient::new(
            config.rest_url(),
            Some(config.http_timeout_secs),
            proxy_url.clone(),
            None,
        )?;
        let provider = DeriveInstrumentProvider::with_expired(
            http_client.clone(),
            config.currencies.clone(),
            config.include_expired,
        );
        let mut ws_client = DeriveWebSocketClient::new(
            Some(config.ws_url()),
            config.environment,
            config.transport_backend,
            proxy_url,
        )
        .with_socket_control(SocketControl::new(
            client_id,
            Some(*DERIVE_VENUE),
            "derive-data-streams",
        ));

        if let Some(secs) = config.ws_timeout_secs {
            ws_client.set_request_timeout(Duration::from_secs(secs));
        }

        let ws_handle = ws_client.subscription_handle();
        let ws_shutdown = ws_client.shutdown_handle();
        let session_tasks = TaskGroup::new();
        let pending_tasks = TaskGroup::new();

        Ok(Self {
            client_id,
            config,
            http_client,
            provider,
            ws_client: Arc::new(tokio::sync::Mutex::new(ws_client)),
            ws_handle,
            ws_shutdown,
            is_connected: Arc::new(AtomicBool::new(false)),
            rest_failover_active: Arc::new(AtomicBool::new(false)),
            ws_session_generation: Arc::new(AtomicU64::new(0)),
            cancellation_token: CancellationToken::new(),
            session_tasks,
            pending_tasks,
            shutdown_errors: Vec::new(),
            data_sender,
            instruments: Arc::new(AtomicMap::new()),
            active_book_delta_channels: Arc::new(AtomicMap::new()),
            active_book_depth_channels: Arc::new(AtomicMap::new()),
            active_ticker_channels: Arc::new(AtomicMap::new()),
            active_quote_subs: Arc::new(AtomicSet::new()),
            quote_poll_subs: Arc::new(Mutex::new(AHashMap::new())),
            active_trade_subs: Arc::new(AtomicSet::new()),
            active_mark_subs: Arc::new(AtomicSet::new()),
            active_index_subs: Arc::new(AtomicSet::new()),
            active_funding_subs: Arc::new(AtomicSet::new()),
            active_greeks_subs: Arc::new(AtomicSet::new()),
            channel_subscriptions: Arc::new(ChannelSubscriptionRegistry::default()),
            subscription_lock: Arc::new(Mutex::new(())),
            quote_cache: Arc::new(Mutex::new(QuoteCache::new())),
            quote_feed_state: Arc::new(Mutex::new(AHashMap::new())),
            clock,
        })
    }

    /// Spawns a fire-and-forget task, tracks its handle in `pending_tasks`
    /// for teardown abort, and logs failures with the task description.
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let future = async move {
            if let Err(e) = fut.await {
                log::warn!("{description} failed: {e:?}");
            }
        };

        if let Err(e) = self.pending_tasks.spawn(future) {
            log::warn!("Skipping Derive {description} after shutdown began: {e}");
        }
    }

    fn abort_pending_tasks(&self) {
        self.pending_tasks.begin_shutdown();
    }

    fn abort_session_tasks(&self) {
        self.session_tasks.begin_shutdown();
        self.ws_shutdown.begin_shutdown();
    }

    /// Clears every local subscription map. Called from `disconnect` and
    /// `reset` because the venue drops all subscriptions on socket close,
    /// and aborted in-flight subscribe tasks can leak entries staged before
    /// spawn (they never reach their on-error rollback branch).
    fn clear_subscription_state(&self) {
        let _guard = self.subscription_lock.lock();
        self.channel_subscriptions.clear();
        self.active_book_delta_channels.store(AHashMap::new());
        self.active_book_depth_channels.store(AHashMap::new());
        self.active_ticker_channels.store(AHashMap::new());
        self.active_quote_subs.store(AHashSet::new());
        self.active_trade_subs.store(AHashSet::new());
        self.active_mark_subs.store(AHashSet::new());
        self.active_index_subs.store(AHashSet::new());
        self.active_funding_subs.store(AHashSet::new());
        self.active_greeks_subs.store(AHashSet::new());
        self.quote_feed_state.lock().clear();
        self.quote_cache.lock().clear();
        self.quote_poll_subs.lock().clear();
    }

    async fn teardown_partial_connect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.abort_session_tasks();
        self.abort_pending_tasks();

        if let Err(e) = self.ws_client.lock().await.disconnect().await {
            self.shutdown_errors
                .push(format!("Derive WebSocket shutdown failed: {e}"));
        }
        let (session_result, pending_result) =
            tokio::join!(self.join_session_tasks(), self.join_pending_tasks());
        self.clear_subscription_state();
        self.channel_subscriptions.clear_transitions();
        self.is_connected.store(false, Ordering::Release);
        self.rest_failover_active.store(false, Ordering::Release);

        if let Err(e) = session_result {
            self.shutdown_errors.push(e.to_string());
        }

        if let Err(e) = pending_result {
            self.shutdown_errors.push(e.to_string());
        }

        if !self.shutdown_errors.is_empty() {
            anyhow::bail!(std::mem::take(&mut self.shutdown_errors).join("; "));
        }
        Ok(())
    }

    fn spawn_stream_task(
        &self,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<DeriveWsMessage>,
    ) -> anyhow::Result<()> {
        let ctx = self.ws_message_context();
        let cancellation = self.cancellation_token.clone();
        let is_connected = Arc::clone(&self.is_connected);

        self.session_tasks.spawn(async move {
            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                if matches!(&msg, DeriveWsMessage::SessionRecoveryFailed(_)) {
                                    is_connected.store(false, Ordering::Release);
                                } else if matches!(&msg, DeriveWsMessage::Reconnected) {
                                    is_connected.store(true, Ordering::Release);
                                }
                                Self::handle_ws_message(msg, &ctx);
                            }
                            None => {
                                log::debug!("Derive WebSocket data stream ended");
                                break;
                            }
                        }
                    }
                    () = cancellation.cancelled() => {
                        log::debug!("Derive WebSocket data stream task cancelled");
                        break;
                    }
                }
            }
        })?;

        Ok(())
    }

    fn ws_message_context(&self) -> WsMessageContext {
        WsMessageContext {
            clock: self.clock,
            data_sender: self.data_sender.clone(),
            instruments: Arc::clone(&self.instruments),
            active_book_delta_channels: Arc::clone(&self.active_book_delta_channels),
            active_book_depth_channels: Arc::clone(&self.active_book_depth_channels),
            active_ticker_channels: Arc::clone(&self.active_ticker_channels),
            active_quote_subs: Arc::clone(&self.active_quote_subs),
            active_trade_subs: Arc::clone(&self.active_trade_subs),
            active_mark_subs: Arc::clone(&self.active_mark_subs),
            active_index_subs: Arc::clone(&self.active_index_subs),
            active_funding_subs: Arc::clone(&self.active_funding_subs),
            active_greeks_subs: Arc::clone(&self.active_greeks_subs),
            subscription_lock: Arc::clone(&self.subscription_lock),
            quote_cache: Arc::clone(&self.quote_cache),
            quote_feed_state: Arc::clone(&self.quote_feed_state),
            ws_session_generation: Arc::clone(&self.ws_session_generation),
            ws_subscription_handle: self.ws_handle.clone(),
            // Derived from the enable flag, not merely from whether a bound
            // happens to be set: an operator who disables the flag while
            // leaving a previously configured bound in place must get
            // byte-for-byte WS-only behavior back, not failover-specific
            // confirmation/freshness/monotonicity dropping.
            max_quote_age_secs: self
                .config
                .rest_market_data_failover_enabled
                .then_some(self.config.rest_market_data_max_quote_age_secs)
                .flatten(),
        }
    }

    /// Invalidates all per-instrument quote feed state and the cached quote
    /// sides at a transport/session boundary.
    ///
    /// Shared by the in-place `Reconnected` handler and the initial retry
    /// owner's automatic full transport rebuild: both start a new
    /// transport/session identity that no prior confirmation, cached quote
    /// side or receipt-time evidence can still apply to. Without this, a
    /// stale `ws_subscription_confirmed = true` (or a cached bid/ask side)
    /// from before the boundary could otherwise survive into the new
    /// session purely because the per-transport epoch happens to restart
    /// at the same value the old transport used.
    fn invalidate_quote_feed_state(ctx: &WsMessageContext) {
        let _guard = ctx.subscription_lock.lock();
        ctx.ws_session_generation.fetch_add(1, Ordering::AcqRel);
        for state in ctx.quote_feed_state.lock().values_mut() {
            state.last_ws_received = None;
            state.ws_subscription_confirmed = false;
            state.ws_epoch = None;
        }
        ctx.quote_cache.lock().clear();
    }

    fn handle_ws_message(message: DeriveWsMessage, ctx: &WsMessageContext) {
        match message {
            DeriveWsMessage::Subscription(payload) => match parse_public_ws_data(&payload) {
                Ok(data) => Self::handle_public_ws_data(data, ctx),
                Err(e) => {
                    // Include channel and a truncated payload snippet so the
                    // wire shape that broke the parser is actionable from the
                    // log alone.
                    let snippet = truncated_payload_snippet(payload.data.get());
                    log::warn!(
                        "Failed to parse Derive public WS data on channel `{}`: {e}; payload: {snippet}",
                        payload.channel,
                    );
                }
            },
            DeriveWsMessage::Reconnected => {
                Self::invalidate_quote_feed_state(ctx);
                log::info!("Derive WebSocket reconnected");
            }
            DeriveWsMessage::SessionRecoveryFailed(reason) => {
                log::error!("Derive WebSocket session recovery failed: {reason}");
            }
            DeriveWsMessage::Authenticated => log::debug!("Derive WebSocket authenticated"),
        }
    }

    fn handle_public_ws_data(data: DerivePublicWsData, ctx: &WsMessageContext) {
        // Lifecycle mutation takes this lock before QuoteCache, so a feed
        // generation cannot change during cache mutation.
        let _guard = ctx.subscription_lock.lock();

        match data {
            DerivePublicWsData::Orderbook(msg) => {
                let instrument_id = msg.data.instrument_id();
                let channel = msg.channel.as_str();
                let deltas_active =
                    channel_is_active(&ctx.active_book_delta_channels, instrument_id, channel);
                let depth_active =
                    channel_is_active(&ctx.active_book_depth_channels, instrument_id, channel);

                if !deltas_active && !depth_active {
                    return;
                }

                let Some(instrument) = ctx.instruments.get_cloned(&instrument_id) else {
                    log::warn!("Orderbook message received for unknown instrument {instrument_id}");
                    return;
                };

                let ts_init = ctx.clock.get_time_ns();

                if deltas_active {
                    match parse_orderbook_deltas(
                        &msg,
                        instrument.price_precision(),
                        instrument.size_precision(),
                        ts_init,
                    ) {
                        Ok(deltas) => {
                            Self::send_data(ctx, Data::BookDeltas(Box::new(deltas)));
                        }
                        Err(e) => log::warn!("Failed to parse Derive orderbook deltas: {e}"),
                    }
                }

                if depth_active {
                    match parse_orderbook_depth(
                        &msg,
                        instrument.price_precision(),
                        instrument.size_precision(),
                        ts_init,
                    ) {
                        Ok(depth) => Self::send_data(ctx, Data::BookDepth(Box::new(depth))),
                        Err(e) => log::warn!("Failed to parse Derive orderbook depth: {e}"),
                    }
                }
            }
            DerivePublicWsData::Trades(msg) => {
                let ts_init = ctx.clock.get_time_ns();

                for trade in &msg.trades {
                    let instrument_id = format_instrument_id(trade.instrument_name);

                    if !ctx.active_trade_subs.contains(&instrument_id) {
                        continue;
                    }

                    let Some(instrument) = ctx.instruments.get_cloned(&instrument_id) else {
                        log::warn!("Trade message received for unknown instrument {instrument_id}");
                        continue;
                    };

                    match parse_trade_tick(
                        trade,
                        instrument.price_precision(),
                        instrument.size_precision(),
                        ts_init,
                    ) {
                        Ok(tick) => Self::send_data(ctx, Data::Trade(tick)),
                        Err(e) => log::warn!("Failed to parse Derive trade tick: {e}"),
                    }
                }
            }
            DerivePublicWsData::Ticker(msg) => {
                let instrument_id = msg.data.instrument_id();

                if !channel_is_active(
                    &ctx.active_ticker_channels,
                    instrument_id,
                    msg.channel.as_str(),
                ) {
                    return;
                }

                let Some(instrument) = ctx.instruments.get_cloned(&instrument_id) else {
                    log::warn!("Ticker message received for unknown instrument {instrument_id}");
                    return;
                };

                let ts_init = ctx.clock.get_time_ns();
                let price_precision = instrument.price_precision();

                if ctx.active_quote_subs.contains(&instrument_id) {
                    let quote = match parse_ticker_quote(
                        &msg,
                        price_precision,
                        instrument.size_precision(),
                        ts_init,
                    ) {
                        Ok(quote) => quote,
                        Err(e) => {
                            log::warn!("Failed to parse Derive ticker quote: {e}");
                            return;
                        }
                    };
                    let current_session_epoch = ctx.ws_subscription_handle.current_session_epoch();
                    let mut feed_state = ctx.quote_feed_state.lock();
                    let state = feed_state.entry(instrument_id).or_default();
                    if state.ws_epoch != Some(current_session_epoch) {
                        // Never confirmed, or confirmed under a now-stale
                        // transport/session identity (survived a reconnect
                        // or a full transport rebuild). The prior session's
                        // confirmation must not let a new-session ticker
                        // through; re-establish it only from a genuine
                        // current-session subscribe acknowledgement. The
                        // full `SessionEpoch` (transport generation + its
                        // own epoch) is required here, not the bare epoch:
                        // a rebuilt transport's epoch restarts at the same
                        // values an earlier, now-dead transport used.
                        state.ws_subscription_confirmed = false;
                        state.ws_epoch = Some(current_session_epoch);
                    }
                    if !state.ws_subscription_confirmed
                        && ctx
                            .ws_subscription_handle
                            .channel_confirmed_for_current_epoch(msg.channel.as_str())
                    {
                        state.ws_subscription_confirmed = true;
                    }
                    // Only the REST-failover path needs to fence a stale
                    // pre-reconnect confirmation from a genuine current-epoch
                    // one; with the feature off this must stay byte-for-byte
                    // the pre-change WS path regardless of ack status.
                    if ctx.max_quote_age_secs.is_some() && !state.ws_subscription_confirmed {
                        return;
                    }
                    if ctx.max_quote_age_secs.is_some()
                        && !quote_is_fresh(quote.ts_event, ts_init, ctx.max_quote_age_secs)
                    {
                        log::debug!("Skipping stale Derive WebSocket quote for {instrument_id}");
                        return;
                    }
                    if ctx.max_quote_age_secs.is_some()
                        && state
                            .last_published_event
                            .is_some_and(|last_event| quote.ts_event <= last_event)
                    {
                        log::debug!(
                            "Skipping non-monotonic Derive WebSocket quote for {instrument_id}"
                        );
                        return;
                    }
                    let mut quote_cache = ctx.quote_cache.lock();

                    match process_ticker_quote(
                        &msg,
                        price_precision,
                        instrument.size_precision(),
                        ts_init,
                        &mut quote_cache,
                    ) {
                        Ok(Some(quote)) => {
                            if ctx.max_quote_age_secs.is_some() {
                                state.last_published_event = Some(quote.ts_event);
                                state.last_ws_received = Some(ts_init);
                                if state.rest_polling {
                                    let age_nanos =
                                        ts_init.as_u64().saturating_sub(quote.ts_event.as_u64());
                                    log::info!(
                                        "Derive quote source changed to websocket for {instrument_id}; quote_age_ns={age_nanos}; reason=current_websocket_quote"
                                    );
                                    state.rest_polling = false;
                                }
                            }
                            Self::send_data(ctx, Data::Quote(quote));
                        }
                        Ok(None) => {}
                        Err(e) => log::warn!("Failed to parse Derive ticker quote: {e}"),
                    }
                }

                if ctx.active_mark_subs.contains(&instrument_id) {
                    match parse_mark_price(&msg, price_precision, ts_init) {
                        Ok(Some(update)) => Self::send_data(ctx, Data::MarkPrice(update)),
                        Ok(None) => {}
                        Err(e) => log::warn!("Failed to parse Derive mark price: {e}"),
                    }
                }

                if ctx.active_index_subs.contains(&instrument_id) {
                    match parse_index_price(&msg, price_precision, ts_init) {
                        Ok(Some(update)) => Self::send_data(ctx, Data::IndexPrice(update)),
                        Ok(None) => {}
                        Err(e) => log::warn!("Failed to parse Derive index price: {e}"),
                    }
                }

                if ctx.active_funding_subs.contains(&instrument_id) {
                    match parse_funding_rate(&msg, ts_init) {
                        Ok(Some(update)) => {
                            if let Err(e) = ctx.data_sender.send(DataEvent::FundingRate(update)) {
                                log::error!("Failed to send Derive funding rate: {e}");
                            }
                        }
                        Ok(None) => {}
                        Err(e) => log::warn!("Failed to parse Derive funding rate: {e}"),
                    }
                }

                if ctx.active_greeks_subs.contains(&instrument_id) {
                    match parse_option_greeks(&msg, ts_init) {
                        Ok(Some(greeks)) => {
                            if let Err(e) = ctx.data_sender.send(DataEvent::OptionGreeks(greeks)) {
                                log::error!("Failed to send Derive option greeks: {e}");
                            }
                        }
                        Ok(None) => {}
                        Err(e) => log::warn!("Failed to parse Derive option greeks: {e}"),
                    }
                }
            }
        }
    }

    fn send_data(ctx: &WsMessageContext, data: Data) {
        if let Err(e) = ctx.data_sender.send(DataEvent::Data(data)) {
            log::error!("Failed to send Derive data event: {e}");
        }
    }

    fn cache_provider_instruments(&self) {
        let instruments = self
            .provider
            .store()
            .get_all()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for instrument in instruments {
            self.cache_instrument(&instrument);
            if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                log::warn!("Failed to send Derive instrument: {e}");
            }
        }
    }

    fn cache_instrument(&self, instrument: &InstrumentAny) {
        cache_instrument(&self.instruments, instrument);
    }

    fn prepare_subscribe(&self, instrument_id: InstrumentId) -> anyhow::Result<bool> {
        if self.instruments.contains_key(&instrument_id) {
            return Ok(false);
        }

        if !self.config.auto_load_missing_instruments {
            anyhow::bail!(
                "Instrument {instrument_id} not found and `auto_load_missing_instruments` is disabled"
            );
        }
        Ok(true)
    }

    async fn lazy_load_instrument(
        http_client: DeriveHttpClient,
        instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
        instrument_id: InstrumentId,
        include_expired: bool,
    ) -> anyhow::Result<()> {
        let currency = currency_from_instrument_id(&instrument_id)?;
        let definitions = fetch_instrument_definitions(&http_client, currency, include_expired)
            .await
            .with_context(|| format!("failed to lazy-load Derive instruments for {currency}"))?;
        let mut found = false;

        for instrument in parse_instrument_definitions(definitions) {
            if instrument.id() == instrument_id {
                found = true;
            }
            cache_instrument(&instruments, &instrument);
        }

        if !found {
            anyhow::bail!(InstrumentLookupError::not_found(instrument_id));
        }

        Ok(())
    }

    fn ws_handle(&self) -> DeriveWebSocketSubscriptionHandle {
        self.ws_handle.clone()
    }

    fn subscription_lifecycle(&self) -> SubscriptionLifecycle {
        SubscriptionLifecycle {
            registry: Arc::clone(&self.channel_subscriptions),
            lock: Arc::clone(&self.subscription_lock),
            is_connected: Arc::clone(&self.is_connected),
            ws_session_generation: Arc::clone(&self.ws_session_generation),
            cancellation: self.cancellation_token.clone(),
            dispatch: SubscriptionDispatchState {
                active_book_delta_channels: Arc::clone(&self.active_book_delta_channels),
                active_book_depth_channels: Arc::clone(&self.active_book_depth_channels),
                active_ticker_channels: Arc::clone(&self.active_ticker_channels),
                active_quote_subs: Arc::clone(&self.active_quote_subs),
                active_trade_subs: Arc::clone(&self.active_trade_subs),
                active_mark_subs: Arc::clone(&self.active_mark_subs),
                active_index_subs: Arc::clone(&self.active_index_subs),
                active_funding_subs: Arc::clone(&self.active_funding_subs),
                active_greeks_subs: Arc::clone(&self.active_greeks_subs),
                quote_cache: Arc::clone(&self.quote_cache),
                quote_feed_state: Arc::clone(&self.quote_feed_state),
            },
        }
    }

    fn subscribe_ticker_feed(
        &self,
        instrument_id: InstrumentId,
        params: &Option<Params>,
        feed: TickerFeed,
        label: &'static str,
    ) -> anyhow::Result<()> {
        let owner = ChannelOwner::Ticker {
            instrument_id,
            feed,
        };
        let lifecycle = self.subscription_lifecycle();
        if lifecycle.is_active(owner) {
            return Ok(());
        }

        let channel = match self.active_ticker_channels.get_cloned(&instrument_id) {
            Some(channel) => channel,
            None => {
                let instrument_name = format_venue_symbol(&instrument_id)?.to_string();
                let interval = ticker_interval(params)?;
                ticker_channel(&instrument_name, &interval)
            }
        };
        let request = ChannelRequest::from_channel(&channel)?;
        let needs_load = self.prepare_subscribe(instrument_id)?;
        let Some(generation) = lifecycle.activate(owner, Some(&channel)) else {
            return Ok(());
        };
        if feed == TickerFeed::Quote && self.config.rest_market_data_failover_enabled {
            let active_count = {
                let mut quote_poll_subs = self.quote_poll_subs.lock();
                quote_poll_subs.insert(instrument_id, generation);
                quote_poll_subs.len()
            };
            if let Some(poll_interval_ms) = self.config.rest_market_data_poll_interval_ms
                && rest_poll_budget_exceeded(
                    active_count,
                    poll_interval_ms,
                    REST_POLL_BUDGET_WARNING_FRACTION,
                )
            {
                log::warn!(
                    "Derive REST market-data polling for {active_count} instruments at \
                     {poll_interval_ms}ms may alone exceed half the documented 5 TPS \
                     non-matching budget, which is shared with execution's private-state \
                     reconciliation polling from the same wallet/IP; consider a longer \
                     poll interval or fewer polled instruments",
                );
            }
        }
        let ws = self.ws_handle();
        let quote_poll_subs = Arc::clone(&self.quote_poll_subs);
        let quote_poll_subs_for_poll = Arc::clone(&quote_poll_subs);
        let http_client = self.http_client.clone();
        let include_expired = self.config.include_expired;
        let instruments = Arc::clone(&self.instruments);

        if feed == TickerFeed::Quote && self.config.rest_market_data_failover_enabled {
            let http_client = self.http_client.clone();
            let instruments = Arc::clone(&self.instruments);
            let quote_cache = Arc::clone(&self.quote_cache);
            let quote_feed_state = Arc::clone(&self.quote_feed_state);
            let subscription_lock = Arc::clone(&self.subscription_lock);
            let sender = self.data_sender.clone();
            let clock = self.clock;
            let cancellation = self.cancellation_token.clone();
            let ws_subscription_handle = ws.clone();
            let poll_interval_ms = self
                .config
                .rest_market_data_poll_interval_ms
                .expect("validated REST quote polling interval");
            let max_quote_age_secs = self
                .config
                .rest_market_data_max_quote_age_secs
                .expect("validated REST quote maximum age");

            self.spawn_task("rest_market_data_failover", async move {
                poll_rest_quotes(
                    instrument_id,
                    http_client,
                    instruments,
                    quote_cache,
                    quote_feed_state,
                    quote_poll_subs_for_poll,
                    subscription_lock,
                    generation,
                    sender,
                    clock,
                    ws_subscription_handle,
                    cancellation,
                    poll_interval_ms,
                    max_quote_age_secs,
                )
                .await
            });
        }

        self.spawn_task("subscribe_ticker_feed", async move {
            if needs_load
                && let Err(e) = Self::lazy_load_instrument(
                    http_client,
                    instruments,
                    instrument_id,
                    include_expired,
                )
                .await
            {
                lifecycle.rollback(owner, generation);
                remove_quote_poll_generation(&quote_poll_subs, instrument_id, generation);
                log::error!("Lazy-load failed for {instrument_id} ({label}): {e}");
                return Ok(());
            }

            let subscribe_lifecycle = lifecycle.clone();
            let result =
                run_channel_subscribe(subscribe_lifecycle, owner, generation, request, ws).await;
            if !lifecycle.is_current(owner, generation) {
                remove_quote_poll_generation(&quote_poll_subs, instrument_id, generation);
            }
            result
        });

        Ok(())
    }

    fn unsubscribe_channel_owner(&self, owner: ChannelOwner) -> anyhow::Result<()> {
        let lifecycle = self.subscription_lifecycle();
        let Some(removed) = lifecycle.remove(owner) else {
            return Ok(());
        };

        if !removed.channel_empty {
            return Ok(());
        }
        let Some(channel) = removed.channel else {
            return Ok(());
        };
        let request = ChannelRequest::from_channel(&channel)?;
        let ws = self.ws_handle();

        self.spawn_task("unsubscribe_channel", async move {
            run_channel_unsubscribe(lifecycle, request, ws).await
        });
        Ok(())
    }
}

#[async_trait(?Send)]
impl DataClient for DeriveDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*DERIVE_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Derive data client: {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Derive data client: {}", self.client_id);
        self.cancellation_token.cancel();
        self.abort_session_tasks();
        self.abort_pending_tasks();
        self.is_connected.store(false, Ordering::Relaxed);
        self.rest_failover_active.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::info!("Resetting Derive data client: {}", self.client_id);
        self.cancellation_token.cancel();

        self.abort_session_tasks();
        self.abort_pending_tasks();
        self.is_connected.store(false, Ordering::Relaxed);
        self.rest_failover_active.store(false, Ordering::Relaxed);

        // Leave the cancellation token cancelled; connect() refreshes it
        // (and tears down the inner WS client) on the next lifecycle start.
        self.instruments.store(AHashMap::new());
        self.clear_subscription_state();
        self.provider.store_mut().clear();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing Derive data client: {}", self.client_id);
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
            || (self.config.rest_market_data_failover_enabled
                && self.rest_failover_active.load(Ordering::SeqCst))
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        // While REST failover is the active path, `is_connected()` reports
        // true (see above), so this also treats a `connect()` call made
        // while the initial-reconnect retry task owns the WebSocket
        // lifecycle as a no-op, rather than racing that task for the
        // WebSocket client's sole event receiver (which could otherwise get
        // the fast-path `Ok` right after the retry task takes the receiver,
        // then `None` back, tearing down a session the retry task just
        // recovered).
        if self.is_connected()
            && !self.cancellation_token.is_cancelled()
            && self.session_tasks.is_open()
            && self.pending_tasks.is_open()
        {
            return Ok(());
        }

        // Completes the async teardown deferred by sync reset()/stop().
        if self.cancellation_token.is_cancelled()
            || !self.session_tasks.is_open()
            || !self.pending_tasks.is_open()
        {
            self.teardown_partial_connect().await?;
            self.cancellation_token = CancellationToken::new();
            self.session_tasks.start_generation().map_err(|e| {
                anyhow::anyhow!("Failed to start Derive data session generation: {e}")
            })?;
            self.pending_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start Derive data task generation: {e}"))?;
        }
        let cancellation_token = self.cancellation_token.clone();
        let ws_shutdown = self.ws_shutdown.clone();
        let setup_guard =
            TaskGroupGuard::new(&[&self.session_tasks, &self.pending_tasks], move || {
                cancellation_token.cancel();
                ws_shutdown.begin_shutdown();
            });

        if !self.config.currencies.is_empty() {
            self.provider
                .load_all(None)
                .await
                .context("failed to load Derive instruments")?;
            self.cache_provider_instruments();
        }

        let connect_result = self.ws_client.lock().await.connect().await;
        if let Err(e) = connect_result {
            if !self.config.rest_market_data_failover_enabled {
                return Err(e).context("failed to connect Derive WebSocket");
            }

            let ws_client = Arc::clone(&self.ws_client);
            let cancellation = self.cancellation_token.clone();
            let is_connected = Arc::clone(&self.is_connected);
            let ws_session_generation = Arc::clone(&self.ws_session_generation);
            let ctx = self.ws_message_context();
            self.spawn_task("derive_websocket_initial_reconnect", async move {
                let mut retry_delay = RECONNECT_BASE_BACKOFF;
                loop {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Ok(()),
                        () = tokio::time::sleep(retry_delay) => {}
                    }

                    let mut client = ws_client.lock().await;
                    let connect_result = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return Ok(()),
                        result = client.connect() => result,
                    };
                    match connect_result {
                        Ok(()) => {
                            retry_delay = RECONNECT_BASE_BACKOFF;
                            let Some(mut rx) = client.take_event_receiver() else {
                                anyhow::bail!("Derive WebSocket event receiver not initialized");
                            };
                            // This is a fresh transport, not an in-place
                            // reconnect, so nothing has fired `Reconnected`
                            // to invalidate prior-session feed state; do it
                            // explicitly before anything can observe this
                            // transport as connected.
                            Self::invalidate_quote_feed_state(&ctx);
                            // A rebuilt transport starts with no subscriptions
                            // at all (unlike an in-place reconnect, which
                            // replays them automatically), so this retry
                            // owner must reissue every channel the data
                            // client still considers active or WS data never
                            // resumes for the rest of the process.
                            let replay_channels = active_channels_for_replay(&ctx);
                            if !replay_channels.is_empty()
                                && let Err(e) =
                                    client.subscribe_channels(replay_channels.clone()).await
                            {
                                log::warn!(
                                    "Failed to replay Derive channel subscriptions after \
                                     WebSocket reconnect: {e}; channels={replay_channels:?}",
                                );
                            }
                            drop(client);
                            is_connected.store(true, Ordering::Release);
                            ws_session_generation.fetch_add(1, Ordering::AcqRel);

                            loop {
                                tokio::select! {
                                    biased;
                                    () = cancellation.cancelled() => return Ok(()),
                                    maybe_msg = rx.recv() => match maybe_msg {
                                        Some(msg) => {
                                            if matches!(&msg, DeriveWsMessage::SessionRecoveryFailed(_)) {
                                                is_connected.store(false, Ordering::Release);
                                            } else if matches!(&msg, DeriveWsMessage::Reconnected) {
                                                is_connected.store(true, Ordering::Release);
                                            }
                                            Self::handle_ws_message(msg, &ctx);
                                        }
                                        None => {
                                            // The stream ended after a terminal
                                            // recovery failure exhausted its
                                            // retry budget. Keep owning
                                            // reconnection: fall back to the
                                            // outer loop and dial again rather
                                            // than abandoning WebSocket
                                            // recovery for the rest of the
                                            // session.
                                            is_connected.store(false, Ordering::Release);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            drop(client);
                            log::warn!("Derive WebSocket initial reconnect failed: {e}");
                            retry_delay = next_reconnect_delay(retry_delay);
                        }
                    }
                }
            });
            self.is_connected.store(false, Ordering::Release);
            self.rest_failover_active.store(true, Ordering::Release);
            setup_guard.disarm();
            log::warn!("Derive WebSocket unavailable; REST market-data failover remains active");
            return Ok(());
        }

        let session_result = self
            .ws_client
            .lock()
            .await
            .take_event_receiver()
            .ok_or_else(|| anyhow::anyhow!("Derive WebSocket event receiver not initialized"))
            .and_then(|rx| self.spawn_stream_task(rx));

        if let Err(e) = session_result {
            if let Err(teardown_error) = self.teardown_partial_connect().await {
                return Err(e.context(format!(
                    "Derive data startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e);
        }

        self.is_connected.store(true, Ordering::Release);
        self.ws_session_generation.fetch_add(1, Ordering::AcqRel);
        setup_guard.disarm();
        log::info!(
            "Connected Derive data client ({:?})",
            self.config.environment
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.teardown_partial_connect().await?;
        log::info!("Disconnected Derive data client");
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Derive only supports L2_MBP order book deltas");
        }

        let instrument_id = cmd.instrument_id;
        let owner = ChannelOwner::BookDeltas(instrument_id);
        let lifecycle = self.subscription_lifecycle();
        if lifecycle.is_active(owner) {
            return Ok(());
        }

        let instrument_name = format_venue_symbol(&instrument_id)?.to_string();
        let group = orderbook_group(&cmd.params)?;
        let depth = orderbook_depth(cmd.depth.map(|d| d.get()), &cmd.params)?;
        let channel = orderbook_channel(&instrument_name, &group, &depth);
        let request = ChannelRequest::from_channel(&channel)?;
        let needs_load = self.prepare_subscribe(instrument_id)?;
        let Some(generation) = lifecycle.activate(owner, Some(&channel)) else {
            return Ok(());
        };
        let ws = self.ws_handle();
        let http_client = self.http_client.clone();
        let include_expired = self.config.include_expired;
        let instruments = Arc::clone(&self.instruments);

        self.spawn_task("subscribe_book_deltas", async move {
            if needs_load
                && let Err(e) = Self::lazy_load_instrument(
                    http_client,
                    instruments,
                    instrument_id,
                    include_expired,
                )
                .await
            {
                lifecycle.rollback(owner, generation);
                log::error!("Lazy-load failed for {instrument_id} (book deltas): {e}");
                return Ok(());
            }

            run_channel_subscribe(lifecycle, owner, generation, request, ws).await
        });

        Ok(())
    }

    fn subscribe_book_depth(&mut self, cmd: SubscribeBookDepth) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Derive only supports L2_MBP order book depth");
        }

        let instrument_id = cmd.instrument_id;
        let owner = ChannelOwner::BookDepth(instrument_id);
        let lifecycle = self.subscription_lifecycle();
        if lifecycle.is_active(owner) {
            return Ok(());
        }

        let instrument_name = format_venue_symbol(&instrument_id)?.to_string();
        let group = orderbook_group(&cmd.params)?;
        let depth = DeriveOrderbookDepth::D10.to_string();
        let channel = orderbook_channel(&instrument_name, &group, &depth);
        let request = ChannelRequest::from_channel(&channel)?;
        let needs_load = self.prepare_subscribe(instrument_id)?;
        let Some(generation) = lifecycle.activate(owner, Some(&channel)) else {
            return Ok(());
        };
        let ws = self.ws_handle();
        let http_client = self.http_client.clone();
        let include_expired = self.config.include_expired;
        let instruments = Arc::clone(&self.instruments);

        self.spawn_task("subscribe_book_depth", async move {
            if needs_load
                && let Err(e) = Self::lazy_load_instrument(
                    http_client,
                    instruments,
                    instrument_id,
                    include_expired,
                )
                .await
            {
                lifecycle.rollback(owner, generation);
                log::error!("Lazy-load failed for {instrument_id} (book depth): {e}");
                return Ok(());
            }

            run_channel_subscribe(lifecycle, owner, generation, request, ws).await
        });

        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.subscribe_ticker_feed(cmd.instrument_id, &cmd.params, TickerFeed::Quote, "quotes")
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let owner = ChannelOwner::Trades(instrument_id);
        let lifecycle = self.subscription_lifecycle();
        if lifecycle.is_active(owner) {
            return Ok(());
        }

        let needs_load = self.prepare_subscribe(instrument_id)?;
        let Some(generation) = lifecycle.activate(owner, None) else {
            return Ok(());
        };
        let ws = self.ws_handle();
        let http_client = self.http_client.clone();
        let include_expired = self.config.include_expired;
        let instruments = Arc::clone(&self.instruments);

        self.spawn_task("subscribe_trades", async move {
            if needs_load
                && let Err(e) = Self::lazy_load_instrument(
                    http_client,
                    Arc::clone(&instruments),
                    instrument_id,
                    include_expired,
                )
                .await
            {
                lifecycle.rollback(owner, generation);
                log::error!("Lazy-load failed for {instrument_id} (trades): {e}");
                return Ok(());
            }

            if !lifecycle.is_current(owner, generation) {
                return Ok(());
            }

            let Some(instrument) = instruments.get_cloned(&instrument_id) else {
                lifecycle.rollback(owner, generation);
                log::error!("Instrument {instrument_id} not found for Derive trades");
                return Ok(());
            };
            let channel = match trade_channel(&instrument) {
                Ok(channel) => channel,
                Err(e) => {
                    lifecycle.rollback(owner, generation);
                    log::error!("Failed to resolve Derive trades channel: {e}");
                    return Ok(());
                }
            };
            let request = match ChannelRequest::from_channel(&channel) {
                Ok(request) => request,
                Err(e) => {
                    lifecycle.rollback(owner, generation);
                    log::error!("Invalid Derive trades channel `{channel}`: {e}");
                    return Ok(());
                }
            };

            if !lifecycle.attach_channel(owner, generation, channel) {
                return Ok(());
            }

            run_channel_subscribe(lifecycle, owner, generation, request, ws).await
        });

        Ok(())
    }

    fn subscribe_mark_prices(&mut self, cmd: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscribe_ticker_feed(
            cmd.instrument_id,
            &cmd.params,
            TickerFeed::Mark,
            "mark prices",
        )
    }

    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_ticker_feed(
            cmd.instrument_id,
            &cmd.params,
            TickerFeed::Index,
            "index prices",
        )
    }

    fn subscribe_funding_rates(&mut self, cmd: SubscribeFundingRates) -> anyhow::Result<()> {
        self.subscribe_ticker_feed(
            cmd.instrument_id,
            &cmd.params,
            TickerFeed::Funding,
            "funding rates",
        )
    }

    fn subscribe_option_greeks(&mut self, cmd: SubscribeOptionGreeks) -> anyhow::Result<()> {
        self.subscribe_ticker_feed(
            cmd.instrument_id,
            &cmd.params,
            TickerFeed::Greeks,
            "option greeks",
        )
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::BookDeltas(cmd.instrument_id))
    }

    fn unsubscribe_book_depth(&mut self, cmd: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::BookDepth(cmd.instrument_id))
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.quote_poll_subs.lock().remove(&cmd.instrument_id);
        self.unsubscribe_channel_owner(ChannelOwner::Ticker {
            instrument_id: cmd.instrument_id,
            feed: TickerFeed::Quote,
        })
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::Trades(cmd.instrument_id))
    }

    fn unsubscribe_mark_prices(&mut self, cmd: &UnsubscribeMarkPrices) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::Ticker {
            instrument_id: cmd.instrument_id,
            feed: TickerFeed::Mark,
        })
    }

    fn unsubscribe_index_prices(&mut self, cmd: &UnsubscribeIndexPrices) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::Ticker {
            instrument_id: cmd.instrument_id,
            feed: TickerFeed::Index,
        })
    }

    fn unsubscribe_funding_rates(&mut self, cmd: &UnsubscribeFundingRates) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::Ticker {
            instrument_id: cmd.instrument_id,
            feed: TickerFeed::Funding,
        })
    }

    fn unsubscribe_option_greeks(&mut self, cmd: &UnsubscribeOptionGreeks) -> anyhow::Result<()> {
        self.unsubscribe_channel_owner(ChannelOwner::Ticker {
            instrument_id: cmd.instrument_id,
            feed: TickerFeed::Greeks,
        })
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        // No historical quote endpoint; `public/get_tickers` is a current snapshot.
        let instrument_id = request.instrument_id;
        let instrument = self
            .instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id))?;
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();

        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);

        self.spawn_task("request_quotes", async move {
            let ticker = match http_client.get_ticker(&venue_symbol).await {
                Ok(ticker) => ticker,
                Err(e) => {
                    log::error!("Failed to fetch Derive ticker for {instrument_id}: {e:?}");
                    return Ok(());
                }
            };

            let ts_init = clock.get_time_ns();
            let quotes = match parse_ticker_quote_from_rest(
                &ticker,
                price_precision,
                size_precision,
                ts_init,
            ) {
                Ok(quote) => {
                    // Drop the snapshot when bounded callers ask for a past window.
                    let within_start = start_nanos.is_none_or(|nanos| quote.ts_event >= nanos);
                    let within_end = end_nanos.is_none_or(|nanos| quote.ts_event <= nanos);
                    if within_start && within_end {
                        vec![quote]
                    } else {
                        Vec::new()
                    }
                }
                Err(e) => {
                    log::warn!("Failed to parse Derive ticker for {instrument_id}: {e}");
                    Vec::new()
                }
            };

            let response = DataResponse::Quotes(QuotesResponse::new(
                request_id,
                client_id,
                instrument_id,
                quotes,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            ));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive quotes response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        let instrument = self
            .instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id))?;
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();

        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(NonZeroUsize::get);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let from_timestamp = start.map(|dt| dt.as_millisecond());
        let to_timestamp = Some(match end {
            Some(dt) => dt.as_millisecond(),
            None => i64::try_from(clock.get_time_ms())
                .context("Derive current time exceeds i64 milliseconds")?,
        });

        self.spawn_task("request_trades", async move {
            // Hold page_size constant across requests: the venue paginates by
            // offset = (page - 1) * page_size, so shrinking page_size mid-walk
            // would skip and duplicate trades.
            let page_size = limit.map_or(DERIVE_TRADES_PAGE_SIZE, |cap| {
                cap.min(DERIVE_TRADES_PAGE_SIZE as usize) as u32
            });
            let mut trades = Vec::new();
            let mut seen_trade_ids = AHashSet::new();
            let mut page = 1u32;

            loop {
                let result = match http_client
                    .get_trade_history(&venue_symbol, from_timestamp, to_timestamp, page, page_size)
                    .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        log::error!("Failed to fetch Derive trades for {instrument_id}: {e:?}");
                        return Ok(());
                    }
                };

                if result.trades.is_empty() {
                    break;
                }

                let num_pages = result.pagination.num_pages;
                let ts_init = clock.get_time_ns();

                for trade in &result.trades {
                    match parse_trade_tick_from_rest(
                        trade,
                        price_precision,
                        size_precision,
                        ts_init,
                    ) {
                        Ok(tick) if seen_trade_ids.insert(tick.trade_id) => trades.push(tick),
                        Ok(_) => {}
                        Err(e) => log::warn!(
                            "Failed to parse Derive trade {} for {instrument_id}: {e}",
                            trade.trade_id,
                        ),
                    }
                }

                if let Some(cap) = limit
                    && trades.len() >= cap
                {
                    break;
                }

                if (page as i64) >= num_pages {
                    break;
                }
                page += 1;
            }

            trades.sort_by_key(|trade| trade.ts_event);
            if let Some(cap) = limit
                && trades.len() > cap
            {
                trades.drain(..trades.len() - cap);
            }

            let response = DataResponse::Trades(TradesResponse::new(
                request_id,
                client_id,
                instrument_id,
                trades,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            ));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive trades response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        let instrument = self
            .instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id))?;
        anyhow::ensure!(
            matches!(instrument, InstrumentAny::CryptoPerpetual(_)),
            "Funding rates are only available for Derive perpetual instruments (got {instrument_id})",
        );
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();

        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(NonZeroUsize::get);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);
        let start_ms = start.map(|dt| dt.as_millisecond());
        let end_ms = end.map(|dt| dt.as_millisecond());

        self.spawn_task("request_funding_rates", async move {
            let result = match http_client
                .get_funding_rate_history(&venue_symbol, start_ms, end_ms, None)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    log::error!(
                        "Failed to fetch Derive funding rate history for {instrument_id}: {e:?}",
                    );
                    return Ok(());
                }
            };

            let ts_init = clock.get_time_ns();
            let mut updates = Vec::with_capacity(result.funding_rate_history.len());

            for record in &result.funding_rate_history {
                match parse_funding_rate_history_record(record, instrument_id, None, ts_init) {
                    Ok(update) => updates.push(update),
                    Err(e) => log::warn!(
                        "Failed to parse Derive funding rate record for {instrument_id} at {}: {e}",
                        record.timestamp,
                    ),
                }
            }

            updates.sort_by_key(|update| update.ts_event);
            if let Some(cap) = limit
                && updates.len() > cap
            {
                updates.drain(..updates.len() - cap);
            }

            let response = DataResponse::FundingRates(FundingRatesResponse::new(
                request_id,
                client_id,
                instrument_id,
                updates,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            ));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive funding rates response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.bar_type;
        anyhow::ensure!(
            bar_type.aggregation_source() == AggregationSource::External,
            "Derive only supports EXTERNAL aggregation source (got {bar_type})",
        );
        let spec = bar_type.spec();
        anyhow::ensure!(
            spec.price_type == PriceType::Last,
            "Derive candles are trade-based; only PriceType::Last is supported (got {bar_type})",
        );

        let instrument_id = bar_type.instrument_id();
        let instrument = self
            .instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id))?;
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();

        let period = bar_spec_to_derive_period(spec.aggregation, spec.step.get() as u64)
            .with_context(|| format!("unsupported Derive bar spec for {bar_type}"))?;

        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(NonZeroUsize::get);
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);

        // The venue requires both bounds in UNIX seconds. Default end to now
        // and start to one window of `limit` buckets (or 1000) before end.
        let request_time = clock.get_time_ns();
        let now_secs = (request_time.as_u64() / NANOSECONDS_IN_SECOND) as i64;
        let end_ts = end.map_or(now_secs, |dt| dt.as_second());
        let default_span = i64::from(period) * limit.unwrap_or(DERIVE_CANDLES_DEFAULT_LIMIT) as i64;
        let start_ts = start.map_or(end_ts - default_span, |dt| dt.as_second());

        self.spawn_task("request_bars", async move {
            // Venue caps each call at 5000 candles; walk backwards by shrinking
            // `current_end` to one second before each page's earliest bucket.
            let mut seen_timestamps: AHashSet<i64> = AHashSet::new();
            let mut pages: Vec<Vec<Bar>> = Vec::new();
            let mut total_bars = 0usize;
            let mut current_end = end_ts;
            let mut page_count = 0;

            loop {
                page_count += 1;

                let mut records = match http_client
                    .get_candles(&venue_symbol, start_ts, current_end, period)
                    .await
                {
                    Ok(records) => records,
                    Err(e) => {
                        log::error!("Failed to fetch Derive candles for {bar_type}: {e:?}");
                        return Ok(());
                    }
                };

                if records.is_empty() {
                    break;
                }

                records.sort_by_key(|r| r.timestamp_bucket);

                let has_new = records
                    .iter()
                    .any(|r| !seen_timestamps.contains(&r.timestamp_bucket));

                if !has_new {
                    break;
                }

                let ts_init = clock.get_time_ns();
                let mut page_bars = Vec::with_capacity(records.len());
                let mut earliest_ts: Option<i64> = None;

                for record in &records {
                    let bucket = record.timestamp_bucket;
                    if earliest_ts.is_none_or(|ts| bucket < ts) {
                        earliest_ts = Some(bucket);
                    }

                    if seen_timestamps.contains(&bucket) {
                        continue;
                    }

                    match parse_candle_record(
                        record,
                        bar_type,
                        price_precision,
                        size_precision,
                        ts_init,
                    ) {
                        Ok(bar) => {
                            seen_timestamps.insert(bucket);

                            if bar.ts_event <= request_time {
                                page_bars.push(bar);
                            }
                        }
                        Err(e) => log::warn!(
                            "Failed to parse Derive candle for {bar_type} at {bucket}: {e}",
                        ),
                    }
                }

                total_bars += page_bars.len();
                pages.push(page_bars);

                if let Some(cap) = limit
                    && total_bars >= cap
                {
                    break;
                }

                let Some(earliest) = earliest_ts else {
                    break;
                };

                if earliest <= start_ts {
                    break;
                }

                current_end = earliest - 1;

                if page_count >= DERIVE_CANDLES_MAX_PAGES {
                    log::warn!(
                        "Derive bars pagination hit safety cap of {DERIVE_CANDLES_MAX_PAGES} pages for {bar_type}",
                    );
                    break;
                }
            }

            let mut bars: Vec<Bar> = Vec::with_capacity(total_bars);
            for page in pages.into_iter().rev() {
                bars.extend(page);
            }

            if let Some(cap) = limit
                && bars.len() > cap
            {
                let drop_count = bars.len() - cap;
                bars.drain(..drop_count);
            }

            let response = DataResponse::Bars(BarsResponse::new(
                request_id,
                client_id,
                bar_type,
                bars,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            ));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive bars response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_option_chain_reference_price(
        &self,
        request: RequestOptionChainReferencePrice,
    ) -> anyhow::Result<()> {
        let series_id = request.series_id;
        let instrument_id = request.instrument_id;
        let instrument = self
            .instruments
            .get_cloned(&instrument_id)
            .ok_or_else(|| InstrumentLookupError::not_found(instrument_id))?;
        anyhow::ensure!(
            matches!(instrument, InstrumentAny::CryptoOption(_)),
            "Derive option-chain reference prices require an option instrument (got {instrument_id})",
        );
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();

        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let params = request.params;

        self.spawn_task("request_option_chain_reference_price", async move {
            let price = match http_client.get_ticker(&venue_symbol).await {
                Ok(ticker) => match ticker.option_pricing.as_ref() {
                    Some(pricing) if pricing.forward_price > Decimal::ZERO => {
                        match Price::from_decimal(pricing.forward_price) {
                            Ok(price) => Some(price),
                            Err(e) => {
                                log::warn!(
                                    "Invalid Derive option-chain reference price for {instrument_id}: {e}"
                                );
                                None
                            }
                        }
                    }
                    None => {
                        log::warn!(
                            "Derive ticker for {instrument_id} has no option pricing reference"
                        );
                        None
                    }
                    Some(_) => None,
                },
                Err(e) => {
                    log::error!(
                        "Option-chain reference price request failed for {series_id}: {e:?}"
                    );
                    None
                }
            };

            let response = DataResponse::OptionChainReferencePrice(
                OptionChainReferencePriceResponse::new(
                    request_id,
                    client_id,
                    series_id,
                    price,
                    clock.get_time_ns(),
                    params,
                ),
            );

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send option-chain reference price response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let currencies = self.config.currencies.clone();
        if currencies.is_empty() {
            anyhow::bail!(
                "Derive request_instruments requires at least one configured currency \
                 (DeriveDataClientConfig::currencies)"
            );
        }

        let http_client = self.http_client.clone();
        let include_expired = self.config.include_expired;
        let instruments_cache = Arc::clone(&self.instruments);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let venue = self.venue().unwrap_or(*DERIVE_VENUE);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;

        self.spawn_task("request_instruments", async move {
            let mut all_instruments = Vec::new();

            for currency in currencies {
                match fetch_instrument_definitions(&http_client, &currency, include_expired).await {
                    Ok(definitions) => {
                        for instrument in parse_instrument_definitions(definitions) {
                            cache_instrument(&instruments_cache, &instrument);
                            all_instruments.push(instrument);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to fetch Derive instruments for {currency}: {e:?}");
                    }
                }
            }

            let response = DataResponse::Instruments(InstrumentsResponse::new(
                request_id,
                client_id,
                venue,
                all_instruments,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            ));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive instruments response: {e}");
            }
            Ok(())
        });

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        let venue_symbol = format_venue_symbol(&instrument_id)?.to_string();

        let http_client = self.http_client.clone();
        let instruments_cache = Arc::clone(&self.instruments);
        let sender = self.data_sender.clone();
        let clock = self.clock;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let request_id = request.request_id;
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;

        self.spawn_task("request_instrument", async move {
            let definition = match http_client.get_instrument(&venue_symbol).await {
                Ok(definition) => definition,
                Err(e) => {
                    log::error!("Failed to fetch Derive instrument {instrument_id}: {e:?}");
                    return Ok(());
                }
            };

            let ts_init = clock.get_time_ns();
            let instrument = match parse_derive_instrument_any(&definition, ts_init) {
                Ok(Some(instrument)) => instrument,
                Ok(None) => {
                    log::warn!(
                        "Derive instrument {instrument_id} resolved to an unsupported type ({:?})",
                        definition.instrument_type,
                    );
                    return Ok(());
                }
                Err(e) => {
                    log::error!("Failed to parse Derive instrument {instrument_id}: {e}");
                    return Ok(());
                }
            };

            cache_instrument(&instruments_cache, &instrument);

            let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                request_id,
                client_id,
                instrument.id(),
                instrument,
                start_nanos,
                end_nanos,
                clock.get_time_ns(),
                params,
            )));

            if let Err(e) = sender.send(DataEvent::Response(response)) {
                log::error!("Failed to send Derive instrument response: {e}");
            }
            Ok(())
        });

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TickerFeed {
    Quote,
    Mark,
    Index,
    Funding,
    Greeks,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ChannelOwner {
    BookDeltas(InstrumentId),
    BookDepth(InstrumentId),
    Ticker {
        instrument_id: InstrumentId,
        feed: TickerFeed,
    },
    Trades(InstrumentId),
}

#[derive(Debug)]
struct OwnedChannel {
    generation: u64,
    channel: Option<String>,
}

#[derive(Debug, Default)]
struct ChannelSubscriptionState {
    next_generation: u64,
    owners: AHashMap<ChannelOwner, OwnedChannel>,
    channels: AHashMap<String, AHashSet<ChannelOwner>>,
}

#[derive(Debug, Default)]
struct ChannelSubscriptionRegistry {
    state: Mutex<ChannelSubscriptionState>,
    transitions: DashMap<String, Weak<tokio::sync::Mutex<()>>>,
}

const TRANSITION_GC_THRESHOLD: usize = 256;

#[derive(Debug)]
struct RemovedSubscription {
    channel: Option<String>,
    channel_empty: bool,
}

#[derive(Debug, Clone)]
struct SubscriptionDispatchState {
    active_book_delta_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_book_depth_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_ticker_channels: Arc<AtomicMap<InstrumentId, String>>,
    active_quote_subs: Arc<AtomicSet<InstrumentId>>,
    active_trade_subs: Arc<AtomicSet<InstrumentId>>,
    active_mark_subs: Arc<AtomicSet<InstrumentId>>,
    active_index_subs: Arc<AtomicSet<InstrumentId>>,
    active_funding_subs: Arc<AtomicSet<InstrumentId>>,
    active_greeks_subs: Arc<AtomicSet<InstrumentId>>,
    quote_cache: Arc<Mutex<QuoteCache>>,
    quote_feed_state: Arc<Mutex<AHashMap<InstrumentId, QuoteFeedState>>>,
}

#[derive(Debug, Clone)]
struct SubscriptionLifecycle {
    registry: Arc<ChannelSubscriptionRegistry>,
    lock: Arc<Mutex<()>>,
    is_connected: Arc<AtomicBool>,
    ws_session_generation: Arc<AtomicU64>,
    cancellation: CancellationToken,
    dispatch: SubscriptionDispatchState,
}

#[derive(Debug)]
enum ChannelRequest {
    Orderbook {
        channel: String,
        instrument_name: String,
        group: String,
        depth: String,
    },
    Ticker {
        channel: String,
        instrument_name: String,
        interval: String,
    },
    Trades {
        channel: String,
        instrument_type: String,
        currency: String,
    },
}

async fn run_channel_subscribe(
    lifecycle: SubscriptionLifecycle,
    owner: ChannelOwner,
    generation: u64,
    request: ChannelRequest,
    ws: DeriveWebSocketSubscriptionHandle,
) -> anyhow::Result<()> {
    let channel = request.channel().to_string();
    let transition = lifecycle.registry.transition(&channel);
    let _guard = transition.lock().await;

    while !lifecycle.is_connected.load(Ordering::Acquire) {
        if !lifecycle.is_current_channel(owner, generation, &channel) {
            return Ok(());
        }
        tokio::select! {
            () = lifecycle.cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }

    if !lifecycle.is_current_channel(owner, generation, &channel) || ws.has_subscription(&channel) {
        return Ok(());
    }

    let session_generation = lifecycle.ws_session_generation.load(Ordering::Acquire);

    if let Err(e) = request.subscribe(&ws).await {
        lifecycle.rollback(owner, generation);
        log::error!("Failed to subscribe to Derive channel `{channel}`: {e}");
        let cleanup_error = match request.unsubscribe(&ws).await {
            Ok(()) => None,
            Err(cleanup_error) => {
                log::error!(
                    "Failed to clean up uncertain Derive channel `{channel}`: {cleanup_error}",
                );
                Some(cleanup_error)
            }
        };

        if retain_channel_for_reconnect(
            &ws,
            &channel,
            lifecycle.has_owners(&channel),
            cleanup_error.as_ref(),
        ) {
            log::error!(
                "Derive channel `{channel}` remains uncertain and will replay on reconnect",
            );
        }
        return Ok(());
    }

    if matches!(
        owner,
        ChannelOwner::Ticker {
            feed: TickerFeed::Quote,
            ..
        }
    ) {
        lifecycle.confirm_quote_subscription_if_current(
            owner,
            generation,
            &channel,
            session_generation,
            ws.current_session_epoch(),
        );
    }

    if !lifecycle.has_owners(&channel)
        && let Err(e) = request.unsubscribe(&ws).await
    {
        log::error!("Failed to clean up stale Derive channel `{channel}`: {e}");
        ws.forget_subscription(&channel);
    }
    Ok(())
}

async fn run_channel_unsubscribe(
    lifecycle: SubscriptionLifecycle,
    request: ChannelRequest,
    ws: DeriveWebSocketSubscriptionHandle,
) -> anyhow::Result<()> {
    let channel = request.channel().to_string();
    let transition = lifecycle.registry.transition(&channel);
    let _guard = transition.lock().await;

    if lifecycle.has_owners(&channel) || !ws.has_subscription(&channel) {
        return Ok(());
    }

    if let Err(e) = request.unsubscribe(&ws).await {
        log::error!("Failed to unsubscribe from Derive channel `{channel}`: {e}");
        ws.forget_subscription(&channel);
    }
    Ok(())
}

impl SubscriptionLifecycle {
    fn is_active(&self, owner: ChannelOwner) -> bool {
        let _guard = self.lock.lock();
        self.registry.state.lock().owners.contains_key(&owner)
    }

    fn activate(&self, owner: ChannelOwner, channel: Option<&str>) -> Option<u64> {
        let _guard = self.lock.lock();
        let mut state = self.registry.state.lock();
        let generation = state.activate(owner, channel)?;
        self.dispatch.activate(owner, channel);
        Some(generation)
    }

    fn attach_channel(&self, owner: ChannelOwner, generation: u64, channel: String) -> bool {
        let _guard = self.lock.lock();
        self.registry
            .state
            .lock()
            .attach_channel(owner, generation, channel)
    }

    fn is_current(&self, owner: ChannelOwner, generation: u64) -> bool {
        let _guard = self.lock.lock();
        self.registry.state.lock().is_current(owner, generation)
    }

    fn is_current_channel(&self, owner: ChannelOwner, generation: u64, channel: &str) -> bool {
        let _guard = self.lock.lock();
        self.registry
            .state
            .lock()
            .is_current_channel(owner, generation, channel)
    }

    fn rollback(&self, owner: ChannelOwner, generation: u64) -> bool {
        let _guard = self.lock.lock();
        let mut state = self.registry.state.lock();
        let Some(removed) = state.remove_if_generation(owner, generation) else {
            return false;
        };
        self.dispatch.deactivate(owner, removed.channel_empty);
        true
    }

    fn remove(&self, owner: ChannelOwner) -> Option<RemovedSubscription> {
        let _guard = self.lock.lock();
        let mut state = self.registry.state.lock();
        let removed = state.remove(owner)?;
        self.dispatch.deactivate(owner, removed.channel_empty);
        Some(removed)
    }

    fn has_owners(&self, channel: &str) -> bool {
        let _guard = self.lock.lock();
        self.registry.state.lock().has_owners(channel)
    }

    /// Atomically checks that `owner`'s channel ownership and the session
    /// generation are still current, and if so records the quote
    /// subscription as confirmed for `epoch`.
    ///
    /// The check and the write happen under one `self.lock` acquisition
    /// (the same lock `Reconnected` holds while bumping the session
    /// generation and clearing confirmation state), closing the gap where a
    /// stale subscribe task could otherwise re-confirm a channel in between
    /// a passed check and the write landing — after a concurrent
    /// unsubscribe/resubscribe or reconnect already invalidated it.
    fn confirm_quote_subscription_if_current(
        &self,
        owner: ChannelOwner,
        generation: u64,
        channel: &str,
        session_generation_before_subscribe: u64,
        session_epoch: SessionEpoch,
    ) -> bool {
        let _guard = self.lock.lock();
        if !self
            .registry
            .state
            .lock()
            .is_current_channel(owner, generation, channel)
            || self.ws_session_generation.load(Ordering::Acquire)
                != session_generation_before_subscribe
        {
            return false;
        }
        if let ChannelOwner::Ticker {
            instrument_id,
            feed: TickerFeed::Quote,
        } = owner
        {
            let mut state = self.dispatch.quote_feed_state.lock();
            let entry = state.entry(instrument_id).or_default();
            entry.ws_subscription_confirmed = true;
            entry.ws_epoch = Some(session_epoch);
        }
        true
    }
}

impl SubscriptionDispatchState {
    fn activate(&self, owner: ChannelOwner, channel: Option<&str>) {
        match owner {
            ChannelOwner::BookDeltas(instrument_id) => {
                self.active_book_delta_channels.insert(
                    instrument_id,
                    channel.expect("book channel present").to_string(),
                );
            }
            ChannelOwner::BookDepth(instrument_id) => {
                self.active_book_depth_channels.insert(
                    instrument_id,
                    channel.expect("book channel present").to_string(),
                );
            }
            ChannelOwner::Ticker {
                instrument_id,
                feed,
            } => {
                self.active_ticker_channels.insert(
                    instrument_id,
                    channel.expect("ticker channel present").to_string(),
                );
                self.ticker_subscriptions(feed).insert(instrument_id);
                if feed == TickerFeed::Quote {
                    let mut feed_states = self.quote_feed_state.lock();
                    let state = feed_states.entry(instrument_id).or_default();
                    state.last_ws_received = None;
                    state.ws_subscription_confirmed = false;
                }
            }
            ChannelOwner::Trades(instrument_id) => {
                self.active_trade_subs.insert(instrument_id);
            }
        }
    }

    fn deactivate(&self, owner: ChannelOwner, channel_empty: bool) {
        match owner {
            ChannelOwner::BookDeltas(instrument_id) => {
                self.active_book_delta_channels.remove(&instrument_id);
            }
            ChannelOwner::BookDepth(instrument_id) => {
                self.active_book_depth_channels.remove(&instrument_id);
            }
            ChannelOwner::Ticker {
                instrument_id,
                feed,
            } => {
                self.ticker_subscriptions(feed).remove(&instrument_id);
                if feed == TickerFeed::Quote {
                    self.quote_feed_state.lock().remove(&instrument_id);
                    self.quote_cache.lock().remove(&instrument_id);
                }

                if channel_empty {
                    self.active_ticker_channels.remove(&instrument_id);
                }
            }
            ChannelOwner::Trades(instrument_id) => {
                self.active_trade_subs.remove(&instrument_id);
            }
        }
    }

    fn ticker_subscriptions(&self, feed: TickerFeed) -> &AtomicSet<InstrumentId> {
        match feed {
            TickerFeed::Quote => &self.active_quote_subs,
            TickerFeed::Mark => &self.active_mark_subs,
            TickerFeed::Index => &self.active_index_subs,
            TickerFeed::Funding => &self.active_funding_subs,
            TickerFeed::Greeks => &self.active_greeks_subs,
        }
    }
}

impl ChannelSubscriptionRegistry {
    fn transition(&self, channel: &str) -> Arc<tokio::sync::Mutex<()>> {
        if self.transitions.len() >= TRANSITION_GC_THRESHOLD {
            self.transitions
                .retain(|_, transition| transition.strong_count() > 0);
        }

        let mut entry = self.transitions.entry(channel.to_string()).or_default();
        if let Some(transition) = entry.value().upgrade() {
            return transition;
        }

        let transition = Arc::new(tokio::sync::Mutex::new(()));
        *entry.value_mut() = Arc::downgrade(&transition);
        transition
    }

    fn clear(&self) {
        let mut state = self.state.lock();
        let next_generation = state.next_generation;
        *state = ChannelSubscriptionState {
            next_generation,
            ..Default::default()
        };
    }

    fn clear_transitions(&self) {
        self.transitions.clear();
    }
}

impl ChannelSubscriptionState {
    fn activate(&mut self, owner: ChannelOwner, channel: Option<&str>) -> Option<u64> {
        if self.owners.contains_key(&owner) {
            return None;
        }

        self.next_generation = self
            .next_generation
            .checked_add(1)
            .expect("subscription generation overflow");
        let generation = self.next_generation;

        if let Some(channel) = channel {
            self.channels
                .entry(channel.to_string())
                .or_default()
                .insert(owner);
        }
        self.owners.insert(
            owner,
            OwnedChannel {
                generation,
                channel: channel.map(ToOwned::to_owned),
            },
        );
        Some(generation)
    }

    fn attach_channel(&mut self, owner: ChannelOwner, generation: u64, channel: String) -> bool {
        let Some(owned) = self.owners.get(&owner) else {
            return false;
        };

        if owned.generation != generation {
            return false;
        }

        if let Some(active_channel) = &owned.channel {
            return active_channel == &channel;
        }

        self.owners.get_mut(&owner).expect("owner present").channel = Some(channel.clone());
        self.channels.entry(channel).or_default().insert(owner);
        true
    }

    fn is_current(&self, owner: ChannelOwner, generation: u64) -> bool {
        self.owners
            .get(&owner)
            .is_some_and(|owned| owned.generation == generation)
    }

    fn is_current_channel(&self, owner: ChannelOwner, generation: u64, channel: &str) -> bool {
        self.owners.get(&owner).is_some_and(|owned| {
            owned.generation == generation && owned.channel.as_deref() == Some(channel)
        })
    }

    fn remove_if_generation(
        &mut self,
        owner: ChannelOwner,
        generation: u64,
    ) -> Option<RemovedSubscription> {
        if !self.is_current(owner, generation) {
            return None;
        }
        self.remove(owner)
    }

    fn remove(&mut self, owner: ChannelOwner) -> Option<RemovedSubscription> {
        let owned = self.owners.remove(&owner)?;
        let channel_empty = owned.channel.as_ref().is_some_and(|channel| {
            let Some(owners) = self.channels.get_mut(channel) else {
                return true;
            };
            owners.remove(&owner);
            owners.is_empty()
        });

        if channel_empty && let Some(channel) = &owned.channel {
            self.channels.remove(channel);
        }

        Some(RemovedSubscription {
            channel: owned.channel,
            channel_empty,
        })
    }

    fn has_owners(&self, channel: &str) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|owners| !owners.is_empty())
    }
}

impl ChannelRequest {
    fn from_channel(channel: &str) -> anyhow::Result<Self> {
        if channel.starts_with("orderbook.") {
            let (instrument_name, group, depth) = orderbook_channel_parts(channel)?;
            return Ok(Self::Orderbook {
                channel: channel.to_string(),
                instrument_name,
                group,
                depth,
            });
        }

        if channel.starts_with("ticker_slim.") || channel.starts_with("ticker.") {
            let (instrument_name, interval) = ticker_channel_parts(channel)?;
            return Ok(Self::Ticker {
                channel: channel.to_string(),
                instrument_name,
                interval,
            });
        }

        if let Some((instrument_type, currency)) = channel
            .strip_prefix("trades.")
            .and_then(|value| value.split_once('.'))
        {
            return Ok(Self::Trades {
                channel: channel.to_string(),
                instrument_type: instrument_type.to_string(),
                currency: currency.to_string(),
            });
        }
        anyhow::bail!("invalid Derive subscription channel `{channel}`")
    }

    fn channel(&self) -> &str {
        match self {
            Self::Orderbook { channel, .. }
            | Self::Ticker { channel, .. }
            | Self::Trades { channel, .. } => channel,
        }
    }

    async fn subscribe(&self, ws: &DeriveWebSocketSubscriptionHandle) -> Result<(), DeriveWsError> {
        match self {
            Self::Orderbook {
                instrument_name,
                group,
                depth,
                ..
            } => {
                ws.subscribe_orderbook(instrument_name, group, depth)
                    .await?;
            }
            Self::Ticker {
                instrument_name,
                interval,
                ..
            } => ws.subscribe_ticker(instrument_name, interval).await?,
            Self::Trades {
                instrument_type,
                currency,
                ..
            } => ws.subscribe_trades(instrument_type, currency).await?,
        }
        Ok(())
    }

    async fn unsubscribe(
        &self,
        ws: &DeriveWebSocketSubscriptionHandle,
    ) -> Result<(), DeriveWsError> {
        match self {
            Self::Orderbook {
                instrument_name,
                group,
                depth,
                ..
            } => {
                ws.unsubscribe_orderbook(instrument_name, group, depth)
                    .await?;
            }
            Self::Ticker {
                instrument_name,
                interval,
                ..
            } => ws.unsubscribe_ticker(instrument_name, interval).await?,
            Self::Trades {
                instrument_type,
                currency,
                ..
            } => ws.unsubscribe_trades(instrument_type, currency).await?,
        }
        Ok(())
    }
}

fn retain_channel_for_reconnect(
    ws: &DeriveWebSocketSubscriptionHandle,
    channel: &str,
    has_surviving_owner: bool,
    cleanup_error: Option<&DeriveWsError>,
) -> bool {
    let replay = has_surviving_owner
        && cleanup_error.is_some_and(|e| {
            matches!(
                e,
                DeriveWsError::Transport(_)
                    | DeriveWsError::RequestCancelled { .. }
                    | DeriveWsError::Timeout { .. }
                    | DeriveWsError::NotConnected
            )
        });

    if replay {
        ws.remember_subscription(channel);
    } else {
        ws.forget_subscription(channel);
    }
    replay
}

impl DeriveDataClient {
    async fn join_session_tasks(&self) -> anyhow::Result<()> {
        self.session_tasks.begin_shutdown();
        self.session_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to terminate Derive data session tasks: {e}"))?;
        Ok(())
    }

    async fn join_pending_tasks(&self) -> anyhow::Result<()> {
        self.pending_tasks.begin_shutdown();
        self.pending_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to terminate Derive data tasks: {e}"))?;
        Ok(())
    }
}

fn cache_instrument(
    instruments: &Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    instrument: &InstrumentAny,
) {
    instruments.insert(instrument.id(), instrument.clone());
}

fn process_ticker_quote(
    msg: &DeriveTickerMsg,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
    quote_cache: &mut QuoteCache,
) -> anyhow::Result<Option<QuoteTick>> {
    let quote = parse_ticker_quote(msg, price_precision, size_precision, ts_init)?;
    let (bid_price, bid_size) = quote_side(quote.bid_price, quote.bid_size);
    let (ask_price, ask_size) = quote_side(quote.ask_price, quote.ask_size);

    match quote_cache.process(
        quote.instrument_id,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        quote.ts_event,
        quote.ts_init,
    ) {
        Ok(quote) => Ok(Some(quote)),
        Err(e) => {
            log::debug!(
                "Skipping partial Derive ticker quote for {}: {e}",
                msg.data.instrument_name(),
            );
            Ok(None)
        }
    }
}

/// Returns `true` when the WebSocket subscription for `state` is both
/// genuinely confirmed under the transport's current identity and recent
/// enough that the REST poller can skip this instrument.
///
/// Shared by the pre-request and post-request decisions in
/// [`poll_rest_quotes`] so the two can never drift apart: a sustained
/// upgrade outage (socket down, no successful redial yet) must suppress
/// polling in neither case, even though a now-stale `confirmed_channels`
/// entry's transport epoch happens to still match.
fn ws_quote_is_current(
    state: &QuoteFeedState,
    transport_is_active: bool,
    current_session_epoch: SessionEpoch,
    now: UnixNanos,
    max_age_nanos: u64,
) -> bool {
    transport_is_active
        && state.ws_subscription_confirmed
        && state.ws_epoch == Some(current_session_epoch)
        && state.last_ws_received.is_some_and(|received| {
            now.as_u64() >= received.as_u64() && now.as_u64() - received.as_u64() <= max_age_nanos
        })
}

#[expect(clippy::too_many_arguments)]
async fn poll_rest_quotes(
    instrument_id: InstrumentId,
    http_client: DeriveHttpClient,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    quote_cache: Arc<Mutex<QuoteCache>>,
    quote_feed_state: Arc<Mutex<AHashMap<InstrumentId, QuoteFeedState>>>,
    active_quote_subs: Arc<Mutex<AHashMap<InstrumentId, u64>>>,
    subscription_lock: Arc<Mutex<()>>,
    generation: u64,
    sender: EventSender<DataEvent>,
    clock: &'static AtomicTime,
    ws_subscription_handle: DeriveWebSocketSubscriptionHandle,
    cancellation: CancellationToken,
    poll_interval_ms: u64,
    max_quote_age_secs: u64,
) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_millis(poll_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let max_age_nanos = Duration::from_secs(max_quote_age_secs).as_nanos() as u64;
    let mut rest_poll_attempts: u64 = 0;
    let mut has_ever_published = false;

    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            _ = interval.tick() => {}
        }

        if !quote_poll_generation_is_current(&active_quote_subs, instrument_id, generation) {
            return Ok(());
        }

        let now = clock.get_time_ns();
        let current_session_epoch = ws_subscription_handle.current_session_epoch();
        let transport_is_active = ws_subscription_handle.is_active();
        let ws_is_current = quote_feed_state
            .lock()
            .get(&instrument_id)
            .is_some_and(|state| {
                ws_quote_is_current(
                    state,
                    transport_is_active,
                    current_session_epoch,
                    now,
                    max_age_nanos,
                )
            });
        if ws_is_current {
            continue;
        }

        rest_poll_attempts += 1;
        if rest_poll_failure_warning_due(rest_poll_attempts, has_ever_published) {
            log::warn!(
                "Derive REST market-data polling for {instrument_id} has not produced a single \
                 quote after {rest_poll_attempts} attempts; the instrument has no fresh quote \
                 from any source despite the client reporting connected",
            );
        }

        let Some(instrument) = instruments.get_cloned(&instrument_id) else {
            continue;
        };
        let instrument_name = match format_venue_symbol(&instrument_id) {
            Ok(name) => name.to_string(),
            Err(e) => {
                log::warn!("Cannot poll Derive quote for {instrument_id}: {e}");
                continue;
            }
        };

        let poll_started = Instant::now();
        let ticker = match http_client
            .get_ticker_for_instrument(&instrument_name)
            .await
        {
            Ok(ticker) => ticker,
            Err(e) => {
                log::warn!("Failed to poll Derive REST quote for {instrument_id}: {e}");
                continue;
            }
        };
        if !quote_poll_generation_is_current(&active_quote_subs, instrument_id, generation) {
            return Ok(());
        }
        let ts_init = clock.get_time_ns();
        let quote = match parse_ticker_quote_from_rest(
            &ticker,
            instrument.price_precision(),
            instrument.size_precision(),
            ts_init,
        ) {
            Ok(quote) => quote,
            Err(e) => {
                log::warn!("Failed to parse Derive REST quote for {instrument_id}: {e}");
                continue;
            }
        };
        if quote.instrument_id != instrument_id {
            log::warn!("Skipping Derive REST quote with unexpected instrument for {instrument_id}");
            continue;
        }
        if quote.bid_price.as_decimal() <= Decimal::ZERO
            || quote.ask_price.as_decimal() <= Decimal::ZERO
            || quote.bid_size.is_zero()
            || quote.ask_size.is_zero()
            || quote.bid_price >= quote.ask_price
        {
            log::warn!("Skipping invalid Derive REST quote for {instrument_id}");
            continue;
        }
        if !quote_is_fresh(quote.ts_event, ts_init, Some(max_quote_age_secs)) {
            log::debug!("Skipping stale or future Derive REST quote for {instrument_id}");
            continue;
        }

        let _subscription_guard = subscription_lock.lock();
        if !quote_poll_generation_is_current(&active_quote_subs, instrument_id, generation) {
            return Ok(());
        }
        let mut feed_states = quote_feed_state.lock();
        let now = clock.get_time_ns();
        let current_session_epoch = ws_subscription_handle.current_session_epoch();
        let transport_is_active = ws_subscription_handle.is_active();
        let ws_is_current = feed_states.get(&instrument_id).is_some_and(|state| {
            ws_quote_is_current(
                state,
                transport_is_active,
                current_session_epoch,
                now,
                max_age_nanos,
            )
        });
        if ws_is_current {
            continue;
        }
        let state = feed_states.entry(instrument_id).or_default();
        if state
            .last_published_event
            .is_some_and(|last_event| quote.ts_event <= last_event)
        {
            log::debug!("Skipping non-monotonic Derive REST quote for {instrument_id}");
            continue;
        }
        let quote = match quote_cache.lock().process(
            instrument_id,
            Some(quote.bid_price),
            Some(quote.ask_price),
            Some(quote.bid_size),
            Some(quote.ask_size),
            quote.ts_event,
            quote.ts_init,
        ) {
            Ok(quote) => quote,
            Err(e) => {
                log::warn!("Failed to construct Derive REST quote for {instrument_id}: {e}");
                continue;
            }
        };

        state.last_published_event = Some(quote.ts_event);
        has_ever_published = true;
        if !state.rest_polling {
            let age_nanos = ts_init.as_u64().saturating_sub(quote.ts_event.as_u64());
            let poll_latency_ms = poll_started.elapsed().as_millis();
            log::info!(
                "Derive quote source changed to rest_polling for {instrument_id}; quote_age_ns={age_nanos}; poll_latency_ms={poll_latency_ms}; reason=websocket_quote_stale"
            );
            state.rest_polling = true;
        }
        if let Err(e) = sender.send(DataEvent::Data(Data::Quote(quote))) {
            log::error!("Failed to publish Derive REST quote for {instrument_id}: {e}");
        }
    }
}

fn quote_poll_generation_is_current(
    generations: &Arc<Mutex<AHashMap<InstrumentId, u64>>>,
    instrument_id: InstrumentId,
    generation: u64,
) -> bool {
    generations.lock().get(&instrument_id) == Some(&generation)
}

fn remove_quote_poll_generation(
    generations: &Arc<Mutex<AHashMap<InstrumentId, u64>>>,
    instrument_id: InstrumentId,
    generation: u64,
) {
    let mut generations = generations.lock();
    if generations.get(&instrument_id) == Some(&generation) {
        generations.remove(&instrument_id);
    }
}

fn quote_is_fresh(ts_event: UnixNanos, ts_now: UnixNanos, max_quote_age_secs: Option<u64>) -> bool {
    let Some(max_age_secs) = max_quote_age_secs else {
        return true;
    };
    let max_age_nanos = Duration::from_secs(max_age_secs).as_nanos() as u64;
    ts_event.as_u64() <= ts_now.as_u64() && ts_now.as_u64() - ts_event.as_u64() <= max_age_nanos
}

fn quote_side(price: Price, size: Quantity) -> (Option<Price>, Option<Quantity>) {
    if price.is_zero() || size.is_zero() {
        (None, None)
    } else {
        (Some(price), Some(size))
    }
}

fn channel_is_active(
    channels: &AtomicMap<InstrumentId, String>,
    instrument_id: InstrumentId,
    channel: &str,
) -> bool {
    channels
        .get_cloned(&instrument_id)
        .is_some_and(|active_channel| active_channel == channel)
}

fn orderbook_channel_parts(channel: &str) -> anyhow::Result<(String, String, String)> {
    let rest = channel
        .strip_prefix("orderbook.")
        .ok_or_else(|| anyhow::anyhow!("invalid Derive orderbook channel `{channel}`"))?;
    let mut parts = rest.rsplitn(3, '.');
    let depth = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid Derive orderbook channel `{channel}`"))?;
    let group = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid Derive orderbook channel `{channel}`"))?;
    let instrument_name = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("invalid Derive orderbook channel `{channel}`"))?;

    Ok((
        instrument_name.to_string(),
        group.to_string(),
        depth.to_string(),
    ))
}

fn ticker_channel_parts(channel: &str) -> anyhow::Result<(String, String)> {
    let rest = channel
        .strip_prefix("ticker_slim.")
        .or_else(|| channel.strip_prefix("ticker."))
        .ok_or_else(|| anyhow::anyhow!("invalid Derive ticker channel `{channel}`"))?;
    let (instrument_name, interval) = rest
        .rsplit_once('.')
        .ok_or_else(|| anyhow::anyhow!("invalid Derive ticker channel `{channel}`"))?;
    anyhow::ensure!(
        !instrument_name.is_empty() && !interval.is_empty(),
        "invalid Derive ticker channel `{channel}`"
    );

    Ok((instrument_name.to_string(), interval.to_string()))
}

fn orderbook_group(params: &Option<Params>) -> anyhow::Result<String> {
    let group = params
        .as_ref()
        .and_then(|p| {
            p.get_str("group")
                .map(ToOwned::to_owned)
                .or_else(|| p.get_u64("group").map(|value| value.to_string()))
        })
        .unwrap_or_else(|| DEFAULT_ORDERBOOK_GROUP.to_string());

    DeriveOrderbookGroup::from_str(&group)
        .with_context(|| format!("invalid Derive orderbook group `{group}`"))?;
    Ok(group)
}

fn orderbook_depth(depth: Option<usize>, params: &Option<Params>) -> anyhow::Result<String> {
    let depth = depth
        .map(|value| value.to_string())
        .or_else(|| {
            params.as_ref().and_then(|p| {
                p.get_str("depth")
                    .map(ToOwned::to_owned)
                    .or_else(|| p.get_u64("depth").map(|value| value.to_string()))
            })
        })
        .unwrap_or_else(|| DEFAULT_ORDERBOOK_DEPTH.to_string());

    DeriveOrderbookDepth::from_str(&depth)
        .with_context(|| format!("invalid Derive orderbook depth `{depth}`"))?;
    Ok(depth)
}

fn ticker_interval(params: &Option<Params>) -> anyhow::Result<String> {
    let interval = params
        .as_ref()
        .and_then(|p| {
            p.get_str("interval")
                .map(ToOwned::to_owned)
                .or_else(|| p.get_u64("interval").map(|value| value.to_string()))
        })
        .unwrap_or_else(|| DEFAULT_TICKER_INTERVAL.to_string());

    DeriveTickerInterval::from_str(&interval)
        .with_context(|| format!("invalid Derive ticker interval `{interval}`"))?;
    Ok(interval)
}

fn trade_channel(instrument: &InstrumentAny) -> anyhow::Result<String> {
    let instrument_type = derive_instrument_type(instrument)?.to_string();
    let instrument_id = instrument.id();
    let currency = currency_from_instrument_id(&instrument_id)?;
    Ok(trades_channel(&instrument_type, currency))
}

/// How many consecutive REST poll attempts elapse between "this instrument
/// has never produced a quote" warnings, once polling is actually due (i.e.
/// excluding attempts the WS-is-current check short-circuits).
const REST_POLL_FAILURE_WARNING_EVERY: u64 = 20;

/// Returns `true` when a periodic "REST polling has never produced a
/// quote" warning is due for this attempt count.
///
/// A one-way latch: once `has_ever_published` is `true`, the instrument is
/// no longer in a total-failure state by this definition and the warning
/// never fires again, even if a later individual poll fails.
fn rest_poll_failure_warning_due(attempts: u64, has_ever_published: bool) -> bool {
    !has_ever_published && attempts > 0 && attempts.is_multiple_of(REST_POLL_FAILURE_WARNING_EVERY)
}

/// Fraction of the documented non-matching rate budget that REST
/// market-data polling alone may consume before warning. Advisory, not a
/// hard limit: market-data polling and execution's own private-state
/// reconciliation polling draw from the same per-wallet/IP budget
/// (`crate::common::rate_limit`), and this client cannot see execution's
/// share of it, so half the budget is a conservative threshold for
/// warning about this client's own contribution alone.
const REST_POLL_BUDGET_WARNING_FRACTION: f64 = 0.5;

/// Returns `true` when polling `instrument_count` instruments every
/// `poll_interval_ms` would alone exceed `budget_fraction` of the
/// documented non-matching TPS allowance.
fn rest_poll_budget_exceeded(
    instrument_count: usize,
    poll_interval_ms: u64,
    budget_fraction: f64,
) -> bool {
    if poll_interval_ms == 0 {
        return instrument_count > 0;
    }
    let requests_per_sec = instrument_count as f64 / (poll_interval_ms as f64 / 1_000.0);
    requests_per_sec > f64::from(DERIVE_NON_MATCHING_TPS) * budget_fraction
}

/// Grows `current` by the shared reconnect backoff factor, capped at the
/// shared maximum — the same growth `execution.rs`'s WebSocket retry loop
/// uses. A pure function so the retry schedule is unit-testable without
/// waiting out real sleeps.
fn next_reconnect_delay(current: Duration) -> Duration {
    current
        .mul_f64(RECONNECT_BACKOFF_FACTOR)
        .min(RECONNECT_MAX_BACKOFF)
}

/// Rebuilds the channel list a freshly built transport needs resubscribed.
///
/// A transport rebuild (full teardown + `connect()`, as opposed to the
/// generic transport's own in-place reconnect) starts with no subscriptions
/// at all: `DeriveWebSocketClient::teardown` clears its desired-subscription
/// set because the venue session ended with the transport. Only an in-place
/// reconnect replays subscriptions automatically (`recover_session`), so
/// whoever owns a transport rebuild must re-derive and reissue every
/// currently active channel from the data client's own subscription registry
/// or WS data silently stops for the rest of the process.
fn active_channels_for_replay(ctx: &WsMessageContext) -> Vec<String> {
    let mut channels: Vec<String> = ctx
        .active_ticker_channels
        .load()
        .values()
        .cloned()
        .chain(ctx.active_book_delta_channels.load().values().cloned())
        .chain(ctx.active_book_depth_channels.load().values().cloned())
        .collect();

    for instrument_id in ctx.active_trade_subs.load().iter() {
        let Some(instrument) = ctx.instruments.get_cloned(instrument_id) else {
            continue;
        };
        match trade_channel(&instrument) {
            Ok(channel) => channels.push(channel),
            Err(e) => {
                log::warn!("Cannot rebuild Derive trade channel for {instrument_id}: {e}");
            }
        }
    }

    channels
}

fn derive_instrument_type(instrument: &InstrumentAny) -> anyhow::Result<DeriveInstrumentType> {
    match instrument {
        InstrumentAny::CryptoPerpetual(_) => Ok(DeriveInstrumentType::Perp),
        InstrumentAny::CryptoOption(_) => Ok(DeriveInstrumentType::Option),
        InstrumentAny::CurrencyPair(_) => Ok(DeriveInstrumentType::Erc20),
        other => anyhow::bail!("unsupported Derive instrument type for trades: {other:?}"),
    }
}

fn currency_from_instrument_id(instrument_id: &InstrumentId) -> anyhow::Result<&str> {
    anyhow::ensure!(
        instrument_id.venue == *DERIVE_VENUE,
        "instrument ID `{instrument_id}` is not for venue {}",
        DERIVE_VENUE.as_str(),
    );

    instrument_id
        .symbol
        .as_str()
        .split_once('-')
        .and_then(|(currency, _)| (!currency.is_empty()).then_some(currency))
        .ok_or_else(|| anyhow::anyhow!("cannot derive currency from {instrument_id}"))
}

// Caps the rendered JSON at ~512 bytes for log grep-ability and backs the
// slice off to a UTF-8 char boundary so a multi-byte codepoint near the cap
// can never produce a panicking slice.
fn truncated_payload_snippet(raw: &str) -> String {
    const MAX_LEN: usize = 512;
    if raw.len() <= MAX_LEN {
        return raw.to_string();
    }
    let mut end = MAX_LEN;
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...(truncated)", &raw[..end])
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use nautilus_common::{live::runner::replace_data_event_sender, testing::wait_until_async};
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        common::{
            consts::DERIVE_CLIENT_ID, enums::DeriveEnvironment, parse::parse_derive_instrument_any,
        },
        http::models::DeriveInstrument,
        websocket::{DeriveWsFrame, WsSubscriptionPayload},
    };

    fn data_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_data")
    }

    fn load_json(filename: &str) -> Value {
        let content = std::fs::read_to_string(data_path().join(filename))
            .unwrap_or_else(|_| panic!("failed to read {filename}"));
        serde_json::from_str(&content).expect("invalid json")
    }

    #[rstest]
    fn test_next_reconnect_delay_grows_and_caps_at_the_shared_maximum() {
        // A sustained upgrade outage must not keep dialling at the base
        // interval forever (≈14k attempts/hour against an already-503ing
        // venue); it must grow like `execution.rs`'s retry loop and
        // eventually plateau at the shared cap.
        let mut delay = RECONNECT_BASE_BACKOFF;
        let mut delays = vec![delay];
        for _ in 0..10 {
            delay = next_reconnect_delay(delay);
            delays.push(delay);
        }

        assert!(
            delays.windows(2).all(|pair| pair[1] >= pair[0]),
            "delay must never shrink between attempts: {delays:?}",
        );
        assert!(
            delays[1] > RECONNECT_BASE_BACKOFF,
            "the second attempt must already back off past the base interval",
        );
        assert_eq!(
            *delays.last().unwrap(),
            RECONNECT_MAX_BACKOFF,
            "growth must plateau at the shared cap rather than diverging",
        );
    }

    #[rstest]
    fn test_rest_poll_failure_warning_due_fires_periodically_until_first_success() {
        // Readiness that hides a total REST-polling failure forever is a
        // production-blind-spot risk; this must fire at a predictable
        // cadence while nothing has ever published, and never once
        // something has.
        let fires: Vec<bool> = (0..=REST_POLL_FAILURE_WARNING_EVERY * 2)
            .map(|attempts| rest_poll_failure_warning_due(attempts, false))
            .collect();

        assert!(!fires[0], "attempt 0 (no attempt yet) must not warn");
        assert!(fires[REST_POLL_FAILURE_WARNING_EVERY as usize]);
        assert!(fires[(REST_POLL_FAILURE_WARNING_EVERY * 2) as usize]);
        assert_eq!(
            fires.iter().filter(|&&due| due).count(),
            2,
            "must fire exactly on multiples of the interval: {fires:?}",
        );

        assert!(
            !rest_poll_failure_warning_due(REST_POLL_FAILURE_WARNING_EVERY, true),
            "must never fire once a quote has ever been published",
        );
    }

    #[rstest]
    fn test_rest_poll_budget_exceeded_flags_half_the_documented_non_matching_tps() {
        // 5 TPS documented non-matching budget; this client alone must not
        // exceed half of it (2.5 req/s), leaving headroom for execution's
        // own private-state polling sharing the same per-wallet/IP budget.
        // 10 instruments at a 2s interval is 5 req/s, over the half-budget
        // threshold.
        assert!(rest_poll_budget_exceeded(
            10,
            2_000,
            REST_POLL_BUDGET_WARNING_FRACTION
        ));
        // 10 instruments at a 10s interval is 1 req/s, comfortably under.
        assert!(!rest_poll_budget_exceeded(
            10,
            10_000,
            REST_POLL_BUDGET_WARNING_FRACTION
        ));
        // No instruments polled yet: never exceeded.
        assert!(!rest_poll_budget_exceeded(
            0,
            20_000,
            REST_POLL_BUDGET_WARNING_FRACTION
        ));
    }

    #[rstest]
    fn test_truncated_payload_snippet_short_payload_is_unchanged() {
        let short = json!({"ok": true}).to_string();
        assert_eq!(truncated_payload_snippet(&short), r#"{"ok":true}"#);
    }

    #[rstest]
    fn test_truncated_payload_snippet_truncates_long_ascii_payload() {
        let big = json!({"msg": "x".repeat(1024)}).to_string();
        let snippet = truncated_payload_snippet(&big);
        assert!(snippet.ends_with("...(truncated)"));
        // Body before the suffix capped near the 512-byte target.
        assert!(snippet.len() <= 512 + "...(truncated)".len());
    }

    #[rstest]
    fn test_truncated_payload_snippet_handles_multibyte_at_boundary() {
        // Construct a payload where MAX_LEN (512) lands *inside* a UTF-8
        // codepoint. `{"a":"x` is 7 bytes (odd-aligned), each 'e' with acute
        // accent (U+00E9) is 2 bytes, and 7 + 252*2 = 511, so byte 512 falls
        // inside the 253rd accented char. Under the old fixed-byte slice
        // `&raw[..512]` would panic with "byte index 512 is not a char
        // boundary"; with the backoff it must return a valid `String`
        // strictly shorter than 512 bytes for the body.
        let value: String = format!("x{}", "\u{00E9}".repeat(1024));
        let big = json!({"a": value});
        let raw = big.to_string();
        assert!(
            !raw.is_char_boundary(512),
            "test premise: 512 must be mid-codepoint",
        );

        let snippet = truncated_payload_snippet(&raw);
        assert!(snippet.ends_with("...(truncated)"));
        let body_len = snippet.len() - "...(truncated)".len();
        assert!(body_len <= 512);
    }

    fn subscription_payload(channel: &str, data: &Value) -> WsSubscriptionPayload {
        let frame = json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": channel,
                "data": data
            }
        });

        match DeriveWsFrame::parse(&frame.to_string()).unwrap() {
            DeriveWsFrame::Subscription(payload) => payload,
            other => panic!("expected subscription frame, was {other:?}"),
        }
    }

    fn make_ctx(
        instrument: Option<InstrumentAny>,
    ) -> (
        WsMessageContext,
        tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    ) {
        let (data_sender, data_rx) = tokio::sync::mpsc::unbounded_channel();
        let instruments = Arc::new(AtomicMap::new());
        let quote_feed_state = Arc::new(Mutex::new(AHashMap::new()));
        let ws_client =
            DeriveWebSocketClient::new(None, DeriveEnvironment::Mainnet, Default::default(), None);
        if let Some(instrument) = instrument {
            quote_feed_state
                .lock()
                .entry(instrument.id())
                .or_insert_with(|| QuoteFeedState {
                    ws_subscription_confirmed: true,
                    // Matches a freshly constructed (never reconnected) WS
                    // client's current transport/session identity, so this
                    // pre-confirmed state is session-consistent rather than
                    // immediately fenced as stale by the identity check in
                    // `handle_public_ws_data`.
                    ws_epoch: Some(ws_client.subscription_handle().current_session_epoch()),
                    ..QuoteFeedState::default()
                });
            cache_instrument(&instruments, &instrument);
        }

        (
            WsMessageContext {
                clock: get_atomic_clock_realtime(),
                data_sender: data_sender.into(),
                instruments,
                active_book_delta_channels: Arc::new(AtomicMap::new()),
                active_book_depth_channels: Arc::new(AtomicMap::new()),
                active_ticker_channels: Arc::new(AtomicMap::new()),
                active_quote_subs: Arc::new(AtomicSet::new()),
                active_trade_subs: Arc::new(AtomicSet::new()),
                active_mark_subs: Arc::new(AtomicSet::new()),
                active_index_subs: Arc::new(AtomicSet::new()),
                active_funding_subs: Arc::new(AtomicSet::new()),
                active_greeks_subs: Arc::new(AtomicSet::new()),
                subscription_lock: Arc::new(Mutex::new(())),
                quote_cache: Arc::new(Mutex::new(QuoteCache::new())),
                quote_feed_state,
                ws_session_generation: Arc::new(AtomicU64::new(0)),
                ws_subscription_handle: ws_client.subscription_handle(),
                max_quote_age_secs: None,
            },
            data_rx,
        )
    }

    fn perp_instrument() -> InstrumentAny {
        parse_derive_instrument_any(&perp_definition("ETH-PERP", "ETH"), UnixNanos::from(1))
            .unwrap()
            .unwrap()
    }

    fn btc_perp_instrument() -> InstrumentAny {
        parse_derive_instrument_any(&perp_definition("BTC-PERP", "BTC"), UnixNanos::from(1))
            .unwrap()
            .unwrap()
    }

    fn perp_definition(name: &str, currency: &str) -> DeriveInstrument {
        let mut value = load_json("perps/instrument_eth.json");
        value["base_currency"] = json!(currency);
        value["instrument_name"] = json!(name);
        value["perp_details"]["index"] = json!(format!("{currency}-USD"));

        serde_json::from_value(value).unwrap()
    }

    fn option_instrument() -> InstrumentAny {
        let definition: DeriveInstrument =
            serde_json::from_value(load_json("options/instrument_eth.json")).unwrap();
        parse_derive_instrument_any(&definition, UnixNanos::from(1))
            .unwrap()
            .unwrap()
    }

    fn spot_instrument() -> InstrumentAny {
        let definition: DeriveInstrument =
            serde_json::from_value(load_json("spot/instrument_eth.json")).unwrap();
        parse_derive_instrument_any(&definition, UnixNanos::from(1))
            .unwrap()
            .unwrap()
    }

    fn ticker_json(timestamp: i64) -> Value {
        let mut value = load_json("perps/ws_ticker_eth.json");
        value["timestamp"] = json!(timestamp);
        value
    }

    fn option_ticker_json(timestamp: i64) -> Value {
        let mut value = load_json("options/http_ticker_eth_snapshot.json");
        value["timestamp"] = json!(timestamp);
        value
    }

    fn spot_ticker_slim_json() -> Value {
        load_json("spot/ws_ticker_slim_eth.json")
    }

    fn orderbook_json() -> Value {
        load_json("perps/ws_orderbook_eth.json")
    }

    fn trade_json(instrument_name: &str, trade_id: &str) -> Value {
        let mut value = load_json("perps/ws_trade_eth.json");
        value["instrument_name"] = json!(instrument_name);
        value["trade_id"] = json!(trade_id);
        value
    }

    #[rstest]
    fn test_handle_ticker_subscription_emits_quote_with_instrument_precision() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        ctx.active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.1000".to_string());
        ctx.active_quote_subs.insert(instrument_id);
        let payload = subscription_payload(
            "ticker_slim.ETH-PERP.1000",
            &json!({
                "timestamp": 1_700_000_000_010_i64,
                "instrument_ticker": ticker_json(1_700_000_000_000)
            }),
        );

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::Quote(quote)) => {
                assert_eq!(quote.instrument_id, instrument_id);
                assert_eq!(quote.bid_price, Price::from("3500.00"));
                assert_eq!(quote.ask_price, Price::from("3501.00"));
                assert_eq!(quote.bid_size, Quantity::from("1.000"));
                assert_eq!(quote.ask_size, Quantity::from("2.000"));
                assert_eq!(quote.bid_price.precision, 2);
                assert_eq!(quote.bid_size.precision, 3);
            }
            other => panic!("expected quote data event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_handle_ticker_delivers_quote_without_confirmation_when_failover_disabled() {
        // REST failover off (`max_quote_age_secs: None`, `make_ctx`'s
        // default) must behave byte-for-byte like the pre-change WS path:
        // an ack that never confirmed the channel (an unusual venue status,
        // a server that doesn't track confirmations) must still deliver
        // quotes rather than being silently dropped by the failover-only
        // confirmation fence.
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        // Override `make_ctx`'s pre-seeded `ws_subscription_confirmed: true`
        // to simulate a channel the handle never actually confirmed.
        ctx.quote_feed_state
            .lock()
            .get_mut(&instrument_id)
            .unwrap()
            .ws_subscription_confirmed = false;
        ctx.active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.1000".to_string());
        ctx.active_quote_subs.insert(instrument_id);
        let payload = subscription_payload(
            "ticker_slim.ETH-PERP.1000",
            &json!({
                "timestamp": 1_700_000_000_010_i64,
                "instrument_ticker": ticker_json(1_700_000_000_000)
            }),
        );

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::Quote(quote)) => assert_eq!(quote.instrument_id, instrument_id),
            other => panic!(
                "expected quote data event even without a confirmed WS subscription, \
                 was {other:?}"
            ),
        }
    }

    #[rstest]
    fn test_ws_message_context_disables_quote_age_bound_when_failover_flag_is_off() {
        // F11: disabling `rest_market_data_failover_enabled` must restore
        // byte-for-byte WS-only behavior even if the operator leaves the
        // bounds configured (e.g. toggling the flag off without clearing
        // them). The effective bound must derive from the enable flag,
        // not merely from whether a bound happens to be set -- otherwise
        // every failover-specific gate downstream (`ctx.max_quote_age_secs.is_some()`)
        // stays active for an operator who believes they disabled the
        // workaround.
        let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(data_tx);
        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Mainnet,
            rest_market_data_failover_enabled: false,
            rest_market_data_max_quote_age_secs: Some(60),
            ..Default::default()
        };
        let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config).unwrap();

        let ctx = client.ws_message_context();

        assert!(
            ctx.max_quote_age_secs.is_none(),
            "max_quote_age_secs must be None when the failover flag is off, \
             regardless of a configured bound",
        );
    }

    #[rstest]
    fn test_handle_ticker_partial_quote_without_cache_emits_no_quote() {
        let instrument = spot_instrument();
        let instrument_id = instrument.id();
        let channel = "ticker_slim.ETH-USDC.1000";
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, channel);
        ctx.active_quote_subs.insert(instrument_id);
        let payload = subscription_payload(channel, &spot_ticker_slim_json());

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_handle_ticker_partial_quote_uses_cached_side() {
        let instrument = spot_instrument();
        let instrument_id = instrument.id();
        let channel = "ticker_slim.ETH-USDC.1000";
        let (ctx, mut rx) = make_ctx(Some(instrument));
        let cached_quote = QuoteTick::new(
            instrument_id,
            Price::from("0.1"),
            Price::from("0.3"),
            Quantity::from("10.00"),
            Quantity::from("20.00"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        ctx.quote_cache.lock().insert(instrument_id, cached_quote);
        install_ticker(&ctx, instrument_id, channel);
        ctx.active_quote_subs.insert(instrument_id);
        let payload = subscription_payload(channel, &spot_ticker_slim_json());

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::Quote(quote)) => {
                assert_eq!(quote.instrument_id, instrument_id);
                assert_eq!(quote.bid_price, Price::from("0.2"));
                assert_eq!(quote.ask_price, Price::from("0.3"));
                assert_eq!(quote.bid_size, Quantity::from("45.00"));
                assert_eq!(quote.ask_size, Quantity::from("20.00"));
            }
            other => panic!("expected quote data event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_handle_reconnected_clears_quote_cache() {
        let instrument = spot_instrument();
        let instrument_id = instrument.id();
        let channel = "ticker_slim.ETH-USDC.1000";
        let (ctx, mut rx) = make_ctx(Some(instrument));
        let cached_quote = QuoteTick::new(
            instrument_id,
            Price::from("0.1"),
            Price::from("0.3"),
            Quantity::from("10.00"),
            Quantity::from("20.00"),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        ctx.quote_cache.lock().insert(instrument_id, cached_quote);
        install_ticker(&ctx, instrument_id, channel);
        ctx.active_quote_subs.insert(instrument_id);

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Reconnected, &ctx);
        let payload = subscription_payload(channel, &spot_ticker_slim_json());
        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_invalidate_quote_feed_state_resets_confirmation_at_rebuild_boundary() {
        // F6: the retry owner's automatic transport rebuild has no
        // `Reconnected` message to trigger the existing in-place-reconnect
        // reset (that message only fires for a low-level reconnect within
        // the same transport), so it calls this shared helper directly.
        // Prove it performs the identical reset: a stale confirmation,
        // transport/session identity and receipt-time evidence from a
        // now-dead transport must not survive a rebuild boundary either.
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, _rx) = make_ctx(Some(instrument));
        {
            let mut feed_state = ctx.quote_feed_state.lock();
            let state = feed_state.entry(instrument_id).or_default();
            state.ws_subscription_confirmed = true;
            state.ws_epoch = Some(SessionEpoch::for_test(1, 0));
            state.last_ws_received = Some(UnixNanos::from(1));
        }
        let generation_before = ctx.ws_session_generation.load(Ordering::Acquire);

        DeriveDataClient::invalidate_quote_feed_state(&ctx);

        let feed_state = ctx.quote_feed_state.lock();
        let state = feed_state.get(&instrument_id).unwrap();
        assert!(
            !state.ws_subscription_confirmed,
            "a stale confirmation must not survive a rebuild boundary",
        );
        assert!(
            state.ws_epoch.is_none(),
            "the prior transport's session identity must not survive a rebuild boundary",
        );
        assert!(
            state.last_ws_received.is_none(),
            "stale receipt-time evidence must not survive a rebuild boundary",
        );
        assert!(
            ctx.ws_session_generation.load(Ordering::Acquire) > generation_before,
            "the session generation must advance at a rebuild boundary too",
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_session_recovery_failure_marks_client_disconnected() {
        let (data_tx, _data_rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(data_tx);
        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Mainnet,
            ..Default::default()
        };
        let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config).unwrap();
        let (ws_tx, ws_rx) = tokio::sync::mpsc::unbounded_channel();
        client.is_connected.store(true, Ordering::Release);
        client.spawn_stream_task(ws_rx).unwrap();

        ws_tx
            .send(DeriveWsMessage::SessionRecoveryFailed(
                "subscription replay failed".to_string(),
            ))
            .unwrap();
        wait_until_async(|| async { !client.is_connected() }, Duration::from_secs(2)).await;

        assert!(!client.is_connected());

        client.cancellation_token.cancel();
    }

    #[rstest]
    fn test_handle_orderbook_subscription_emits_snapshot_deltas() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        ctx.active_book_delta_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        let payload = subscription_payload("orderbook.ETH-PERP.1.10", &orderbook_json());

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::BookDeltas(deltas)) => {
                assert_eq!(deltas.instrument_id, instrument_id);
                assert_eq!(deltas.deltas.len(), 3);
                assert_eq!(deltas.deltas[1].order.price, Price::from("3500.00"));
                assert_eq!(deltas.deltas[1].order.size, Quantity::from("1.000"));
                assert_eq!(deltas.deltas[2].order.price, Price::from("3501.00"));
                assert_eq!(deltas.deltas[2].order.size, Quantity::from("2.000"));
            }
            other => panic!("expected deltas data event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_handle_orderbook_subscription_emits_for_depth_subscription() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        ctx.active_book_depth_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        let payload = subscription_payload("orderbook.ETH-PERP.1.10", &orderbook_json());

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::BookDepth(depth)) => {
                assert_eq!(depth.instrument_id, instrument_id);
                assert_eq!(depth.bids[0].price, Price::from("3500.00"));
                assert_eq!(depth.bids[0].size, Quantity::from("1.000"));
                assert_eq!(depth.asks[0].price, Price::from("3501.00"));
                assert_eq!(depth.asks[0].size, Quantity::from("2.000"));
            }
            other => panic!("expected depth data event, was {other:?}"),
        }
    }

    #[rstest]
    fn test_orderbook_frame_ignored_for_inactive_channel() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        ctx.active_book_delta_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.20".to_string());
        let payload = subscription_payload("orderbook.ETH-PERP.1.10", &orderbook_json());

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_handle_trades_subscription_filters_and_emits_active_instrument() {
        let instrument = perp_instrument();
        let other = btc_perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        cache_instrument(&ctx.instruments, &other);
        ctx.active_trade_subs.insert(instrument_id);
        let payload = subscription_payload(
            "trades.perp.ETH",
            &json!([
                trade_json("ETH-PERP", "trade-1"),
                trade_json("BTC-PERP", "trade-2")
            ]),
        );

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::Trade(trade)) => {
                assert_eq!(trade.instrument_id, instrument_id);
                assert_eq!(trade.trade_id.to_string(), "trade-1");
                assert_eq!(trade.price, Price::from("3500.00"));
                assert_eq!(trade.size, Quantity::from("1.000"));
            }
            other => panic!("expected trade data event, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_handle_subscription_without_cached_instrument_emits_no_event() {
        let (ctx, mut rx) = make_ctx(None);
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        ctx.active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.1000".to_string());
        ctx.active_quote_subs.insert(instrument_id);
        let payload =
            subscription_payload("ticker_slim.ETH-PERP.1000", &ticker_json(1_700_000_000_000));

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_frame_ignored_without_quote_subscription() {
        let instrument = perp_instrument();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        let payload =
            subscription_payload("ticker_slim.ETH-PERP.1000", &ticker_json(1_700_000_000_000));

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_frame_ignored_for_inactive_channel() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        ctx.active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.100".to_string());
        ctx.active_quote_subs.insert(instrument_id);
        let payload =
            subscription_payload("ticker_slim.ETH-PERP.1000", &ticker_json(1_700_000_000_000));

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_trade_channel_uses_instrument_type_and_currency() {
        let instrument = perp_instrument();

        assert_eq!(trade_channel(&instrument).unwrap(), "trades.perp.ETH");
    }

    #[rstest]
    fn test_trade_channel_uses_erc20_for_spot() {
        let instrument = spot_instrument();

        assert_eq!(
            derive_instrument_type(&instrument).unwrap(),
            DeriveInstrumentType::Erc20
        );
        assert_eq!(trade_channel(&instrument).unwrap(), "trades.erc20.ETH");
    }

    #[rstest]
    fn test_param_defaults_match_derive_public_channels() {
        assert_eq!(orderbook_group(&None).unwrap(), DEFAULT_ORDERBOOK_GROUP);
        assert_eq!(
            orderbook_depth(None, &None).unwrap(),
            DEFAULT_ORDERBOOK_DEPTH
        );
        assert_eq!(ticker_interval(&None).unwrap(), DEFAULT_TICKER_INTERVAL);
    }

    #[rstest]
    fn test_orderbook_channel_parts_splits_from_right() {
        assert_eq!(
            orderbook_channel_parts("orderbook.ETH.TEST-PERP.10.100").unwrap(),
            (
                "ETH.TEST-PERP".to_string(),
                "10".to_string(),
                "100".to_string()
            )
        );
    }

    #[rstest]
    fn test_ticker_channel_parts_splits_from_right() {
        assert_eq!(
            ticker_channel_parts("ticker_slim.ETH.TEST-PERP.1000").unwrap(),
            ("ETH.TEST-PERP".to_string(), "1000".to_string())
        );
    }

    #[rstest]
    fn test_ticker_channel_parts_accepts_legacy_ticker_channel() {
        assert_eq!(
            ticker_channel_parts("ticker.ETH.TEST-PERP.1000").unwrap(),
            ("ETH.TEST-PERP".to_string(), "1000".to_string())
        );
    }

    #[rstest]
    fn test_stale_generation_rollback_preserves_resubscription() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let lifecycle = client.subscription_lifecycle();
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        let owner = ChannelOwner::Ticker {
            instrument_id,
            feed: TickerFeed::Quote,
        };
        let channel = "ticker_slim.ETH-PERP.1000".to_string();

        let first_generation = lifecycle.activate(owner, Some(&channel)).unwrap();
        lifecycle.remove(owner).unwrap();
        let second_generation = lifecycle.activate(owner, Some(&channel)).unwrap();

        assert!(!lifecycle.rollback(owner, first_generation));
        assert!(lifecycle.is_current_channel(owner, second_generation, &channel));
        assert!(client.active_quote_subs.contains(&instrument_id));
        assert!(channel_is_active(
            &client.active_ticker_channels,
            instrument_id,
            &channel,
        ));
    }

    #[rstest]
    fn test_subscription_registry_clear_does_not_reuse_generation() {
        let registry = ChannelSubscriptionRegistry::default();
        let owner = ChannelOwner::Trades(InstrumentId::from("ETH-20260627-3500-C.DERIVE"));
        let first_generation = registry.state.lock().activate(owner, None).unwrap();

        registry.clear();
        let second_generation = registry.state.lock().activate(owner, None).unwrap();

        assert!(second_generation > first_generation);
    }

    #[rstest]
    fn test_subscription_registry_clear_preserves_inflight_transition() {
        let registry = ChannelSubscriptionRegistry::default();
        let first = registry.transition("ticker_slim.ETH-PERP.1000");

        registry.clear();
        let second = registry.transition("ticker_slim.ETH-PERP.1000");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[rstest]
    fn test_subscription_transition_registry_collects_inactive_channels() {
        let registry = ChannelSubscriptionRegistry::default();
        let live = registry.transition("ticker_slim.ETH-PERP.1000");

        for index in 0..TRANSITION_GC_THRESHOLD + 16 {
            drop(registry.transition(&format!("ticker_slim.ETH-OPTION-{index}.1000")));
        }
        let same_live = registry.transition("ticker_slim.ETH-PERP.1000");

        assert!(Arc::ptr_eq(&live, &same_live));
        assert!(registry.transitions.len() < TRANSITION_GC_THRESHOLD);
    }

    #[rstest]
    fn test_ambiguous_cleanup_retains_survivor_for_reconnect() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let lifecycle = client.subscription_lifecycle();
        let instrument_id = InstrumentId::from("ETH-20260627-3600-C.DERIVE");
        let owner = ChannelOwner::Trades(instrument_id);
        lifecycle.activate(owner, Some("trades.option.ETH"));
        let error = DeriveWsError::Timeout {
            method: "unsubscribe".to_string(),
        };
        let ws = client.ws_handle();

        let retained = retain_channel_for_reconnect(&ws, "trades.option.ETH", true, Some(&error));

        assert!(retained);
        assert!(lifecycle.is_active(owner));
        assert!(client.active_trade_subs.contains(&instrument_id));
        assert!(ws.has_subscription("trades.option.ETH"));
    }

    #[rstest]
    fn test_explicit_cleanup_rejection_preserves_surviving_channel_owner() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let lifecycle = client.subscription_lifecycle();
        let instrument_id = InstrumentId::from("ETH-20260627-3600-C.DERIVE");
        let owner = ChannelOwner::Trades(instrument_id);
        lifecycle.activate(owner, Some("trades.option.ETH"));
        let error = DeriveWsError::JsonRpc {
            code: -32603,
            message: "not subscribed".to_string(),
            data: None,
        };

        let ws = client.ws_handle();
        ws.remember_subscription("trades.option.ETH");

        let retained = retain_channel_for_reconnect(&ws, "trades.option.ETH", true, Some(&error));

        assert!(!retained);
        assert!(lifecycle.is_active(owner));
        assert!(client.active_trade_subs.contains(&instrument_id));
        assert!(!ws.has_subscription("trades.option.ETH"));
    }

    #[rstest]
    fn test_ambiguous_cleanup_without_survivor_is_not_replayed() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let ws = client.ws_handle();
        ws.remember_subscription("trades.option.ETH");
        let error = DeriveWsError::Timeout {
            method: "unsubscribe".to_string(),
        };

        let retained = retain_channel_for_reconnect(&ws, "trades.option.ETH", false, Some(&error));

        assert!(!retained);
        assert!(!ws.has_subscription("trades.option.ETH"));
    }

    #[rstest]
    fn test_unsubscribe_quotes_prunes_cached_quote() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let mut client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        let owner = ChannelOwner::Ticker {
            instrument_id,
            feed: TickerFeed::Quote,
        };
        client
            .subscription_lifecycle()
            .activate(owner, Some("ticker_slim.ETH-PERP.1000"));
        client.quote_cache.lock().insert(
            instrument_id,
            QuoteTick::new(
                instrument_id,
                Price::from("3500.00"),
                Price::from("3501.00"),
                Quantity::from("1.000"),
                Quantity::from("2.000"),
                UnixNanos::from(1),
                UnixNanos::from(1),
            ),
        );
        let command = UnsubscribeQuotes::new(
            instrument_id,
            Some(*DERIVE_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        client.unsubscribe_quotes(&command).unwrap();

        assert!(!client.quote_cache.lock().contains(&instrument_id));
    }

    #[rstest]
    #[tokio::test]
    async fn test_unsubscribe_trades_uses_recorded_channel_without_cached_instrument() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);
        let mut client = DeriveDataClient::new(
            *DERIVE_CLIENT_ID,
            DeriveDataClientConfig {
                environment: DeriveEnvironment::Mainnet,
                ..Default::default()
            },
        )
        .unwrap();
        let instrument_id = InstrumentId::from("ETH-20260627-3500-C.DERIVE");
        let owner = ChannelOwner::Trades(instrument_id);
        let lifecycle = client.subscription_lifecycle();
        let generation = lifecycle.activate(owner, None).unwrap();
        assert!(lifecycle.attach_channel(owner, generation, "trades.option.ETH".to_string(),));
        let command = UnsubscribeTrades::new(
            instrument_id,
            Some(*DERIVE_CLIENT_ID),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );

        client.unsubscribe_trades(&command).unwrap();

        wait_until_async(
            || {
                let registry = Arc::clone(&client.channel_subscriptions);
                async move { registry.transitions.contains_key("trades.option.ETH") }
            },
            Duration::from_secs(1),
        )
        .await;

        assert!(!lifecycle.is_active(owner));
        assert!(!client.active_trade_subs.contains(&instrument_id));
    }

    fn perp_ticker_payload(instrument_id: InstrumentId) -> WsSubscriptionPayload {
        let channel = "ticker_slim.ETH-PERP.1000";
        let payload = subscription_payload(
            channel,
            &json!({
                "timestamp": 1_700_000_000_010_i64,
                "instrument_ticker": ticker_json(1_700_000_000_000)
            }),
        );
        assert_eq!(payload.channel, channel);
        let _ = instrument_id;
        payload
    }

    fn install_ticker(ctx: &WsMessageContext, instrument_id: InstrumentId, channel: &str) {
        ctx.active_ticker_channels
            .insert(instrument_id, channel.to_string());
    }

    #[rstest]
    fn test_ticker_emits_mark_price_when_mark_subscribed() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, "ticker_slim.ETH-PERP.1000");
        ctx.active_mark_subs.insert(instrument_id);

        DeriveDataClient::handle_ws_message(
            DeriveWsMessage::Subscription(perp_ticker_payload(instrument_id)),
            &ctx,
        );

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::MarkPrice(mark)) => {
                assert_eq!(mark.instrument_id, instrument_id);
                assert_eq!(mark.value, Price::from("3500.50"));
            }
            other => panic!("expected MarkPriceUpdate, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_emits_index_price_when_index_subscribed() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, "ticker_slim.ETH-PERP.1000");
        ctx.active_index_subs.insert(instrument_id);

        DeriveDataClient::handle_ws_message(
            DeriveWsMessage::Subscription(perp_ticker_payload(instrument_id)),
            &ctx,
        );

        match rx.try_recv().unwrap() {
            DataEvent::Data(Data::IndexPrice(index)) => {
                assert_eq!(index.instrument_id, instrument_id);
                assert_eq!(index.value, Price::from("3500.00"));
            }
            other => panic!("expected IndexPriceUpdate, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_emits_funding_rate_for_perp_when_subscribed() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, "ticker_slim.ETH-PERP.1000");
        ctx.active_funding_subs.insert(instrument_id);

        DeriveDataClient::handle_ws_message(
            DeriveWsMessage::Subscription(perp_ticker_payload(instrument_id)),
            &ctx,
        );

        match rx.try_recv().unwrap() {
            DataEvent::FundingRate(update) => {
                assert_eq!(update.instrument_id, instrument_id);
                assert_eq!(update.rate, "0.0002".parse().unwrap());
            }
            other => panic!("expected FundingRateUpdate, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_skips_funding_rate_when_not_perp() {
        let instrument = option_instrument();
        let instrument_id = instrument.id();
        let channel = format!("ticker_slim.{}.1000", instrument_id.symbol.as_str());
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, &channel);
        ctx.active_funding_subs.insert(instrument_id);

        let mut option_data = option_ticker_json(1_700_000_000_000);
        option_data["instrument_name"] = json!(instrument_id.symbol.as_str());
        let payload = subscription_payload(
            &channel,
            &json!({
                "timestamp": 1_700_000_000_010_i64,
                "instrument_ticker": option_data
            }),
        );

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_emits_option_greeks_when_subscribed() {
        let instrument = option_instrument();
        let instrument_id = instrument.id();
        let channel = format!("ticker_slim.{}.1000", instrument_id.symbol.as_str());
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, &channel);
        ctx.active_greeks_subs.insert(instrument_id);

        let mut option_data = option_ticker_json(1_700_000_000_000);
        option_data["instrument_name"] = json!(instrument_id.symbol.as_str());
        let payload = subscription_payload(
            &channel,
            &json!({
                "timestamp": 1_700_000_000_010_i64,
                "instrument_ticker": option_data
            }),
        );

        DeriveDataClient::handle_ws_message(DeriveWsMessage::Subscription(payload), &ctx);

        match rx.try_recv().unwrap() {
            DataEvent::OptionGreeks(greeks) => {
                assert_eq!(greeks.instrument_id, instrument_id);
                assert!((greeks.greeks.delta - 0.55).abs() < 1e-9);
                assert!((greeks.greeks.gamma - 0.0008).abs() < 1e-9);
                assert!((greeks.greeks.vega - 4.5).abs() < 1e-9);
                assert!((greeks.greeks.theta + 2.1).abs() < 1e-9);
                assert!((greeks.greeks.rho - 1.2).abs() < 1e-9);
                assert_eq!(greeks.mark_iv, Some(0.60));
                assert_eq!(greeks.bid_iv, Some(0.58));
                assert_eq!(greeks.ask_iv, Some(0.62));
                assert_eq!(greeks.underlying_price, Some(3505.0));
                assert_eq!(greeks.open_interest, Some(1000.0));
            }
            other => panic!("expected OptionGreeks, was {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[rstest]
    fn test_ticker_emits_all_subscribed_feeds_in_one_frame() {
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        let (ctx, mut rx) = make_ctx(Some(instrument));
        install_ticker(&ctx, instrument_id, "ticker_slim.ETH-PERP.1000");
        ctx.active_quote_subs.insert(instrument_id);
        ctx.active_mark_subs.insert(instrument_id);
        ctx.active_index_subs.insert(instrument_id);
        ctx.active_funding_subs.insert(instrument_id);

        DeriveDataClient::handle_ws_message(
            DeriveWsMessage::Subscription(perp_ticker_payload(instrument_id)),
            &ctx,
        );

        let mut quote = None;
        let mut mark = None;
        let mut index = None;
        let mut funding = None;

        while let Ok(event) = rx.try_recv() {
            match event {
                DataEvent::Data(Data::Quote(q)) => {
                    assert!(quote.replace(q).is_none(), "duplicate Quote emission");
                }
                DataEvent::Data(Data::MarkPrice(m)) => {
                    assert!(
                        mark.replace(m).is_none(),
                        "duplicate MarkPriceUpdate emission"
                    );
                }
                DataEvent::Data(Data::IndexPrice(i)) => {
                    assert!(
                        index.replace(i).is_none(),
                        "duplicate IndexPriceUpdate emission"
                    );
                }
                DataEvent::FundingRate(f) => {
                    assert!(
                        funding.replace(f).is_none(),
                        "duplicate FundingRate emission"
                    );
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }

        let quote = quote.expect("Quote event missing");
        let mark = mark.expect("MarkPriceUpdate missing");
        let index = index.expect("IndexPriceUpdate missing");
        let funding = funding.expect("FundingRateUpdate missing");

        assert_eq!(quote.instrument_id, instrument_id);
        assert_eq!(quote.bid_price, Price::from("3500.00"));
        assert_eq!(quote.ask_price, Price::from("3501.00"));
        assert_eq!(mark.instrument_id, instrument_id);
        assert_eq!(mark.value, Price::from("3500.50"));
        assert_eq!(index.instrument_id, instrument_id);
        assert_eq!(index.value, Price::from("3500.00"));
        assert_eq!(funding.instrument_id, instrument_id);
        assert_eq!(funding.rate, "0.0002".parse().unwrap());
    }

    #[rstest]
    fn test_reset_clears_all_subscription_state() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);

        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Mainnet,
            ..Default::default()
        };
        let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config).unwrap();
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        cache_instrument(&client.instruments, &instrument);

        client
            .active_book_delta_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        client
            .active_book_depth_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        client
            .active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.1000".to_string());
        client.active_quote_subs.insert(instrument_id);
        client.active_trade_subs.insert(instrument_id);
        client
            .channel_subscriptions
            .state
            .lock()
            .activate(ChannelOwner::Trades(instrument_id), Some("trades.perp.ETH"));
        client.channel_subscriptions.transition("trades.perp.ETH");
        client.active_mark_subs.insert(instrument_id);
        client.active_index_subs.insert(instrument_id);
        client.active_funding_subs.insert(instrument_id);
        client.active_greeks_subs.insert(instrument_id);

        client.reset().unwrap();

        assert!(!client.instruments.contains_key(&instrument_id));
        assert!(
            !client
                .active_book_delta_channels
                .contains_key(&instrument_id)
        );
        assert!(
            !client
                .active_book_depth_channels
                .contains_key(&instrument_id)
        );
        assert!(!client.active_ticker_channels.contains_key(&instrument_id));
        assert!(!client.active_quote_subs.contains(&instrument_id));
        assert!(!client.active_trade_subs.contains(&instrument_id));
        assert!(client.channel_subscriptions.state.lock().owners.is_empty());

        // Sync reset cannot await canceled tasks, so their channel locks stay
        // stable until the next async teardown joins them.
        assert_eq!(client.channel_subscriptions.transitions.len(), 1);
        assert!(!client.active_mark_subs.contains(&instrument_id));
        assert!(!client.active_index_subs.contains(&instrument_id));
        assert!(!client.active_funding_subs.contains(&instrument_id));
        assert!(!client.active_greeks_subs.contains(&instrument_id));
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_disconnect_clears_subscription_state() {
        // Regression: aborting in-flight subscribe tasks during disconnect()
        // skips their on-error rollback, so any `active_*` entries staged
        // before spawn would leak across reconnect and silently suppress
        // the next subscribe. disconnect() must clear the same local
        // subscription maps that reset() does.
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);

        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Mainnet,
            ..Default::default()
        };
        let mut client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config).unwrap();
        let instrument = perp_instrument();
        let instrument_id = instrument.id();
        cache_instrument(&client.instruments, &instrument);

        client
            .active_book_delta_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        client
            .active_book_depth_channels
            .insert(instrument_id, "orderbook.ETH-PERP.1.10".to_string());
        client
            .active_ticker_channels
            .insert(instrument_id, "ticker_slim.ETH-PERP.1000".to_string());
        client.active_quote_subs.insert(instrument_id);
        client.active_trade_subs.insert(instrument_id);
        client
            .channel_subscriptions
            .state
            .lock()
            .activate(ChannelOwner::Trades(instrument_id), Some("trades.perp.ETH"));
        client.channel_subscriptions.transition("trades.perp.ETH");
        client.active_mark_subs.insert(instrument_id);
        client.active_index_subs.insert(instrument_id);
        client.active_funding_subs.insert(instrument_id);
        client.active_greeks_subs.insert(instrument_id);
        client.is_connected.store(true, Ordering::Relaxed);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (drop_tx, mut drop_rx) = tokio::sync::oneshot::channel::<()>();
        client
            .pending_tasks
            .spawn(async move {
                let _drop_tx = drop_tx;
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            })
            .unwrap();
        started_rx.await.unwrap();

        client.disconnect().await.unwrap();

        // Instrument cache must survive disconnect (reset() is the path
        // that wipes it); only the subscription maps are cleared.
        assert!(client.instruments.contains_key(&instrument_id));
        assert!(
            !client
                .active_book_delta_channels
                .contains_key(&instrument_id)
        );
        assert!(
            !client
                .active_book_depth_channels
                .contains_key(&instrument_id)
        );
        assert!(!client.active_ticker_channels.contains_key(&instrument_id));
        assert!(!client.active_quote_subs.contains(&instrument_id));
        assert!(!client.active_trade_subs.contains(&instrument_id));
        assert!(client.channel_subscriptions.state.lock().owners.is_empty());
        assert!(client.channel_subscriptions.transitions.is_empty());
        assert!(!client.active_mark_subs.contains(&instrument_id));
        assert!(!client.active_index_subs.contains(&instrument_id));
        assert!(!client.active_funding_subs.contains(&instrument_id));
        assert!(!client.active_greeks_subs.contains(&instrument_id));
        assert!(!client.is_connected());
        assert_eq!(
            drop_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed),
        );
    }

    #[tokio::test]
    async fn test_task_group_unregisters_finished_tasks_before_drain() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        replace_data_event_sender(tx);

        let config = DeriveDataClientConfig {
            environment: DeriveEnvironment::Mainnet,
            ..Default::default()
        };
        let client = DeriveDataClient::new(*DERIVE_CLIENT_ID, config).unwrap();

        for _ in 0..100 {
            client.spawn_task("test_noop", async { Ok(()) });
        }

        wait_until_async(
            || async { client.pending_tasks.all_finished() },
            Duration::from_secs(2),
        )
        .await;

        assert!(client.pending_tasks.is_empty());
        client.pending_tasks.begin_shutdown();
        client
            .pending_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .unwrap();
        assert!(client.pending_tasks.is_empty());
    }
}
