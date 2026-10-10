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

//! Live execution client implementation for the Derive adapter.
//!
//! Mirrors the Hyperliquid adapter's structural pattern: an
//! [`ExecutionClientCore`] holds identity and connection state, an
//! [`ExecutionEventEmitter`] publishes order/account events back to the live
//! engine, and the venue clients ([`DeriveHttpClient`], [`DeriveWebSocketClient`])
//! handle the wire. All state-changing requests are EIP-712 typed-data signed
//! against the per-action module contracts on the Derive Chain; the
//! `private/order` body in particular is built by [`order_to_derive_payload`].

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    cache::ORDER_NOT_FOUND,
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    messages::{
        ExecutionReport,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
        },
    },
};
use nautilus_core::{
    AtomicMap, DurationNanos, Params, UUID4, UnixNanos,
    string::secret::SecretString,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_live::{
    ExecutionClientCore, ExecutionEventEmitter, SocketControl,
    execution::reports::retain_order_status_reports,
    task::{TaskGroup, TaskGroupGuard, TaskSpawner},
};
use nautilus_model::{
    accounts::AccountAny,
    data::QuoteTick,
    enums::{OmsType, OrderSide, OrderStatus, OrderType, PositionSide},
    events::{
        OrderAccepted, OrderCanceled, OrderEventAny, OrderExpired, OrderFilled, OrderRejected,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, Symbol, Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Price, Quantity},
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{
    common::{
        consts::{
            DERIVE_ACCOUNT_REGISTRATION_TIMEOUT_SECS, DERIVE_VENUE, MIN_SIGNATURE_TTL,
            RECONNECT_BACKOFF_FACTOR, RECONNECT_BASE_BACKOFF, RECONNECT_MAX_BACKOFF,
            TRIGGER_ORDER_SIGNATURE_TTL,
        },
        credential::DeriveCredential,
        enums::{DeriveInstrumentType, DeriveOrderSide},
        parse::{
            derive_order_type_to_nautilus_for_order, derive_rejection_due_post_only,
            format_instrument_id, format_venue_symbol,
        },
        retry::{http_retry_config, is_write_outcome_ambiguous_ws},
    },
    config::DeriveExecutionClientConfig,
    http::{
        DeriveCredentials, DeriveHttpClient,
        models::{DeriveInstrument, DeriveOrder, DeriveReplaceOutcome, DeriveTrade},
        parse::{
            parse_derive_order_to_report_with_precision,
            parse_derive_position_to_report_with_precision, parse_derive_subaccount_to_balances,
            parse_derive_trade_to_fill_report_with_precision,
        },
        query::{
            DeriveCancelByInstrumentParams, DeriveCancelByLabelParams, DeriveCancelParams,
            DeriveCancelTriggerOrderParams, DeriveGetOpenOrdersParams, DeriveGetOrderHistoryParams,
            DeriveGetOrderParams, DeriveGetPositionsParams, DeriveGetSubaccountParams,
            DeriveGetTradeHistoryParams, DeriveGetTriggerOrdersParams,
            order_replace_to_derive_payload, order_to_derive_payload,
            trigger_order_to_derive_payload, validate_order_support,
            validate_trigger_order_support,
        },
    },
    signing::{
        context::{SigningContext, resolve_signing_context},
        nonce::{NonceError, NonceManager},
    },
    websocket::client::DeriveWebSocketShutdownHandle,
    websocket::{
        DeriveOrdersSubscriptionData, DeriveTradesSubscriptionData, DeriveWebSocketClient,
        DeriveWsChannel, DeriveWsCredentials, DeriveWsError, DeriveWsExecutionHandle,
        DeriveWsMessage, OrderIdentity, WsDispatchState, parse::parse_ticker_quote_from_rest,
    },
};

const DERIVE_PRIVATE_PAGE_SIZE: u32 = 500;

/// Refusal reason when fallback private state is missing, stale, incomplete,
/// not yet applied, or holds an unresolved write.
const REST_GATE_PRIVATE_STATE: &str =
    "Derive REST fallback is waiting for fresh, fully reconciled private state";

/// Refusal reason when no sufficiently fresh quote backs a risk-increasing
/// write.
const REST_GATE_QUOTE: &str =
    "Derive REST fallback has no sufficiently fresh quote for this instrument";

/// Refusal reason when the targeted order's own write outcome is unknown.
const REST_GATE_UNRESOLVED_ORDER: &str =
    "Derive REST fallback cannot act on this order while its write outcome is unresolved";

/// Refusal reason when any write's venue outcome is still unknown. Applies on
/// either transport: a possibly-live order blocks risk-increasing commands
/// whether it was sent over REST or the WebSocket.
const REST_GATE_UNRESOLVED_WRITE: &str = "Derive execution has a write with an unknown venue outcome; risk-increasing orders are halted until reconciliation resolves it";

/// Whether every row a reconciliation needed was converted.
///
/// The reused report APIs are deliberately best-effort: they log and omit a row
/// they cannot convert, and keep that behaviour for every other caller. A REST
/// admission decision may not read an omission as a complete picture, so the
/// counts travel alongside the reports.
#[derive(Debug, Clone, Copy, Default)]
struct ReportCompleteness {
    skipped_orders: usize,
    skipped_fills: usize,
    skipped_positions: usize,
}

impl ReportCompleteness {
    const fn is_complete(self) -> bool {
        self.skipped_orders == 0 && self.skipped_fills == 0 && self.skipped_positions == 0
    }
}

/// Live execution client for Derive.
///
/// Owns the HTTP and WebSocket clients used to talk to the venue plus an
/// [`ExecutionEventEmitter`] that publishes order/account events back to the
/// live engine. Order operations are signed against the per-environment
/// EIP-712 signing context resolved at construction.
#[derive(Debug)]
pub struct DeriveExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: DeriveExecutionClientConfig,
    credential: DeriveCredential,
    emitter: ExecutionEventEmitter,
    http_client: DeriveHttpClient,
    ws_client: Arc<tokio::sync::Mutex<DeriveWebSocketClient>>,
    ws_shutdown: DeriveWebSocketShutdownHandle,
    ws_exec: DeriveWsExecutionHandle,
    instruments: Arc<AtomicMap<InstrumentId, DeriveInstrument>>,
    nonce_manager: Arc<NonceManager>,
    signing: SigningContext,
    is_connected: Arc<AtomicBool>,
    rest_execution_state: Arc<RestExecutionState>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    pending_tasks: TaskGroup,
    shutdown_errors: Vec<String>,
    dispatch_state: Arc<WsDispatchState>,
}

impl DeriveExecutionClient {
    /// Creates a new [`DeriveExecutionClient`].
    ///
    /// Resolves wallet/session-key/subaccount from the supplied config, falling
    /// back to the documented environment variables when fields are unset, and
    /// parses the EIP-712 signing constants (domain separator, action typehash,
    /// trade-module address) from config overrides or the shipped per-environment
    /// defaults.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    /// - `max_fee_per_contract` is missing or not greater than zero.
    /// - Required credentials are not provided via config or environment.
    /// - Signing constants are still placeholders or cannot be parsed as hex.
    /// - The HTTP or WebSocket client cannot be constructed.
    pub fn new(
        core: ExecutionClientCore,
        config: DeriveExecutionClientConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;

        let credential = DeriveCredential::resolve(
            config.wallet_address.clone(),
            config.session_key.clone().map(SecretString::into_inner),
            config.subaccount_id,
            config.environment,
        )?;

        let http_credentials = DeriveCredentials::new(
            credential.wallet_address().to_string(),
            credential.session_key(),
        )
        .context("failed to build Derive HTTP credentials")?;
        let retry_config = http_retry_config(
            config.max_retries,
            config.retry_delay_initial_ms,
            config.retry_delay_max_ms,
        );
        let proxy_url = config
            .proxy_url
            .as_ref()
            .map(|value| value.expose_secret().to_owned());
        let http_client = DeriveHttpClient::with_credentials(
            config.rest_url(),
            http_credentials,
            Some(config.http_timeout_secs),
            proxy_url.clone(),
            Some(retry_config),
        )
        .context("failed to create Derive HTTP client")?;

        let ws_credentials = DeriveWsCredentials::new(
            credential.wallet_address().to_string(),
            credential.session_key(),
        )
        .context("failed to build Derive WebSocket credentials")?;
        let mut ws_client = DeriveWebSocketClient::with_credentials(
            Some(config.ws_url()),
            config.environment,
            config.transport_backend,
            proxy_url,
            ws_credentials,
            config.max_matching_requests_per_second,
            config.max_per_instrument_matching_requests_per_second,
        )
        .with_socket_control(SocketControl::new(
            core.client_id,
            Some(*DERIVE_VENUE),
            "derive-user-streams",
        ));

        if let Some(secs) = config.ws_timeout_secs {
            ws_client.set_request_timeout(Duration::from_secs(secs));
        }
        // The handle shares the client's command channel, which survives the
        // reconnect swap, so it stays valid for the client's lifetime.
        let ws_exec = ws_client.execution_handle();
        let ws_shutdown = ws_client.shutdown_handle();
        let ws_client = Arc::new(tokio::sync::Mutex::new(ws_client));

        let signing = resolve_signing_context(&credential, &config)?;

        let clock = get_atomic_clock_realtime();
        let emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            core.account_type,
            core.base_currency,
        );

        let session_tasks = TaskGroup::new();
        let pending_tasks = TaskGroup::new();

        Ok(Self {
            core,
            clock,
            config,
            credential,
            emitter,
            http_client,
            ws_client,
            ws_shutdown,
            ws_exec,
            instruments: Arc::new(AtomicMap::new()),
            nonce_manager: Arc::new(NonceManager::new()),
            signing,
            is_connected: Arc::new(AtomicBool::new(false)),
            rest_execution_state: Arc::new(RestExecutionState::default()),
            cancellation_token: CancellationToken::new(),
            session_tasks,
            pending_tasks,
            shutdown_errors: Vec::new(),
            dispatch_state: Arc::new(WsDispatchState::new()),
        })
    }

    /// Returns the resolved subaccount id.
    #[must_use]
    pub const fn subaccount_id(&self) -> u64 {
        self.credential.subaccount_id()
    }

    /// Returns a reference to the resolved configuration.
    #[must_use]
    pub fn config(&self) -> &DeriveExecutionClientConfig {
        &self.config
    }

    /// Returns a reference to the underlying HTTP client.
    #[must_use]
    pub fn http_client(&self) -> &DeriveHttpClient {
        &self.http_client
    }

    /// Caches a Derive instrument by instrument ID so order submission can
    /// resolve `base_asset_address` and `base_asset_sub_id` without
    /// re-querying the venue.
    pub fn cache_instrument(&self, instrument: DeriveInstrument) {
        let instrument_id = format_instrument_id(instrument.instrument_name);
        if let (Ok(price_increment), Ok(size_increment)) = (
            Price::from_decimal(instrument.tick_size),
            Quantity::from_decimal(instrument.amount_step),
        ) {
            self.dispatch_state.register_instrument_precision(
                instrument_id,
                price_increment.precision,
                size_increment.precision,
            );
        }
        self.instruments.insert(instrument_id, instrument);
    }

    /// Spawns a fire-and-forget task tracked in `pending_tasks` for teardown.
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

    async fn await_pending_tasks(&self) -> anyhow::Result<()> {
        self.pending_tasks.begin_shutdown();
        self.pending_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to terminate Derive execution tasks: {e}"))?;
        Ok(())
    }

    async fn await_session_tasks(&self) -> anyhow::Result<()> {
        self.session_tasks.begin_shutdown();
        self.session_tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(2))
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to terminate Derive execution session tasks: {e}")
            })?;
        Ok(())
    }

    async fn ensure_instruments_initialized(&self) -> anyhow::Result<()> {
        if self.core.instruments_initialized() {
            return Ok(());
        }
        // Lazy bootstrap: exec-side fetches per-instrument on first reference.
        // Marking the flag prevents duplicate work across reconnect cycles.
        self.core.set_instruments_initialized();
        Ok(())
    }

    fn reconciliation_context(&self) -> DeriveReconciliationContext {
        DeriveReconciliationContext {
            http_client: self.http_client.clone(),
            emitter: self.emitter.clone(),
            client_id: self.core.client_id,
            account_id: self.core.account_id,
            subaccount_id: self.credential.subaccount_id(),
            clock: self.clock,
            dispatch_state: Arc::clone(&self.dispatch_state),
            rest_execution_state: Arc::clone(&self.rest_execution_state),
            is_connected: Arc::clone(&self.is_connected),
            max_account_state_age_secs: self.config.max_account_state_age_secs,
        }
    }

    fn start_rest_reconciliation_loop(&self) -> anyhow::Result<()> {
        if !self.config.rest_fallback_enabled {
            return Ok(());
        }
        let interval_secs = self
            .config
            .private_state_poll_interval_secs
            .expect("validated REST fallback polling interval");
        let interval = Duration::from_secs(interval_secs);
        let context = self.reconciliation_context();
        let ws_exec = self.ws_exec.clone();
        let cancellation = self.cancellation_token.clone();
        let rest_execution_state = Arc::clone(&self.rest_execution_state);

        self.session_tasks.spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    _ = ticker.tick() => {
                        if !ws_exec.is_authenticated() {
                            set_rest_fallback_active(
                                &rest_execution_state,
                                true,
                                "websocket_unauthenticated",
                                context.clock.get_time_ns().as_u64(),
                            );
                            rest_execution_state.websocket_ready.store(false, Ordering::Release);
                        }
                        // Reconcile while routing through REST, and also
                        // whenever any write's outcome is unknown: that is the
                        // only way positive evidence about it can arrive, and a
                        // JSON-RPC ambiguity does not require the socket to
                        // drop, so it can happen with routing still on the
                        // WebSocket.
                        let needs_reconciliation = rest_execution_state.active.load(Ordering::Acquire)
                            || has_unresolved_writes(&rest_execution_state);
                        if needs_reconciliation
                            && let Err(e) = context.reconcile_private_state().await
                        {
                            rest_execution_state
                                .reconciliation_failed
                                .store(true, Ordering::Release);
                            log::warn!("Derive REST-mode private reconciliation failed: {e:?}");
                        } else if rest_execution_state.active.load(Ordering::Acquire)
                            && rest_execution_state.websocket_ready.load(Ordering::Acquire)
                            && context.rest_mode_may_exit()
                        {
                            // Same predicate as `recover_after_reconnect`: an
                            // incomplete, stale or unapplied generation may not
                            // hand routing back to the WebSocket, where these
                            // gates would otherwise be skipped.
                            set_rest_fallback_active(
                                &rest_execution_state,
                                false,
                                "reconciliation_complete_fresh_and_applied",
                                context.clock.get_time_ns().as_u64(),
                            );
                        }
                    }
                }
            }
        })?;
        Ok(())
    }

    fn should_use_rest_fallback(&self) -> bool {
        self.config.rest_fallback_enabled
            && (self.rest_execution_state.active.load(Ordering::Acquire)
                || !self.ws_exec.is_authenticated())
    }

    /// Synchronous pre-check for a risk-increasing write, so an order that
    /// cannot be admitted is denied before `OrderSubmitted` is emitted.
    /// [`reserve_write`] re-applies the same predicate atomically with the
    /// write itself.
    ///
    /// Returns the refusal reason, or `None` when the write may proceed. The
    /// unresolved-write term applies on either transport, because a
    /// possibly-live order is just as dangerous when the socket that sent it is
    /// still healthy.
    fn risk_increasing_write_blocked(
        &self,
        quote_ts_event: Option<UnixNanos>,
    ) -> Option<&'static str> {
        let now_ns = self.clock.get_time_ns().as_u64();
        // Always evaluated, and before any early return, because it is what
        // latches the applied generation while the cache is reachable — both
        // for the reservation that re-checks inside the spawned task and for
        // the routing decision that hands order entry back to the WebSocket.
        let applied = self.refresh_applied_generation();

        if has_unresolved_writes(&self.rest_execution_state) {
            return Some(REST_GATE_UNRESOLVED_WRITE);
        }
        if !applied
            || !private_state_is_fresh(
                &self.rest_execution_state,
                now_ns,
                self.config.max_account_state_age_secs,
            )
        {
            return Some(REST_GATE_PRIVATE_STATE);
        }
        if !quote_is_fresh(quote_ts_event, now_ns, self.config.max_quote_age_secs) {
            return Some(REST_GATE_QUOTE);
        }
        None
    }

    /// Refreshes the applied-generation latch and re-evaluates order routing.
    ///
    /// Only the command thread can reach the cache, so every command refreshes
    /// the latch even when its own admission does not depend on it; otherwise a
    /// quiet period with no risk-increasing command would leave the generation
    /// unlatched and strand routing in REST mode after the WebSocket had
    /// recovered. Having just learned that the engine applied a complete, fresh
    /// generation, this is also the earliest point at which routing may return
    /// to the WebSocket, so the decision is taken here rather than waiting for
    /// the next poll tick. An unresolved write deliberately does not keep
    /// routing in REST mode — it halts risk-increasing commands on either
    /// transport instead.
    fn note_reconciliation_application(&self) {
        if !self.config.rest_fallback_enabled {
            return;
        }
        let applied = self.refresh_applied_generation();
        let state = &self.rest_execution_state;
        let now_ns = self.clock.get_time_ns().as_u64();
        if applied
            && state.active.load(Ordering::Acquire)
            && state.websocket_ready.load(Ordering::Acquire)
            && self.ws_exec.is_authenticated()
            && private_state_is_fresh(state, now_ns, self.config.max_account_state_age_secs)
        {
            set_rest_fallback_active(
                state,
                false,
                "reconciliation_complete_fresh_and_applied",
                now_ns,
            );
        }
    }

    /// The configured staleness bounds for an admission decision.
    fn freshness_bounds(&self) -> FreshnessBounds {
        FreshnessBounds {
            private_state_secs: self.config.max_account_state_age_secs,
            quote_secs: self.config.max_quote_age_secs,
        }
    }

    /// Whether an unresolved write currently halts risk-increasing commands,
    /// regardless of which transport they would be routed over.
    fn unresolved_write_halt(&self) -> Option<&'static str> {
        (self.config.rest_fallback_enabled && has_unresolved_writes(&self.rest_execution_state))
            .then_some(REST_GATE_UNRESOLVED_WRITE)
    }

    /// Latches and returns whether the engine has applied the last complete
    /// reconciliation's reports, by checking that the cache now agrees with
    /// what that generation reported.
    ///
    /// Agreement is checked in both directions and on the fields an entry
    /// decision depends on. For every reported order the engine tracks, the
    /// cached order must carry the reported venue order id and agree on
    /// closed-ness, quantity and cumulative filled quantity, so a queued
    /// replacement or an unapplied partial fill is never mistaken for applied
    /// state. For every reported open position the cache must hold the same
    /// side and quantity, and the cache must not hold an open order or position
    /// that this generation's snapshot should have contained but did not — so
    /// an empty report set proves application only when the cache is also free
    /// of contrary state, never vacuously. Local state whose last event is
    /// newer than the snapshot is exempt, because a snapshot cannot contain
    /// what did not yet exist when it was taken.
    ///
    /// The cache is not `Send`, so this runs on the command thread and latches
    /// per generation for the reservation that follows inside the spawned task.
    /// A generation that is never applied goes stale on its own, so this can
    /// delay the gate opening but never hold it open.
    fn refresh_applied_generation(&self) -> bool {
        let reported = self.rest_execution_state.reported_state.lock();
        if reported.generation == 0 {
            // No complete reconciliation has been recorded, so there is nothing
            // that could have been applied.
            return false;
        }
        if self
            .rest_execution_state
            .applied_generation
            .load(Ordering::Acquire)
            == reported.generation
        {
            return true;
        }

        let cache = self.core.cache();
        let mut disagreement: Option<String> = None;

        for (client_order_id, expected) in &reported.orders {
            let Some(order) = cache.order(client_order_id) else {
                // Not tracked locally (an order placed outside this client), so
                // there is no local state for it to disagree with.
                continue;
            };
            if order.venue_order_id() != Some(expected.venue_order_id) {
                disagreement = Some(format!(
                    "{client_order_id} venue_order_id cached={:?} reported={}",
                    order.venue_order_id(),
                    expected.venue_order_id,
                ));
                break;
            }
            if order.is_closed() != expected.closed {
                disagreement = Some(format!(
                    "{client_order_id} closed cached={} reported={}",
                    order.is_closed(),
                    expected.closed,
                ));
                break;
            }
            if order.quantity().as_decimal() != expected.quantity {
                disagreement = Some(format!(
                    "{client_order_id} quantity cached={} reported={}",
                    order.quantity(),
                    expected.quantity,
                ));
                break;
            }
            if order.filled_qty().as_decimal() != expected.filled_qty {
                disagreement = Some(format!(
                    "{client_order_id} filled_qty cached={} reported={}",
                    order.filled_qty(),
                    expected.filled_qty,
                ));
                break;
            }
        }

        if disagreement.is_none() {
            for position in cache.positions_open_refs(
                Some(&self.core.venue),
                None,
                None,
                Some(&self.core.account_id),
                None,
            ) {
                match reported.positions.get(&position.instrument_id) {
                    Some((side, quantity))
                        if *side == position.side
                            && *quantity == position.quantity.as_decimal() => {}
                    Some((side, quantity)) => {
                        disagreement = Some(format!(
                            "{} position cached={:?} {} reported={side:?} {quantity}",
                            position.instrument_id, position.side, position.quantity,
                        ));
                        break;
                    }
                    None if position.ts_last <= reported.observed_at => {
                        disagreement = Some(format!(
                            "{} position cached={:?} {} but reported flat",
                            position.instrument_id, position.side, position.quantity,
                        ));
                        break;
                    }
                    None => {}
                }
            }
        }

        if disagreement.is_none() {
            for order in cache.orders_open_refs(
                Some(&self.core.venue),
                None,
                None,
                Some(&self.core.account_id),
                None,
            ) {
                let client_order_id = order.client_order_id();
                if cache.client_id(&client_order_id) != Some(&self.core.client_id) {
                    continue;
                }
                if order.ts_last() <= reported.observed_at
                    && !reported.orders.contains_key(&client_order_id)
                {
                    disagreement = Some(format!(
                        "{client_order_id} is open in the cache but absent from the reconciliation"
                    ));
                    break;
                }
            }
        }

        drop(cache);
        let generation = reported.generation;
        drop(reported);

        if let Some(disagreement) = disagreement {
            log::debug!(
                "Derive reconciliation generation {generation} is not applied yet: {disagreement}"
            );
            return false;
        }

        self.rest_execution_state
            .applied_generation
            .store(generation, Ordering::Release);
        true
    }

    /// Event time of the newest cached quote for `instrument_id`, captured on
    /// the command thread because the cache is not `Send`.
    fn cached_quote_ts_event(&self, instrument_id: &InstrumentId) -> Option<UnixNanos> {
        self.core.cache().quote(instrument_id).map(|q| q.ts_event)
    }

    async fn refresh_account_state(&self) -> anyhow::Result<()> {
        self.reconciliation_context()
            .refresh_account_state()
            .await?;
        Ok(())
    }

    /// Blocks until the account appears in the cache, or `timeout_secs` elapses.
    ///
    /// The execution engine populates the cache from the [`refresh_account_state`]
    /// event asynchronously; strategies that begin issuing orders before the
    /// account is registered race the portfolio. Connecting blocks here so the
    /// runner can rely on `core.cache().account(account_id)` immediately after
    /// `connect()` returns.
    async fn await_account_registered(&self, timeout_secs: f64) -> anyhow::Result<()> {
        let account_id = self.core.account_id;

        if self.core.cache().account(&account_id).is_some() {
            log::info!("Account {account_id} registered");
            return Ok(());
        }

        let start = Instant::now();
        let timeout = Duration::from_secs_f64(timeout_secs);
        let interval = Duration::from_millis(10);

        loop {
            tokio::time::sleep(interval).await;

            if self.core.cache().account(&account_id).is_some() {
                log::info!("Account {account_id} registered");
                return Ok(());
            }

            if start.elapsed() >= timeout {
                anyhow::bail!(
                    "Timeout waiting for account {account_id} to be registered after {timeout_secs}s"
                );
            }
        }
    }

    /// Reverses the partial state `connect()` set up before the failing step:
    /// cancels the shared cancellation token, aborts the WS dispatch task,
    /// and closes the WS client. Used when initial account state cannot be
    /// loaded so that the next `connect()` call starts from a clean slate.
    async fn teardown_partial_connect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.abort_session_tasks();
        self.abort_pending_tasks();

        if let Err(e) = self.ws_client.lock().await.disconnect().await {
            self.shutdown_errors
                .push(format!("Derive WebSocket shutdown failed: {e}"));
        }
        let (session_result, pending_result) =
            tokio::join!(self.await_session_tasks(), self.await_pending_tasks());
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Release);

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

    fn start_ws_dispatch(
        &self,
        rx: tokio::sync::mpsc::UnboundedReceiver<DeriveWsMessage>,
    ) -> anyhow::Result<()> {
        let session_spawner = self
            .session_tasks
            .spawner()
            .map_err(|e| anyhow::anyhow!("Derive session task admission is closed: {e}"))?;
        spawn_ws_dispatch(
            &session_spawner,
            rx,
            self.emitter.clone(),
            self.core.account_id,
            self.clock,
            Arc::clone(&self.dispatch_state),
            self.reconciliation_context(),
            Arc::clone(&self.is_connected),
            self.cancellation_token.clone(),
            self.config.rest_fallback_enabled,
        )
    }

    /// Starts the session's single WebSocket supervisor.
    ///
    /// One task owns redialling for the whole session, on both startup routes.
    /// A healthy start parks it immediately; it wakes only once the socket's own
    /// recovery has been exhausted (`ws_recovery_exhausted`), so it never races
    /// the transport's internal reconnect. Without it on the healthy route, a
    /// later terminal `SessionRecoveryFailed` would leave the client in REST
    /// mode for the rest of the process while still reporting itself connected.
    fn start_ws_supervisor(&self, channels: Vec<DeriveWsChannel>) -> anyhow::Result<()> {
        let session_spawner = self
            .session_tasks
            .spawner()
            .map_err(|e| anyhow::anyhow!("Derive session task admission is closed: {e}"))?;
        let ws_client = Arc::clone(&self.ws_client);
        let context = self.reconciliation_context();
        let emitter = self.emitter.clone();
        let account_id = self.core.account_id;
        let clock = self.clock;
        let dispatch_state = Arc::clone(&self.dispatch_state);
        let is_connected = Arc::clone(&self.is_connected);
        let cancellation = self.cancellation_token.clone();
        let rest_fallback_enabled = self.config.rest_fallback_enabled;

        self.session_tasks.spawn(async move {
            let mut retry_delay = RECONNECT_BASE_BACKOFF;
            loop {
                // Park until the session is known to be unusable: either the
                // transport's own recovery gave up (`SessionRecoveryFailed`),
                // or the reconciliation loop observed an unauthenticated
                // socket. A healthy startup therefore does nothing here until
                // the session actually fails.
                while !{
                    let state = &context.rest_execution_state;
                    state.ws_recovery_exhausted.load(Ordering::Acquire)
                        || (state.active.load(Ordering::Acquire)
                            && !state.websocket_ready.load(Ordering::Acquire))
                } {
                    tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return,
                        () = tokio::time::sleep(RECONNECT_BASE_BACKOFF) => {}
                    }
                }

                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break,
                    () = tokio::time::sleep(retry_delay) => {}
                }

                let receiver = {
                    let mut client = ws_client.lock().await;
                    match client.connect().await {
                        Ok(()) => match client.subscribe_channels(channels.clone()).await {
                            Ok(()) => client.take_event_receiver(),
                            Err(e) => {
                                log::warn!("Derive startup WebSocket subscription failed: {e}");
                                if let Err(disconnect_error) = client.disconnect().await {
                                    log::warn!("Failed to reset Derive WebSocket after subscription failure: {disconnect_error}");
                                }
                                None
                            }
                        },
                        Err(e) => {
                            log::warn!("Derive startup WebSocket retry failed: {e}");
                            None
                        }
                    }
                };

                let Some(receiver) = receiver else {
                    retry_delay = retry_delay
                        .mul_f64(RECONNECT_BACKOFF_FACTOR)
                        .min(RECONNECT_MAX_BACKOFF);
                    continue;
                };

                if let Err(e) = spawn_ws_dispatch(
                    &session_spawner,
                    receiver,
                    emitter.clone(),
                    account_id,
                    clock,
                    Arc::clone(&dispatch_state),
                    context.clone(),
                    Arc::clone(&is_connected),
                    cancellation.clone(),
                    rest_fallback_enabled,
                ) {
                    log::warn!("Failed to start Derive WebSocket dispatch after retry: {e}");
                    let _ = ws_client.lock().await.disconnect().await;
                    retry_delay = retry_delay
                        .mul_f64(RECONNECT_BACKOFF_FACTOR)
                        .min(RECONNECT_MAX_BACKOFF);
                    continue;
                }

                context
                    .rest_execution_state
                    .websocket_ready
                    .store(true, Ordering::Release);
                if let Err(e) = context.recover_after_reconnect().await {
                    // The transport itself is healthy — only the admission gate
                    // is unsatisfied (an incomplete, stale or not-yet-applied
                    // reconciliation). Tearing the socket down again would
                    // churn the venue connection over a condition that the
                    // reconciliation loop resolves on its own, so park instead
                    // and let it hand routing back when the generation is good.
                    log::warn!(
                        "Derive recovery after WebSocket redial is not complete yet: {e:?}; holding REST routing"
                    );
                    retry_delay = RECONNECT_BASE_BACKOFF;
                    context
                        .rest_execution_state
                        .ws_recovery_exhausted
                        .store(false, Ordering::Release);
                    continue;
                }

                // The session is healthy again; hand recovery back to the
                // transport and park until it exhausts itself once more.
                retry_delay = RECONNECT_BASE_BACKOFF;
                context
                    .rest_execution_state
                    .ws_recovery_exhausted
                    .store(false, Ordering::Release);
                log::info!("Derive execution WebSocket session redialled and reconciled");
            }
        })?;
        Ok(())
    }
}

#[expect(clippy::too_many_arguments)]
fn spawn_ws_dispatch(
    session_spawner: &TaskSpawner,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<DeriveWsMessage>,
    emitter: ExecutionEventEmitter,
    account_id: AccountId,
    clock: &'static AtomicTime,
    dispatch_state: Arc<WsDispatchState>,
    reconciliation: DeriveReconciliationContext,
    is_connected: Arc<AtomicBool>,
    cancellation: CancellationToken,
    rest_fallback_enabled: bool,
) -> anyhow::Result<()> {
    let child_spawner = session_spawner.clone();
    session_spawner.spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                maybe = rx.recv() => {
                    match maybe {
                        Some(DeriveWsMessage::Reconnected) => {
                            set_rest_fallback_active(
                                &reconciliation.rest_execution_state,
                                true,
                                "websocket_reconnected_pending_reconciliation",
                                clock.get_time_ns().as_u64(),
                            );
                            reconciliation
                                .rest_execution_state
                                .websocket_ready
                                .store(true, Ordering::Release);
                            let context = reconciliation.clone();
                            let task_cancellation = cancellation.clone();

                            if let Err(e) = child_spawner.spawn(async move {
                                tokio::select! {
                                    () = task_cancellation.cancelled() => {}
                                    result = context.recover_after_reconnect() => {
                                        if let Err(e) = result {
                                            log::warn!("Derive post-reconnect recovery failed: {e:?}");
                                        }
                                    }
                                }
                            }) {
                                log::warn!("Skipping Derive reconnect recovery after shutdown began: {e}");
                            }
                        }
                        Some(DeriveWsMessage::SessionRecoveryFailed(reason)) => {
                            set_rest_fallback_active(
                                &reconciliation.rest_execution_state,
                                true,
                                "websocket_session_recovery_failed",
                                clock.get_time_ns().as_u64(),
                            );
                            reconciliation
                                .rest_execution_state
                                .websocket_ready
                                .store(false, Ordering::Release);
                            // The transport has given up, so the supervisor now
                            // owns the next dial.
                            reconciliation
                                .rest_execution_state
                                .ws_recovery_exhausted
                                .store(true, Ordering::Release);
                            if !rest_fallback_enabled {
                                is_connected.store(false, Ordering::Release);
                            }
                            log::error!("Derive execution WebSocket recovery failed: {reason}");
                        }
                        Some(DeriveWsMessage::Subscription(payload))
                            if payload.channel.ends_with(".balances") =>
                        {
                            let context = reconciliation.clone();
                            let task_cancellation = cancellation.clone();

                            if let Err(e) = child_spawner.spawn(async move {
                                tokio::select! {
                                    () = task_cancellation.cancelled() => {}
                                    result = context.refresh_account_state() => {
                                        if let Err(e) = result {
                                            log::warn!("Derive balance update refresh failed: {e:?}");
                                        }
                                    }
                                }
                            }) {
                                log::warn!("Skipping Derive account refresh after shutdown began: {e}");
                            }
                        }
                        Some(message) => handle_ws_message(
                            message,
                            &emitter,
                            account_id,
                            clock,
                            &dispatch_state,
                        ),
                        None => break,
                    }
                }
            }
        }
    })?;
    Ok(())
}

#[async_trait(?Send)]
impl ExecutionClient for DeriveExecutionClient {
    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        *DERIVE_VENUE
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();

        log::info!(
            "Started: client_id={}, account_id={}, subaccount_id={}, environment={:?}, proxy_url={:?}",
            self.core.client_id,
            self.core.account_id,
            self.credential.subaccount_id(),
            self.config.environment,
            self.config.proxy_url,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }

        log::info!("Stopping Derive execution client");

        self.cancellation_token.cancel();
        self.abort_session_tasks();
        self.abort_pending_tasks();

        self.core.set_stopped();
        self.core.set_disconnected();
        self.is_connected.store(false, Ordering::Release);

        log::info!("Derive execution client stopped");
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected()
            && !self.cancellation_token.is_cancelled()
            && self.session_tasks.is_open()
            && self.pending_tasks.is_open()
        {
            return Ok(());
        }

        log::info!("Connecting Derive execution client");

        if self.cancellation_token.is_cancelled()
            || !self.session_tasks.is_open()
            || !self.pending_tasks.is_open()
        {
            self.teardown_partial_connect().await?;
            self.session_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start Derive session generation: {e}"))?;
            self.pending_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start Derive task generation: {e}"))?;
            self.cancellation_token = CancellationToken::new();
        }
        let cancellation_token = self.cancellation_token.clone();
        let ws_shutdown = self.ws_shutdown.clone();
        let setup_guard =
            TaskGroupGuard::new(&[&self.session_tasks, &self.pending_tasks], move || {
                cancellation_token.cancel();
                ws_shutdown.begin_shutdown();
            });

        self.ensure_instruments_initialized()
            .await
            .context("failed to initialize Derive instruments")?;

        let channels = vec![
            DeriveWsChannel::orders(self.credential.subaccount_id()),
            DeriveWsChannel::private_trades(self.credential.subaccount_id()),
            DeriveWsChannel::balances(self.credential.subaccount_id()),
        ];
        // The design's startup trigger is "disconnected **or**
        // unauthenticated", so both a failed upgrade and a session that comes
        // up but cannot authenticate its private subscriptions enter REST
        // fallback rather than hard-failing startup. A genuinely bad credential
        // still fails, because the initial REST reconciliation signs with the
        // same key.
        let mut fallback_reason: Option<String> = None;
        let mut event_receiver = None;

        let ws_connect_result = self.ws_client.lock().await.connect().await;
        match ws_connect_result {
            Err(e) => {
                if !self.config.rest_fallback_enabled {
                    self.teardown_partial_connect().await?;
                    return Err(anyhow::Error::new(e).context("failed to connect Derive WebSocket"));
                }
                fallback_reason = Some(format!("WebSocket connect failed: {e}"));
            }
            Ok(()) => {
                let Some(rx) = self.ws_client.lock().await.take_event_receiver() else {
                    let e = anyhow::anyhow!("Derive execution WS event receiver not initialized");
                    if let Err(teardown_error) = self.teardown_partial_connect().await {
                        return Err(e.context(format!(
                            "Derive execution startup teardown failed: {teardown_error}"
                        )));
                    }
                    return Err(e);
                };
                event_receiver = Some(rx);

                let subscribe_result = {
                    self.ws_client
                        .lock()
                        .await
                        .subscribe_channels(channels.clone())
                        .await
                };
                if let Err(e) = subscribe_result {
                    if !self.config.rest_fallback_enabled {
                        log::warn!("Derive private WS subscriptions failed: {e}; tearing down");
                        if let Err(teardown_error) = self.teardown_partial_connect().await {
                            return Err(anyhow::Error::new(e).context(format!(
                                "Derive execution startup teardown failed: {teardown_error}"
                            )));
                        }
                        return Err(
                            anyhow::Error::new(e).context("failed Derive private WS subscriptions")
                        );
                    }
                    fallback_reason = Some(format!("WebSocket private subscriptions failed: {e}"));
                }
            }
        }

        if let Some(reason) = fallback_reason {
            log::warn!("Derive WebSocket is unusable at startup; entering REST fallback: {reason}");
            // Drop any half-open session so the retry task dials from scratch.
            drop(event_receiver);
            if let Err(e) = self.ws_client.lock().await.disconnect().await {
                log::warn!("Failed to reset Derive WebSocket before REST fallback startup: {e}");
            }
            set_rest_fallback_active(
                &self.rest_execution_state,
                true,
                "websocket_unusable_at_startup",
                self.clock.get_time_ns().as_u64(),
            );
            self.reconciliation_context()
                .reconcile_private_state()
                .await
                .context("failed initial Derive REST private-state reconciliation")?;
            if let Err(e) = self
                .await_account_registered(DERIVE_ACCOUNT_REGISTRATION_TIMEOUT_SECS)
                .await
            {
                self.teardown_partial_connect().await?;
                return Err(e.context("failed waiting for Derive REST account registration"));
            }
            self.rest_execution_state
                .ws_recovery_exhausted
                .store(true, Ordering::Release);
            self.start_rest_reconciliation_loop()
                .context("failed to start Derive REST reconciliation task")?;
            self.start_ws_supervisor(channels)
                .context("failed to start Derive WebSocket supervisor task")?;

            self.core.set_connected();
            self.is_connected.store(true, Ordering::Release);
            setup_guard.disarm();
            return Ok(());
        }

        let Some(rx) = event_receiver else {
            let e = anyhow::anyhow!("Derive execution WS event receiver was consumed");
            if let Err(teardown_error) = self.teardown_partial_connect().await {
                return Err(e.context(format!(
                    "Derive execution startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e);
        };

        if let Err(e) = self.start_ws_dispatch(rx) {
            if let Err(teardown_error) = self.teardown_partial_connect().await {
                return Err(e.context(format!(
                    "Derive execution startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e.context("failed to register Derive execution WebSocket dispatch task"));
        }

        self.start_rest_reconciliation_loop()
            .context("failed to start Derive REST reconciliation task")?;
        self.rest_execution_state
            .ws_recovery_exhausted
            .store(false, Ordering::Release);
        self.start_ws_supervisor(channels)
            .context("failed to start Derive WebSocket supervisor task")?;

        // Fail-fast if the initial account snapshot cannot load: without it,
        // `await_account_registered` would block the full timeout window and
        // surface a misleading registration timeout. Tear down the WS we
        // already started so the caller does not leak the dispatch task.
        if let Err(e) = self.refresh_account_state().await {
            log::warn!("Initial Derive account state refresh failed: {e}; tearing down");
            if let Err(teardown_error) = self.teardown_partial_connect().await {
                return Err(e.context(format!(
                    "Derive execution startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e.context("failed initial Derive account state refresh"));
        }

        if let Err(e) = self
            .await_account_registered(DERIVE_ACCOUNT_REGISTRATION_TIMEOUT_SECS)
            .await
        {
            log::warn!("Derive account did not register in time: {e}; tearing down");
            if let Err(teardown_error) = self.teardown_partial_connect().await {
                return Err(e.context(format!(
                    "Derive execution startup teardown failed: {teardown_error}"
                )));
            }
            return Err(e.context("failed waiting for Derive account registration"));
        }

        self.core.set_connected();
        self.is_connected.store(true, Ordering::Release);
        // Routing, not admission: an unresolved write deliberately does not
        // force REST routing, because the unresolved-write gate halts
        // risk-increasing commands on either transport.
        let recovery_pending = self
            .rest_execution_state
            .reconciliation_failed
            .load(Ordering::Acquire);
        set_rest_fallback_active(
            &self.rest_execution_state,
            recovery_pending,
            if recovery_pending {
                "startup_recovery_pending"
            } else {
                "websocket_connected"
            },
            self.clock.get_time_ns().as_u64(),
        );
        self.rest_execution_state
            .websocket_ready
            .store(true, Ordering::Release);
        setup_guard.disarm();
        log::info!(
            "Connected Derive execution client ({:?})",
            self.config.environment
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        log::info!("Disconnecting Derive execution client");
        self.teardown_partial_connect().await?;
        log::info!("Derive execution client disconnected");
        Ok(())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .emit_account_state(balances, margins, reported, ts_event, info);
        Ok(())
    }

    fn on_instrument(&mut self, instrument: InstrumentAny) {
        self.dispatch_state.register_instrument_precision(
            instrument.id(),
            instrument.price_precision(),
            instrument.size_precision(),
        );
        // The exec-side instrument cache holds `DeriveInstrument` records so
        // signing can pull `base_asset_address` / `base_asset_sub_id`; the
        // generic `InstrumentAny` shape published on the bus does not carry
        // those, so the data client populates the cache via
        // [`Self::cache_instrument`] from its bootstrap pass instead.
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        self.reconciliation_context()
            .generate_order_status_report(cmd)
            .await
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.reconciliation_context()
            .generate_order_status_reports(cmd, false)
            .await
    }

    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        self.reconciliation_context()
            .generate_fill_reports(cmd)
            .await
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let snapshot = self
            .reconciliation_context()
            .generate_position_status_snapshot(cmd)
            .await?;
        Ok(snapshot.reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        Box::pin(
            self.reconciliation_context()
                .generate_mass_status(lookback_mins),
        )
        .await
        .map(Some)
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        self.note_reconciliation_application();
        let order = self.core.cache().try_order_owned(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        // Deny before emit_order_submitted so unsupported fields never
        // surface as venue rejections.
        let is_trigger_order = is_derive_trigger_order_type(order.order_type());
        let support = if is_trigger_order {
            validate_trigger_order_support(&order)
        } else {
            validate_order_support(&order)
        };

        if let Err(e) = support {
            let reason = e.to_string();
            log::warn!("Cannot submit order {}: {reason}", order.client_order_id());
            self.emitter.emit_order_denied(&order, &reason);
            return Ok(());
        }

        let use_rest_fallback = self.should_use_rest_fallback();
        // Reduce-only submits cannot increase exposure — the venue refuses a
        // reduce-only order that would — so they are deliberately exempt from
        // the private-state freshness and quote gates, and from unrelated
        // account-wide uncertainty. That asymmetry rests entirely on the venue
        // enforcing `reduce_only`, and it is what keeps the de-risking lever
        // available during exactly the outage this failover exists for. The
        // local position view may be stale while such an order is sent.
        let write_kind = if order.is_reduce_only() {
            RestWriteKind::ReducingSubmit
        } else {
            RestWriteKind::RiskIncreasingSubmit
        };
        let cached_quote_ts_event = self.cached_quote_ts_event(&cmd.instrument_id);
        let gate_reason = if is_trigger_order && use_rest_fallback {
            Some("Derive trigger order submission is unsupported during REST fallback")
        } else if !write_kind.is_risk_increasing() {
            None
        } else if use_rest_fallback {
            self.risk_increasing_write_blocked(cached_quote_ts_event)
        } else {
            // Routing is over the WebSocket, where freshness gates do not
            // apply, but an unresolved write still halts new exposure.
            self.unresolved_write_halt()
        };
        if let Some(reason) = gate_reason {
            log_rest_write_halted(
                &self.rest_execution_state,
                order.client_order_id(),
                write_kind,
                reason,
                cached_quote_ts_event,
                self.clock.get_time_ns().as_u64(),
            );
            self.emitter.emit_order_denied(&order, reason);
            return Ok(());
        }

        // Spot has no position to reduce; the venue rejects reduce-only
        // unconditionally (11025), so deny locally. Perp/option reduce-only is
        // position-conditional and must still reach the venue.
        if order.is_reduce_only()
            && matches!(
                self.core.cache().instrument(&cmd.instrument_id),
                Some(InstrumentAny::CurrencyPair(_))
            )
        {
            let reason = format!(
                "reduce-only is not supported for spot instrument {}; Derive spot has no position to reduce",
                cmd.instrument_id,
            );
            log::warn!("{reason}");
            self.emitter.emit_order_denied(&order, &reason);
            return Ok(());
        }

        // Keep the existing OrderDenied path here, then refresh before signing
        let market_quote = if order.order_type() == OrderType::Market {
            match self.core.cache().quote(&cmd.instrument_id) {
                Some(_) => Some(()),
                None => {
                    let reason = format!(
                        "no cached quote for {}; subscribe to quote data before submitting market orders",
                        cmd.instrument_id,
                    );
                    log::warn!("{reason}");
                    self.emitter.emit_order_denied(&order, &reason);
                    return Ok(());
                }
            }
        } else {
            None
        };

        let venue_symbol = format_venue_symbol(&cmd.instrument_id)?.to_string();
        let http_client = self.http_client.clone();
        let ws_exec = self.ws_exec.clone();
        let signing = self.signing.clone();
        let nonce_manager = self.nonce_manager.clone();
        let wallet_str = self.credential.wallet_address().to_string();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let instruments = self.instruments.clone();
        let instrument_id = cmd.instrument_id;
        let order_for_task = order.clone();
        let account_id = self.core.account_id;

        // Capture identity so the WS dispatch can route subsequent updates
        // for this order to proper events rather than execution reports.
        let identity = OrderIdentity {
            instrument_id: order.instrument_id(),
            strategy_id: order.strategy_id(),
            order_side: order.order_side(),
            order_type: order.order_type(),
        };
        self.dispatch_state
            .register_identity(order.client_order_id(), identity);
        let rest_execution_state = Arc::clone(&self.rest_execution_state);

        self.emitter.emit_order_submitted(&order);

        let slippage_bps = self.signing.market_order_slippage_bps;
        let dispatch_state = self.dispatch_state.clone();
        let rest_http_client = http_client.clone();
        let max_account_state_age_secs = self.config.max_account_state_age_secs;
        let rest_fallback_enabled = self.config.rest_fallback_enabled;
        // The quote age bound belongs to the fallback feature, so the
        // default-off WebSocket-only path keeps its existing behaviour. It is
        // the dedicated execution quote bound, never the account-state bound.
        let quote_max_age_secs = rest_fallback_enabled
            .then_some(self.config.max_quote_age_secs)
            .flatten();

        self.spawn_task("submit_order", async move {
            let instrument = match cached_or_fetch_instrument(
                &http_client,
                &instruments,
                &instrument_id,
                &venue_symbol,
            )
            .await
            {
                Ok(i) => i,
                Err(e) => {
                    log::warn!("Failed to resolve instrument {venue_symbol}: {e}");
                    dispatch_state.forget(&order_for_task.client_order_id());
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected(
                        &order_for_task,
                        &format!("instrument resolution failed: {e}"),
                        ts,
                        false,
                    );
                    return Ok(());
                }
            };

            // Lazy-resolution net: the synchronous deny is skipped when the
            // cache was empty at submit time. OrderSubmitted already fired, so
            // reject here rather than deny.
            if order_for_task.is_reduce_only()
                && instrument.instrument_type == DeriveInstrumentType::Erc20
            {
                let reason = format!(
                    "reduce-only is not supported for spot instrument {}; Derive spot has no position to reduce",
                    order_for_task.instrument_id(),
                );
                log::warn!("{reason}");
                dispatch_state.forget(&order_for_task.client_order_id());
                let ts = clock.get_time_ns();
                emitter.emit_order_rejected(&order_for_task, &reason, ts, false);
                return Ok(());
            }

            // Avoid signing against a quote captured before instrument resolution
            let mut refreshed_quote_ts_event = None;
            let explicit_price = if market_quote.is_some() {
                let quote = match refresh_market_order_quote(
                    &http_client,
                    &venue_symbol,
                    &instrument,
                    clock,
                    quote_max_age_secs,
                )
                .await
                {
                    Ok(quote) => {
                        refreshed_quote_ts_event = Some(quote.ts_event);
                        quote
                    }
                    Err(e) => {
                        let reason = format!(
                            "market-order quote refresh failed for {}: {e}",
                            order_for_task.client_order_id(),
                        );
                        log::warn!("{reason}");
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(&order_for_task, &reason, ts, false);
                        return Ok(());
                    }
                };

                match market_order_limit_price(
                    &quote,
                    order_for_task.order_side(),
                    slippage_bps,
                    instrument.tick_size,
                ) {
                    Some(p) => Some(p),
                    None => {
                        let reason = format!(
                            "market-order slippage bound is non-positive for {} ({} bps)",
                            order_for_task.client_order_id(),
                            slippage_bps,
                        );
                        log::warn!("{reason}");
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(&order_for_task, &reason, ts, false);
                        return Ok(());
                    }
                }
            } else if matches!(
                order_for_task.order_type(),
                OrderType::StopMarket | OrderType::MarketIfTouched
            ) {
                let trigger_price = match order_for_task.trigger_price() {
                    Some(price) => price.as_decimal(),
                    None => {
                        let reason = format!(
                            "trigger market order {} is missing trigger_price",
                            order_for_task.client_order_id(),
                        );
                        log::warn!("{reason}");
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(&order_for_task, &reason, ts, false);
                        return Ok(());
                    }
                };

                match trigger_market_limit_price(
                    trigger_price,
                    order_for_task.order_side(),
                    slippage_bps,
                    instrument.tick_size,
                ) {
                    Some(p) => Some(p),
                    None => {
                        let reason = format!(
                            "trigger market-order slippage bound is non-positive for {} ({} bps)",
                            order_for_task.client_order_id(),
                            slippage_bps,
                        );
                        log::warn!("{reason}");
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(&order_for_task, &reason, ts, false);
                        return Ok(());
                    }
                }
            } else {
                None
            };

            let matching_reservation = if use_rest_fallback {
                None
            } else {
                match ws_exec
                    .reserve_matching_request(
                        if is_trigger_order {
                            "private/trigger_order"
                        } else {
                            "private/order"
                        },
                        &instrument.instrument_name,
                    )
                    .await
                {
                    Ok(reservation) => Some(reservation),
                    Err(e) => {
                        let (reason, due_post_only) = ws_rejection_reason(&e);
                        log::warn!(
                            "Cannot reserve Derive order quota for {}: {reason}",
                            order_for_task.client_order_id(),
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(
                            &order_for_task,
                            &reason,
                            ts,
                            due_post_only,
                        );
                        return Ok(());
                    }
                }
            };

            if is_trigger_order {
                let nonce = match resolve_submit_nonce(
                    nonce_manager.next_nonce(&wallet_str, signing.subaccount_id),
                    &emitter,
                    &dispatch_state,
                    &order_for_task,
                    clock,
                ) {
                    Some(nonce) => nonce,
                    None => return Ok(()),
                };
                let expiry = trigger_order_signature_expiry(clock);
                let payload = match trigger_order_to_derive_payload(
                    &order_for_task,
                    &instrument,
                    signing.subaccount_id,
                    signing.wallet_address,
                    &signing.signer,
                    nonce,
                    expiry,
                    signing.trade_module_address,
                    signing.domain_separator,
                    signing.action_typehash,
                    signing.max_fee_per_contract,
                    explicit_price,
                    ws_exec.conn_id(),
                    UUID4::new().to_string(),
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        log::warn!(
                            "Trigger order encode failed for {}: {e}",
                            order_for_task.client_order_id()
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(
                            &order_for_task,
                            &format!("order encoding failed: {e}"),
                            ts,
                            false,
                        );
                        return Ok(());
                    }
                };

                log::debug!(
                    "Derive trigger submit payload client_order_id={} instrument_name={} direction={} order_type={} time_in_force={} amount={} limit_price={} trigger_price={:?} trigger_price_type={:?} trigger_type={:?}",
                    order_for_task.client_order_id(),
                    payload.order.instrument_name.as_str(),
                    payload.order.direction,
                    payload.order.order_type,
                    payload.order.time_in_force,
                    payload.order.amount,
                    payload.order.limit_price,
                    payload.order.trigger_price,
                    payload.order.trigger_price_type,
                    payload.order.trigger_type,
                );

                let Some(reservation) = matching_reservation else {
                    let reason =
                        "Derive trigger-order submit lost its matching-engine reservation";
                    log::warn!("{reason} for {}", order_for_task.client_order_id());
                    dispatch_state.forget(&order_for_task.client_order_id());
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected(&order_for_task, reason, ts, false);
                    return Ok(());
                };
                match ws_exec
                    .submit_trigger_order_after_rate_limit(&payload, reservation)
                    .await
                {
                    Ok(order) => {
                        let venue_order_id = VenueOrderId::new(order.order_id.as_str());
                        dispatch_state.record_venue_order_id(
                            order_for_task.client_order_id(),
                            venue_order_id,
                        );
                        let ts_now = clock.get_time_ns();
                        ensure_accepted_emitted(
                            &emitter,
                            &dispatch_state,
                            order_for_task.client_order_id(),
                            identity,
                            venue_order_id,
                            account_id,
                            ts_now,
                            ts_now,
                        );
                        log::debug!(
                            "Trigger order submitted: client_order_id={} venue_order_id={venue_order_id}",
                            order_for_task.client_order_id(),
                        );
                    }
                    Err(e) if is_write_outcome_ambiguous_ws(&e) => {
                        log::warn!(
                            "Derive trigger submit for {} returned ambiguous WS outcome: {e}; awaiting reconciliation",
                            order_for_task.client_order_id(),
                        );
                        if rest_fallback_enabled {
                            record_unresolved_write(
                                &rest_execution_state,
                                order_for_task.client_order_id(),
                                None,
                                write_kind,
                                clock.get_time_ns().as_u64(),
                            );
                        }
                    }
                    Err(e) => {
                        let (reason, due_post_only) = ws_rejection_reason(&e);
                        log::debug!(
                            "Derive rejected trigger order {}: {reason}",
                            order_for_task.client_order_id(),
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(
                            &order_for_task,
                            &reason,
                            ts,
                            due_post_only,
                        );
                    }
                }
                return Ok(());
            }

            let expiry =
                match normal_order_signature_expiry(clock, signing.signature_expiry_secs) {
                    Ok(expiry) => expiry,
                    Err(e) => {
                        log::warn!(
                            "Order expiry validation failed for {}: {e}",
                            order_for_task.client_order_id()
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(
                            &order_for_task,
                            &format!("order expiry validation failed: {e}"),
                            ts,
                            false,
                        );
                        return Ok(());
                    }
                };
            let nonce = match resolve_submit_nonce(
                nonce_manager.next_nonce(&wallet_str, signing.subaccount_id),
                &emitter,
                &dispatch_state,
                &order_for_task,
                clock,
            ) {
                Some(nonce) => nonce,
                None => return Ok(()),
            };
            let payload = match order_to_derive_payload(
                &order_for_task,
                &instrument,
                signing.subaccount_id,
                signing.wallet_address,
                &signing.signer,
                nonce,
                expiry,
                signing.trade_module_address,
                signing.domain_separator,
                signing.action_typehash,
                signing.max_fee_per_contract,
                explicit_price,
            ) {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("Order encode failed for {}: {e}", order_for_task.client_order_id());
                    dispatch_state.forget(&order_for_task.client_order_id());
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected(
                        &order_for_task,
                        &format!("order encoding failed: {e}"),
                        ts,
                        false,
                    );
                    return Ok(());
                }
            };

            // Pre-flight debug log so a venue 11012-style rejection can be
            // diagnosed without re-running with full payload tracing.
            log::debug!(
                "Derive submit payload client_order_id={} instrument_name={} direction={} order_type={} time_in_force={} amount={} limit_price={}",
                order_for_task.client_order_id(),
                payload.instrument_name.as_str(),
                payload.direction,
                payload.order_type,
                payload.time_in_force,
                payload.amount,
                payload.limit_price,
            );

            if use_rest_fallback {
                let now_ns = clock.get_time_ns().as_u64();
                let quote_ts_event = refreshed_quote_ts_event.or(cached_quote_ts_event);
                let token = match reserve_write(
                    &rest_execution_state,
                    order_for_task.client_order_id(),
                    None,
                    write_kind,
                    quote_ts_event,
                    now_ns,
                    FreshnessBounds {
                        private_state_secs: max_account_state_age_secs,
                        quote_secs: quote_max_age_secs,
                    },
                ) {
                    Ok(token) => Some(token),
                    Err(reason) => {
                        log_rest_write_halted(
                            &rest_execution_state,
                            order_for_task.client_order_id(),
                            write_kind,
                            reason,
                            quote_ts_event,
                            now_ns,
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        emitter.emit_order_rejected(&order_for_task, reason, clock.get_time_ns(), false);
                        return Ok(());
                    }
                };
                match rest_http_client.submit_order(&payload).await {
                    Ok(submitted_order) => {
                        let venue_order_id = VenueOrderId::new(submitted_order.order_id.as_str());
                        dispatch_state.record_venue_order_id(
                            order_for_task.client_order_id(),
                            venue_order_id,
                        );
                        resolve_write(
                            &rest_execution_state,
                            &order_for_task.client_order_id(),
                            token,
                        );
                        let ts_now = clock.get_time_ns();
                        ensure_accepted_emitted(
                            &emitter,
                            &dispatch_state,
                            order_for_task.client_order_id(),
                            identity,
                            venue_order_id,
                            account_id,
                            ts_now,
                            ts_now,
                        );
                    }
                    Err(e) if e.is_ambiguous_write_outcome() => {
                        log::warn!(
                            "Derive REST submit for {} returned ambiguous outcome: {e}; awaiting reconciliation",
                            order_for_task.client_order_id(),
                        );
                        mark_write_ambiguous(
                            &rest_execution_state,
                            &order_for_task.client_order_id(),
                            token,
                            clock.get_time_ns().as_u64(),
                        );
                    }
                    Err(e) => {
                        resolve_write(
                            &rest_execution_state,
                            &order_for_task.client_order_id(),
                            token,
                        );
                        dispatch_state.forget(&order_for_task.client_order_id());
                        let ts = clock.get_time_ns();
                        emitter.emit_order_rejected(
                            &order_for_task,
                            &e.to_string(),
                            ts,
                            false,
                        );
                    }
                }
                return Ok(());
            }

            // Discard the result (and any `trades` it carries): fills arrive on
            // the `.trades` channel and are deduped by trade id.
            let Some(reservation) = matching_reservation else {
                let reason = "Derive WebSocket submit lost its matching-engine reservation";
                log::warn!("{reason} for {}", order_for_task.client_order_id());
                dispatch_state.forget(&order_for_task.client_order_id());
                let ts = clock.get_time_ns();
                emitter.emit_order_rejected(&order_for_task, reason, ts, false);
                return Ok(());
            };
            match ws_exec.submit_order_after_rate_limit(&payload, reservation).await {
                Ok(_) => {
                    log::debug!(
                        "Order submitted: client_order_id={}",
                        order_for_task.client_order_id(),
                    );
                }
                // See docs/integrations/derive.md "Order rejection semantics".
                Err(e) if is_write_outcome_ambiguous_ws(&e) => {
                    log::warn!(
                        "Derive submit for {} returned ambiguous WS outcome: {e}; awaiting reconciliation",
                        order_for_task.client_order_id(),
                    );
                    // The transport died after the routing decision was taken,
                    // so the next command will be routed through REST: record
                    // the uncertainty or that command would pass the entry gate
                    // while a possibly-live WebSocket order exists.
                    if rest_fallback_enabled {
                        record_unresolved_write(
                            &rest_execution_state,
                            order_for_task.client_order_id(),
                            None,
                            write_kind,
                            clock.get_time_ns().as_u64(),
                        );
                    }
                }
                Err(e) => {
                    let (reason, due_post_only) = ws_rejection_reason(&e);
                    log::debug!(
                        "Derive rejected order {}: {reason}",
                        order_for_task.client_order_id(),
                    );
                    dispatch_state.forget(&order_for_task.client_order_id());
                    let ts = clock.get_time_ns();
                    emitter.emit_order_rejected(&order_for_task, &reason, ts, due_post_only);
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        for order in orders {
            let sub = SubmitOrder::from_order(
                &order,
                cmd.trader_id,
                cmd.client_id,
                cmd.position_id,
                UUID4::new(),
                cmd.ts_init,
            );
            self.submit_order(sub)?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        self.note_reconciliation_application();
        let use_rest_fallback = self.should_use_rest_fallback();
        let http_client = self.http_client.clone();
        let ws_exec = self.ws_exec.clone();
        let subaccount_id = self.credential.subaccount_id();
        let venue_symbol = format_venue_symbol(&cmd.instrument_id)?.to_string();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let account_id = self.core.account_id;
        let dispatch_state = self.dispatch_state.clone();
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let venue_order_id = cmd.venue_order_id;
        let is_trigger_order = self
            .core
            .cache()
            .order(&client_order_id)
            .is_some_and(|order| is_derive_trigger_order_type(order.order_type()));
        let is_resolved_open_order = self
            .core
            .cache()
            .order(&client_order_id)
            .is_some_and(|order| !order.is_closed() && order.venue_order_id() == venue_order_id);

        let mut cancel_token: Option<OperationToken> = None;
        if use_rest_fallback {
            let reason = if is_trigger_order {
                Some("Derive trigger-order cancellation is unsupported during REST fallback")
            } else if venue_order_id.is_none() {
                Some("Derive REST cancellation requires a resolved venue_order_id")
            } else if !is_resolved_open_order {
                Some(
                    "Derive REST cancellation requires a currently open order with matching venue state",
                )
            } else {
                // Cancellation is exposure-reducing, so it needs only this
                // order's own outcome to be resolved; it is deliberately not
                // gated on account-wide freshness.
                match reserve_write(
                    &self.rest_execution_state,
                    client_order_id,
                    venue_order_id,
                    RestWriteKind::Cancel,
                    None,
                    self.clock.get_time_ns().as_u64(),
                    self.freshness_bounds(),
                ) {
                    Ok(token) => {
                        cancel_token = Some(token);
                        None
                    }
                    Err(reason) => Some(reason),
                }
            };
            if let Some(reason) = reason {
                self.emitter.emit_order_cancel_rejected_event(
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    venue_order_id,
                    reason,
                    clock.get_time_ns(),
                );
                return Ok(());
            }
        }
        let rest_execution_state = Arc::clone(&self.rest_execution_state);
        let rest_fallback_enabled = self.config.rest_fallback_enabled;

        self.spawn_task("cancel_order", async move {
            if use_rest_fallback {
                let Some(resolved_venue_order_id) = venue_order_id else {
                    let reason = "Derive REST cancellation requires a resolved venue_order_id";
                    log::warn!("Cannot cancel {client_order_id}: {reason}");
                    resolve_write(&rest_execution_state, &client_order_id, cancel_token);
                    emitter.emit_order_cancel_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        None,
                        reason,
                        clock.get_time_ns(),
                    );
                    return Ok(());
                };
                match http_client
                    .cancel_order(&DeriveCancelParams::new(
                        subaccount_id,
                        venue_symbol.as_str(),
                        resolved_venue_order_id.as_str(),
                    ))
                    .await
                {
                    Ok(_) => {
                        // The venue confirmed the cancellation, so this write's
                        // outcome is known and must not keep the gate closed.
                        resolve_write(&rest_execution_state, &client_order_id, cancel_token);
                    }
                    Err(e) if e.is_ambiguous_write_outcome() => {
                        log::warn!(
                            "Derive REST cancel for {client_order_id} returned ambiguous outcome: {e}; awaiting reconciliation",
                        );
                        mark_write_ambiguous(
                            &rest_execution_state,
                            &client_order_id,
                            cancel_token,
                            clock.get_time_ns().as_u64(),
                        );
                    }
                    Err(e) => {
                        resolve_write(&rest_execution_state, &client_order_id, cancel_token);
                        emitter.emit_order_cancel_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            Some(resolved_venue_order_id),
                            &e.to_string(),
                            clock.get_time_ns(),
                        );
                    }
                }
                return Ok(());
            }
            let outcome = match venue_order_id {
                Some(venue_order_id) if is_trigger_order => {
                    ws_exec
                        .cancel_trigger_order(&DeriveCancelTriggerOrderParams::new(
                            subaccount_id,
                            venue_order_id.as_str(),
                        ))
                        .await
                        .map(Some)
                }
                Some(venue_order_id) => ws_exec
                    .cancel_order(&DeriveCancelParams::new(
                        subaccount_id,
                        venue_symbol.as_str(),
                        venue_order_id.as_str(),
                    ))
                    .await
                    .map(|()| None),
                None if is_trigger_order => {
                    let trigger_orders = match http_client
                        .get_trigger_orders(&DeriveGetTriggerOrdersParams::new(subaccount_id))
                        .await
                    {
                        Ok(result) => result.orders,
                        Err(e) => {
                            let reason = format!("failed to resolve trigger order by label: {e}");
                            log::warn!("Cannot cancel trigger order {client_order_id}: {reason}");
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                None,
                                &reason,
                                clock.get_time_ns(),
                            );
                            return Ok(());
                        }
                    };
                    let Some(trigger_order) = trigger_orders.into_iter().find(|order| {
                        order.label == client_order_id.as_str()
                            && order.instrument_name == venue_symbol
                    }) else {
                        let reason = "trigger order not found for client_order_id";
                        log::warn!("Cannot cancel trigger order {client_order_id}: {reason}");
                        emitter.emit_order_cancel_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            None,
                            reason,
                            clock.get_time_ns(),
                        );
                        return Ok(());
                    };
                    ws_exec
                        .cancel_trigger_order(&DeriveCancelTriggerOrderParams::new(
                            subaccount_id,
                            trigger_order.order_id.as_str(),
                        ))
                        .await
                        .map(Some)
                }
                None => ws_exec
                    .cancel_by_label(&DeriveCancelByLabelParams::new(
                        subaccount_id,
                        client_order_id.as_str(),
                    ))
                    .await
                    .map(|result| {
                        if result.cancelled_orders == 0 {
                            let reason = "no open order matched the client_order_id label";
                            log::debug!(
                                "Derive rejected cancel for {client_order_id}: {reason}"
                            );
                            let ts = clock.get_time_ns();
                            emitter.emit_order_cancel_rejected_event(
                                strategy_id,
                                instrument_id,
                                client_order_id,
                                None,
                                reason,
                                ts,
                            );
                        }
                        None
                    }),
            };

            match outcome {
                Ok(Some(canceled_order)) => {
                    let canceled_venue_order_id =
                        VenueOrderId::new(canceled_order.order_id.as_str());
                    let ts = clock.get_time_ns();

                    ensure_canceled_emitted(
                        &emitter,
                        &dispatch_state,
                        client_order_id,
                        OrderIdentity {
                            instrument_id,
                            strategy_id,
                            order_side: match canceled_order.direction {
                                DeriveOrderSide::Buy => OrderSide::Buy,
                                DeriveOrderSide::Sell => OrderSide::Sell,
                            },
                            order_type: derive_order_type_to_nautilus_for_order(
                                canceled_order.order_type,
                                canceled_order.trigger_type,
                            ),
                        },
                        canceled_venue_order_id,
                        account_id,
                        ts,
                        ts,
                    );
                    dispatch_state.forget(&client_order_id);
                }
                Ok(None) => {}
                // See docs/integrations/derive.md "Order rejection semantics".
                Err(e) if is_write_outcome_ambiguous_ws(&e) => {
                    log::warn!(
                        "Derive cancel for {client_order_id} returned ambiguous WS outcome: {e}; awaiting reconciliation",
                    );
                    if rest_fallback_enabled {
                        record_unresolved_write(
                            &rest_execution_state,
                            client_order_id,
                            venue_order_id,
                            RestWriteKind::Cancel,
                            clock.get_time_ns().as_u64(),
                        );
                    }
                }
                Err(e) => {
                    let (reason, _) = ws_rejection_reason(&e);
                    log::debug!("Derive rejected cancel for {client_order_id}: {reason}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_cancel_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        venue_order_id,
                        &reason,
                        ts,
                    );
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let use_rest_fallback = self.should_use_rest_fallback();
        let venue_symbol = format_venue_symbol(&cmd.instrument_id)?.to_string();
        let side_filter = cmd.order_side;
        let cache = self.core.cache();
        let orders = cache.orders_open_refs(
            Some(&self.core.venue),
            Some(&cmd.instrument_id),
            None,
            Some(&self.core.account_id),
            side_filter,
        );
        let mut cancels = Vec::with_capacity(orders.len());

        for order in orders {
            let client_order_id = order.client_order_id();
            if cache.client_id(&client_order_id) != Some(&self.core.client_id) {
                continue;
            }

            if use_rest_fallback {
                let reason = "Derive cancel_all_orders is unsupported during REST fallback; cancel resolved orders individually";
                self.emitter.emit_order_cancel_rejected_event(
                    order.strategy_id(),
                    cmd.instrument_id,
                    client_order_id,
                    order.venue_order_id(),
                    reason,
                    self.clock.get_time_ns(),
                );
                continue;
            }

            let is_trigger = is_derive_trigger_order_type(order.order_type());
            if side_filter.is_none() && !is_trigger {
                continue;
            }

            let Some(venue_order_id) = order.venue_order_id() else {
                log::warn!(
                    "Cannot cancel all orders for {}: order {client_order_id} has no venue_order_id",
                    cmd.instrument_id,
                );
                return Ok(());
            };
            cancels.push((venue_order_id, is_trigger));
        }
        drop(cache);

        if use_rest_fallback {
            return Ok(());
        }

        if side_filter.is_some() && cancels.is_empty() {
            return Ok(());
        }

        let ws_exec = self.ws_exec.clone();
        let subaccount_id = self.credential.subaccount_id();

        self.spawn_task("cancel_all_orders", async move {
            for (venue_order_id, is_trigger) in cancels {
                let outcome = if is_trigger {
                    ws_exec
                        .cancel_trigger_order(&DeriveCancelTriggerOrderParams::new(
                            subaccount_id,
                            venue_order_id.as_str(),
                        ))
                        .await
                        .map(|_| ())
                } else {
                    ws_exec
                        .cancel_order(&DeriveCancelParams::new(
                            subaccount_id,
                            venue_symbol.as_str(),
                            venue_order_id.as_str(),
                        ))
                        .await
                };

                if let Err(e) = outcome {
                    log::warn!(
                        "Derive cancel_all_orders: cancel for {venue_order_id} failed: {e}",
                    );
                }
            }

            if side_filter.is_none() {
                match ws_exec
                    .cancel_by_instrument(&DeriveCancelByInstrumentParams::new(
                        subaccount_id,
                        venue_symbol.as_str(),
                    ))
                    .await
                {
                    Ok(result) if result.cancelled_orders == 0 => {
                        log::debug!("No open orders to cancel for {venue_symbol}");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log::warn!("Derive cancel_all_orders failed for {venue_symbol}: {e}");
                    }
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        for inner in cmd.cancels {
            self.cancel_order(inner)?;
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        self.note_reconciliation_application();
        let use_rest_fallback = self.should_use_rest_fallback();
        let ts_now = self.clock.get_time_ns();

        let Some(venue_order_id) = cmd.venue_order_id else {
            let reason = "venue_order_id is required for modify";
            log::warn!("Cannot modify order {}: {reason}", cmd.client_order_id);
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                reason,
                ts_now,
            );
            return Ok(());
        };

        let Ok(order) = self.core.cache().try_order_owned(&cmd.client_order_id) else {
            let reason = ORDER_NOT_FOUND;
            log::warn!("Cannot modify order {}: {reason}", cmd.client_order_id);
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                Some(venue_order_id),
                reason,
                ts_now,
            );
            return Ok(());
        };

        if is_derive_trigger_order_type(order.order_type()) {
            let reason = "Derive trigger orders cannot be modified; cancel and resubmit";
            log::warn!("Cannot modify order {}: {reason}", cmd.client_order_id);
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                Some(venue_order_id),
                reason,
                ts_now,
            );
            return Ok(());
        }

        let target_quantity = cmd.quantity.unwrap_or_else(|| order.quantity());
        let target_price = cmd.price.or_else(|| order.price());
        let write_kind = replace_write_kind(&order, target_quantity, target_price);
        let cached_quote_ts_event = self.cached_quote_ts_event(&cmd.instrument_id);

        let gate_reason = if use_rest_fallback && order.venue_order_id() != Some(venue_order_id) {
            Some("Derive REST replace requires the current resolved venue_order_id")
        } else if !write_kind.is_risk_increasing() {
            None
        } else if use_rest_fallback {
            self.risk_increasing_write_blocked(cached_quote_ts_event)
        } else {
            self.unresolved_write_halt()
        };
        if let Some(reason) = gate_reason {
            log_rest_write_halted(
                &self.rest_execution_state,
                cmd.client_order_id,
                write_kind,
                reason,
                cached_quote_ts_event,
                self.clock.get_time_ns().as_u64(),
            );
            self.emitter.emit_order_modify_rejected_event(
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                Some(venue_order_id),
                reason,
                ts_now,
            );
            return Ok(());
        }

        let venue_symbol = format_venue_symbol(&cmd.instrument_id)?.to_string();
        let http_client = self.http_client.clone();
        let rest_http_client = http_client.clone();
        let ws_exec = self.ws_exec.clone();
        let signing = self.signing.clone();
        let nonce_manager = self.nonce_manager.clone();
        let wallet_str = self.credential.wallet_address().to_string();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let instruments = self.instruments.clone();
        let dispatch_state = self.dispatch_state.clone();
        let order_for_task = order;
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;
        let stale_venue_order_id = venue_order_id;
        let account_id = self.core.account_id;
        let voi_str = venue_order_id.to_string();
        let rest_execution_state = Arc::clone(&self.rest_execution_state);
        let max_account_state_age_secs = self.config.max_account_state_age_secs;
        let quote_max_age_secs = self.config.max_quote_age_secs;
        let rest_fallback_enabled = self.config.rest_fallback_enabled;

        self.spawn_task("modify_order", async move {
            let instrument = match cached_or_fetch_instrument(
                &http_client,
                &instruments,
                &instrument_id,
                &venue_symbol,
            )
            .await
            {
                Ok(i) => i,
                Err(e) => {
                    let reason = format!("instrument resolution failed: {e}");
                    log::warn!("Cannot modify order {client_order_id}: {reason}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(stale_venue_order_id),
                        &reason,
                        ts,
                    );
                    return Ok(());
                }
            };

            let matching_reservation = if use_rest_fallback {
                None
            } else {
                match ws_exec
                    .reserve_matching_request("private/replace", &instrument.instrument_name)
                    .await
                {
                    Ok(reservation) => Some(reservation),
                    Err(e) => {
                        let (reason, _) = ws_rejection_reason(&e);
                        log::warn!("Cannot reserve Derive replace quota for {client_order_id}: {reason}");
                        let ts = clock.get_time_ns();
                        emitter.emit_order_modify_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            Some(stale_venue_order_id),
                            &reason,
                            ts,
                        );
                        return Ok(());
                    }
                }
            };

            let expiry = match normal_order_signature_expiry(clock, signing.signature_expiry_secs) {
                Ok(expiry) => expiry,
                Err(e) => {
                    let reason = format!("replace expiry validation failed: {e}");
                    log::warn!("Cannot modify order {client_order_id}: {reason}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(stale_venue_order_id),
                        &reason,
                        ts,
                    );
                    return Ok(());
                }
            };
            let nonce = match resolve_modify_nonce(
                nonce_manager.next_nonce(&wallet_str, signing.subaccount_id),
                &emitter,
                strategy_id,
                instrument_id,
                client_order_id,
                stale_venue_order_id,
                clock,
            ) {
                Some(nonce) => nonce,
                None => return Ok(()),
            };

            let payload = match order_replace_to_derive_payload(
                &order_for_task,
                &instrument,
                signing.subaccount_id,
                signing.wallet_address,
                &signing.signer,
                nonce,
                expiry,
                signing.trade_module_address,
                signing.domain_separator,
                signing.action_typehash,
                signing.max_fee_per_contract,
                Some(target_quantity.as_decimal()),
                target_price.map(|p| p.as_decimal()),
                &voi_str,
            ) {
                Ok(p) => p,
                Err(e) => {
                    let reason = format!("replace encoding failed: {e}");
                    log::warn!("Cannot modify order {client_order_id}: {reason}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(stale_venue_order_id),
                        &reason,
                        ts,
                    );
                    return Ok(());
                }
            };

            let mut replace_token: Option<OperationToken> = None;
            if use_rest_fallback {
                let now_ns = clock.get_time_ns().as_u64();
                match reserve_write(
                    &rest_execution_state,
                    client_order_id,
                    Some(stale_venue_order_id),
                    write_kind,
                    cached_quote_ts_event,
                    now_ns,
                    FreshnessBounds {
                        private_state_secs: max_account_state_age_secs,
                        quote_secs: quote_max_age_secs,
                    },
                ) {
                    Ok(token) => replace_token = Some(token),
                    Err(reason) => {
                        log_rest_write_halted(
                            &rest_execution_state,
                            client_order_id,
                            write_kind,
                            reason,
                            cached_quote_ts_event,
                            now_ns,
                        );
                        let ts = clock.get_time_ns();
                        emitter.emit_order_modify_rejected_event(
                            strategy_id,
                            instrument_id,
                            client_order_id,
                            Some(stale_venue_order_id),
                            reason,
                            ts,
                        );
                        return Ok(());
                    }
                }
            }

            // Mark before sending so the cancel-of-old leg is suppressed even if
            // it arrives before this response.
            dispatch_state.mark_pending_modify(client_order_id, stale_venue_order_id);

            let (outcome, ambiguous_outcome) = if use_rest_fallback {
                match rest_http_client.replace_order(&payload).await {
                    Ok(outcome) => (Ok(outcome), false),
                    Err(e) => {
                        let ambiguous = e.is_ambiguous_write_outcome();
                        (Err(e.to_string()), ambiguous)
                    }
                }
            } else {
                let Some(reservation) = matching_reservation else {
                    let reason = "Derive WebSocket replace lost its matching-engine reservation";
                    log::warn!("{reason} for {client_order_id}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(stale_venue_order_id),
                        reason,
                        ts,
                    );
                    return Ok(());
                };
                match ws_exec
                    .modify_order_after_rate_limit(&payload, reservation)
                    .await
                {
                    Ok(outcome) => (Ok(outcome), false),
                    Err(e) => {
                        let ambiguous = is_write_outcome_ambiguous_ws(&e);
                        let (reason, _) = ws_rejection_reason(&e);
                        (Err(reason), ambiguous)
                    }
                }
            };

            if outcome.is_err() && ambiguous_outcome {
                dispatch_state.clear_pending_modify(&client_order_id);
                log::warn!(
                    "Derive modify for {client_order_id} returned ambiguous outcome; awaiting reconciliation",
                );
                if use_rest_fallback {
                    mark_write_ambiguous(
                        &rest_execution_state,
                        &client_order_id,
                        replace_token,
                        clock.get_time_ns().as_u64(),
                    );
                } else if rest_fallback_enabled {
                    record_unresolved_write(
                        &rest_execution_state,
                        client_order_id,
                        Some(stale_venue_order_id),
                        write_kind,
                        clock.get_time_ns().as_u64(),
                    );
                }
                return Ok(());
            }

            match outcome {
                Ok(DeriveReplaceOutcome::Replaced(order)) => {
                    let new_voi = VenueOrderId::new(order.order_id.as_str());

                    if !dispatch_state.take_pending_modify(
                        &client_order_id,
                        stale_venue_order_id,
                        Some(new_voi),
                    ) {
                        log::debug!(
                            "Skipping private/replace response event for {client_order_id}: an incoming terminal frame already resolved the modify",
                        );
                        if use_rest_fallback {
                            resolve_write(&rest_execution_state, &client_order_id, replace_token);
                        }
                        return Ok(());
                    }
                    log::debug!(
                        "Order replaced: client_order_id={client_order_id}, new venue_order_id={new_voi}",
                    );
                    let ts = clock.get_time_ns();
                    resolve_write(&rest_execution_state, &client_order_id, replace_token);
                    emitter.emit_order_updated(
                        &order_for_task,
                        new_voi,
                        target_quantity,
                        target_price,
                        None,
                        None,
                        ts,
                    );
                }
                Ok(DeriveReplaceOutcome::Canceled {
                    cancelled_order,
                    create_order_error,
                }) => {
                    if !dispatch_state.take_pending_modify(
                        &client_order_id,
                        stale_venue_order_id,
                        None,
                    ) {
                        log::debug!(
                            "Skipping partial private/replace response for {client_order_id}: an incoming terminal frame already resolved the modify",
                        );
                        if use_rest_fallback {
                            resolve_write(&rest_execution_state, &client_order_id, replace_token);
                        }
                        return Ok(());
                    }

                    log::warn!(
                        "Derive cancelled {client_order_id} ({}) but did not create its replacement: JSON-RPC {}: {}",
                        cancelled_order.order_id,
                        create_order_error.code,
                        create_order_error.message,
                    );
                    let ts = clock.get_time_ns();

                    ensure_canceled_emitted(
                        &emitter,
                        &dispatch_state,
                        client_order_id,
                        OrderIdentity {
                            instrument_id,
                            strategy_id,
                            order_side: order_for_task.order_side(),
                            order_type: order_for_task.order_type(),
                        },
                        stale_venue_order_id,
                        account_id,
                        ts,
                        ts,
                    );
                    resolve_write(&rest_execution_state, &client_order_id, replace_token);
                    dispatch_state.forget(&client_order_id);
                }
                Err(e) => {
                    if !dispatch_state.take_pending_modify(
                        &client_order_id,
                        stale_venue_order_id,
                        None,
                    ) {
                        log::debug!(
                            "Skipping private/replace rejection for {client_order_id}: an incoming terminal frame already resolved the modify",
                        );
                        return Ok(());
                    }
                    let reason = e;
                    if use_rest_fallback {
                        resolve_write(&rest_execution_state, &client_order_id, replace_token);
                    }
                    log::debug!("Derive rejected modify for {client_order_id}: {reason}");
                    let ts = clock.get_time_ns();
                    emitter.emit_order_modify_rejected_event(
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(stale_venue_order_id),
                        &reason,
                        ts,
                    );
                }
            }
            Ok(())
        });
        Ok(())
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let subaccount_id = self.credential.subaccount_id();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        self.spawn_task("query_account", async move {
            let subaccount = http_client
                .get_subaccount(&DeriveGetSubaccountParams::new(subaccount_id))
                .await?;
            let (balances, margins, info) = parse_derive_subaccount_to_balances(&subaccount)?;
            let ts_event = clock.get_time_ns();
            emitter.emit_account_state(balances, margins, true, ts_event, Some(info));
            Ok(())
        });
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let context = self.reconciliation_context();

        let report_cmd = GenerateOrderStatusReport::new(
            cmd.command_id,
            cmd.ts_init,
            Some(cmd.instrument_id),
            Some(cmd.client_order_id),
            cmd.venue_order_id,
            cmd.params,
            cmd.correlation_id,
        );

        self.spawn_task("query_order", async move {
            let report = context
                .generate_order_status_report(&report_cmd)
                .await
                .with_context(|| {
                    format!(
                        "failed to query Derive order: client_order_id={}, venue_order_id={:?}",
                        cmd.client_order_id, cmd.venue_order_id,
                    )
                })?;

            if let Some(report) = report {
                context.emitter.send_order_status_report(report);
            } else {
                log::debug!(
                    "Derive order not found: client_order_id={}, venue_order_id={:?}",
                    cmd.client_order_id,
                    cmd.venue_order_id,
                );
            }

            Ok(())
        });
        Ok(())
    }
}

/// What a write was trying to do. This decides both the evidence that can
/// resolve an ambiguous outcome and whether the write is risk-increasing (and
/// therefore subject to the full entry gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestWriteKind {
    /// A new order that may increase exposure.
    RiskIncreasingSubmit,
    /// A new reduce-only order. The venue refuses it if it would increase
    /// exposure, so unrelated account-wide uncertainty must not block it.
    ReducingSubmit,
    /// Cancellation of a known venue order.
    Cancel,
    /// A replace that raises quantity or moves the limit price toward the
    /// market.
    RiskIncreasingReplace,
    /// A replace that cannot increase exposure.
    ReducingReplace,
}

impl RestWriteKind {
    /// Risk-increasing writes require fresh, applied private state, a fresh
    /// per-instrument quote, and no unresolved write anywhere on the account.
    /// Exposure-reducing writes require only that the order they act on has no
    /// unresolved write of its own.
    const fn is_risk_increasing(self) -> bool {
        matches!(
            self,
            Self::RiskIncreasingSubmit | Self::RiskIncreasingReplace
        )
    }

    /// Name used in halt/transition logs.
    const fn as_str(self) -> &'static str {
        match self {
            Self::RiskIncreasingSubmit => "risk_increasing_submit",
            Self::ReducingSubmit => "reducing_submit",
            Self::Cancel => "cancel",
            Self::RiskIncreasingReplace => "risk_increasing_replace",
            Self::ReducingReplace => "reducing_replace",
        }
    }
}

/// Ownership token for an [`UnresolvedWrite`].
///
/// Only the write that obtained a token may retire its record, so a late
/// response can never release uncertainty about a different operation. Tokens
/// start at 1, so no default-initialised value is ever a valid token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OperationToken(u64);

/// How far a tracked write has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteStage {
    /// The request has been sent and no response has been observed yet. A
    /// request still in flight can land at any moment, so reconciliation
    /// evidence must never retire it: only its own response may.
    InFlight,
    /// The request completed without a definitive verdict. The venue may or may
    /// not have applied it, and the write is never retried.
    Ambiguous,
}

/// A write whose venue outcome is not yet known.
///
/// The write is never retried and is resolved **only** by positive evidence
/// about this exact operation: for a cancel or replace, a terminal report for
/// the venue order it targeted; for a submit, a report carrying its client
/// label. There is deliberately no time- or absence-based resolution: no client
/// polling age can prove that a signed request did not land, and treating an
/// explicitly live target as proof that its cancellation never happened would
/// release uncertainty that is still real. An operation that never gains
/// evidence keeps risk-increasing entries blocked until the process restarts,
/// and says so at every reconciliation.
#[derive(Debug, Clone, Copy)]
struct UnresolvedWrite {
    /// Token identifying the write that recorded this entry.
    token: OperationToken,
    kind: RestWriteKind,
    stage: WriteStage,
    /// The venue order this write acted on. `None` for a submit, whose venue
    /// order id is only learned from a definitive response.
    venue_order_id: Option<VenueOrderId>,
    /// Reconciliations started when the write was recorded. A reconciliation
    /// whose snapshots predate the write carries no evidence about it.
    recorded_at_reconciliation: u64,
    /// Clock time the write was recorded, for diagnostics.
    recorded_ns: u64,
}

#[derive(Debug, Default)]
struct RestExecutionState {
    active: AtomicBool,
    websocket_ready: AtomicBool,
    reconciliation_failed: AtomicBool,
    /// Clock time the oldest snapshot of the last complete reconciliation was
    /// *observed* (the account request start), never the time its long history
    /// traversal finished.
    last_private_reconciliation_ns: AtomicU64,
    /// Generation of the last complete reconciliation whose reports the engine
    /// has been observed to apply. Reports are applied asynchronously, so
    /// entries admitted before that would be sized against pre-reconciliation
    /// state. Compared against [`Self::applied_generation`]'s expectation.
    applied_generation: AtomicU64,
    /// Reconciliations started so far. An ambiguous write records the current
    /// value so evidence gathered earlier cannot resolve it.
    reconciliations_started: AtomicU64,
    /// Monotonic source for [`OperationToken`]; the first token issued is 1.
    next_operation_token: AtomicU64,
    /// Set once the WebSocket session's own recovery has been exhausted, so the
    /// supervisor task (rather than the socket's internal retry) owns the next
    /// dial.
    ws_recovery_exhausted: AtomicBool,
    unresolved_orders: Mutex<AHashMap<ClientOrderId, UnresolvedWrite>>,
    /// What the last complete reconciliation reported, used to decide whether
    /// the engine has applied that generation yet.
    reported_state: Mutex<ReconciliationExpectation>,
}

/// Order state one reconciliation reported for a client order id.
#[derive(Debug, Clone, Copy)]
struct ReportedOrderState {
    venue_order_id: VenueOrderId,
    closed: bool,
    quantity: Decimal,
    filled_qty: Decimal,
    ts_last: UnixNanos,
}

/// The venue state a reconciliation generation reported, against which the
/// cache is compared to decide whether the engine has applied it.
#[derive(Debug, Default)]
struct ReconciliationExpectation {
    /// Generation this expectation belongs to; `0` means nothing recorded yet.
    generation: u64,
    /// When this generation's oldest snapshot was observed. Local state whose
    /// last event is newer than this cannot be expected to appear in it.
    observed_at: UnixNanos,
    orders: AHashMap<ClientOrderId, ReportedOrderState>,
    /// Reported open position per instrument as `(side, quantity)`. An
    /// instrument reported flat is absent from this map.
    positions: AHashMap<InstrumentId, (PositionSide, Decimal)>,
}

/// Private-state health required by every risk-increasing write and by either
/// route back to WebSocket order entry: the last reconciliation was complete,
/// its oldest snapshot was observed within the configured maximum age, and the
/// engine has applied that same generation.
fn private_state_is_fresh(
    state: &RestExecutionState,
    now_ns: u64,
    max_age_secs: Option<u64>,
) -> bool {
    let Some(max_age_secs) = max_age_secs else {
        return false;
    };
    let observed_ns = state.last_private_reconciliation_ns.load(Ordering::Acquire);
    observed_ns > 0
        && now_ns.saturating_sub(observed_ns) <= max_age_secs.saturating_mul(1_000_000_000)
        && !state.reconciliation_failed.load(Ordering::Acquire)
        && reported_generation_is_applied(state)
}

/// Whether the engine has applied the generation the last complete
/// reconciliation recorded.
fn reported_generation_is_applied(state: &RestExecutionState) -> bool {
    let generation = state.reported_state.lock().generation;
    generation > 0 && state.applied_generation.load(Ordering::Acquire) == generation
}

/// Whether any write's outcome is still unknown.
///
/// This is independent of transport routing: a possibly-live order is a
/// possibly-live order whether it was sent over REST or the WebSocket, so it
/// must block risk-increasing commands on both paths.
fn has_unresolved_writes(state: &RestExecutionState) -> bool {
    !state.unresolved_orders.lock().is_empty()
}

/// The staleness bounds an admission decision is measured against.
///
/// Private state and quotes carry separate bounds on purpose: a generous
/// account-state age must not silently widen the age of the quote an entry is
/// priced against.
#[derive(Debug, Clone, Copy, Default)]
struct FreshnessBounds {
    private_state_secs: Option<u64>,
    quote_secs: Option<u64>,
}

/// Quote freshness required before a risk-increasing write, measured against
/// the dedicated execution quote bound rather than the account-state bound, so
/// a generous account-state age cannot silently widen the quote age an entry is
/// priced against. A missing or future-dated quote denies the write.
fn quote_is_fresh(ts_event: Option<UnixNanos>, now_ns: u64, max_age_secs: Option<u64>) -> bool {
    let (Some(ts_event), Some(max_age_secs)) = (ts_event, max_age_secs) else {
        return false;
    };
    let ts_event = ts_event.as_u64();
    ts_event <= now_ns
        && now_ns.saturating_sub(ts_event) <= max_age_secs.saturating_mul(1_000_000_000)
}

/// Admission gate applied immediately before a write is sent, on either
/// transport.
///
/// On success the write is tracked as in-flight, so a lost or ambiguous
/// response can never be mistaken for a rejection and no concurrent
/// risk-increasing write slips past while this one's fate is open. The returned
/// token is the only thing that may later retire the record.
fn reserve_write(
    state: &RestExecutionState,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    kind: RestWriteKind,
    quote_ts_event: Option<UnixNanos>,
    now_ns: u64,
    bounds: FreshnessBounds,
) -> Result<OperationToken, &'static str> {
    let FreshnessBounds {
        private_state_secs: private_max_age_secs,
        quote_secs: quote_max_age_secs,
    } = bounds;
    let mut unresolved = state.unresolved_orders.lock();

    if kind.is_risk_increasing() {
        if !unresolved.is_empty() {
            return Err(REST_GATE_UNRESOLVED_WRITE);
        }
        if !private_state_is_fresh(state, now_ns, private_max_age_secs) {
            return Err(REST_GATE_PRIVATE_STATE);
        }
        if !quote_is_fresh(quote_ts_event, now_ns, quote_max_age_secs) {
            return Err(REST_GATE_QUOTE);
        }
    } else if unresolved.contains_key(&client_order_id) {
        return Err(REST_GATE_UNRESOLVED_ORDER);
    }

    let token = OperationToken(state.next_operation_token.fetch_add(1, Ordering::AcqRel) + 1);
    unresolved.insert(
        client_order_id,
        UnresolvedWrite {
            token,
            kind,
            stage: WriteStage::InFlight,
            venue_order_id,
            recorded_at_reconciliation: state.reconciliations_started.load(Ordering::Acquire),
            recorded_ns: now_ns,
        },
    );
    Ok(token)
}

/// Moves a tracked write from in-flight to ambiguous once its request has
/// completed without a definitive verdict. Only from this point can
/// reconciliation evidence resolve it.
fn mark_write_ambiguous(
    state: &RestExecutionState,
    client_order_id: &ClientOrderId,
    token: Option<OperationToken>,
    now_ns: u64,
) {
    let Some(token) = token else {
        return;
    };
    let mut unresolved = state.unresolved_orders.lock();
    if let Some(write) = unresolved.get_mut(client_order_id)
        && write.token == token
    {
        write.stage = WriteStage::Ambiguous;
        write.recorded_ns = now_ns;
        log::warn!(
            "Derive {} for {client_order_id} has an unknown venue outcome; risk-increasing writes stay blocked until a venue report resolves it",
            write.kind.as_str(),
        );
    }
}

/// Records a write whose outcome became unknown without having been reserved —
/// a WebSocket write that turned ambiguous after its routing decision was
/// taken. The uncertainty blocks risk-increasing commands on either transport,
/// including a socket that is still authenticated (a JSON-RPC `-32603` does not
/// require the transport to drop).
fn record_unresolved_write(
    state: &RestExecutionState,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    kind: RestWriteKind,
    now_ns: u64,
) {
    let token = OperationToken(state.next_operation_token.fetch_add(1, Ordering::AcqRel) + 1);
    let recorded_at_reconciliation = state.reconciliations_started.load(Ordering::Acquire);
    state.unresolved_orders.lock().insert(
        client_order_id,
        UnresolvedWrite {
            token,
            kind,
            stage: WriteStage::Ambiguous,
            venue_order_id,
            recorded_at_reconciliation,
            recorded_ns: now_ns,
        },
    );
    log::warn!(
        "Derive {} for {client_order_id} returned an ambiguous outcome; risk-increasing writes stay blocked until a venue report resolves it",
        kind.as_str(),
    );
}

/// Retires a tracked write after a definitive outcome. Only the token actually
/// obtained by that operation retires its record, and a write that never
/// reserved one (so holds `None`) retires nothing.
fn resolve_write(
    state: &RestExecutionState,
    client_order_id: &ClientOrderId,
    token: Option<OperationToken>,
) {
    let Some(token) = token else {
        return;
    };
    let mut unresolved = state.unresolved_orders.lock();
    if unresolved
        .get(client_order_id)
        .is_some_and(|write| write.token == token)
    {
        unresolved.remove(client_order_id);
    }
}

/// Flips REST fallback activity, logging the transition once with the reason
/// and the age of the private state the decision was taken against.
///
/// Must not be called while `unresolved_orders` is locked.
fn set_rest_fallback_active(state: &RestExecutionState, active: bool, reason: &str, now_ns: u64) {
    if state.active.swap(active, Ordering::AcqRel) == active {
        return;
    }
    let observed_ns = state.last_private_reconciliation_ns.load(Ordering::Acquire);
    let private_state_age_ns = (observed_ns > 0).then(|| now_ns.saturating_sub(observed_ns));
    log::info!(
        "Derive execution mode changed to {} (reason={reason}, private_state_age_ns={private_state_age_ns:?}, unresolved_writes={})",
        if active { "rest_polling" } else { "websocket" },
        state.unresolved_orders.lock().len(),
    );
}

/// Logs a refused risk-increasing write as a `halted` transition so a wedged
/// gate is visible in production rather than silent.
fn log_rest_write_halted(
    state: &RestExecutionState,
    client_order_id: ClientOrderId,
    kind: RestWriteKind,
    reason: &str,
    quote_ts_event: Option<UnixNanos>,
    now_ns: u64,
) {
    let observed_ns = state.last_private_reconciliation_ns.load(Ordering::Acquire);
    let private_state_age_ns = (observed_ns > 0).then(|| now_ns.saturating_sub(observed_ns));
    let quote_age_ns = quote_ts_event.map(|ts| now_ns.saturating_sub(ts.as_u64()));
    log::warn!(
        "Derive execution halted for {client_order_id} (state=halted, operation={}, reason={reason}, private_state_age_ns={private_state_age_ns:?}, quote_age_ns={quote_age_ns:?}, reconciliation_failed={}, applied_generation={}, reported_generation={}, unresolved_writes={})",
        kind.as_str(),
        state.reconciliation_failed.load(Ordering::Acquire),
        state.applied_generation.load(Ordering::Acquire),
        state.reported_state.lock().generation,
        state.unresolved_orders.lock().len(),
    );
}

#[derive(Clone)]
struct DeriveReconciliationContext {
    http_client: DeriveHttpClient,
    emitter: ExecutionEventEmitter,
    client_id: ClientId,
    account_id: AccountId,
    subaccount_id: u64,
    clock: &'static AtomicTime,
    dispatch_state: Arc<WsDispatchState>,
    rest_execution_state: Arc<RestExecutionState>,
    is_connected: Arc<AtomicBool>,
    max_account_state_age_secs: Option<u64>,
}

impl DeriveReconciliationContext {
    /// Fetches and publishes the subaccount snapshot, returning the time the
    /// snapshot was *observed*.
    ///
    /// The observation time is the request start, which is the conservative
    /// bound: the venue's view is at least that old by the time the response
    /// lands. Callers use it as the age of the account state, so a long history
    /// traversal afterwards can never make an already-expired snapshot look
    /// fresh.
    async fn refresh_account_state(&self) -> anyhow::Result<UnixNanos> {
        let observed_at = self.clock.get_time_ns();
        let value = self
            .http_client
            .get_subaccount(&DeriveGetSubaccountParams::new(self.subaccount_id))
            .await
            .context("failed to fetch Derive subaccount snapshot")?;
        let (balances, margins, info) = parse_derive_subaccount_to_balances(&value)
            .context("failed to parse Derive subaccount balances")?;
        let ts_event = self.clock.get_time_ns();
        self.emitter
            .emit_account_state(balances, margins, true, ts_event, Some(info));
        Ok(observed_at)
    }

    async fn recover_after_reconnect(&self) -> anyhow::Result<()> {
        if let Err(e) = self.reconcile_private_state().await {
            self.rest_execution_state
                .reconciliation_failed
                .store(true, Ordering::Release);
            return Err(e);
        }
        // Both routes back to WebSocket order entry require the same thing: a
        // complete reconciliation, observed inside the configured age, that the
        // engine has applied. An unresolved write deliberately does *not* block
        // this — routing and admission are separate concerns, and the
        // unresolved-write gate keeps risk-increasing commands halted on the
        // WebSocket path too (see `reserve_write`). Blocking recovery on it
        // instead would strand the client in REST mode for the whole process.
        if !self.rest_mode_may_exit() {
            anyhow::bail!("Derive REST recovery reconciliation is not complete, fresh and applied");
        }
        self.is_connected.store(true, Ordering::Release);
        set_rest_fallback_active(
            &self.rest_execution_state,
            false,
            "post_reconnect_reconciliation_complete_fresh_applied",
            self.clock.get_time_ns().as_u64(),
        );
        Ok(())
    }

    /// Whether the last reconciliation is a good enough picture of venue state
    /// to resume WebSocket order routing.
    fn rest_mode_may_exit(&self) -> bool {
        private_state_is_fresh(
            &self.rest_execution_state,
            self.clock.get_time_ns().as_u64(),
            self.max_account_state_age_secs,
        )
    }

    /// Repeats, once per reconciliation, that a write's outcome is still
    /// unknown. A silently wedged entry gate is the failure mode that makes
    /// this state dangerous in production, so it is logged every cycle rather
    /// than once.
    fn log_unresolved_writes(&self) {
        let unresolved = self.rest_execution_state.unresolved_orders.lock();
        if unresolved.is_empty() {
            return;
        }
        let now_ns = self.clock.get_time_ns().as_u64();
        for (client_order_id, write) in unresolved.iter() {
            log::warn!(
                "Derive execution halted (state=halted): {} for {client_order_id} still has an unknown venue outcome after {} ns (stage={:?}, venue_order_id={:?}); it is never retried and only a venue report for this operation can resolve it. Risk-increasing orders stay blocked; operator action or a process restart is required if no report ever arrives.",
                write.kind.as_str(),
                now_ns.saturating_sub(write.recorded_ns),
                write.stage,
                write.venue_order_id,
            );
        }
    }

    async fn reconcile_private_state(&self) -> anyhow::Result<()> {
        let reconciliation_index = self
            .rest_execution_state
            .reconciliations_started
            .fetch_add(1, Ordering::AcqRel);
        let account_observed_at = self.refresh_account_state().await?;
        let (mass_status, completeness) =
            Box::pin(self.generate_mass_status_with_completeness(None)).await?;
        let order_count = mass_status.order_reports().len();
        let fill_count: usize = mass_status.fill_reports().values().map(Vec::len).sum();
        let position_count = mass_status.position_reports().len();

        if !completeness.is_complete() {
            // The reports still go to the engine (they are the best available
            // view), but an incomplete picture may not open the entry gate: a
            // dropped fill or position would otherwise release an ambiguous
            // order or hide exposure.
            self.emitter
                .send_execution_report(ExecutionReport::MassStatus(Box::new(mass_status)));
            self.rest_execution_state
                .reconciliation_failed
                .store(true, Ordering::Release);
            // An incomplete generation is not recorded as an expectation, so
            // the applied check keeps failing against the last complete one.
            self.rest_execution_state
                .applied_generation
                .store(0, Ordering::Release);
            log::warn!(
                "Derive private reconciliation incomplete (state=halted, skipped_orders={}, skipped_fills={}, skipped_positions={}); risk-increasing writes stay blocked",
                completeness.skipped_orders,
                completeness.skipped_fills,
                completeness.skipped_positions,
            );
            self.log_unresolved_writes();
            return Ok(());
        }

        self.resolve_unresolved_writes(&mass_status, reconciliation_index);
        self.record_reported_state(&mass_status, reconciliation_index + 1, account_observed_at);
        self.emitter
            .send_execution_report(ExecutionReport::MassStatus(Box::new(mass_status)));
        self.rest_execution_state
            .last_private_reconciliation_ns
            .store(account_observed_at.as_u64(), Ordering::Release);
        self.rest_execution_state
            .reconciliation_failed
            .store(false, Ordering::Release);
        log::info!(
            "Derive private reconciliation submitted (state=reconciling, orders={order_count}, fills={fill_count}, positions={position_count}, account_snapshot_age_ns={})",
            self.clock
                .get_time_ns()
                .as_u64()
                .saturating_sub(account_observed_at.as_u64()),
        );
        self.log_unresolved_writes();
        Ok(())
    }
    /// Records what this reconciliation generation reported, so the entry gate
    /// can tell whether the engine has applied it.
    ///
    /// Only reports carrying a client order id are usable: an order the engine
    /// tracks is matched by label, and the per-label expectation is the current
    /// leg — an open report beats a closed one, and among equally closed
    /// reports the most recently updated wins — so a closed historical
    /// replacement leg never stands in for the live replacement.
    fn record_reported_state(
        &self,
        mass_status: &ExecutionMassStatus,
        generation: u64,
        observed_at: UnixNanos,
    ) {
        let mut reported = self.rest_execution_state.reported_state.lock();
        reported.generation = generation;
        reported.observed_at = observed_at;
        reported.orders.clear();
        reported.positions.clear();

        for (_, report) in mass_status.order_reports() {
            let Some(client_order_id) = report.client_order_id else {
                continue;
            };
            let candidate = ReportedOrderState {
                venue_order_id: report.venue_order_id,
                closed: report.order_status.is_closed(),
                quantity: report.quantity.as_decimal(),
                filled_qty: report.filled_qty.as_decimal(),
                ts_last: report.ts_last,
            };
            let keep_existing = reported
                .orders
                .get(&client_order_id)
                .is_some_and(|existing| {
                    (!existing.closed && candidate.closed)
                        || (existing.closed == candidate.closed
                            && existing.ts_last >= candidate.ts_last)
                });
            if !keep_existing {
                reported.orders.insert(client_order_id, candidate);
            }
        }

        for report in mass_status.position_reports().values().flatten() {
            if report.position_side == PositionSide::Flat || report.quantity.is_zero() {
                continue;
            }
            reported.positions.insert(
                report.instrument_id,
                (report.position_side, report.quantity.as_decimal()),
            );
        }
    }

    /// Applies one complete reconciliation's evidence to the tracked writes.
    ///
    /// Resolution requires positive evidence about the exact operation and
    /// nothing weaker. A cancel or replace is resolved only by a terminal
    /// report, or a fill, for the venue order it targeted; a submit is resolved
    /// by any order *or fill* report carrying its client label, because a
    /// landed submit whose order report was dropped or had its label detached
    /// still shows up as a fill. A report that merely shows the target alive is
    /// not evidence that a cancellation never happened, and elapsed time is not
    /// evidence of anything, so neither resolves a write. An in-flight request
    /// is never resolved from reconciliation: it can still land at any moment,
    /// and only its own response may retire it.
    fn resolve_unresolved_writes(
        &self,
        mass_status: &ExecutionMassStatus,
        reconciliation_index: u64,
    ) {
        let mut unresolved = self.rest_execution_state.unresolved_orders.lock();
        if unresolved.is_empty() {
            return;
        }

        let mut closed_by_venue_order_id: AHashMap<VenueOrderId, bool> =
            AHashMap::with_capacity(mass_status.order_reports().len());
        let mut evidenced_client_order_ids: AHashSet<ClientOrderId> = AHashSet::new();
        let mut filled_venue_order_ids: AHashSet<VenueOrderId> = AHashSet::new();

        for (_, report) in mass_status.order_reports() {
            let closed = report.order_status.is_closed();
            closed_by_venue_order_id
                .entry(report.venue_order_id)
                .and_modify(|existing| *existing = *existing || closed)
                .or_insert(closed);
            if let Some(client_order_id) = report.client_order_id {
                evidenced_client_order_ids.insert(client_order_id);
            }
        }

        for fill in mass_status.fill_reports().values().flatten() {
            filled_venue_order_ids.insert(fill.venue_order_id);
            if let Some(client_order_id) = fill.client_order_id {
                evidenced_client_order_ids.insert(client_order_id);
            }
        }

        unresolved.retain(|client_order_id, write| {
            if write.stage == WriteStage::InFlight {
                return true;
            }
            if write.recorded_at_reconciliation >= reconciliation_index {
                // These snapshots were requested before the write; they say
                // nothing about it.
                return true;
            }

            let resolved = match write.venue_order_id {
                Some(venue_order_id) => {
                    closed_by_venue_order_id.get(&venue_order_id) == Some(&true)
                        || filled_venue_order_ids.contains(&venue_order_id)
                }
                None => evidenced_client_order_ids.contains(client_order_id),
            };
            if resolved {
                log::info!(
                    "Derive {} for {client_order_id} resolved by reconciliation evidence",
                    write.kind.as_str(),
                );
                return false;
            }
            true
        });
    }
    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        if cmd.venue_order_id.is_none() && cmd.client_order_id.is_none() {
            log::warn!(
                "Derive generate_order_status_report requires venue_order_id or client_order_id"
            );
            return Ok(None);
        }

        let subaccount_id = self.subaccount_id;

        let order = if let Some(venue_order_id) = cmd.venue_order_id {
            match self
                .http_client
                .get_order(&DeriveGetOrderParams::new(
                    subaccount_id,
                    venue_order_id.as_str(),
                ))
                .await
            {
                Ok(order) => Some(order),
                Err(e) => {
                    let trigger_orders = self
                        .http_client
                        .get_trigger_orders(&DeriveGetTriggerOrdersParams::new(subaccount_id))
                        .await?
                        .orders;

                    match trigger_orders
                        .into_iter()
                        .find(|o| o.order_id.as_str() == venue_order_id.as_str())
                    {
                        Some(order) => Some(order),
                        None => return Err(e.into()),
                    }
                }
            }
        } else {
            // Derive has no by-label lookup endpoint; scan open orders first,
            // then trigger orders, then fall through to paginated history so
            // terminal orders resolve for reconcilers that only carry the
            // client_order_id.
            let label = cmd.client_order_id.expect("guarded above");

            let open_orders = self
                .http_client
                .get_open_orders(&DeriveGetOpenOrdersParams::new(subaccount_id))
                .await?
                .orders;
            let mut found = open_orders.into_iter().find(|o| o.label == label.as_str());

            if found.is_none() {
                let trigger_orders = self
                    .http_client
                    .get_trigger_orders(&DeriveGetTriggerOrdersParams::new(subaccount_id))
                    .await?
                    .orders;
                found = trigger_orders
                    .into_iter()
                    .find(|o| o.label == label.as_str());
            }

            if found.is_none() {
                let instrument_name = cmd.instrument_id.map(|id| id.symbol.as_str().to_string());
                let mut page: u32 = 1;

                'history: loop {
                    let mut params = DeriveGetOrderHistoryParams::new(
                        subaccount_id,
                        page,
                        DERIVE_PRIVATE_PAGE_SIZE,
                    );

                    if let Some(name) = instrument_name.as_deref() {
                        params = params.with_instrument_name(name);
                    }

                    let result = self.http_client.get_order_history(&params).await?;
                    let total_pages = result.pagination.num_pages;

                    for order in result.orders {
                        if order.label == label.as_str() {
                            found = Some(order);
                            break 'history;
                        }
                    }

                    if (page as i64) >= total_pages || total_pages == 0 {
                        break;
                    }

                    page += 1;
                }
            }

            found
        };

        let Some(order) = order else {
            return Ok(None);
        };

        if let Some(instrument_id) = cmd.instrument_id
            && InstrumentId::new(Symbol::new(order.instrument_name), *DERIVE_VENUE) != instrument_id
        {
            log::warn!(
                "Derive order {} is for {} but report requested {}",
                order.order_id,
                order.instrument_name.as_str(),
                instrument_id,
            );
            return Ok(None);
        }

        let (price_precision, size_precision) =
            report_precision(&self.dispatch_state, order.instrument_name.as_str());
        let ts_init = self.clock.get_time_ns();

        let mut report = parse_derive_order_to_report_with_precision(
            &order,
            self.account_id,
            price_precision,
            size_precision,
            ts_init,
        )?;

        // Prefer the parsed label (the venue's source of truth); only stamp
        // the cmd's id when the venue order has no label at all.
        if report.client_order_id.is_none()
            && let Some(client_order_id) = cmd.client_order_id
        {
            report = report.with_client_order_id(client_order_id);
        }

        Ok(Some(report))
    }

    /// Best-effort order status reports, as every other caller expects.
    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
        normalize_history_client_order_ids: bool,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let (reports, _) = self
            .generate_order_status_reports_with_completeness(
                cmd,
                normalize_history_client_order_ids,
            )
            .await?;
        Ok(reports)
    }

    /// As [`Self::generate_order_status_reports`], also returning how many rows
    /// the venue sent that could not be converted.
    async fn generate_order_status_reports_with_completeness(
        &self,
        cmd: &GenerateOrderStatusReports,
        normalize_history_client_order_ids: bool,
    ) -> anyhow::Result<(Vec<OrderStatusReport>, usize)> {
        let instrument_name = cmd.instrument_id.map(|id| id.symbol.as_str().to_string());
        let orders: Vec<DeriveOrder> = if cmd.open_only {
            let mut orders = self
                .http_client
                .get_open_orders(&DeriveGetOpenOrdersParams::new(self.subaccount_id))
                .await?
                .orders;
            orders.extend(
                self.http_client
                    .get_trigger_orders(&DeriveGetTriggerOrdersParams::new(self.subaccount_id))
                    .await?
                    .orders,
            );
            orders
        } else {
            let start_ms = cmd.start.map(|t| t.as_millis() as i64);
            let end_ms = cmd.end.map(|t| t.as_millis() as i64);
            let mut page: u32 = 1;
            let mut collected = Vec::new();

            loop {
                let mut params = DeriveGetOrderHistoryParams::new(
                    self.subaccount_id,
                    page,
                    DERIVE_PRIVATE_PAGE_SIZE,
                )
                .with_window(start_ms, end_ms);

                if let Some(name) = instrument_name.as_deref() {
                    params = params.with_instrument_name(name);
                }

                let result = self.http_client.get_order_history(&params).await?;
                let total_pages = result.pagination.num_pages;
                collected.extend(result.orders);

                if (page as i64) >= total_pages || total_pages == 0 {
                    break;
                }
                page += 1;
            }
            collected
        };

        let ts_init = self.clock.get_time_ns();
        let orders: Vec<DeriveOrder> = orders
            .into_iter()
            .filter(|order| {
                cmd.instrument_id.is_none_or(|instrument_id| {
                    InstrumentId::new(Symbol::new(order.instrument_name), *DERIVE_VENUE)
                        == instrument_id
                })
            })
            .collect();

        let ambiguous_client_order_ids = if normalize_history_client_order_ids {
            ambiguous_history_client_order_ids(&orders)
        } else {
            AHashSet::new()
        };

        let mut reports = Vec::with_capacity(orders.len());
        let mut skipped = 0_usize;

        for order in orders {
            let (price_precision, size_precision) =
                report_precision(&self.dispatch_state, order.instrument_name.as_str());
            match parse_derive_order_to_report_with_precision(
                &order,
                self.account_id,
                price_precision,
                size_precision,
                ts_init,
            ) {
                Ok(mut report) => {
                    if report.client_order_id.is_some_and(|client_order_id| {
                        ambiguous_client_order_ids.contains(&client_order_id)
                    }) {
                        report.client_order_id = None;
                    }
                    reports.push(report);
                }
                Err(e) => {
                    skipped += 1;
                    log::warn!("Skipping order in status report: {e}");
                }
            }
        }

        retain_order_status_reports(&mut reports, cmd);
        Ok((reports, skipped))
    }

    /// Best-effort fill reports, as every other caller expects.
    async fn generate_fill_reports(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        let (reports, _) = self.generate_fill_reports_with_completeness(cmd).await?;
        Ok(reports)
    }

    /// As [`Self::generate_fill_reports`], also returning how many trade rows
    /// could not be converted. Pending settlements and already-applied trades
    /// are deliberate skips and are not counted.
    async fn generate_fill_reports_with_completeness(
        &self,
        cmd: GenerateFillReports,
    ) -> anyhow::Result<(Vec<FillReport>, usize)> {
        let instrument_name = cmd.instrument_id.map(|id| id.symbol.as_str().to_string());
        let mut page: u32 = 1;
        let mut all_trades: Vec<DeriveTrade> = Vec::new();

        loop {
            let mut params = DeriveGetTradeHistoryParams::new(
                self.subaccount_id,
                page,
                DERIVE_PRIVATE_PAGE_SIZE,
            )
            .with_window(
                cmd.start.map(|t| t.as_millis() as i64),
                cmd.end.map(|t| t.as_millis() as i64),
            );

            if let Some(name) = instrument_name.as_deref() {
                params = params.with_instrument_name(name);
            }

            let result = self.http_client.get_private_trade_history(&params).await?;
            let total_pages = result.pagination.num_pages;
            all_trades.extend(result.trades);

            if (page as i64) >= total_pages || total_pages == 0 {
                break;
            }
            page += 1;
        }

        let ts_init = self.clock.get_time_ns();

        let venue_order_id_filter = cmd
            .venue_order_id
            .as_ref()
            .map(|id| id.as_str().to_string());

        let mut reports = Vec::with_capacity(all_trades.len());
        let mut skipped = 0_usize;

        for trade in all_trades {
            if let Some(target) = venue_order_id_filter.as_deref()
                && trade.order_id != target
            {
                continue;
            }

            let (price_precision, size_precision) =
                report_precision(&self.dispatch_state, trade.instrument_name.as_str());
            match parse_derive_trade_to_fill_report_with_precision(
                &trade,
                self.account_id,
                Currency::USDC(),
                price_precision,
                size_precision,
                ts_init,
            ) {
                Ok(Some(report)) => {
                    if self.dispatch_state.contains_trade(&report.trade_id) {
                        log::debug!(
                            "Skipping duplicate Derive fill (trade_id={}) in generate_fill_reports",
                            report.trade_id,
                        );
                        continue;
                    }
                    reports.push(report);
                }
                Ok(None) => {}
                Err(e) => {
                    skipped += 1;
                    log::warn!("Skipping trade in fill report: {e}");
                }
            }
        }
        Ok((reports, skipped))
    }

    async fn generate_position_status_snapshot(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<PositionStatusSnapshot> {
        let positions = self
            .http_client
            .get_positions(&DeriveGetPositionsParams::new(self.subaccount_id))
            .await?
            .positions;
        let ts_init = self.clock.get_time_ns();
        let mut reports = Vec::with_capacity(positions.len());
        let mut instruments = AHashSet::with_capacity(positions.len());
        let mut skipped = 0_usize;

        for position in positions {
            let instrument_id = format_instrument_id(position.instrument_name);
            if let Some(target) = cmd.instrument_id
                && instrument_id != target
            {
                continue;
            }

            instruments.insert(instrument_id);

            let (_, size_precision) =
                report_precision(&self.dispatch_state, position.instrument_name.as_str());
            match parse_derive_position_to_report_with_precision(
                &position,
                self.account_id,
                size_precision,
                ts_init,
            ) {
                Ok(report) => reports.push(report),
                Err(e) => {
                    skipped += 1;
                    log::warn!("Skipping position in status report: {e}");
                }
            }
        }

        Ok(PositionStatusSnapshot {
            reports,
            instruments,
            skipped,
        })
    }

    /// Best-effort mass status, as every other caller expects.
    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<ExecutionMassStatus> {
        let (mass_status, _) =
            Box::pin(self.generate_mass_status_with_completeness(lookback_mins)).await?;
        Ok(mass_status)
    }

    /// As [`Self::generate_mass_status`], also returning whether every row the
    /// venue sent was converted.
    async fn generate_mass_status_with_completeness(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<(ExecutionMassStatus, ReportCompleteness)> {
        log::info!("Generating ExecutionMassStatus (lookback_mins={lookback_mins:?})");

        let ts_now = self.clock.get_time_ns();
        let start = lookback_mins
            .map(DurationNanos::try_from_mins)
            .transpose()?
            .map(|lookback| ts_now.saturating_sub(lookback));

        let open_order_cmd = GenerateOrderStatusReports::new(
            UUID4::new(),
            ts_now,
            true,
            None,
            None,
            None,
            None,
            None,
        );
        let history_order_cmd = GenerateOrderStatusReports::new(
            UUID4::new(),
            ts_now,
            false,
            None,
            start,
            None,
            None,
            None,
        );
        let fill_cmd =
            GenerateFillReports::new(UUID4::new(), ts_now, None, None, start, None, None, None);
        let position_cmd =
            GeneratePositionStatusReports::new(UUID4::new(), ts_now, None, None, None, None, None);

        let (
            (history_order_reports, skipped_history_orders),
            (open_order_reports, skipped_open_orders),
            (mut fill_reports, skipped_fills),
            position_snapshot,
        ) = tokio::try_join!(
            self.generate_order_status_reports_with_completeness(&history_order_cmd, true),
            self.generate_order_status_reports_with_completeness(&open_order_cmd, false),
            self.generate_fill_reports_with_completeness(fill_cmd),
            self.generate_position_status_snapshot(&position_cmd),
        )?;
        let completeness = ReportCompleteness {
            skipped_orders: skipped_history_orders + skipped_open_orders,
            skipped_fills,
            skipped_positions: position_snapshot.skipped,
        };
        let detached_history_order_ids: AHashSet<VenueOrderId> = history_order_reports
            .iter()
            .filter(|report| report.client_order_id.is_none())
            .map(|report| report.venue_order_id)
            .collect();

        for report in &mut fill_reports {
            if detached_history_order_ids.contains(&report.venue_order_id) {
                report.client_order_id = None;
            }
        }

        log::info!(
            "Received {} historical OrderStatusReports",
            history_order_reports.len()
        );
        log::info!(
            "Received {} open OrderStatusReports",
            open_order_reports.len()
        );
        log::info!("Received {} FillReports", fill_reports.len());
        log::info!(
            "Received {} PositionReports",
            position_snapshot.reports.len()
        );

        let mut touched_instruments = AHashSet::new();

        for report in history_order_reports
            .iter()
            .chain(open_order_reports.iter())
        {
            touched_instruments.insert(report.instrument_id);
        }

        for report in &fill_reports {
            touched_instruments.insert(report.instrument_id);
        }

        let PositionStatusSnapshot {
            reports: position_reports,
            instruments: position_instruments,
            skipped: _,
        } = position_snapshot;
        let mut mass_status =
            ExecutionMassStatus::new(self.client_id, self.account_id, *DERIVE_VENUE, ts_now, None);
        mass_status.add_order_reports(history_order_reports);
        mass_status.add_order_reports(open_order_reports);
        mass_status.add_fill_reports(fill_reports);
        mass_status.add_position_reports(position_reports);

        add_missing_flat_position_reports(
            &mut mass_status,
            self.account_id,
            touched_instruments,
            &position_instruments,
            ts_now,
        );

        Ok((mass_status, completeness))
    }
}

fn ambiguous_history_client_order_ids(orders: &[DeriveOrder]) -> AHashSet<ClientOrderId> {
    let mut orders_by_label: AHashMap<Ustr, AHashMap<&str, Option<&str>>> = AHashMap::new();

    for order in orders {
        if order.label.is_empty() {
            continue;
        }
        orders_by_label
            .entry(order.label)
            .or_default()
            .insert(order.order_id.as_str(), order.replaced_order_id.as_deref());
    }

    let mut ambiguous_client_order_ids = AHashSet::new();

    for (label, orders_by_id) in orders_by_label {
        if orders_by_id.len() < 2 {
            continue;
        }

        let predecessors: AHashMap<&str, &str> = orders_by_id
            .iter()
            .filter_map(|(order_id, replaced_order_id)| {
                let replaced_order_id = (*replaced_order_id)?;
                orders_by_id
                    .contains_key(replaced_order_id)
                    .then_some((*order_id, replaced_order_id))
            })
            .collect();
        let predecessor_ids: AHashSet<&str> = predecessors.values().copied().collect();
        let heads: Vec<&str> = orders_by_id
            .keys()
            .copied()
            .filter(|order_id| !predecessor_ids.contains(order_id))
            .collect();

        // One client order may own several venue IDs only when they form one linear replace chain
        let is_linear_chain = predecessors.len() + 1 == orders_by_id.len()
            && predecessor_ids.len() == predecessors.len()
            && heads.len() == 1
            && {
                let mut visited = AHashSet::new();
                let mut current = Some(heads[0]);
                while let Some(order_id) = current {
                    if !visited.insert(order_id) {
                        break;
                    }
                    current = predecessors.get(order_id).copied();
                }
                visited.len() == orders_by_id.len()
            };

        if !is_linear_chain {
            ambiguous_client_order_ids.insert(ClientOrderId::new(label));
        }
    }

    ambiguous_client_order_ids
}

struct PositionStatusSnapshot {
    reports: Vec<PositionStatusReport>,
    instruments: AHashSet<InstrumentId>,
    /// Position rows the venue sent that could not be converted.
    skipped: usize,
}

// Reason text and post-only classification for a definitive WS write failure.
// Non-JSON-RPC errors carry no venue code and are never post-only crossings.
fn ws_rejection_reason(error: &DeriveWsError) -> (String, bool) {
    match error {
        DeriveWsError::JsonRpc { code, message, .. } => (
            format!("JSON-RPC {code}: {message}"),
            derive_rejection_due_post_only(Some(*code), message),
        ),
        other => (other.to_string(), false),
    }
}

fn add_missing_flat_position_reports(
    mass_status: &mut ExecutionMassStatus,
    account_id: AccountId,
    touched_instruments: AHashSet<InstrumentId>,
    position_instruments: &AHashSet<InstrumentId>,
    ts_init: UnixNanos,
) {
    let mut flat_reports = Vec::new();

    for instrument_id in touched_instruments {
        if position_instruments.contains(&instrument_id) {
            continue;
        }

        flat_reports.push(PositionStatusReport::new(
            account_id,
            instrument_id,
            PositionSide::Flat,
            Quantity::from("0"),
            ts_init,
            ts_init,
            Some(UUID4::new()),
            None,
            None,
        ));
    }

    if !flat_reports.is_empty() {
        log::info!(
            "Added {} flat PositionReports for Derive instruments absent from current positions",
            flat_reports.len()
        );
        mass_status.add_position_reports(flat_reports);
    }
}

fn report_precision(
    dispatch_state: &WsDispatchState,
    instrument_name: &str,
) -> (Option<u8>, Option<u8>) {
    let instrument_id = format_instrument_id(instrument_name);
    dispatch_state
        .instrument_precision(&instrument_id)
        .map_or((None, None), |(price, size)| (Some(price), Some(size)))
}

fn handle_ws_message(
    message: DeriveWsMessage,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
) {
    let payload = match message {
        DeriveWsMessage::Subscription(payload) => payload,
        DeriveWsMessage::Authenticated
        | DeriveWsMessage::Reconnected
        | DeriveWsMessage::SessionRecoveryFailed(_) => return,
    };

    let is_orders_channel = payload.channel.ends_with(".orders");
    let is_trades_channel = payload.channel.ends_with(".trades");

    if is_orders_channel {
        let data = match serde_json::from_str::<DeriveOrdersSubscriptionData>(payload.data.get()) {
            Ok(data) => data,
            Err(e) => {
                log::warn!(
                    "Failed to decode Derive orders frame on channel {}: {e}",
                    payload.channel,
                );
                return;
            }
        };
        dispatch_orders_payload(data, emitter, account_id, clock, dispatch_state);
    } else if is_trades_channel {
        let data = match serde_json::from_str::<DeriveTradesSubscriptionData>(payload.data.get()) {
            Ok(data) => data,
            Err(e) => {
                log::warn!(
                    "Failed to decode Derive trades frame on channel {}: {e}",
                    payload.channel,
                );
                return;
            }
        };
        dispatch_trades_payload(data, emitter, account_id, clock, dispatch_state);
    }
}

/// Dispatches a parsed `{subaccount_id}.orders` payload to the execution event
/// emitter.
///
/// Emits tracked order events when an order's client order id resolves to a
/// registered identity in `dispatch_state`, and forwards a raw status report
/// otherwise.
pub fn dispatch_orders_payload(
    data: DeriveOrdersSubscriptionData,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
) {
    let ts_init = clock.get_time_ns();

    for order in data.orders {
        let (price_precision, size_precision) =
            report_precision(dispatch_state, order.instrument_name.as_str());
        let report = match parse_derive_order_to_report_with_precision(
            &order,
            account_id,
            price_precision,
            size_precision,
            ts_init,
        ) {
            Ok(report) => report,
            Err(e) => {
                log::warn!("Failed to parse Derive order WS update: {e}");
                continue;
            }
        };

        let identity = tracked_order_identity(report.client_order_id, dispatch_state);

        match identity {
            Some((client_order_id, identity)) => emit_tracked_order_event(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                &report,
                account_id,
                ts_init,
            ),
            None => emitter.send_order_status_report(report),
        }
    }
}

/// Dispatches a parsed `{subaccount_id}.trades` payload to the execution event
/// emitter.
///
/// Deduplicates by trade id, then emits a tracked fill when the trade's client
/// order id resolves to a registered identity in `dispatch_state`, and forwards
/// a raw fill report otherwise.
pub fn dispatch_trades_payload(
    data: DeriveTradesSubscriptionData,
    emitter: &ExecutionEventEmitter,
    account_id: AccountId,
    clock: &'static AtomicTime,
    dispatch_state: &WsDispatchState,
) {
    let fee_currency = Currency::USDC();
    let ts_init = clock.get_time_ns();

    for trade in data.trades {
        let (price_precision, size_precision) =
            report_precision(dispatch_state, trade.instrument_name.as_str());
        match parse_derive_trade_to_fill_report_with_precision(
            &trade,
            account_id,
            fee_currency,
            price_precision,
            size_precision,
            ts_init,
        ) {
            Ok(Some(report)) => {
                if dispatch_state.check_and_insert_trade(report.trade_id) {
                    log::debug!(
                        "Skipping duplicate Derive fill (trade_id={}) on WS dispatch",
                        report.trade_id,
                    );
                    continue;
                }

                let identity = tracked_order_identity(report.client_order_id, dispatch_state);

                match identity {
                    Some((client_order_id, identity)) => emit_tracked_fill(
                        emitter,
                        dispatch_state,
                        client_order_id,
                        identity,
                        &report,
                        account_id,
                        ts_init,
                    ),
                    None => emitter.send_fill_report(report),
                }
            }
            Ok(None) => {}
            Err(e) => log::warn!("Failed to parse Derive trade WS update: {e}"),
        }
    }
}

fn tracked_order_identity(
    client_order_id: Option<ClientOrderId>,
    dispatch_state: &WsDispatchState,
) -> Option<(ClientOrderId, OrderIdentity)> {
    client_order_id.and_then(|cid| {
        dispatch_state
            .identity(&cid)
            .map(|identity| (cid, identity))
    })
}

/// Synthesizes and emits `OrderAccepted` when one has not yet been emitted
/// for the order. Used to guarantee the `Submitted -> Accepted -> ...`
/// lifecycle when a fill or terminal event arrives before (or instead of)
/// the venue's `Open` notice.
#[expect(clippy::too_many_arguments)]
fn ensure_accepted_emitted(
    emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    client_order_id: ClientOrderId,
    identity: OrderIdentity,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    if dispatch_state.mark_accepted(client_order_id) {
        return;
    }
    let accepted = OrderAccepted::new(
        emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        client_order_id,
        venue_order_id,
        account_id,
        UUID4::new(),
        ts_event,
        ts_init,
        false,
    );
    emitter.send_order_event(OrderEventAny::Accepted(accepted));
}

#[expect(clippy::too_many_arguments)]
fn ensure_canceled_emitted(
    emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    client_order_id: ClientOrderId,
    identity: OrderIdentity,
    venue_order_id: VenueOrderId,
    account_id: AccountId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) {
    if dispatch_state.mark_canceled(client_order_id) {
        return;
    }
    let canceled = OrderCanceled::new(
        emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        client_order_id,
        UUID4::new(),
        ts_event,
        ts_init,
        false,
        Some(venue_order_id),
        Some(account_id),
        None,
    );
    emitter.send_order_event(OrderEventAny::Canceled(canceled));
}

fn emit_tracked_order_event(
    emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    client_order_id: ClientOrderId,
    identity: OrderIdentity,
    report: &OrderStatusReport,
    account_id: AccountId,
    ts_init: UnixNanos,
) {
    let venue_order_id = report.venue_order_id;
    let ts_accepted = report.ts_accepted;
    let ts_event = report.ts_last;

    // A `private/replace` cancels the old order and opens a new one under the
    // same label; suppress events for the superseded old venue order id so they
    // don't terminate the order that `modify_order` rebinds via `OrderUpdated`.
    // `pending_modify` covers the in-flight window; the bound-id check covers
    // after the rebind.
    if dispatch_state.pending_modify(&client_order_id) == Some(venue_order_id) {
        log::debug!(
            "Skipping cancel-replace leg for {client_order_id}: stale venue_order_id={venue_order_id}",
        );
        return;
    }

    if let Some(bound) = dispatch_state.bound_venue_order_id(&client_order_id)
        && bound != venue_order_id
    {
        let terminal = matches!(
            report.order_status,
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected
        );

        if dispatch_state.bind_incoming_modify(client_order_id, venue_order_id, terminal) {
            log::debug!(
                "Bound incoming replacement for {client_order_id}: venue_order_id={venue_order_id}",
            );
        } else {
            log::debug!(
                "Skipping stale {:?} for {client_order_id}: venue_order_id={venue_order_id} superseded by {bound}",
                report.order_status,
            );
            return;
        }
    }

    match report.order_status {
        OrderStatus::Accepted | OrderStatus::PartiallyFilled => {
            if dispatch_state.contains_filled(&client_order_id) {
                log::debug!("Skipping stale Accepted for {client_order_id} (already filled)",);
                return;
            }
            dispatch_state.record_venue_order_id(client_order_id, venue_order_id);
            ensure_accepted_emitted(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                ts_accepted,
                ts_init,
            );
        }
        OrderStatus::Filled => {
            dispatch_state.record_venue_order_id(client_order_id, venue_order_id);
            ensure_accepted_emitted(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                ts_accepted,
                ts_init,
            );
            // Mark the order terminal so replayed Accepted frames are
            // suppressed, but keep its identity alive: the matching
            // `.trades` frame may arrive after this `.orders` Filled
            // notice and still needs the tracked path to emit a proper
            // `OrderFilled`. Identity is retired by Canceled/Expired/
            // Rejected paths; full-fill leaks are bounded by submission
            // throughput.
            dispatch_state.mark_filled(client_order_id);
        }
        OrderStatus::Canceled => {
            ensure_accepted_emitted(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                ts_accepted,
                ts_init,
            );
            ensure_canceled_emitted(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                ts_event,
                ts_init,
            );
            dispatch_state.forget(&client_order_id);
        }
        OrderStatus::Expired => {
            ensure_accepted_emitted(
                emitter,
                dispatch_state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                ts_accepted,
                ts_init,
            );
            let expired = OrderExpired::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                Some(venue_order_id),
                Some(account_id),
            );
            emitter.send_order_event(OrderEventAny::Expired(expired));
            dispatch_state.forget(&client_order_id);
        }
        OrderStatus::Rejected => {
            let reason = report
                .cancel_reason
                .as_deref()
                .unwrap_or("Order rejected by Derive");
            let due_post_only = derive_rejection_due_post_only(None, reason);
            let rejected = OrderRejected::new(
                emitter.trader_id(),
                identity.strategy_id,
                identity.instrument_id,
                client_order_id,
                account_id,
                Ustr::from(reason),
                UUID4::new(),
                ts_event,
                ts_init,
                false,
                due_post_only,
            );
            emitter.send_order_event(OrderEventAny::Rejected(rejected));
            dispatch_state.forget(&client_order_id);
        }
        other => {
            log::debug!(
                "Unhandled tracked order status {other:?} for {client_order_id}, sending as report",
            );
            emitter.send_order_status_report(report.clone());
        }
    }
}

fn emit_tracked_fill(
    emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    client_order_id: ClientOrderId,
    identity: OrderIdentity,
    report: &FillReport,
    account_id: AccountId,
    ts_init: UnixNanos,
) {
    ensure_accepted_emitted(
        emitter,
        dispatch_state,
        client_order_id,
        identity,
        report.venue_order_id,
        account_id,
        report.ts_event,
        ts_init,
    );

    let filled = OrderFilled::new(
        emitter.trader_id(),
        identity.strategy_id,
        identity.instrument_id,
        client_order_id,
        report.venue_order_id,
        account_id,
        report.trade_id,
        identity.order_side,
        identity.order_type,
        report.last_qty,
        report.last_px,
        report.commission.currency,
        report.liquidity_side,
        UUID4::new(),
        report.ts_event,
        ts_init,
        false,
        report.venue_position_id,
        Some(report.commission),
        None,
    );
    emitter.send_order_event(OrderEventAny::Filled(filled));
}

/// Derives the worst-acceptable limit price for a market order from the
/// top-of-book quote and a slippage bound in basis points, rounded to the
/// instrument's `tick_size`.
///
/// Buys lift the ask by `slippage_bps` then round up to the next tick; sells
/// drop the bid by the same and round down. The result is the signed
/// `limit_price` slot in the EIP-712 trade module data; the venue uses it
/// as a worst-case bound while the order sweeps. A non-positive sell bound
/// is rejected (`None`) so the caller can deny the order rather than sign
/// an invalid zero limit.
fn market_order_limit_price(
    quote: &QuoteTick,
    side: OrderSide,
    slippage_bps: u32,
    tick_size: Decimal,
) -> Option<Decimal> {
    let bps = Decimal::from(slippage_bps);
    let scale = Decimal::from(10_000_u32);
    let one = Decimal::ONE;
    let raw = match side {
        OrderSide::Buy => quote.ask_price.as_decimal() * (one + bps / scale),
        OrderSide::Sell => quote.bid_price.as_decimal() * (one - bps / scale),
    };
    let rounded = round_to_tick(raw, tick_size, side);
    if rounded <= Decimal::ZERO {
        return None;
    }
    Some(rounded)
}

fn trigger_market_limit_price(
    trigger_price: Decimal,
    side: OrderSide,
    slippage_bps: u32,
    tick_size: Decimal,
) -> Option<Decimal> {
    let bps = Decimal::from(slippage_bps);
    let scale = Decimal::from(10_000_u32);
    let one = Decimal::ONE;
    let raw = match side {
        OrderSide::Buy => trigger_price * (one + bps / scale),
        OrderSide::Sell => trigger_price * (one - bps / scale),
    };
    let rounded = round_to_tick(raw, tick_size, side);
    if rounded <= Decimal::ZERO {
        return None;
    }
    Some(rounded)
}

fn is_derive_trigger_order_type(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
    )
}

fn trigger_order_signature_expiry(clock: &'static AtomicTime) -> i64 {
    let now_secs = (clock.get_time_ns().as_u64() / 1_000_000_000) as i64;
    now_secs + TRIGGER_ORDER_SIGNATURE_TTL.as_secs() as i64
}

fn resolve_submit_nonce(
    nonce: Result<u64, NonceError>,
    emitter: &ExecutionEventEmitter,
    dispatch_state: &WsDispatchState,
    order: &OrderAny,
    clock: &'static AtomicTime,
) -> Option<u64> {
    match nonce {
        Ok(nonce) => Some(nonce),
        Err(e) => {
            let reason = format!("nonce allocation failed: {e}");
            log::warn!("Cannot submit order {}: {reason}", order.client_order_id());
            dispatch_state.forget(&order.client_order_id());
            emitter.emit_order_rejected(order, &reason, clock.get_time_ns(), false);
            None
        }
    }
}

fn resolve_modify_nonce(
    nonce: Result<u64, NonceError>,
    emitter: &ExecutionEventEmitter,
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: VenueOrderId,
    clock: &'static AtomicTime,
) -> Option<u64> {
    match nonce {
        Ok(nonce) => Some(nonce),
        Err(e) => {
            let reason = format!("nonce allocation failed: {e}");
            log::warn!("Cannot modify order {client_order_id}: {reason}");
            emitter.emit_order_modify_rejected_event(
                strategy_id,
                instrument_id,
                client_order_id,
                Some(venue_order_id),
                &reason,
                clock.get_time_ns(),
            );
            None
        }
    }
}

fn normal_order_signature_expiry(
    clock: &'static AtomicTime,
    signature_expiry_secs: u64,
) -> anyhow::Result<i64> {
    let min_ttl_secs = MIN_SIGNATURE_TTL.as_secs();
    if signature_expiry_secs <= min_ttl_secs {
        anyhow::bail!(
            "signature_expiry_secs {signature_expiry_secs}s must be greater than the Derive minimum {min_ttl_secs}s"
        );
    }

    let now_secs_u64 = clock.get_time_ns().as_u64() / 1_000_000_000;
    let now_secs = i64::try_from(now_secs_u64).with_context(|| {
        format!("current UNIX time {now_secs_u64}s cannot fit in Derive signature_expiry_sec")
    })?;
    let ttl_secs = i64::try_from(signature_expiry_secs).with_context(|| {
        format!(
            "signature_expiry_secs {signature_expiry_secs}s cannot fit in Derive signature_expiry_sec"
        )
    })?;

    now_secs.checked_add(ttl_secs).ok_or_else(|| {
        anyhow::anyhow!(
            "signature expiry overflows Derive signature_expiry_sec: now {now_secs}s plus TTL {ttl_secs}s"
        )
    })
}

/// Fetches a top-of-book quote to price a market order against.
///
/// Uses the instrument-specific `public/get_ticker` endpoint, not the bulk
/// `public/get_tickers` map: during the outage this failover exists for, the
/// bulk map comes back empty, which would reject every market order — entries
/// and risk-reducing exits alike. The returned quote is validated for a usable
/// two-sided book and bounded against `max_quote_age_secs` before anything is
/// signed; `None` leaves the age unbounded, which is the default-off
/// WebSocket-only configuration.
async fn refresh_market_order_quote(
    http_client: &DeriveHttpClient,
    venue_symbol: &str,
    instrument: &DeriveInstrument,
    clock: &'static AtomicTime,
    max_quote_age_secs: Option<u64>,
) -> anyhow::Result<QuoteTick> {
    let ticker = http_client.get_ticker_for_instrument(venue_symbol).await?;
    let price_precision = Price::from_decimal(instrument.tick_size)
        .with_context(|| format!("invalid Derive tick_size for {venue_symbol}"))?
        .precision;
    let size_precision = Quantity::from_decimal(instrument.amount_step)
        .with_context(|| format!("invalid Derive amount_step for {venue_symbol}"))?
        .precision;

    let ts_init = clock.get_time_ns();
    let quote = parse_ticker_quote_from_rest(&ticker, price_precision, size_precision, ts_init)?;

    anyhow::ensure!(
        quote.bid_price.as_decimal() > Decimal::ZERO
            && quote.ask_price.as_decimal() > Decimal::ZERO
            && !quote.bid_size.is_zero()
            && !quote.ask_size.is_zero()
            && quote.bid_price < quote.ask_price,
        "unusable Derive REST quote for {venue_symbol}: bid={} ask={} bid_size={} ask_size={}",
        quote.bid_price,
        quote.ask_price,
        quote.bid_size,
        quote.ask_size,
    );

    if let Some(max_quote_age_secs) = max_quote_age_secs {
        anyhow::ensure!(
            quote_is_fresh(
                Some(quote.ts_event),
                ts_init.as_u64(),
                Some(max_quote_age_secs)
            ),
            "stale or future-dated Derive REST quote for {venue_symbol}: ts_event={} ts_now={ts_init} max_age_secs={max_quote_age_secs}",
            quote.ts_event,
        );
    }

    Ok(quote)
}

/// Classifies a replace by whether it can increase exposure.
///
/// A larger quantity increases exposure directly; moving the limit price toward
/// or through the market (up for a buy, down for a sell) increases the chance
/// of being filled. Either makes the replace risk-increasing and subject to the
/// full entry gate. A reduce-only order's replace can never increase exposure,
/// and a replace with no resolvable target price is treated conservatively as
/// risk-increasing.
fn replace_write_kind(
    order: &OrderAny,
    target_quantity: Quantity,
    target_price: Option<Price>,
) -> RestWriteKind {
    if order.is_reduce_only() {
        return RestWriteKind::ReducingReplace;
    }
    if target_quantity > order.quantity() {
        return RestWriteKind::RiskIncreasingReplace;
    }
    let (Some(target_price), Some(current_price)) = (target_price, order.price()) else {
        return RestWriteKind::RiskIncreasingReplace;
    };
    let more_aggressive = match order.order_side() {
        OrderSide::Buy => target_price > current_price,
        OrderSide::Sell => target_price < current_price,
    };
    if more_aggressive {
        RestWriteKind::RiskIncreasingReplace
    } else {
        RestWriteKind::ReducingReplace
    }
}

/// Rounds `value` to the nearest multiple of `tick_size`. Buys round up so
/// the signed bound remains acceptable to the venue; sells round down so the
/// caller does not accidentally tighten the floor. A non-positive `tick_size`
/// is treated as a no-op.
fn round_to_tick(value: Decimal, tick_size: Decimal, side: OrderSide) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return value;
    }
    let ratio = value / tick_size;
    let ticks = match side {
        OrderSide::Buy => ratio.ceil(),
        OrderSide::Sell => ratio.floor(),
    };
    ticks * tick_size
}

async fn cached_or_fetch_instrument(
    http_client: &DeriveHttpClient,
    instruments: &Arc<AtomicMap<InstrumentId, DeriveInstrument>>,
    instrument_id: &InstrumentId,
    venue_symbol: &str,
) -> anyhow::Result<DeriveInstrument> {
    if let Some(cached) = instruments.get_cloned(instrument_id) {
        return Ok(cached);
    }
    let instrument = http_client
        .get_instrument(venue_symbol)
        .await
        .with_context(|| format!("failed to fetch instrument {venue_symbol}"))?;
    instruments.insert(*instrument_id, instrument.clone());
    Ok(instrument)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        messages::{ExecutionEvent, ExecutionReport},
    };
    use nautilus_core::UnixNanos;
    use nautilus_live::ExecutionClientCore;
    use nautilus_model::{
        data::QuoteTick,
        enums::{AccountType, OmsType, TimeInForce},
        identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId},
        orders::OrderTestBuilder,
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::common::{
        consts::DERIVE,
        enums::{DeriveEnvironment, DeriveOrderStatus, DeriveOrderType},
        parse::parse_derive_instrument_any,
    };

    const TEST_WALLET: &str = "0x0000000000000000000000000000000000001234";
    const TEST_SESSION_KEY: &str =
        "0x2ae8be44db8a590d20bffbe3b6872df9b569147d3bf6801a35a28281a4816bbd";
    const TEST_SUBACCOUNT: u64 = 30769;

    fn test_core() -> ExecutionClientCore {
        let cache = Rc::new(RefCell::new(Cache::default()));
        ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from(DERIVE),
            *DERIVE_VENUE,
            OmsType::Netting,
            AccountId::from("DERIVE-001"),
            AccountType::Margin,
            None,
            cache,
        )
    }

    fn test_config() -> DeriveExecutionClientConfig {
        DeriveExecutionClientConfig {
            wallet_address: Some(TEST_WALLET.to_string()),
            session_key: Some(TEST_SESSION_KEY.into()),
            subaccount_id: Some(TEST_SUBACCOUNT),
            environment: DeriveEnvironment::Testnet,
            domain_separator: Some(
                "0x2222222222222222222222222222222222222222222222222222222222222222".to_string(),
            ),
            action_typehash: Some(
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            ),
            trade_module_address: Some("0x000000000000000000000000000000000000bbbb".to_string()),
            max_fee_per_contract: Some(dec!(1000)),
            ..DeriveExecutionClientConfig::default()
        }
    }

    #[rstest]
    fn test_market_order_limit_price_buy_lifts_ask_and_rounds_up_to_tick() {
        let quote = QuoteTick::new(
            InstrumentId::from("ETH-PERP.DERIVE"),
            Price::from("3500.00"),
            Price::from("3501.00"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(0),
            UnixNanos::from(0),
        );
        // 50 bps; raw = 3501 * 1.005 = 3518.505; tick 0.01 rounds up to 3518.51.
        let price = market_order_limit_price(&quote, OrderSide::Buy, 50, dec!(0.01)).unwrap();
        assert_eq!(price, dec!(3518.51));
    }

    #[rstest]
    fn test_market_order_limit_price_sell_drops_bid_rounds_down_and_denies_non_positive() {
        let quote = QuoteTick::new(
            InstrumentId::from("ETH-PERP.DERIVE"),
            Price::from("3500.00"),
            Price::from("3501.00"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(0),
            UnixNanos::from(0),
        );
        // 50 bps; raw = 3500 * 0.995 = 3482.5; tick 0.01 stays at 3482.5.
        let price = market_order_limit_price(&quote, OrderSide::Sell, 50, dec!(0.01)).unwrap();
        assert_eq!(price, dec!(3482.5));

        // 20_000 bps = 200% slippage drives the rounded bound below zero; deny.
        let zero = market_order_limit_price(&quote, OrderSide::Sell, 20_000, dec!(0.01));
        assert!(zero.is_none());
    }

    #[rstest]
    fn test_trigger_market_limit_price_uses_trigger_price_bound() {
        let buy = trigger_market_limit_price(dec!(3600), OrderSide::Buy, 50, dec!(0.01)).unwrap();
        let sell = trigger_market_limit_price(dec!(3600), OrderSide::Sell, 50, dec!(0.01)).unwrap();
        let zero = trigger_market_limit_price(dec!(1), OrderSide::Sell, 20_000, dec!(0.01));

        assert_eq!(buy, dec!(3618));
        assert_eq!(sell, dec!(3582));
        assert!(zero.is_none());
    }

    #[rstest]
    fn test_normal_order_signature_expiry_accepts_ttl_above_minimum() {
        let clock = get_atomic_clock_realtime();
        let start_secs = (clock.get_time_ns().as_u64() / 1_000_000_000) as i64;
        let ttl_secs = MIN_SIGNATURE_TTL.as_secs() + 1;

        let expiry = normal_order_signature_expiry(clock, ttl_secs).expect("expiry is valid");

        assert!(expiry >= start_secs + ttl_secs as i64);
    }

    #[rstest]
    #[case(MIN_SIGNATURE_TTL.as_secs(), "must be greater than the Derive minimum")]
    #[case(MIN_SIGNATURE_TTL.as_secs() - 1, "must be greater than the Derive minimum")]
    fn test_normal_order_signature_expiry_rejects_minimum_or_lower_ttl(
        #[case] ttl_secs: u64,
        #[case] reason_fragment: &str,
    ) {
        let clock = get_atomic_clock_realtime();

        let err = normal_order_signature_expiry(clock, ttl_secs).expect_err("TTL is too short");

        assert!(
            err.to_string().contains(reason_fragment),
            "unexpected error: {err}",
        );
    }

    #[rstest]
    #[case(i64::MAX as u64, "overflows Derive signature_expiry_sec")]
    #[case(u64::MAX, "cannot fit in Derive signature_expiry_sec")]
    fn test_normal_order_signature_expiry_rejects_extreme_ttl(
        #[case] ttl_secs: u64,
        #[case] reason_fragment: &str,
    ) {
        let clock = get_atomic_clock_realtime();

        let err = normal_order_signature_expiry(clock, ttl_secs).expect_err("TTL is invalid");

        assert!(
            err.to_string().contains(reason_fragment),
            "unexpected error: {err}",
        );
    }

    #[rstest]
    #[case(None, "max_fee_per_contract is required")]
    #[case(Some(dec!(0)), "max_fee_per_contract must be greater than zero")]
    #[case(Some(dec!(-1)), "max_fee_per_contract must be greater than zero")]
    fn test_new_rejects_invalid_max_fee_per_contract(
        #[case] max_fee_per_contract: Option<Decimal>,
        #[case] expected: &str,
    ) {
        let mut config = test_config();
        config.max_fee_per_contract = max_fee_per_contract;

        let err = DeriveExecutionClient::new(test_core(), config).expect_err("must reject");

        assert_eq!(err.to_string(), expected);
    }

    #[rstest]
    #[case(OrderType::StopMarket, true)]
    #[case(OrderType::StopLimit, true)]
    #[case(OrderType::MarketIfTouched, true)]
    #[case(OrderType::LimitIfTouched, true)]
    #[case(OrderType::Market, false)]
    #[case(OrderType::Limit, false)]
    #[case(OrderType::MarketToLimit, false)]
    #[case(OrderType::TrailingStopMarket, false)]
    fn test_is_derive_trigger_order_type(#[case] order_type: OrderType, #[case] expected: bool) {
        assert_eq!(is_derive_trigger_order_type(order_type), expected);
    }

    #[rstest]
    fn test_operation_tokens_start_at_one_so_no_default_value_is_valid() {
        let state = RestExecutionState::default();
        let token = reserve_write(
            &state,
            ClientOrderId::from("STRAT-TOKEN-1"),
            None,
            RestWriteKind::Cancel,
            None,
            0,
            FreshnessBounds::default(),
        )
        .expect("a reducing write needs no freshness");
        assert_eq!(token, OperationToken(1));
    }

    #[rstest]
    fn test_a_write_without_a_token_retires_nothing() {
        // A WebSocket-routed operation never reserves a REST write, so it holds
        // no token and must not be able to clear another operation's record.
        let state = RestExecutionState::default();
        let client_order_id = ClientOrderId::from("STRAT-TOKEN-NONE");
        reserve_write(
            &state,
            client_order_id,
            Some(VenueOrderId::from("ord-1")),
            RestWriteKind::Cancel,
            None,
            0,
            FreshnessBounds::default(),
        )
        .expect("reducing write reserved");

        resolve_write(&state, &client_order_id, None);

        assert!(
            has_unresolved_writes(&state),
            "an operation with no token must not retire the cancellation's record",
        );
    }

    #[rstest]
    fn test_a_stale_token_does_not_retire_a_newer_operations_record() {
        let state = RestExecutionState::default();
        let client_order_id = ClientOrderId::from("STRAT-TOKEN-STALE");
        let stale = reserve_write(
            &state,
            client_order_id,
            Some(VenueOrderId::from("ord-1")),
            RestWriteKind::Cancel,
            None,
            0,
            FreshnessBounds::default(),
        )
        .expect("first write reserved");
        // The first write finishes definitively, then a second operation on the
        // same order takes its own record.
        resolve_write(&state, &client_order_id, Some(stale));
        let fresh = reserve_write(
            &state,
            client_order_id,
            Some(VenueOrderId::from("ord-2")),
            RestWriteKind::Cancel,
            None,
            0,
            FreshnessBounds::default(),
        )
        .expect("second write reserved");
        assert_ne!(stale, fresh);

        resolve_write(&state, &client_order_id, Some(stale));

        assert!(
            has_unresolved_writes(&state),
            "a late response must not release uncertainty about a different write",
        );
        resolve_write(&state, &client_order_id, Some(fresh));
        assert!(!has_unresolved_writes(&state));
    }

    #[rstest]
    fn test_an_in_flight_write_is_not_resolved_by_reconciliation_evidence() {
        let state = RestExecutionState::default();
        let client_order_id = ClientOrderId::from("STRAT-TOKEN-IN-FLIGHT");
        let token = reserve_write(
            &state,
            client_order_id,
            Some(VenueOrderId::from("ord-1")),
            RestWriteKind::Cancel,
            None,
            0,
            FreshnessBounds::default(),
        )
        .expect("write reserved");
        assert_eq!(
            state.unresolved_orders.lock()[&client_order_id].stage,
            WriteStage::InFlight,
        );

        mark_write_ambiguous(&state, &client_order_id, Some(token), 1);

        assert_eq!(
            state.unresolved_orders.lock()[&client_order_id].stage,
            WriteStage::Ambiguous,
            "only a completed request becomes resolvable by reconciliation",
        );
    }

    #[rstest]
    fn test_resolve_submit_nonce_emits_rejection_and_forgets_identity() {
        let clock = get_atomic_clock_realtime();
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        let strategy_id = StrategyId::from("S-1");
        let client_order_id = ClientOrderId::from("NONCE-SUBMIT-1");
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TRADER-001"))
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .client_order_id(client_order_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.000"))
            .price(Price::from("3500.00"))
            .build();
        let identity = OrderIdentity {
            instrument_id,
            strategy_id,
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
        };
        let state = WsDispatchState::new();
        state.register_identity(client_order_id, identity);
        let (emitter, mut rx) = test_emitter(clock);

        let nonce = resolve_submit_nonce(
            Err(NonceError::ClockBeforeEpoch),
            &emitter,
            &state,
            &order,
            clock,
        );
        let event = rx.try_recv().expect("OrderRejected event");

        assert!(nonce.is_none());
        assert!(state.identity(&client_order_id).is_none());
        if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = event {
            assert_eq!(rejected.client_order_id, client_order_id);
            assert_eq!(
                rejected.reason,
                "nonce allocation failed: system clock is before UNIX epoch",
            );
        } else {
            panic!("expected OrderRejected, event was {event:?}");
        }
    }

    #[rstest]
    fn test_resolve_modify_nonce_emits_modify_rejection() {
        let clock = get_atomic_clock_realtime();
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        let strategy_id = StrategyId::from("S-1");
        let client_order_id = ClientOrderId::from("NONCE-MODIFY-1");
        let venue_order_id = VenueOrderId::from("ord-nonce-modify-1");
        let (emitter, mut rx) = test_emitter(clock);

        let nonce = resolve_modify_nonce(
            Err(NonceError::ClockBeforeEpoch),
            &emitter,
            strategy_id,
            instrument_id,
            client_order_id,
            venue_order_id,
            clock,
        );
        let event = rx.try_recv().expect("OrderModifyRejected event");

        assert!(nonce.is_none());

        if let ExecutionEvent::Order(OrderEventAny::ModifyRejected(rejected)) = event {
            assert_eq!(rejected.client_order_id, client_order_id);
            assert_eq!(rejected.venue_order_id, Some(venue_order_id));
            assert_eq!(
                rejected.reason,
                "nonce allocation failed: system clock is before UNIX epoch",
            );
        } else {
            panic!("expected OrderModifyRejected, event was {event:?}");
        }
    }

    #[rstest]
    #[case(dec!(0))]
    #[case(dec!(-1))]
    fn test_round_to_tick_treats_non_positive_tick_as_no_op(#[case] tick: Decimal) {
        // Non-positive tick must pass through both sides untouched so the
        // signing path does not divide by zero or amplify garbage tick data.
        assert_eq!(
            round_to_tick(dec!(3501.55), tick, OrderSide::Buy),
            dec!(3501.55)
        );
        assert_eq!(
            round_to_tick(dec!(3501.55), tick, OrderSide::Sell),
            dec!(3501.55)
        );
    }

    #[rstest]
    fn test_resolve_signing_context_rejects_placeholder_domain_separator() {
        // The shipped mainnet defaults are real Protocol Constants, so force
        // an explicit placeholder via the config override to verify the
        // placeholder-detection path still refuses to construct.
        let mut config = test_config();
        config.environment = DeriveEnvironment::Mainnet;
        config.domain_separator =
            Some("0x<paste_from_docs.derive.xyz_protocol_constants>".to_string());
        let err = DeriveExecutionClient::new(test_core(), config).expect_err("must reject");
        let msg = err.to_string();
        assert!(msg.contains("placeholder"), "unexpected error: {msg}",);
    }

    #[rstest]
    fn test_resolve_signing_context_uses_mainnet_defaults() {
        let mut config = test_config();
        config.environment = DeriveEnvironment::Mainnet;
        config.domain_separator = None;
        config.action_typehash = None;
        config.trade_module_address = None;

        DeriveExecutionClient::new(test_core(), config).expect("mainnet defaults should parse");
    }

    #[rstest]
    fn test_resolve_signing_context_uses_testnet_defaults() {
        let mut config = test_config();
        config.environment = DeriveEnvironment::Testnet;
        config.domain_separator = None;
        config.action_typehash = None;
        config.trade_module_address = None;

        DeriveExecutionClient::new(test_core(), config).expect("testnet defaults should parse");
    }

    #[rstest]
    fn test_market_order_limit_price_rounds_to_coarse_tick() {
        // Coarse tick = 1.0 (e.g. weekly option strikes); raw 3518.505 rounds
        // up to 3519, raw 3482.5 rounds down to 3482.
        let quote = QuoteTick::new(
            InstrumentId::from("ETH-20260627-3500-C.DERIVE"),
            Price::from("3500"),
            Price::from("3501"),
            Quantity::from("1.000"),
            Quantity::from("1.000"),
            UnixNanos::from(0),
            UnixNanos::from(0),
        );
        let buy = market_order_limit_price(&quote, OrderSide::Buy, 50, dec!(1)).unwrap();
        assert_eq!(buy, dec!(3519));
        let sell = market_order_limit_price(&quote, OrderSide::Sell, 50, dec!(1)).unwrap();
        assert_eq!(sell, dec!(3482));
    }

    #[rstest]
    fn test_new_populates_identity() {
        let core = test_core();
        let client = DeriveExecutionClient::new(core, test_config()).unwrap();

        assert_eq!(client.client_id(), ClientId::from(DERIVE));
        assert_eq!(client.account_id(), AccountId::from("DERIVE-001"));
        assert_eq!(client.venue(), *DERIVE_VENUE);
        assert_eq!(client.oms_type(), OmsType::Netting);
        assert_eq!(client.subaccount_id(), TEST_SUBACCOUNT);
        assert!(!client.is_connected());
    }

    #[rstest]
    fn test_cache_instrument_registers_report_precision() {
        let client = DeriveExecutionClient::new(test_core(), test_config()).unwrap();
        let instrument = sample_derive_instrument();
        let instrument_id = format_instrument_id(instrument.instrument_name);

        client.cache_instrument(instrument);

        assert_eq!(
            client.dispatch_state.instrument_precision(&instrument_id),
            Some((2, 3)),
        );
    }

    #[rstest]
    fn test_order_dispatch_uses_registered_instrument_precision() {
        let clock = get_atomic_clock_realtime();
        let client = test_client_with_instrument();

        let mut order: DeriveOrder = serde_json::from_str(include_str!(
            "../test_data/perps/http_order_eth_partially_filled.json"
        ))
        .unwrap();
        order.amount = Decimal::from_str_exact("25.000").unwrap();
        order.filled_amount = Decimal::from_str_exact("5.000").unwrap();
        order.limit_price = Decimal::from_str_exact("25.000").unwrap();
        order.order_status = DeriveOrderStatus::Open;
        order.order_type = DeriveOrderType::Limit;

        let (emitter, mut rx) = test_emitter(clock);
        dispatch_orders_payload(
            DeriveOrdersSubscriptionData {
                orders: vec![order],
            },
            &emitter,
            AccountId::from("DERIVE-001"),
            clock,
            &client.dispatch_state,
        );

        let event = rx.try_recv().unwrap();
        let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
            panic!("Expected OrderStatusReport");
        };
        assert_eq!(report.price, Some(Price::from("25.00")));
        assert_eq!(report.price.unwrap().precision, 2);
        assert_eq!(report.quantity, Quantity::from("25.000"));
        assert_eq!(report.quantity.precision, 3);
        assert_eq!(report.filled_qty, Quantity::from("5.000"));
        assert_eq!(report.filled_qty.precision, 3);
    }

    #[rstest]
    fn test_trade_dispatch_uses_registered_instrument_precision() {
        let clock = get_atomic_clock_realtime();
        let client = test_client_with_instrument();
        let mut trade: DeriveTrade = serde_json::from_str(include_str!(
            "../test_data/perps/http_private_trade_eth.json"
        ))
        .unwrap();
        trade.trade_amount = Decimal::from_str_exact("25.000").unwrap();
        trade.trade_price = Decimal::from_str_exact("25.000").unwrap();

        let (emitter, mut rx) = test_emitter(clock);
        dispatch_trades_payload(
            DeriveTradesSubscriptionData {
                trades: vec![trade],
            },
            &emitter,
            AccountId::from("DERIVE-001"),
            clock,
            &client.dispatch_state,
        );

        let event = rx.try_recv().unwrap();
        let ExecutionEvent::Report(ExecutionReport::Fill(report)) = event else {
            panic!("Expected FillReport");
        };
        assert_eq!(report.last_px, Price::from("25.00"));
        assert_eq!(report.last_px.precision, 2);
        assert_eq!(report.last_qty, Quantity::from("25.000"));
        assert_eq!(report.last_qty.precision, 3);
    }

    #[rstest]
    fn test_emit_tracked_event_suppresses_in_flight_replace_cancel_leg() {
        // Derive's `private/replace` cancels the old order; the `.orders`
        // cancel-of-old leg can arrive before `modify_order` rebinds the order,
        // i.e. while the replace is in flight. In that window only the
        // `pending_modify` marker (not the bound-id check) can suppress it. The
        // integration suite covers the post-rebind bound-id branch; this covers
        // the in-flight branch, which is otherwise unexercised end to end.
        let clock = get_atomic_clock_realtime();
        let account_id = AccountId::from("DERIVE-001");
        let instrument_id = InstrumentId::from("ETH-PERP.DERIVE");
        let cid = ClientOrderId::from("STRAT-MOD-INFLIGHT");
        let stale_voi = VenueOrderId::from("ord-stale-1");
        let identity = OrderIdentity {
            instrument_id,
            strategy_id: StrategyId::from("S-1"),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
        };
        // A `cancelled` report for the stale leg, identical across both cases:
        // only the dispatch-state marker differs.
        let report = OrderStatusReport::new(
            account_id,
            instrument_id,
            Some(cid),
            stale_voi,
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Canceled,
            Quantity::from("1.000"),
            Quantity::from("0.000"),
            UnixNanos::from(1_000),
            UnixNanos::from(2_000),
            UnixNanos::from(3_000),
            None,
        );

        // Marker targets the cancel's venue order id and no bound id is
        // recorded, so suppression can only come from the in-flight branch.
        let (emitter, mut rx) = test_emitter(clock);
        let state = WsDispatchState::new();
        state.mark_pending_modify(cid, stale_voi);
        emit_tracked_order_event(
            &emitter,
            &state,
            cid,
            identity,
            &report,
            account_id,
            UnixNanos::from(0),
        );
        let suppressed = rx.try_recv().is_err();

        // A marker for a different venue order id must not suppress: the guard
        // keys on the specific id, so the cancel-of-old still terminates.
        let (emitter, mut rx) = test_emitter(clock);
        let state = WsDispatchState::new();
        state.mark_pending_modify(cid, VenueOrderId::from("ord-other"));
        emit_tracked_order_event(
            &emitter,
            &state,
            cid,
            identity,
            &report,
            account_id,
            UnixNanos::from(0),
        );
        let mut saw_canceled = false;

        while let Ok(event) = rx.try_recv() {
            if matches!(event, ExecutionEvent::Order(OrderEventAny::Canceled(_))) {
                saw_canceled = true;
            }
        }

        assert!(
            suppressed,
            "in-flight cancel-of-old leg must be suppressed by the pending-modify marker",
        );
        assert!(
            saw_canceled,
            "a pending-modify marker for a different venue order id must not suppress",
        );
    }

    #[rstest]
    fn test_ensure_canceled_emitted_is_idempotent() {
        let clock = get_atomic_clock_realtime();
        let account_id = AccountId::from("DERIVE-001");
        let client_order_id = ClientOrderId::from("TRIGGER-CANCEL-1");
        let identity = OrderIdentity {
            instrument_id: InstrumentId::from("ETH-PERP.DERIVE"),
            strategy_id: StrategyId::from("S-1"),
            order_side: OrderSide::Buy,
            order_type: OrderType::StopMarket,
        };
        let venue_order_id = VenueOrderId::from("trigger-cancel-1");
        let state = WsDispatchState::new();
        let (emitter, mut rx) = test_emitter(clock);

        for _ in 0..2 {
            ensure_canceled_emitted(
                &emitter,
                &state,
                client_order_id,
                identity,
                venue_order_id,
                account_id,
                UnixNanos::from(1_000),
                UnixNanos::from(1_000),
            );
        }

        assert!(matches!(
            rx.try_recv(),
            Ok(ExecutionEvent::Order(OrderEventAny::Canceled(_)))
        ));
        assert!(rx.try_recv().is_err(), "duplicate OrderCanceled emitted");
    }

    fn test_emitter(
        clock: &'static AtomicTime,
    ) -> (
        ExecutionEventEmitter,
        tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            TraderId::from("TRADER-001"),
            AccountId::from("DERIVE-001"),
            AccountType::Margin,
            Some(Currency::USDC()),
        );
        emitter.set_sender(tx);
        (emitter, rx)
    }

    fn sample_derive_instrument() -> DeriveInstrument {
        serde_json::from_str(include_str!("../test_data/perps/instrument_eth.json")).unwrap()
    }

    fn test_client_with_instrument() -> DeriveExecutionClient {
        let mut client = DeriveExecutionClient::new(test_core(), test_config()).unwrap();
        let instrument =
            parse_derive_instrument_any(&sample_derive_instrument(), UnixNanos::default())
                .unwrap()
                .unwrap();
        client.on_instrument(instrument);
        client
    }
}
