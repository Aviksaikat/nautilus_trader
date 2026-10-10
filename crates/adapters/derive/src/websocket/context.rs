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

//! WebSocket message dispatch context for the Derive data client.

use std::sync::Arc;

use nautilus_common::{cache::quote::QuoteCache, live::sender::EventSender, messages::DataEvent};
use nautilus_core::{AtomicMap, AtomicSet, UnixNanos, time::AtomicTime};
use nautilus_model::{identifiers::InstrumentId, instruments::InstrumentAny};
use parking_lot::Mutex;

pub(crate) struct WsMessageContext {
    pub(crate) clock: &'static AtomicTime,
    pub(crate) data_sender: EventSender<DataEvent>,
    pub(crate) instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    pub(crate) active_book_delta_channels: Arc<AtomicMap<InstrumentId, String>>,
    pub(crate) active_book_depth_channels: Arc<AtomicMap<InstrumentId, String>>,
    pub(crate) active_ticker_channels: Arc<AtomicMap<InstrumentId, String>>,
    pub(crate) active_quote_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) active_trade_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) active_mark_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) active_index_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) active_funding_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) active_greeks_subs: Arc<AtomicSet<InstrumentId>>,
    pub(crate) subscription_lock: Arc<Mutex<()>>,
    pub(crate) quote_cache: Arc<Mutex<QuoteCache>>,
    pub(crate) quote_feed_state: Arc<Mutex<ahash::AHashMap<InstrumentId, QuoteFeedState>>>,
    pub(crate) ws_session_generation: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) ws_subscription_handle: crate::websocket::client::DeriveWebSocketSubscriptionHandle,
    pub(crate) max_quote_age_secs: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QuoteFeedState {
    pub(crate) last_published_event: Option<UnixNanos>,
    pub(crate) last_ws_received: Option<UnixNanos>,
    pub(crate) ws_subscription_confirmed: bool,
    pub(crate) rest_polling: bool,
    /// The transport connection epoch as of the most recent genuine WS
    /// subscription confirmation or accepted quote. Fences
    /// `ws_subscription_confirmed` and WS freshness against a prior
    /// session's state surviving a reconnect: `has_subscription` reflects a
    /// retained desired-subscription map, not a current-epoch
    /// acknowledgement, so a stale confirmation from before a reconnect
    /// must not let a new-session replay ticker (or the REST poller) treat
    /// the WS as current before this epoch is reached again.
    pub(crate) ws_epoch: Option<u64>,
}

/// Read-only, cloneable view of the Derive data client's per-instrument
/// quote freshness.
///
/// The execution client has no direct access to the data client's internal
/// state and currently infers market-data health only from
/// [`crate::data::DeriveDataClient::is_connected`], which can report `true`
/// throughout a REST-failover outage even if no REST poll has ever actually
/// produced a quote (`rest_failover_active` latches as soon as the initial
/// WebSocket connect fails, independent of REST poll outcomes). This handle
/// lets a caller check the venue-observed, per-instrument ground truth
/// instead: obtain it from [`crate::data::DeriveDataClient::quote_health_handle`]
/// and share it with whatever needs to gate on "has a quote actually been
/// seen for this instrument, and how recently" (e.g. the execution client's
/// "missing required quote disables new entries" check).
// Not yet called outside this crate's data-client tests: it is staged for
// the execution client (a separate, concurrently developed file) to consume
// once wired up. Keep it rather than deleting it as dead code; removing it
// would hand execution no path to the per-instrument quote evidence the
// "missing required quote disables new entries" design requirement needs.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct DeriveQuoteHealthHandle {
    quote_feed_state: Arc<Mutex<ahash::AHashMap<InstrumentId, QuoteFeedState>>>,
}

#[allow(dead_code)]
impl DeriveQuoteHealthHandle {
    pub(crate) fn new(
        quote_feed_state: Arc<Mutex<ahash::AHashMap<InstrumentId, QuoteFeedState>>>,
    ) -> Self {
        Self { quote_feed_state }
    }

    /// Returns the local receipt time of the most recent successfully
    /// published quote (WebSocket or REST) for `instrument_id`.
    ///
    /// `None` means no quote has ever been published for this instrument —
    /// including while the data client itself reports connected, since that
    /// readiness latch does not require a REST poll to have succeeded.
    pub(crate) fn last_quote_event(&self, instrument_id: InstrumentId) -> Option<UnixNanos> {
        self.quote_feed_state
            .lock()
            .get(&instrument_id)
            .and_then(|state| state.last_published_event)
    }
}
