# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Data client for the GMX V2 decentralized perpetual exchange.

GMX V2 has no WebSocket API. All market data subscriptions are implemented
as asyncio polling loops against the GMX REST API via eth_defi.
"""

import asyncio

from eth_defi.gmx.api import GMXAPI

from nautilus_trader.adapters.gmx_v2.common.constants import GMX_V2_VENUE
from nautilus_trader.adapters.gmx_v2.common.parsing import parse_funding_rate
from nautilus_trader.adapters.gmx_v2.common.parsing import parse_quote_tick
from nautilus_trader.adapters.gmx_v2.common.symbol import gmx_symbol_to_instrument_id
from nautilus_trader.adapters.gmx_v2.common.symbol import instrument_id_to_gmx_symbol
from nautilus_trader.adapters.gmx_v2.config import GmxV2DataClientConfig
from nautilus_trader.adapters.gmx_v2.providers import GmxV2InstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import SubscribeFundingRates
from nautilus_trader.data.messages import SubscribeMarkPrices
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeFundingRates
from nautilus_trader.data.messages import UnsubscribeMarkPrices
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


def _bar_type_to_gmx_period(bar_type: BarType) -> str | None:
    """
    Map a NautilusTrader BarType to a GMX period string.

    :param bar_type: The NautilusTrader BarType to map.
    :return: A GMX period string (e.g. "1m", "1h") or ``None`` if unsupported.
    """
    agg = bar_type.spec.aggregation
    step = bar_type.spec.step

    if agg == BarAggregation.MINUTE:
        if step == 1:
            return "1m"
        if step == 5:
            return "5m"
        if step == 15:
            return "15m"
        if step == 30:
            return "30m"
    elif agg == BarAggregation.HOUR:
        if step == 1:
            return "1h"
        if step == 4:
            return "4h"
    elif agg == BarAggregation.DAY:
        if step == 1:
            return "1d"
    return None


def _parse_candlestick_response(data: dict | list, bar_type: BarType, ts_init: int) -> list[Bar]:
    """
    Parse a GMX candlestick API response into Bar objects.

    :param data: Raw response from ``get_candlesticks()`` — either a dict with a
        ``"candles"`` key or a direct list of candles.
    :param bar_type: The NautilusTrader BarType for the resulting bars.
    :param ts_init: Current time in nanoseconds (UNIX epoch).
    :return: A list of ``Bar`` instances.
    """
    # eth_defi returns either dict with "candles" key or direct list
    candles = data if isinstance(data, list) else data.get("candles", [])
    bars = []
    for c in candles:
        # GMX candles format: [timestamp, open, high, low, close]
        if not isinstance(c, (list, tuple)) or len(c) < 5:
            continue
        ts_event = int(c[0]) * 1_000_000_000
        bars.append(
            Bar(
                bar_type=bar_type,
                open=Price.from_str(str(c[1])),
                high=Price.from_str(str(c[2])),
                low=Price.from_str(str(c[3])),
                close=Price.from_str(str(c[4])),
                volume=Quantity(0, 0),  # GMX doesn't provide volume in candles
                ts_event=ts_event,
                ts_init=ts_init,
            )
        )
    return bars


class GmxV2DataClient(LiveMarketDataClient):
    """
    Provides a data client for the GMX V2 decentralized perpetual exchange.

    GMX V2 has no WebSocket API. All market data subscriptions are implemented
    as asyncio polling loops against the GMX REST API via eth_defi.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    gmx_api : GMXAPI
        The eth_defi GMX API client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : GmxV2InstrumentProvider
        The instrument provider.
    config : GmxV2DataClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        gmx_api: GMXAPI,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: GmxV2InstrumentProvider,
        config: GmxV2DataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or GMX_V2_VENUE.value),
            venue=GMX_V2_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._gmx_api: GMXAPI = gmx_api
        self._config = config
        self._instrument_provider = instrument_provider

        # Tracks running poll tasks keyed by name ("prices", "markets")
        self._poll_tasks: dict[str, asyncio.Task] = {}

        # Sets of subscribed instrument IDs
        self._subscribed_quote_ticks: set[InstrumentId] = set()
        self._subscribed_funding_rates: set[InstrumentId] = set()
        self._subscribed_mark_prices: set[InstrumentId] = set()

        # Populated on connect; maps token symbol → token info dict
        self._tokens_by_symbol: dict[str, dict] = {}

    async def _connect(self) -> None:
        # 1. Initialize instruments
        await self._instrument_provider.initialize()
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

        # 2. Build tokens_by_symbol lookup for price scaling
        tokens_response = await asyncio.to_thread(self._gmx_api.get_tokens)
        self._tokens_by_symbol = {
            t["symbol"]: t
            for t in tokens_response.get("tokens", [])
            if "symbol" in t
        }
        self._log.info(f"Loaded token info for {len(self._tokens_by_symbol)} tokens")

    async def _disconnect(self) -> None:
        for task in self._poll_tasks.values():
            if not task.done():
                task.cancel()
        self._poll_tasks.clear()
        self._subscribed_quote_ticks.clear()
        self._subscribed_funding_rates.clear()
        self._subscribed_mark_prices.clear()

    # -- QUOTE TICKS -----------------------------------------------------------------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._subscribed_quote_ticks:
            return
        self._subscribed_quote_ticks.add(instrument_id)

        # Start a single shared price polling task if not already running
        if "prices" not in self._poll_tasks or self._poll_tasks["prices"].done():
            self._poll_tasks["prices"] = self._loop.create_task(self._poll_prices_loop())

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        self._subscribed_quote_ticks.discard(command.instrument_id)
        if not self._subscribed_quote_ticks and not self._subscribed_mark_prices:
            task = self._poll_tasks.pop("prices", None)
            if task and not task.done():
                task.cancel()

    # -- MARK PRICES -----------------------------------------------------------------------

    async def _subscribe_mark_prices(self, command: SubscribeMarkPrices) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._subscribed_mark_prices:
            return
        self._subscribed_mark_prices.add(instrument_id)

        # Share the same prices polling task as quote ticks
        if "prices" not in self._poll_tasks or self._poll_tasks["prices"].done():
            self._poll_tasks["prices"] = self._loop.create_task(self._poll_prices_loop())

    async def _unsubscribe_mark_prices(self, command: UnsubscribeMarkPrices) -> None:
        self._subscribed_mark_prices.discard(command.instrument_id)
        # Cancel the shared "prices" task only when both sets are empty
        if not self._subscribed_quote_ticks and not self._subscribed_mark_prices:
            task = self._poll_tasks.pop("prices", None)
            if task and not task.done():
                task.cancel()

    # -- FUNDING RATES ---------------------------------------------------------------------

    async def _subscribe_funding_rates(self, command: SubscribeFundingRates) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._subscribed_funding_rates:
            return
        self._subscribed_funding_rates.add(instrument_id)

        if "markets" not in self._poll_tasks or self._poll_tasks["markets"].done():
            self._poll_tasks["markets"] = self._loop.create_task(self._poll_markets_loop())

    async def _unsubscribe_funding_rates(self, command: UnsubscribeFundingRates) -> None:
        self._subscribed_funding_rates.discard(command.instrument_id)
        if not self._subscribed_funding_rates:
            task = self._poll_tasks.pop("markets", None)
            if task and not task.done():
                task.cancel()

    # -- BARS (historical request) ---------------------------------------------------------

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        instrument_id = bar_type.instrument_id
        token_symbol = instrument_id_to_gmx_symbol(instrument_id)

        period = _bar_type_to_gmx_period(bar_type)
        if period is None:
            self._log.warning(f"Unsupported bar type: {bar_type}")
            return

        try:
            data = await asyncio.to_thread(
                self._gmx_api.get_candlesticks, token_symbol, period
            )
            bars = _parse_candlestick_response(data, bar_type, self._clock.timestamp_ns())
            for bar in bars:
                self._handle_data(bar)
        except Exception as exc:
            self._log.error(f"Failed to fetch bars for {instrument_id}: {exc}")

    # -- PRIVATE POLLING LOOPS -------------------------------------------------------------

    async def _poll_prices_loop(self) -> None:
        """Poll GMX tickers and dispatch QuoteTick/MarkPriceUpdate to subscribers."""
        while True:
            try:
                tickers = await asyncio.to_thread(self._gmx_api.get_tickers, False)
                ts_init = self._clock.timestamp_ns()

                for ticker in tickers:
                    symbol = ticker.get("tokenSymbol")
                    if symbol is None:
                        continue

                    instrument_id = gmx_symbol_to_instrument_id(symbol)
                    token_info = self._tokens_by_symbol.get(symbol)
                    if token_info is None:
                        continue
                    decimals = token_info.get("decimals", 18)

                    if instrument_id in self._subscribed_quote_ticks:
                        quote = parse_quote_tick(ticker, instrument_id, decimals, ts_init)
                        if quote is not None:
                            self._handle_data(quote)

                    if instrument_id in self._subscribed_mark_prices:
                        min_price_raw = ticker.get("minPrice")
                        max_price_raw = ticker.get("maxPrice")
                        timestamp = ticker.get("timestamp")
                        if min_price_raw is not None and max_price_raw is not None and timestamp is not None:
                            # Use mid price as mark price
                            mid_raw = (int(min_price_raw) + int(max_price_raw)) // 2
                            divisor = 10 ** (30 - decimals)
                            mark_price_usd = round(mid_raw / divisor, 2)
                            mark = MarkPriceUpdate(
                                instrument_id=instrument_id,
                                value=Price(mark_price_usd, 2),
                                ts_event=int(timestamp) * 1_000_000_000,
                                ts_init=ts_init,
                            )
                            self._handle_data(mark)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error(f"Error polling prices: {exc}")

            await asyncio.sleep(self._config.poll_interval_prices_secs)

    async def _poll_markets_loop(self) -> None:
        """Poll GMX markets info and dispatch FundingRateUpdate to subscribers."""
        while True:
            try:
                markets_resp = await asyncio.to_thread(
                    self._gmx_api.get_markets_info, True, False
                )
                ts_init = self._clock.timestamp_ns()

                for market in markets_resp.get("markets", []):
                    if not market.get("isListed", False):
                        continue

                    # Resolve symbol from market name: "ETH/USD [WETH-USDC]" → "ETH"
                    name = market.get("name", "")
                    symbol = name.split("/")[0] if "/" in name else None
                    if symbol is None:
                        continue

                    instrument_id = gmx_symbol_to_instrument_id(symbol)

                    if instrument_id in self._subscribed_funding_rates:
                        funding = parse_funding_rate(market, instrument_id, ts_init)
                        if funding is not None:
                            self._handle_data(funding)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._log.error(f"Error polling markets: {exc}")

            await asyncio.sleep(self._config.poll_interval_markets_secs)
