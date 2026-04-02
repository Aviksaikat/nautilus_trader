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
"""Parsing utilities for the GMX V2 adapter.

Converts eth_defi API responses into NautilusTrader model objects.
"""

from decimal import Decimal

from nautilus_trader.adapters.gmx_v2.common.symbol import gmx_symbol_to_instrument_id
from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import FundingRateUpdate
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


# GMX maker fee: 0.02%, taker fee: 0.05%
_GMX_MAKER_FEE = Decimal("0.00020")
_GMX_TAKER_FEE = Decimal("0.00050")

# Standard price/size precision for GMX perps
_PRICE_PRECISION = 2
_SIZE_PRECISION = 4
_PRICE_INCREMENT = Price.from_str("0.01")
_SIZE_INCREMENT = Quantity.from_str("0.0001")

# GMX uses 30-decimal fixed-point internally
_GMX_PRICE_EXPONENT = 30


def parse_instrument(
    market: dict,
    tokens_by_address: dict,
) -> CryptoPerpetual | None:
    """
    Parse a GMX V2 market dict into a NautilusTrader CryptoPerpetual instrument.

    :param market: A GMX V2 market dict from ``get_markets_info()``.
    :param tokens_by_address: Mapping of token address to token info dict
        ``{"symbol": str, "decimals": int}``.
    :return: A ``CryptoPerpetual`` instance, or ``None`` if the market is not listed
        or if the required token info cannot be found/parsed.
    """
    if not market.get("isListed", False):
        return None

    index_token_address = market.get("indexToken")
    short_token_address = market.get("shortToken")

    if index_token_address not in tokens_by_address:
        return None
    if short_token_address not in tokens_by_address:
        return None

    index_token = tokens_by_address[index_token_address]
    short_token = tokens_by_address[short_token_address]

    base_symbol_str = index_token["symbol"]
    settlement_symbol_str = short_token["symbol"]

    try:
        base_currency = Currency.from_str(base_symbol_str)
    except Exception:
        return None

    quote_currency = USD

    try:
        settlement_currency = Currency.from_str(settlement_symbol_str)
    except Exception:
        return None

    instrument_id = gmx_symbol_to_instrument_id(base_symbol_str)
    raw_symbol = Symbol(f"{base_symbol_str}-USD-PERP")

    # GMX V2: non-inverse when indexToken == longToken (standard perp)
    is_inverse = False

    return CryptoPerpetual(
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        base_currency=base_currency,
        quote_currency=quote_currency,
        settlement_currency=settlement_currency,
        is_inverse=is_inverse,
        price_precision=_PRICE_PRECISION,
        size_precision=_SIZE_PRECISION,
        price_increment=_PRICE_INCREMENT,
        size_increment=_SIZE_INCREMENT,
        max_quantity=None,
        min_quantity=None,
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0"),
        margin_maint=Decimal("0"),
        maker_fee=_GMX_MAKER_FEE,
        taker_fee=_GMX_TAKER_FEE,
        ts_event=0,
        ts_init=0,
    )


def parse_quote_tick(
    ticker: dict,
    instrument_id: InstrumentId,
    decimals: int,
    ts_init: int,
) -> QuoteTick | None:
    """
    Parse a GMX ticker dict into a NautilusTrader QuoteTick.

    :param ticker: A GMX ticker dict from ``get_tickers()``.
    :param instrument_id: The NautilusTrader instrument ID for this ticker.
    :param decimals: The token's decimal places (e.g. 18 for ETH, 8 for BTC).
    :param ts_init: Current time in nanoseconds (UNIX epoch).
    :return: A ``QuoteTick`` instance, or ``None`` if required fields are missing.
    """
    min_price_raw = ticker.get("minPrice")
    max_price_raw = ticker.get("maxPrice")
    timestamp_secs = ticker.get("timestamp")

    if min_price_raw is None or max_price_raw is None or timestamp_secs is None:
        return None

    # GMX prices are 30-decimal fixed-point; divide by 10^(30 - token_decimals)
    divisor = 10 ** (_GMX_PRICE_EXPONENT - decimals)
    bid_price_usd = int(min_price_raw) / divisor
    ask_price_usd = int(max_price_raw) / divisor

    bid_price = Price(round(bid_price_usd, _PRICE_PRECISION), _PRICE_PRECISION)
    ask_price = Price(round(ask_price_usd, _PRICE_PRECISION), _PRICE_PRECISION)

    # GMX is not an orderbook; sizes are unknown
    bid_size = Quantity(0, 0)
    ask_size = Quantity(0, 0)

    ts_event = int(timestamp_secs) * 1_000_000_000

    return QuoteTick(
        instrument_id=instrument_id,
        bid_price=bid_price,
        ask_price=ask_price,
        bid_size=bid_size,
        ask_size=ask_size,
        ts_event=ts_event,
        ts_init=ts_init,
    )


def parse_funding_rate(
    market: dict,
    instrument_id: InstrumentId,
    ts_init: int,
) -> FundingRateUpdate | None:
    """
    Parse the funding rate from a GMX V2 market dict.

    GMX uses continuous (per-second) funding. ``fundingRateLong`` is the signed
    per-second rate × 10^30, where negative means longs pay shorts.

    :param market: A GMX V2 market dict from ``get_markets_info()``.
    :param instrument_id: The NautilusTrader instrument ID for this market.
    :param ts_init: Current time in nanoseconds (UNIX epoch).
    :return: A ``FundingRateUpdate`` instance, or ``None`` if the funding rate
        field is missing.
    """
    funding_rate_raw = market.get("fundingRateLong")
    if funding_rate_raw is None:
        return None

    # Convert from 30-decimal fixed-point per-second rate to a plain Decimal
    rate_per_second = Decimal(str(int(funding_rate_raw))) / Decimal("1e30")

    return FundingRateUpdate(
        instrument_id=instrument_id,
        rate=rate_per_second,
        ts_event=ts_init,
        ts_init=ts_init,
        next_funding_ns=0,  # GMX uses continuous funding; no discrete next funding time
    )
