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
"""Tests for GMX V2 parsing utilities."""

import pytest

# NOTE: Tests require the nautilus_trader package to be installed (compiled extensions).
# Skip all tests if the package is not available.
pytest.importorskip("nautilus_trader.model.instruments")

from decimal import Decimal

from nautilus_trader.adapters.gmx_v2.common.parsing import parse_funding_rate
from nautilus_trader.adapters.gmx_v2.common.parsing import parse_instrument
from nautilus_trader.adapters.gmx_v2.common.parsing import parse_quote_tick
from nautilus_trader.adapters.gmx_v2.common.symbol import gmx_symbol_to_instrument_id


# -- Fixtures ------------------------------------------------------------------

TOKENS_BY_ADDRESS = {
    "0xIndexETH": {"symbol": "ETH", "decimals": 18},
    "0xLongETH": {"symbol": "ETH", "decimals": 18},
    "0xShortUSDC": {"symbol": "USDC", "decimals": 6},
    "0xIndexBTC": {"symbol": "BTC", "decimals": 8},
    "0xShortUSDT": {"symbol": "USDT", "decimals": 6},
}

ETH_MARKET = {
    "name": "ETH/USD [WETH-USDC]",
    "marketToken": "0xMarketETH",
    "indexToken": "0xIndexETH",
    "longToken": "0xLongETH",
    "shortToken": "0xShortUSDC",
    "isListed": True,
    "fundingRateLong": "-234415323020867061571811615482",
    "fundingRateShort": "285795378003572022502143648000",
    "borrowingRateLong": "37131225434902894175472240000",
    "openInterestLong": "50962362065685009972399965082522856",
    "openInterestShort": "41800391066452521980674550527863996",
}

ETH_TICKER = {
    "tokenAddress": "0xIndexETH",
    "tokenSymbol": "ETH",
    "minPrice": "2049525498631329",
    "maxPrice": "2050000000000000",
    "updatedAt": 1775152319585,
    "timestamp": 1775152317,
}


# -- Tests: parse_instrument ---------------------------------------------------

def test_parse_instrument_eth_returns_perp():
    """parse_instrument returns a CryptoPerpetual for a valid listed ETH market."""
    instrument = parse_instrument(ETH_MARKET, TOKENS_BY_ADDRESS)
    assert instrument is not None
    assert instrument.base_currency.code == "ETH"
    assert instrument.quote_currency.code == "USD"
    assert instrument.settlement_currency.code == "USDC"
    assert not instrument.is_inverse


def test_parse_instrument_unlisted_market_returns_none():
    """parse_instrument returns None for unlisted markets."""
    unlisted = {**ETH_MARKET, "isListed": False}
    assert parse_instrument(unlisted, TOKENS_BY_ADDRESS) is None


def test_parse_instrument_missing_token_returns_none():
    """parse_instrument returns None if token address is not in tokens_by_address."""
    market = {**ETH_MARKET, "indexToken": "0xUnknown"}
    assert parse_instrument(market, TOKENS_BY_ADDRESS) is None


def test_parse_instrument_unknown_currency_returns_none():
    """parse_instrument returns None if Currency.from_str fails for base currency."""
    tokens_with_unknown = {
        **TOKENS_BY_ADDRESS,
        "0xIndexUNKNOWN": {"symbol": "UNKNOWNCOIN123", "decimals": 18},
    }
    market = {**ETH_MARKET, "indexToken": "0xIndexUNKNOWN"}
    # Currency.from_str("UNKNOWNCOIN123") should fail -> returns None
    result = parse_instrument(market, tokens_with_unknown)
    assert result is None


# -- Tests: parse_quote_tick ---------------------------------------------------

def test_parse_quote_tick_eth():
    """parse_quote_tick correctly scales ETH prices from 30-decimal fixed-point."""
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    ts_init = 1_775_152_319_585_000_000

    quote = parse_quote_tick(ETH_TICKER, instrument_id, decimals=18, ts_init=ts_init)
    assert quote is not None
    # ETH: minPrice=2049525498631329, decimals=18 -> 2049525498631329 / 10^12 = 2049.53
    assert float(quote.bid_price) == pytest.approx(2049.53, abs=0.01)
    assert float(quote.ask_price) == pytest.approx(2050.00, abs=0.01)
    assert quote.instrument_id == instrument_id
    assert quote.ts_event == 1775152317 * 1_000_000_000


def test_parse_quote_tick_missing_field_returns_none():
    """parse_quote_tick returns None if required fields are missing."""
    incomplete = {"tokenSymbol": "ETH", "minPrice": "2049525498631329"}  # missing maxPrice
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    result = parse_quote_tick(incomplete, instrument_id, decimals=18, ts_init=0)
    assert result is None


# -- Tests: parse_funding_rate -------------------------------------------------

def test_parse_funding_rate_eth():
    """parse_funding_rate correctly converts 30-decimal fixed-point rate."""
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    ts_init = 1_775_152_319_585_000_000

    funding = parse_funding_rate(ETH_MARKET, instrument_id, ts_init=ts_init)
    assert funding is not None
    assert funding.instrument_id == instrument_id
    # fundingRateLong = -234415323020867061571811615482 -> rate = -2.34e-28... / 1e30 ~ very small negative
    assert funding.rate < Decimal(0)  # Negative: longs paying
    assert funding.ts_event == ts_init
    assert funding.next_funding_ns == 0


def test_parse_funding_rate_missing_field_returns_none():
    """parse_funding_rate returns None if fundingRateLong is missing."""
    market_no_funding = {k: v for k, v in ETH_MARKET.items() if k != "fundingRateLong"}
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    result = parse_funding_rate(market_no_funding, instrument_id, ts_init=0)
    assert result is None
