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
"""Tests for GMX V2 symbol utilities."""

import pytest

# NOTE: Tests require the nautilus_trader package to be installed (compiled extensions).
# Skip all tests if the package is not available.
pytest.importorskip("nautilus_trader.model.identifiers")

from nautilus_trader.adapters.gmx_v2.common.symbol import gmx_symbol_to_instrument_id
from nautilus_trader.adapters.gmx_v2.common.symbol import instrument_id_to_gmx_symbol


def test_gmx_symbol_to_instrument_id_eth():
    """Test ETH symbol maps to correct InstrumentId."""
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    assert instrument_id.symbol.value == "ETH-USD-PERP"
    assert instrument_id.venue.value == "GMX_V2"


def test_gmx_symbol_to_instrument_id_btc():
    """Test BTC symbol maps to correct InstrumentId."""
    instrument_id = gmx_symbol_to_instrument_id("BTC")
    assert instrument_id.symbol.value == "BTC-USD-PERP"
    assert instrument_id.venue.value == "GMX_V2"


def test_gmx_symbol_to_instrument_id_custom_quote():
    """Test custom quote currency."""
    instrument_id = gmx_symbol_to_instrument_id("SOL", quote="USDC")
    assert instrument_id.symbol.value == "SOL-USDC-PERP"


def test_instrument_id_to_gmx_symbol():
    """Test round-trip: symbol -> instrument_id -> symbol."""
    instrument_id = gmx_symbol_to_instrument_id("ETH")
    symbol = instrument_id_to_gmx_symbol(instrument_id)
    assert symbol == "ETH"


def test_instrument_id_to_gmx_symbol_btc():
    """Test BTC round-trip."""
    instrument_id = gmx_symbol_to_instrument_id("BTC")
    symbol = instrument_id_to_gmx_symbol(instrument_id)
    assert symbol == "BTC"
