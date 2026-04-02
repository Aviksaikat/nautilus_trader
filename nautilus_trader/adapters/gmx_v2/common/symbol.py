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
"""Symbol utilities for the GMX V2 adapter."""

from nautilus_trader.adapters.gmx_v2.common.constants import GMX_V2_VENUE
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol


def gmx_symbol_to_instrument_id(token_symbol: str, quote: str = "USD") -> InstrumentId:
    """
    Convert a GMX token symbol to a NautilusTrader InstrumentId.

    :param token_symbol: The GMX token symbol (e.g. "ETH", "BTC").
    :param quote: The quote currency (default "USD").
    :return: NautilusTrader InstrumentId.
    """
    symbol = Symbol(f"{token_symbol}-{quote}-PERP")
    return InstrumentId(symbol=symbol, venue=GMX_V2_VENUE)


def instrument_id_to_gmx_symbol(instrument_id: InstrumentId) -> str:
    """
    Extract the base token symbol from a NautilusTrader InstrumentId.

    :param instrument_id: The NautilusTrader InstrumentId.
    :return: The GMX token symbol (e.g. "ETH").
    """
    # Symbol format: "ETH-USD-PERP"
    parts = instrument_id.symbol.value.split("-")
    return parts[0]
