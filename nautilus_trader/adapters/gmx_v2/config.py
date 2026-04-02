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
"""Configuration classes for the GMX V2 adapter."""

from nautilus_trader.adapters.gmx_v2.common.constants import DEFAULT_POLL_INTERVAL_MARKETS_SECS
from nautilus_trader.adapters.gmx_v2.common.constants import DEFAULT_POLL_INTERVAL_PRICES_SECS
from nautilus_trader.adapters.gmx_v2.common.constants import DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS
from nautilus_trader.adapters.gmx_v2.common.constants import GMX_CHAIN_ARBITRUM
from nautilus_trader.common.config import PositiveInt
from nautilus_trader.config import LiveDataClientConfig


class GmxV2DataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``GmxV2DataClient`` instances.

    Parameters
    ----------
    chain : str, default "arbitrum"
        The blockchain network. Supported: "arbitrum", "avalanche".
    base_url_http : str, optional
        The base URL for GMX REST API endpoints.
        Defaults to the Arbitrum primary URL.
    arbitrum_rpc_url : str, optional
        The Arbitrum JSON-RPC URL for on-chain contract reads.
        If ``None`` then will source the ``JSON_RPC_ARBITRUM`` environment variable.
        Required for ``GMXMarketData`` on-chain queries.
    poll_interval_prices_secs : float, default 2.0
        How often to poll the price tickers endpoint (seconds).
    poll_interval_markets_secs : float, default 10.0
        How often to poll the markets info endpoint for funding/OI data (seconds).
    update_instruments_interval_mins : int, default 60
        How often to reload instruments from the API (minutes).
    max_retries : PositiveInt, optional
        The maximum number of retries for failed requests.
    retry_delay_secs : float, default 1.0
        Delay between retries in seconds.

    """

    chain: str = GMX_CHAIN_ARBITRUM
    base_url_http: str | None = None
    arbitrum_rpc_url: str | None = None
    poll_interval_prices_secs: float = DEFAULT_POLL_INTERVAL_PRICES_SECS
    poll_interval_markets_secs: float = DEFAULT_POLL_INTERVAL_MARKETS_SECS
    update_instruments_interval_mins: int = DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS
    max_retries: PositiveInt | None = 3
    retry_delay_secs: float = 1.0
