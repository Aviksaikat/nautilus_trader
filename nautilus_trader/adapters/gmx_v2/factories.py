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
"""Factories for creating GMX V2 data clients."""

import asyncio
from functools import lru_cache

from eth_defi.gmx.api import GMXAPI

from nautilus_trader.adapters.gmx_v2.config import GmxV2DataClientConfig
from nautilus_trader.adapters.gmx_v2.data import GmxV2DataClient
from nautilus_trader.adapters.gmx_v2.providers import GmxV2InstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory


@lru_cache(1)
def get_cached_gmx_api(chain: str) -> GMXAPI:
    """
    Cache and return a GMX API client.

    If a cached client for the given chain already exists, it will be returned.

    Parameters
    ----------
    chain : str
        The chain name (e.g. "arbitrum").

    Returns
    -------
    GMXAPI

    """
    return GMXAPI(chain=chain)


@lru_cache(1)
def get_cached_gmx_v2_instrument_provider(
    gmx_api: GMXAPI,
    config: InstrumentProviderConfig | None = None,
) -> GmxV2InstrumentProvider:
    """
    Cache and return a GMX V2 instrument provider.

    If a cached provider already exists, it will be returned.

    Parameters
    ----------
    gmx_api : GMXAPI
        The GMX API client.
    config : InstrumentProviderConfig, optional
        The instrument provider configuration.

    Returns
    -------
    GmxV2InstrumentProvider

    """
    return GmxV2InstrumentProvider(
        gmx_api=gmx_api,
        config=config,
    )


class GmxV2LiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a GMX V2 live data client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: GmxV2DataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> GmxV2DataClient:
        """
        Create a new GMX V2 data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : GmxV2DataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        GmxV2DataClient

        """
        gmx_api = get_cached_gmx_api(chain=config.chain)
        provider = get_cached_gmx_v2_instrument_provider(
            gmx_api=gmx_api,
            config=config.instrument_provider,
        )
        return GmxV2DataClient(
            loop=loop,
            gmx_api=gmx_api,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
