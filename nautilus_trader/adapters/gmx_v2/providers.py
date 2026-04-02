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
"""Instrument provider for the GMX V2 venue."""

import asyncio

from eth_defi.gmx.api import GMXAPI

from nautilus_trader.adapters.gmx_v2.common.parsing import parse_instrument
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig


class GmxV2InstrumentProvider(InstrumentProvider):
    """
    Provides Nautilus instrument definitions from GMX V2.

    Uses eth_defi ``GMXAPI`` to fetch market listings from the GMX REST API.

    Parameters
    ----------
    gmx_api : GMXAPI
        The GMX API client (from eth_defi).
    config : InstrumentProviderConfig, optional
        The instrument provider configuration, by default None.

    """

    def __init__(
        self,
        gmx_api: GMXAPI,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config=config)
        self._gmx_api = gmx_api
        self._log_warnings = config.log_warnings if config else True

    async def load_all_async(self, filters: dict | None = None) -> None:
        """
        Load all listed GMX V2 markets as CryptoPerpetual instruments.

        Fetches token metadata and market listings from the GMX API, parses each
        listed market into a ``CryptoPerpetual``, and registers it with the provider.

        :param filters: Unused; reserved for future filtering support.
        """
        filters_str = "..." if not filters else f" with filters {filters}..."
        self._log.info(f"Loading all instruments{filters_str}")

        # Fetch tokens and build address → token info lookup
        tokens_response = await asyncio.to_thread(self._gmx_api.get_tokens)
        tokens_list = tokens_response.get("tokens", [])
        tokens_by_address: dict = {
            token["address"]: token for token in tokens_list if "address" in token
        }

        # Fetch markets info
        markets_response = await asyncio.to_thread(self._gmx_api.get_markets_info)
        markets_list = markets_response.get("markets", [])

        loaded = 0
        skipped = 0

        for market in markets_list:
            market_name = market.get("name", "<unknown>")

            try:
                instrument = parse_instrument(market, tokens_by_address)
            except Exception as exc:
                if self._log_warnings:
                    self._log.warning(
                        f"Failed to parse market '{market_name}': {exc}",
                    )
                skipped += 1
                continue

            if instrument is None:
                # Market not listed or token not found
                skipped += 1
                continue

            self.add(instrument=instrument)
            loaded += 1

        self._log.info(f"Loaded {loaded} instruments ({skipped} skipped)")
