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
"""Constants for the GMX V2 adapter."""

from nautilus_trader.model.identifiers import Venue

GMX_V2_VENUE: Venue = Venue("GMX_V2")

# REST API base URLs (Arbitrum)
GMX_V2_BASE_URL_ARBITRUM = "https://arbitrum-api.gmxinfra.io"
GMX_V2_BACKUP_URL_ARBITRUM = "https://arbitrum-api.gmxinfra2.io"

# REST API base URLs (Avalanche)
GMX_V2_BASE_URL_AVALANCHE = "https://avalanche-api.gmxinfra.io"

# Smart contract addresses (Arbitrum)
GMX_V2_DATA_STORE_ARBITRUM = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"
GMX_V2_READER_ARBITRUM = "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789"
GMX_V2_EXCHANGE_ROUTER_ARBITRUM = "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"

# Chain names used by eth_defi
GMX_CHAIN_ARBITRUM = "arbitrum"
GMX_CHAIN_AVALANCHE = "avalanche"

# Price precision for GMX (30 decimal fixed-point internally, displayed in standard form)
GMX_PRICE_PRECISION = 30

# Default poll intervals
DEFAULT_POLL_INTERVAL_PRICES_SECS = 2.0
DEFAULT_POLL_INTERVAL_MARKETS_SECS = 10.0
DEFAULT_UPDATE_INSTRUMENTS_INTERVAL_MINS = 60
