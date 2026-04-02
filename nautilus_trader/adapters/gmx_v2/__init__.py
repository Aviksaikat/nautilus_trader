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
"""GMX V2 adapter for NautilusTrader."""

from nautilus_trader.adapters.gmx_v2.config import GmxV2DataClientConfig
from nautilus_trader.adapters.gmx_v2.data import GmxV2DataClient
from nautilus_trader.adapters.gmx_v2.factories import GmxV2LiveDataClientFactory
from nautilus_trader.adapters.gmx_v2.providers import GmxV2InstrumentProvider


__all__ = [
    "GmxV2DataClientConfig",
    "GmxV2DataClient",
    "GmxV2LiveDataClientFactory",
    "GmxV2InstrumentProvider",
]
