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
"""Tests for GMX V2 configuration."""

import pytest

# NOTE: Tests require the nautilus_trader package to be installed (compiled extensions).
# Skip all tests if the package is not available.
pytest.importorskip("nautilus_trader.config")

from nautilus_trader.adapters.gmx_v2.common.constants import GMX_CHAIN_ARBITRUM
from nautilus_trader.adapters.gmx_v2.config import GmxV2DataClientConfig


def test_default_config():
    """GmxV2DataClientConfig has correct defaults."""
    config = GmxV2DataClientConfig()
    assert config.chain == GMX_CHAIN_ARBITRUM
    assert config.base_url_http is None
    assert config.arbitrum_rpc_url is None
    assert config.poll_interval_prices_secs == 2.0
    assert config.poll_interval_markets_secs == 10.0
    assert config.update_instruments_interval_mins == 60
    assert config.max_retries == 3
    assert config.retry_delay_secs == 1.0


def test_config_is_frozen():
    """GmxV2DataClientConfig is immutable (frozen=True)."""
    config = GmxV2DataClientConfig()
    with pytest.raises((TypeError, AttributeError)):
        config.chain = "avalanche"  # type: ignore


def test_config_custom_values():
    """GmxV2DataClientConfig accepts custom values."""
    config = GmxV2DataClientConfig(
        chain="avalanche",
        poll_interval_prices_secs=5.0,
    )
    assert config.chain == "avalanche"
    assert config.poll_interval_prices_secs == 5.0
