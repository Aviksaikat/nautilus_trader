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
"""Tests for GMX V2 data client helper functions."""

import pytest

# NOTE: Tests require the nautilus_trader package to be installed (compiled extensions).
# Skip all tests if the package is not available.
pytest.importorskip("nautilus_trader.model.enums")

from nautilus_trader.adapters.gmx_v2.data import _bar_type_to_gmx_period
from nautilus_trader.adapters.gmx_v2.data import _parse_candlestick_response
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType


class MockBarSpec:
    def __init__(self, step, aggregation):
        self.step = step
        self.aggregation = aggregation


class MockBarType:
    def __init__(self, step, aggregation):
        self.spec = MockBarSpec(step, aggregation)


@pytest.mark.parametrize("step,agg,expected", [
    (1, BarAggregation.MINUTE, "1m"),
    (5, BarAggregation.MINUTE, "5m"),
    (15, BarAggregation.MINUTE, "15m"),
    (30, BarAggregation.MINUTE, "30m"),
    (1, BarAggregation.HOUR, "1h"),
    (4, BarAggregation.HOUR, "4h"),
    (1, BarAggregation.DAY, "1d"),
])
def test_bar_type_to_gmx_period_supported(step, agg, expected):
    """_bar_type_to_gmx_period maps known BarType specs to GMX period strings."""
    bar_type = MockBarType(step, agg)
    assert _bar_type_to_gmx_period(bar_type) == expected


@pytest.mark.parametrize("step,agg", [
    (2, BarAggregation.MINUTE),
    (1, BarAggregation.WEEK),
])
def test_bar_type_to_gmx_period_unsupported_returns_none(step, agg):
    """_bar_type_to_gmx_period returns None for unsupported bar types."""
    bar_type = MockBarType(step, agg)
    assert _bar_type_to_gmx_period(bar_type) is None
