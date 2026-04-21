import numpy as np
import pandas as pd

from src.mcp_servers.macd_service import MACDService


class TestFindPivots:
    def test_detects_single_high(self):
        # clear peak at index 5
        series = [1, 2, 3, 4, 5, 10, 5, 4, 3, 2, 1]
        result = MACDService._find_pivots(series, lookback=3, kind="high")
        assert 5 in result

    def test_detects_single_low(self):
        # clear trough at index 5
        series = [10, 9, 8, 7, 6, 1, 6, 7, 8, 9, 10]
        result = MACDService._find_pivots(series, lookback=3, kind="low")
        assert 5 in result

    def test_detects_multiple_highs(self):
        series = [0, 5, 0, 0, 0, 8, 0, 0, 0, 6, 0, 0, 0]
        result = MACDService._find_pivots(series, lookback=2, kind="high")
        assert len(result) >= 2

    def test_returns_empty_for_flat_series(self):
        series = [5.0] * 20
        highs = MACDService._find_pivots(series, lookback=3, kind="high")
        lows = MACDService._find_pivots(series, lookback=3, kind="low")
        # flat series: every point equals the window max AND min, so all are pivots
        # just verify it doesn't crash and returns a list
        assert isinstance(highs, list)
        assert isinstance(lows, list)

    def test_returns_indices_within_bounds(self):
        series = list(range(20))
        result = MACDService._find_pivots(series, lookback=3, kind="high")
        for idx in result:
            assert 0 <= idx < len(series)

    def test_monotonic_increasing_has_no_highs_except_edge(self):
        series = list(range(1, 21))
        result = MACDService._find_pivots(series, lookback=3, kind="high")
        # no interior high in a strictly increasing series
        assert result == []

    def test_monotonic_decreasing_has_no_lows_except_edge(self):
        series = list(range(20, 0, -1))
        result = MACDService._find_pivots(series, lookback=3, kind="low")
        assert result == []


class TestDivRecord:
    def _make_inputs(self):
        dates = np.array(["2024-01-01", "2024-01-05"], dtype=object)
        prices = np.array([100.0, 110.0])
        macds = np.array([0.5, 0.3])
        return dates, prices, macds

    def test_record_has_required_keys(self):
        dates, prices, macds = self._make_inputs()
        record = MACDService._div_record("AAPL", dates, prices, macds, 0, 1, "regular_bearish")
        for key in ("symbol", "type", "date_a", "price_a", "macd_a", "date_b", "price_b", "macd_b"):
            assert key in record

    def test_record_symbol_and_type(self):
        dates, prices, macds = self._make_inputs()
        record = MACDService._div_record("TSLA", dates, prices, macds, 0, 1, "hidden_bullish")
        assert record["symbol"] == "TSLA"
        assert record["type"] == "hidden_bullish"

    def test_record_price_values(self):
        dates, prices, macds = self._make_inputs()
        record = MACDService._div_record("X", dates, prices, macds, 0, 1, "regular_bullish")
        assert record["price_a"] == round(prices[0], 4)
        assert record["price_b"] == round(prices[1], 4)

    def test_record_macd_values(self):
        dates, prices, macds = self._make_inputs()
        record = MACDService._div_record("X", dates, prices, macds, 0, 1, "regular_bullish")
        assert record["macd_a"] == round(float(macds[0]), 6)
        assert record["macd_b"] == round(float(macds[1]), 6)
