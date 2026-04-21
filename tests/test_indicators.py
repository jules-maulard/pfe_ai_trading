import pandas as pd
import pytest

from src.data.indicators import INDICATOR_REGISTRY, compute_indicators


def _make_ohlcv(n: int = 60, symbol: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.date_range("2024-01-01", periods=n, freq="D"),
            "open": [100.0 + i * 0.1 for i in range(n)],
            "high": [105.0 + i * 0.1 for i in range(n)],
            "low": [95.0 + i * 0.1 for i in range(n)],
            "close": [100.0 + i * 0.3 + (i % 5) * -0.8 for i in range(n)],
            "volume": [1_000_000.0] * n,
        }
    )


class TestIndicatorRegistry:
    def test_registry_has_rsi(self):
        assert "rsi" in INDICATOR_REGISTRY

    def test_registry_has_macd(self):
        assert "macd" in INDICATOR_REGISTRY

    def test_registry_has_pivot(self):
        assert "pivot" in INDICATOR_REGISTRY

    def test_rsi_columns_spec(self):
        assert INDICATOR_REGISTRY["rsi"].columns == ["rsi"]

    def test_macd_columns_spec(self):
        assert INDICATOR_REGISTRY["macd"].columns == ["macd", "macd_signal", "macd_hist"]

    def test_pivot_columns_spec(self):
        assert INDICATOR_REGISTRY["pivot"].columns == ["pivot", "r1", "s1", "r2", "s2", "r3", "s3"]


class TestComputeIndicators:
    def test_raises_on_unknown_indicator(self):
        df = _make_ohlcv()
        with pytest.raises(ValueError, match="Unknown indicator"):
            compute_indicators(df, ["unknown"])

    def test_rsi_adds_rsi_column(self):
        df = _make_ohlcv()
        result = compute_indicators(df, ["rsi"])
        assert "rsi" in result.columns

    def test_macd_adds_macd_columns(self):
        df = _make_ohlcv()
        result = compute_indicators(df, ["macd"])
        for col in ("macd", "macd_signal", "macd_hist"):
            assert col in result.columns

    def test_pivot_adds_pivot_columns(self):
        df = _make_ohlcv()
        result = compute_indicators(df, ["pivot"])
        for col in ("pivot", "r1", "s1", "r2", "s2", "r3", "s3"):
            assert col in result.columns

    def test_multiple_indicators_at_once(self):
        df = _make_ohlcv()
        result = compute_indicators(df, ["rsi", "macd"])
        assert "rsi" in result.columns
        assert "macd" in result.columns

    def test_output_preserves_symbol_and_date(self):
        df = _make_ohlcv()
        result = compute_indicators(df, ["rsi"])
        assert "symbol" in result.columns
        assert "date" in result.columns
