import pandas as pd
import pytest

from src.mcp_servers.macd_service import MACDService


def _make_ohlcv(n: int = 60, symbol: str = "AAPL") -> pd.DataFrame:
    prices = [100.0 + i * 0.3 + (i % 7) * -1.2 for i in range(n)]
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.date_range("2024-01-01", periods=n, freq="D"),
            "close": prices,
        }
    )


class TestComputeMacd:
    def test_output_has_required_columns(self):
        df = _make_ohlcv()
        result = MACDService.compute_macd(df)
        for col in ("macd", "macd_signal", "macd_hist"):
            assert col in result.columns

    def test_hist_equals_macd_minus_signal(self):
        df = _make_ohlcv()
        result = MACDService.compute_macd(df)
        valid = result.dropna(subset=["macd", "macd_signal", "macd_hist"])
        diff = (valid["macd"] - valid["macd_signal"] - valid["macd_hist"]).abs()
        assert (diff < 1e-9).all()

    def test_original_columns_preserved(self):
        df = _make_ohlcv()
        result = MACDService.compute_macd(df)
        for col in ("symbol", "date", "close"):
            assert col in result.columns

    def test_raises_on_missing_symbol_column(self):
        df = _make_ohlcv().drop(columns=["symbol"])
        with pytest.raises(ValueError, match="Missing required columns"):
            MACDService.compute_macd(df)

    def test_raises_on_missing_date_column(self):
        df = _make_ohlcv().drop(columns=["date"])
        with pytest.raises(ValueError, match="Missing required columns"):
            MACDService.compute_macd(df)

    def test_raises_on_missing_price_column(self):
        df = _make_ohlcv()
        with pytest.raises(ValueError, match="Missing required columns"):
            MACDService.compute_macd(df, price_col="adj_close")

    def test_multi_symbol_produces_separate_rows(self):
        aapl = _make_ohlcv(n=60, symbol="AAPL")
        msft = _make_ohlcv(n=60, symbol="MSFT")
        msft["close"] = msft["close"] * 3
        combined = pd.concat([aapl, msft], ignore_index=True)

        result = MACDService.compute_macd(combined)

        assert set(result["symbol"].unique()) == {"AAPL", "MSFT"}
        assert len(result) == len(combined)

    def test_custom_fast_slow_signal(self):
        df = _make_ohlcv(n=80)
        result = MACDService.compute_macd(df, fast=5, slow=10, signal=3)
        valid = result.dropna(subset=["macd", "macd_signal", "macd_hist"])
        assert len(valid) > 0

    def test_row_count_unchanged(self):
        df = _make_ohlcv(n=50)
        result = MACDService.compute_macd(df)
        assert len(result) == len(df)
