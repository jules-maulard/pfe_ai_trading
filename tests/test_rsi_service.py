import pandas as pd
import pytest

from src.mcp_servers.rsi_service import RSIService


def _make_ohlcv(n: int = 50, symbol: str = "AAPL") -> pd.DataFrame:
    prices = [100.0 + i * 0.5 + (i % 5) * -1.5 for i in range(n)]
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.date_range("2024-01-01", periods=n, freq="D"),
            "close": prices,
        }
    )


class TestComputeRsiWilder:
    def test_output_has_rsi_column(self):
        df = _make_ohlcv()
        result = RSIService.compute_rsi_wilder(df)
        assert "rsi14" in result.columns

    def test_rsi_values_in_valid_range(self):
        df = _make_ohlcv(n=60)
        result = RSIService.compute_rsi_wilder(df)
        valid = result["rsi14"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_initial_rows_are_nan(self):
        df = _make_ohlcv(n=30)
        result = RSIService.compute_rsi_wilder(df)
        assert result["rsi14"].iloc[:14].isna().all()

    def test_raises_on_missing_symbol_column(self):
        df = _make_ohlcv().drop(columns=["symbol"])
        with pytest.raises(ValueError, match="Missing required columns"):
            RSIService.compute_rsi_wilder(df)

    def test_raises_on_missing_date_column(self):
        df = _make_ohlcv().drop(columns=["date"])
        with pytest.raises(ValueError, match="Missing required columns"):
            RSIService.compute_rsi_wilder(df)

    def test_raises_on_missing_price_column(self):
        df = _make_ohlcv()
        with pytest.raises(ValueError, match="Missing required columns"):
            RSIService.compute_rsi_wilder(df, price_col="adj_close")

    def test_custom_window(self):
        df = _make_ohlcv(n=60)
        result = RSIService.compute_rsi_wilder(df, window=7)
        assert "rsi7" in result.columns
        assert result["rsi7"].iloc[:7].isna().all()

    def test_multi_symbol_isolation(self):
        aapl = _make_ohlcv(n=40, symbol="AAPL")
        msft = _make_ohlcv(n=40, symbol="MSFT")
        msft["close"] = msft["close"] * 2
        combined = pd.concat([aapl, msft], ignore_index=True)

        result = RSIService.compute_rsi_wilder(combined)

        aapl_rsi = result[result["symbol"] == "AAPL"]["rsi14"].dropna()
        msft_rsi = result[result["symbol"] == "MSFT"]["rsi14"].dropna()
        assert len(aapl_rsi) > 0 and len(msft_rsi) > 0
        assert not aapl_rsi.equals(msft_rsi)

    def test_flat_price_series_returns_na(self):
        df = pd.DataFrame(
            {
                "symbol": "FLAT",
                "date": pd.date_range("2024-01-01", periods=20, freq="D"),
                "close": [100.0] * 20,
            }
        )
        result = RSIService.compute_rsi_wilder(df)
        assert result["rsi14"].dropna().empty or (result["rsi14"].dropna() == 100.0).all()
