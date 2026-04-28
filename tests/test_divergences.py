import pandas as pd
from unittest.mock import MagicMock

from src.mcp_servers.rsi_service import RSIService
from src.mcp_servers.macd_service import MACDService


# ------------------------------------------------------------------ #
#  Shared helpers                                                      #
# ------------------------------------------------------------------ #

def _dates(n: int) -> list[str]:
    return [f"2024-01-{i+1:02d}" for i in range(n)]


def _rsi_service(ohlcv_df: pd.DataFrame, rsi_df: pd.DataFrame) -> RSIService:
    storage = MagicMock()
    storage.load_ohlcv.return_value = ohlcv_df
    storage.load_indicator.return_value = rsi_df
    return RSIService(storage=storage)


def _macd_service(ohlcv_df: pd.DataFrame, macd_df: pd.DataFrame) -> MACDService:
    storage = MagicMock()
    storage.load_ohlcv.return_value = ohlcv_df
    storage.load_indicator.return_value = macd_df
    return MACDService(storage=storage)


# ------------------------------------------------------------------ #
#  RSI divergences                                                     #
# ------------------------------------------------------------------ #

class TestRSIFindDivergences:
    def _make_regular_bullish(self):
        """
        Price makes lower low; RSI makes higher low → regular bullish divergence.

        Use a zigzag so that only the two intended troughs are pivot lows
        (a flat baseline creates spurious lows at every flat point).

        Layout (n=13, lookback=3):
          idx 3 : price=80, rsi=25  → trough 1
          idx 7 : price=70, rsi=30  → trough 2 (lower price, higher RSI)
          all other positions: price=110, rsi=50
        """
        n = 13
        dates = _dates(n)
        prices = [110.0] * n
        rsi_vals = [50.0] * n
        prices[3] = 80.0;  rsi_vals[3] = 25.0
        prices[7] = 70.0;  rsi_vals[7] = 30.0

        ohlcv = pd.DataFrame({"symbol": "AAPL", "date": dates, "close": prices, "open": prices, "high": prices, "low": prices, "volume": 1e6})
        rsi_df = pd.DataFrame({"symbol": "AAPL", "date": dates, "rsi": rsi_vals})
        return ohlcv, rsi_df

    def test_regular_bullish_detected(self):
        ohlcv, rsi_df = self._make_regular_bullish()
        svc = _rsi_service(ohlcv, rsi_df)
        result = svc.find_divergences(pivot_lookback=3)
        types = [e["type"] for e in result["sample"]]
        assert "regular_bullish" in types

    def test_no_divergence_when_price_and_rsi_aligned(self):
        # Both troughs going lower in both price AND rsi → no regular bullish divergence.
        # zigzag baseline to avoid spurious pivot lows.
        n = 13
        dates = _dates(n)
        prices = [110.0] * n
        rsi_vals = [50.0] * n
        prices[3] = 85.0;  rsi_vals[3] = 30.0  # trough 1
        prices[7] = 75.0;  rsi_vals[7] = 25.0  # trough 2: lower price AND lower rsi

        ohlcv = pd.DataFrame({"symbol": "AAPL", "date": dates, "close": prices, "open": prices, "high": prices, "low": prices, "volume": 1e6})
        rsi_df = pd.DataFrame({"symbol": "AAPL", "date": dates, "rsi": rsi_vals})
        svc = _rsi_service(ohlcv, rsi_df)
        result = svc.find_divergences(pivot_lookback=3)
        bullish = [e for e in result["sample"] if e["type"] == "regular_bullish"]
        assert bullish == []

    def test_result_has_status_ok(self):
        n = 10
        dates = _dates(n)
        df = pd.DataFrame({"symbol": "X", "date": dates, "close": 100.0, "open": 100.0, "high": 100.0, "low": 100.0, "volume": 1e6})
        rsi = pd.DataFrame({"symbol": "X", "date": dates, "rsi": 50.0})
        svc = _rsi_service(df, rsi)
        result = svc.find_divergences()
        assert result["status"] == "ok"

    def test_divergence_record_has_required_keys(self):
        ohlcv, rsi_df = self._make_regular_bullish()
        svc = _rsi_service(ohlcv, rsi_df)
        result = svc.find_divergences(pivot_lookback=3)
        if len(result["sample"]) > 0:
            rec = result["sample"][0]
            for key in ("symbol", "type", "date_a", "price_a", "rsi_a", "date_b", "price_b", "rsi_b"):
                assert key in rec


# ------------------------------------------------------------------ #
#  MACD divergences                                                    #
# ------------------------------------------------------------------ #

class TestMACDFindDivergences:
    def _make_regular_bearish(self):
        """
        Price makes higher high; MACD makes lower high → regular bearish divergence.

        Layout (n=13, lookback=3):
          idx 3 : price=80,  macd=0.8  → peak 1
          idx 7 : price=90,  macd=0.6  → peak 2 (higher price, lower MACD)
          all other positions: price=60, macd=0.0
        """
        n = 13
        dates = _dates(n)
        prices = [60.0] * n
        macd_vals = [0.0] * n
        prices[3] = 80.0;  macd_vals[3] = 0.8
        prices[7] = 90.0;  macd_vals[7] = 0.6

        ohlcv = pd.DataFrame({"symbol": "AAPL", "date": dates, "close": prices, "open": prices, "high": prices, "low": prices, "volume": 1e6})
        macd_df = pd.DataFrame({"symbol": "AAPL", "date": dates, "macd": macd_vals, "macd_signal": [0.0]*n, "macd_hist": macd_vals})
        return ohlcv, macd_df

    def test_regular_bearish_detected(self):
        ohlcv, macd_df = self._make_regular_bearish()
        svc = _macd_service(ohlcv, macd_df)
        result = svc.find_divergences(pivot_lookback=3)
        types = [e["type"] for e in result["sample"]]
        assert "regular_bearish" in types

    def test_no_divergence_on_flat_data(self):
        n = 20
        dates = _dates(n)
        df = pd.DataFrame({"symbol": "FLAT", "date": dates, "close": 100.0, "open": 100.0, "high": 100.0, "low": 100.0, "volume": 1e6})
        macd_df = pd.DataFrame({"symbol": "FLAT", "date": dates, "macd": 0.0, "macd_signal": 0.0, "macd_hist": 0.0})
        svc = _macd_service(df, macd_df)
        result = svc.find_divergences()
        assert result["sample"] == []

    def test_result_has_status_ok(self):
        n = 10
        dates = _dates(n)
        df = pd.DataFrame({"symbol": "X", "date": dates, "close": 100.0, "open": 100.0, "high": 100.0, "low": 100.0, "volume": 1e6})
        macd_df = pd.DataFrame({"symbol": "X", "date": dates, "macd": 0.0, "macd_signal": 0.0, "macd_hist": 0.0})
        svc = _macd_service(df, macd_df)
        result = svc.find_divergences()
        assert result["status"] == "ok"
