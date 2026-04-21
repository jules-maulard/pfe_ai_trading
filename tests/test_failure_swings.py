import pandas as pd
from unittest.mock import MagicMock

from src.mcp_servers.rsi_service import RSIService


def _service(rsi_df: pd.DataFrame) -> RSIService:
    storage = MagicMock()
    storage.load_indicator.return_value = rsi_df
    return RSIService(storage=storage)


def _make_bullish_pattern() -> pd.DataFrame:
    """
    Construct an RSI series containing a textbook bullish failure swing:
      low0 < 30 (oversold) → high0 → low1 > 30 → break above high0

    Using lookback=3, peaks/troughs must dominate a 7-point window.
    Layout (40 values):
      idx  0-6  : 50  (neutral warm-up)
      idx  7    : 25  → local LOW (oversold)       = low0
      idx  8-12 : 50  (rises)
      idx  13   : 60  → local HIGH                 = high0
      idx  14-18: 50  (falls)
      idx  19   : 40  → local LOW (above 30)       = low1
      idx  20-24: 50  (rises)
      idx  25   : 65  → break above high0 (60)     = trigger
      idx  26-39: 50  (padding)
    """
    n = 40
    rsi = [50.0] * n
    rsi[7] = 25.0   # low0 — below oversold 30
    rsi[13] = 60.0  # high0
    rsi[19] = 40.0  # low1 — above oversold 30
    rsi[25] = 65.0  # break above high0

    dates = [f"2024-01-{i+1:02d}" for i in range(n)]
    return pd.DataFrame({"symbol": "AAPL", "date": dates, "rsi": rsi})


def _make_bearish_pattern() -> pd.DataFrame:
    """
    Bearish failure swing:
      high0 > 70 (overbought) → low0 → high1 < 70 → break below low0

    Layout (40 values):
      idx  7   : 80  → local HIGH (overbought)     = high0
      idx  13  : 55  → local LOW                   = low0
      idx  19  : 65  → local HIGH (below 70)       = high1
      idx  25  : 50  → break below low0 (55)       = trigger
    """
    n = 40
    rsi = [60.0] * n
    rsi[7] = 80.0   # high0 — above overbought 70
    rsi[13] = 55.0  # low0
    rsi[19] = 65.0  # high1 — below overbought 70
    rsi[25] = 50.0  # break below low0

    dates = [f"2024-01-{i+1:02d}" for i in range(n)]
    return pd.DataFrame({"symbol": "AAPL", "date": dates, "rsi": rsi})


class TestDetectFailureSwings:
    def test_bullish_failure_swing_detected(self):
        df = _make_bullish_pattern()
        svc = _service(df)
        result = svc.detect_failure_swings(overbought=70.0, oversold=30.0, pivot_lookback=3)
        types = [e["type"] for e in result["sample"]]
        assert "bullish_failure_swing" in types

    def test_bearish_failure_swing_detected(self):
        df = _make_bearish_pattern()
        svc = _service(df)
        result = svc.detect_failure_swings(overbought=70.0, oversold=30.0, pivot_lookback=3)
        types = [e["type"] for e in result["sample"]]
        assert "bearish_failure_swing" in types

    def test_no_swing_on_neutral_rsi(self):
        dates = [f"2024-01-{i+1:02d}" for i in range(20)]
        df = pd.DataFrame({"symbol": "X", "date": dates, "rsi": [50.0] * 20})
        svc = _service(df)
        result = svc.detect_failure_swings()
        assert result["total_failure_swings"] == 0

    def test_result_has_status_ok(self):
        dates = [f"2024-01-{i+1:02d}" for i in range(10)]
        df = pd.DataFrame({"symbol": "X", "date": dates, "rsi": [50.0] * 10})
        svc = _service(df)
        result = svc.detect_failure_swings()
        assert result["status"] == "ok"
        assert "total_failure_swings" in result

    def test_bullish_swing_record_has_required_keys(self):
        df = _make_bullish_pattern()
        svc = _service(df)
        result = svc.detect_failure_swings(pivot_lookback=3)
        if result["total_failure_swings"] > 0:
            rec = result["sample"][0]
            for key in ("symbol", "type", "date_trigger", "rsi_at_trigger",
                        "low0_date", "low0_rsi", "high0_date", "high0_rsi",
                        "low1_date", "low1_rsi"):
                assert key in rec

    def test_empty_dataframe_returns_zero_swings(self):
        svc = _service(pd.DataFrame(columns=["symbol", "date", "rsi"]))
        result = svc.detect_failure_swings()
        assert result["total_failure_swings"] == 0
