import pandas as pd
from unittest.mock import MagicMock

from src.mcp_servers.rsi_service import RSIService


def _make_rsi_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _service(rsi_df: pd.DataFrame) -> RSIService:
    storage = MagicMock()
    storage.load_indicator.return_value = rsi_df
    return RSIService(storage=storage)


def _row(symbol: str, date: str, rsi: float) -> dict:
    return {"symbol": symbol, "date": date, "rsi": rsi}


class TestDetectExtremes:
    def test_overbought_event_detected(self):
        rows = [
            _row("AAPL", "2024-01-01", 65.0),  # neutral
            _row("AAPL", "2024-01-02", 75.0),  # enters overbought → event
        ]
        svc = _service(_make_rsi_df(rows))
        result = svc.detect_extremes(overbought=70.0, oversold=30.0)
        zones = [e["zone"] for e in result["sample"]]
        assert "overbought" in zones

    def test_oversold_event_detected(self):
        rows = [
            _row("AAPL", "2024-01-01", 50.0),  # neutral
            _row("AAPL", "2024-01-02", 25.0),  # enters oversold → event
        ]
        svc = _service(_make_rsi_df(rows))
        result = svc.detect_extremes(overbought=70.0, oversold=30.0)
        zones = [e["zone"] for e in result["sample"]]
        assert "oversold" in zones

    def test_no_event_when_staying_neutral(self):
        rows = [
            _row("AAPL", "2024-01-01", 50.0),
            _row("AAPL", "2024-01-02", 55.0),
            _row("AAPL", "2024-01-03", 45.0),
        ]
        svc = _service(_make_rsi_df(rows))
        result = svc.detect_extremes()
        assert result["total_events"] == 0

    def test_no_duplicate_event_while_staying_in_zone(self):
        rows = [
            _row("AAPL", "2024-01-01", 50.0),  # neutral
            _row("AAPL", "2024-01-02", 75.0),  # enters overbought → 1 event
            _row("AAPL", "2024-01-03", 80.0),  # stays overbought → no new event
            _row("AAPL", "2024-01-04", 82.0),  # stays overbought → no new event
        ]
        svc = _service(_make_rsi_df(rows))
        result = svc.detect_extremes(overbought=70.0, oversold=30.0)
        overbought_events = [e for e in result["sample"] if e["zone"] == "overbought"]
        assert len(overbought_events) == 1

    def test_result_has_status_ok(self):
        svc = _service(_make_rsi_df([_row("X", "2024-01-01", 50.0)]))
        result = svc.detect_extremes()
        assert result["status"] == "ok"

    def test_result_contains_threshold_info(self):
        svc = _service(_make_rsi_df([_row("X", "2024-01-01", 50.0)]))
        result = svc.detect_extremes(overbought=80.0, oversold=20.0)
        assert result["overbought_threshold"] == 80.0
        assert result["oversold_threshold"] == 20.0

    def test_empty_dataframe_returns_zero_events(self):
        svc = _service(pd.DataFrame(columns=["symbol", "date", "rsi"]))
        result = svc.detect_extremes()
        assert result["total_events"] == 0

    def test_multi_symbol_events_counted_separately(self):
        rows = [
            _row("AAPL", "2024-01-01", 50.0),
            _row("AAPL", "2024-01-02", 75.0),  # AAPL enters overbought
            _row("MSFT", "2024-01-01", 50.0),
            _row("MSFT", "2024-01-02", 25.0),  # MSFT enters oversold
        ]
        svc = _service(_make_rsi_df(rows))
        result = svc.detect_extremes(overbought=70.0, oversold=30.0, sample_rows=100)
        assert result["total_events"] == 2
