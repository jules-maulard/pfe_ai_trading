import pandas as pd
import pytest
from unittest.mock import MagicMock

from src.mcp_servers.macd_service import MACDService


def _make_macd_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _service(macd_df: pd.DataFrame) -> MACDService:
    storage = MagicMock()
    storage.load_indicator.return_value = macd_df
    return MACDService(storage=storage)


class TestDetectCrossovers:
    def _base_row(self, symbol, date, macd, signal) -> dict:
        return {
            "symbol": symbol,
            "date": date,
            "macd": macd,
            "macd_signal": signal,
            "macd_hist": round(macd - signal, 6),
        }

    def test_bullish_signal_crossover_detected(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", -0.5, 0.0),   # hist < 0
            self._base_row("AAPL", "2024-01-02", 0.5, 0.0),    # hist > 0 → bullish crossover
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        types = [e["type"] for e in result["sample"]]
        assert "bullish_signal_crossover" in types

    def test_bearish_signal_crossover_detected(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", 0.5, 0.0),    # hist > 0
            self._base_row("AAPL", "2024-01-02", -0.5, 0.0),   # hist < 0 → bearish crossover
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        types = [e["type"] for e in result["sample"]]
        assert "bearish_signal_crossover" in types

    def test_bullish_zero_crossover_detected(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", -0.1, -0.2),  # macd < 0
            self._base_row("AAPL", "2024-01-02", 0.1, 0.0),    # macd > 0 → zero crossover
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        types = [e["type"] for e in result["sample"]]
        assert "bullish_zero_crossover" in types

    def test_bearish_zero_crossover_detected(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", 0.1, 0.2),    # macd > 0
            self._base_row("AAPL", "2024-01-02", -0.1, 0.0),   # macd < 0 → zero crossover
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        types = [e["type"] for e in result["sample"]]
        assert "bearish_zero_crossover" in types

    def test_no_crossover_on_same_sign(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", 0.3, 0.1),   # hist > 0
            self._base_row("AAPL", "2024-01-02", 0.5, 0.2),   # hist > 0, no change
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        signal_types = [e["type"] for e in result["sample"] if "signal" in e["type"]]
        assert signal_types == []

    def test_result_has_status_ok(self):
        rows = [self._base_row("AAPL", "2024-01-01", 0.1, 0.0)]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers()
        assert result["status"] == "ok"

    def test_total_crossovers_count_matches_sample(self):
        rows = [
            self._base_row("AAPL", "2024-01-01", -0.5, 0.0),
            self._base_row("AAPL", "2024-01-02", 0.5, 0.0),
        ]
        svc = _service(_make_macd_df(rows))
        result = svc.detect_crossovers(sample_rows=100)
        assert result["total_crossovers"] == len(result["sample"])

    def test_empty_dataframe_returns_zero_crossovers(self):
        svc = _service(pd.DataFrame(columns=["symbol", "date", "macd", "macd_signal", "macd_hist"]))
        result = svc.detect_crossovers()
        assert result["total_crossovers"] == 0
        assert result["sample"] == []
