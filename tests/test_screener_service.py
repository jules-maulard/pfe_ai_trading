import pandas as pd
from unittest.mock import MagicMock

from src.mcp_servers.screener_service import ScreenerService


def _make_ohlcv(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _service(ohlcv_df: pd.DataFrame) -> ScreenerService:
    storage = MagicMock()
    storage.load_ohlcv.return_value = ohlcv_df
    return ScreenerService(storage=storage)


def _rows(symbol: str, volumes: list[float], closes: list[float]) -> list[dict]:
    return [
        {"symbol": symbol, "date": f"2024-01-{i+1:02d}", "volume": v, "close": c}
        for i, (v, c) in enumerate(zip(volumes, closes))
    ]


class TestGetVolumeAnomalies:
    def test_detects_symbol_with_spike(self):
        normal = [1_000_000.0] * 21
        spike = normal[:-1] + [10_000_000.0]
        df = _make_ohlcv(
            _rows("AAPL", spike, [100.0] * 21)
            + _rows("MSFT", normal, [200.0] * 21)
        )
        svc = _service(df)
        result = svc.get_volume_anomalies(limit=5, window=20, multiplier=2.0)
        assert "AAPL" in result
        assert "MSFT" not in result

    def test_respects_limit(self):
        rows = []
        for sym in ["A", "B", "C", "D"]:
            vols = [1_000_000.0] * 20 + [50_000_000.0]
            rows += _rows(sym, vols, [100.0] * 21)
        svc = _service(_make_ohlcv(rows))
        result = svc.get_volume_anomalies(limit=2, window=20)
        assert len(result) <= 2

    def test_skips_symbol_with_insufficient_data(self):
        df = _make_ohlcv(_rows("TINY", [1_000_000.0] * 5, [100.0] * 5))
        svc = _service(df)
        result = svc.get_volume_anomalies(window=20)
        assert "TINY" not in result

    def test_returns_sorted_list(self):
        rows = (
            _rows("ZZYX", [1_000_000.0] * 20 + [50_000_000.0], [100.0] * 21)
            + _rows("AAPL", [1_000_000.0] * 20 + [50_000_000.0], [100.0] * 21)
        )
        svc = _service(_make_ohlcv(rows))
        result = svc.get_volume_anomalies(limit=10, window=20)
        assert result == sorted(result)

    def test_no_anomaly_returns_empty_list(self):
        df = _make_ohlcv(_rows("AAPL", [1_000_000.0] * 25, [100.0] * 25))
        svc = _service(df)
        result = svc.get_volume_anomalies(window=20, multiplier=100.0)
        assert result == []


class TestGetTopMovers:
    def test_top_gainers_identified(self):
        rows = (
            _rows("AAPL", [1e6] * 3, [100.0, 100.0, 120.0])   # +20%
            + _rows("MSFT", [1e6] * 3, [100.0, 100.0, 101.0])  # +1%
            + _rows("GOOG", [1e6] * 3, [100.0, 100.0, 80.0])   # -20%
        )
        svc = _service(_make_ohlcv(rows))
        result = svc.get_top_movers(limit=1)
        assert "AAPL" in result["top_gainers"]

    def test_top_losers_identified(self):
        rows = (
            _rows("AAPL", [1e6] * 3, [100.0, 100.0, 120.0])
            + _rows("MSFT", [1e6] * 3, [100.0, 100.0, 101.0])
            + _rows("GOOG", [1e6] * 3, [100.0, 100.0, 80.0])   # biggest loser
        )
        svc = _service(_make_ohlcv(rows))
        result = svc.get_top_movers(limit=1)
        assert "GOOG" in result["top_losers"]

    def test_result_has_both_keys(self):
        rows = _rows("AAPL", [1e6] * 3, [100.0, 100.0, 105.0])
        svc = _service(_make_ohlcv(rows))
        result = svc.get_top_movers()
        assert "top_gainers" in result
        assert "top_losers" in result

    def test_skips_symbol_with_only_one_row(self):
        rows = (
            [{"symbol": "SOLO", "date": "2024-01-01", "volume": 1e6, "close": 100.0}]
            + _rows("MSFT", [1e6] * 3, [100.0, 100.0, 102.0])
        )
        svc = _service(_make_ohlcv(rows))
        result = svc.get_top_movers(limit=5)
        all_movers = result["top_gainers"] + result["top_losers"]
        assert "SOLO" not in all_movers

    def test_respects_limit(self):
        rows = []
        for i, sym in enumerate(["A", "B", "C", "D", "E"]):
            rows += _rows(sym, [1e6] * 3, [100.0, 100.0, 100.0 + i * 5])
        svc = _service(_make_ohlcv(rows))
        result = svc.get_top_movers(limit=2)
        assert len(result["top_gainers"]) <= 2
        assert len(result["top_losers"]) <= 2
