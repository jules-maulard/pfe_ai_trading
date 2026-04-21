import pandas as pd
from unittest.mock import MagicMock

from src.mcp_servers.pivot_service import PivotService


def _make_ohlcv(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _make_pivot_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _service(ohlcv_df: pd.DataFrame, pivot_df: pd.DataFrame) -> PivotService:
    storage = MagicMock()
    storage.load_ohlcv.return_value = ohlcv_df
    storage.load_indicator.return_value = pivot_df
    return PivotService(storage=storage)


def _row(symbol, date, close, pivot, r1=None, r2=None, r3=None, s1=None, s2=None, s3=None):
    return {
        "symbol": symbol, "date": date, "open": close, "high": close, "low": close,
        "close": close, "volume": 1e6,
        "pivot": pivot,
        "r1": r1 or pivot * 1.02, "r2": r2 or pivot * 1.04, "r3": r3 or pivot * 1.06,
        "s1": s1 or pivot * 0.98, "s2": s2 or pivot * 0.96, "s3": s3 or pivot * 0.94,
    }


class TestDetectPivotInteraction:
    def test_detects_close_above_level(self):
        p = 100.0
        # close is 0.3% above pivot → within 0.5% → "above_level"
        close = 100.3
        data = _row("AAPL", "2024-01-02", close, p)
        ohlcv = _make_ohlcv([{k: data[k] for k in ("symbol", "date", "open", "high", "low", "close", "volume")}])
        pivots = _make_pivot_df([{k: data[k] for k in ("symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3")}])
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction(proximity_pct=0.5)
        at_pivot = [e for e in result["sample"] if e["level"] == "pivot"]
        assert len(at_pivot) == 1
        assert at_pivot[0]["interaction"] == "above_level"

    def test_detects_close_below_level(self):
        p = 100.0
        close = 99.7  # 0.3% below pivot
        data = _row("AAPL", "2024-01-02", close, p)
        ohlcv = _make_ohlcv([{k: data[k] for k in ("symbol", "date", "open", "high", "low", "close", "volume")}])
        pivots = _make_pivot_df([{k: data[k] for k in ("symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3")}])
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction(proximity_pct=0.5)
        at_pivot = [e for e in result["sample"] if e["level"] == "pivot"]
        assert len(at_pivot) == 1
        assert at_pivot[0]["interaction"] == "below_level"

    def test_no_interaction_when_far_from_level(self):
        p = 100.0
        close = 105.0  # 5% away → outside 0.5%
        data = _row("AAPL", "2024-01-02", close, p)
        ohlcv = _make_ohlcv([{k: data[k] for k in ("symbol", "date", "open", "high", "low", "close", "volume")}])
        pivots = _make_pivot_df([{k: data[k] for k in ("symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3")}])
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction(proximity_pct=0.5)
        assert result["total_interactions"] == 0

    def test_result_has_status_ok(self):
        data = _row("X", "2024-01-02", 100.0, 100.0)
        ohlcv = _make_ohlcv([{k: data[k] for k in ("symbol", "date", "open", "high", "low", "close", "volume")}])
        pivots = _make_pivot_df([{k: data[k] for k in ("symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3")}])
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction()
        assert result["status"] == "ok"

    def test_event_contains_required_keys(self):
        p = 100.0
        data = _row("AAPL", "2024-01-02", 100.0, p)
        ohlcv = _make_ohlcv([{k: data[k] for k in ("symbol", "date", "open", "high", "low", "close", "volume")}])
        pivots = _make_pivot_df([{k: data[k] for k in ("symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3")}])
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction(proximity_pct=1.0)
        assert len(result["sample"]) > 0
        for key in ("symbol", "date", "level", "level_value", "close", "distance_pct", "interaction"):
            assert key in result["sample"][0]

    def test_empty_data_returns_zero_interactions(self):
        ohlcv_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
        pivot_cols = ["symbol", "date", "pivot", "r1", "r2", "r3", "s1", "s2", "s3"]
        ohlcv = pd.DataFrame(columns=ohlcv_cols)
        pivots = pd.DataFrame(columns=pivot_cols)
        svc = _service(ohlcv, pivots)
        result = svc.detect_pivot_interaction()
        assert result["total_interactions"] == 0
