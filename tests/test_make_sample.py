import pandas as pd
import pytest

from src.mcp_servers.macd_service import MACDService
from src.mcp_servers.rsi_service import RSIService


def _make_df_with_string_dates(n: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": "AAPL",
            "date": [f"2024-01-{i+1:02d}" for i in range(n)],
            "value": [float(i) for i in range(n)],
        }
    )


def _make_df_with_datetime_dates(n: int = 5) -> pd.DataFrame:
    df = _make_df_with_string_dates(n)
    df["date"] = pd.to_datetime(df["date"])
    return df


class TestMakeSampleMacd:
    def test_returns_empty_on_n_zero(self):
        df = _make_df_with_string_dates()
        assert MACDService._make_sample(df, 0) == []

    def test_returns_empty_on_empty_df(self):
        df = pd.DataFrame({"symbol": [], "date": [], "value": []})
        assert MACDService._make_sample(df, 5) == []

    def test_returns_last_n_rows(self):
        df = _make_df_with_string_dates(10)
        result = MACDService._make_sample(df, 3)
        assert len(result) == 3

    def test_returns_all_rows_when_n_larger_than_df(self):
        df = _make_df_with_string_dates(3)
        result = MACDService._make_sample(df, 100)
        assert len(result) == 3

    def test_converts_datetime_date_column_to_string(self):
        df = _make_df_with_datetime_dates(5)
        result = MACDService._make_sample(df, 5)
        for row in result:
            assert isinstance(row["date"], str)

    def test_string_dates_pass_through_unchanged(self):
        df = _make_df_with_string_dates(3)
        result = MACDService._make_sample(df, 3)
        for row in result:
            assert isinstance(row["date"], str)

    def test_returns_list_of_dicts(self):
        df = _make_df_with_string_dates(5)
        result = MACDService._make_sample(df, 3)
        assert isinstance(result, list)
        for row in result:
            assert isinstance(row, dict)


class TestMakeSampleRsi:
    def test_returns_empty_on_n_zero(self):
        df = _make_df_with_string_dates()
        assert RSIService._make_sample(df, 0) == []

    def test_converts_datetime_date_column_to_string(self):
        df = _make_df_with_datetime_dates(5)
        result = RSIService._make_sample(df, 5)
        for row in result:
            assert isinstance(row["date"], str)
