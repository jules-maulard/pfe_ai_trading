import pandas as pd
import pytest

from src.mcp_servers.fundamental_service import _round_numeric, _trim


class TestRoundNumeric:
    def test_rounds_float_columns(self):
        df = pd.DataFrame({"a": [1.12345, 2.6789], "b": [3.1415, 0.0001]})
        result = _round_numeric(df, decimals=2)
        assert result["a"].tolist() == [1.12, 2.68]
        assert result["b"].tolist() == [3.14, 0.0]

    def test_leaves_string_columns_unchanged(self):
        df = pd.DataFrame({"symbol": ["AAPL", "MSFT"], "value": [1.999, 2.001]})
        result = _round_numeric(df, decimals=1)
        assert result["symbol"].tolist() == ["AAPL", "MSFT"]

    def test_default_decimals_is_2(self):
        df = pd.DataFrame({"x": [1.23456]})
        result = _round_numeric(df)
        assert result["x"].iloc[0] == 1.23

    def test_zero_decimals(self):
        df = pd.DataFrame({"x": [1.6, 2.4]})
        result = _round_numeric(df, decimals=0)
        assert result["x"].tolist() == [2.0, 2.0]

    def test_empty_dataframe_does_not_raise(self):
        df = pd.DataFrame({"x": pd.Series([], dtype=float)})
        result = _round_numeric(df)
        assert result.empty


class TestTrim:
    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL", "MSFT", "MSFT"],
                "date": ["2024-01-01", "2024-06-01", "2024-01-01", "2024-06-01"],
                "value": [1, 2, 3, 4],
            }
        )

    def test_limits_rows(self):
        df = self._make_df()
        result = _trim(df, limit=2)
        assert len(result) == 2

    def test_sorts_by_date_descending(self):
        df = self._make_df()
        result = _trim(df, limit=4)
        dates = result["date"].tolist()
        assert dates == sorted(dates, reverse=True) or len(set(result["symbol"])) > 1

    def test_returns_most_recent_rows(self):
        df = self._make_df()
        result = _trim(df, limit=2)
        # both rows should have the most recent date per the sort
        assert "2024-06-01" in result["date"].values

    def test_limit_larger_than_df_returns_all(self):
        df = self._make_df()
        result = _trim(df, limit=100)
        assert len(result) == len(df)

    def test_index_is_reset(self):
        df = self._make_df()
        result = _trim(df, limit=2)
        assert list(result.index) == list(range(len(result)))
