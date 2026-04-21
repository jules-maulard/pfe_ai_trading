import pandas as pd
import pytest

from src.mcp_servers.pivot_service import PivotService


def _make_ohlcv(n: int = 10, symbol: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": symbol,
            "date": pd.date_range("2024-01-01", periods=n, freq="D"),
            "high": [110.0 + i for i in range(n)],
            "low": [90.0 + i for i in range(n)],
            "close": [100.0 + i for i in range(n)],
        }
    )


class TestComputePivots:
    def test_output_has_all_pivot_columns(self):
        df = _make_ohlcv()
        result = PivotService.compute_pivots(df)
        for col in ("pivot", "r1", "s1", "r2", "s2", "r3", "s3"):
            assert col in result.columns

    def test_first_row_pivot_is_nan(self):
        df = _make_ohlcv()
        result = PivotService.compute_pivots(df)
        assert pd.isna(result.iloc[0]["pivot"])

    def test_pivot_formula_on_second_row(self):
        df = _make_ohlcv()
        result = PivotService.compute_pivots(df)
        row1 = result.iloc[1]
        expected_p = (df.iloc[0]["high"] + df.iloc[0]["low"] + df.iloc[0]["close"]) / 3
        assert abs(row1["pivot"] - expected_p) < 1e-9

    def test_r1_formula(self):
        df = _make_ohlcv()
        result = PivotService.compute_pivots(df)
        row1 = result.iloc[1]
        p = row1["pivot"]
        expected_r1 = 2 * p - df.iloc[0]["low"]
        assert abs(row1["r1"] - expected_r1) < 1e-9

    def test_s1_formula(self):
        df = _make_ohlcv()
        result = PivotService.compute_pivots(df)
        row1 = result.iloc[1]
        p = row1["pivot"]
        expected_s1 = 2 * p - df.iloc[0]["high"]
        assert abs(row1["s1"] - expected_s1) < 1e-9

    def test_raises_on_missing_columns(self):
        df = _make_ohlcv().drop(columns=["high"])
        with pytest.raises(ValueError, match="Missing required columns"):
            PivotService.compute_pivots(df)

    def test_row_count_unchanged(self):
        df = _make_ohlcv(n=20)
        result = PivotService.compute_pivots(df)
        assert len(result) == len(df)

    def test_multi_symbol_independence(self):
        aapl = _make_ohlcv(n=5, symbol="AAPL")
        msft = _make_ohlcv(n=5, symbol="MSFT")
        msft[["high", "low", "close"]] = msft[["high", "low", "close"]] * 2
        combined = pd.concat([aapl, msft], ignore_index=True)

        result = PivotService.compute_pivots(combined)

        aapl_pivots = result[result["symbol"] == "AAPL"]["pivot"].dropna()
        msft_pivots = result[result["symbol"] == "MSFT"]["pivot"].dropna()
        assert not aapl_pivots.reset_index(drop=True).equals(msft_pivots.reset_index(drop=True))
