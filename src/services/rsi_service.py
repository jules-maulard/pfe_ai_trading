from __future__ import annotations

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from typing import Any, Dict, List, Optional

from data.duckdb_csv_storage import DuckDbCsvStorage


class RSIService:
    def __init__(self, storage: DuckDbCsvStorage = None):
        self.storage = storage or DuckDbCsvStorage()

    def compute(
        self,
        window: int = 14,
        price_col: str = "close",
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        df = self.storage.load_prices(symbols=symbols, start=start, end=end)

        needed = {"symbol", "date", price_col}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        out = self.compute_rsi_wilder(df, price_col=price_col, window=window)
        rsi_col = f"rsi{window}"

        result = (
            out[["symbol", "date", rsi_col]]
            .dropna(subset=[rsi_col])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "columns": ["symbol", "date", rsi_col],
            "sample": sample,
        }

    @staticmethod
    def compute_rsi_wilder(
        df: pd.DataFrame, price_col: str = "close", window: int = 14
    ) -> pd.DataFrame:
        required = {"symbol", "date", price_col}
        if not required.issubset(df.columns):
            raise ValueError(
                f"Missing required columns: {sorted(required - set(df.columns))}"
            )

        out = df.copy().sort_values(["symbol", "date"])
        parts = []
        for _, g in out.groupby("symbol"):
            g = g.copy()
            delta = g[price_col].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
            avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            rsi = rsi.where(avg_loss != 0, 100.0)
            rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), pd.NA)
            g[f"rsi{window}"] = rsi
            parts.append(g)
        return pd.concat(parts, ignore_index=True)

    @staticmethod
    def _make_sample(df: pd.DataFrame, n: int) -> list:
        if n <= 0 or df.empty:
            return []
        tail = df.tail(n).copy()
        if pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        return tail.to_dict(orient="records")
