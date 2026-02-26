from __future__ import annotations

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from typing import Any, Dict, List, Optional

from data.duckdb_csv_storage import DuckDbCsvStorage


class MACDService:
    def __init__(self, storage: DuckDbCsvStorage = None):
        self.storage = storage or DuckDbCsvStorage()

    def compute(
        self,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
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

        out = self.compute_macd(df, price_col=price_col, fast=fast, slow=slow, signal=signal)

        result = (
            out[["symbol", "date", "macd", "macd_signal", "macd_hist"]]
            .dropna(subset=["macd", "macd_signal", "macd_hist"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "columns": ["symbol", "date", "macd", "macd_signal", "macd_hist"],
            "sample": sample,
        }

    @staticmethod
    def compute_macd(
        df: pd.DataFrame,
        price_col: str = "close",
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
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
            ema_fast = g[price_col].ewm(span=fast, adjust=False, min_periods=fast).mean()
            ema_slow = g[price_col].ewm(span=slow, adjust=False, min_periods=slow).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
            g["macd"] = macd_line
            g["macd_signal"] = signal_line
            g["macd_hist"] = macd_line - signal_line
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
