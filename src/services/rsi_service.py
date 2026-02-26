from __future__ import annotations

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from typing import Any, Dict, List, Optional

from data.duckdb_csv_storage import DuckDbCsvStorage
from mcp_servers.ta_indicators import compute_rsi_wilder


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
        save: bool = False,
        save_path: str = "data/indicators/rsi14.csv",
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        df = self.storage.load_prices(symbols=symbols, start=start, end=end)

        needed = {"symbol", "date", price_col}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        out = compute_rsi_wilder(df, price_col=price_col, window=window)
        rsi_col = f"rsi{window}"

        result = (
            out[["symbol", "date", rsi_col]]
            .dropna(subset=[rsi_col])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        saved_to = None
        if save:
            saved_to = self.storage.save_indicator(result, save_path)

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "saved_to": saved_to,
            "columns": ["symbol", "date", rsi_col],
            "sample": sample,
        }

    @staticmethod
    def _make_sample(df: pd.DataFrame, n: int) -> list:
        if n <= 0 or df.empty:
            return []
        tail = df.tail(n).copy()
        if pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        return tail.to_dict(orient="records")
