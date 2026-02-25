from __future__ import annotations

import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from typing import Any, Dict, List, Optional

from data.duckdb_csv_storage import DuckDbCsvStorage
from mcp_servers.ta_indicators import compute_macd


class MACDService:
    def __init__(self, storage: DuckDbCsvStorage = None):
        self.storage = storage or DuckDbCsvStorage()

    def compute(
        self,
        data_path: str,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
        price_col: str = "close",
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        save: bool = False,
        save_path: str = "data/indicators/macd.csv",
        partition_by_symbol: bool = False,
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        df = self.storage.load_prices(data_path, symbols=symbols, start=start, end=end)

        needed = {"symbol", "date", price_col}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        out = compute_macd(df, price_col=price_col, fast=fast, slow=slow, signal=signal)

        result = (
            out[["symbol", "date", "macd", "macd_signal", "macd_hist"]]
            .dropna(subset=["macd", "macd_signal", "macd_hist"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        saved_to = None
        if save:
            saved_to = self.storage.save_indicator(
                result, save_path, partition_by_symbol=partition_by_symbol
            )

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "saved_to": saved_to,
            "columns": ["symbol", "date", "macd", "macd_signal", "macd_hist"],
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
