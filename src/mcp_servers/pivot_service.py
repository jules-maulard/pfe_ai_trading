from __future__ import annotations

import pandas as pd
from typing import Any, Dict, List, Optional

from ..data import BaseStorage
from ..data.config.settings import get_storage
from ..utils import get_logger

logger = get_logger(__name__)


class PivotService:
    def __init__(self, storage: BaseStorage = None):
        self.storage = storage or get_storage()

    @staticmethod
    def compute_pivots(df: pd.DataFrame) -> pd.DataFrame:
        required = {"symbol", "date", "high", "low", "close"}
        if not required.issubset(df.columns):
            raise ValueError(
                f"Missing required columns: {sorted(required - set(df.columns))}"
            )

        out = df.copy().sort_values(["symbol", "date"])
        parts = []
        for _, g in out.groupby("symbol"):
            g = g.copy()
            prev_high = g["high"].shift(1)
            prev_low = g["low"].shift(1)
            prev_close = g["close"].shift(1)

            p = (prev_high + prev_low + prev_close) / 3
            g["pivot"] = p
            g["r1"] = 2 * p - prev_low
            g["s1"] = 2 * p - prev_high
            g["r2"] = p + (prev_high - prev_low)
            g["s2"] = p - (prev_high - prev_low)
            g["r3"] = prev_high + 2 * (p - prev_low)
            g["s3"] = prev_low - 2 * (prev_high - p)
            parts.append(g)
        return pd.concat(parts, ignore_index=True)

    def compute(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        logger.debug("compute_pivots: symbols=%s start=%s end=%s", symbols, start, end)
        df = self.storage.load_indicator("pivot", symbols=symbols, start=start, end=end)

        pivot_cols = ["pivot", "r1", "s1", "r2", "s2", "r3", "s3"]
        result = (
            df[["symbol", "date"] + pivot_cols]
            .dropna(subset=["pivot"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "columns": ["symbol", "date"] + pivot_cols,
            "sample": sample,
        }

    def detect_pivot_interaction(
        self,
        proximity_pct: float = 0.5,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        logger.debug(
            "detect_pivot_interaction: symbols=%s proximity=%.2f%% start=%s end=%s",
            symbols, proximity_pct, start, end,
        )
        ohlcv_df = self.storage.load_ohlcv(symbols=symbols, start=start, end=end)
        pivot_df = self.storage.load_indicator("pivot", symbols=symbols, start=start, end=end)

        merged = ohlcv_df.merge(pivot_df, on=["symbol", "date"], how="inner")
        merged = (
            merged.dropna(subset=["pivot"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        level_names = ["s3", "s2", "s1", "pivot", "r1", "r2", "r3"]
        events: list[dict] = []

        for _, row in merged.iterrows():
            close = row["close"]
            for level_name in level_names:
                level_value = row[level_name]
                if pd.isna(level_value) or level_value == 0:
                    continue
                distance_pct = abs(close - level_value) / level_value * 100
                if distance_pct <= proximity_pct:
                    interaction_type = "at_level"
                    if close > level_value:
                        interaction_type = "above_level"
                    elif close < level_value:
                        interaction_type = "below_level"
                    events.append({
                        "symbol": str(row["symbol"]),
                        "date": str(row["date"]),
                        "level": level_name,
                        "level_value": round(float(level_value), 4),
                        "close": round(float(close), 4),
                        "distance_pct": round(distance_pct, 4),
                        "interaction": interaction_type,
                    })

        sample = events[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "total_interactions": len(events),
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
