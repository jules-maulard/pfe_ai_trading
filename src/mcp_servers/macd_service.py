from __future__ import annotations

import pandas as pd
from typing import Any, Dict, List, Optional

from ..data import BaseStorage
from ..data import CsvStorage
from ..data.config.settings import get_storage
from ..utils import get_logger

logger = get_logger(__name__)


class MACDService:
    def __init__(self, storage: BaseStorage = None):
        self.storage = storage or get_storage()

    def compute(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        logger.debug("compute_macd: symbols=%s start=%s end=%s", symbols, start, end)
        df = self.storage.load_indicator("macd", symbols=symbols, start=start, end=end)

        result = (
            df[["symbol", "date", "macd", "macd_signal", "macd_hist"]]
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

    def detect_crossovers(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        """Detect MACD / signal-line and MACD / zero-line crossovers."""
        logger.debug("detect_crossovers: symbols=%s start=%s end=%s", symbols, start, end)
        macd_df = self.storage.load_indicator("macd", symbols=symbols, start=start, end=end)
        macd_df = (
            macd_df.dropna(subset=["macd", "macd_signal", "macd_hist"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        events: list[dict] = []

        for symbol, g in macd_df.groupby("symbol"):
            g = g.reset_index(drop=True)
            macd_vals = g["macd"].values
            hist_vals = g["macd_hist"].values
            dates = g["date"].values
            n = len(g)

            for i in range(1, n):
                # Signal-line crossover (histogram sign change)
                if hist_vals[i - 1] <= 0 < hist_vals[i]:
                    events.append({
                        "symbol": symbol,
                        "date": str(dates[i]),
                        "type": "bullish_signal_crossover",
                        "macd": round(float(macd_vals[i]), 6),
                        "macd_signal": round(float(macd_vals[i] - hist_vals[i] + hist_vals[i]), 6),
                        "macd_hist": round(float(hist_vals[i]), 6),
                    })
                elif hist_vals[i - 1] >= 0 > hist_vals[i]:
                    events.append({
                        "symbol": symbol,
                        "date": str(dates[i]),
                        "type": "bearish_signal_crossover",
                        "macd": round(float(macd_vals[i]), 6),
                        "macd_signal": round(float(macd_vals[i] - hist_vals[i]), 6),
                        "macd_hist": round(float(hist_vals[i]), 6),
                    })

                # Zero-line crossover
                if macd_vals[i - 1] <= 0 < macd_vals[i]:
                    events.append({
                        "symbol": symbol,
                        "date": str(dates[i]),
                        "type": "bullish_zero_crossover",
                        "macd": round(float(macd_vals[i]), 6),
                    })
                elif macd_vals[i - 1] >= 0 > macd_vals[i]:
                    events.append({
                        "symbol": symbol,
                        "date": str(dates[i]),
                        "type": "bearish_zero_crossover",
                        "macd": round(float(macd_vals[i]), 6),
                    })

        sample = events[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "total_crossovers": len(events),
            "sample": sample,
        }

    def find_divergences(
        self,
        price_col: str = "close",
        pivot_lookback: int = 5,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        """Detect regular and hidden divergences between price and MACD."""
        logger.debug("find_divergences: symbols=%s start=%s end=%s pivot_lookback=%d", symbols, start, end, pivot_lookback)
        ohlcv_df = self.storage.load_ohlcv(symbols=symbols, start=start, end=end)
        macd_df = self.storage.load_indicator("macd", symbols=symbols, start=start, end=end)
        merged = ohlcv_df.merge(macd_df, on=["symbol", "date"], how="inner")
        merged = (
            merged.dropna(subset=["macd"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        divergences: list[dict] = []

        for symbol, g in merged.groupby("symbol"):
            g = g.reset_index(drop=True)
            prices = g[price_col].values
            macds = g["macd"].values
            dates = g["date"].values

            highs = self._find_pivots(prices, pivot_lookback, kind="high")
            lows = self._find_pivots(prices, pivot_lookback, kind="low")

            # Regular bearish: price higher high, MACD lower high
            for j in range(1, len(highs)):
                i0, i1 = highs[j - 1], highs[j]
                if prices[i1] > prices[i0] and macds[i1] < macds[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, macds, i0, i1, "regular_bearish",
                    ))

            # Regular bullish: price lower low, MACD higher low
            for j in range(1, len(lows)):
                i0, i1 = lows[j - 1], lows[j]
                if prices[i1] < prices[i0] and macds[i1] > macds[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, macds, i0, i1, "regular_bullish",
                    ))

            # Hidden bearish: price lower high, MACD higher high
            for j in range(1, len(highs)):
                i0, i1 = highs[j - 1], highs[j]
                if prices[i1] < prices[i0] and macds[i1] > macds[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, macds, i0, i1, "hidden_bearish",
                    ))

            # Hidden bullish: price higher low, MACD lower low
            for j in range(1, len(lows)):
                i0, i1 = lows[j - 1], lows[j]
                if prices[i1] > prices[i0] and macds[i1] < macds[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, macds, i0, i1, "hidden_bullish",
                    ))

        sample = divergences[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "total_divergences": len(divergences),
            "sample": sample,
        }

    @staticmethod
    def _find_pivots(series, lookback: int, kind: str = "high") -> List[int]:
        import numpy as np
        arr = np.asarray(series, dtype=float)
        n = len(arr)
        pivots: list[int] = []
        for i in range(lookback, n - lookback):
            window = arr[i - lookback : i + lookback + 1]
            if kind == "high" and arr[i] == window.max():
                pivots.append(i)
            elif kind == "low" and arr[i] == window.min():
                pivots.append(i)
        return pivots

    @staticmethod
    def _div_record(symbol, dates, prices, macds, i0, i1, div_type) -> dict:
        return {
            "symbol": symbol,
            "type": div_type,
            "date_a": str(dates[i0]),
            "price_a": round(float(prices[i0]), 4),
            "macd_a": round(float(macds[i0]), 6),
            "date_b": str(dates[i1]),
            "price_b": round(float(prices[i1]), 4),
            "macd_b": round(float(macds[i1]), 6),
        }

    @staticmethod
    def _make_sample(df: pd.DataFrame, n: int) -> list:
        if n <= 0 or df.empty:
            return []
        tail = df.tail(n).copy()
        if pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        return tail.to_dict(orient="records")
