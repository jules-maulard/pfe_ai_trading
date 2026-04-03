from __future__ import annotations

import pandas as pd
from typing import Any, Dict, List, Optional

from ..data import BaseStorage
from ..data import CsvStorage
from ..data.config.settings import get_storage
from ..utils import get_logger

logger = get_logger(__name__)


class RSIService:
    def __init__(self, storage: BaseStorage = None):
        self.storage = storage or get_storage()

    def compute(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 5,
    ) -> Dict[str, Any]:
        logger.debug("compute_rsi: symbols=%s start=%s end=%s", symbols, start, end)
        df = self.storage.load_indicator("rsi", symbols=symbols, start=start, end=end)

        result = (
            df[["symbol", "date", "rsi"]]
            .dropna(subset=["rsi"])
            .sort_values(["symbol", "date"])
            .reset_index(drop=True)
        )

        sample = self._make_sample(result, sample_rows)

        return {
            "status": "ok",
            "count": int(len(result)),
            "columns": ["symbol", "date", "rsi"],
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

    # ------------------------------------------------------------------ #
    #  detectExtremes                                                      #
    # ------------------------------------------------------------------ #
    def detect_extremes(
        self,
        overbought: float = 70.0,
        oversold: float = 30.0,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        """Identify periods where the RSI crosses overbought / oversold thresholds."""
        logger.debug("detect_extremes: symbols=%s overbought=%.1f oversold=%.1f start=%s end=%s", symbols, overbought, oversold, start, end)
        rsi_df = self.storage.load_indicator("rsi", symbols=symbols, start=start, end=end)
        rsi_df = rsi_df.dropna(subset=["rsi"]).sort_values(["symbol", "date"]).reset_index(drop=True)

        events: list[dict] = []
        for symbol, g in rsi_df.groupby("symbol"):
            g = g.reset_index(drop=True)
            prev_zone = "neutral"
            for i, row in g.iterrows():
                rsi_val = row["rsi"]
                if rsi_val >= overbought:
                    zone = "overbought"
                elif rsi_val <= oversold:
                    zone = "oversold"
                else:
                    zone = "neutral"

                if zone != prev_zone and zone != "neutral":
                    events.append({
                        "symbol": symbol,
                        "date": str(row["date"]),
                        "rsi": round(float(rsi_val), 4),
                        "zone": zone,
                        "threshold": overbought if zone == "overbought" else oversold,
                    })
                prev_zone = zone

        sample = events[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "overbought_threshold": overbought,
            "oversold_threshold": oversold,
            "total_events": len(events),
            "sample": sample,
        }

    # ------------------------------------------------------------------ #
    #  findDivergences                                                     #
    # ------------------------------------------------------------------ #
    def find_divergences(
        self,
        price_col: str = "close",
        pivot_lookback: int = 5,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        """Detect regular and hidden divergences between price and RSI."""
        logger.debug("find_divergences: symbols=%s start=%s end=%s pivot_lookback=%d", symbols, start, end, pivot_lookback)
        ohlcv_df = self.storage.load_ohlcv(symbols=symbols, start=start, end=end)
        rsi_df = self.storage.load_indicator("rsi", symbols=symbols, start=start, end=end)
        merged = ohlcv_df.merge(rsi_df, on=["symbol", "date"], how="inner")
        merged = merged.dropna(subset=["rsi"]).sort_values(["symbol", "date"]).reset_index(drop=True)

        divergences: list[dict] = []

        for symbol, g in merged.groupby("symbol"):
            g = g.reset_index(drop=True)
            prices = g[price_col].values
            rsis = g["rsi"].values
            dates = g["date"].values
            n = len(g)

            # Locate local swing highs and lows
            highs = self._find_pivots(prices, pivot_lookback, kind="high")
            lows = self._find_pivots(prices, pivot_lookback, kind="low")

            # --- Regular bearish: price higher high, RSI lower high ---
            for j in range(1, len(highs)):
                i0, i1 = highs[j - 1], highs[j]
                if prices[i1] > prices[i0] and rsis[i1] < rsis[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, rsis, i0, i1, "regular_bearish",
                    ))

            # --- Regular bullish: price lower low, RSI higher low ---
            for j in range(1, len(lows)):
                i0, i1 = lows[j - 1], lows[j]
                if prices[i1] < prices[i0] and rsis[i1] > rsis[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, rsis, i0, i1, "regular_bullish",
                    ))

            # --- Hidden bearish: price lower high, RSI higher high ---
            for j in range(1, len(highs)):
                i0, i1 = highs[j - 1], highs[j]
                if prices[i1] < prices[i0] and rsis[i1] > rsis[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, rsis, i0, i1, "hidden_bearish",
                    ))

            # --- Hidden bullish: price higher low, RSI lower low ---
            for j in range(1, len(lows)):
                i0, i1 = lows[j - 1], lows[j]
                if prices[i1] > prices[i0] and rsis[i1] < rsis[i0]:
                    divergences.append(self._div_record(
                        symbol, dates, prices, rsis, i0, i1, "hidden_bullish",
                    ))

        sample = divergences[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "total_divergences": len(divergences),
            "sample": sample,
        }

    # ------------------------------------------------------------------ #
    #  analyzeMultiTimeframeRSI                                            #
    # ------------------------------------------------------------------ #
    def analyze_multi_timeframe_rsi(
        self,
        window: int = 14,
        price_col: str = "close",
        timeframes: Optional[List[str]] = None,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare latest RSI value across multiple resampled timeframes.

        Supported timeframe codes follow pandas offset aliases:
        ``1D`` (daily, default), ``1W`` (weekly), ``1M`` / ``1ME`` (monthly).
        """
        if timeframes is None:
            timeframes = ["1D", "1W", "1ME"]

        logger.debug("analyze_multi_timeframe_rsi: symbols=%s timeframes=%s start=%s end=%s", symbols, timeframes, start, end)
        df = self.storage.load_ohlcv(symbols=symbols, start=start, end=end)
        needed = {"symbol", "date", price_col}
        missing = needed - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        df["date"] = pd.to_datetime(df["date"])
        results: list[dict] = []

        for symbol, g in df.groupby("symbol"):
            g = g.set_index("date").sort_index()
            for tf in timeframes:
                tf_label = tf
                resampled = (
                    g[[price_col]]
                    .resample(tf)
                    .agg({price_col: "last"})
                    .dropna()
                    .reset_index()
                )
                resampled["symbol"] = symbol
                if len(resampled) < window + 1:
                    results.append({
                        "symbol": symbol,
                        "timeframe": tf_label,
                        "latest_rsi": None,
                        "latest_date": None,
                        "data_points": len(resampled),
                        "note": "not enough data",
                    })
                    continue
                rsi_out = self.compute_rsi_wilder(resampled, price_col=price_col, window=window)
                rsi_col = f"rsi{window}"
                last_row = rsi_out.dropna(subset=[rsi_col]).iloc[-1]
                results.append({
                    "symbol": symbol,
                    "timeframe": tf_label,
                    "latest_rsi": round(float(last_row[rsi_col]), 4),
                    "latest_date": str(last_row["date"]),
                    "data_points": len(resampled),
                })

        return {"status": "ok", "results": results}

    # ------------------------------------------------------------------ #
    #  detectFailureSwings                                                 #
    # ------------------------------------------------------------------ #
    def detect_failure_swings(
        self,
        overbought: float = 70.0,
        oversold: float = 30.0,
        pivot_lookback: int = 5,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        sample_rows: int = 10,
    ) -> Dict[str, Any]:
        """Detect RSI failure-swing patterns (bullish & bearish).

        *Bullish failure swing*: RSI dips below oversold, rallies, pulls back
        (stays above oversold), then breaks the rally high.

        *Bearish failure swing*: RSI rises above overbought, drops, rises again
        (stays below overbought), then breaks the drop low.
        """
        logger.debug("detect_failure_swings: symbols=%s overbought=%.1f oversold=%.1f start=%s end=%s", symbols, overbought, oversold, start, end)
        rsi_df = self.storage.load_indicator("rsi", symbols=symbols, start=start, end=end)
        rsi_df = rsi_df.dropna(subset=["rsi"]).sort_values(["symbol", "date"]).reset_index(drop=True)

        swings: list[dict] = []

        for symbol, g in rsi_df.groupby("symbol"):
            g = g.reset_index(drop=True)
            rsis = g["rsi"].values
            dates = g["date"].values
            n = len(g)

            highs = self._find_pivots(rsis, pivot_lookback, kind="high")
            lows = self._find_pivots(rsis, pivot_lookback, kind="low")

            # --- Bullish failure swing ---
            # Pattern: low0 < oversold → high0 → low1 > oversold → break above high0
            for li in range(len(lows) - 1):
                l0 = lows[li]
                if rsis[l0] >= oversold:
                    continue
                # find next high after l0
                h_candidates = [h for h in highs if h > l0]
                if not h_candidates:
                    continue
                h0 = h_candidates[0]
                # find next low after h0
                l1_candidates = [l for l in lows if l > h0]
                if not l1_candidates:
                    continue
                l1 = l1_candidates[0]
                if rsis[l1] <= oversold:
                    continue  # must stay above oversold
                # check if RSI breaks above h0 level after l1
                break_idx = None
                for k in range(l1 + 1, min(l1 + 20, n)):
                    if rsis[k] > rsis[h0]:
                        break_idx = k
                        break
                if break_idx is not None:
                    swings.append({
                        "symbol": symbol,
                        "type": "bullish_failure_swing",
                        "date_trigger": str(dates[break_idx]),
                        "rsi_at_trigger": round(float(rsis[break_idx]), 4),
                        "low0_date": str(dates[l0]),
                        "low0_rsi": round(float(rsis[l0]), 4),
                        "high0_date": str(dates[h0]),
                        "high0_rsi": round(float(rsis[h0]), 4),
                        "low1_date": str(dates[l1]),
                        "low1_rsi": round(float(rsis[l1]), 4),
                    })

            # --- Bearish failure swing ---
            # Pattern: high0 > overbought → low0 → high1 < overbought → break below low0
            for hi in range(len(highs) - 1):
                h0 = highs[hi]
                if rsis[h0] <= overbought:
                    continue
                l_candidates = [l for l in lows if l > h0]
                if not l_candidates:
                    continue
                l0 = l_candidates[0]
                h1_candidates = [h for h in highs if h > l0]
                if not h1_candidates:
                    continue
                h1 = h1_candidates[0]
                if rsis[h1] >= overbought:
                    continue  # must stay below overbought
                break_idx = None
                for k in range(h1 + 1, min(h1 + 20, n)):
                    if rsis[k] < rsis[l0]:
                        break_idx = k
                        break
                if break_idx is not None:
                    swings.append({
                        "symbol": symbol,
                        "type": "bearish_failure_swing",
                        "date_trigger": str(dates[break_idx]),
                        "rsi_at_trigger": round(float(rsis[break_idx]), 4),
                        "high0_date": str(dates[h0]),
                        "high0_rsi": round(float(rsis[h0]), 4),
                        "low0_date": str(dates[l0]),
                        "low0_rsi": round(float(rsis[l0]), 4),
                        "high1_date": str(dates[h1]),
                        "high1_rsi": round(float(rsis[h1]), 4),
                    })

        sample = swings[-sample_rows:] if sample_rows > 0 else []
        return {
            "status": "ok",
            "total_failure_swings": len(swings),
            "sample": sample,
        }

    # ------------------------------------------------------------------ #
    #  Private helpers                                                     #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _find_pivots(series, lookback: int, kind: str = "high") -> List[int]:
        """Return indices of local swing highs or lows in *series*."""
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
    def _div_record(symbol, dates, prices, rsis, i0, i1, div_type) -> dict:
        return {
            "symbol": symbol,
            "type": div_type,
            "date_a": str(dates[i0]),
            "price_a": round(float(prices[i0]), 4),
            "rsi_a": round(float(rsis[i0]), 4),
            "date_b": str(dates[i1]),
            "price_b": round(float(prices[i1]), 4),
            "rsi_b": round(float(rsis[i1]), 4),
        }

    @staticmethod
    def _make_sample(df: pd.DataFrame, n: int) -> list:
        if n <= 0 or df.empty:
            return []
        tail = df.tail(n).copy()
        if pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        return tail.to_dict(orient="records")
