from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

_SRC = str(Path(__file__).resolve().parent.parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd

from data.storage.base_storage import BaseStorage
from utils.logger import get_logger

logger = get_logger(__name__)


def _build_storage() -> BaseStorage:
    backend = os.environ.get("STORAGE_BACKEND", "csv").lower()

    if backend == "csv":
        from data.storage.csv_storage import CsvStorage
        return CsvStorage()
    elif backend == "parquet":
        from data.storage.parquet_storage import ParquetStorage
        return ParquetStorage()
    elif backend == "snowflake":
        from data.storage.snowflake_storage import SnowflakeStorage
        return SnowflakeStorage()
    else:
        raise ValueError(f"Unknown STORAGE_BACKEND: {backend}")


def compute_rsi(df: pd.DataFrame, window: int = 14, price_col: str = "close") -> pd.DataFrame:
    parts = []
    for _, g in df.groupby("symbol"):
        g = g.copy().sort_values("date")
        delta = g[price_col].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(avg_loss != 0, 100.0)
        rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), pd.NA)
        g["rsi"] = rsi
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def compute_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
) -> pd.DataFrame:
    parts = []
    for _, g in df.groupby("symbol"):
        g = g.copy().sort_values("date")
        ema_fast = g[price_col].ewm(span=fast, adjust=False, min_periods=fast).mean()
        ema_slow = g[price_col].ewm(span=slow, adjust=False, min_periods=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
        g["macd"] = macd_line
        g["macd_signal"] = signal_line
        g["macd_hist"] = macd_line - signal_line
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def run_indicators(
    storage: BaseStorage,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    rsi_window: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
) -> pd.DataFrame:
    logger.info("Loading OHLCV data from storage")
    ohlcv = storage.load_ohlcv(symbols=symbols, start=start, end=end)

    if ohlcv.empty:
        logger.warning("No OHLCV data found. Run ingestion first.")
        return pd.DataFrame()

    logger.info("Computing RSI(%d)", rsi_window)
    rsi_df = compute_rsi(ohlcv, window=rsi_window)

    logger.info("Computing MACD(%d, %d, %d)", macd_fast, macd_slow, macd_signal)
    macd_df = compute_macd(rsi_df, fast=macd_fast, slow=macd_slow, signal=macd_signal)

    indicators = macd_df[["symbol", "date", "rsi", "macd", "macd_signal", "macd_hist"]].copy()
    indicators = indicators.dropna(subset=["rsi", "macd", "macd_signal", "macd_hist"])
    indicators = indicators.sort_values(["symbol", "date"]).reset_index(drop=True)

    saved = storage.upsert_indicators(indicators)
    logger.info("Saved %d indicator rows to %s", len(indicators), saved)
    return indicators


def main():
    parser = argparse.ArgumentParser(description="Compute technical indicators from stored OHLCV data")
    parser.add_argument("--tickers", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--rsi-window", type=int, default=14)
    parser.add_argument("--macd-fast", type=int, default=12)
    parser.add_argument("--macd-slow", type=int, default=26)
    parser.add_argument("--macd-signal", type=int, default=9)
    args = parser.parse_args()

    symbols = None
    if args.tickers:
        symbols = [s.strip() for s in args.tickers.split(",") if s.strip()]

    storage = _build_storage()

    logger.info("Backend: %s", os.environ.get('STORAGE_BACKEND', 'csv'))
    run_indicators(
        storage=storage,
        symbols=symbols,
        start=args.start,
        end=args.end,
        rsi_window=args.rsi_window,
        macd_fast=args.macd_fast,
        macd_slow=args.macd_slow,
        macd_signal=args.macd_signal,
    )


if __name__ == "__main__":
    main()
