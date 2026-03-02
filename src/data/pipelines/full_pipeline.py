from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from data.retrievers.yfinance_retriever import CAC40_TICKERS
from data.storage.base_storage import BaseStorage


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


def main():
    parser = argparse.ArgumentParser(description="Full pipeline: ingest data then compute indicators")
    parser.add_argument(
        "--mode", choices=["auto", "manual"], default="auto",
        help="auto: incremental ingestion | manual: use --start/--end",
    )
    parser.add_argument("--preset", choices=["cac40"], default="cac40")
    parser.add_argument("--tickers", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--interval", type=str, default="1d")
    parser.add_argument("--rsi-window", type=int, default=14)
    parser.add_argument("--macd-fast", type=int, default=12)
    parser.add_argument("--macd-slow", type=int, default=26)
    parser.add_argument("--macd-signal", type=int, default=9)
    args = parser.parse_args()

    if args.tickers:
        symbols = [s.strip() for s in args.tickers.split(",") if s.strip()]
    else:
        symbols = CAC40_TICKERS

    storage = _build_storage()
    backend_name = os.environ.get("STORAGE_BACKEND", "csv")

    print(f"=== FULL PIPELINE === Backend: {backend_name} | Mode: {args.mode} | Symbols: {len(symbols)}")

    print("\n--- Step 1: Ingestion ---")
    from data.pipelines.ingestion_pipeline import run_ingestion

    run_ingestion(
        symbols=symbols,
        storage=storage,
        start=args.start,
        end=args.end,
        interval=args.interval,
        mode=args.mode,
    )

    print("\n--- Step 2: Indicators ---")
    from data.pipelines.indicators_pipeline import run_indicators

    tickers_filter = symbols if args.tickers else None
    run_indicators(
        storage=storage,
        symbols=tickers_filter,
        start=args.start,
        end=args.end,
        rsi_window=args.rsi_window,
        macd_fast=args.macd_fast,
        macd_slow=args.macd_slow,
        macd_signal=args.macd_signal,
    )

    print("\n=== FULL PIPELINE COMPLETE ===")


def run_full(
    symbols=None,
    mode="auto",
    start=None,
    end=None,
    interval="1d",
    rsi_window=14,
    macd_fast=12,
    macd_slow=26,
    macd_signal=9,
):
    from data.pipelines.ingestion_pipeline import run_ingestion
    from data.pipelines.indicators_pipeline import run_indicators

    effective_symbols = symbols or CAC40_TICKERS
    storage = _build_storage()
    backend_name = os.environ.get("STORAGE_BACKEND", "csv")

    print(f"=== FULL PIPELINE === Backend: {backend_name} | Mode: {mode} | Symbols: {len(effective_symbols)}")

    print("\n--- Step 1: Ingestion ---")
    run_ingestion(
        symbols=effective_symbols,
        storage=storage,
        start=start,
        end=end,
        interval=interval,
        mode=mode,
    )

    print("\n--- Step 2: Indicators ---")
    run_indicators(
        storage=storage,
        symbols=symbols,
        start=start,
        end=end,
        rsi_window=rsi_window,
        macd_fast=macd_fast,
        macd_slow=macd_slow,
        macd_signal=macd_signal,
    )

    print("\n=== FULL PIPELINE COMPLETE ===")


if __name__ == "__main__":
    main()
