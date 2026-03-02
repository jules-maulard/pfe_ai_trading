from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

_SRC = str(Path(__file__).resolve().parent.parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd

from data.retrievers.yfinance_retriever import CAC40_TICKERS, YFinanceRetriever
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


def run_ingestion(
    symbols: List[str],
    storage: BaseStorage,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: str = "1d",
    mode: str = "auto",
) -> pd.DataFrame:
    retriever = YFinanceRetriever()
    from datetime import timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if mode == "auto":
        all_ohlcv: list[pd.DataFrame] = []
        all_dividends: list[pd.DataFrame] = []
        for symbol in symbols:
            last_date_str = storage.get_last_date("ohlcv", symbol)
            if last_date_str:
                sym_start = (pd.Timestamp(last_date_str) + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                sym_start = start or "2016-01-01"
            sym_end = end or today
            if sym_start > sym_end:
                print(f"  {symbol}: already up to date")
                continue
            print(f"  {symbol}: fetching from {sym_start} to {sym_end}")
            df = retriever.get_ohlcv([symbol], start=sym_start, end=sym_end, interval=interval)
            if not df.empty:
                all_ohlcv.append(df)
            div_df = retriever.get_dividends(symbol)
            if not div_df.empty:
                all_dividends.append(div_df)
        if not all_ohlcv:
            print("No new data to ingest.")
            return pd.DataFrame()
        combined = pd.concat(all_ohlcv, ignore_index=True)
        if all_dividends:
            combined_div = pd.concat(all_dividends, ignore_index=True)
        else:
            combined_div = pd.DataFrame(columns=["symbol", "date", "amount"])
    else:
        effective_start = start or "2016-01-01"
        effective_end = end or today
        print(f"  Manual mode: fetching {len(symbols)} symbols from {effective_start} to {effective_end}")
        combined = retriever.get_ohlcv(symbols, start=effective_start, end=effective_end, interval=interval)
        all_dividends = [retriever.get_dividends(symbol) for symbol in symbols]
        all_dividends = [df for df in all_dividends if not df.empty]
        if all_dividends:
            combined_div = pd.concat(all_dividends, ignore_index=True)
        else:
            combined_div = pd.DataFrame(columns=["symbol", "date", "amount"])

    if combined.empty:
        print("No data fetched.")
        return combined

    try:
        existing = storage.load_ohlcv()
    except FileNotFoundError:
        existing = pd.DataFrame()

    if not existing.empty:
        merged = pd.concat([existing, combined], ignore_index=True)
        merged["date"] = pd.to_datetime(merged["date"], utc=True)
        merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
    else:
        merged = combined

    saved = storage.save_ohlcv(merged)
    if "symbol" not in merged.columns and "SYMBOL" in merged.columns:
        merged["symbol"] = merged["SYMBOL"]
    summary = merged.groupby("symbol").size()
    print(f"\nSaved {len(merged)} total rows for {len(summary)} symbols to {saved}")

    if not combined_div.empty:
        saved_div = storage.save_dividend(combined_div)
        print(f"Saved {len(combined_div)} dividend rows to {saved_div}")

    return merged


def main():
    parser = argparse.ArgumentParser(description="Ingest market data from Yahoo Finance")
    parser.add_argument(
        "--mode", choices=["auto", "manual"], default="auto",
        help="auto: incremental since last date | manual: use --start/--end",
    )
    parser.add_argument("--preset", choices=["cac40"], default="cac40")
    parser.add_argument("--tickers", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--interval", type=str, default="1d")
    args = parser.parse_args()

    if args.tickers:
        symbols = [s.strip() for s in args.tickers.split(",") if s.strip()]
    else:
        symbols = CAC40_TICKERS

    storage = _build_storage()

    print(f"Mode: {args.mode} | Symbols: {len(symbols)} | Backend: {os.environ.get('STORAGE_BACKEND', 'csv')}")
    run_ingestion(
        symbols=symbols,
        storage=storage,
        start=args.start,
        end=args.end,
        interval=args.interval,
        mode=args.mode,
    )


if __name__ == "__main__":
    main()
