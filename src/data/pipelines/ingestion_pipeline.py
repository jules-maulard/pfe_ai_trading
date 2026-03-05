from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

_SRC = str(Path(__file__).resolve().parent.parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd

from data.retrievers.yfinance_retriever import CAC40_TICKERS, YFinanceRetriever
from data.storage.base_storage import BaseStorage
from utils.logger import get_logger

logger = get_logger(__name__)


def _build_storage() -> BaseStorage:
    load_dotenv()
    backend = os.environ.get("STORAGE_BACKEND", "none").lower()

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


def _today() -> str:
    from datetime import timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


DEFAULT_START = "2016-01-01"


def _fetch_ohlcv_auto(
    symbols: List[str],
    retriever: YFinanceRetriever,
    storage: BaseStorage,
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    effective_end = end or _today()

    for symbol in symbols:
        last_date_str = storage.get_last_date("ohlcv", symbol)
        sym_start = (
            (pd.Timestamp(last_date_str) + timedelta(days=1)).strftime("%Y-%m-%d")
            if last_date_str
            else (start or DEFAULT_START)
        )
        if sym_start > effective_end:
            logger.debug("%s: already up to date", symbol)
            continue

        logger.info("%s: fetching from %s to %s", symbol, sym_start, effective_end)
        df = retriever.get_ohlcv([symbol], start=sym_start, end=effective_end, interval=interval)
        if not df.empty:
            frames.append(df)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_ohlcv_manual(
    symbols: List[str],
    retriever: YFinanceRetriever,
    start: Optional[str],
    end: Optional[str],
    interval: str,
) -> pd.DataFrame:
    effective_start = start or DEFAULT_START
    effective_end = end or _today()
    logger.info("Manual mode: fetching %d symbols from %s to %s", len(symbols), effective_start, effective_end)
    return retriever.get_ohlcv(symbols, start=effective_start, end=effective_end, interval=interval)


def _merge_and_save_ohlcv(
    new_data: pd.DataFrame,
    storage: BaseStorage,
    symbols: List[str],
    mode: str,
) -> pd.DataFrame:
    if mode == "auto":
        saved = storage.append_ohlcv(new_data)
        if "symbol" not in new_data.columns and "SYMBOL" in new_data.columns:
            new_data["symbol"] = new_data["SYMBOL"]
        logger.info("Appended %d new rows for %d symbols to %s", len(new_data), new_data['symbol'].nunique(), saved)
        return new_data

    try:
        existing = storage.load_ohlcv(symbols=symbols)
    except FileNotFoundError:
        existing = pd.DataFrame()

    if not existing.empty:
        merged = pd.concat([existing, new_data], ignore_index=True)
        merged["date"] = pd.to_datetime(merged["date"], utc=True)
        merged = merged.drop_duplicates(subset=["symbol", "date"], keep="last")
    else:
        merged = new_data

    saved = storage.upsert_ohlcv(merged)

    if "symbol" not in merged.columns and "SYMBOL" in merged.columns:
        merged["symbol"] = merged["SYMBOL"]
    summary = merged.groupby("symbol").size()
    logger.info("Saved %d total rows for %d symbols to %s", len(merged), len(summary), saved)
    return merged


def ingest_ohlcv(
    symbols: List[str],
    retriever: YFinanceRetriever,
    storage: BaseStorage,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: str = "1d",
    mode: str = "auto",
) -> pd.DataFrame:
    if mode == "auto":
        fetched = _fetch_ohlcv_auto(symbols, retriever, storage, start, end, interval)
    else:
        fetched = _fetch_ohlcv_manual(symbols, retriever, start, end, interval)

    if fetched.empty:
        logger.warning("No OHLCV data fetched.")
        return fetched

    return _merge_and_save_ohlcv(fetched, storage, symbols, mode)


def ingest_dividends(
    symbols: List[str],
    retriever: YFinanceRetriever,
    storage: BaseStorage,
) -> None:
    frames = [retriever.get_dividends(s) for s in symbols]
    frames = [df for df in frames if not df.empty]

    if not frames:
        return pd.DataFrame(columns=["symbol", "date", "amount"])

    new_data = pd.concat(frames, ignore_index=True)

    try:
        existing = storage.load_dividend(symbols=symbols)
    except FileNotFoundError:
        existing = pd.DataFrame()


    def normalize_date_to_midnight_utc(df):
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["date"] = df["date"].dt.normalize().dt.tz_localize(None)
        return df

    new_data = normalize_date_to_midnight_utc(new_data)
    if not existing.empty:
        existing = normalize_date_to_midnight_utc(existing)
        combined = pd.concat([existing, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "date"], keep="last")
        new_rows = len(combined) - len(existing)
    else:
        combined = new_data
        new_rows = len(combined)

    if new_rows > 0:
        saved = storage.upsert_dividend(combined)
    else:
        saved = None
    logger.info("Saved %d new dividend rows to %s", new_rows, saved if saved else "no file written")
    return combined

def ingest_assets(
    symbols: List[str],
    retriever: YFinanceRetriever,
    storage: BaseStorage,
) -> None:
    added_symbols = []
    for symbol in symbols:
        if storage.load_asset(symbols=[symbol]).empty:
            info = retriever.get_asset_info(symbol)
            if not info.empty:
                storage.save_asset(info)
                added_symbols.append(symbol)
            else:
                logger.warning("No asset info found for %s", symbol)
    logger.info("Saved asset info for %d new symbols: %s", len(added_symbols), ', '.join(added_symbols))

def run_ingestion(
    symbols: List[str],
    storage: BaseStorage,
    start: Optional[str] = None,
    end: Optional[str] = None,
    interval: str = "1d",
    mode: str = "auto",
) -> None:
    retriever = YFinanceRetriever()

    ingest_ohlcv(symbols, retriever, storage, start, end, interval, mode)
    ingest_dividends(symbols, retriever, storage)
    ingest_assets(symbols, retriever, storage)


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

    logger.info("Mode: %s | Symbols: %d | Backend: %s", args.mode, len(symbols), os.environ.get('STORAGE_BACKEND', 'csv'))
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
