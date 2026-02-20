#!/usr/bin/env python3
"""
Data Ingestion from Yahoo Finance (yfinance)

Purpose
-------
A small, dependency-light module to fetch OHLCV market data from Yahoo Finance
using the free `yfinance` package, then normalize and persist it locally
(CSV or Parquet). Designed for quick MVP workflows.

Features
--------
- Batch download for multiple tickers via `yfinance.download`.
- Clean, tidy output schema: [symbol, date, open, high, low, close, adj_close, volume].
- Optional auto-adjusted prices.
- Save to CSV or Parquet (partition by symbol optional).
- Simple retry with exponential backoff.
- CLI usage for ad-hoc runs.

Notes
-----
- This script does not require any API key (Yahoo Finance via yfinance is free).
- Intraday intervals may have constraints on historical depth; daily is recommended for MVP.
- Educational use only. Not investment advice.

Examples
--------
Fetch SBF120 subset, daily, and save to Parquet:
    python -m src.data_ingest_yfinance \
        --tickers "AI.PA,SU.PA,DG.PA" \
        --start 2018-01-01 --end 2026-02-19 \
        --interval 1d \
        --out-dir data/prices \
        --format parquet

Fetch a single ticker and save CSVs per symbol:
    python -m src.data_ingest_yfinance \
        --tickers "AIR.PA" --start 2020-01-01 --interval 1d \
        --out-dir data/prices --format csv --partition-by-symbol
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

import pandas as pd

try:
    import yfinance as yf
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: yfinance. Install with `pip install yfinance` and retry."
    ) from e


# -------------------------
# Configuration dataclass
# -------------------------
@dataclass
class FetchConfig:
    tickers: List[str]
    start: Optional[str]
    end: Optional[str]
    interval: str = "1d"
    auto_adjust: bool = True
    prepost: bool = False
    progress: bool = False
    actions: bool = False
    backoff_max_retries: int = 3
    backoff_base_sec: float = 1.0


# -------------------------
# Core functions
# -------------------------

def _validate_interval(interval: str) -> None:
    allowed = {
        # Intraday (depth varies by Yahoo policies)
        "1m", "2m", "5m", "15m", "30m", "60m", "90m",
        # Daily/weekly/monthly
        "1d", "5d", "1wk", "1mo", "3mo",
    }
    if interval not in allowed:
        raise ValueError(f"Unsupported interval '{interval}'. Allowed: {sorted(allowed)}")


def _normalize_columns_lower(df: pd.DataFrame) -> pd.DataFrame:
    cols = [str(c).lower().replace(" ", "_") for c in df.columns]
    df = df.copy()
    df.columns = cols
    return df


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    return df[required]


def _normalize_download(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance.download output to a tidy schema.

    Returns a DataFrame with columns:
    [symbol, date, open, high, low, close, adj_close, volume]
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "symbol", "date", "open", "high", "low", "close", "adj_close", "volume"
        ])

    out = df.copy()
    # Ensure DateTime index named 'date'
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce")
    out.index.name = "date"

    if isinstance(out.columns, pd.MultiIndex):
        # When multiple tickers are downloaded, columns are MultiIndex (ticker -> OHLCV)
        symbols = out.columns.get_level_values(0).unique().tolist()
        parts = []
        for sym in symbols:
            sub = out[sym].copy()
            sub = _normalize_columns_lower(sub)
            # Standardize names (adj close may be 'adj_close' or 'adj close')
            rename_map = {
                "adj close": "adj_close",
                "adj_close": "adj_close",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            }
            sub = sub.rename(columns=rename_map)
            sub = sub.reset_index()  # brings 'date'
            sub.insert(0, "symbol", sym)
            sub = _ensure_required_columns(sub)
            parts.append(sub)
        out = pd.concat(parts, axis=0, ignore_index=True)
    else:
        # Single ticker case: flat columns
        out = out.reset_index()  # brings index -> 'date'
        out = _normalize_columns_lower(out)
        # Standardize names
        rename_map = {
            "adj close": "adj_close",
            "adj_close": "adj_close",
        }
        out = out.rename(columns=rename_map)
        # Insert symbol placeholder; caller will fill when single ticker
        out.insert(0, "symbol", "")
        out = _ensure_required_columns(out)

    # Dtypes and cleaning
    out["symbol"] = out["symbol"].astype(str)
    out["date"] = pd.to_datetime(out["date"], utc=True)
    for col in ["open", "high", "low", "close", "adj_close"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")

    # Drop rows where all price fields are NaN
    price_cols = ["open", "high", "low", "close", "adj_close"]
    out = out.dropna(subset=price_cols, how="all").reset_index(drop=True)

    return out


def fetch_ohlcv(cfg: FetchConfig) -> pd.DataFrame:
    """Fetch OHLCV for given tickers with simple retry/backoff.

    If multiple tickers, uses a single batch `yf.download` call for efficiency.
    """
    _validate_interval(cfg.interval)

    tickers_str = ",".join([t.strip() for t in cfg.tickers if t.strip()])
    if not tickers_str:
        raise ValueError("No tickers provided.")

    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= cfg.backoff_max_retries:
        try:
            df = yf.download(
                tickers=tickers_str,
                start=cfg.start,
                end=cfg.end,
                interval=cfg.interval,
                auto_adjust=cfg.auto_adjust,
                prepost=cfg.prepost,
                progress=cfg.progress,
                actions=cfg.actions,
                group_by="ticker",
                threads=True,
            )
            normalized = _normalize_download(df)

            # If single ticker and symbol column is empty, fill it
            if len(cfg.tickers) == 1 and not normalized.empty:
                normalized["symbol"] = cfg.tickers[0].strip()

            return normalized.sort_values(["symbol", "date"]).reset_index(drop=True)
        except Exception as e:
            last_exc = e
            if attempt == cfg.backoff_max_retries:
                break
            sleep_s = (2 ** attempt) * cfg.backoff_base_sec
            time.sleep(sleep_s)
            attempt += 1
    raise RuntimeError(f"Failed to fetch data after retries: {last_exc}")


# -------------------------
# Persistence utilities
# -------------------------
def save_prices(
    df: pd.DataFrame,
    out_dir: str,
    fmt: str = "parquet",
    partition_by_symbol: bool = False,
) -> None:
    """Save prices to disk in a tidy, reproducible way.

    Parameters
    ----------
    df : DataFrame with columns [symbol, date, open, high, low, close, adj_close, volume]
    out_dir : base directory to write files
    fmt : "parquet" or "csv"
    partition_by_symbol : if True, writes one file per symbol under out_dir/{symbol}/
                          else writes a single consolidated file per format.
    """
    os.makedirs(out_dir, exist_ok=True)
    fmt = fmt.lower()
    allowed = {"csv", "parquet"}
    if fmt not in allowed:
        raise ValueError(f"Unsupported format '{fmt}'. Use one of {allowed}.")

    base_cols = ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    missing = [c for c in base_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if partition_by_symbol:
        for sym, g in df.groupby("symbol", dropna=False):
            sym = (sym or "UNKNOWN").replace("/", "-")
            subdir = os.path.join(out_dir, sym)
            os.makedirs(subdir, exist_ok=True)
            if fmt == "csv":
                out_path = os.path.join(subdir, f"prices_{sym}.csv")
                g.to_csv(out_path, index=False)
            else:
                out_path = os.path.join(subdir, f"prices_{sym}.parquet")
                g.to_parquet(out_path, index=False, engine="pyarrow")
    else:
        if fmt == "csv":
            out_path = os.path.join(out_dir, "prices.csv")
            df.to_csv(out_path, index=False)
        else:
            out_path = os.path.join(out_dir, "prices.parquet")
            df.to_parquet(out_path, index=False, engine="pyarrow")


# -------------------------
# CLI
# -------------------------
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch OHLCV data from Yahoo Finance (yfinance)")
    p.add_argument("--tickers", type=str, required=True, help="Comma-separated list, e.g. 'AIR.PA,SU.PA'")
    p.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD (optional)")
    p.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (optional)")
    p.add_argument("--interval", type=str, default="1d", help="Interval: 1d,1wk,1mo (intraday like 1m,5m if needed)")
    p.add_argument("--out-dir", type=str, default="data/prices", help="Output directory")
    p.add_argument("--format", type=str, default="parquet", choices=["csv", "parquet"], help="Output format")
    p.add_argument("--no-auto-adjust", action="store_true", help="Disable auto-adjusted prices")
    p.add_argument("--partition-by-symbol", action="store_true", help="Write one file per symbol")
    return p.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    cfg = FetchConfig(
        tickers=tickers,
        start=args.start,
        end=args.end,
        interval=args.interval,
        auto_adjust=(not args.no_auto_adjust),
    )

    df = fetch_ohlcv(cfg)
    save_prices(df, out_dir=args.out_dir, fmt=args.format, partition_by_symbol=args.partition_by_symbol)

    # Print a small preview
    print(df.groupby('symbol').size().rename('rows_per_symbol'))
    print("Saved to:", os.path.abspath(args.out_dir))


if __name__ == "__main__":
    main(sys.argv[1:])