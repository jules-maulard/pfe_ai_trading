from __future__ import annotations

import time
from typing import List, Optional

import pandas as pd
import yfinance as yf


class YFinanceRetriever:
    def __init__(self, max_retries: int = 3, backoff_base: float = 1.0):
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def get_prices(
        self,
        symbols: List[str],
        start: str,
        end: Optional[str] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        raw = self._download_with_retry(symbols, start, end, interval)
        return self._normalize(raw, symbols)

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        data = ticker.income_stmt.T
        data["symbol"] = symbol
        return data.rename_axis("date")

    def get_dividends(self, symbol: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        data = ticker.dividends.to_frame()
        data["symbol"] = symbol
        return data.rename_axis("date")

    def _download_with_retry(self, symbols, start, end, interval):
        tickers_str = " ".join(symbols)
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                return yf.download(
                    tickers=tickers_str,
                    start=start,
                    end=end,
                    interval=interval,
                    auto_adjust=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
            except Exception as e:
                last_exc = e
                if attempt < self.max_retries:
                    time.sleep((2 ** attempt) * self.backoff_base)
        raise RuntimeError(f"Failed after {self.max_retries} retries: {last_exc}")

    def _normalize(self, df: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])

        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        df.index.name = "date"

        if isinstance(df.columns, pd.MultiIndex):
            parts = []
            for sym in df.columns.get_level_values(0).unique():
                sub = df[sym].copy().reset_index()
                sub.columns = [str(c).lower().replace(" ", "_") for c in sub.columns]
                sub.insert(0, "symbol", sym)
                parts.append(sub)
            out = pd.concat(parts, ignore_index=True)
        else:
            out = df.reset_index()
            out.columns = [str(c).lower().replace(" ", "_") for c in out.columns]
            out.insert(0, "symbol", symbols[0] if len(symbols) == 1 else "")

        out["date"] = pd.to_datetime(out["date"], utc=True)
        for col in ["open", "high", "low", "close"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        if "volume" in out.columns:
            out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")

        price_cols = [c for c in ["open", "high", "low", "close"] if c in out.columns]
        out = out.dropna(subset=price_cols, how="all").reset_index(drop=True)

        keep = ["symbol", "date"] + [c for c in ["open", "high", "low", "close", "volume"] if c in out.columns]
        return out[keep].sort_values(["symbol", "date"]).reset_index(drop=True)