from __future__ import annotations

import time
from typing import List, Optional

import pandas as pd
import yfinance as yf


CAC40_TICKERS = [
    "AI.PA", "AIR.PA", "ALO.PA", "MT.AS", "CS.PA", "BNP.PA", "EN.PA",
    "CAP.PA", "CA.PA", "ACA.PA", "BN.PA", "DSY.PA", "ENGI.PA", "EL.PA",
    "ERF.PA", "RMS.PA", "KER.PA", "LR.PA", "OR.PA", "MC.PA", "ML.PA",
    "ORA.PA", "RI.PA", "PUB.PA", "SAF.PA", "SGO.PA", "SAN.PA", "SU.PA",
    "GLE.PA", "STLAP.PA", "STM.PA", "TEP.PA", "HO.PA", "TTE.PA",
    "URW.AS", "VIE.PA", "DG.PA", "VIV.PA", "WLN.PA",
]

BATCH_SIZE = 10


class YFinanceRetriever:

    def __init__(self, max_retries: int = 3, backoff_base: float = 1.0):
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def get_ohlcv(
        self,
        symbols: List[str],
        start: str,
        end: Optional[str] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        all_dfs: list[pd.DataFrame] = []
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i : i + BATCH_SIZE]
            raw = self._download_with_retry(batch, start, end, interval)
            df = self._normalize_ohlcv(raw, batch)
            all_dfs.append(df)
        if not all_dfs:
            return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"])
        return pd.concat(all_dfs, ignore_index=True)

    def get_asset_info(self, symbol: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        info = ticker.get_info() if hasattr(ticker, "get_info") else ticker.info
        
        ASSET_COLUMNS = [
            "symbol",
            "company_name",
            "sector",
            "industry",
            "currency",
            "country",
            "exchange",
            "long_business_summary",
            "website",
        ]
        if not info:
            return pd.DataFrame(columns=ASSET_COLUMNS)

        company_name = info.get("longName") or info.get("shortName")
        if info.get("quoteType") == "NONE" or not company_name:
            return pd.DataFrame(columns=ASSET_COLUMNS)

        row = {
            "symbol": symbol,
            "company_name": company_name,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "currency": info.get("currency"),
            "country": info.get("country"),
            "exchange": info.get("exchange"),
            "long_business_summary": info.get("longBusinessSummary"),
            "website": info.get("website"),
        }
        return pd.DataFrame([row], columns=ASSET_COLUMNS)
    
    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        raw = ticker.income_stmt.T
        if raw.empty:
            return pd.DataFrame(columns=[
                "symbol", "date", "total_revenue", "gross_profit",
                "operating_income", "net_income", "eps",
            ])

        col_mapping = {
            "Total Revenue": "total_revenue",
            "Gross Profit": "gross_profit",
            "Operating Income": "operating_income",
            "Net Income": "net_income",
            "Basic EPS": "eps",
        }

        out = pd.DataFrame()
        out["date"] = raw.index
        out["symbol"] = symbol
        for src_col, dst_col in col_mapping.items():
            out[dst_col] = raw[src_col].values if src_col in raw.columns else pd.NA

        return out[["symbol", "date", "total_revenue", "gross_profit",
                     "operating_income", "net_income", "eps"]].reset_index(drop=True)

    def get_dividends(self, symbol: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        divs = ticker.dividends
        if divs.empty:
            return pd.DataFrame(columns=["symbol", "date", "amount"])
        out = divs.to_frame(name="amount").reset_index()
        out.columns = ["date", "amount"]
        out.insert(0, "symbol", symbol)
        return out

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

    @staticmethod
    def _normalize_ohlcv(df: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
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
