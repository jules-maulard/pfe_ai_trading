from __future__ import annotations

import time
from typing import List, Literal, Optional

import pandas as pd
import yfinance as yf

from ...utils import get_logger

logger = get_logger(__name__)

INCOME_STATEMENT_COLUMNS = [
    "symbol", "date", "period_type",
    "total_revenue", "cost_of_revenue", "gross_profit",
    "operating_income", "operating_expense", "net_income",
    "ebitda", "eps_basic", "eps_diluted",
]

BALANCE_SHEET_COLUMNS = [
    "symbol", "date", "period_type",
    "total_assets", "total_liabilities", "stockholders_equity",
    "cash_and_equivalents", "total_debt", "net_debt",
    "current_assets", "current_liabilities",
]

CASH_FLOW_COLUMNS = [
    "symbol", "date", "period_type",
    "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
    "free_cash_flow", "capital_expenditure",
]

FINANCIAL_RATIOS_COLUMNS = [
    "symbol", "date", "period_type",
    "gross_margin", "operating_margin", "net_margin",
    "return_on_equity", "return_on_assets",
    "debt_to_equity", "current_ratio",
]

DIVIDEND_COLUMNS = [
    "symbol", "date", "amount",
]

_INCOME_STMT_MAPPING = {
    "Total Revenue": "total_revenue",
    "Cost Of Revenue": "cost_of_revenue",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Operating Expense": "operating_expense",
    "Net Income": "net_income",
    "EBITDA": "ebitda",
    "Basic EPS": "eps_basic",
    "Diluted EPS": "eps_diluted",
}

_BALANCE_SHEET_MAPPING = {
    "Total Assets": "total_assets",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Stockholders Equity": "stockholders_equity",
    "Cash And Cash Equivalents": "cash_and_equivalents",
    "Total Debt": "total_debt",
    "Net Debt": "net_debt",
    "Current Assets": "current_assets",
    "Current Liabilities": "current_liabilities",
}

_CASH_FLOW_MAPPING = {
    "Operating Cash Flow": "operating_cash_flow",
    "Investing Cash Flow": "investing_cash_flow",
    "Financing Cash Flow": "financing_cash_flow",
    "Free Cash Flow": "free_cash_flow",
    "Capital Expenditure": "capital_expenditure",
}


class FundamentalRetriever:

    def __init__(self, max_retries: int = 3, backoff_base: float = 1.0):
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    def get_income_statement(
        self,
        symbols: List[str],
        period: Literal["annual", "quarterly"] = "annual",
    ) -> pd.DataFrame:
        return self._fetch_fundamental(
            symbols=symbols,
            period=period,
            statement_attr="income_stmt" if period == "annual" else "quarterly_income_stmt",
            column_mapping=_INCOME_STMT_MAPPING,
            output_columns=INCOME_STATEMENT_COLUMNS,
        )

    def get_balance_sheet(
        self,
        symbols: List[str],
        period: Literal["annual", "quarterly"] = "annual",
    ) -> pd.DataFrame:
        return self._fetch_fundamental(
            symbols=symbols,
            period=period,
            statement_attr="balance_sheet" if period == "annual" else "quarterly_balance_sheet",
            column_mapping=_BALANCE_SHEET_MAPPING,
            output_columns=BALANCE_SHEET_COLUMNS,
        )

    def get_cash_flow(
        self,
        symbols: List[str],
        period: Literal["annual", "quarterly"] = "annual",
    ) -> pd.DataFrame:
        return self._fetch_fundamental(
            symbols=symbols,
            period=period,
            statement_attr="cashflow" if period == "annual" else "quarterly_cashflow",
            column_mapping=_CASH_FLOW_MAPPING,
            output_columns=CASH_FLOW_COLUMNS,
        )

    def get_financial_ratios(
        self,
        symbols: List[str],
        period: Literal["annual", "quarterly"] = "annual",
    ) -> pd.DataFrame:
        income_df = self.get_income_statement(symbols, period)
        balance_df = self.get_balance_sheet(symbols, period)

        if income_df.empty or balance_df.empty:
            return pd.DataFrame(columns=FINANCIAL_RATIOS_COLUMNS)

        merged = pd.merge(
            income_df, balance_df,
            on=["symbol", "date", "period_type"],
            how="inner",
        )

        if merged.empty:
            return pd.DataFrame(columns=FINANCIAL_RATIOS_COLUMNS)

        ratios = pd.DataFrame()
        ratios["symbol"] = merged["symbol"]
        ratios["date"] = merged["date"]
        ratios["period_type"] = merged["period_type"]

        ratios["gross_margin"] = _safe_divide(merged["gross_profit"], merged["total_revenue"])
        ratios["operating_margin"] = _safe_divide(merged["operating_income"], merged["total_revenue"])
        ratios["net_margin"] = _safe_divide(merged["net_income"], merged["total_revenue"])
        ratios["return_on_equity"] = _safe_divide(merged["net_income"], merged["stockholders_equity"])
        ratios["return_on_assets"] = _safe_divide(merged["net_income"], merged["total_assets"])
        ratios["debt_to_equity"] = _safe_divide(merged["total_liabilities"], merged["stockholders_equity"])
        ratios["current_ratio"] = _safe_divide(merged["current_assets"], merged["current_liabilities"])

        return ratios[FINANCIAL_RATIOS_COLUMNS].reset_index(drop=True)

    def get_dividends(
        self,
        symbols: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        all_dfs: list[pd.DataFrame] = []
        for symbol in symbols:
            logger.info("Fetching dividends for %s", symbol)
            raw = self._get_dividends_with_retry(symbol)
            if raw is None or raw.empty:
                logger.warning("No dividend data for %s", symbol)
                continue
            df = pd.DataFrame({"date": pd.to_datetime(raw.index), "amount": raw.values})
            df.insert(0, "symbol", symbol)
            if start:
                df = df[df["date"] >= pd.to_datetime(start)]
            if end:
                df = df[df["date"] <= pd.to_datetime(end)]
            if not df.empty:
                all_dfs.append(df[DIVIDEND_COLUMNS])

        if not all_dfs:
            return pd.DataFrame(columns=DIVIDEND_COLUMNS)
        return pd.concat(all_dfs, ignore_index=True)

    def _get_dividends_with_retry(self, symbol: str):
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                divs = yf.Ticker(symbol).dividends
                return divs
            except Exception as e:
                last_exc = e
                if attempt < self.max_retries:
                    wait = (2 ** attempt) * self.backoff_base
                    logger.warning(
                        "Attempt %d/%d failed for %s dividends: %s — retrying in %.1fs",
                        attempt + 1, self.max_retries + 1, symbol, e, wait,
                    )
                    time.sleep(wait)
        logger.error("Failed to fetch dividends for %s after %d retries: %s", symbol, self.max_retries, last_exc)
        return None

    def _fetch_fundamental(
        self,
        symbols: List[str],
        period: str,
        statement_attr: str,
        column_mapping: dict,
        output_columns: List[str],
    ) -> pd.DataFrame:
        all_dfs: list[pd.DataFrame] = []
        for symbol in symbols:
            logger.info("Fetching %s for %s", statement_attr, symbol)
            raw = self._get_statement_with_retry(symbol, statement_attr)
            if raw is None or raw.empty:
                logger.warning("No %s data for %s", statement_attr, symbol)
                continue
            df = self._normalize_statement(raw, symbol, period, column_mapping, output_columns)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame(columns=output_columns)
        return pd.concat(all_dfs, ignore_index=True)

    def _get_statement_with_retry(self, symbol: str, attr: str) -> pd.DataFrame | None:
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                ticker = yf.Ticker(symbol)
                raw = getattr(ticker, attr)
                if raw is not None and not raw.empty:
                    return raw.T
                return None
            except Exception as e:
                last_exc = e
                if attempt < self.max_retries:
                    wait = (2 ** attempt) * self.backoff_base
                    logger.warning(
                        "Attempt %d/%d failed for %s.%s: %s — retrying in %.1fs",
                        attempt + 1, self.max_retries + 1, symbol, attr, e, wait,
                    )
                    time.sleep(wait)
        logger.error("Failed to fetch %s.%s after %d retries: %s", symbol, attr, self.max_retries, last_exc)
        return None

    @staticmethod
    def _normalize_statement(
        raw: pd.DataFrame,
        symbol: str,
        period: str,
        column_mapping: dict,
        output_columns: List[str],
    ) -> pd.DataFrame:
        out = pd.DataFrame()
        out["date"] = pd.to_datetime(raw.index)
        out["symbol"] = symbol
        out["period_type"] = period

        for src_col, dst_col in column_mapping.items():
            out[dst_col] = raw[src_col].values if src_col in raw.columns else pd.NA

        for col in output_columns:
            if col not in out.columns:
                out[col] = pd.NA

        return out[output_columns].reset_index(drop=True)


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, pd.NA)
