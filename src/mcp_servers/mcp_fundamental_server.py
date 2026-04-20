from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from fastmcp import FastMCP

from .fundamental_service import FundamentalService

mcp = FastMCP("Fundamental Tools")
fundamental_service = FundamentalService()

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_OHLCV_PATH = _PROJECT_ROOT / "database" / "csv" / "ohlcv.csv"
_ASSET_PATH = _PROJECT_ROOT / "database" / "csv" / "asset.csv"


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@mcp.tool(
    name="list_symbols",
    description=(
        "Return all ticker symbols available in the local database with company names. "
        "ALWAYS call this first when the user mentions a company by name."
    ),
)
def list_symbols() -> Dict[str, Any]:
    import pandas as pd
    available = set(pd.read_csv(_OHLCV_PATH, usecols=["symbol"])["symbol"].unique())
    names: Dict[str, str] = {}
    try:
        assets = pd.read_csv(_ASSET_PATH, usecols=["symbol", "company_name"])
        for _, row in assets.iterrows():
            sym = str(row["symbol"])
            if sym in available:
                names[sym] = str(row["company_name"]) if pd.notna(row["company_name"]) else sym
    except Exception:
        pass
    universe = [{"symbol": s, "company_name": names.get(s, s)} for s in sorted(available)]
    return {"count": len(universe), "symbols": universe}


@mcp.tool(
    name="get_income_statement",
    description=(
        "Return income statement data (revenue, net income, EBITDA, EPS) for given symbols. "
        "Use period_type='annual' or 'quarterly'. Output is limited to key columns."
    ),
)
def get_income_statement(
    symbols: List[str],
    period_type: Optional[str] = "annual",
    limit: int = 8,
) -> Dict[str, Any]:
    return fundamental_service.get_income_statement(symbols, period_type, limit)


@mcp.tool(
    name="get_balance_sheet",
    description=(
        "Return balance sheet data (assets, liabilities, equity, debt) for given symbols. "
        "Use period_type='annual' or 'quarterly'. Output is limited to key columns."
    ),
)
def get_balance_sheet(
    symbols: List[str],
    period_type: Optional[str] = "annual",
    limit: int = 8,
) -> Dict[str, Any]:
    return fundamental_service.get_balance_sheet(symbols, period_type, limit)


@mcp.tool(
    name="get_cash_flow",
    description=(
        "Return cash flow data (operating, investing, financing, free cash flow) for given symbols. "
        "Use period_type='annual' or 'quarterly'. Output is limited to key columns."
    ),
)
def get_cash_flow(
    symbols: List[str],
    period_type: Optional[str] = "annual",
    limit: int = 8,
) -> Dict[str, Any]:
    return fundamental_service.get_cash_flow(symbols, period_type, limit)


@mcp.tool(
    name="get_financial_ratios",
    description=(
        "Return financial ratios (margins, ROE, ROA, debt/equity, current ratio) for given symbols. "
        "Use period_type='annual' or 'quarterly'. Output is limited to key columns."
    ),
)
def get_financial_ratios(
    symbols: List[str],
    period_type: Optional[str] = "annual",
    limit: int = 8,
) -> Dict[str, Any]:
    return fundamental_service.get_financial_ratios(symbols, period_type, limit)


@mcp.tool(
    name="get_dividends",
    description="Return dividend history for given symbols.",
)
def get_dividends(
    symbols: List[str],
    limit: int = 10,
) -> Dict[str, Any]:
    return fundamental_service.get_dividends(symbols, limit)


@mcp.tool(
    name="get_fundamental_summary",
    description=(
        "Return a compact summary of all fundamental data (income, balance sheet, "
        "cash flow, ratios) for given symbols. Limited to 2 rows per statement per symbol. "
        "Use this for a quick overview before diving into specific statements."
    ),
)
def get_fundamental_summary(
    symbols: List[str],
    period_type: Optional[str] = "annual",
) -> Dict[str, Any]:
    return fundamental_service.get_fundamental_summary(symbols, period_type)


@mcp.prompt(
    name="full_fundamental_analysis",
    description="Step-by-step workflow for a comprehensive fundamental analysis on a symbol.",
)
def full_fundamental_analysis_prompt(symbol: str) -> str:
    return (
        f"Perform a comprehensive fundamental analysis for {symbol}. Follow these steps:\n"
        f"1. Call get_fundamental_summary with symbols=['{symbol}'] for a quick overview.\n"
        f"2. Call get_income_statement with symbols=['{symbol}'] to examine revenue and profitability trends.\n"
        f"3. Call get_balance_sheet with symbols=['{symbol}'] to assess financial health.\n"
        f"4. Call get_cash_flow with symbols=['{symbol}'] to evaluate cash generation.\n"
        f"5. Call get_financial_ratios with symbols=['{symbol}'] to check margins and leverage.\n"
        f"6. Call get_dividends with symbols=['{symbol}'] if relevant.\n"
        "7. Synthesise a structured report: summary, key metrics, trends, strengths/weaknesses, conclusion."
    )


@mcp.prompt(
    name="fundamental_comparison",
    description="Compare fundamentals across multiple symbols.",
)
def fundamental_comparison_prompt(symbols: str) -> str:
    return (
        f"Compare the fundamentals for the following symbols: {symbols}.\n"
        f"1. Call get_fundamental_summary with symbols={symbols}.\n"
        f"2. Call get_financial_ratios with symbols={symbols}.\n"
        "3. For each symbol, note: revenue trend, profitability, leverage, cash flow quality.\n"
        "4. Rank symbols by overall financial health.\n"
        "5. Present a comparison table and highlight the strongest candidates."
    )


if __name__ == "__main__":
    mcp.run()
