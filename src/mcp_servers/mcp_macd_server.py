from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from fastmcp import FastMCP

from .macd_service import MACDService

mcp = FastMCP("MACD Tools")
macd_service = MACDService()

_MACD_RESOURCES_DIR = Path(__file__).resolve().parent.parent.parent / "database" / "ressources" / "macd"


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_OHLCV_PATH   = _PROJECT_ROOT / "database" / "csv" / "ohlcv.csv"
_ASSET_PATH   = _PROJECT_ROOT / "database" / "csv" / "asset.csv"


@mcp.tool(
    name="list_symbols",
    description=(
        "Return all ticker symbols available in the local OHLCV database, "
        "together with their company names. "
        "ALWAYS call this tool first when the user mentions a company by name "
        "(e.g. 'orange', 'airbus', 'air france') to resolve the exact ticker, "
        "and when asked to compare or rank all symbols in the universe."
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
    name="compute_macd",
    description="Compute MACD for symbols from local OHLCV data.",
)
def compute_macd_tool(
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    return macd_service.compute(
        fast=fast, slow=slow, signal=signal,
        price_col=price_col, symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="detect_crossovers",
    description=(
        "Detect recent MACD/signal-line and MACD/zero-line crossovers. "
        "Returns bullish and bearish crossover events with dates and values."
    ),
)
def detect_crossovers_tool(
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    return macd_service.detect_crossovers(
        fast=fast, slow=slow, signal=signal,
        price_col=price_col, symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="find_divergences",
    description=(
        "Detect regular and hidden divergences between price and MACD. "
        "The 'symbols' parameter is REQUIRED — always pass a list like "
        "['AIR.PA'] to avoid scanning the entire dataset."
    ),
)
def find_divergences_tool(
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
    pivot_lookback: int = 5,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    return macd_service.find_divergences(
        fast=fast, slow=slow, signal=signal,
        price_col=price_col, pivot_lookback=pivot_lookback,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


# ──────────────────────────────────────────────
# RESOURCES
# ──────────────────────────────────────────────

@mcp.resource(
    "macd://knowledge/calculation-theory",
    description="MACD calculation theory, formulas and parameter tuning.",
)
def macd_calculation_theory() -> str:
    return (_MACD_RESOURCES_DIR / "macd_calculation_theory.md").read_text(encoding="utf-8")


@mcp.resource(
    "macd://knowledge/crossovers-guide",
    description="Guide to MACD signal-line and zero-line crossovers.",
)
def macd_crossovers_guide() -> str:
    return (_MACD_RESOURCES_DIR / "macd_crossovers_guide.md").read_text(encoding="utf-8")


@mcp.resource(
    "macd://knowledge/divergences-guide",
    description="Guide to MACD divergences (regular and hidden).",
)
def macd_divergences_guide() -> str:
    return (_MACD_RESOURCES_DIR / "macd_divergences_guide.md").read_text(encoding="utf-8")


# ──────────────────────────────────────────────
# PROMPTS
# ──────────────────────────────────────────────

@mcp.prompt(
    name="full_macd_analysis",
    description="Step-by-step workflow for a comprehensive MACD analysis on a symbol.",
)
def full_macd_analysis_prompt(symbol: str) -> str:
    return (
        f"Perform a comprehensive MACD analysis for {symbol}. Follow these steps:\n"
        f"1. Call compute_macd with symbols=['{symbol}'] and sample_rows=20.\n"
        f"2. Call detect_crossovers with symbols=['{symbol}'].\n"
        f"3. Call find_divergences with symbols=['{symbol}'].\n"
        "4. Read relevant knowledge resources to ground your interpretation.\n"
        "5. Synthesise a structured report: summary, key data, interpretation, recommendation."
    )


@mcp.prompt(
    name="crossover_check",
    description="Quick check for recent MACD crossovers on a symbol.",
)
def crossover_check_prompt(symbol: str) -> str:
    return (
        f"Detect and analyse recent MACD crossovers for {symbol}.\n"
        f"1. Call detect_crossovers with symbols=['{symbol}'].\n"
        "2. Read the resource macd://knowledge/crossovers-guide.\n"
        "3. For each crossover found, report: date, type, MACD and signal values.\n"
        "4. State current momentum direction (bullish or bearish) and strength."
    )


@mcp.prompt(
    name="divergence_scan",
    description="Detect and interpret MACD divergences for a symbol.",
)
def divergence_scan_prompt(symbol: str) -> str:
    return (
        f"Detect and analyse MACD divergences for {symbol}.\n"
        f"1. Call find_divergences with symbols=['{symbol}'].\n"
        "2. Read the resource macd://knowledge/divergences-guide.\n"
        "3. For each divergence found, explain type, dates, price/MACD levels, and trend implication.\n"
        "4. Conclude with the strongest signal and a trading recommendation."
    )


@mcp.prompt(
    name="macd_momentum_comparison",
    description="Compare MACD momentum across multiple symbols.",
)
def macd_momentum_comparison_prompt(symbols: str) -> str:
    return (
        f"Compare the MACD momentum for the following symbols: {symbols}.\n"
        f"1. Call compute_macd with symbols={symbols}.\n"
        f"2. Call detect_crossovers with symbols={symbols}.\n"
        "3. Read the resource macd://knowledge/calculation-theory.\n"
        "4. For each symbol, note: latest MACD/signal values, histogram trend, zero-line position.\n"
        "5. Rank symbols from most bullish to most bearish momentum.\n"
        "6. Present a comparison table and highlight the strongest opportunities."
    )


if __name__ == "__main__":
    mcp.run()
