from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from services.macd_service import MACDService

mcp = FastMCP("MACD Tools")
macd_service = MACDService()


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


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
        "1. Call `compute_macd` for the symbol with a high sample_rows (e.g. 20) to see recent history.\n"
        "2. Examine the MACD line vs signal line:\n"
        "   - Is MACD above or below the signal? → Current momentum direction.\n"
        "   - Did a crossover happen recently? → Potential trend change.\n"
        "3. Analyse the histogram:\n"
        "   - Is it positive and growing? → Bullish momentum strengthening.\n"
        "   - Is it negative and shrinking? → Bearish momentum weakening.\n"
        "4. Check the zero-line:\n"
        "   - MACD above 0 → Bullish regime; below 0 → Bearish regime.\n"
        "5. Look for divergences between price trend and MACD direction.\n"
        "6. Provide a structured report with: summary, data table, interpretation, recommendation."
    )


@mcp.prompt(
    name="crossover_check",
    description="Quick check for recent MACD crossovers on a symbol.",
)
def crossover_check_prompt(symbol: str) -> str:
    return (
        f"Check for recent MACD crossovers on {symbol}.\n"
        "1. Call `compute_macd` for the symbol with sample_rows=15.\n"
        "2. Scan the returned data for sign changes in the histogram (macd_hist):\n"
        "   - Positive → negative = bearish crossover (MACD crosses below signal).\n"
        "   - Negative → positive = bullish crossover (MACD crosses above signal).\n"
        "3. Report the most recent crossover: date, type, and MACD values at that point.\n"
        "4. State whether the current position is bullish or bearish."
    )


@mcp.prompt(
    name="macd_momentum_comparison",
    description="Compare MACD momentum across multiple symbols.",
)
def macd_momentum_comparison_prompt(symbols: str) -> str:
    return (
        f"Compare the MACD momentum for the following symbols: {symbols}.\n"
        "1. Call `compute_macd` for each symbol (you can pass them all at once as a list).\n"
        "2. For each symbol, note:\n"
        "   - The latest MACD value and signal value\n"
        "   - The current histogram value and its recent trend (growing/shrinking)\n"
        "   - Whether MACD is above or below zero\n"
        "3. Rank the symbols from most bullish to most bearish momentum.\n"
        "4. Present a comparison table and highlight the strongest opportunities."
    )


if __name__ == "__main__":
    mcp.run()
