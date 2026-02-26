from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import Context, FastMCP

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from services.rsi_service import RSIService

mcp = FastMCP("RSI Tools")
rsi_service = RSIService()


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@mcp.tool(
    name="compute_rsi",
    description="Compute Wilder's RSI for symbols from local OHLCV data.",
)
def compute_rsi(
    window: int = 14,
    price_col: str = "close",
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    return rsi_service.compute(
        window=window, price_col=price_col,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="detect_extremes",
    description=(
        "Identify periods where the RSI crosses overbought or oversold thresholds "
        "(e.g. 70/30 or 80/20 for strong trends)."
    ),
)
def detect_extremes(
    window: int = 14,
    price_col: str = "close",
    overbought: float = 70.0,
    oversold: float = 30.0,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    return rsi_service.detect_extremes(
        window=window, price_col=price_col,
        overbought=overbought, oversold=oversold,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="find_divergences",
    description=(
        "Detect regular and hidden divergences between price peaks/troughs "
        "and RSI peaks/troughs. Returns bullish/bearish regular and hidden divergences."
    ),
)
def find_divergences(
    window: int = 14,
    price_col: str = "close",
    pivot_lookback: int = 5,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    return rsi_service.find_divergences(
        window=window, price_col=price_col,
        pivot_lookback=pivot_lookback,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="analyze_multi_timeframe_rsi",
    description=(
        "Compare the latest RSI value across different resampled timeframes "
        "(e.g. 1D, 1W, 1ME) to assess the underlying trend strength."
    ),
)
def analyze_multi_timeframe_rsi(
    window: int = 14,
    price_col: str = "close",
    timeframes: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, Any]:
    return rsi_service.analyze_multi_timeframe_rsi(
        window=window, price_col=price_col,
        timeframes=timeframes,
        symbols=symbols, start=start, end=end,
    )


@mcp.tool(
    name="detect_failure_swings",
    description=(
        "Identify RSI failure-swing patterns (bullish and bearish) where the RSI "
        "fails to make a new extreme in the overbought/oversold zone, signaling "
        "a high-probability reversal."
    ),
)
def detect_failure_swings(
    window: int = 14,
    price_col: str = "close",
    overbought: float = 70.0,
    oversold: float = 30.0,
    pivot_lookback: int = 5,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    return rsi_service.detect_failure_swings(
        window=window, price_col=price_col,
        overbought=overbought, oversold=oversold,
        pivot_lookback=pivot_lookback,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


@mcp.prompt(
    name="compute_rsi_prompt", 
    description="Prompt for computing RSI."
)
def compute_rsi_prompt(symbol: str) -> str:
    return f"Compute the RSI for {symbol} using the compute_rsi tool."



if __name__ == "__main__":
    mcp.run()
