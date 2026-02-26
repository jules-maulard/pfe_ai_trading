from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import Context, FastMCP

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from services.rsi_service import RSIService
from services.macd_service import MACDService

mcp = FastMCP("RSI Tools")
rsi_service = RSIService()
macd_service = MACDService()


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
    save: bool = False,
    save_path: str = "data/indicators/rsi14.csv",
    sample_rows: int = 5,
) -> Dict[str, Any]:
    return rsi_service.compute(
        window=window, price_col=price_col,
        symbols=symbols, start=start, end=end,
        save=save, save_path=save_path, sample_rows=sample_rows,
    )


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
    save: bool = False,
    save_path: str = "data/indicators/macd.csv",
    sample_rows: int = 5,
) -> Dict[str, Any]:
    return macd_service.compute(
        fast=fast, slow=slow, signal=signal,
        price_col=price_col, symbols=symbols, start=start, end=end,
        save=save, save_path=save_path, sample_rows=sample_rows,
    )


@mcp.prompt(name="compute_rsi_prompt", description="Prompt for computing RSI.")
def compute_rsi_prompt(symbol: str) -> str:
    return f"Compute the RSI for {symbol} using the compute_rsi tool."


@mcp.tool(name="compute_rsi_sampling_test", description="Test sampling with RSI.")
async def compute_rsi_sampling_test(symbol: str, ctx: Context) -> str:
    rsi = compute_rsi(symbols=[symbol], sample_rows=1)["sample"][0]["rsi14"]
    result = await ctx.sample(f"Interpret this RSI result: {rsi}")
    return result.text


if __name__ == "__main__":
    mcp.run()
