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


@mcp.prompt(
    name="compute_rsi_prompt", 
    description="Prompt for computing RSI."
)
def compute_rsi_prompt(symbol: str) -> str:
    return f"Compute the RSI for {symbol} using the compute_rsi tool."



if __name__ == "__main__":
    mcp.run()
