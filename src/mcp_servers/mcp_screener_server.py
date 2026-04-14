from __future__ import annotations

from typing import Dict, List
from fastmcp import FastMCP

from .screener_service import ScreenerService

mcp = FastMCP("Screener Tools")
screener_service = ScreenerService()


@mcp.tool(
    name="get_volume_anomalies",
    description=(
        "Return symbols whose latest volume is strictly greater than "
        "the rolling average volume over a given window multiplied by a multiplier. "
        "Useful for detecting unusual trading activity."
    ),
)
def get_volume_anomalies(
    limit: int = 3,
    window: int = 20,
    multiplier: float = 2.0,
) -> List[str]:
    return screener_service.get_volume_anomalies(
        limit=limit, window=window, multiplier=multiplier,
    )


@mcp.tool(
    name="get_top_movers",
    description=(
        "Return the top gainers and top losers based on the percentage change "
        "between the last two closing prices. "
        "Useful for spotting the biggest daily price movements."
    ),
)
def get_top_movers(limit: int = 3) -> Dict[str, List[str]]:
    return screener_service.get_top_movers(limit=limit)


@mcp.prompt(name="daily_screening")
def daily_screening(target_limit: str) -> str:
    return (
        f"Run a full daily screening with a limit of {target_limit} symbols. "
        "First call get_volume_anomalies, then call get_top_movers, both with "
        f"limit={target_limit}. Compile a clear, concise list of all retained symbols."
    )


if __name__ == "__main__":
    mcp.run()