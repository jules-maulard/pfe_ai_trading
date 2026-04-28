from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
from fastmcp import FastMCP

from .pivot_service import PivotService
from ._validation import validate_symbols

mcp = FastMCP("Pivot Tools")
pivot_service = PivotService()


def _round_floats(value: Any, decimal_places: int = 4) -> Any:
    if isinstance(value, float):
        return round(value, decimal_places)
    if isinstance(value, dict):
        return {k: _round_floats(v, decimal_places) for k, v in value.items()}
    if isinstance(value, list):
        return [_round_floats(item, decimal_places) for item in value]
    return value

_PIVOT_RESOURCES_DIR = Path(__file__).resolve().parent.parent.parent / "database" / "ressources" / "pivot"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_OHLCV_PATH = _PROJECT_ROOT / "database" / "csv" / "ohlcv.csv"
_ASSET_PATH = _PROJECT_ROOT / "database" / "csv" / "asset.csv"


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@mcp.tool(
    name="list_symbols",
    description=(
        "Return all ticker symbols available in the local OHLCV database, "
        "together with their company names. "
        "ALWAYS call this tool first when the user mentions a company by name "
        "to resolve the exact ticker, "
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
    name="compute_pivots",
    description="Retrieve precomputed Standard Pivot Points (P, R1, S1, R2, S2, R3, S3) for symbols.",
)
def compute_pivots_tool(
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    if err := validate_symbols(symbols):
        return err
    return _round_floats(pivot_service.compute(
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    ))


@mcp.tool(
    name="detect_pivot_interaction",
    description=(
        "Detect when the closing price is near or crossing a pivot level. "
        "Returns interaction events with distance percentage and direction. "
        "The 'symbols' parameter is REQUIRED."
    ),
)
def detect_pivot_interaction_tool(
    proximity_pct: float = 0.5,
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    if err := validate_symbols(symbols):
        return err
    return _round_floats(pivot_service.detect_pivot_interaction(
        proximity_pct=proximity_pct,
        symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    ))


@mcp.tool(
    name="get_pivot_context",
    description=(
        "Return a compact Pivot Points interpretation guide covering levels, "
        "interactions and pitfalls. Call this before synthesising any analysis report."
    ),
)
def get_pivot_context() -> Dict[str, Any]:
    return {
        "status": "ok",
        "content": (_PIVOT_RESOURCES_DIR / "pivot_quick_reference.md").read_text(encoding="utf-8"),
    }


# ──────────────────────────────────────────────
# RESOURCES
# ──────────────────────────────────────────────

@mcp.resource(
    "pivot://knowledge/calculation-theory",
    description="Pivot Points calculation theory and formulas.",
)
def pivot_calculation_theory() -> str:
    return (_PIVOT_RESOURCES_DIR / "pivot_calculation_theory.md").read_text(encoding="utf-8")


@mcp.resource(
    "pivot://knowledge/interaction-guide",
    description="Guide to pivot point support/resistance interactions.",
)
def pivot_interaction_guide() -> str:
    return (_PIVOT_RESOURCES_DIR / "pivot_interaction_guide.md").read_text(encoding="utf-8")


@mcp.resource(
    "pivot://knowledge/quick-reference",
    description="Compact summary of Pivot Points concepts (levels, interactions, pitfalls). Token-efficient.",
)
def pivot_quick_reference() -> str:
    return (_PIVOT_RESOURCES_DIR / "pivot_quick_reference.md").read_text(encoding="utf-8")


# ──────────────────────────────────────────────
# PROMPTS
# ──────────────────────────────────────────────

@mcp.prompt(
    name="full_pivot_analysis",
    description="Step-by-step workflow for a comprehensive Pivot Points analysis on a symbol.",
)
def full_pivot_analysis_prompt(symbol: str) -> str:
    return (
        f"Perform a comprehensive Pivot Points analysis for {symbol}. Follow these steps:\n"
        f"1. Call compute_pivots with symbols=['{symbol}'] and sample_rows=5.\n"
        f"2. Call detect_pivot_interaction with symbols=['{symbol}'] and sample_rows=5.\n"
        "3. Call get_pivot_context to ground your interpretation.\n"
        "4. Synthesise a structured report: current levels, nearby interactions, "
        "support/resistance bias, and conclusion."
    )


@mcp.prompt(
    name="pivot_interaction_check",
    description="Quick check for recent pivot level interactions on a symbol.",
)
def pivot_interaction_check_prompt(symbol: str) -> str:
    return (
        f"Check if {symbol} is currently near any pivot point level:\n"
        f"1. Call detect_pivot_interaction with symbols=['{symbol}'] and proximity_pct=0.5.\n"
        "2. Summarise which levels the price is interacting with and the likely direction."
    )


if __name__ == "__main__":
    mcp.run()
