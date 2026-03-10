from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import Context, FastMCP

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from services.rsi_service import RSIService

mcp = FastMCP("RSI Tools")
rsi_service = RSIService()

_DEFAULT_LOOKBACK_DAYS = 60 


def _default_start() -> str:
    """Return an ISO date string (used when no start is provided)."""
    return (datetime.now() - timedelta(days=_DEFAULT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

_RSI_RESOURCES_DIR = Path(__file__).resolve().parent.parent.parent / "database" / "ressources" / "rsi"

# ──────────────────────────────────────────────
# TOOLS
# ──────────────────────────────────────────────

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
        symbols=symbols, start=start or _default_start(), end=end,
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
        symbols=symbols, start=start or _default_start(), end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="find_divergences",
    description=(
        "Detect regular and hidden divergences between price peaks/troughs "
        "and RSI peaks/troughs. The 'symbols' parameter is REQUIRED — always "
        "pass a list like ['AIR.PA'] to avoid scanning the entire dataset."
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
        symbols=symbols, start=start or _default_start(), end=end,
        sample_rows=sample_rows,
    )


@mcp.tool(
    name="analyze_multi_timeframe_rsi",
    description=(
        "Compare the latest RSI value across resampled timeframes. "
        "Valid timeframe codes: '1D' (daily), '1W' (weekly), '1ME' (monthly). "
        "Do NOT use '1M' — use '1ME' for monthly. "
        "The 'symbols' parameter is REQUIRED."
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
        symbols=symbols, start=start or _default_start(), end=end,
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
        symbols=symbols, start=start or _default_start(), end=end,
        sample_rows=sample_rows,
    )

# ──────────────────────────────────────────────
# RESOURCES
# ──────────────────────────────────────────────

@mcp.resource(
    "rsi://knowledge/calculation-theory",
    description="RSI calculation theory and formulas."
)
def rsi_calculation_theory() -> str:
    return (_RSI_RESOURCES_DIR / "rsi_calculation_theory.md").read_text(encoding="utf-8")


@mcp.resource(
    "rsi://knowledge/divergences-guide",
    description="Guide to RSI divergences."
)
def rsi_divergences_guide() -> str:
    return (_RSI_RESOURCES_DIR / "rsi_divergences_guide.md").read_text(encoding="utf-8")


@mcp.resource(
    "rsi://knowledge/extremes-and-regimes",
    description="RSI extremes and regime analysis."
)
def rsi_extremes_and_regimes() -> str:
    return (_RSI_RESOURCES_DIR / "rsi_extremes_and_regimes.md").read_text(encoding="utf-8")


@mcp.resource(
    "rsi://knowledge/failure-swings",
    description="RSI failure-swing patterns."
)
def rsi_failure_swings() -> str:
    return (_RSI_RESOURCES_DIR / "rsi_failure_swings.md").read_text(encoding="utf-8")


@mcp.resource(
    "rsi://knowledge/multi-timeframe-analysis",
    description="Multi-timeframe RSI analysis."
)
def rsi_multi_timeframe_analysis() -> str:
    return (_RSI_RESOURCES_DIR / "rsi_multi_timeframe_analysis.md").read_text(encoding="utf-8")

# ──────────────────────────────────────────────
# PROMPTS
# ──────────────────────────────────────────────

@mcp.prompt(
    name="full_rsi_analysis",
    description="Step-by-step workflow for a comprehensive RSI analysis on a symbol.",
)
def full_rsi_analysis_prompt(symbol: str) -> str:
    return (
        f"Perform a comprehensive RSI analysis for {symbol}. Follow these steps:\n"
        f"1. Call compute_rsi with symbols=['{symbol}'].\n"
        f"2. Call detect_extremes with symbols=['{symbol}'].\n"
        f"3. Call find_divergences with symbols=['{symbol}'].\n"
        f"4. Call detect_failure_swings with symbols=['{symbol}'].\n"
        f"5. Call analyze_multi_timeframe_rsi with symbols=['{symbol}'] and timeframes=['1D','1W','1ME'].\n"
        "6. Read relevant knowledge resources to ground your interpretation.\n"
        "7. Synthesise a structured report: summary, key data, interpretation, recommendation."
    )


@mcp.prompt(
    name="overbought_oversold_scan",
    description="Workflow to screen symbols for overbought or oversold conditions.",
)
def overbought_oversold_scan_prompt(symbol: str) -> str:
    return (
        f"Screen all available CAC 40 symbols for overbought/oversold conditions "
        f"using defaults thresholds (OB=70 / OS=30).\n"
        f"1. Call `detect_extremes` with symbols=['{symbol}'] and a high sample_rows (e.g. 50) to capture recent events.\n"
        "2. Read the resource `rsi://knowledge/extremes-and-regimes` to understand regime-adjusted thresholds.\n"
        "3. Group results by symbol and zone (overbought / oversold).\n"
        "4. Rank symbols by how extreme their latest RSI reading is.\n"
        "5. Present a clear table and highlight the most actionable opportunities."
    )


@mcp.prompt(
    name="divergence_scan",
    description="Workflow to detect and interpret RSI divergences for a symbol.",
)
def divergence_scan_prompt(symbol: str) -> str:
    return (
        f"Detect and analyse RSI divergences for {symbol}.\n"
        f"1. Call find_divergences with symbols=['{symbol}'].\n"
        "2. Read the resource rsi://knowledge/divergences-guide.\n"
        "3. For each divergence found, explain type, dates, price/RSI levels, and trend implication.\n"
        "4. Conclude with the strongest signal and a trading recommendation."
    )


@mcp.prompt(
    name="failure_swing_detection",
    description="Workflow to find and interpret RSI failure-swing reversal patterns.",
)
def failure_swing_detection_prompt(symbol: str) -> str:
    return (
        f"Detect RSI failure-swing patterns for {symbol}.\n"
        f"1. Call detect_failure_swings with symbols=['{symbol}'].\n"
        "2. Read the resource rsi://knowledge/failure-swings.\n"
        "3. For each failure swing, explain type, key RSI levels, and trigger date.\n"
        "4. Assess whether any recent failure swing is still active or confirmed."
    )


@mcp.prompt(
    name="multi_timeframe_trend",
    description="Workflow to assess trend strength via multi-timeframe RSI analysis.",
)
def multi_timeframe_trend_prompt(symbol: str) -> str:
    return (
        f"Assess trend strength for {symbol} using multi-timeframe RSI.\n"
        f"1. Call analyze_multi_timeframe_rsi with symbols=['{symbol}'] and timeframes=['1D','1W','1ME'].\n"
        "2. Read the resource rsi://knowledge/multi-timeframe-analysis.\n"
        "3. Determine if RSI across timeframes is aligned (all bullish, all bearish, or mixed).\n"
        "4. Apply top-down rules: monthly RSI > 50 = bullish, weekly confirms/contradicts, daily for timing.\n"
        "5. Conclude with overall trend assessment and confidence level."
    )


if __name__ == "__main__":
    mcp.run()
