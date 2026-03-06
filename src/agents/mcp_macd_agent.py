from __future__ import annotations

import asyncio
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

# Ensure both project root and src directory are on sys.path so
# imports using `src.*` and bare `utils.*` both work when running
# this script directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
_ROOT = str(PROJECT_ROOT)
_SRC_DIR = str(SRC_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from src.agents.entities import Configuration
from src.agents.agent import Agent
from src.agents.cli_interface import CliInterface

MCP_SERVER_SCRIPT = "src/mcp_servers/mcp_macd_server.py"

BASE_SYSTEM_PROMPT = """\
You are an expert financial technical analysis assistant specialised in the \
MACD (Moving Average Convergence Divergence) indicator. You analyse CAC 40 \
equities using local OHLCV data served through an MCP tool server.

# Available data
- Source: database/ohlcv.csv (daily OHLCV bars from yfinance)
- Symbols: full CAC 40 index (AIR.PA, DG.PA, SU.PA, MC.PA, BNP.PA, …)
- Columns: symbol, date, open, high, low, close, volume

# Interpretation guidelines
After computing the MACD, analyse the returned data looking for:
1. **Crossovers**: MACD line crossing above signal → bullish; below → bearish.
2. **Histogram direction**: Growing positive bars → strengthening bullish momentum; \
   growing negative bars → strengthening bearish momentum.
3. **Zero-line crossover**: MACD crossing above 0 → shift to bullish trend; below 0 → bearish.
4. **Divergences**: Price making new highs while MACD makes lower highs (bearish divergence) \
   or price making new lows while MACD makes higher lows (bullish divergence).

# Output format
- Start with a short **summary** (1-2 sentences: bullish / bearish / neutral).
- Present key figures in a **data table** (date, MACD, signal, histogram).
- Provide a detailed **interpretation** of the current MACD state.
- End with an **actionable recommendation** (and caveats).

# Rules
- NEVER invent data. Always base your analysis on actual tool results.
- If a tool returns an error, report it clearly and suggest a fix.
- Respond in the **same language** as the user.
- Be precise and concise.
"""


async def main() -> None:
    configuration = Configuration.from_env(
        mcp_server_scripts=[MCP_SERVER_SCRIPT],
        system_prompt=BASE_SYSTEM_PROMPT,
    )
    agent = Agent(configuration)
    try:
        await agent.connect()
        cli = CliInterface(agent, agent_name="MACD MCP Agent")
        await cli.run()
    finally:
        await agent.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
