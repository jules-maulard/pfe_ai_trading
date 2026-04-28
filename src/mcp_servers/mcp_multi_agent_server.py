from __future__ import annotations

from pathlib import Path

import yaml
from fastmcp import FastMCP

from src.agents.agent import Agent
from src.agents.entities import Configuration
from src.agents.llm_client import LlmClient
from src.agents.memory import Memory
from src.agents.server import Server
from src.agents.token_monitor import TokenMonitor

mcp = FastMCP("Multi-Agent Orchestrator")

_CONFIGS_DIR = Path(__file__).resolve().parents[1] / "agents" / "configs"


def _load_agent_config(config_name: str) -> dict:
    return yaml.safe_load((_CONFIGS_DIR / f"{config_name}.yaml").read_text(encoding="utf-8"))


async def run_sub_agent(config_name: str, user_prompt: str) -> str:
    cfg = _load_agent_config(config_name)
    system_prompt = cfg.get("system_prompt", "")
    mcp_server_scripts = cfg.get("mcp_server_scripts", [])
    model = cfg.get("model", "openai/gpt-oss-20b")

    configuration = Configuration.from_env(
        mcp_server_scripts=mcp_server_scripts,
        system_prompt=system_prompt,
        model=model,
    )
    llm_client = LlmClient(
        api_keys=configuration.api_keys,
        model=configuration.model,
        max_retries=configuration.max_retries,
        retry_delay=configuration.retry_delay,
    )
    servers = [
        Server(
            mcp_server_script=script,
            max_retries=configuration.max_retries,
            retry_delay=configuration.retry_delay,
            tool_call_timeout=configuration.tool_call_timeout,
        )
        for script in mcp_server_scripts
    ]
    agent = Agent(
        configuration=configuration,
        llm_client=llm_client,
        servers=servers,
        memory=Memory(),
        token_monitor=TokenMonitor(),
    )
    try:
        await agent.connect()
        response = await agent.chat(user_prompt)
    finally:
        await agent.disconnect()
    return response


@mcp.tool(
    name="ask_macd_analyst",
    description=(
        "Delegate a question to the MACD technical analysis sub-agent. "
        "Use this for any MACD-related analysis request."
    ),
)
async def ask_macd_analyst(question: str) -> str:
    return await run_sub_agent(config_name="macd", user_prompt=question)


@mcp.tool(
    name="ask_rsi_analyst",
    description=(
        "Delegate a question to the RSI technical analysis sub-agent. "
        "Use this for any RSI-related analysis request."
    ),
)
async def ask_rsi_analyst(question: str) -> str:
    return await run_sub_agent(config_name="rsi", user_prompt=question)


@mcp.tool(
    name="ask_pivot_analyst",
    description=(
        "Delegate a question to the Pivot Points technical analysis sub-agent. "
        "Use this for any pivot-related analysis request."
    ),
)
async def ask_pivot_analyst(question: str) -> str:
    return await run_sub_agent(config_name="pivot", user_prompt=question)


@mcp.tool(
    name="ask_fundamental_analyst",
    description=(
        "Delegate a question to the fundamental analysis sub-agent. "
        "Use this for financial statements, ratios, margins, or dividend analysis."
    ),
)
async def ask_fundamental_analyst(question: str) -> str:
    return await run_sub_agent(config_name="fundamentals", user_prompt=question)


@mcp.tool(
    name="ask_news_analyst",
    description=(
        "Delegate a question to the news sentiment analysis sub-agent. "
        "Use this to assess recent news sentiment and expected volatility for symbols."
    ),
)
async def ask_news_analyst(question: str) -> str:
    return await run_sub_agent(config_name="news", user_prompt=question)


@mcp.tool(
    name="ask_screener",
    description=(
        "Delegate a question to the screener sub-agent. "
        "Use this to detect volume anomalies and top daily movers."
    ),
)
async def ask_screener(question: str) -> str:
    return await run_sub_agent(config_name="screener", user_prompt=question)

# ──────────────────────────────────────────────
# PROMPTS
# ──────────────────────────────────────────────

@mcp.prompt(
    name="deep_symbol_analysis",
    description=(
        "Full 360° analysis of a single symbol: technicals (MACD, RSI, Pivots), "
        "fundamentals, and latest news sentiment. Use this when the user asks for "
        "a comprehensive or detailed analysis of one stock."
    ),
)
def deep_symbol_analysis_prompt(symbol: str) -> str:
    return (
        f"Perform a complete 360° analysis of {symbol}. Follow these steps in order:\n\n"
        f"1. **MACD** — Ask ask_macd_analyst: 'Full MACD analysis for {symbol}: compute MACD, detect crossovers and divergences.'\n"
        f"2. **RSI** — Ask ask_rsi_analyst: 'Full RSI analysis for {symbol}: compute RSI, detect extremes, divergences, failure swings, and multi-timeframe trend.'\n"
        f"3. **Pivot Points** — Ask ask_pivot_analyst: 'Full pivot analysis for {symbol}: compute pivot levels and detect recent price interactions.'\n"
        f"4. **Fundamentals** — Ask ask_fundamental_analyst: 'Summarise key fundamentals for {symbol}: revenue, margins, EPS, P/E, debt ratios, and dividend yield.'\n"
        f"5. **News** — Ask ask_news_analyst: 'Analyse recent news sentiment for {symbol} and assess short-term volatility risk.'\n\n"
        "6. **Synthesis** — Combine all sub-agent responses into a structured report:\n"
        "   - Executive summary (2-3 sentences)\n"
        "   - Technical outlook table (MACD / RSI / Pivots signals)\n"
        "   - Fundamental health scorecard\n"
        "   - News sentiment + volatility flag\n"
        "   - Overall recommendation (Strong Buy / Buy / Hold / Sell / Strong Sell) with rationale\n"
        "   - Key risks to watch\n"
    )


@mcp.prompt(
    name="daily_investment_pick",
    description=(
        "Answer 'what should I invest in today?': screens the market for top movers and "
        "volume anomalies, then cross-validates the best candidates with technical and news "
        "analysis before issuing a ranked shortlist."
    ),
)
def daily_investment_pick_prompt() -> str:
    return (
        "The user wants to know what to invest in today. Follow this workflow:\n\n"
        "1. **Screen the market** — Ask ask_screener: 'Run a daily screening: get top 10 movers and volume anomalies with limit=10.'\n"
        "2. **Select candidates** — From the screener output, pick the top 3-5 symbols showing both price movement AND volume confirmation.\n"
        "3. **Technical check** — For each selected symbol:\n"
        "   a. Ask ask_macd_analyst: 'Is the MACD momentum bullish for {symbol}? Detect recent crossovers.'\n"
        "   b. Ask ask_rsi_analyst: 'Is RSI in a favorable zone for {symbol}? Check for extremes and divergences.'\n"
        "4. **News filter** — Ask ask_news_analyst: 'Check news sentiment for [selected symbols list]. Flag any with negative or high-risk headlines.'\n"
        "5. **Rank and present** — Produce a ranked shortlist table with columns:\n"
        "   | Rank | Symbol | Price Move | Volume Signal | MACD | RSI | News Sentiment | Confidence |\n"
        "6. **Top pick** — Highlight the #1 opportunity with a 2-3 sentence rationale and the main risk.\n"
        "Today's date: use the most recent data available in the database.\n"
    )


@mcp.prompt(
    name="risk_scan",
    description=(
        "Identify overextended or at-risk positions across the market: detects RSI overbought/oversold "
        "extremes, MACD bearish divergences, and negative news — useful for risk management."
    ),
)
def risk_scan_prompt(symbols: str) -> str:
    return (
        f"Run a risk scan for the following symbols: {symbols}.\n\n"
        f"1. **RSI Extremes** — Ask ask_rsi_analyst: 'Detect RSI overbought (>70) and oversold (<30) conditions for {symbols}. Include failure swings.'\n"
        f"2. **MACD Divergences** — Ask ask_macd_analyst: 'Find bearish MACD divergences for {symbols}.'\n"
        f"3. **News Risk** — Ask ask_news_analyst: 'Flag any negative or uncertain news for {symbols}.'\n\n"
        "4. **Risk Report** — Compile a structured risk summary:\n"
        "   - 🔴 High risk: multiple bearish signals converging\n"
        "   - 🟡 Medium risk: one bearish signal or mixed signals\n"
        "   - 🟢 Low risk: no notable red flags\n"
        "   For each high-risk symbol, state the specific signals and suggest a stop-loss zone based on pivot S1/S2 levels.\n"
    )


@mcp.prompt(
    name="compare_symbols",
    description=(
        "Side-by-side comparison of 2-5 symbols across all dimensions "
        "(technicals, fundamentals, news) to help choose between them."
    ),
)
def compare_symbols_prompt(symbols: str) -> str:
    return (
        f"Compare the following symbols side by side: {symbols}.\n\n"
        f"1. **Technical momentum** — Ask ask_macd_analyst: 'Compare MACD momentum for {symbols}. Rank from most bullish to most bearish.'\n"
        f"2. **RSI positioning** — Ask ask_rsi_analyst: 'Compare RSI levels and trends for {symbols}. Which are in healthy zones vs. extremes?'\n"
        f"3. **Support/Resistance** — Ask ask_pivot_analyst: 'Compare current price position relative to pivot levels for {symbols}.'\n"
        f"4. **Fundamentals** — Ask ask_fundamental_analyst: 'Compare key ratios for {symbols}: P/E, revenue growth, profit margin, dividend yield.'\n"
        f"5. **News** — Ask ask_news_analyst: 'Compare news sentiment for {symbols}.'\n\n"
        "6. **Comparison table** — Produce a single consolidated table:\n"
        "   | Metric | " + " | ".join(symbols.split(",")) + " |\n"
        "   (rows: MACD signal, RSI level, Pivot bias, P/E, Rev Growth, Margin, Dividend, News)\n"
        "7. **Verdict** — State which symbol offers the best risk/reward today and why.\n"
    )


@mcp.prompt(
    name="earnings_watchlist",
    description=(
        "Pre-earnings due-diligence for a symbol: fundamentals deep-dive combined with "
        "news sentiment to assess whether the stock is positioned for a positive or negative surprise."
    ),
)
def earnings_watchlist_prompt(symbol: str) -> str:
    return (
        f"Run a pre-earnings due-diligence for {symbol}.\n\n"
        f"1. **Fundamentals deep-dive** — Ask ask_fundamental_analyst: "
        f"'Analyse {symbol} financials in detail: revenue trend (YoY), EPS growth, operating margin, "
        f"net margin, free cash flow, debt-to-equity, and any recent dividend changes.'\n"
        f"2. **News & analyst sentiment** — Ask ask_news_analyst: "
        f"'What is the recent news sentiment for {symbol}? Are there any analyst upgrades/downgrades or earnings previews?'\n"
        f"3. **Technical positioning** — Ask ask_rsi_analyst: "
        f"'What is the RSI trend for {symbol} going into earnings? Is it overbought or oversold?'\n"
        f"4. **Pivot levels** — Ask ask_pivot_analyst: "
        f"'What are the key support and resistance levels for {symbol} based on pivot points?'\n\n"
        "5. **Pre-earnings summary**:\n"
        "   - Fundamental health: bullish / neutral / bearish\n"
        "   - Market sentiment: positive / mixed / negative\n"
        "   - Technical setup: breakout candidate / range-bound / exhaustion risk\n"
        "   - Key pivot levels to watch post-earnings\n"
        "   - Probability assessment: beat / in-line / miss (qualitative)\n"
    )


if __name__ == "__main__":
    mcp.run()