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


async def run_sub_agent(
    config_name: str,
    user_prompt: str,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    """Spawn a sub-agent. When *prompt_name* is provided the agent uses the
    named MCP prompt exposed by the sub-agent's server instead of sending
    *user_prompt* as free-form text.  *user_prompt* is still required so
    the orchestrator can fall back gracefully if the prompt is not found."""
    cfg = _load_agent_config(config_name)
    system_prompt = cfg.get("system_prompt", "")
    mcp_server_scripts = cfg.get("mcp_server_scripts", [])
    model = cfg.get("model", "gpt-4o")

    configuration = Configuration.from_env(
        mcp_server_scripts=mcp_server_scripts,
        system_prompt=system_prompt,
        model=model,
    )
    llm_client = LlmClient(
        api_key=configuration.api_key,
        model=configuration.model,
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
        if prompt_name:
            response = await agent.run_prompt(prompt_name, prompt_arguments)
        else:
            response = await agent.chat(user_prompt)
    finally:
        await agent.disconnect()
    return response


@mcp.tool(
    name="ask_macd_analyst",
    description=(
        "Delegate a question to the MACD technical analysis sub-agent. "
        "Use this for any MACD-related analysis request.\n"
        "Available prompts (pass as prompt_name):\n"
        "  - full_macd_analysis(symbol): complete MACD analysis workflow\n"
        "  - crossover_check(symbol): quick check for recent crossovers\n"
        "  - divergence_scan(symbol): detect and interpret MACD divergences\n"
        "  - macd_momentum_comparison(symbols): compare momentum across symbols\n"
        "When a prompt fits the request, prefer using prompt_name over a free-form question."
    ),
)
async def ask_macd_analyst(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="macd", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )


@mcp.tool(
    name="ask_rsi_analyst",
    description=(
        "Delegate a question to the RSI technical analysis sub-agent. "
        "Use this for any RSI-related analysis request.\n"
        "Available prompts (pass as prompt_name):\n"
        "  - full_rsi_analysis(symbol): complete RSI analysis workflow\n"
        "  - overbought_oversold_scan(symbol): detect RSI extremes\n"
        "  - divergence_scan(symbol): detect RSI divergences\n"
        "  - failure_swing_detection(symbol): detect failure swings\n"
        "  - multi_timeframe_trend(symbol): multi-timeframe RSI trend\n"
        "When a prompt fits the request, prefer using prompt_name over a free-form question."
    ),
)
async def ask_rsi_analyst(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="rsi", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )


@mcp.tool(
    name="ask_pivot_analyst",
    description=(
        "Delegate a question to the Pivot Points technical analysis sub-agent. "
        "Use this for any pivot-related analysis request.\n"
        "Available prompts (pass as prompt_name):\n"
        "  - full_pivot_analysis(symbol): complete pivot analysis workflow\n"
        "  - pivot_interaction_check(symbol): detect recent price interactions with pivot levels\n"
        "When a prompt fits the request, prefer using prompt_name over a free-form question."
    ),
)
async def ask_pivot_analyst(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="pivot", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )


@mcp.tool(
    name="ask_fundamental_analyst",
    description=(
        "Delegate a question to the fundamental analysis sub-agent. "
        "Use this for financial statements, ratios, margins, or dividend analysis.\n"
        "Available prompts (pass as prompt_name):\n"
        "  - full_fundamental_analysis(symbol): complete fundamental analysis\n"
        "  - fundamental_comparison(symbols): compare fundamentals across symbols\n"
        "When a prompt fits the request, prefer using prompt_name over a free-form question."
    ),
)
async def ask_fundamental_analyst(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="fundamentals", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )


@mcp.tool(
    name="ask_news_analyst",
    description=(
        "Delegate a question to the news sentiment analysis sub-agent. "
        "Use this to assess recent news sentiment and expected volatility for symbols.\n"
        "No named prompts available — use a free-form question."
    ),
)
async def ask_news_analyst(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="news", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )


@mcp.tool(
    name="ask_screener",
    description=(
        "Delegate a question to the screener sub-agent. "
        "Use this to detect volume anomalies and top daily movers.\n"
        "Available prompts (pass as prompt_name):\n"
        "  - daily_screening(target_limit): daily market screening\n"
        "When a prompt fits the request, prefer using prompt_name over a free-form question."
    ),
)
async def ask_screener(
    question: str | None = None,
    prompt_name: str | None = None,
    prompt_arguments: dict | None = None,
) -> str:
    if question is None:
        import json
        question = (
            f"Run prompt '{prompt_name}' with args {json.dumps(prompt_arguments or {})}"
            if prompt_name
            else ""
        )
    return await run_sub_agent(
        config_name="screener", user_prompt=question,
        prompt_name=prompt_name, prompt_arguments=prompt_arguments,
    )

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
        f"1. **MACD** — Call ask_macd_analyst with prompt_name=\"full_macd_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"2. **RSI** — Call ask_rsi_analyst with prompt_name=\"full_rsi_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"3. **Pivot Points** — Call ask_pivot_analyst with prompt_name=\"full_pivot_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"4. **Fundamentals** — Call ask_fundamental_analyst with prompt_name=\"full_fundamental_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"5. **News** — Call ask_news_analyst with question='Analyse recent news sentiment for {symbol} and assess short-term volatility risk.'\n\n"
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
        "1. **Screen the market** — Call ask_screener with prompt_name=\"daily_screening\", prompt_arguments={\"target_limit\": \"3\"}\n"
        "2. **Select candidates** — From the screener output, pick the top 2-3 symbols showing both price movement AND volume confirmation.\n"
        "3. **Technical check** — For each selected symbol:\n"
        "   a. Call ask_macd_analyst with prompt_name=\"crossover_check\", prompt_arguments={\"symbol\": \"<SYMBOL>\"}\n"
        "   b. Call ask_rsi_analyst with prompt_name=\"overbought_oversold_scan\", prompt_arguments={\"symbol\": \"<SYMBOL>\"}\n"
        "4. **News filter** — Call ask_news_analyst with question='Check news sentiment for [selected symbols list]. Flag any with negative or high-risk headlines.'\n"
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
    sym_list = [s.strip() for s in symbols.split(",")]
    steps = [f"Run a risk scan for the following symbols: {symbols}.\n"]
    for s in sym_list:
        steps.append(
            f"- Call ask_rsi_analyst with prompt_name=\"overbought_oversold_scan\", prompt_arguments={{\"symbol\": \"{s}\"}}"
        )
        steps.append(
            f"- Call ask_macd_analyst with prompt_name=\"divergence_scan\", prompt_arguments={{\"symbol\": \"{s}\"}}"
        )
    steps.append(
        f"- Call ask_news_analyst with question='Flag any negative or uncertain news for {symbols}.'"
    )
    steps.append(
        "\n**Risk Report** — Compile a structured risk summary:\n"
        "   - 🔴 High risk: multiple bearish signals converging\n"
        "   - 🟡 Medium risk: one bearish signal or mixed signals\n"
        "   - 🟢 Low risk: no notable red flags\n"
        "   For each high-risk symbol, state the specific signals and suggest a stop-loss zone based on pivot S1/S2 levels.\n"
    )
    return "\n".join(steps)


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
        f"1. **Technical momentum** — Call ask_macd_analyst with prompt_name=\"macd_momentum_comparison\", prompt_arguments={{\"symbols\": \"{symbols}\"}}\n"
        f"2. **RSI positioning** — For each symbol, call ask_rsi_analyst with prompt_name=\"full_rsi_analysis\", prompt_arguments={{\"symbol\": \"<SYMBOL>\"}}\n"
        f"3. **Support/Resistance** — For each symbol, call ask_pivot_analyst with prompt_name=\"full_pivot_analysis\", prompt_arguments={{\"symbol\": \"<SYMBOL>\"}}\n"
        f"4. **Fundamentals** — Call ask_fundamental_analyst with prompt_name=\"fundamental_comparison\", prompt_arguments={{\"symbols\": \"{symbols}\"}}\n"
        f"5. **News** — Call ask_news_analyst with question='Compare news sentiment for {symbols}.'\n\n"
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
        f"1. **Fundamentals deep-dive** — Call ask_fundamental_analyst with prompt_name=\"full_fundamental_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"2. **News & analyst sentiment** — Call ask_news_analyst with question="
        f"'What is the recent news sentiment for {symbol}? Are there any analyst upgrades/downgrades or earnings previews?'\n"
        f"3. **Technical positioning** — Call ask_rsi_analyst with prompt_name=\"overbought_oversold_scan\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n"
        f"4. **Pivot levels** — Call ask_pivot_analyst with prompt_name=\"full_pivot_analysis\", prompt_arguments={{\"symbol\": \"{symbol}\"}}\n\n"
        "5. **Pre-earnings summary**:\n"
        "   - Fundamental health: bullish / neutral / bearish\n"
        "   - Market sentiment: positive / mixed / negative\n"
        "   - Technical setup: breakout candidate / range-bound / exhaustion risk\n"
        "   - Key pivot levels to watch post-earnings\n"
        "   - Probability assessment: beat / in-line / miss (qualitative)\n"
    )


if __name__ == "__main__":
    mcp.run()