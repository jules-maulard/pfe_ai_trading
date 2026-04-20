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

if __name__ == "__main__":
    mcp.run()