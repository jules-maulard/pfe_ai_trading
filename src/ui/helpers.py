import sys
import asyncio
from pathlib import Path

import pandas as pd
from functools import lru_cache

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.data.config.settings import get_storage
from src.agents.agent_runner import run_from_config
from src.agents.entities import Configuration, Message
from src.agents.llm_client import LlmClient
from src.agents.server import Server
from src.agents.agent import Agent
from src.agents.memory import Memory
from src.agents.token_monitor import TokenMonitor

AGENTS = {
    "MACD": str(ROOT / "src/agents/configs/macd.yaml"),
    "RSI": str(ROOT / "src/agents/configs/rsi.yaml"),
    "Pivot Points": str(ROOT / "src/agents/configs/pivot.yaml"),
    "News": str(ROOT / "src/agents/configs/news.yaml"),
    "Fundamentals": str(ROOT / "src/agents/configs/fundamentals.yaml"),
}

INDICATORS = ["RSI", "MACD", "Pivot"]


def load_yaml(path: str) -> dict:
    import yaml
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(coro)
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


async def ask_agent(config_path: str, history: list[dict], user_input: str) -> str:
    cfg = load_yaml(config_path)
    configuration = Configuration.from_env(
        mcp_server_scripts=cfg.get("mcp_server_scripts", []),
        system_prompt=cfg.get("system_prompt", ""),
        model=cfg.get("model", "openai/gpt-oss-20b"),
    )
    llm_client = LlmClient(
        api_keys=configuration.api_keys,
        model=configuration.model,
        max_retries=configuration.max_retries,
        retry_delay=configuration.retry_delay,
    )
    servers = [
        Server(
            mcp_server_script=s,
            max_retries=configuration.max_retries,
            retry_delay=configuration.retry_delay,
            tool_call_timeout=configuration.tool_call_timeout,
        )
        for s in configuration.mcp_server_scripts
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
        for msg in history:
            agent._memory.add_message(Message(role=msg["role"], content=msg["content"]))
        return await agent.chat(user_input)
    finally:
        await agent.disconnect()


def load_ohlcv(symbols: list[str], start: str, end: str) -> pd.DataFrame:
    return get_storage().load_ohlcv(symbols=symbols, start=start, end=end)


def load_indicator(name: str, symbols: list[str], start: str, end: str) -> pd.DataFrame:
    return get_storage().load_indicator(indicator_name=name.lower(), symbols=symbols, start=start, end=end)


@lru_cache(maxsize=1)
def list_symbols() -> list[str]:
    """Return list of available OHLCV symbols.

    Cached to avoid expensive storage queries on every Streamlit re-render.
    """
    try:
        return sorted(get_storage().list_symbols("ohlcv"))
    except Exception:
        return []


def load_fundamental(statement_type: str, symbols: list[str], start: str | None = None, end: str | None = None) -> pd.DataFrame:
    return get_storage().load_fundamental(statement_type=statement_type, symbols=symbols, start=start, end=end)
