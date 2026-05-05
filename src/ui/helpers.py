import os
import sys
import asyncio
from datetime import datetime, timezone
from pathlib import Path

import requests as _requests

import pandas as pd
from functools import lru_cache

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.data.config.settings import get_storage
from src.agents.entities import Configuration
from src.agents.llm_client import LlmClient
from src.agents.server import Server
from src.agents.agent import Agent
from src.agents.memory import Memory
from src.agents.token_monitor import TokenMonitor

AGENTS = {
    "Orchestrator": str(ROOT / "src/agents/configs/orchestrator.yaml"),
    "MACD": str(ROOT / "src/agents/configs/macd.yaml"),
    "RSI": str(ROOT / "src/agents/configs/rsi.yaml"),
    "Pivot Points": str(ROOT / "src/agents/configs/pivot.yaml"),
    "News": str(ROOT / "src/agents/configs/news.yaml"),
    "Fundamentals": str(ROOT / "src/agents/configs/fundamentals.yaml"),
}

INDICATORS = ["RSI", "MACD", "Pivot"]


@lru_cache(maxsize=None)
def load_yaml(path: str) -> dict:
    import yaml
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


async def build_agent(config_path: str) -> Agent:
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
    await agent.connect()
    return agent


import threading

_bg_loop: asyncio.AbstractEventLoop | None = None
_bg_thread: threading.Thread | None = None
_loop_lock = threading.Lock()


def _get_bg_loop() -> asyncio.AbstractEventLoop:
    global _bg_loop, _bg_thread
    with _loop_lock:
        if _bg_loop is None or _bg_loop.is_closed():
            _bg_loop = asyncio.new_event_loop()
            _bg_thread = threading.Thread(target=_bg_loop.run_forever, daemon=True)
            _bg_thread.start()
    return _bg_loop


def run_async(coro):
    loop = _get_bg_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


async def ask_agent(agent: Agent, user_input: str) -> str:
    return await agent.chat(user_input)


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


def get_pipeline_last_run_summary() -> str | None:
    """Retourne la date OHLCV la plus récente et son âge en jours (proxy du dernier run pipeline)."""
    try:
        storage = get_storage()
        symbols = list_symbols()
        if not symbols:
            return None
        last_dates = storage.get_last_dates("ohlcv", symbols)
        if not last_dates:
            return None
        max_date = max(last_dates.values())
        date_str = str(max_date)[:10]
        d = datetime.fromisoformat(date_str).date()
        age = (datetime.utcnow().date() - d).days
        if age == 0:
            age_text = "aujourd'hui"
        elif age == 1:
            age_text = "il y a 1 jour"
        else:
            age_text = f"il y a {age} jours"
        return f"{date_str} ({age_text})"
    except Exception:
        return None


def get_github_actions_summary(
    owner: str | None = None,
    repo: str | None = None,
    token: str | None = None,
) -> dict | None:
    """Interroge l'API GitHub pour récupérer le dernier run du workflow CI.

    Lit GITHUB_OWNER, GITHUB_REPO (ou GITHUB_REPOSITORY au format 'owner/repo'),
    et GITHUB_TOKEN depuis l'environnement. Retourne None si non configuré ou en cas d'erreur.
    """
    owner = owner or os.getenv("GITHUB_OWNER")
    repo = repo or os.getenv("GITHUB_REPO")
    gh_repo_env = os.getenv("GITHUB_REPOSITORY")
    if not (owner and repo) and gh_repo_env and "/" in gh_repo_env:
        owner, repo = gh_repo_env.split("/", 1)
    if not (owner and repo):
        return None

    token = token or os.getenv("GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs?per_page=1"
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "pfe-ai-trading/streamlit",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = _requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            return None
        run = runs[0]
        started_raw = run.get("run_started_at") or run.get("created_at")
        started_dt: datetime | None = None
        age_days: int | None = None
        if started_raw:
            started_dt = datetime.fromisoformat(started_raw.replace("Z", "+00:00"))
            age_days = (datetime.now(tz=timezone.utc) - started_dt).days
        return {
            "conclusion": run.get("conclusion"),
            "status": run.get("status"),
            "name": run.get("name"),
            "started_at": started_dt.strftime("%Y-%m-%d %H:%M UTC") if started_dt else None,
            "age_days": age_days,
            "html_url": run.get("html_url"),
            "run_id": run.get("id"),
            "owner": owner,
            "repo": repo,
        }
    except Exception:
        return None


def load_fundamental(statement_type: str, symbols: list[str], start: str | None = None, end: str | None = None) -> pd.DataFrame:
    if statement_type == "dividends":
        return get_storage().load_dividend(symbols=symbols, start=start, end=end)
    return get_storage().load_fundamental(statement_type=statement_type, symbols=symbols, start=start, end=end)
