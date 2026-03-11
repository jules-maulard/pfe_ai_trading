from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import yaml

# Ensure project root and src are on sys.path so imports work when running directly
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
from src.agents.llm_client import LlmClient
from src.agents.memory import Memory
from src.agents.server import Server
from src.agents.token_monitor import TokenMonitor


async def run_from_config(config_path: str) -> None:
    cfg_text = Path(config_path).read_text(encoding="utf-8")
    cfg = yaml.safe_load(cfg_text)

    mcp_server_scripts = cfg.get("mcp_server_scripts", [])
    system_prompt = cfg.get("system_prompt")
    agent_name = cfg.get("agent_name")

    configuration = Configuration.from_env(
        mcp_server_scripts=mcp_server_scripts,
        system_prompt=system_prompt,
    )

    llm_client = LlmClient(
        api_key=configuration.api_key,
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
        for script in configuration.mcp_server_scripts
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
        cli = CliInterface(agent, agent_name=agent_name)
        await cli.run()
    finally:
        await agent.disconnect()


def main(config_path: str | None = None) -> None:
    if config_path is None:
        config_path = Path(__file__).resolve().parent / "configs" / "macd.yaml"
    asyncio.run(run_from_config(str(config_path)))


def cli_entry() -> None:
    parser = argparse.ArgumentParser(description="Run an agent from a YAML config or agent name")
    parser.add_argument("--config", "-c", help="Path to YAML config file")
    parser.add_argument("--agent", "-a", help="Agent short name (looks up configs/<agent>.yaml)")
    args = parser.parse_args()

    cfg_path: Path
    if args.config:
        cfg_path = Path(args.config)
    elif args.agent:
        cfg_path = Path(__file__).resolve().parent / "configs" / f"{args.agent}.yaml"
    else:
        cfg_path = Path(__file__).resolve().parent / "configs" / "macd.yaml"

    if not cfg_path.exists():
        raise SystemExit(f"Config not found: {cfg_path}")

    main(str(cfg_path))


if __name__ == "__main__":
    cli_entry()
