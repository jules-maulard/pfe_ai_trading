from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from utils.logger import get_logger

logger = get_logger(__name__)

from src.agents.agent import Agent


class CliInterface:
    QUIT_COMMANDS = {"/quit", "/exit", "/stop", "/q"}

    def __init__(self, agent: Agent, agent_name: str = "MCP Agent") -> None:
        self._agent = agent
        self._agent_name = agent_name

    async def run(self) -> None:
        self._print_banner()

        while True:
            try:
                user_input = input("You > ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not user_input:
                continue

            if user_input.lower() in self.QUIT_COMMANDS:
                print("Goodbye!")
                break

            if user_input.lower().startswith("/"):
                await self._handle_command(user_input)
                continue

            try:
                response = await self._agent.chat(user_input)
                print(f"\nAgent > {response}\n")
            except Exception as exc:
                logger.error("Chat error: %s", exc, exc_info=True)

    async def _handle_command(self, command: str) -> None:
        lower = command.lower()

        if lower == "/reset":
            await self._agent.reset_conversation()
            print("Conversation reset.")
            return

        if lower == "/tools":
            self._list_tools()
            return

        if lower == "/resources":
            self._list_resources()
            return

        if lower == "/prompts":
            self._list_prompts()
            return

        if lower.startswith("/prompt "):
            await self._execute_prompt(command)
            return

        print(f"Unknown command: {command}")

    def _list_tools(self) -> None:
        tools = self._agent.tools
        if not tools:
            print("No tools available.")
            return
        for tool in tools:
            print(f"  - {tool.name} — {tool.description[:80]}")

    def _list_resources(self) -> None:
        resources = self._agent.resources
        if not resources:
            print("No resources available.")
            return
        for resource in resources:
            desc = getattr(resource, "description", "") or ""
            name = getattr(resource, "name", str(resource))
            uri = getattr(resource, "uri", "")
            print(f"  - {name} ({uri})" + (f" — {desc[:80]}" if desc else ""))

    def _list_prompts(self) -> None:
        prompts = self._agent.prompts
        if not prompts:
            print("No prompts available.")
            return
        for prompt in prompts:
            desc = prompt.description or ""
            arg_names = [a.name for a in (prompt.arguments or [])]
            params = f" <{'> <'.join(arg_names)}>" if arg_names else ""
            print(f"  - /prompt {prompt.name}{params}")
            if desc:
                print(f"    {desc[:100]}")

    async def _execute_prompt(self, command: str) -> None:
        parts = command.split(None, 2)
        if len(parts) < 2:
            print("Usage: /prompt <name> [key=value ...]")
            return
        prompt_name = parts[1]
        prompt_args: Dict[str, str] = {}
        if len(parts) == 3:
            for kv in parts[2].split():
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    prompt_args[k] = v
                else:
                    all_prompts = self._agent.prompts
                    target = [p for p in all_prompts if p.name == prompt_name]
                    if target and target[0].arguments:
                        first_arg = target[0].arguments[0].name
                        prompt_args[first_arg] = kv
        try:
            response = await self._agent.run_prompt(prompt_name, prompt_args)
            print(f"\nAgent > {response}\n")
        except Exception as exc:
            logger.error("Prompt execution failed: %s", exc, exc_info=True)

    def _print_banner(self) -> None:
        print("\n" + "=" * 60)
        print(f"  {self._agent_name} — Technical Analysis Assistant")
        print("=" * 60)
        print("Commands:")
        print("  /reset      — Reset conversation")
        print("  /tools      — List available tools")
        print("  /resources  — List available resources")
        print("  /prompts    — List available prompt workflows")
        print("  /prompt <name> [args] — Run a prompt workflow")
        print("  /quit       — Quit")
        print("=" * 60 + "\n")
