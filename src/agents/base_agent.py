from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from utils.logger import get_logger

logger = get_logger(__name__)

try:
    from groq import AsyncGroq
except ImportError:
    logger.error("The 'groq' package is not installed. Run: pip install groq")
    sys.exit(1)

from fastmcp import Client


def mcp_tools_to_openai(mcp_tools) -> List[Dict[str, Any]]:
    openai_tools = []
    for tool in mcp_tools:
        func_def = {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description or "",
                "parameters": tool.inputSchema if tool.inputSchema else {"type": "object", "properties": {}},
            },
        }
        openai_tools.append(func_def)
    return openai_tools


class BaseMCPAgent:
    def __init__(self, mcp_server_script: str, system_prompt: str, model: str = "openai/gpt-oss-20b"):
        self.mcp_server_script = mcp_server_script
        self.base_system_prompt = system_prompt
        self.model = model
        self.groq = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY"))
        self.mcp_client: Client | None = None
        self.openai_tools: List[Dict[str, Any]] = []
        self.prompts: List[Dict[str, Any]] = []
        self.resources: list = []
        self.messages: List[Dict[str, Any]] = []

    async def connect(self):
        self.mcp_client = Client(self.mcp_server_script)
        await self.mcp_client.__aenter__()

        tools = await self.mcp_client.list_tools()
        self.openai_tools = mcp_tools_to_openai(tools)

        try:
            self.prompts = await self.mcp_client.list_prompts()
        except Exception:
            self.prompts = []

        try:
            self.resources = await self.mcp_client.list_resources()
        except Exception:
            self.resources = []

        if self.resources:
            self.openai_tools.append({
                "type": "function",
                "function": {
                    "name": "read_resource",
                    "description": "Read the content of a knowledge resource by its URI.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "uri": {
                                "type": "string",
                                "description": "The resource URI to read.",
                            }
                        },
                        "required": ["uri"],
                    },
                },
            })

        self.messages = [{"role": "system", "content": await self._build_system_prompt()}]

        tool_names = [t["function"]["name"] for t in self.openai_tools]
        logger.info("Connected to MCP server — %d tool(s): %s", len(tool_names), tool_names)

    async def disconnect(self):
        if self.mcp_client:
            await self.mcp_client.__aexit__(None, None, None)
            self.mcp_client = None

    async def _build_system_prompt(self) -> str:
        system_prompt = self.base_system_prompt

        if self.resources:
            system_prompt += "\n\nAvailable knowledge resources (use the read_resource tool to read one):\n"
            for r in self.resources:
                uri = getattr(r, 'uri', '')
                desc = getattr(r, 'description', '') or ''
                system_prompt += f"\n- {uri}: {desc}"

        return system_prompt

    async def _call_mcp_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        logger.debug("Tool call: %s(%s)", name, json.dumps(arguments, ensure_ascii=False))
        try:
            result = await self.mcp_client.call_tool(name, arguments, timeout=60.0)
            data = result.structured_content or result.data
            if data is None:
                for part in result.content:
                    if getattr(part, "type", None) == "json":
                        data = part.data
                        break
                    if getattr(part, "type", None) == "text":
                        try:
                            data = json.loads(part.text)
                        except (json.JSONDecodeError, TypeError):
                            data = part.text
                        break
            if data is None:
                return json.dumps({"error": "No data returned by tool"})
            if isinstance(data, (dict, list)):
                return json.dumps(data, ensure_ascii=False, default=str)
            return str(data)
        except Exception as e:
            return json.dumps({"error": str(e)})

    async def _read_mcp_resource(self, uri: str) -> str:
        logger.debug("Resource read: %s", uri)
        try:
            result = await self.mcp_client.read_resource(uri)
            if isinstance(result, str):
                return result
            if hasattr(result, 'content'):
                for part in result.content:
                    if hasattr(part, 'text'):
                        return part.text
            return str(result)
        except Exception as e:
            return json.dumps({"error": str(e)})

    async def _use_mcp_prompt(self, prompt_name: str, arguments: Dict[str, Any] | None = None) -> str:
        logger.debug("Prompt invoke: %s(%s)", prompt_name, json.dumps(arguments or {}, ensure_ascii=False))
        try:
            result = await self.mcp_client.get_prompt(prompt_name, arguments or {})
            if hasattr(result, 'messages') and result.messages:
                parts = []
                for msg in result.messages:
                    text = getattr(msg, 'text', None)
                    if text is None and hasattr(msg, 'content'):
                        text = msg.content if isinstance(msg.content, str) else str(msg.content)
                    if text:
                        parts.append(text)
                return "\n".join(parts) if parts else str(result)
            if hasattr(result, 'text'):
                return result.text
            return str(result)
        except Exception as e:
            return json.dumps({"error": str(e)})

    async def chat(self, user_message: str) -> str:
        self.messages.append({"role": "user", "content": user_message})

        def _filter(msg: dict) -> dict:
            allowed = {"role", "content", "name", "tool_calls", "tool_call_id"}
            return {k: v for k, v in msg.items() if k in allowed and v is not None}

        while True:
            response = await self.groq.chat.completions.create(
                model=self.model,
                messages=self.messages,
                tools=self.openai_tools if self.openai_tools else None,
                tool_choice="auto",
            )
            choice = response.choices[0]
            message = choice.message
            self.messages.append(_filter(message.model_dump()))

            if not message.tool_calls:
                return message.content or ""

            for tool_call in message.tool_calls:
                fn_name = tool_call.function.name
                try:
                    fn_args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError:
                    fn_args = {}

                if fn_name == "read_resource":
                    tool_result = await self._read_mcp_resource(fn_args.get("uri", ""))
                else:
                    tool_result = await self._call_mcp_tool(fn_name, fn_args)
                self.messages.append(_filter({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result,
                }))

    async def reset_conversation(self):
        self.messages = [{"role": "system", "content": await self._build_system_prompt()}]
        logger.info("Conversation reset")


async def interactive_loop(agent: BaseMCPAgent, agent_name: str):
    print("\n" + "=" * 60)
    print(f"  {agent_name} — Technical Analysis Assistant")
    print("=" * 60)
    print("Commands:")
    print("  /reset      — Reset conversation")
    print("  /tools      — List available tools")
    print("  /resources  — List available resources")
    print("  /prompts    — List available prompt workflows")
    print("  /prompt <name> [args] — Run a prompt workflow")
    print("  /quit       — Quit")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("You > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        quit_commands = {"/quit", "/exit", "/stop", "/q"}
        if user_input.lower() in quit_commands:
            print("Goodbye!")
            break
        if user_input.lower() == "/reset":
            await agent.reset_conversation()
            continue
        if user_input.lower() == "/tools":
            for t in agent.openai_tools:
                fn = t["function"]
                print(f"  - {fn['name']} — {fn['description'][:80]}")
            continue
        if user_input.lower() == "/resources":
            if not agent.resources:
                print("No resources available.")
            else:
                for r in agent.resources:
                    desc = getattr(r, 'description', '') or ''
                    name = getattr(r, 'name', str(r))
                    uri = getattr(r, 'uri', '')
                    print(f"  - {name} ({uri})" + (f" — {desc[:80]}" if desc else ""))
            continue
        if user_input.lower() == "/prompts":
            if not agent.prompts:
                print("No prompts available.")
            else:
                for p in agent.prompts:
                    desc = p.description or ""
                    arg_names = [a.name for a in (p.arguments or [])]
                    params = f" <{'> <'.join(arg_names)}>" if arg_names else ""
                    print(f"  - /prompt {p.name}{params}")
                    if desc:
                        print(f"    {desc[:100]}")
            continue
        if user_input.lower().startswith("/prompt "):
            parts = user_input.split(None, 2)  # /prompt name arg1=val1 arg2=val2 ...
            if len(parts) < 2:
                print("Usage: /prompt <name> [key=value ...]")
                continue
            prompt_name = parts[1]
            # Parse key=value arguments
            prompt_args: Dict[str, str] = {}
            if len(parts) == 3:
                for kv in parts[2].split():
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        prompt_args[k] = v
                    else:
                        # Single positional arg → try to match first expected parameter
                        matched = agent.prompts
                        target = [p for p in matched if p.name == prompt_name]
                        if target and target[0].arguments:
                            first_arg = target[0].arguments[0].name
                            prompt_args[first_arg] = kv
            try:
                prompt_text = await agent._use_mcp_prompt(prompt_name, prompt_args)
                logger.info("Prompt loaded — sending to agent...")
                response = await agent.chat(prompt_text)
                print(f"\nAgent > {response}\n")
            except Exception as e:
                logger.error("Prompt execution failed: %s", e, exc_info=True)
            continue

        try:
            response = await agent.chat(user_input)
            print(f"\nAgent > {response}\n")
        except Exception as e:
            logger.error("Chat error: %s", e, exc_info=True)


async def run_agent(agent_name: str, mcp_server_script: str, system_prompt: str, default_model: str = "openai/gpt-oss-20b"):
    parser = argparse.ArgumentParser(description=f"{agent_name} — LLM + MCP (Groq)")
    parser.add_argument("--model", default=default_model, help="Groq model")
    args = parser.parse_args()

    api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key or not api_key.startswith("gsk_"):
        logger.error("GROQ_API_KEY not set or invalid. Add it to the .env file at the project root.")
        sys.exit(1)

    agent = BaseMCPAgent(mcp_server_script, system_prompt, model=args.model)
    try:
        await agent.connect()
        await interactive_loop(agent, agent_name)
    finally:
        await agent.disconnect()
