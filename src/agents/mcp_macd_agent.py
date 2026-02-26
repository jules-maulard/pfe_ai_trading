from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any, Dict, List

try:
    from groq import AsyncGroq
except ImportError:
    print("The 'groq' package is not installed. Run: pip install groq")
    sys.exit(1)

from fastmcp import Client

MCP_SERVER_SCRIPT = "src/mcp_servers/mcp_macd_server.py"

BASE_SYSTEM_PROMPT = """\
You are an expert financial technical analysis assistant.
You have access to an MCP server exposing tools to compute MACD indicators \
on local OHLCV data (CSV) produced by a yfinance ingester.

Available data:
- Database: database/ohlcv.csv
- Available symbols: full CAC 40 (AIR.PA, DG.PA, SU.PA, MC.PA, etc.)
- Columns: symbol, date, open, high, low, close, volume

When the user requests a computation:
1. Use health_check if you have any doubt about the server.
2. Call compute_macd with the appropriate parameters.
3. Interpret the results (bullish/bearish crossovers, histogram direction, divergence).
4. Give contextual advice based on the values.

Be precise, concise, and use the actual data returned by the tools.
Respond in the same language as the user.
"""


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


class MCPAgent:
    def __init__(self, model: str = "openai/gpt-oss-20b"):
        self.model = model
        self.groq = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY"))
        self.mcp_client: Client | None = None
        self.openai_tools: List[Dict[str, Any]] = []
        self.prompts: List[Dict[str, Any]] = []
        self.messages: List[Dict[str, Any]] = []

    async def connect(self):
        self.mcp_client = Client(MCP_SERVER_SCRIPT)
        await self.mcp_client.__aenter__()

        tools = await self.mcp_client.list_tools()
        self.openai_tools = mcp_tools_to_openai(tools)

        try:
            self.prompts = await self.mcp_client.list_prompts()
        except Exception:
            self.prompts = []

        system_prompt = BASE_SYSTEM_PROMPT
        if self.prompts:
            system_prompt += "\n\nAvailable MCP prompts:\n"
            for p in self.prompts:
                try:
                    prompt_example = await self.mcp_client.get_prompt(
                        p.name,
                        {k: f"<{k}>" for k in (p.inputSchema or {}).get("properties", {})},
                    )
                    prompt_text = prompt_example.text if hasattr(prompt_example, "text") else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name}: {prompt_text.strip()}"

        self.messages = [{"role": "system", "content": system_prompt}]

        tool_names = [t["function"]["name"] for t in self.openai_tools]
        print(f"Connected to MCP server — {len(tool_names)} tool(s): {tool_names}")

    async def disconnect(self):
        if self.mcp_client:
            await self.mcp_client.__aexit__(None, None, None)
            self.mcp_client = None

    async def _call_mcp_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        print(f"  Tool call: {name}({json.dumps(arguments, ensure_ascii=False)})")
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

                tool_result = await self._call_mcp_tool(fn_name, fn_args)
                self.messages.append(_filter({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result,
                }))

    async def reset_conversation(self):
        system_prompt = BASE_SYSTEM_PROMPT
        if self.prompts:
            system_prompt += "\n\nAvailable MCP prompts:\n"
            for p in self.prompts:
                try:
                    prompt_example = await self.mcp_client.get_prompt(
                        p.name,
                        {k: f"<{k}>" for k in (p.inputSchema or {}).get("properties", {})},
                    )
                    prompt_text = prompt_example.text if hasattr(prompt_example, "text") else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name}: {prompt_text.strip()}"
        self.messages = [{"role": "system", "content": system_prompt}]
        print("  Conversation reset.")


async def interactive_loop(agent: MCPAgent):
    print("\n" + "=" * 60)
    print("  MACD MCP Agent — Technical Analysis Assistant")
    print("=" * 60)
    print("Commands:")
    print("  /reset   — Reset conversation")
    print("  /tools   — List available tools")
    print("  /quit    — Quit")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("You > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        if user_input.lower() == "/quit":
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

        try:
            response = await agent.chat(user_input)
            print(f"\nAgent > {response}\n")
        except Exception as e:
            print(f"\nError: {e}\n")


async def main():
    parser = argparse.ArgumentParser(description="MACD MCP Agent — LLM + MCP (Groq)")
    parser.add_argument("--model", default="openai/gpt-oss-20b", help="Groq model")
    args = parser.parse_args()

    api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key or not api_key.startswith("gsk_"):
        print("GROQ_API_KEY not set or invalid. Example:")
        print("  set GROQ_API_KEY=gsk-...")
        sys.exit(1)

    agent = MCPAgent(model=args.model)
    try:
        await agent.connect()
        await interactive_loop(agent)
    finally:
        await agent.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
