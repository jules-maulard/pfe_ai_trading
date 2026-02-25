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
    print("Le package 'groq' n'est pas installe. Faites : pip install groq")
    sys.exit(1)

from fastmcp import Client

MCP_SERVER_SCRIPT = "src/mcp_servers/mcp_rsi_server.py"

BASE_SYSTEM_PROMPT = """\
Tu es un assistant expert en analyse technique financiere.
Tu as acces a un serveur MCP qui expose des outils pour calculer des indicateurs techniques (RSI, MACD, etc.) \
sur des donnees OHLCV locales (CSV) produites par un ingesteur yfinance.

Donnees disponibles :
- Repertoire des prix : data/prices
- Symboles disponibles : CAC 40 complet (AIR.PA, DG.PA, SU.PA, MC.PA, etc.)
- Colonnes : symbol, date, open, high, low, close, volume

Quand l'utilisateur demande un calcul :
1. Utilise health_check si tu as un doute sur le serveur.
2. Appelle compute_rsi ou compute_macd avec les bons parametres.
3. Interprete les resultats (RSI: surachat >70, survente <30, neutre 30-70).
4. Donne des conseils contextuels bases sur les valeurs.

Sois precis, concis, et utilise les donnees reelles retournees par les outils.
Reponds en francais sauf si l'utilisateur parle en anglais.
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
            system_prompt += "\n\nPrompts MCP disponibles :\n"
            for p in self.prompts:
                try:
                    prompt_example = await self.mcp_client.get_prompt(
                        p.name,
                        {k: f"<{k}>" for k in (p.inputSchema or {}).get("properties", {})},
                    )
                    prompt_text = prompt_example.text if hasattr(prompt_example, "text") else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name} : {prompt_text.strip()}"

        self.messages = [{"role": "system", "content": system_prompt}]

        tool_names = [t["function"]["name"] for t in self.openai_tools]
        print(f"Connecte au serveur MCP — {len(tool_names)} outil(s) : {tool_names}")

    async def disconnect(self):
        if self.mcp_client:
            await self.mcp_client.__aexit__(None, None, None)
            self.mcp_client = None

    async def _call_mcp_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        print(f"  Appel outil : {name}({json.dumps(arguments, ensure_ascii=False)})")
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
                return json.dumps({"error": "Aucune donnee retournee par l'outil"})
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
            system_prompt += "\n\nPrompts MCP disponibles :\n"
            for p in self.prompts:
                try:
                    prompt_example = await self.mcp_client.get_prompt(
                        p.name,
                        {k: f"<{k}>" for k in (p.inputSchema or {}).get("properties", {})},
                    )
                    prompt_text = prompt_example.text if hasattr(prompt_example, "text") else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name} : {prompt_text.strip()}"
        self.messages = [{"role": "system", "content": system_prompt}]
        print("  Conversation reinitialisee.")


async def interactive_loop(agent: MCPAgent):
    print("\n" + "=" * 60)
    print("  RSI MCP Agent — Assistant Analyse Technique")
    print("=" * 60)
    print("Commandes :")
    print("  /reset   — Reinitialiser la conversation")
    print("  /tools   — Lister les outils disponibles")
    print("  /quit    — Quitter")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("Vous > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAu revoir !")
            break

        if not user_input:
            continue

        if user_input.lower() == "/quit":
            print("Au revoir !")
            break
        elif user_input.lower() == "/reset":
            await agent.reset_conversation()
            continue
        elif user_input.lower() == "/tools":
            for t in agent.openai_tools:
                fn = t["function"]
                print(f"  - {fn['name']} — {fn['description'][:80]}")
            continue

        try:
            response = await agent.chat(user_input)
            print(f"\nAgent > {response}\n")
        except Exception as e:
            print(f"\nErreur : {e}\n")


async def main():
    parser = argparse.ArgumentParser(description="RSI MCP Agent — LLM + MCP (Groq)")
    parser.add_argument("--model", default="openai/gpt-oss-20b", help="Modele Groq")
    args = parser.parse_args()

    api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key or not api_key.startswith("gsk_"):
        print("GROQ_API_KEY non definie ou incorrecte. Exemple :")
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
