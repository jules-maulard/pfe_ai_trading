"""
RSI MCP Agent — Agent LLM connecté au serveur MCP RSI Tools.

Architecture MCP :
  ┌──────────┐       ┌────────────┐       ┌────────────────┐
  │  User    │ <---> │   Agent    │ <---> │  MCP Server    │
  │ (stdin)  │       │  (LLM +    │  MCP  │ mcp_rsi_server │
  │          │       │   Client)  │ stdio │  (tools)       │
  └──────────┘       └────────────┘       └────────────────┘

L'agent :
1. Lance le serveur MCP en subprocess (stdio)
2. Découvre les outils disponibles via MCP
3. Convertit les schémas MCP → format OpenAI function-calling
4. Boucle interactive : l'utilisateur pose des questions en langage naturel
5. Le LLM (GPT-4o / GPT-4o-mini) décide quels outils appeler
6. L'agent exécute les appels d'outils via le client MCP
7. Le LLM interprète les résultats et répond à l'utilisateur

Utilisation :
    # Variable d'environnement requise
    set OPENAI_API_KEY=sk-...

    # Lancer l'agent
    python src/mcp_rsi_agent.py

    # Avec un modèle spécifique
    python src/mcp_rsi_agent.py --model gpt-4o-mini
"""
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
    print("✗ Le package 'groq' n'est pas installé. Faites : pip install groq")
    sys.exit(1)
from fastmcp import Client


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
BASE_SYSTEM_PROMPT = """\
Tu es un assistant expert en analyse technique financière.
Tu as accès à un serveur MCP qui expose des outils pour calculer des indicateurs techniques (RSI, etc.) \
sur des données OHLCV locales (CSV/Parquet) produites par un ingesteur yfinance.

Données disponibles :
- Répertoire des prix : data/prices
- Symboles disponibles : AIR.PA, DG.PA, SU.PA (actions du CAC 40)
- Colonnes : symbol, date, open, high, low, close, adj_close, volume

Quand l'utilisateur demande un calcul :
1. Utilise d'abord health_check pour vérifier que le serveur fonctionne (seulement si tu as un doute).
2. Appelle compute_rsi avec les bons paramètres.
3. Interprète les résultats : explique le niveau du RSI (surachat >70, survente <30, neutre entre 30-70).
4. Donne des conseils contextuels basés sur les valeurs.

Sois précis, concis, et utilise les données réelles retournées par les outils.
Réponds en français sauf si l'utilisateur parle en anglais.
"""

MCP_SERVER_SCRIPT = "src/mcp_rsi_server.py"


# ─────────────────────────────────────────────
# Conversion MCP tool schemas → OpenAI format
# ─────────────────────────────────────────────
def mcp_tools_to_openai(mcp_tools) -> List[Dict[str, Any]]:
    """
    Convertit les schémas d'outils MCP en format OpenAI function-calling.
    """
    openai_tools = []
    for tool in mcp_tools:
        # Le schéma MCP contient name, description, inputSchema
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


# ─────────────────────────────────────────────
# Agent principal
# ─────────────────────────────────────────────

class MCPAgent:
    """Agent LLM qui utilise un serveur MCP pour exécuter des outils (Groq)."""

    def __init__(self, model: str = "openai/gpt-oss-20b"):
        self.model = model
        self.groq = AsyncGroq(api_key=os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY"))
        self.mcp_client: Client | None = None
        self.openai_tools: List[Dict[str, Any]] = []
        self.prompts: List[Dict[str, Any]] = []
        self.messages: List[Dict[str, Any]] = []

    async def connect(self):
        """Connecte le client MCP au serveur et découvre les outils et prompts."""
        self.mcp_client = Client(MCP_SERVER_SCRIPT)
        await self.mcp_client.__aenter__()

        # Découverte des outils
        tools = await self.mcp_client.list_tools()
        self.openai_tools = mcp_tools_to_openai(tools)

        # Découverte des prompts MCP
        try:
            self.prompts = await self.mcp_client.list_prompts()
        except Exception:
            self.prompts = []

        # Générer le system prompt enrichi avec les prompts MCP
        system_prompt = BASE_SYSTEM_PROMPT
        if self.prompts:
            system_prompt += "\n\nExemples de prompts MCP disponibles :\n"
            for p in self.prompts:
                # On affiche le nom et le template (si possible)
                try:
                    prompt_example = await self.mcp_client.get_prompt(p.name, {k: f"<{k}>" for k in (p.inputSchema or {}).get('properties', {})})
                    prompt_text = prompt_example.text if hasattr(prompt_example, 'text') else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name} : {prompt_text.strip()}"

        self.messages = [{"role": "system", "content": system_prompt}]

        tool_names = [t["function"]["name"] for t in self.openai_tools]
        print(f"✓ Connecté au serveur MCP — {len(tool_names)} outil(s) disponible(s) : {tool_names}")



    async def disconnect(self):
        """Ferme la connexion MCP."""
        if self.mcp_client:
            await self.mcp_client.__aexit__(None, None, None)
            self.mcp_client = None

    async def _call_mcp_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        """Exécute un outil MCP et retourne le résultat sous forme de texte."""
        print(f"  ⚙ Appel outil : {name}({json.dumps(arguments, ensure_ascii=False)})")
        try:
            result = await self.mcp_client.call_tool(name, arguments, timeout=60.0)

            # Extraire le contenu structuré ou texte
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
                return json.dumps({"error": "Aucune donnée retournée par l'outil"})

            if isinstance(data, dict) or isinstance(data, list):
                return json.dumps(data, ensure_ascii=False, default=str)
            return str(data)

        except Exception as e:
            return json.dumps({"error": str(e)})


    async def chat(self, user_message: str) -> str:
        """
        Envoie un message utilisateur au LLM (Groq), gère la boucle d'appels d'outils,
        et retourne la réponse finale.
        """
        self.messages.append({"role": "user", "content": user_message})

        def filter_message_fields(msg: dict) -> dict:
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

            # Ajoute uniquement les champs autorisés (sinon Groq plante)
            self.messages.append(filter_message_fields(message.model_dump()))

            if not message.tool_calls:
                return message.content or ""

            for tool_call in message.tool_calls:
                fn_name = tool_call.function.name
                try:
                    fn_args = json.loads(tool_call.function.arguments)
                except json.JSONDecodeError:
                    fn_args = {}

                tool_result = await self._call_mcp_tool(fn_name, fn_args)

                self.messages.append(filter_message_fields({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": tool_result,
                }))

    async def reset_conversation(self):
        """Remet l'historique à zéro (garde le system prompt enrichi avec prompts MCP)."""
        # Regénère le system prompt comme dans connect()
        system_prompt = BASE_SYSTEM_PROMPT
        if self.prompts:
            system_prompt += "\n\nExemples de prompts MCP disponibles :\n"
            for p in self.prompts:
                try:
                    prompt_example = await self.mcp_client.get_prompt(p.name, {k: f"<{k}>" for k in (p.inputSchema or {}).get('properties', {})})
                    prompt_text = prompt_example.text if hasattr(prompt_example, 'text') else str(prompt_example)
                except Exception:
                    prompt_text = p.description or ""
                system_prompt += f"\n- {p.name} : {prompt_text.strip()}"
        self.messages = [{"role": "system", "content": system_prompt}]
        print("  ↺ Conversation réinitialisée.")


# ─────────────────────────────────────────────
# Boucle interactive
# ─────────────────────────────────────────────
async def interactive_loop(agent: MCPAgent):
    """REPL interactif pour discuter avec l'agent."""
    print("\n" + "=" * 60)
    print("  RSI MCP Agent — Assistant Analyse Technique")
    print("=" * 60)
    print("Commandes spéciales :")
    print("  /reset   — Réinitialiser la conversation")
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

        # Commandes spéciales
        if user_input.lower() == "/quit":
            print("Au revoir !")
            break
        elif user_input.lower() == "/reset":
            await agent.reset_conversation()
            continue
        elif user_input.lower() == "/tools":
            for t in agent.openai_tools:
                fn = t["function"]
                print(f"  • {fn['name']} — {fn['description'][:80]}")
            continue

        # Envoyer au LLM
        try:
            response = await agent.chat(user_input)
            print(f"\nAgent > {response}\n")
        except Exception as e:
            print(f"\n✗ Erreur : {e}\n")


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="RSI MCP Agent — Agent LLM + MCP (Groq)")
    parser.add_argument(
        "--model", default="openai/gpt-oss-20b",
        help="Modèle Groq à utiliser (défaut: openai/gpt-oss-20b)"
    )
    args = parser.parse_args()

    # Vérifier la clé API Groq
    api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key or not api_key.startswith("gsk_"):
        print("✗ GROQ_API_KEY non définie ou incorrecte. Exemple :")
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
