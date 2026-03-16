from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from ..utils import get_logger
logger = get_logger(__name__)

from .entities import Configuration, Message, Tool
from .llm_client import LlmClient
from .memory import Memory
from .server import Server
from .token_monitor import TokenMonitor


class Agent:
    def __init__(
        self,
        configuration: Configuration,
        llm_client: LlmClient,
        servers: List[Server],
        memory: Memory,
        token_monitor: TokenMonitor,
    ) -> None:
        self._configuration = configuration
        self._llm_client = llm_client
        self._servers = servers
        self._tools_cache: List[Tool] = []
        self._tool_server_map: Dict[str, Server] = {}
        self._memory = memory
        self._token_monitor = token_monitor

    @property
    def tools(self) -> List[Tool]:
        return list(self._tools_cache)

    @property
    def token_monitor(self) -> TokenMonitor:
        return self._token_monitor

    @property
    def prompts(self) -> list:
        result = []
        for server in self._servers:
            result.extend(server.prompts)
        return result

    @property
    def resources(self) -> list:
        result = []
        for server in self._servers:
            result.extend(server.resources)
        return result

    async def connect(self) -> None:
        for server in self._servers:
            await server.connect()

        self._populate_tools_cache()
        self._register_read_resource_tool()

        system_prompt = self._build_system_prompt()
        self._memory.reset(system_prompt)

        logger.info(
            "Agent initialized — %d server(s), %d tool(s): %s",
            len(self._servers),
            len(self._tools_cache),
            [t.name for t in self._tools_cache],
        )

    async def disconnect(self) -> None:
        for server in self._servers:
            await server.disconnect()


    async def chat(self, user_input: str) -> str:
        self._memory.add_message(Message(role="user", content=user_input))
        _nudge_sent = False
        while True:
            choice, usage = await self._llm_client.get_response(
                messages=self._memory.get_history(),
                tools=self._openai_tools(),
            )
            if usage:
                self._token_monitor.record(usage.prompt_tokens, usage.completion_tokens)
            assistant_message = choice.message
            raw = assistant_message.model_dump()

            self._memory.add_message(Message(
                role="assistant",
                content=raw.get("content") or "",
                tool_calls=raw.get("tool_calls"),
            ))

            if not assistant_message.tool_calls:
                content = assistant_message.content or ""
                should_continue, _nudge_sent = self._maybe_nudge_for_synthesis(content, _nudge_sent)
                if should_continue:
                    continue
                return content

            for tool_call in assistant_message.tool_calls:
                tool_result = await self._execute_tool_call(tool_call)
                self._memory.add_message(Message(
                    role="tool",
                    content=tool_result,
                    tool_call_id=tool_call.id,
                ))

    async def run_prompt(self, prompt_name: str, arguments: Dict[str, Any] | None = None) -> str:
        for server in self._servers:
            for prompt in server.prompts:
                if prompt.name == prompt_name:
                    prompt_text = await server.get_prompt(prompt_name, arguments)
                    return await self.chat(prompt_text)
        return json.dumps({"error": f"Prompt not found: {prompt_name}"})

    async def reset_conversation(self) -> None:
        system_prompt = self._build_system_prompt()
        self._memory.reset(system_prompt)
        self._token_monitor.reset()
        logger.info("Conversation reset")

    
    def _populate_tools_cache(self) -> None:
        self._tools_cache.clear()
        self._tool_server_map.clear()
        for server in self._servers:
            for tool in server.tools:
                self._tools_cache.append(tool)
                self._tool_server_map[tool.name] = server
    
    def _register_read_resource_tool(self) -> None:
        if not self.resources:
            return
        self._tools_cache.append(Tool(
            name="read_resource",
            description="Read the content of a knowledge resource by its URI.",
            parameters_schema={
                "type": "object",
                "properties": {
                    "uri": {"type": "string", "description": "The resource URI to read."},
                },
                "required": ["uri"],
            },
        ))

    def _build_system_prompt(self) -> str:
        system_prompt = self._configuration.system_prompt

        all_resources = self.resources
        if all_resources:
            system_prompt += "\n\n# Available knowledge resources\nUse the read_resource tool to read one.\n"
            for resource in all_resources:
                uri = getattr(resource, "uri", "")
                desc = getattr(resource, "description", "") or ""
                system_prompt += f"\n- {uri}: {desc}"
        return system_prompt

    def _openai_tools(self) -> List[Dict[str, Any]] | None:
        if not self._tools_cache:
            return None
        return [tool.to_openai_format() for tool in self._tools_cache]

    async def _execute_tool_call(self, tool_call) -> str:
        fn_name = tool_call.function.name
        try:
            fn_args = json.loads(tool_call.function.arguments)
        except json.JSONDecodeError:
            fn_args = {}

        if fn_name == "read_resource":
            return await self._read_resource(fn_args.get("uri", ""))

        server = self._tool_server_map.get(fn_name)
        if server is None:
            return json.dumps({"error": f"No server found for tool '{fn_name}'"})
        return await server.call_tool(fn_name, fn_args)

    async def _read_resource(self, uri: str) -> str:
        for server in self._servers:
            for resource in server.resources:
                resource_uri = str(getattr(resource, "uri", ""))
                if resource_uri == uri:
                    return await server.read_resource(uri)
        return json.dumps({"error": f"Resource not found: {uri}"})

    def _maybe_nudge_for_synthesis(self, content: str, nudge_sent: bool) -> Tuple[bool, bool]:
        """If the assistant returned empty content, send a nudge once.

        Returns a tuple `(should_continue_loop, updated_nudge_sent)`.
        """
        if not content.strip() and not nudge_sent:
            logger.warning("LLM returned empty content — nudging for synthesis")
            self._memory.add_message(Message(
                role="user",
                content=(
                    "Please now write your complete analysis and recommendation "
                    "based on all the data you have gathered above."
                ),
            ))
            return True, True
        return False, nudge_sent
    