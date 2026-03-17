from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from ..utils import get_logger
logger = get_logger(__name__)

from .entities import Configuration, Message
from .llm_client import LlmClient
from .memory import Memory
from .server import Server
from .token_monitor import TokenMonitor
from .toolbox import ToolBox


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
        self._toolbox = ToolBox()
        self._memory = memory
        self._token_monitor = token_monitor

    @property
    def tools(self):
        return self._toolbox.tools

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
            self._toolbox.register_server(server)

        self._toolbox.register_read_resource_tool()

        system_prompt = self._build_system_prompt()
        self._memory.reset(system_prompt)

        logger.info(
            "Agent initialized — %d server(s), %d tool(s): %s",
            len(self._servers),
            len(self._toolbox.tools),
            [t.name for t in self._toolbox.tools],
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
                tools=self._toolbox.get_openai_tools(),
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
                tool_result = await self._toolbox.execute_tool_call(tool_call)
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
    