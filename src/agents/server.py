from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List
from fastmcp import Client

from utils.logger import get_logger
logger = get_logger(__name__)

from .entities import Tool


class Server:
    def __init__(self, mcp_server_script: str, max_retries: int = 3, retry_delay: float = 1.0, tool_call_timeout: float = 60.0) -> None:
        self._mcp_server_script = mcp_server_script
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._tool_call_timeout = tool_call_timeout
        self._client: Client | None = None
        self._tools: List[Tool] = []
        self._prompts: list = []
        self._resources: list = []

    @property
    def tools(self) -> List[Tool]:
        return list(self._tools)

    @property
    def prompts(self) -> list:
        return list(self._prompts)

    @property
    def resources(self) -> list:
        return list(self._resources)

    @property
    def tool_names(self) -> List[str]:
        return [t.name for t in self._tools]

    def has_tool(self, name: str) -> bool:
        return any(t.name == name for t in self._tools)

    async def connect(self) -> None:
        self._client = Client(self._mcp_server_script)
        await self._client.__aenter__()

        raw_tools = await self._client.list_tools()
        self._tools = [Tool.from_mcp_tool(t) for t in raw_tools]

        try:
            self._prompts = await self._client.list_prompts()
        except Exception:
            self._prompts = []

        try:
            self._resources = await self._client.list_resources()
        except Exception:
            self._resources = []

        logger.info(
            "Connected to MCP server %s — %d tool(s): %s",
            self._mcp_server_script,
            len(self._tools),
            self.tool_names,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)
            self._client = None
            logger.info("Disconnected from MCP server %s", self._mcp_server_script)

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        logger.info("Tool call: %s(%s)", name, json.dumps(arguments, ensure_ascii=False))
        last_exception: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                result = await self._client.call_tool(name, arguments, timeout=self._tool_call_timeout)
                return self._extract_tool_result(result)
            except Exception as exc:
                last_exception = exc
                logger.warning("Tool call %s failed (attempt %d/%d): %s", name, attempt, self._max_retries, exc)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._retry_delay * attempt)
        return json.dumps({"error": f"Tool {name} failed after {self._max_retries} attempts: {last_exception}"})

    async def read_resource(self, uri: str) -> str:
        logger.info("Resource read: %s", uri)
        try:
            result = await self._client.read_resource(uri)
            if isinstance(result, str):
                return result
            if hasattr(result, "content"):
                for part in result.content:
                    if hasattr(part, "text"):
                        return part.text
            return str(result)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    async def get_prompt(self, prompt_name: str, arguments: Dict[str, Any] | None = None) -> str:
        logger.info("Prompt invoke: %s(%s)", prompt_name, json.dumps(arguments or {}, ensure_ascii=False))
        try:
            result = await self._client.get_prompt(prompt_name, arguments or {})
            if hasattr(result, "messages") and result.messages:
                parts = []
                for msg in result.messages:
                    text = getattr(msg, "text", None)
                    if text is None and hasattr(msg, "content"):
                        text = msg.content if isinstance(msg.content, str) else str(msg.content)
                    if text:
                        parts.append(text)
                return "\n".join(parts) if parts else str(result)
            if hasattr(result, "text"):
                return result.text
            return str(result)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    @staticmethod
    def _extract_tool_result(result) -> str:
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
