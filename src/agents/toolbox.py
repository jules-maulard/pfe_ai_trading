from __future__ import annotations

import json
from typing import Any, Dict, List

from ..utils import get_logger
logger = get_logger(__name__)

from .entities import Tool
from .server import Server

_READ_RESOURCE_TOOL = Tool(
    name="read_resource",
    description="Read the content of a knowledge resource by its URI.",
    parameters_schema={
        "type": "object",
        "properties": {
            "uri": {"type": "string", "description": "The resource URI to read."},
        },
        "required": ["uri"],
    },
)


class ToolBox:
    def __init__(self) -> None:
        self._tools: List[Tool] = []
        self._tool_server_map: Dict[str, Server] = {}
        self._servers: List[Server] = []

    @property
    def tools(self) -> List[Tool]:
        return list(self._tools)

    def register_server(self, server: Server) -> None:
        self._servers.append(server)
        for tool in server.tools:
            self._tools.append(tool)
            self._tool_server_map[tool.name] = server

    def register_read_resource_tool(self) -> None:
        resources = [r for s in self._servers for r in s.resources]
        if resources:
            self._tools.append(_READ_RESOURCE_TOOL)

    def get_openai_tools(self) -> List[Dict[str, Any]] | None:
        if not self._tools:
            return None
        return [tool.to_openai_format() for tool in self._tools]

    async def execute_tool_call(self, tool_call) -> str:
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
                if str(getattr(resource, "uri", "")) == uri:
                    return await server.read_resource(uri)
        return json.dumps({"error": f"Resource not found: {uri}"})
