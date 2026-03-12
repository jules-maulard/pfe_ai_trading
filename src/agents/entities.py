from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv


@dataclass
class Message:
    role: str
    content: str
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"role": self.role, "content": self.content}
        if self.tool_calls is not None:
            data["tool_calls"] = self.tool_calls
        if self.tool_call_id is not None:
            data["tool_call_id"] = self.tool_call_id
        return data


@dataclass
class Tool:
    name: str
    description: str
    parameters_schema: Dict[str, Any]

    def to_openai_format(self) -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters_schema,
            },
        }

    @classmethod
    def from_mcp_tool(cls, mcp_tool) -> Tool:
        return cls(
            name=mcp_tool.name,
            description=mcp_tool.description or "",
            parameters_schema=mcp_tool.inputSchema if mcp_tool.inputSchema else {"type": "object", "properties": {}},
        )


@dataclass
class Configuration:
    api_key: str
    model: str
    mcp_server_scripts: List[str] = field(default_factory=list)
    system_prompt: str = ""
    tool_call_timeout: float = 60.0
    max_retries: int = 3
    retry_delay: float = 1.0

    @classmethod
    def from_env(
        cls,
        mcp_server_scripts: List[str] | None = None,
        system_prompt: str = "",
        model: str = "openai/gpt-oss-20b",
    ) -> Configuration:
        load_dotenv(Path(__file__).resolve().parents[2] / ".env")
        api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
        return cls(
            api_key=api_key,
            model=model,
            mcp_server_scripts=mcp_server_scripts or [],
            system_prompt=system_prompt,
        )
