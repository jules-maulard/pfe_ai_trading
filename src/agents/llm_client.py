from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from langchain_github_copilot import ChatGitHubCopilot

from ..utils import get_logger

logger = get_logger(__name__)


# ─── Adapter classes to maintain OpenAI-style response interface ─────────────


@dataclass
class _Usage:
    prompt_tokens: int
    completion_tokens: int


@dataclass
class _FunctionCall:
    name: str
    arguments: str


@dataclass
class _ToolCall:
    id: str
    type: str
    function: _FunctionCall


@dataclass
class _Message:
    content: Optional[str]
    tool_calls: Optional[List[_ToolCall]]

    def model_dump(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {"content": self.content}
        if self.tool_calls:
            data["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": tc.type,
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in self.tool_calls
            ]
        else:
            data["tool_calls"] = None
        return data


@dataclass
class _Choice:
    message: _Message


# ─── LLM Client ─────────────────────────────────────────────────────────────


class LlmClient:
    def __init__(self, api_key: str, model: str = "gpt-4o", **kwargs) -> None:
        self._model = model
        self._api_key = api_key
        self._llm = ChatGitHubCopilot(model=model, api_key=api_key)

    async def get_response(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]] | None = None,
    ):
        langchain_messages = self._convert_messages(messages)

        llm = self._llm
        if tools:
            llm = llm.bind_tools(tools)

        response = await llm.ainvoke(langchain_messages)

        # Build adapter objects
        tool_calls = None
        if response.tool_calls:
            tool_calls = [
                _ToolCall(
                    id=tc["id"],
                    type="function",
                    function=_FunctionCall(
                        name=tc["name"],
                        arguments=json.dumps(tc["args"]) if isinstance(tc["args"], dict) else tc["args"],
                    ),
                )
                for tc in response.tool_calls
            ]

        message = _Message(
            content=response.content if isinstance(response.content, str) else None,
            tool_calls=tool_calls if tool_calls else None,
        )

        usage_meta = response.usage_metadata
        usage = _Usage(
            prompt_tokens=usage_meta.get("input_tokens", 0) if usage_meta else 0,
            completion_tokens=usage_meta.get("output_tokens", 0) if usage_meta else 0,
        )

        return _Choice(message=message), usage

    @staticmethod
    def _convert_messages(messages: List[Dict[str, Any]]) -> List:
        from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

        result = []
        for msg in messages:
            role = msg["role"]
            content = msg.get("content") or ""
            if role == "system":
                result.append(SystemMessage(content=content))
            elif role == "user":
                result.append(HumanMessage(content=content))
            elif role == "assistant":
                tool_calls = msg.get("tool_calls")
                if tool_calls:
                    lc_tool_calls = [
                        {
                            "id": tc["id"],
                            "name": tc["function"]["name"],
                            "args": json.loads(tc["function"]["arguments"])
                            if isinstance(tc["function"]["arguments"], str)
                            else tc["function"]["arguments"],
                        }
                        for tc in tool_calls
                    ]
                    result.append(AIMessage(content=content, tool_calls=lc_tool_calls))
                else:
                    result.append(AIMessage(content=content))
            elif role == "tool":
                result.append(ToolMessage(content=content, tool_call_id=msg.get("tool_call_id", "")))
        return result
