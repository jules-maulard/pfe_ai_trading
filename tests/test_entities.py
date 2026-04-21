from unittest.mock import MagicMock

from src.agents.entities import Message, Tool


class TestMessage:
    def test_to_dict_minimal(self):
        msg = Message(role="user", content="hello")
        d = msg.to_dict()
        assert d == {"role": "user", "content": "hello"}

    def test_to_dict_excludes_none_tool_calls(self):
        msg = Message(role="user", content="hello")
        assert "tool_calls" not in msg.to_dict()
        assert "tool_call_id" not in msg.to_dict()

    def test_to_dict_includes_tool_calls_when_set(self):
        tool_calls = [{"id": "call_1", "type": "function", "function": {"name": "foo", "arguments": "{}"}}]
        msg = Message(role="assistant", content="", tool_calls=tool_calls)
        d = msg.to_dict()
        assert d["tool_calls"] == tool_calls

    def test_to_dict_includes_tool_call_id_when_set(self):
        msg = Message(role="tool", content="result", tool_call_id="call_1")
        d = msg.to_dict()
        assert d["tool_call_id"] == "call_1"

    def test_to_dict_all_fields(self):
        tool_calls = [{"id": "c1"}]
        msg = Message(role="assistant", content="ok", tool_calls=tool_calls, tool_call_id="c1")
        d = msg.to_dict()
        assert set(d.keys()) == {"role", "content", "tool_calls", "tool_call_id"}


class TestTool:
    def _make_tool(self) -> Tool:
        return Tool(
            name="get_price",
            description="Fetch price for a symbol",
            parameters_schema={
                "type": "object",
                "properties": {"symbol": {"type": "string"}},
                "required": ["symbol"],
            },
        )

    def test_to_openai_format_structure(self):
        tool = self._make_tool()
        fmt = tool.to_openai_format()
        assert fmt["type"] == "function"
        assert "function" in fmt
        assert fmt["function"]["name"] == "get_price"
        assert fmt["function"]["description"] == "Fetch price for a symbol"
        assert fmt["function"]["parameters"] == tool.parameters_schema

    def test_from_mcp_tool_maps_fields(self):
        mcp_tool = MagicMock()
        mcp_tool.name = "list_symbols"
        mcp_tool.description = "List available symbols"
        mcp_tool.inputSchema = {"type": "object", "properties": {}}

        tool = Tool.from_mcp_tool(mcp_tool)

        assert tool.name == "list_symbols"
        assert tool.description == "List available symbols"
        assert tool.parameters_schema == {"type": "object", "properties": {}}

    def test_from_mcp_tool_none_description_defaults_to_empty(self):
        mcp_tool = MagicMock()
        mcp_tool.name = "foo"
        mcp_tool.description = None
        mcp_tool.inputSchema = {}

        tool = Tool.from_mcp_tool(mcp_tool)
        assert tool.description == ""

    def test_from_mcp_tool_none_schema_defaults_to_empty_object(self):
        mcp_tool = MagicMock()
        mcp_tool.name = "foo"
        mcp_tool.description = "bar"
        mcp_tool.inputSchema = None

        tool = Tool.from_mcp_tool(mcp_tool)
        assert tool.parameters_schema == {"type": "object", "properties": {}}
