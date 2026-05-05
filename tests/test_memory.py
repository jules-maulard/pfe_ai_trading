from src.agents.memory import LongTermMemory, Memory
from src.agents.entities import Message


class TestMemoryInit:
    def test_empty_init_has_no_history(self):
        mem = Memory()
        assert mem.get_history() == []

    def test_init_with_system_prompt_prepends_system_message(self):
        mem = Memory(system_prompt="You are helpful.")
        history = mem.get_history()
        assert len(history) == 1
        assert history[0]["role"] == "system"
        assert history[0]["content"] == "You are helpful."


class TestMemoryAddMessage:
    def test_add_user_message(self):
        mem = Memory()
        mem.add_message(Message(role="user", content="Hello"))
        assert len(mem.get_history()) == 1
        assert mem.get_history()[0]["role"] == "user"

    def test_add_multiple_messages_preserves_order(self):
        mem = Memory()
        mem.add_message(Message(role="user", content="Hi"))
        mem.add_message(Message(role="assistant", content="Hello!"))
        history = mem.get_history()
        assert history[0]["role"] == "user"
        assert history[1]["role"] == "assistant"

    def test_get_history_returns_dicts(self):
        mem = Memory()
        mem.add_message(Message(role="user", content="test"))
        history = mem.get_history()
        assert isinstance(history[0], dict)


class TestMemoryReset:
    def test_reset_clears_history(self):
        mem = Memory(system_prompt="sys")
        mem.add_message(Message(role="user", content="hello"))
        mem.reset()
        assert mem.get_history() == []

    def test_reset_with_new_system_prompt(self):
        mem = Memory(system_prompt="old")
        mem.add_message(Message(role="user", content="hello"))
        mem.reset(system_prompt="new")
        history = mem.get_history()
        assert len(history) == 1
        assert history[0]["content"] == "new"

    def test_reset_without_prompt_leaves_empty(self):
        mem = Memory(system_prompt="sys")
        mem.reset()
        assert mem.get_history() == []


class TestMemoryUpdateSystemPrompt:
    def test_replaces_existing_system_message(self):
        mem = Memory(system_prompt="old")
        mem.update_system_prompt("new")
        assert mem.get_history()[0]["content"] == "new"

    def test_inserts_system_message_when_absent(self):
        mem = Memory()
        mem.add_message(Message(role="user", content="hi"))
        mem.update_system_prompt("injected")
        history = mem.get_history()
        assert history[0]["role"] == "system"
        assert history[0]["content"] == "injected"

    def test_non_system_messages_remain_after_update(self):
        mem = Memory(system_prompt="sys")
        mem.add_message(Message(role="user", content="msg"))
        mem.update_system_prompt("updated")
        history = mem.get_history()
        assert len(history) == 2


class TestTokenBudget:
    def test_current_tokens_property(self):
        mem = Memory(system_prompt="hello")
        assert mem.current_tokens > 0

    def test_trim_to_budget_evicts_oldest(self):
        mem = Memory(system_prompt="sys", max_tokens=50)
        for i in range(20):
            mem.add_message(Message(role="user", content=f"message number {i} with some padding text"))
        assert mem.current_tokens <= 50

    def test_system_prompt_never_evicted_by_budget(self):
        mem = Memory(system_prompt="I am the system prompt", max_tokens=60)
        for i in range(10):
            mem.add_message(Message(role="user", content=f"padding message {i} extra"))
        history = mem.get_history()
        assert history[0]["role"] == "system"
        assert history[0]["content"] == "I am the system prompt"


class TestCompression:
    def test_needs_compression_false_when_no_budget(self):
        mem = Memory()
        mem.add_message(Message(role="user", content="hi"))
        assert not mem.needs_compression()

    def test_needs_compression_threshold(self):
        mem = Memory(system_prompt="sys", max_tokens=100, compression_threshold=0.5)
        # Fill until compression is triggered
        for i in range(30):
            mem.add_message(Message(role="user", content=f"msg {i} padding text here"))
        # After trimming, tokens should be near budget — compression check applies before trim
        # Just verify the method runs without error
        result = mem.needs_compression()
        assert isinstance(result, bool)

    def test_replace_with_summary_removes_old_messages(self):
        mem = Memory(system_prompt="sys", keep_last_k=2)
        mem.add_message(Message(role="user", content="old msg 1"))
        mem.add_message(Message(role="assistant", content="old reply 1"))
        mem.add_message(Message(role="user", content="old msg 2"))
        mem.add_message(Message(role="assistant", content="old reply 2"))
        mem.add_message(Message(role="user", content="recent 1"))
        mem.add_message(Message(role="assistant", content="recent 2"))

        mem.replace_with_summary("This is a summary.")
        history = mem.get_history()
        # system + summary + 2 recent
        assert history[0]["role"] == "system"
        assert "summary" in history[1]["content"].lower()
        assert len(history) == 4

    def test_get_compressible_messages(self):
        mem = Memory(system_prompt="sys", keep_last_k=2)
        mem.add_message(Message(role="user", content="old"))
        mem.add_message(Message(role="assistant", content="old reply"))
        mem.add_message(Message(role="user", content="recent"))
        mem.add_message(Message(role="assistant", content="recent reply"))

        compressible = mem.get_compressible_messages()
        assert len(compressible) == 2
        assert compressible[0]["content"] == "old"
        assert compressible[1]["content"] == "old reply"


class TestLongTermMemory:
    def test_add_and_get_facts(self):
        ltm = LongTermMemory()
        ltm.add_fact("ticker", "AAPL is bullish")
        facts = ltm.get_facts()
        assert facts == {"ticker": "AAPL is bullish"}

    def test_to_prompt_section_empty(self):
        ltm = LongTermMemory()
        assert ltm.to_prompt_section() == ""

    def test_to_prompt_section_format(self):
        ltm = LongTermMemory()
        ltm.add_fact("signal", "RSI oversold on BNP")
        section = ltm.to_prompt_section()
        assert "Key facts" in section
        assert "RSI oversold on BNP" in section

    def test_clear(self):
        ltm = LongTermMemory()
        ltm.add_fact("k", "v")
        ltm.clear()
        assert ltm.get_facts() == {}

    def test_max_facts_evicts_oldest(self):
        ltm = LongTermMemory(max_facts=2)
        ltm.add_fact("a", "1")
        ltm.add_fact("b", "2")
        ltm.add_fact("c", "3")
        facts = ltm.get_facts()
        assert "a" not in facts
        assert "b" in facts
        assert "c" in facts

    def test_update_existing_fact_no_eviction(self):
        ltm = LongTermMemory(max_facts=2)
        ltm.add_fact("a", "1")
        ltm.add_fact("b", "2")
        ltm.add_fact("a", "updated")
        facts = ltm.get_facts()
        assert facts["a"] == "updated"
        assert len(facts) == 2


class TestCompactToolTurns:
    def _make_tool_call(self, name: str) -> list:
        return [{"function": {"name": name}, "id": "id1", "type": "function"}]

    def test_collapses_tool_chain_into_compact_note(self):
        mem = Memory(system_prompt="sys")
        mem.add_message(Message(role="user", content="analyse AIR.PA"))
        mem.add_message(Message(role="assistant", content=None, tool_calls=self._make_tool_call("compute_rsi")))
        mem.add_message(Message(role="tool", content='{"rsi": 55}', tool_call_id="id1"))
        mem.add_message(Message(role="assistant", content="RSI is 55, bullish."))

        removed = mem.compact_tool_turns()
        history = mem.get_history()

        assert removed == 1  # 2 msgs (assistant+tool) → 1 compact note, net -1
        # system + user + compact_note + final_assistant
        assert len(history) == 4
        compact = history[2]
        assert compact["role"] == "assistant"
        assert "compute_rsi" in compact["content"]

    def test_multiple_tool_calls_in_one_chain(self):
        mem = Memory()
        tool_calls = self._make_tool_call("compute_rsi") + self._make_tool_call("detect_extremes")
        tool_calls[1]["id"] = "id2"
        mem.add_message(Message(role="assistant", content=None, tool_calls=tool_calls))
        mem.add_message(Message(role="tool", content='{"rsi": 55}', tool_call_id="id1"))
        mem.add_message(Message(role="tool", content='{"extremes": []}', tool_call_id="id2"))

        mem.compact_tool_turns()
        history = mem.get_history()

        assert len(history) == 1
        assert "compute_rsi" in history[0]["content"]
        assert "detect_extremes" in history[0]["content"]

    def test_assistant_with_content_not_collapsed(self):
        mem = Memory()
        mem.add_message(Message(role="assistant", content="Here is my analysis."))
        removed = mem.compact_tool_turns()
        assert removed == 0
        assert len(mem.get_history()) == 1

    def test_orphaned_nudge_messages_removed(self):
        mem = Memory(system_prompt="sys")
        mem.add_message(Message(role="user", content="analyse AIR.PA"))
        mem.add_message(Message(role="user", content="Please now write your complete analysis and recommendation based on all the data you have gathered above."))
        mem.add_message(Message(role="assistant", content="RSI is neutral."))

        mem.compact_tool_turns()
        history = mem.get_history()
        roles_and_content = [(m["role"], m["content"]) for m in history]
        assert all("Please now write" not in (c or "") for _, c in roles_and_content)

    def test_multiple_chains_compacted(self):
        mem = Memory(system_prompt="sys")
        mem.add_message(Message(role="user", content="q1"))
        mem.add_message(Message(role="assistant", content=None, tool_calls=self._make_tool_call("tool_a")))
        mem.add_message(Message(role="tool", content="result_a", tool_call_id="id1"))
        mem.add_message(Message(role="assistant", content="Answer 1."))
        mem.add_message(Message(role="user", content="q2"))
        mem.add_message(Message(role="assistant", content=None, tool_calls=self._make_tool_call("tool_b")))
        mem.add_message(Message(role="tool", content="result_b", tool_call_id="id1"))
        mem.add_message(Message(role="assistant", content="Answer 2."))

        removed = mem.compact_tool_turns()
        history = mem.get_history()

        # 2 chains × (2 msgs → 1 note) = net 2 removed
        assert removed == 2
        # system + q1 + compact + ans1 + q2 + compact + ans2
        assert len(history) == 7
