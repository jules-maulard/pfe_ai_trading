from src.agents.memory import Memory
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
        assert history[1]["content"] == "msg"
