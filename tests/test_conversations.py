"""Tests for conversation history persistence."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from src.agents.conversations import ConversationStore, PersistentMemory
from src.agents.entities import Message


@pytest.fixture
def tmp_store(tmp_path):
    return ConversationStore(directory=tmp_path)


class TestConversationStore:
    def test_create_and_load(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Test Conv", agent_name="MACD")
        assert len(conv_id) == 12

        payload = tmp_store.load(conv_id)
        assert payload["name"] == "Test Conv"
        assert payload["agent_name"] == "MACD"
        assert payload["history"] == []

    def test_list_empty(self, tmp_store: ConversationStore):
        assert tmp_store.list() == []

    def test_list_returns_metadata(self, tmp_store: ConversationStore):
        tmp_store.create(name="A")
        tmp_store.create(name="B")
        items = tmp_store.list()
        assert len(items) == 2
        assert all("id" in c and "name" in c for c in items)

    def test_save_and_load_history(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Hist Test")
        history = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
        ]
        tmp_store.save(conv_id, history)
        payload = tmp_store.load(conv_id)
        assert payload["history"] == history
        assert payload["last_updated"] != payload["created_at"]

    def test_rename(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Old Name")
        tmp_store.rename(conv_id, "New Name")
        payload = tmp_store.load(conv_id)
        assert payload["name"] == "New Name"

    def test_delete(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="To Delete")
        tmp_store.delete(conv_id)
        assert tmp_store.list() == []
        with pytest.raises(FileNotFoundError):
            tmp_store.load(conv_id)

    def test_export_json(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Export")
        tmp_store.save(conv_id, [{"role": "user", "content": "test"}])
        exported = tmp_store.export_json(conv_id)
        data = json.loads(exported)
        assert data["id"] == conv_id
        assert len(data["history"]) == 1

    def test_load_nonexistent_raises(self, tmp_store: ConversationStore):
        with pytest.raises(FileNotFoundError):
            tmp_store.load("nonexistent123")

    def test_path_traversal_safe(self, tmp_store: ConversationStore):
        """Ensure path traversal characters are sanitized."""
        conv_id = tmp_store.create(name="safe")
        # Attempt to use a malicious ID
        malicious_id = "../../../etc/passwd"
        with pytest.raises(FileNotFoundError):
            tmp_store.load(malicious_id)


class TestPersistentMemory:
    def test_persist_saves_history(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Persist Test")
        mem = PersistentMemory(
            store=tmp_store,
            conversation_id=conv_id,
            system_prompt="System",
        )
        mem.add_message(Message(role="user", content="Hello"))
        mem.add_message(Message(role="assistant", content="Hi!"))
        mem.persist()

        payload = tmp_store.load(conv_id)
        assert len(payload["history"]) == 3  # system + user + assistant

    def test_from_existing_loads_history(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Load Test")
        history = [
            {"role": "system", "content": "Sys prompt"},
            {"role": "user", "content": "What?"},
            {"role": "assistant", "content": "Answer."},
        ]
        tmp_store.save(conv_id, history)

        mem = PersistentMemory.from_existing(
            store=tmp_store,
            conversation_id=conv_id,
            system_prompt="Sys prompt",
        )
        h = mem.get_history()
        assert len(h) == 3
        assert h[0]["role"] == "system"
        assert h[1]["content"] == "What?"

    def test_from_existing_updates_system_prompt(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="SP Update")
        history = [
            {"role": "system", "content": "Old prompt"},
            {"role": "user", "content": "Hi"},
        ]
        tmp_store.save(conv_id, history)

        mem = PersistentMemory.from_existing(
            store=tmp_store,
            conversation_id=conv_id,
            system_prompt="New prompt",
        )
        h = mem.get_history()
        assert h[0]["content"] == "New prompt"

    def test_conversation_id_property(self, tmp_store: ConversationStore):
        conv_id = tmp_store.create(name="Prop Test")
        mem = PersistentMemory(store=tmp_store, conversation_id=conv_id)
        assert mem.conversation_id == conv_id
