"""Persistent conversation history store.

Stores chat conversations as JSON files under `database/conversations/`.
Each conversation is a standalone file containing metadata and the full
message history, enabling users to resume past sessions.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .entities import Message
from .memory import Memory

logger = logging.getLogger(__name__)

_DEFAULT_DIR = Path(__file__).resolve().parents[2] / "database" / "conversations"


class ConversationStore:
    """CRUD operations for conversation files stored as JSON."""

    def __init__(self, directory: Path | str | None = None) -> None:
        self._dir = Path(directory) if directory else _DEFAULT_DIR
        self._dir.mkdir(parents=True, exist_ok=True)

    # ─── Create ──────────────────────────────────────────────────────

    def create(self, name: str = "", agent_name: str = "") -> str:
        """Create a new empty conversation and return its ID."""
        conv_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        payload: Dict[str, Any] = {
            "id": conv_id,
            "name": name or f"Conversation {conv_id[:6]}",
            "agent_name": agent_name,
            "created_at": now,
            "last_updated": now,
            "history": [],
        }
        self._write(conv_id, payload)
        logger.info("Created conversation %s (%s)", conv_id, payload["name"])
        return conv_id

    # ─── Read ────────────────────────────────────────────────────────

    def load(self, conv_id: str) -> Dict[str, Any]:
        """Load a conversation payload by ID. Raises FileNotFoundError if missing."""
        path = self._path(conv_id)
        if not path.exists():
            raise FileNotFoundError(f"Conversation {conv_id} not found")
        return json.loads(path.read_text(encoding="utf-8"))

    def list(self) -> List[Dict[str, Any]]:
        """Return metadata for all conversations, sorted by last_updated desc."""
        conversations = []
        for path in self._dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                conversations.append({
                    "id": data["id"],
                    "name": data.get("name", ""),
                    "agent_name": data.get("agent_name", ""),
                    "created_at": data.get("created_at", ""),
                    "last_updated": data.get("last_updated", ""),
                    "message_count": len(data.get("history", [])),
                })
            except (json.JSONDecodeError, KeyError):
                logger.warning("Skipping corrupt conversation file: %s", path.name)
        conversations.sort(key=lambda c: c.get("last_updated", ""), reverse=True)
        return conversations

    # ─── Update ──────────────────────────────────────────────────────

    def save(self, conv_id: str, history: List[Dict[str, Any]]) -> None:
        """Persist the current message history for an existing conversation."""
        payload = self.load(conv_id)
        payload["history"] = history
        payload["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._write(conv_id, payload)

    def rename(self, conv_id: str, new_name: str) -> None:
        """Rename a conversation."""
        payload = self.load(conv_id)
        payload["name"] = new_name
        payload["last_updated"] = datetime.now(timezone.utc).isoformat()
        self._write(conv_id, payload)

    # ─── Delete ──────────────────────────────────────────────────────

    def delete(self, conv_id: str) -> None:
        """Delete a conversation file."""
        path = self._path(conv_id)
        if path.exists():
            path.unlink()
            logger.info("Deleted conversation %s", conv_id)

    # ─── Export ──────────────────────────────────────────────────────

    def export_json(self, conv_id: str) -> str:
        """Return the full conversation as a formatted JSON string."""
        payload = self.load(conv_id)
        return json.dumps(payload, ensure_ascii=False, indent=2)

    # ─── Internal ────────────────────────────────────────────────────

    def _path(self, conv_id: str) -> Path:
        # Sanitize to prevent path traversal
        safe_id = "".join(c for c in conv_id if c.isalnum())
        return self._dir / f"{safe_id}.json"

    def _write(self, conv_id: str, payload: Dict[str, Any]) -> None:
        path = self._path(conv_id)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class PersistentMemory(Memory):
    """A Memory subclass that auto-persists to disk after assistant responses.

    Persistence is triggered explicitly via `persist()` — the caller (UI layer)
    decides when to save (typically after the assistant finishes responding).
    """

    def __init__(
        self,
        store: ConversationStore,
        conversation_id: str,
        system_prompt: str = "",
        max_messages: int | None = None,
        max_tokens: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            system_prompt=system_prompt,
            max_messages=max_messages,
            max_tokens=max_tokens,
            **kwargs,
        )
        self._store = store
        self._conversation_id = conversation_id

    @property
    def conversation_id(self) -> str:
        return self._conversation_id

    def persist(self) -> None:
        """Save the current history to disk."""
        try:
            self._store.save(self._conversation_id, self.get_history())
        except Exception as exc:
            logger.warning("Failed to persist conversation %s: %s", self._conversation_id, exc)

    @classmethod
    def from_existing(
        cls,
        store: ConversationStore,
        conversation_id: str,
        system_prompt: str = "",
        **kwargs,
    ) -> "PersistentMemory":
        """Load an existing conversation into a PersistentMemory instance."""
        instance = cls(
            store=store,
            conversation_id=conversation_id,
            system_prompt="",
            **kwargs,
        )
        payload = store.load(conversation_id)
        history = payload.get("history", [])
        # Reconstruct internal history from serialized messages
        instance._history = [
            Message(
                role=msg["role"],
                content=msg.get("content"),
                tool_calls=msg.get("tool_calls"),
                tool_call_id=msg.get("tool_call_id"),
            )
            for msg in history
        ]
        # If history is empty or system prompt changed, set the system prompt
        if not instance._history or (
            system_prompt and instance._history[0].role == "system"
            and instance._history[0].content != system_prompt
        ):
            instance.update_system_prompt(system_prompt)
        elif not instance._history and system_prompt:
            instance._history.insert(0, Message(role="system", content=system_prompt))
        return instance
