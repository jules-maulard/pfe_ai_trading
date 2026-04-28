from __future__ import annotations

from typing import Dict, List, Any

from .entities import Message


class Memory:
    def __init__(self, system_prompt: str = "", max_messages: int | None = None) -> None:
        self._history: List[Message] = []
        self._max_messages = max_messages
        if system_prompt:
            self._history.append(Message(role="system", content=system_prompt))

    def add_message(self, message: Message) -> None:
        self._history.append(message)
        if self._max_messages is not None and len(self._history) > self._max_messages:
            has_system_prompt = self._history and self._history[0].role == "system"
            evict_from_index = 1 if has_system_prompt else 0
            self._history.pop(evict_from_index)

    def get_history(self) -> List[Dict[str, Any]]:
        return [msg.to_dict() for msg in self._history]

    def reset(self, system_prompt: str = "") -> None:
        self._history.clear()
        if system_prompt:
            self._history.append(Message(role="system", content=system_prompt))

    def update_system_prompt(self, system_prompt: str) -> None:
        if self._history and self._history[0].role == "system":
            self._history[0] = Message(role="system", content=system_prompt)
        else:
            self._history.insert(0, Message(role="system", content=system_prompt))
