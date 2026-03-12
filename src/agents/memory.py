from __future__ import annotations

from typing import Dict, List, Any

from .entities import Message


class Memory:
    def __init__(self, system_prompt: str = "") -> None:
        self._history: List[Message] = []
        if system_prompt:
            self._history.append(Message(role="system", content=system_prompt))

    def add_message(self, message: Message) -> None:
        self._history.append(message)

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
