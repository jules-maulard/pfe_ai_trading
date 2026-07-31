from __future__ import annotations

import logging
from typing import Dict, List, Any

from .entities import Message

logger = logging.getLogger(__name__)


class Memory:
    def __init__(
        self,
        system_prompt: str = "",
        max_messages: int | None = None,
        max_tokens: int | None = None,
        compression_threshold: float = 0.80,
        keep_last_k: int = 4,
    ) -> None:
        self._history: List[Message] = []
        self._max_messages = max_messages
        self._max_tokens = max_tokens
        self._compression_threshold = compression_threshold
        self._keep_last_k = keep_last_k
        if system_prompt:
            self._history.append(Message(role="system", content=system_prompt))

    # ─── Token counting ──────────────────────────────────────────────

    @property
    def current_tokens(self) -> int:
        return self._count_tokens(self.get_history())

    def _count_tokens(self, messages: List[Dict[str, Any]]) -> int:
        # Approximate: ~4 chars per token
        return sum(len(m.get("content") or "") // 4 for m in messages)

    # ─── Add / evict ─────────────────────────────────────────────────

    def add_message(self, message: Message) -> None:
        self._history.append(message)
        if self._max_messages is not None and len(self._history) > self._max_messages:
            self._evict_oldest()
        if self._max_tokens is not None:
            self._trim_to_budget()

    def _evict_oldest(self) -> None:
        has_system_prompt = self._history and self._history[0].role == "system"
        evict_from_index = 1 if has_system_prompt else 0
        self._history.pop(evict_from_index)

    def _trim_to_budget(self) -> None:
        has_system_prompt = self._history and self._history[0].role == "system"
        evict_from_index = 1 if has_system_prompt else 0
        while (
            len(self._history) > evict_from_index + 1
            and self.current_tokens > self._max_tokens
        ):
            self._history.pop(evict_from_index)

    # ─── Compression primitives ──────────────────────────────────────

    def needs_compression(self) -> bool:
        if self._max_tokens is None:
            return False
        return self.current_tokens >= int(self._max_tokens * self._compression_threshold)

    def get_compressible_messages(self) -> List[Dict[str, Any]]:
        has_system = self._history and self._history[0].role == "system"
        start = 1 if has_system else 0
        end = max(start, len(self._history) - self._keep_last_k)
        return [msg.to_dict() for msg in self._history[start:end]]

    def replace_with_summary(self, summary: str) -> None:
        has_system = self._history and self._history[0].role == "system"
        start = 1 if has_system else 0
        end = max(start, len(self._history) - self._keep_last_k)
        if end <= start:
            return
        del self._history[start:end]
        summary_msg = Message(
            role="system",
            content=f"## Conversation summary so far:\n{summary}",
        )
        self._history.insert(start, summary_msg)
        logger.info("Context compressed — replaced %d messages with summary", end - start)

    # ─── History access ──────────────────────────────────────────────

    def compact_tool_turns(self) -> int:
        """Collapse completed agentic tool-call chains into compact single-line notes.

        A tool-call chain is: assistant[tool_calls, content=None] followed by
        one or more tool[result] messages. Each such chain is replaced by a single
        assistant message listing which tools were called, preserving readability
        without the structural noise.

        Also removes orphaned nudge injections (user messages asking for synthesis)
        that no longer have matching agentic context.

        Returns the number of messages removed.
        """
        original_len = len(self._history)
        compacted: List[Message] = []
        i = 0
        while i < len(self._history):
            msg = self._history[i]
            # Detect start of a tool-call chain: assistant with tool_calls and no text content
            if msg.role == "assistant" and msg.tool_calls and not (msg.content or "").strip():
                tool_names: List[str] = []
                for tc in msg.tool_calls:
                    if isinstance(tc, dict):
                        name = tc.get("function", {}).get("name", "?")
                    else:
                        fn = getattr(tc, "function", None)
                        name = getattr(fn, "name", "?") if fn else "?"
                    tool_names.append(name)
                # Skip all following tool result messages
                j = i + 1
                while j < len(self._history) and self._history[j].role == "tool":
                    j += 1
                compact_content = f"[Tool calls: {', '.join(tool_names)}]"
                compacted.append(Message(role="assistant", content=compact_content))
                i = j
            else:
                compacted.append(msg)
                i += 1

        # Strip orphaned nudge injections (injected user messages asking for synthesis)
        _nudge_text = "Please now write your complete analysis"
        compacted = [
            m for m in compacted
            if not (m.role == "user" and (m.content or "").startswith(_nudge_text))
        ]

        self._history = compacted
        removed = original_len - len(self._history)
        if removed > 0:
            logger.debug("compact_tool_turns: removed %d messages", removed)
        return removed

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


class LongTermMemory:
    """Stores extracted key facts across conversation turns.

    Facts are injected into the system prompt to maintain context
    even after working memory is compressed or evicted.
    """

    def __init__(self, max_facts: int = 20) -> None:
        self._facts: Dict[str, str] = {}
        self._max_facts = max_facts

    def add_fact(self, key: str, value: str) -> None:
        if len(self._facts) >= self._max_facts and key not in self._facts:
            oldest_key = next(iter(self._facts))
            del self._facts[oldest_key]
        self._facts[key] = value

    def get_facts(self) -> Dict[str, str]:
        return dict(self._facts)

    def to_prompt_section(self) -> str:
        if not self._facts:
            return ""
        lines = ["", "# Key facts from this session"]
        for key, value in self._facts.items():
            lines.append(f"- **{key}**: {value}")
        return "\n".join(lines)

    def clear(self) -> None:
        self._facts.clear()
