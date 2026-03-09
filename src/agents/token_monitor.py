from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class TurnUsage:
    prompt_tokens: int
    completion_tokens: int

    @property
    def total(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class TokenMonitor:
    def __init__(self) -> None:
        self._turns: List[TurnUsage] = []

    def record(self, prompt_tokens: int, completion_tokens: int) -> None:
        self._turns.append(TurnUsage(prompt_tokens, completion_tokens))

    def reset(self) -> None:
        self._turns.clear()

    @property
    def turns(self) -> int:
        return len(self._turns)

    @property
    def total_prompt_tokens(self) -> int:
        return sum(t.prompt_tokens for t in self._turns)

    @property
    def total_completion_tokens(self) -> int:
        return sum(t.completion_tokens for t in self._turns)

    @property
    def total_tokens(self) -> int:
        return self.total_prompt_tokens + self.total_completion_tokens

    @property
    def last_context_tokens(self) -> int:
        return self._turns[-1].prompt_tokens if self._turns else 0

    def stats(self) -> dict:
        return {
            "llm_calls": self.turns,
            "last_context_tokens": self.last_context_tokens,
            "total_prompt_tokens": self.total_prompt_tokens,
            "total_completion_tokens": self.total_completion_tokens,
            "total_tokens": self.total_tokens,
        }
