from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from utils.logger import get_logger

logger = get_logger(__name__)

try:
    from groq import AsyncGroq
except ImportError:
    logger.error("The 'groq' package is not installed. Run: pip install groq")
    sys.exit(1)


class LlmClient:
    def __init__(self, api_key: str, model: str, max_retries: int = 3, retry_delay: float = 1.0) -> None:
        self._model = model
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._client = AsyncGroq(api_key=api_key)

    async def get_response(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]] | None = None,
    ):
        last_exception: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto" if tools else None,
                )
                return response.choices[0]
            except Exception as exc:
                last_exception = exc
                logger.warning("LLM request failed (attempt %d/%d): %s", attempt, self._max_retries, exc)
                if attempt < self._max_retries:
                    await asyncio.sleep(self._retry_delay * attempt)
        raise ConnectionError(f"LLM request failed after {self._max_retries} attempts: {last_exception}")
