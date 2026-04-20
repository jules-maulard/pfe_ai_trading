from __future__ import annotations

import logging
from typing import Any, Dict, List

from ..utils import get_logger
logger = get_logger(__name__)

import litellm
from litellm import Router

litellm.suppress_debug_info = True
logging.getLogger("LiteLLM").setLevel(logging.WARNING)
logging.getLogger("LiteLLM Router").setLevel(logging.WARNING)
logging.getLogger("LiteLLM Proxy").setLevel(logging.WARNING)


class LlmClient:
    def __init__(self, api_keys: List[str], model: str, max_retries: int = 3, retry_delay: float = 1.0) -> None:
        self._model = model
        model_list = self._build_model_list(api_keys, model)
        self._router = Router(
            model_list=model_list,
            routing_strategy="least-busy",
            num_retries=max(max_retries, len(api_keys)),
            retry_after=retry_delay,
            allowed_fails=1,
            cooldown_time=60,
        )

    async def get_response(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]] | None = None,
    ):
        kwargs: Dict[str, Any] = {"model": self._model, "messages": messages}
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        response = await self._router.acompletion(**kwargs)
        return response.choices[0], response.usage

    @staticmethod
    def _build_model_list(api_keys: List[str], model: str) -> List[Dict[str, Any]]:
        litellm_model = f"groq/{model}"
        return [
            {
                "model_name": model,
                "litellm_params": {
                    "model": litellm_model,
                    "api_key": key,
                },
            }
            for key in api_keys
        ]
