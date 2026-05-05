from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Tuple

import litellm

from ..utils import get_logger
logger = get_logger(__name__)

from .entities import Configuration, Message
from .llm_client import LlmClient
from .memory import LongTermMemory, Memory
from .server import Server
from .token_monitor import TokenMonitor
from .toolbox import ToolBox

_COMPRESSION_PROMPT = (
    "Summarize the following conversation in under 300 words. "
    "Preserve all key financial data, signals, ticker symbols, dates, "
    "and conclusions. Output only the summary, no preamble."
)

_FACT_EXTRACTION_PROMPT = (
    "Extract 1-5 key trading facts from this analysis. "
    "Return ONLY a JSON array: [{\"key\": \"short_label\", \"fact\": \"concise fact\"}]. "
    "No markdown, no preamble."
)


class Agent:
    def __init__(
        self,
        configuration: Configuration,
        llm_client: LlmClient,
        servers: List[Server],
        memory: Memory,
        token_monitor: TokenMonitor,
        long_term_memory: LongTermMemory | None = None,
    ) -> None:
        self._configuration = configuration
        self._llm_client = llm_client
        self._servers = servers
        self._toolbox = ToolBox()
        self._memory = memory
        self._token_monitor = token_monitor
        self._long_term_memory = long_term_memory

    @property
    def tools(self):
        return self._toolbox.tools

    @property
    def token_monitor(self) -> TokenMonitor:
        return self._token_monitor

    @property
    def prompts(self) -> list:
        result = []
        for server in self._servers:
            result.extend(server.prompts)
        return result

    @property
    def resources(self) -> list:
        result = []
        for server in self._servers:
            result.extend(server.resources)
        return result

    async def connect(self) -> None:
        for server in self._servers:
            await server.connect()
            self._toolbox.register_server(server)

        self._toolbox.register_read_resource_tool()

        system_prompt = self._build_system_prompt()
        self._memory.reset(system_prompt)

        logger.info(
            "Agent initialized — %d server(s), %d tool(s): %s",
            len(self._servers),
            len(self._toolbox.tools),
            [t.name for t in self._toolbox.tools],
        )

    async def disconnect(self) -> None:
        for server in self._servers:
            await server.disconnect()


    async def chat(self, user_input: str) -> str:
        self._memory.add_message(Message(role="user", content=user_input))
        _nudge_count = 0
        _max_nudges = 3
        while True:
            await self._maybe_compress_context()
            tools = self._toolbox.get_openai_tools()
            try:
                choice, usage = await self._llm_client.get_response(
                    messages=self._memory.get_history(),
                    tools=tools,
                )
            except litellm.RateLimitError as exc:
                wait = self._parse_retry_after(str(exc))
                logger.warning(
                    "Rate limit hit — waiting %.1fs before retry (TPM limit)", wait
                )
                await asyncio.sleep(wait)
                continue
            except litellm.BadRequestError as exc:
                # Log and print full exception for debugging parsing failures before any fix attempt.
                try:
                    logger.error("BadRequestError during LLM call: %s", exc)
                    logger.debug("BadRequestError repr: %r", exc)
                    # Also print to stdout for immediate terminal visibility.
                    print("BadRequestError during LLM call:", repr(exc))
                    print("BadRequestError args:", exc.args)
                except Exception:
                    logger.exception("Failed to log BadRequestError")

                msg = str(exc)
                # ── "Tool choice is none, but model called a tool" ──────────────
                # Happens when we sent tools=None but the model still called one.
                # Re-enable tools and retry without injecting anything.
                if "tool choice is none" in msg.lower():
                    logger.warning("tools=None but model called a tool — re-enabling tools and retrying.")
                    _nudge_count = 0
                    continue
                # ── output_parse_failed ──────────────────────────────────────────
                # Model produced unparseable output (usually context overflow).
                # Do NOT disable tools — just retry; the model should self-correct.
                if "parsing failed" in msg.lower() or "output_parse_failed" in msg.lower():
                    logger.warning("Model output parsing failed — retrying with tools still enabled.")
                    continue
                # ── tool call validation failed (wrong tool name) ────────────────
                if "tool call validation failed" in msg.lower() or "Tool call validation failed" in msg:
                    bad_tool = re.search(r"attempted to call tool '([^']+)'", msg)
                    bad_name = bad_tool.group(1) if bad_tool else "unknown"
                    valid_names = [t["function"]["name"] for t in self._toolbox.get_openai_tools()]

                    # Attempt fuzzy auto-correction before asking the model to retry
                    corrected = self._fuzzy_match_tool(bad_name, valid_names)
                    if corrected:
                        logger.warning(
                            "Auto-corrected tool name '%s' → '%s'", bad_name, corrected
                        )
                        # Extract the failed_generation JSON from the error message and
                        # parse the arguments properly (regex on nested JSON is unreliable)
                        args_str = "{}"
                        try:
                            # The error message contains: GroqException - {...JSON...}
                            outer_match = re.search(r"GroqException - (\{.*\})", msg, re.DOTALL)
                            if outer_match:
                                outer = json.loads(outer_match.group(1))
                                fg_raw = outer.get("error", {}).get("failed_generation", "")
                                if fg_raw:
                                    fg_obj = json.loads(fg_raw)
                                    args_str = json.dumps(fg_obj.get("arguments", {}))
                        except Exception as parse_exc:
                            logger.debug("Could not parse failed_generation args: %s", parse_exc)
                        tool_result = await self._toolbox.execute_tool_by_name(corrected, args_str)
                        # Inject as if the model had called the correct tool
                        self._memory.add_message(Message(
                            role="assistant",
                            content=f"[Auto-corrected tool call: {corrected}]",
                        ))
                        self._memory.add_message(Message(
                            role="tool",
                            content=tool_result,
                            tool_call_id=f"auto_{corrected}",
                        ))
                        continue

                    # No fuzzy match found — fall back to asking model to retry
                    logger.warning(
                        "Model tried invalid tool '%s'. Valid tools: %s. Injecting correction.",
                        bad_name,
                        valid_names,
                    )
                    self._memory.add_message(Message(
                        role="user",
                        content=(
                            f"The tool '{bad_name}' does not exist. "
                            f"You must only call tools from this exact list: {valid_names}. "
                            "Please retry your previous step using the correct tool name."
                        ),
                    ))
                    continue
                raise
            if usage:
                self._token_monitor.record(usage.prompt_tokens, usage.completion_tokens)
            assistant_message = choice.message
            raw = assistant_message.model_dump()

            self._memory.add_message(Message(
                role="assistant",
                content=raw.get("content"),
                tool_calls=raw.get("tool_calls"),
            ))

            if not assistant_message.tool_calls:
                content = assistant_message.content or ""
                if not content.strip() and _nudge_count < _max_nudges:
                    _nudge_count += 1
                    logger.warning(
                        "LLM returned empty content — nudging for synthesis (attempt %d/%d)",
                        _nudge_count, _max_nudges,
                    )
                    self._memory.add_message(Message(
                        role="user",
                        content=(
                            "Please now write your complete analysis and recommendation "
                            "based on all the data you have gathered above."
                        ),
                    ))
                    continue
                # Fire fact extraction in background — does not block return or burn tokens synchronously
                asyncio.ensure_future(self._extract_and_store_facts(content))
                # Compact tool-call scaffolding so future turns see clean Q&A context
                removed = self._memory.compact_tool_turns()
                if removed > 0:
                    logger.debug("Compacted %d agentic messages from working memory", removed)
                return content

            for tool_call in assistant_message.tool_calls:
                tool_result = await self._toolbox.execute_tool_call(tool_call)
                # Truncate large tool results to prevent context overflow which causes
                # the model to leak reasoning tokens into subsequent tool call arguments
                tool_result = self._truncate_tool_result(tool_result)
                self._memory.add_message(Message(
                    role="tool",
                    content=tool_result,
                    tool_call_id=tool_call.id,
                ))

    async def run_prompt(self, prompt_name: str, arguments: Dict[str, Any] | None = None) -> str:
        for server in self._servers:
            for prompt in server.prompts:
                if prompt.name == prompt_name:
                    prompt_text = await server.get_prompt(prompt_name, arguments)
                    return await self.chat(prompt_text)
        return json.dumps({"error": f"Prompt not found: {prompt_name}"})

    async def reset_conversation(self) -> None:
        system_prompt = self._build_system_prompt()
        self._memory.reset(system_prompt)
        self._token_monitor.reset()
        logger.info("Conversation reset")

    
    def _build_system_prompt(self) -> str:
        system_prompt = self._configuration.system_prompt

        all_resources = self.resources
        if all_resources:
            system_prompt += "\n\n# Available knowledge resources\nUse the read_resource tool to read one.\n"
            for resource in all_resources:
                uri = getattr(resource, "uri", "")
                desc = getattr(resource, "description", "") or ""
                system_prompt += f"\n- {uri}: {desc}"

        if self._long_term_memory:
            system_prompt += self._long_term_memory.to_prompt_section()

        return system_prompt



    def _maybe_nudge_for_synthesis(self, content: str, nudge_sent: bool) -> Tuple[bool, bool]:
        """Kept for backward compatibility — nudge logic is now inlined in chat()."""
        return False, nudge_sent

    # ─── Context engineering ─────────────────────────────────────────

    @staticmethod
    def _parse_retry_after(error_msg: str, default: float = 30.0) -> float:
        """Extract the suggested wait time (seconds) from a Groq RateLimitError message."""
        match = re.search(r"try again in\s+([\d.]+)s", error_msg, re.IGNORECASE)
        if match:
            return float(match.group(1)) + 1.0
        return default

    @staticmethod
    def _truncate_tool_result(result: str, max_chars: int = 3000) -> str:
        """Truncate tool results to avoid context overflow.

        Without truncation, 4-5 verbose sub-agent responses can fill the context
        window and cause the model to leak reasoning tokens into subsequent tool
        call arguments, producing unparseable JSON.
        """
        if len(result) <= max_chars:
            return result
        return result[:max_chars] + "\n[...response truncated for context budget...]"

    @staticmethod
    def _fuzzy_match_tool(bad_name: str, valid_names: List[str], threshold: float = 0.75) -> str | None:
        """Return the closest valid tool name if similarity exceeds threshold, else None."""
        from difflib import SequenceMatcher
        # Strip common model artifacts first
        clean = re.sub(r"<\|channel\|>.*$", "", bad_name).strip()
        # Exact match after cleanup?
        if clean in valid_names:
            return clean
        best, best_ratio = None, 0.0
        for name in valid_names:
            ratio = SequenceMatcher(None, clean, name).ratio()
            if ratio > best_ratio:
                best, best_ratio = name, ratio
        return best if best_ratio >= threshold else None

    async def _maybe_compress_context(self) -> None:
        """Compress old messages into a summary when approaching token budget."""
        if not self._memory.needs_compression():
            return
        compressible = self._memory.get_compressible_messages()
        if not compressible:
            return

        prompt_messages = [
            {"role": "system", "content": _COMPRESSION_PROMPT},
            {"role": "user", "content": json.dumps(compressible, ensure_ascii=False)},
        ]
        try:
            choice, usage = await self._llm_client.get_response(messages=prompt_messages)
            if usage:
                self._token_monitor.record(usage.prompt_tokens, usage.completion_tokens)
            summary = choice.message.content or ""
            self._memory.replace_with_summary(summary)
            logger.info("Context compressed — %d tokens saved", self._memory.current_tokens)
        except Exception as exc:
            logger.warning("Compression failed, skipping: %s", exc)

    async def _extract_and_store_facts(self, assistant_response: str) -> None:
        """Extract key facts from the assistant response into long-term memory."""
        if not self._long_term_memory:
            return
        if not assistant_response.strip():
            return

        prompt_messages = [
            {"role": "system", "content": _FACT_EXTRACTION_PROMPT},
            {"role": "user", "content": assistant_response},
        ]
        try:
            choice, usage = await self._llm_client.get_response(messages=prompt_messages)
            if usage:
                self._token_monitor.record(usage.prompt_tokens, usage.completion_tokens)
            raw = choice.message.content or ""
            facts = json.loads(raw)
            for item in facts:
                if isinstance(item, dict) and "key" in item and "fact" in item:
                    self._long_term_memory.add_fact(item["key"], item["fact"])
            # Refresh system prompt with new facts
            self._memory.update_system_prompt(self._build_system_prompt())
            logger.info("Extracted %d facts into long-term memory", len(facts))
        except (json.JSONDecodeError, Exception) as exc:
            logger.debug("Fact extraction skipped: %s", exc)
    