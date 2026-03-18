import asyncio
import re
import time

from app.compiler.models import LLMResult, PromptEnvelope

_TABLE_ALIAS_RE = re.compile(r"^- Table Alias:\s*(\S+)", re.MULTILINE)


class MockLLMGateway:
    """
    A stable, mock LLM provider gateway for v1 pipeline testing.
    Verifies that the `PromptEnvelope` arrives unchanged and simulates an AI response.

    When no explicit mock_response_sql is provided the gateway dynamically
    resolves the first table alias from the rendered system prompt so the
    generated SQL is valid against any schema (real BIRD, test, future).
    """

    def __init__(self, mock_response_sql: str | None = None):
        self.mock_response_sql = mock_response_sql

    def _resolve_sql(self, prompt: PromptEnvelope) -> str:
        if self.mock_response_sql:
            return self.mock_response_sql
        match = _TABLE_ALIAS_RE.search(prompt.system_instruction)
        if match:
            return f"SELECT count(*) FROM {match.group(1)}"
        return "SELECT 1"

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        """
        Takes the frozen PromptEnvelope and simulates returning an AbstractQuery.
        """
        start_time = time.time()
        await asyncio.sleep(0.05)

        latency_ms = (time.time() - start_time) * 1000.0

        sql = self._resolve_sql(prompt)
        prompt_lengths = len(prompt.system_instruction) + len(prompt.user_prompt)
        prompt_tokens = max(1, prompt_lengths // 4)
        completion_tokens = max(1, len(sql) // 4)

        return LLMResult(
            raw_text=sql,
            model_id="mock-aegis-v1",
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )
