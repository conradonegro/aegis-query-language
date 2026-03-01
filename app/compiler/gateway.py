import asyncio
import time

from app.compiler.models import LLMResult, PromptEnvelope


class MockLLMGateway:
    """
    A stable, mock LLM provider gateway for v1 pipeline testing.
    Verifies that the `PromptEnvelope` arrives unchanged and simulates an AI response.
    """

    def __init__(self, mock_response_sql: str = "SELECT * FROM abstract_table"):
        self.mock_response_sql = mock_response_sql

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        """
        Takes the immutable PromptEnvelope and simulates returning an AbstractQuery.
        """
        # Simulate network latency (50-100ms)
        start_time = time.time()
        await asyncio.sleep(0.05)

        # Calculate simulated latency
        latency_ms = (time.time() - start_time) * 1000.0

        # Provide the hardcoded mock SQL
        raw_sql = self.mock_response_sql

        # Count simulated tokens
        # Very rough approximation: 1 token per 4 chars
        prompt_lengths = len(prompt.system_instruction) + len(prompt.user_prompt)
        prompt_tokens = max(1, prompt_lengths // 4)
        completion_tokens = max(1, len(raw_sql) // 4)

        return LLMResult(
            raw_text=raw_sql,
            model_id="mock-aegis-v1",
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )
