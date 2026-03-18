import json
import logging
import time
from typing import Any

import httpx

from app.compiler.models import LLMResult, PromptEnvelope

logger = logging.getLogger(__name__)

# Shared client — long timeout because Ollama may need to cold-load model weights
# into VRAM on the first request.
_http_client: httpx.AsyncClient = httpx.AsyncClient(timeout=500.0)


class LLMGenerationError(Exception):
    """Raised when the LLM fails to generate a valid response that meets strict
    constraints."""
    def __init__(self, message: str, raw_response: str = ""):
        super().__init__(message)
        self.raw_response = raw_response


class OllamaLLMGateway:
    """
    A gateway to a local Ollama instance (e.g. localhost:11434).
    Always enforces structured JSON output via Ollama's native format parameter.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "llama3",
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model

        # The JSON schema we require Ollama to output.
        # Both sql and refused are optional at schema level so the LLM can
        # signal a refusal without being forced to invent a sql value.
        # The engine's LLMQueryResponse validator enforces the contract.
        self.json_schema = {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The final abstracted SQL dialect string"
                },
                "refused": {
                    "type": "boolean",
                    "description": "True if the request was refused"
                },
                "reason": {
                    "type": "string",
                    "description": "Reason for refusal if refused is true"
                }
            }
        }

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        """
        Sends the PromptEnvelope to Ollama and enforces strict JSON output parsing.
        """
        start_time = time.perf_counter()

        # Format the combined prompt
        # (Ollama API just takes 'prompt', so we stitch our Envelope for now,
        # or use the /api/chat endpoint if we want explicit system/user roles)

        messages = [
            {"role": "system", "content": prompt.system_instruction}
        ]

        for msg in prompt.chat_history:
            messages.append({"role": msg.role, "content": msg.content})

        messages.append({"role": "user", "content": prompt.user_prompt})

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            # We must buffer the entire JSON response to validate it, no partial streams
            "stream": False
        }

        payload["format"] = self.json_schema

        try:
            response = await _http_client.post(
                f"{self.base_url}/api/chat",
                json=payload
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            raise LLMGenerationError(
                f"HTTP Error communicating with Ollama: {e}"
            ) from e
        except Exception as e:
            raise LLMGenerationError(
                f"Unexpected connection error with Ollama: {e}"
            ) from e

        latency_ms = (time.perf_counter() - start_time) * 1000.0

        message_content = data.get("message", {}).get("content", "")

        # Validate JSON is well-formed; the engine handles structural validation
        # (including refusal detection and sql/refused contract enforcement).
        try:
            json.loads(message_content)
        except json.JSONDecodeError as e:
            raise LLMGenerationError(
                f"Ollama failed to return valid JSON. "
                f"Raw output: {message_content[:100]}...",
                raw_response=message_content,
            ) from e
        final_text = message_content

        # Ollama telemetry
        prompt_eval_count = data.get("prompt_eval_count", 0)
        eval_count = data.get("eval_count", 0)

        return LLMResult(
            raw_text=final_text,
            model_id=self.model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_eval_count,
            completion_tokens=eval_count
        )
