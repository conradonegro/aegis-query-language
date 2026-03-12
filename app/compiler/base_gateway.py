import json
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx

from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError
from app.vault import get_secrets_manager

logger = logging.getLogger(__name__)


class RemoteLLMGateway(ABC):
    """
    Abstract base for all remote (API-key-authenticated) LLM gateways.

    Handles the shared HTTP transport, latency measurement, JSON well-formedness
    validation, and LLMResult construction. Subclasses implement only the
    provider-specific payload building, header construction, and response
    extraction.
    """

    _timeout: float = 60.0

    def __init__(self, model: str, strict_json: bool = True) -> None:
        self.model = model
        self.strict_json = strict_json
        self.secrets = get_secrets_manager()

    @property
    @abstractmethod
    def _provider_name(self) -> str:
        """Lower-case provider slug used for API key lookup (e.g. 'openai')."""
        ...

    @property
    @abstractmethod
    def _endpoint_url(self) -> str:
        """The full URL to POST to."""
        ...

    @abstractmethod
    def _build_headers(self, api_key: str) -> dict[str, str]:
        """Construct provider-specific request headers."""
        ...

    @abstractmethod
    def _build_payload(self, prompt: PromptEnvelope) -> dict[str, Any]:
        """Construct the provider-specific request payload."""
        ...

    @abstractmethod
    def _extract_content(self, data: dict[str, Any]) -> str:
        """Extract the text content from the provider response JSON."""
        ...

    @abstractmethod
    def _extract_usage(self, data: dict[str, Any]) -> tuple[int, int]:
        """Return (prompt_tokens, completion_tokens) from the provider response."""
        ...

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        api_key = self.secrets.get_api_key(self._provider_name)
        if not api_key:
            raise LLMGenerationError(
                f"CRITICAL: {self._provider_name} API key is missing. "
                "Check Vault/Env configuration.",
                raw_response="",
            )
        start_time = time.perf_counter()

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(
                    self._endpoint_url,
                    json=self._build_payload(prompt),
                    headers=self._build_headers(api_key),
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            raise LLMGenerationError(
                f"HTTP error communicating with {self._provider_name}: {e}"
            )
        except Exception as e:
            raise LLMGenerationError(
                f"Unexpected error with {self._provider_name}: {e}"
            )

        latency_ms = (time.perf_counter() - start_time) * 1000.0
        message_content = self._extract_content(data)

        if self.strict_json:
            try:
                json.loads(message_content)
            except json.JSONDecodeError:
                raise LLMGenerationError(
                    f"{self._provider_name} returned invalid JSON. "
                    f"Raw: {message_content[:100]}...",
                    raw_response=message_content,
                )

        prompt_tokens, completion_tokens = self._extract_usage(data)
        return LLMResult(
            raw_text=message_content,
            model_id=self.model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
        )


class OpenAICompatibleGateway(RemoteLLMGateway):
    """
    Base for providers with an OpenAI-compatible chat completions API
    (OpenAI, xAI/Grok, etc.). Subclasses only need to supply
    _provider_name and _endpoint_url.
    """

    def _build_headers(self, api_key: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def _build_payload(self, prompt: PromptEnvelope) -> dict[str, Any]:
        messages: list[dict[str, str]] = [
            {"role": "system", "content": prompt.system_instruction}
        ]
        for msg in prompt.chat_history:
            messages.append({"role": msg.role, "content": msg.content})
        messages.append({"role": "user", "content": prompt.user_prompt})

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "temperature": 0.0,
        }
        if self.strict_json:
            payload["response_format"] = {"type": "json_object"}
        return payload

    def _extract_content(self, data: dict[str, Any]) -> str:
        choices = data.get("choices", [])
        if not choices:
            raise LLMGenerationError(
                f"{self._provider_name} returned no choices.",
                raw_response=str(data),
            )
        return str(choices[0].get("message", {}).get("content", ""))

    def _extract_usage(self, data: dict[str, Any]) -> tuple[int, int]:
        usage = data.get("usage", {})
        return usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)
