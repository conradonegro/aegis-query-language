import asyncio
import json
import logging
import os
import random
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx

from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError
from app.vault import get_secrets_manager

logger = logging.getLogger(__name__)

# Shared client — reuses the connection pool across all remote LLM calls instead
# of creating and tearing down a new pool on every request.
_http_client: httpx.AsyncClient = httpx.AsyncClient(timeout=60.0)


async def aclose_http_client() -> None:
    """Close the module-level shared httpx.AsyncClient.

    Intended to be called from app.main:lifespan during shutdown so the
    underlying connection pool is released. Without this, repeated lifespan
    cycles (e.g. TestClient startup/shutdown loops, dev-server reloads) leak
    open sockets to the configured remote LLM endpoint.
    """
    await _http_client.aclose()


# Retry configuration — overridable via environment variables.
_LLM_RETRY_COUNT = int(os.getenv("LLM_RETRY_COUNT", "3"))
_LLM_RETRY_BASE_DELAY = float(os.getenv("LLM_RETRY_BASE_DELAY", "2.0"))


class RemoteLLMGateway(ABC):
    """
    Abstract base for all remote (API-key-authenticated) LLM gateways.

    Handles the shared HTTP transport, latency measurement, JSON well-formedness
    validation, and LLMResult construction. Subclasses implement only the
    provider-specific payload building, header construction, and response
    extraction.
    """

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

    async def _handle_rate_limit(
        self, response: httpx.Response, attempt: int
    ) -> None:
        """Sleep for the appropriate backoff after a 429, or raise if the
        retry budget is exhausted.

        Extracted from `generate` so the main loop stays under the C901
        complexity threshold.
        """
        if attempt == _LLM_RETRY_COUNT:
            raise LLMGenerationError(
                f"Rate limited by {self._provider_name} after"
                f" {_LLM_RETRY_COUNT} retries.",
                raw_response=response.text,
            )

        retry_after_hdr = response.headers.get("Retry-After")
        delay = _LLM_RETRY_BASE_DELAY * (2 ** attempt)
        if retry_after_hdr:
            try:
                delay = float(retry_after_hdr)
            except ValueError:
                pass  # fall through to exponential backoff already set above
        delay += random.uniform(0.0, 1.0)  # jitter
        logger.warning(
            "Rate limited (429) by %s — retrying in %.1fs"
            " (attempt %d/%d)",
            self._provider_name,
            delay,
            attempt + 1,
            _LLM_RETRY_COUNT,
        )
        await asyncio.sleep(delay)

    def _decode_response(self, response: httpx.Response) -> dict[str, Any]:
        """Raise-for-status + JSON decode, normalizing httpx errors into
        LLMGenerationError. Extracted to keep `generate` under C901."""
        try:
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            raise LLMGenerationError(
                f"HTTP error communicating with {self._provider_name}: {e}"
            ) from e
        return data  # type: ignore[no-any-return]

    def _validate_json_payload(self, message_content: str) -> None:
        """Raise LLMGenerationError if `message_content` is not valid JSON
        (only when strict_json is enabled)."""
        if not self.strict_json:
            return
        try:
            json.loads(message_content)
        except json.JSONDecodeError as e:
            raise LLMGenerationError(
                f"{self._provider_name} returned invalid JSON. "
                f"Raw: {message_content[:100]}...",
                raw_response=message_content,
            ) from e

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        api_key = self.secrets.get_api_key(self._provider_name)
        if not api_key:
            raise LLMGenerationError(
                f"CRITICAL: {self._provider_name} API key is missing. "
                "Check Vault/Env configuration.",
                raw_response="",
            )

        for attempt in range(_LLM_RETRY_COUNT + 1):
            start_time = time.perf_counter()

            try:
                response = await _http_client.post(
                    self._endpoint_url,
                    json=self._build_payload(prompt),
                    headers=self._build_headers(api_key),
                )
            except httpx.HTTPError as e:
                raise LLMGenerationError(
                    f"HTTP error communicating with {self._provider_name}: {e}"
                ) from e
            except Exception as e:
                raise LLMGenerationError(
                    f"Unexpected error with {self._provider_name}: {e}"
                ) from e

            if response.status_code == 429:
                await self._handle_rate_limit(response, attempt)
                continue

            data = self._decode_response(response)
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            message_content = self._extract_content(data)
            self._validate_json_payload(message_content)

            prompt_tokens, completion_tokens = self._extract_usage(data)
            return LLMResult(
                raw_text=message_content,
                model_id=self.model,
                latency_ms=latency_ms,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )

        # Unreachable — the loop always returns or raises before exhaustion.
        raise LLMGenerationError(  # pragma: no cover
            f"Exhausted retries for {self._provider_name}."
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
