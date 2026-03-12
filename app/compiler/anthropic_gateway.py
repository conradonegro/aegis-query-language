from typing import Any

from app.compiler.base_gateway import RemoteLLMGateway
from app.compiler.models import PromptEnvelope
from app.compiler.ollama import LLMGenerationError


class AnthropicLLMGateway(RemoteLLMGateway):
    """
    Gateway for Anthropic models (e.g. claude-3-5-sonnet-20241022).

    Uses the Anthropic messages API with JSON prefilling: when strict_json is
    enabled, an assistant turn opening with "{" is appended to the messages so
    the model is forced to continue the JSON object rather than preamble it.
    """

    def __init__(
        self, model: str = "claude-3-5-sonnet-20241022", strict_json: bool = True
    ) -> None:
        super().__init__(model, strict_json)

    @property
    def _provider_name(self) -> str:
        return "anthropic"

    @property
    def _endpoint_url(self) -> str:
        return "https://api.anthropic.com/v1/messages"

    def _build_headers(self, api_key: str) -> dict[str, str]:
        return {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

    def _build_payload(self, prompt: PromptEnvelope) -> dict[str, Any]:
        messages: list[dict[str, str]] = []
        for msg in prompt.chat_history:
            # Anthropic enforces alternating user/assistant roles; map system → user.
            role = msg.role if msg.role in ["user", "assistant"] else "user"
            messages.append({"role": role, "content": msg.content})
        messages.append({"role": "user", "content": prompt.user_prompt})
        if self.strict_json:
            # Prefill: force the model to open a JSON object.
            messages.append({"role": "assistant", "content": "{"})
        return {
            "model": self.model,
            "system": prompt.system_instruction,
            "messages": messages,
            "max_tokens": 4096,
            "temperature": 0.0,
        }

    def _extract_content(self, data: dict[str, Any]) -> str:
        content_blocks = data.get("content", [])
        if not content_blocks:
            raise LLMGenerationError(
                "Anthropic returned no content.", raw_response=str(data)
            )
        text = str(content_blocks[0].get("text", ""))
        if self.strict_json:
            # Prepend the opening brace we prefilled via assistant prefilling.
            text = "{" + text
        return text

    def _extract_usage(self, data: dict[str, Any]) -> tuple[int, int]:
        usage = data.get("usage", {})
        return usage.get("input_tokens", 0), usage.get("output_tokens", 0)
