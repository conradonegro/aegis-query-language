from typing import Any

from app.compiler.base_gateway import RemoteLLMGateway
from app.compiler.models import PromptEnvelope
from app.compiler.ollama import LLMGenerationError


class GoogleLLMGateway(RemoteLLMGateway):
    """Gateway for Google Gemini models (e.g. gemini-1.5-pro)."""

    def __init__(self, model: str = "gemini-1.5-pro", strict_json: bool = True) -> None:
        super().__init__(model, strict_json)

    @property
    def _provider_name(self) -> str:
        return "google"

    @property
    def _endpoint_url(self) -> str:
        return (
            f"https://generativelanguage.googleapis.com/v1beta"
            f"/models/{self.model}:generateContent"
        )

    def _build_headers(self, api_key: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        }

    def _build_payload(self, prompt: PromptEnvelope) -> dict[str, Any]:
        contents: list[dict[str, Any]] = []
        for msg in prompt.chat_history:
            role = "user" if msg.role == "user" else "model"
            contents.append({"role": role, "parts": [{"text": msg.content}]})
        contents.append({"role": "user", "parts": [{"text": prompt.user_prompt}]})

        payload: dict[str, Any] = {
            "systemInstruction": {
                "role": "system",
                "parts": [{"text": prompt.system_instruction}],
            },
            "contents": contents,
            "generationConfig": {"temperature": 0.0},
        }
        if self.strict_json:
            payload["generationConfig"]["responseMimeType"] = "application/json"
        return payload

    def _extract_content(self, data: dict[str, Any]) -> str:
        candidates = data.get("candidates", [])
        if not candidates:
            raise LLMGenerationError(
                "Google returned no candidates.", raw_response=str(data)
            )
        return str(
            candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        )

    def _extract_usage(self, data: dict[str, Any]) -> tuple[int, int]:
        usage = data.get("usageMetadata", {})
        return usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0)
