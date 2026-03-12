import json
import logging
import time
from typing import Any

import httpx

from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError
from app.vault import get_secrets_manager

logger = logging.getLogger(__name__)

class XAILLMGateway(LLMGatewayProtocol):
    """
    A gateway to remote xAI API models (e.g., grok-2).
    Enforces strict JSON schema generation.
    Fetches the API Key from the SecretsManager dynamically.
    xAI's API is fully compatible with OpenAI's API structure.
    """

    def __init__(
        self, 
        model: str = "grok-2-latest",
        strict_json: bool = True
    ):
        self.model = model
        self.strict_json = strict_json
        self.base_url = "https://api.x.ai/v1/chat/completions"
        self.secrets = get_secrets_manager()

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        api_key = self.secrets.get_api_key("xai")
        if not api_key:
            raise LLMGenerationError("CRITICAL: XAI API key is missing. Check Vault/Env configuration.", raw_response="")

        start_time = time.perf_counter()
        
        messages = [
            {"role": "system", "content": prompt.system_instruction}
        ]
        
        for msg in prompt.chat_history:
            messages.append({"role": msg.role, "content": msg.content})

        messages.append({"role": "user", "content": prompt.user_prompt})

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "temperature": 0.0
        }
            
        if self.strict_json:
            payload["response_format"] = {"type": "json_object"}

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self.base_url,
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            raise LLMGenerationError(f"HTTP Error communicating with XAI: {e}")
        except Exception as e:
             raise LLMGenerationError(f"Unexpected connection error with XAI: {e}")

        latency_ms = (time.perf_counter() - start_time) * 1000.0
        
        choices = data.get("choices", [])
        if not choices:
            raise LLMGenerationError("XAI returned no choices.", raw_response=str(data))
            
        message_content = choices[0].get("message", {}).get("content", "")
        
        # Validate JSON is well-formed; the engine handles structural validation
        # (including refusal detection and sql/refused contract enforcement).
        if self.strict_json:
            try:
                json.loads(message_content)
            except json.JSONDecodeError:
                raise LLMGenerationError(f"XAI failed to return valid JSON. Raw output: {message_content[:100]}...", raw_response=message_content)
        final_text = message_content
            
        usage = data.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)

        return LLMResult(
            raw_text=final_text,
            model_id=self.model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )
