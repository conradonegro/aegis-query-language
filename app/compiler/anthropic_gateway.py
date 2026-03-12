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

class AnthropicLLMGateway(LLMGatewayProtocol):
    """
    A gateway to remote Anthropic API models (e.g., claude-3-5-sonnet-20241022).
    Enforces strict JSON schema generation.
    Fetches the API Key from the SecretsManager dynamically.
    """

    def __init__(
        self, 
        model: str = "claude-3-5-sonnet-20241022",
        strict_json: bool = True
    ):
        self.model = model
        self.strict_json = strict_json
        self.base_url = "https://api.anthropic.com/v1/messages"
        self.secrets = get_secrets_manager()

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        api_key = self.secrets.get_api_key("anthropic")
        if not api_key:
            raise LLMGenerationError("CRITICAL: Anthropic API key is missing. Check Vault/Env configuration.", raw_response="")

        start_time = time.perf_counter()
        
        # Anthropic extracts the system prompt from the messages array
        system_content = prompt.system_instruction

        messages = []
        for msg in prompt.chat_history:
            # Anthropic enforces alternating user/assistant roles, mapping system to user if it slips in
            role = msg.role if msg.role in ["user", "assistant"] else "user"
            messages.append({"role": role, "content": msg.content})

        messages.append({"role": "user", "content": prompt.user_prompt})
        
        # If strict json is required, we force Claude to start its response with a JSON bracket
        # using Anthropic's prefilling feature.
        if self.strict_json:
            messages.append({"role": "assistant", "content": "{"})

        payload: dict[str, Any] = {
            "model": self.model,
            "system": system_content,
            "messages": messages,
            "max_tokens": 4096,
            "temperature": 0.0,
        }

        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
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
            raise LLMGenerationError(f"HTTP Error communicating with Anthropic: {e}")
        except Exception as e:
             raise LLMGenerationError(f"Unexpected connection error with Anthropic: {e}")

        latency_ms = (time.perf_counter() - start_time) * 1000.0
        
        content_blocks = data.get("content", [])
        if not content_blocks:
            raise LLMGenerationError("Anthropic returned no content.", raw_response=str(data))
            
        message_content = content_blocks[0].get("text", "")
        
        if self.strict_json:
            # Prepend the opening brace we prefilled
            message_content = "{" + message_content
            try:
                parsed = json.loads(message_content)
                if "sql" not in parsed:
                    raise LLMGenerationError("Anthropic JSON response missing required 'sql' field.", raw_response=message_content)
                final_text = parsed["sql"]
                
                if ";" in final_text and len([s for s in final_text.split(";") if s.strip()]) > 1:
                     raise LLMGenerationError("Anthropic returned multiple SQL statements. Only single queries are permitted.", raw_response=message_content)
                     
            except json.JSONDecodeError:
                raise LLMGenerationError(f"Anthropic failed to return valid JSON. Raw output: {message_content[:100]}...", raw_response=message_content)
        else:
            final_text = message_content
            
        usage = data.get("usage", {})
        prompt_tokens = usage.get("input_tokens", 0)
        completion_tokens = usage.get("output_tokens", 0)

        return LLMResult(
            raw_text=final_text,
            model_id=self.model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )
