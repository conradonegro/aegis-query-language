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

class GoogleLLMGateway(LLMGatewayProtocol):
    """
    A gateway to remote Google Gemini API models (e.g., gemini-1.5-pro).
    Enforces strict JSON schema generation.
    Fetches the API Key from the SecretsManager dynamically.
    """

    def __init__(
        self, 
        model: str = "gemini-1.5-pro",
        strict_json: bool = True
    ):
        self.model = model
        self.strict_json = strict_json
        self.secrets = get_secrets_manager()

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        api_key = self.secrets.get_api_key("google")
        if not api_key:
            raise LLMGenerationError("CRITICAL: Google API key is missing. Check Vault/Env configuration.", raw_response="")

        # Google API dynamically builds the URL with the model and key
        base_url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent?key={api_key}"
        
        start_time = time.perf_counter()
        
        system_content = prompt.system_instruction
        if prompt.schema_context:
            system_content += f"\n\nSchema Context:\n{prompt.schema_context}"

        contents = []
        for msg in prompt.chat_history:
            # Gemini roles: "user", "model"
            role = "user" if msg.role == "user" else "model"
            contents.append({"role": role, "parts": [{"text": msg.content}]})
            
        final_user_prompt = prompt.user_prompt
        if prompt.hints:
            final_user_prompt += f"\n\nHints:\n{prompt.hints}"
            
        contents.append({"role": "user", "parts": [{"text": final_user_prompt}]})

        payload: dict[str, Any] = {
            "systemInstruction": {
                "role": "system",
                "parts": [{"text": system_content}]
            },
            "contents": contents,
            "generationConfig": {
                "temperature": 0.0,
            }
        }
        
        if self.strict_json:
            payload["generationConfig"]["responseMimeType"] = "application/json"

        headers = {
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    base_url,
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            raise LLMGenerationError(f"HTTP Error communicating with Google: {e}")
        except Exception as e:
             raise LLMGenerationError(f"Unexpected connection error with Google: {e}")

        latency_ms = (time.perf_counter() - start_time) * 1000.0
        
        candidates = data.get("candidates", [])
        if not candidates:
            raise LLMGenerationError("Google returned no candidates.", raw_response=str(data))
            
        message_content = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        
        if self.strict_json:
            try:
                parsed = json.loads(message_content)
                if "sql" not in parsed:
                    raise LLMGenerationError("Google JSON response missing required 'sql' field.", raw_response=message_content)
                final_text = parsed["sql"]
                
                if ";" in final_text and len([s for s in final_text.split(";") if s.strip()]) > 1:
                     raise LLMGenerationError("Google returned multiple SQL statements. Only single queries are permitted.", raw_response=message_content)
                     
            except json.JSONDecodeError:
                raise LLMGenerationError(f"Google failed to return valid JSON. Raw output: {message_content[:100]}...", raw_response=message_content)
        else:
            final_text = message_content
            
        usage = data.get("usageMetadata", {})
        prompt_tokens = usage.get("promptTokenCount", 0)
        completion_tokens = usage.get("candidatesTokenCount", 0)

        return LLMResult(
            raw_text=final_text,
            model_id=self.model,
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens
        )
