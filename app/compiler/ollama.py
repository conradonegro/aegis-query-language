import json
import logging
import time
from typing import Any

import httpx

from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.models import LLMResult, PromptEnvelope

logger = logging.getLogger(__name__)

class LLMGenerationError(Exception):
    """Raised when the LLM fails to generate a valid response that meets strict constraints."""
    def __init__(self, message: str, raw_response: str = ""):
        super().__init__(message)
        self.raw_response = raw_response


class OllamaLLMGateway(LLMGatewayProtocol):
    """
    A gateway to a local Ollama instance (e.g. localhost:11434).
    Enforces strict JSON schema generation.
    """

    def __init__(
        self, 
        base_url: str = "http://localhost:11434", 
        model: str = "llama3",
        strict_json: bool = True
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.strict_json = strict_json
        
        # The exact JSON schema we require Ollama to output
        self.json_schema = {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The final abstracted SQL dialect string"
                }
            },
            "required": ["sql"]
        }

    async def generate(self, prompt: PromptEnvelope) -> LLMResult:
        """
        Sends the PromptEnvelope to Ollama and enforces strict JSON output parsing.
        """
        start_time = time.perf_counter()
        
        # Format the combined prompt 
        # (Ollama API just takes 'prompt', so we stitch our Envelope for now, 
        # or use the /api/chat endpoint if we want explicit system/user roles)
        
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": prompt.system_instruction},
                {"role": "system", "content": f"Schema Context:\n{prompt.schema_context}"},
                {"role": "user", "content": prompt.user_prompt}
            ],
            "stream": False # We must buffer the entire JSON response to validate it, no partial streams
        }
        
        if prompt.hints:
            payload["messages"].append({"role": "user", "content": f"Hints:\n{prompt.hints}"})
            
        if self.strict_json:
            payload["format"] = self.json_schema

        try:
            # We use a 120s timeout by default because Ollama might need to
            # cold-load the LLM weights into VRAM on the first query.
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{self.base_url}/api/chat",
                    json=payload
                )
                response.raise_for_status()
                data = response.json()
        except httpx.HTTPError as e:
            raise LLMGenerationError(f"HTTP Error communicating with Ollama: {e}")
        except Exception as e:
             raise LLMGenerationError(f"Unexpected connection error with Ollama: {e}")

        latency_ms = (time.perf_counter() - start_time) * 1000.0
        
        message_content = data.get("message", {}).get("content", "")
        
        # Evaluate Strict JSON Constraints
        if self.strict_json:
            try:
                parsed = json.loads(message_content)
                if "sql" not in parsed:
                    raise LLMGenerationError("Ollama JSON response missing required 'sql' field.", raw_response=message_content)
                # Extract just the SQL string
                final_text = parsed["sql"]
                
                # Assert no multi-statements (SafetyEngine handles deep AST, but surface check here)
                if ";" in final_text and len([s for s in final_text.split(";") if s.strip()]) > 1:
                     raise LLMGenerationError("Ollama returned multiple SQL statements. Only single queries are permitted.", raw_response=message_content)
                     
            except json.JSONDecodeError:
                raise LLMGenerationError(f"Ollama failed to return valid JSON. Raw output: {message_content[:100]}...", raw_response=message_content)
        else:
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
