import os

from app.compiler.anthropic_gateway import AnthropicLLMGateway
from app.compiler.google_gateway import GoogleLLMGateway
from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.ollama import OllamaLLMGateway
from app.compiler.openai_gateway import OpenAILLMGateway
from app.compiler.provider_config import (
    assert_provider_allowed,
    parse_provider_id,
)
from app.compiler.xai_gateway import XAILLMGateway


def get_llm_gateway(provider_id: str | None = None) -> LLMGatewayProtocol:
    """
    Factory to resolve the requested or default LLM Gateway.

    ``provider_id`` must already have been validated and allowlist-checked by
    the router.  This function re-validates as defense-in-depth so the factory
    cannot be called with an unvetted string from any other call site.
    """
    if not provider_id:
        provider_id = os.getenv("LLM_PROVIDER", "ollama")

    # Normalise and re-assert the allowlist (defense-in-depth).
    normalised = parse_provider_id(provider_id)
    assert_provider_allowed(normalised)

    parts = normalised.split(":")
    prefix = parts[0]
    model_part = parts[1] if len(parts) > 1 else None

    if prefix == "ollama":
        url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        return OllamaLLMGateway(base_url=url, model=model_part or "llama3")

    if prefix == "openai":
        return OpenAILLMGateway(model=model_part or "gpt-4o")

    if prefix == "anthropic":
        return AnthropicLLMGateway(model=model_part or "claude-3-opus-20240229")

    if prefix == "google":
        return GoogleLLMGateway(model=model_part or "gemini-pro")

    if prefix == "xai":
        return XAILLMGateway(model=model_part or "grok-1")

    # parse_provider_id already rejects unknown prefixes; this is unreachable.
    raise ValueError(f"Unknown LLM provider prefix: '{prefix}'")
