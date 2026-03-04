from fastapi import Request
from app.compiler.interfaces import LLMGatewayProtocol
from app.compiler.ollama import OllamaLLMGateway
from app.compiler.openai_gateway import OpenAILLMGateway
from app.compiler.anthropic_gateway import AnthropicLLMGateway
from app.compiler.google_gateway import GoogleLLMGateway
from app.compiler.xai_gateway import XAILLMGateway
from app.vault import get_secrets_manager

def get_llm_gateway(provider_id: str | None = None) -> LLMGatewayProtocol:
    """
    Factory to resolve the requested or default LLM Gateway.
    Fails fast if specific parameters (like remote API keys) are not resolved natively.
    """
    import os
    
    # Fallback to default
    if not provider_id:
        provider_id = os.getenv("LLM_PROVIDER", "ollama")
        
    provider_id = provider_id.lower()
    
    if provider_id.startswith("ollama"):
        # Format: ollama:llama3 or just ollama
        parts = provider_id.split(":")
        model = parts[1] if len(parts) > 1 else "llama3"
        url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        return OllamaLLMGateway(base_url=url, model=model)
        
    elif provider_id.startswith("openai"):
        parts = provider_id.split(":")
        model = parts[1] if len(parts) > 1 else "gpt-4o"
        return OpenAILLMGateway(model=model)
        
    elif provider_id.startswith("anthropic"):
        parts = provider_id.split(":")
        model = parts[1] if len(parts) > 1 else "claude-3-opus-20240229"
        return AnthropicLLMGateway(model=model)
        
    elif provider_id.startswith("google"):
        parts = provider_id.split(":")
        model = parts[1] if len(parts) > 1 else "gemini-pro"
        return GoogleLLMGateway(model=model)
        
    elif provider_id.startswith("xai"):
        parts = provider_id.split(":")
        model = parts[1] if len(parts) > 1 else "grok-1"
        return XAILLMGateway(model=model)
        
    else:
        raise ValueError(f"Unknown LLM Provider designated: {provider_id}")
