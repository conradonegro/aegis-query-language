import os
from unittest import mock

import pytest

from app.compiler.llm_factory import get_llm_gateway
from app.compiler.ollama import OllamaLLMGateway
from app.compiler.openai_gateway import OpenAILLMGateway
from app.compiler.anthropic_gateway import AnthropicLLMGateway
from app.compiler.google_gateway import GoogleLLMGateway
from app.compiler.xai_gateway import XAILLMGateway

def test_get_llm_gateway_ollama_default():
    with mock.patch.dict(os.environ, {}, clear=True):
        gateway = get_llm_gateway()
        assert isinstance(gateway, OllamaLLMGateway)
        assert gateway.model == "llama3"

def test_get_llm_gateway_ollama_explicit():
    gateway = get_llm_gateway("ollama:llama3.1")
    assert isinstance(gateway, OllamaLLMGateway)
    assert gateway.model == "llama3.1"

def test_get_llm_gateway_openai_explicit():
    with mock.patch("app.compiler.base_gateway.get_secrets_manager"):
        gateway = get_llm_gateway("openai:gpt-3.5-turbo")
        assert isinstance(gateway, OpenAILLMGateway)
        assert gateway.model == "gpt-3.5-turbo"

def test_get_llm_gateway_anthropic_explicit():
    with mock.patch("app.compiler.base_gateway.get_secrets_manager"):
        gateway = get_llm_gateway("anthropic:claude-3-5-sonnet")
        assert isinstance(gateway, AnthropicLLMGateway)
        assert gateway.model == "claude-3-5-sonnet"

def test_get_llm_gateway_google_explicit():
    with mock.patch("app.compiler.base_gateway.get_secrets_manager"):
        gateway = get_llm_gateway("google:gemini-1.5-pro")
        assert isinstance(gateway, GoogleLLMGateway)
        assert gateway.model == "gemini-1.5-pro"

def test_get_llm_gateway_xai_explicit():
    with mock.patch("app.compiler.base_gateway.get_secrets_manager"):
        gateway = get_llm_gateway("xai:grok-2")
        assert isinstance(gateway, XAILLMGateway)
        assert gateway.model == "grok-2"

def test_get_llm_gateway_unknown_provider():
    with pytest.raises(ValueError, match="Unknown LLM Provider designated: invalid_provider"):
        get_llm_gateway("invalid_provider")
