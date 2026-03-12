"""
Tests for OpenAI, Anthropic, Google, and xAI LLM gateways.

Each gateway is tested for:
- Successful round-trip: returns raw JSON string (not pre-extracted SQL)
- Refusal payload passed through unchanged
- HTTP error raises LLMGenerationError
- Empty response raises LLMGenerationError
- Invalid JSON raises LLMGenerationError
- Missing API key raises LLMGenerationError
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.compiler.models import LLMResult, PromptEnvelope
from app.compiler.ollama import LLMGenerationError
from app.compiler.openai_gateway import OpenAILLMGateway
from app.compiler.anthropic_gateway import AnthropicLLMGateway
from app.compiler.google_gateway import GoogleLLMGateway
from app.compiler.xai_gateway import XAILLMGateway


@pytest.fixture
def envelope() -> PromptEnvelope:
    return PromptEnvelope(
        system_instruction="You are a SQL expert.",
        user_prompt="Count all users",
    )


def _ok_response(body: dict) -> AsyncMock:
    mock = AsyncMock(spec=httpx.Response)
    mock.status_code = 200
    mock.json.return_value = body
    return mock


def _error_response(status: int = 500) -> AsyncMock:
    mock = AsyncMock(spec=httpx.Response)
    mock.status_code = status
    mock.raise_for_status.side_effect = httpx.HTTPStatusError(
        "error", request=MagicMock(), response=mock
    )
    return mock


# ─── OpenAI ──────────────────────────────────────────────────────────────────

@patch("app.compiler.openai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_openai_success_returns_raw_json(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "sk-test"
    payload = json.dumps({"sql": "SELECT COUNT(*) FROM users"})
    mock_post.return_value = _ok_response({
        "choices": [{"message": {"content": payload}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    })
    result = await OpenAILLMGateway().generate(envelope)
    assert result.raw_text == payload
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 5


@patch("app.compiler.openai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_openai_refusal_payload_passed_through(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "sk-test"
    refusal = json.dumps({"refused": True, "reason": "destructive intent"})
    mock_post.return_value = _ok_response({
        "choices": [{"message": {"content": refusal}}],
        "usage": {},
    })
    result = await OpenAILLMGateway().generate(envelope)
    assert result.raw_text == refusal


@patch("app.compiler.openai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_openai_invalid_json_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "sk-test"
    mock_post.return_value = _ok_response({
        "choices": [{"message": {"content": "not valid json"}}],
        "usage": {},
    })
    with pytest.raises(LLMGenerationError, match="(?i)valid JSON"):
        await OpenAILLMGateway().generate(envelope)


@patch("app.compiler.openai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_openai_no_choices_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "sk-test"
    mock_post.return_value = _ok_response({"choices": [], "usage": {}})
    with pytest.raises(LLMGenerationError, match="(?i)no choices"):
        await OpenAILLMGateway().generate(envelope)


@patch("app.compiler.openai_gateway.get_secrets_manager")
@pytest.mark.asyncio
async def test_openai_missing_api_key_raises(mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = None
    with pytest.raises(LLMGenerationError, match="(?i)API key is missing"):
        await OpenAILLMGateway().generate(envelope)


@patch("app.compiler.openai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_openai_http_error_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "sk-test"
    mock_post.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    with pytest.raises(LLMGenerationError, match="(?i)HTTP Error"):
        await OpenAILLMGateway().generate(envelope)


# ─── Anthropic ────────────────────────────────────────────────────────────────

@patch("app.compiler.anthropic_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_anthropic_success_prepends_brace(mock_post, mock_secrets, envelope):
    """
    Anthropic uses assistant prefilling: the gateway sends {"role": "assistant",
    "content": "{"} and Anthropic returns the continuation.  The gateway must
    prepend "{" to reconstruct the full JSON object.
    """
    mock_secrets.return_value.get_api_key.return_value = "anthro-test"
    # Anthropic returns everything AFTER the prefilled "{"
    continuation = '"sql": "SELECT COUNT(*) FROM users"}'
    mock_post.return_value = _ok_response({
        "content": [{"text": continuation}],
        "usage": {"input_tokens": 8, "output_tokens": 4},
    })
    result = await AnthropicLLMGateway().generate(envelope)
    assert result.raw_text == "{" + continuation
    assert json.loads(result.raw_text)["sql"] == "SELECT COUNT(*) FROM users"
    assert result.prompt_tokens == 8
    assert result.completion_tokens == 4


@patch("app.compiler.anthropic_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_anthropic_refusal_passed_through(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "anthro-test"
    continuation = '"refused": true, "reason": "cannot drop tables"}'
    mock_post.return_value = _ok_response({
        "content": [{"text": continuation}],
        "usage": {},
    })
    result = await AnthropicLLMGateway().generate(envelope)
    parsed = json.loads(result.raw_text)
    assert parsed["refused"] is True


@patch("app.compiler.anthropic_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_anthropic_no_content_blocks_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "anthro-test"
    mock_post.return_value = _ok_response({"content": [], "usage": {}})
    with pytest.raises(LLMGenerationError, match="(?i)no content"):
        await AnthropicLLMGateway().generate(envelope)


@patch("app.compiler.anthropic_gateway.get_secrets_manager")
@pytest.mark.asyncio
async def test_anthropic_missing_api_key_raises(mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = None
    with pytest.raises(LLMGenerationError, match="(?i)API key is missing"):
        await AnthropicLLMGateway().generate(envelope)


@patch("app.compiler.anthropic_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_anthropic_http_error_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "anthro-test"
    mock_post.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    with pytest.raises(LLMGenerationError, match="(?i)HTTP Error"):
        await AnthropicLLMGateway().generate(envelope)


# ─── Google ───────────────────────────────────────────────────────────────────

@patch("app.compiler.google_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_google_success_returns_raw_json(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "goog-test"
    payload = json.dumps({"sql": "SELECT * FROM users"})
    mock_post.return_value = _ok_response({
        "candidates": [{"content": {"parts": [{"text": payload}]}}],
        "usageMetadata": {"promptTokenCount": 6, "candidatesTokenCount": 3},
    })
    result = await GoogleLLMGateway().generate(envelope)
    assert result.raw_text == payload
    assert result.prompt_tokens == 6
    assert result.completion_tokens == 3


@patch("app.compiler.google_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_google_refusal_passed_through(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "goog-test"
    refusal = json.dumps({"refused": True, "reason": "forbidden"})
    mock_post.return_value = _ok_response({
        "candidates": [{"content": {"parts": [{"text": refusal}]}}],
        "usageMetadata": {},
    })
    result = await GoogleLLMGateway().generate(envelope)
    assert json.loads(result.raw_text)["refused"] is True


@patch("app.compiler.google_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_google_no_candidates_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "goog-test"
    mock_post.return_value = _ok_response({"candidates": []})
    with pytest.raises(LLMGenerationError, match="(?i)no candidates"):
        await GoogleLLMGateway().generate(envelope)


@patch("app.compiler.google_gateway.get_secrets_manager")
@pytest.mark.asyncio
async def test_google_missing_api_key_raises(mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = None
    with pytest.raises(LLMGenerationError, match="(?i)API key is missing"):
        await GoogleLLMGateway().generate(envelope)


@patch("app.compiler.google_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_google_invalid_json_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "goog-test"
    mock_post.return_value = _ok_response({
        "candidates": [{"content": {"parts": [{"text": "not json"}]}}],
    })
    with pytest.raises(LLMGenerationError, match="(?i)valid JSON"):
        await GoogleLLMGateway().generate(envelope)


@patch("app.compiler.google_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_google_http_error_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "goog-test"
    mock_post.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    with pytest.raises(LLMGenerationError, match="(?i)HTTP Error"):
        await GoogleLLMGateway().generate(envelope)


# ─── xAI ──────────────────────────────────────────────────────────────────────

@patch("app.compiler.xai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_xai_success_returns_raw_json(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "xai-test"
    payload = json.dumps({"sql": "SELECT id FROM users"})
    mock_post.return_value = _ok_response({
        "choices": [{"message": {"content": payload}}],
        "usage": {"prompt_tokens": 7, "completion_tokens": 2},
    })
    result = await XAILLMGateway().generate(envelope)
    assert result.raw_text == payload
    assert result.prompt_tokens == 7


@patch("app.compiler.xai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_xai_refusal_passed_through(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "xai-test"
    refusal = json.dumps({"refused": True, "reason": "not allowed"})
    mock_post.return_value = _ok_response({
        "choices": [{"message": {"content": refusal}}],
        "usage": {},
    })
    result = await XAILLMGateway().generate(envelope)
    assert json.loads(result.raw_text)["refused"] is True


@patch("app.compiler.xai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_xai_no_choices_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "xai-test"
    mock_post.return_value = _ok_response({"choices": [], "usage": {}})
    with pytest.raises(LLMGenerationError, match="(?i)no choices"):
        await XAILLMGateway().generate(envelope)


@patch("app.compiler.xai_gateway.get_secrets_manager")
@pytest.mark.asyncio
async def test_xai_missing_api_key_raises(mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = None
    with pytest.raises(LLMGenerationError, match="(?i)API key is missing"):
        await XAILLMGateway().generate(envelope)


@patch("app.compiler.xai_gateway.get_secrets_manager")
@patch("httpx.AsyncClient.post")
@pytest.mark.asyncio
async def test_xai_http_error_raises(mock_post, mock_secrets, envelope):
    mock_secrets.return_value.get_api_key.return_value = "xai-test"
    mock_post.side_effect = httpx.HTTPStatusError(
        "500", request=MagicMock(), response=MagicMock()
    )
    with pytest.raises(LLMGenerationError, match="(?i)HTTP Error"):
        await XAILLMGateway().generate(envelope)
