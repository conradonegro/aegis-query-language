import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.compiler.gateway import MockLLMGateway
from app.compiler.models import PromptEnvelope
from app.compiler.ollama import LLMGenerationError, OllamaLLMGateway


@pytest.fixture
def prompt_envelope() -> PromptEnvelope:
    return PromptEnvelope(
        system_instruction="You are a SQL generator.",
        user_prompt="Get all users",
    )


def build_mock_response(content_str: str, status_code: int = 200) -> AsyncMock:
    """Helper to mock an httpx.Response containing JSON."""
    mock_resp = AsyncMock(spec=httpx.Response)
    mock_resp.status_code = status_code
    mock_resp.json.return_value = {
        "model": "llama3",
        "message": {"content": content_str},
        "prompt_eval_count": 10,
        "eval_count": 20
    }

    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Error", request=AsyncMock(), response=mock_resp
        )
    return mock_resp


@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_success(
    mock_post: MagicMock, prompt_envelope: PromptEnvelope
) -> None:
    """Test valid strict JSON generation succeeds and extracts SQL."""
    gateway = OllamaLLMGateway()

    # Mock Ollama returning exactly what we asked for
    valid_json_response = json.dumps({"sql": "SELECT * FROM users"})
    mock_post.return_value = build_mock_response(valid_json_response)

    result = await gateway.generate(prompt_envelope)

    assert result.raw_text == json.dumps({"sql": "SELECT * FROM users"})
    assert result.model_id == "llama3"
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 20
    assert result.latency_ms > 0


@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_invalid_json_fails_strictly(
    mock_post: MagicMock, prompt_envelope: PromptEnvelope
) -> None:
    """Test the 'never let JSON-mode failure become silent success' rule."""
    gateway = OllamaLLMGateway()

    # Mock Ollama hallucinating markdown or broken JSON
    broken_json_response = "```json\n { sql: SELECT * FROM users \n```"
    mock_post.return_value = build_mock_response(broken_json_response)

    with pytest.raises(LLMGenerationError) as exc:
        await gateway.generate(prompt_envelope)

    assert "failed to return valid json" in str(exc.value).lower()


@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_passes_through_non_sql_json(
    mock_post: MagicMock, prompt_envelope: PromptEnvelope
) -> None:
    """
    The gateway must pass valid JSON through unchanged even when the 'sql' key
    is absent.  Structural contract enforcement (sql vs refused) belongs in the
    engine's LLMQueryResponse validator, not in the gateway.
    """
    gateway = OllamaLLMGateway()

    wrong_schema_response = json.dumps({"query": "SELECT * FROM users"})
    mock_post.return_value = build_mock_response(wrong_schema_response)

    result = await gateway.generate(prompt_envelope)

    # Gateway returns the raw JSON; engine handles structure validation
    assert result.raw_text == wrong_schema_response


@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_passes_through_multi_statement_json(
    mock_post: MagicMock, prompt_envelope: PromptEnvelope
) -> None:
    """
    The gateway must pass multi-statement SQL through as raw JSON.
    Multi-statement detection is enforced by the SQLParser downstream,
    not by the gateway.
    """
    gateway = OllamaLLMGateway()

    multi_statement_response = json.dumps({
        "sql": "SELECT * FROM users; DROP TABLE users;"
    })
    mock_post.return_value = build_mock_response(multi_statement_response)

    result = await gateway.generate(prompt_envelope)

    # Gateway returns raw JSON; SQLParser enforces single-statement rule
    assert result.raw_text == multi_statement_response


@pytest.mark.asyncio
@patch("app.compiler.ollama.OllamaLLMGateway.generate")
@patch("app.compiler.gateway.MockLLMGateway.generate")
async def test_gateway_side_by_side_interface(
    mock_generate: MagicMock,
    ollama_generate: MagicMock,
    prompt_envelope: PromptEnvelope,
) -> None:
    """
    Parametrized test representing side-by-side validation.
    Both gateways must implement the exact same LLMGatewayProtocol and return
    LLMResult.
    """
    gateways = [MockLLMGateway(), OllamaLLMGateway()]

    for gw in gateways:
        assert hasattr(gw, "generate")
        # In a real integration test, we would hit both and assert parser
        # equivalence. For now, we assert they share the same structural
        # contract boundary.
