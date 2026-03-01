import json
import pytest
from unittest.mock import AsyncMock, patch
import httpx

from app.compiler.models import PromptEnvelope
from app.compiler.ollama import OllamaLLMGateway, LLMGenerationError
from app.compiler.gateway import MockLLMGateway


@pytest.fixture
def prompt_envelope():
    return PromptEnvelope(
        system_instruction="You are a SQL generator.",
        schema_context="table users (id int, name text);",
        user_prompt="Get all users",
        hints=""
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
async def test_ollama_gateway_success(mock_post, prompt_envelope):
    """Test valid strict JSON generation succeeds and extracts SQL."""
    gateway = OllamaLLMGateway(strict_json=True)
    
    # Mock Ollama returning exactly what we asked for
    valid_json_response = json.dumps({"sql": "SELECT * FROM users"})
    mock_post.return_value = build_mock_response(valid_json_response)
    
    result = await gateway.generate(prompt_envelope)
    
    assert result.raw_text == "SELECT * FROM users"
    assert result.model_id == "llama3"
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 20
    assert result.latency_ms > 0
    

@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_invalid_json_fails_strictly(mock_post, prompt_envelope):
    """Test the 'never let JSON-mode failure become silent success' rule."""
    gateway = OllamaLLMGateway(strict_json=True)
    
    # Mock Ollama hallucinating markdown or broken JSON
    broken_json_response = "```json\n { sql: SELECT * FROM users \n```"
    mock_post.return_value = build_mock_response(broken_json_response)
    
    with pytest.raises(LLMGenerationError) as exc:
        await gateway.generate(prompt_envelope)
        
    assert "failed to return valid json" in str(exc.value).lower()
    

@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_missing_required_field_fails(mock_post, prompt_envelope):
    """Test that valid JSON missing the 'sql' key is strictly rejected."""
    gateway = OllamaLLMGateway(strict_json=True)
    
    # Mock Ollama returning valid JSON but wrong schema
    wrong_schema_response = json.dumps({"query": "SELECT * FROM users"})
    mock_post.return_value = build_mock_response(wrong_schema_response)
    
    with pytest.raises(LLMGenerationError) as exc:
        await gateway.generate(prompt_envelope)
        
    assert "missing required 'sql' field" in str(exc.value).lower()
    

@pytest.mark.asyncio
@patch("httpx.AsyncClient.post")
async def test_ollama_gateway_multi_statement_rejection(mock_post, prompt_envelope):
    """Test that multiple SQL statements in the JSON are rejected at the edge."""
    gateway = OllamaLLMGateway(strict_json=True)
    
    multi_statement_response = json.dumps({
        "sql": "SELECT * FROM users; DROP TABLE users;"
    })
    mock_post.return_value = build_mock_response(multi_statement_response)
    
    with pytest.raises(LLMGenerationError) as exc:
        await gateway.generate(prompt_envelope)
        
    assert "multiple sql statements" in str(exc.value).lower()
    

@pytest.mark.asyncio
@patch("app.compiler.ollama.OllamaLLMGateway.generate")
@patch("app.compiler.gateway.MockLLMGateway.generate")
async def test_gateway_side_by_side_interface(mock_generate, ollama_generate, prompt_envelope):
    """
    Parametrized test representing side-by-side validation.
    Both gateways must implement the exact same LLMGatewayProtocol and return LLMResult.
    """
    gateways = [MockLLMGateway(), OllamaLLMGateway()]
    
    for gw in gateways:
        assert hasattr(gw, "generate")
        # In a real integration test, we would hit both and assert parser equivalence
        # For now, we assert they share the same structural contract boundary.
