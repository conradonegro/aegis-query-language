import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.api.auth import ResolvedCredential, require_query_credential
from app.main import app
from tests.conftest import TEST_QUERY_CREDENTIAL_ID

_FAKE_QUERY_CRED = ResolvedCredential(
    credential_id=TEST_QUERY_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="test_user",
    scope="query",
)


@pytest.fixture(params=["mock", "ollama"])
def override_llm_provider(
    request: pytest.FixtureRequest,
) -> Generator[TestClient, None, None]:
    """
    Parametrized fixture that runs tests against both gateways.
    For local testing, ensures Ollama must be running if param is 'ollama'.
    """
    # Override the environment variable temporarily
    original = os.environ.get("LLM_PROVIDER")
    os.environ["LLM_PROVIDER"] = request.param
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED

    # We must force a reload of the app lifespan to pick up the env var change
    with TestClient(app) as client:
        yield client  # Inside context manager, lifespan has run

    app.dependency_overrides.pop(require_query_credential, None)
    if original is not None:
        os.environ["LLM_PROVIDER"] = original
    else:
        del os.environ["LLM_PROVIDER"]

def test_golden_execute_integration(override_llm_provider: TestClient) -> None:
    """
    Golden Path Integration Test
    Asserts:
      - Valid parameterization and mapping to SQLite
      - End-to-end execution returning standard JSON payload
      - Trace populates llm_abstract_query vs parameterized_sql correctly
      - Safety passes
    """
    client = override_llm_provider
    provider = os.environ["LLM_PROVIDER"]

    # We use Alice because she is seeded in both RAG and SQLite
    payload = {
        "intent": "Get Alice from the users table",
        "explain": True
    }

    if provider == "mock":
        app.state.compiler.llm_gateway.mock_response_sql = (
            "SELECT * FROM users WHERE name = 'Alice'"
        )

    response = client.post("/api/v1/query/execute", json=payload)

    # If Ollama is not actually running locally, skip gracefully instead of
    # failing CI
    if provider == "ollama" and response.status_code == 502:
        pytest.skip(
            f"Gateway {provider} unreachable. Ensure Ollama is running locally."
        )

    assert response.status_code == 200, f"Gateway {provider} failed: {response.text}"
    data = response.json()

    # 1. Execution correctness
    assert data["row_count"] == 1
    assert data["results"][0]["name"] == "Alice"

    # 2. Safety & Abstract Parameterization correctness
    explain = data["explainability"]
    assert explain is not None

    translation_trace = explain["translation"]

    # The LLM abstract query MUST have the literal 'Alice'
    assert "Alice" in translation_trace["llm_abstract_query"]
    assert "$p1" not in translation_trace["llm_abstract_query"]
    assert ":p1" not in translation_trace["llm_abstract_query"]

    # The parameterized physical SQL must NOT have 'Alice'
    assert "Alice" not in translation_trace["parameterized_sql"]
    assert ":p1" in translation_trace["parameterized_sql"]

    # The dictionary extracted mapping must equal Alice
    assert translation_trace["parameters"]["p1"] == "Alice"

    # 3. Provider tracking correctness
    if provider == "ollama":
        assert explain["llm"]["provider"] == "llama3"
        assert explain["llm"]["latency_ms"] > 0
    elif provider == "mock":
        assert "mock-aegis-v1" in explain["llm"]["provider"]
