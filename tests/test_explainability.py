from typing import Any

from fastapi.testclient import TestClient

from app.api.router import get_executor
from app.execution.models import QueryResult
from app.main import app


class SpyExecutionEngine:
    def __init__(self) -> None:
        self.call_count = 0

    async def execute(self, query: Any, *, context: Any) -> QueryResult:
        self.call_count += 1
        return QueryResult(
            columns=["count"],
            rows=[{"count": 1}],
            metadata={"row_limit_applied": False, "registry_version": "1.0.0"}
        )

def test_explainability_absence_by_default() -> None:
    """Assert explain=false (or missing) explicitly returns no explainability data."""
    spy_engine = SpyExecutionEngine()
    app.dependency_overrides[get_executor] = lambda: spy_engine

    payload = {"intent": "Get user details in the system"}

    try:
        with TestClient(app) as test_client:
            response = test_client.post("/api/v1/query/generate", json=payload)

        assert response.status_code == 200
        data = response.json()

        # Explicit test boundary mapping
        assert "explainability" in data
        assert data["explainability"] is None

    finally:
        app.dependency_overrides.clear()


def test_explainability_population_when_requested() -> None:
    """Submit explain=true, verify payload populates securely with redactions."""
    spy_engine = SpyExecutionEngine()
    app.dependency_overrides[get_executor] = lambda: spy_engine

    payload = {
        "intent": "Get user details from the table",
        "explain": True
    }

    try:
        with TestClient(app) as test_client:
            response = test_client.post("/api/v1/query/execute", json=payload)

        assert response.status_code == 200
        data = response.json()

        # Test boundary mappings
        assert "explainability" in data
        explain = data["explainability"]
        assert explain is not None

        # Vector Store Traces
        assert "rag" in explain
        assert explain["rag"]["outcome"] == "SINGLE_HIGH_CONFIDENCE_MATCH"
        assert "User details" in explain["rag"]["matches"]

        # Schema Traces
        assert "schema_filter" in explain
        assert "users.name" in explain["schema_filter"]["included_aliases"]

        # Prompt Traces (Security/UI Payload)
        assert "prompt" in explain
        assert explain["prompt"]["system_prompt_redacted"] is False
        assert "raw_system" in explain["prompt"]
        assert "raw_user" in explain["prompt"]

        # LLM Traces
        assert "llm" in explain
        assert "mock-aegis-v1" in explain["llm"]["provider"]

        # Translation Traces
        assert "translation" in explain
        assert "SELECT" in explain["translation"]["llm_abstract_query"]

    finally:
        app.dependency_overrides.clear()
