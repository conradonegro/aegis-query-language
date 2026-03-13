from typing import Any

from fastapi.testclient import TestClient

from app.api.router import get_compiler, get_executor
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


def test_api_generate_boundary() -> None:
    """
    Test 1: Verify /generate strictly isolates DB interactions.
    Assert the execution engine is *never* called.
    """
    spy_engine = SpyExecutionEngine()

    # Override the get_executor dependency to safely bypass startup overrides
    def override_executor() -> SpyExecutionEngine:
        return spy_engine

    app.dependency_overrides[get_executor] = override_executor

    payload = {
        "intent": "Get Alice in the system",
        "schema_hints": []
    }

    try:
        with TestClient(app) as test_client:
            response = test_client.post("/api/v1/query/generate", json=payload)

        assert response.status_code == 200
        data = response.json()

        assert "sql" in data
        assert "query_id" in data

        # BOUNDARY ASSERTION: ExecutionEngine logic was entirely skipped.
        assert spy_engine.call_count == 0
    finally:
        app.dependency_overrides.clear()


def test_api_error_shape() -> None:
    """
    Test 2: Assert the Error API payload schema dynamically responds to failures
    with a stable JSON interface (code, message, request_id).
    """
    from app.compiler.safety import SafetyViolationError

    class MockCompilerRaise:
        async def compile(self, *args: Any, **kwargs: Any) -> None:
            raise SafetyViolationError("Mocked unsafe intent string detected.")

    def override_compiler() -> MockCompilerRaise:
        return MockCompilerRaise()

    app.dependency_overrides[get_compiler] = override_compiler

    try:
        payload = {"intent": "DROP TABLE users;", "schema_hints": []}
        with TestClient(app) as test_client:
            response = test_client.post("/api/v1/query/generate", json=payload)

        assert response.status_code == 403
        data = response.json()

        # Schema Shape Assertions
        assert "code" in data
        assert "message" in data
        assert "request_id" in data

        assert data["code"] == 403
        assert "Safety Violation:" in data["message"]
        assert data["request_id"] is None

    finally:
        app.dependency_overrides.clear()


def test_api_execute_pipeline() -> None:
    """
    Test 3: Verify /execute correctly passes through Compilation and Execution.
    Asserts standard ExecuteResponse schema payload is returned.
    """
    spy_engine = SpyExecutionEngine()

    def override_executor() -> SpyExecutionEngine:
        return spy_engine

    app.dependency_overrides[get_executor] = override_executor

    payload = {
        "intent": "Get Alice in the system",
        "schema_hints": []
    }

    try:
        with TestClient(app) as test_client:
            response = test_client.post("/api/v1/query/execute", json=payload)

        assert response.status_code == 200
        data = response.json()

        # Schema Shape Assertions for ExecuteResponse
        assert "query_id" in data
        assert "results" in data
        assert "row_count" in data
        assert "execution_latency_ms" in data

        # Verify the mock executor returned our mocked shape
        assert data["row_count"] == 1
        assert data["results"] == [{"count": 1}]
        assert "execution_latency_ms" in data

        # BOUNDARY ASSERTION: ExecutionEngine WAS explicitly called this time.
        assert spy_engine.call_count == 1
    finally:
        app.dependency_overrides.clear()
