from typing import Any

from fastapi.testclient import TestClient

from app.api.auth import ResolvedCredential, require_query_credential
from app.api.router import get_compiler, get_executor
from app.execution.models import QueryResult
from app.main import app
from tests.conftest import TEST_ADMIN_CREDENTIAL_ID, TEST_QUERY_CREDENTIAL_ID

_FAKE_QUERY_CRED = ResolvedCredential(
    credential_id=TEST_QUERY_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="test_user",
    scope="query",
)

_FAKE_ADMIN_CRED = ResolvedCredential(
    credential_id=TEST_ADMIN_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="admin_user",
    scope="admin",
)


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
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED

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
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED

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


def test_create_credential_propagates_operational_error_as_5xx() -> None:
    """A non-integrity commit failure (e.g. OperationalError from a DB
    restart or transient network blip) must NOT be rewritten to HTTP 409.

    Code-review finding #8 (2026-04-07): bare `except Exception` was
    catching every commit failure and reporting it as "already exists",
    masking outages and confusing operators and clients. The fix narrows
    the except clause to IntegrityError only.

    create_credential is chosen for this regression test because
    conftest.py creates the tenant_credentials table and seeds an admin
    credential, so the handler path reaches session.commit() before any
    lookup can short-circuit it.
    """
    from unittest.mock import AsyncMock, patch

    from sqlalchemy.exc import OperationalError

    from app.api.auth import require_admin_credential

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED

    op_err = OperationalError(
        "INSERT INTO tenant_credentials ...",
        None,
        Exception("DB went away"),
    )

    try:
        # Patch only the commit — execute/add/refresh still work normally.
        # The SAME AsyncSession instance is used for auth lookups earlier
        # in the request, so we must target commit specifically rather
        # than patching the whole session.
        with patch(
            "sqlalchemy.ext.asyncio.AsyncSession.commit",
            new=AsyncMock(side_effect=op_err),
        ):
            # raise_server_exceptions=False so unhandled exceptions are
            # converted to 500 responses (default TestClient behavior is to
            # re-raise them, which would mask whether the handler narrowed
            # the except clause correctly).
            with TestClient(app, raise_server_exceptions=False) as client:
                response = client.post(
                    "/api/v1/auth/credentials",
                    json={
                        "tenant_id": "test_tenant",
                        "user_id": "new_user",
                        "scope": "query",
                        "description": "regression test",
                    },
                )

        # The critical assertion: OperationalError was NOT masked as 409.
        # FastAPI's default exception handling turns the unhandled exception
        # into a 500. If a future change wires up a dedicated handler that
        # returns 503 for OperationalError, that's also acceptable.
        assert response.status_code != 409, (
            "OperationalError was masked as 409 conflict;"
            f" response body: {response.text}"
        )
        assert response.status_code >= 500, (
            f"expected 5xx for commit failure, got {response.status_code}"
        )
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)


def test_api_execute_pipeline() -> None:
    """
    Test 3: Verify /execute correctly passes through Compilation and Execution.
    Asserts standard ExecuteResponse schema payload is returned.
    """
    spy_engine = SpyExecutionEngine()

    def override_executor() -> SpyExecutionEngine:
        return spy_engine

    app.dependency_overrides[get_executor] = override_executor
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED

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
