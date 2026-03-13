import pytest
from pydantic import ValidationError

from app.api.models import (
    ErrorResponse,
    QueryExecuteResponse,
    QueryGenerateResponse,
    QueryRequest,
)
from app.execution.models import ExecutionContext, QueryResult


def test_api_query_request_validation() -> None:
    """Test QueryRequest schema validations."""
    # Valid payload
    req = QueryRequest(intent="find users", schema_hints=["use active users"])
    assert req.intent == "find users"
    assert "use active users" in req.schema_hints

    # Missing required intent
    with pytest.raises(ValidationError):
        QueryRequest(schema_hints=[])  # type: ignore[call-arg]

def test_api_generate_response_validation() -> None:
    """Test QueryGenerateResponse constraints."""
    res = QueryGenerateResponse(
        query_id="123",
        sql="SELECT *",
        parameters={"id": 1},
        latency_ms=15.5
    )
    assert res.query_id == "123"

    # Frozen instances cannot be mutated
    with pytest.raises(ValidationError):
        res.latency_ms = 20.0

def test_api_execute_response_validation() -> None:
    """Test QueryExecuteResponse fields."""
    res = QueryExecuteResponse(
        query_id="456",
        sql="SELECT col1 FROM t",
        results=[{"col1": "val1"}],
        row_count=1,
        execution_latency_ms=42.0
    )
    assert res.row_count == 1
    assert res.results[0]["col1"] == "val1"

def test_api_error_response_validation() -> None:
    """Test stable ErrorResponse schema."""
    err = ErrorResponse(code=403, message="Denied")
    assert err.code == 403
    assert err.request_id is None

def test_execution_context_validation() -> None:
    """Verify context enforces multi-tenant constraints."""
    ctx = ExecutionContext(tenant_id="tenant_a", user_id="user_b")
    assert ctx.tenant_id == "tenant_a"
    assert ctx.statement_timeout_ms == 5000  # Default enforce

    # Missing required tenant tracking
    with pytest.raises(ValidationError):
        ExecutionContext(user_id="just_user")  # type: ignore[call-arg]

def test_execution_query_result_validation() -> None:
    """Verify strict execution result interface."""
    res = QueryResult(
        columns=["id", "name"],
        rows=[{"id": 1, "name": "Alice"}],
        metadata={"limit": True}
    )
    assert len(res.columns) == 2
    assert res.rows[0]["name"] == "Alice"
    assert res.metadata["limit"] is True
