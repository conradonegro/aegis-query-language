"""
Integration tests for chat history session lifecycle.
Validates that:
1. A session_id is created when absent.
2. Messages are appended atomically across provider changes.
3. The same session_id is honoured across subsequent requests.
"""
import uuid
from typing import Any

from fastapi.testclient import TestClient

from app.api.router import get_compiler
from app.main import app


class MockCompilerTurnA:
    """Returns a fixed SELECT for turn 1."""

    async def compile(
        self,
        intent: Any,
        schema: Any,
        hints: Any,
        explain: bool = False,
        chat_history: Any = None,
        provider_id: Any = None,
        **kwargs: Any,
    ) -> Any:
        from app.compiler.models import ExecutableQuery
        return ExecutableQuery(
            sql="SELECT * FROM users",
            parameters={},
            query_id=str(uuid.uuid4()),
            compilation_latency_ms=10.0,
            registry_version="test-1.0",
            safety_engine_version="test-1.0",
            abstract_query_hash="abc123",
        )


class MockCompilerTurnB:
    """Returns a filtered SELECT for turn 2, reflecting history."""

    async def compile(
        self,
        intent: Any,
        schema: Any,
        hints: Any,
        explain: bool = False,
        chat_history: Any = None,
        provider_id: Any = None,
        **kwargs: Any,
    ) -> Any:
        from app.compiler.models import ExecutableQuery
        # Verify that the history from turn 1 was injected
        assert chat_history is not None and len(chat_history) >= 2
        return ExecutableQuery(
            sql="SELECT * FROM users WHERE active = 1",
            parameters={},
            query_id=str(uuid.uuid4()),
            compilation_latency_ms=10.0,
            registry_version="test-1.0",
            safety_engine_version="test-1.0",
            abstract_query_hash="def456",
        )


def test_chat_session_created_on_first_request() -> None:
    """
    /query/generate must return a session_id even when none was sent.
    """
    app.dependency_overrides[get_compiler] = lambda: MockCompilerTurnA()
    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/query/generate",
                json={
                    "intent": "Show all users",
                    "schema_hints": [],
                    "provider_id": "ollama:llama3",
                },
            )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "session_id" in data
        # Must be a valid UUID string
        uuid.UUID(data["session_id"])
    finally:
        app.dependency_overrides.clear()


_PHYSICAL_SQL = "SELECT real_column FROM real_table"
_ABSTRACT_SQL = "SELECT col0001 FROM table0001"


class MockCompilerAbstractTurn1:
    """Returns an ExecutableQuery with both abstract and physical SQL set."""

    async def compile(
        self,
        intent: Any,
        schema: Any,
        hints: Any,
        explain: bool = False,
        chat_history: Any = None,
        provider_id: Any = None,
        **kwargs: Any,
    ) -> Any:
        from app.compiler.models import ExecutableQuery
        return ExecutableQuery(
            sql=_PHYSICAL_SQL,
            abstract_sql=_ABSTRACT_SQL,
            parameters={},
            query_id=str(uuid.uuid4()),
            compilation_latency_ms=10.0,
            registry_version="test-1.0",
            safety_engine_version="test-1.0",
            abstract_query_hash="aabbcc",
        )


class MockCompilerAbstractTurn2:
    """
    Asserts that the assistant message stored from turn 1 is the abstract SQL,
    not the physical SQL.
    """

    async def compile(
        self,
        intent: Any,
        schema: Any,
        hints: Any,
        explain: bool = False,
        chat_history: Any = None,
        provider_id: Any = None,
        **kwargs: Any,
    ) -> Any:
        from app.compiler.models import ExecutableQuery
        assert chat_history is not None
        assistant_messages = [m for m in chat_history if m.role == "assistant"]
        assert len(assistant_messages) >= 1, (
            "Expected at least one assistant message in history"
        )
        stored_content = assistant_messages[0].content
        # Must contain the obfuscated aliases
        assert "col0001" in stored_content, (
            f"Expected obfuscated alias in history, got: {stored_content!r}"
        )
        assert "table0001" in stored_content, (
            f"Expected obfuscated alias in history, got: {stored_content!r}"
        )
        # Must NOT contain the physical schema names
        assert "real_column" not in stored_content, (
            f"Physical column leaked into history: {stored_content!r}"
        )
        assert "real_table" not in stored_content, (
            f"Physical table leaked into history: {stored_content!r}"
        )
        return ExecutableQuery(
            sql="SELECT 1",
            parameters={},
            query_id=str(uuid.uuid4()),
            compilation_latency_ms=1.0,
            registry_version="test-1.0",
            safety_engine_version="test-1.0",
            abstract_query_hash="ddeeff",
        )


def test_chat_history_stores_abstract_sql_not_physical() -> None:
    """
    The assistant message persisted to chat history must use abstract_sql
    (obfuscated aliases) and must not contain physical schema names.
    This must hold when explain=False (the default), not just when explain=True.
    """
    app.dependency_overrides[get_compiler] = lambda: MockCompilerAbstractTurn1()

    with TestClient(app) as client:
        # Turn 1: no explain flag — default behaviour
        resp1 = client.post(
            "/api/v1/query/generate",
            json={"intent": "Show all users", "schema_hints": []},
        )
        assert resp1.status_code == 200, resp1.text
        session_id = resp1.json()["session_id"]

        # Turn 2: assert the history injected into compile() used abstract_sql
        app.dependency_overrides[get_compiler] = lambda: MockCompilerAbstractTurn2()
        resp2 = client.post(
            "/api/v1/query/generate",
            json={
                "intent": "follow up",
                "schema_hints": [],
                "session_id": session_id,
            },
        )
        assert resp2.status_code == 200, resp2.text

    app.dependency_overrides.clear()


def test_chat_session_preserved_across_provider_switch() -> None:
    """
    Sending the same session_id on a second request with a different provider
    should persist context and return the same session_id.
    """
    app.dependency_overrides[get_compiler] = lambda: MockCompilerTurnA()

    with TestClient(app) as client:
        # Turn 1: initial request
        resp1 = client.post(
            "/api/v1/query/generate",
            json={
                "intent": "Show all users",
                "schema_hints": [],
                "provider_id": "ollama:llama3",
            },
        )
        assert resp1.status_code == 200, resp1.text
        session_id = resp1.json()["session_id"]
        assert session_id

        # Turn 2: switch provider, same session
        app.dependency_overrides[get_compiler] = lambda: MockCompilerTurnB()
        resp2 = client.post(
            "/api/v1/query/generate",
            json={
                "intent": "Filter to active only",
                "schema_hints": [],
                "provider_id": "openai:gpt-4o",
                "session_id": session_id,
            },
        )
        assert resp2.status_code == 200, resp2.text
        data2 = resp2.json()
        # Session ID must be the same
        assert data2["session_id"] == session_id
        # SQL must reflect turn 2 mock output
        assert "active" in data2["sql"]

    app.dependency_overrides.clear()
