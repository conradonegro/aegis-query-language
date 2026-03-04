"""
Integration tests for chat history session lifecycle.
Validates that:
1. A session_id is created when absent.
2. Messages are appended atomically across provider changes.
3. The same session_id is honoured across subsequent requests.
"""
import uuid
from unittest import mock
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.api.router import get_compiler


class MockCompilerTurnA:
    """Returns a fixed SELECT for turn 1."""

    async def compile(self, intent, schema, hints, explain=False, chat_history=None, provider_id=None):
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

    async def compile(self, intent, schema, hints, explain=False, chat_history=None, provider_id=None):
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


def test_chat_session_created_on_first_request():
    """
    /query/generate must return a session_id even when none was sent.
    """
    app.dependency_overrides[get_compiler] = lambda: MockCompilerTurnA()
    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/query/generate",
                json={"intent": "Show all users", "schema_hints": [], "provider_id": "ollama:llama3"},
            )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert "session_id" in data
        # Must be a valid UUID string
        uuid.UUID(data["session_id"])
    finally:
        app.dependency_overrides.clear()


def test_chat_session_preserved_across_provider_switch():
    """
    Sending the same session_id on a second request with a different provider
    should persist context and return the same session_id.
    """
    app.dependency_overrides[get_compiler] = lambda: MockCompilerTurnA()

    with TestClient(app) as client:
        # Turn 1: initial request
        resp1 = client.post(
            "/api/v1/query/generate",
            json={"intent": "Show all users", "schema_hints": [], "provider_id": "ollama:llama3"},
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
