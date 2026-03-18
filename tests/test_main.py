from typing import Any

from fastapi.testclient import TestClient

from app.api.auth import ResolvedCredential, require_query_credential
from app.api.router import get_compiler
from app.compiler.engine import RAGUncertaintyError
from app.compiler.ollama import LLMGenerationError
from app.compiler.translator import TranslationError
from app.main import app
from tests.conftest import TEST_QUERY_CREDENTIAL_ID

_FAKE_QUERY_CRED = ResolvedCredential(
    credential_id=TEST_QUERY_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="test_user",
    scope="query",
)


def test_health_check() -> None:
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

def test_standard_exception_handler() -> None:
    """Verify that unhandled exceptions are caught and formatted as 500s."""
    class MockCrashCompiler:
        async def compile(self, *args: Any, **kwargs: Any) -> None:
            raise ValueError("Something unexpected exploded")

    def override_compiler() -> MockCrashCompiler:
        return MockCrashCompiler()

    app.dependency_overrides[get_compiler] = override_compiler
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/api/v1/query/generate",
                json={"intent": "test", "schema_hints": []},
            )

        assert response.status_code == 500
        data = response.json()
        assert data["code"] == 500
        assert data["message"] == "Internal Server Error"

    finally:
        app.dependency_overrides.clear()

def test_lifespan_initialization() -> None:
    """Verify that the lifespan context injects the required application state."""
    with TestClient(app):
        # The lifespan block should have set these on app.state
        assert hasattr(app.state, "registries")
        assert hasattr(app.state, "vector_stores")
        assert hasattr(app.state, "executor")
        assert hasattr(app.state, "auditor")
        assert hasattr(app.state, "compiler")


def test_translation_error_returns_400() -> None:
    """TranslationError (hallucinated JOIN, etc.) must map to HTTP 400."""
    class TranslationCrashCompiler:
        async def compile(self, *args: Any, **kwargs: Any) -> None:
            raise TranslationError(
                "JOIN condition does not match any declared relationship"
            )

    app.dependency_overrides[get_compiler] = lambda: TranslationCrashCompiler()
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/query/generate",
                json={"intent": "test", "schema_hints": []},
            )
        assert response.status_code == 400
        data = response.json()
        assert data["code"] == 400
    finally:
        app.dependency_overrides.clear()


def test_llm_generation_error_returns_502() -> None:
    """LLMGenerationError (provider unavailable, bad JSON) must map to HTTP 502."""
    class LLMCrashCompiler:
        async def compile(self, *args: Any, **kwargs: Any) -> None:
            raise LLMGenerationError("OpenAI returned no choices.", raw_response="")

    app.dependency_overrides[get_compiler] = lambda: LLMCrashCompiler()
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/query/generate",
                json={"intent": "test", "schema_hints": []},
            )
        assert response.status_code == 502
        data = response.json()
        assert data["code"] == 502
    finally:
        app.dependency_overrides.clear()


def test_rag_uncertainty_error_returns_400() -> None:
    """RAGUncertaintyError (ambiguous match with strict mode) must map to HTTP 400."""
    class RAGCrashCompiler:
        async def compile(self, *args: Any, **kwargs: Any) -> None:
            raise RAGUncertaintyError("Ambiguous RAG match; cannot proceed")

    app.dependency_overrides[get_compiler] = lambda: RAGCrashCompiler()
    app.dependency_overrides[require_query_credential] = lambda: _FAKE_QUERY_CRED
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/query/generate",
                json={"intent": "test", "schema_hints": []},
            )
        assert response.status_code == 400
        data = response.json()
        assert data["code"] == 400
    finally:
        app.dependency_overrides.clear()
