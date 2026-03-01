import pytest
from fastapi.testclient import TestClient

from app.api.router import get_compiler
from app.main import app


def test_health_check() -> None:
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

def test_standard_exception_handler() -> None:
    """Verify that unhandled exceptions are caught and formatted as 500s."""
    class MockCrashCompiler:
        async def compile(self, *args, **kwargs):
            raise ValueError("Something unexpected exploded")
            
    def override_compiler():
        return MockCrashCompiler()
        
    app.dependency_overrides[get_compiler] = override_compiler
    
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/api/v1/query/generate", json={"intent": "test", "schema_hints": []})
            
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
        assert hasattr(app.state, "registry")
        assert hasattr(app.state, "executor")
        assert hasattr(app.state, "auditor")
        assert hasattr(app.state, "compiler")
