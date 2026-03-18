"""
Unit tests for app.compiler.provider_config:
  - parse_provider_id: format validation and normalisation
  - get_allowed_providers: env-var-driven allowlist resolution
  - assert_provider_allowed: allowlist enforcement
Router-level 400 behaviour for blocked / malformed provider_id values.
"""
import os
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from app.compiler.provider_config import (
    MalformedProviderIdError,
    ProviderNotAllowedError,
    assert_provider_allowed,
    get_allowed_providers,
    parse_provider_id,
)

# ─── parse_provider_id ────────────────────────────────────────────────────────

class TestParseProviderId:
    def test_bare_known_provider(self) -> None:
        assert parse_provider_id("openai") == "openai"

    def test_provider_with_model(self) -> None:
        assert parse_provider_id("openai:gpt-4o") == "openai:gpt-4o"

    def test_normalises_case_and_whitespace(self) -> None:
        assert parse_provider_id("  OpenAI:GPT-4o  ") == "openai:gpt-4o"

    def test_all_known_providers_accepted(self) -> None:
        for provider in ("ollama", "openai", "anthropic", "google", "xai"):
            assert parse_provider_id(provider) == provider

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="must not be empty"):
            parse_provider_id("")

    def test_rejects_whitespace_only(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="must not be empty"):
            parse_provider_id("   ")

    def test_rejects_none(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="must not be empty"):
            parse_provider_id(None)

    def test_rejects_trailing_colon(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="Malformed"):
            parse_provider_id("openai:")

    def test_rejects_leading_colon(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="Malformed"):
            parse_provider_id(":gpt-4o")

    def test_rejects_double_colon(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="Malformed"):
            parse_provider_id("openai::gpt-4o")

    def test_rejects_unknown_prefix(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="(?i)unknown"):
            parse_provider_id("bedrock:claude")

    def test_rejects_three_parts(self) -> None:
        with pytest.raises(MalformedProviderIdError, match="Malformed"):
            parse_provider_id("openai:gpt-4o:extra")


# ─── get_allowed_providers ────────────────────────────────────────────────────

class TestGetAllowedProviders:
    def test_wildcard_returns_none(self) -> None:
        with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "*"}):
            assert get_allowed_providers() is None

    def test_explicit_list_parsed(self) -> None:
        with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "openai,anthropic"}):
            allowed = get_allowed_providers()
        assert allowed == {"openai", "anthropic"}

    def test_explicit_list_normalised(self) -> None:
        env = {"ALLOWED_LLM_PROVIDERS": " OpenAI , Anthropic "}
        with mock.patch.dict(os.environ, env):
            allowed = get_allowed_providers()
        assert allowed == {"openai", "anthropic"}

    def test_unset_defaults_to_configured_llm_provider(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"LLM_PROVIDER": "anthropic:claude-3", "ALLOWED_LLM_PROVIDERS": ""},
        ):
            allowed = get_allowed_providers()
        assert allowed == {"anthropic"}

    def test_unset_with_bare_provider(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"LLM_PROVIDER": "ollama", "ALLOWED_LLM_PROVIDERS": ""},
        ):
            allowed = get_allowed_providers()
        assert allowed == {"ollama"}


# ─── assert_provider_allowed ──────────────────────────────────────────────────

class TestAssertProviderAllowed:
    def test_allows_when_wildcard(self) -> None:
        with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "*"}):
            assert_provider_allowed("openai:gpt-4o")  # must not raise

    def test_allows_matching_prefix(self) -> None:
        with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "openai,anthropic"}):
            assert_provider_allowed("openai:gpt-4o")  # must not raise

    def test_blocks_non_matching_prefix(self) -> None:
        with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "openai"}):
            with pytest.raises(ProviderNotAllowedError) as exc:
                assert_provider_allowed("anthropic:claude-3")
        assert exc.value.requested == "anthropic:claude-3"

    def test_blocks_when_default_only(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"LLM_PROVIDER": "ollama", "ALLOWED_LLM_PROVIDERS": ""},
        ):
            with pytest.raises(ProviderNotAllowedError):
                assert_provider_allowed("openai:gpt-4o")


# ─── Router-level 400 enforcement ─────────────────────────────────────────────

def _setup_router_overrides(never_calls_compile: bool = True) -> None:
    """Apply dependency overrides for router integration tests."""
    import uuid

    from app.api.auth import ResolvedCredential, require_query_credential
    from app.api.router import get_compiler
    from app.main import app
    from tests.conftest import TEST_QUERY_CREDENTIAL_ID

    fake_cred = ResolvedCredential(
        credential_id=TEST_QUERY_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="test_user",
        scope="query",
    )

    if never_calls_compile:
        class _Compiler:
            async def compile(self, *_: object, **__: object) -> None:
                raise AssertionError(
                    "compile() must not be reached for invalid provider_id"
                )
    else:
        class _Compiler:  # type: ignore[no-redef]
            async def compile(self, *_: object, **__: object) -> object:
                from app.compiler.models import ExecutableQuery
                return ExecutableQuery(
                    sql="SELECT 1",
                    parameters={},
                    query_id=str(uuid.uuid4()),
                    compilation_latency_ms=1.0,
                    registry_version="test",
                    safety_engine_version="test",
                    abstract_query_hash="aabb",
                )

    app.dependency_overrides[get_compiler] = lambda: _Compiler()
    app.dependency_overrides[require_query_credential] = lambda: fake_cred


class TestRouterProviderValidation:
    """
    Integration tests: the router must return HTTP 400 for blocked or malformed
    provider_id values before any compilation work begins.
    """

    def test_malformed_provider_id_returns_400(self) -> None:
        from app.main import app

        _setup_router_overrides()
        try:
            with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "*"}):
                with TestClient(app) as client:
                    resp = client.post(
                        "/api/v1/query/generate",
                        json={
                            "intent": "test",
                            "schema_hints": [],
                            "provider_id": "openai:",
                        },
                    )
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 400
        assert "malformed" in resp.json()["detail"].lower()

    def test_unknown_provider_prefix_returns_400(self) -> None:
        from app.main import app

        _setup_router_overrides()
        try:
            with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "*"}):
                with TestClient(app) as client:
                    resp = client.post(
                        "/api/v1/query/generate",
                        json={
                            "intent": "test",
                            "schema_hints": [],
                            "provider_id": "bedrock:claude",
                        },
                    )
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 400

    def test_allowlist_blocked_provider_returns_400(self) -> None:
        from app.main import app

        _setup_router_overrides()
        try:
            with mock.patch.dict(os.environ, {"ALLOWED_LLM_PROVIDERS": "ollama"}):
                with TestClient(app) as client:
                    resp = client.post(
                        "/api/v1/query/generate",
                        json={
                            "intent": "test",
                            "schema_hints": [],
                            "provider_id": "openai:gpt-4o",
                        },
                    )
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 400
        # Generic message — allowlist details must not be leaked to the client
        detail = resp.json()["detail"]
        assert "not permitted" in detail.lower()
        assert "openai" not in detail.lower()

    def test_no_provider_id_passes_validation(self) -> None:
        """Omitting provider_id entirely must not trigger validation errors."""
        from app.main import app

        _setup_router_overrides(never_calls_compile=False)
        try:
            with TestClient(app) as client:
                resp = client.post(
                    "/api/v1/query/generate",
                    json={"intent": "test", "schema_hints": []},
                )
        finally:
            app.dependency_overrides.clear()

        assert resp.status_code == 200
