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


def test_compile_pending_review_version_does_not_mutate_runtime_state() -> None:
    """Compiling a pending_review version must not touch app.state.registries
    or app.state.loaded_artifact_hashes.

    Code-review finding #2 (2026-04-07): the router blindly hot-reloaded after
    every successful compile, leaving the worker with mismatched schema/RAG/
    hash state when the compiled version was not yet active.

    To exercise the gate without standing up the full metadata schema in
    SQLite, we patch _run_strategy_refresh + MetadataCompiler.compile_version
    so the function reaches the runtime mutation block. The test then asserts
    that for a pending_review version, that block leaves runtime state alone.
    """
    import uuid as uuid_mod
    from datetime import UTC
    from datetime import datetime as _dt
    from unittest.mock import AsyncMock, MagicMock, patch

    from sqlalchemy import create_engine, text

    from app.api.auth import require_admin_credential

    sqlite_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

    pending_vid = uuid_mod.uuid4()

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO metadata_versions"
                " (version_id, tenant_id, status, created_by)"
                " VALUES (:vid, 'test_tenant', 'pending_review', 'test')"
            ),
            {"vid": pending_vid.hex},
        )

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED

    # Build a stand-in artifact the patched compile_version returns. The
    # router code reads .artifact_id, .version_id, .artifact_hash,
    # .artifact_blob, .compiled_at — provide them all on a MagicMock.
    fake_artifact = MagicMock()
    fake_artifact.artifact_id = uuid_mod.uuid4()
    fake_artifact.version_id = pending_vid
    fake_artifact.artifact_hash = "deadbeef" * 8
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.compiled_at = _dt.now(UTC)

    rebuild_mock = AsyncMock()
    publish_reload_mock = AsyncMock()

    try:
        with (
            patch(
                "app.api.router._run_strategy_refresh",
                new=AsyncMock(return_value=0),
            ),
            patch(
                "app.api.compiler.MetadataCompiler.compile_version",
                new=AsyncMock(return_value=fake_artifact),
            ),
            patch(
                "app.api.router._rebuild_rag_index_for_tenant",
                new=rebuild_mock,
            ),
            patch("app.api.router.publish_reload", new=publish_reload_mock),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            # Snapshot runtime state BEFORE the compile request
            registries_before = dict(app.state.registries)
            hashes_before = dict(app.state.loaded_artifact_hashes)

            response = client.post(
                f"/api/v1/metadata/compile/{pending_vid}",
                params={"wait_for_index": "true"},
            )

            # Snapshot runtime state AFTER the compile request
            registries_after = dict(app.state.registries)
            hashes_after = dict(app.state.loaded_artifact_hashes)

        # The compile must SUCCEED — preview compiles return 200. Any other
        # status (404/422/500) means the endpoint short-circuited before
        # reaching the gate, so the runtime-state assertions below would
        # pass for the wrong reason.
        assert response.status_code == 200, (
            f"preview compile expected 200, got {response.status_code}:"
            f" {response.text}"
        )

        # The critical assertion: runtime state is UNCHANGED for the
        # requesting tenant when compiling a pending_review version.
        assert registries_after.get("test_tenant") is registries_before.get(
            "test_tenant"
        ), "Compiling pending_review should not swap app.state.registries"
        assert hashes_after.get("test_tenant") == hashes_before.get(
            "test_tenant"
        ), (
            "Compiling pending_review should not advance"
            " loaded_artifact_hashes"
            f" (before={hashes_before.get('test_tenant')!r},"
            f" after={hashes_after.get('test_tenant')!r})"
        )

        # And the gated side effects must NOT have fired for a
        # pending_review preview compile.
        rebuild_mock.assert_not_awaited()
        publish_reload_mock.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


def test_compile_skips_hot_reload_if_version_archived_during_compile() -> None:
    """The post-compile hot-reload gate must use a fresh DB status read.

    Validation review (2026-04-08): compile_metadata_version loaded the
    version once, then decided whether to hot-reload from the same ORM
    instance after compile_version returned. If another admin archived the
    version during the compile, the identity-mapped object stayed "active"
    and the worker still published reloads / rebuilt runtime state for an
    archived version.
    """
    import uuid as uuid_mod
    from datetime import UTC
    from datetime import datetime as _dt
    from unittest.mock import AsyncMock, MagicMock, patch

    from sqlalchemy import create_engine, text

    from app.api.auth import require_admin_credential

    sqlite_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

    active_vid = uuid_mod.uuid4()

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO metadata_versions"
                " (version_id, tenant_id, status, created_by)"
                " VALUES (:vid, 'test_tenant', 'active', 'test')"
            ),
            {"vid": active_vid.hex},
        )

    fake_artifact = MagicMock()
    fake_artifact.artifact_id = uuid_mod.uuid4()
    fake_artifact.version_id = active_vid
    fake_artifact.artifact_hash = "bead" * 16
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.compiled_at = _dt.now(UTC)

    async def compile_then_archive(**_: Any) -> Any:
        async with app.state.registry_admin_session_factory() as other_session:
            await other_session.execute(
                text(
                    "UPDATE metadata_versions"
                    " SET status = 'archived'"
                    " WHERE version_id = :vid"
                ),
                {"vid": active_vid.hex},
            )
            await other_session.commit()
        return fake_artifact

    publish_reload_mock = AsyncMock()
    rebuild_mock = AsyncMock()
    load_schema_mock = AsyncMock(return_value=MagicMock())

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED

    try:
        with (
            patch(
                "app.api.router._run_strategy_refresh",
                new=AsyncMock(return_value=0),
            ),
            patch(
                "app.api.compiler.MetadataCompiler.compile_version",
                new=AsyncMock(side_effect=compile_then_archive),
            ),
            patch("app.api.router.publish_reload", new=publish_reload_mock),
            patch(
                "app.api.router._rebuild_rag_index_for_tenant",
                new=rebuild_mock,
            ),
            patch(
                "app.steward.loader.RegistryLoader.load_active_schema",
                new=load_schema_mock,
            ),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            registries_before = dict(app.state.registries)
            hashes_before = dict(app.state.loaded_artifact_hashes)

            response = client.post(
                f"/api/v1/metadata/compile/{active_vid}",
                params={"wait_for_index": "true"},
            )

            registries_after = dict(app.state.registries)
            hashes_after = dict(app.state.loaded_artifact_hashes)

        assert response.status_code == 200, response.text
        assert registries_after == registries_before, (
            "An archived version should not trigger schema hot-reload after"
            " compile completion."
        )
        assert hashes_after == hashes_before, (
            "An archived version should not advance loaded_artifact_hashes."
        )
        publish_reload_mock.assert_not_awaited()
        rebuild_mock.assert_not_awaited()
        load_schema_mock.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()


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
