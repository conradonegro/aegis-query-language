# Code Review Followups Implementation Plan

> **SUPERSEDED — historical plan, not open work.**
> Shipped and verified: the `_resolve_session` user_id IDOR guard and the `pending_review` compile test are both in `main` and passing.
> Its unchecked `- [ ]` boxes are stale TDD-process markers, **not** a backlog.
> Do not re-implement from this document.


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the seven non-BIRD findings from the 2026-04-07 code review (`docs/reviews/20260407_code_review.md`) as targeted, independently reviewable fixes. Finding #7 is intentionally excluded — it lives in the BIRD plan because it touches the same field family that Phase 2 of that plan extends.

**Architecture:** Four phases ordered by risk and dependency, not by reviewer numbering. Phase 1 lands three small high-confidence fixes for momentum. Phase 2 fixes the hot-reload runtime correctness pair. Phase 3 fixes the session-store schema staleness. Phase 4 fixes the WORM audit fork with a schema-level partial unique index (added to the existing `0001_initial_schema.py` baseline) plus an application-side retry. Each task is TDD-style with one failing test, the minimal fix, verification, and commit.

**Tech Stack:** Python 3.12 (strict mypy, ruff), FastAPI, SQLAlchemy 2.x async, asyncpg, Alembic, Redis, pytest, uv

**Out of scope (firm):**
- Code-review finding #7 — tracked in `docs/superpowers/plans/2026-04-07-bird-benchmark-phase-2.md` Task 2.7.
- Any BIRD benchmark accuracy work — separate plan.
- Architectural rewrites of the audit chain or session store beyond what's needed to fix the cited bugs. Any "while we're here" temptation should be deferred to its own plan.

**Mapping to reviewer findings:**

| Plan task | Reviewer finding | Severity |
|---|---|---|
| Task 1.1 | #5 — Cross-user session sharing | Medium |
| Task 1.2 | #8 — Commit failures masked as 409 | Medium |
| Task 1.3 | #6 — SQLAlchemy engines leaked | Medium |
| Task 2.1 | #2 — `pending_review` compile mutates runtime | High |
| Task 2.2 | #3 — RAG rebuild failure marked as success | High |
| Task 3.1 | #1 — Stale schema reused in follow-up | High |
| Task 4.1 + 4.2 | #4 — WORM audit chain forks | High |

---

## File map

| File | Phase | Action |
|---|---|---|
| `app/api/router.py` | 1, 2 | Modify — session user_id check, error mapping, `pending_review` gating |
| `app/main.py` | 1 | Modify — keep engine refs, dispose on shutdown |
| `app/reload.py` | 2 | Modify — defer `loaded_artifact_hashes` update until RAG succeeds |
| `app/compiler/models.py` | 3 | Modify — add `registry_version` to `SessionQueryContext` |
| `app/compiler/engine.py` | 3 | Modify — record + check `registry_version` in follow-up reuse |
| `backend_migrations/versions/0001_initial_schema.py` | 4 | Modify — add partial unique index on `previous_hash` directly to the consolidated baseline (no new migration file) |
| `app/audit/append.py` | 4 | Create — collision classifiers (`is_audit_chain_collision`, `is_activation_collision`) |
| `app/api/compiler.py` | 2, 4 | Modify — gate approval-field stamping on active status + scoped audit-chain retry |
| `tests/test_session_store.py` | 1, 3 | Modify — cross-user reject + stale-schema reject |
| `tests/test_api.py` | 1, 2 | Modify — error mapping + `pending_review` runtime gating |
| `tests/test_main_lifespan.py` | 1 | Create — engine disposal counter |
| `tests/test_reload.py` | 2 | Create — RAG failure does not advance hash + serialization via lock |
| `tests/test_audit_append.py` | 4 | Create — classifier unit tests (asyncpg + sqlite paths) |
| `tests/test_audit_chain.py` | 4 | Create — integration test driving real `MetadataCompiler.compile_version` retry path |

---

# Phase 1 — Quick wins

Three small, high-confidence fixes that land easily and build review momentum. None of them depend on each other; they can be done in any order, but I list the simplest first.

## Task 1.1: Session user_id check (finding #5)

**Why this exists:** `_resolve_session()` at `app/api/router.py:169` looks up `ChatSession` by `session_id` and `tenant_id` only. `ChatSession.user_id` is stored (`app/api/meta_models.py:388`) and passed in to `_resolve_session` (line 173) but never used in the SELECT — only when *creating* a new session at line 217. Any user with a valid key for the same tenant who knows another user's `session_id` can hijack that history and continue the conversation.

The fix is a two-line WHERE clause addition. The risk is essentially zero.

**Files:**
- Modify: `app/api/router.py:191-195`
- Modify: `tests/test_session_store.py` (or the closest existing session-resolution test — find with `grep -rn "_resolve_session\|ChatSession" tests/`)

- [ ] **Step 1: Find the closest existing test for `_resolve_session`**

Run:
```bash
grep -rn "_resolve_session\|ChatSession" tests/ | head -20
```

If a test file already exercises chat session resolution, use it. Otherwise the new test goes into `tests/test_session_store.py` (which exists and is the closest topical match).

- [ ] **Step 2: Write the failing test**

Add to `tests/test_session_store.py`:

```python
def test_resolve_session_rejects_cross_user_access_within_same_tenant() -> None:
    """A session created by user A must NOT be loadable by user B, even when
    both users belong to the same tenant.

    Code-review finding #5 (2026-04-07): _resolve_session previously scoped
    by tenant_id only, allowing horizontal privilege escalation between
    users of the same tenant.
    """
    import uuid as uuid_mod

    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine, text

    from app.api.auth import ResolvedCredential, require_query_credential
    from app.main import app
    from tests.conftest import TEST_QUERY_CREDENTIAL_ID

    sqlite_url = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False})

    user_a_session = uuid_mod.uuid4()

    # Pre-seed a chat session owned by user_a in test_tenant
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO chat_sessions"
                " (session_id, tenant_id, user_id)"
                " VALUES (:sid, 'test_tenant', 'user_a')"
            ),
            {"sid": str(user_a_session)},
        )
        conn.execute(
            text(
                "INSERT INTO chat_messages"
                " (message_id, session_id, sequence_number, role, content)"
                " VALUES (:mid, :sid, 1, 'user', 'A confidential prompt')"
            ),
            {"mid": str(uuid_mod.uuid4()), "sid": str(user_a_session)},
        )

    # Override credential to be a *different* user in the same tenant
    user_b_cred = ResolvedCredential(
        credential_id=TEST_QUERY_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="user_b",  # NOTE: not user_a
        scope="query",
    )
    app.dependency_overrides[require_query_credential] = lambda: user_b_cred

    try:
        with TestClient(app) as client:
            # User B presents user A's session_id in a generate request.
            response = client.post(
                "/api/v1/query/generate",
                json={
                    "natural_language_query": "follow-up",
                    "session_id": str(user_a_session),
                },
            )
        # The endpoint should still respond (it'll create a fresh session for
        # user_b), but user A's history must NOT have been loaded into the
        # prompt or returned in the response.
        assert response.status_code in (200, 400, 422)
        body = response.json()
        # The response must NOT echo user_a's confidential prompt — verify
        # that nothing in the response body contains the planted text.
        assert "A confidential prompt" not in str(body)

        # Stronger: user_a's session row must still own its messages and
        # user_b's request must have been bound to a NEW session row.
        with engine.connect() as conn:
            count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM chat_sessions"
                    " WHERE session_id = :sid AND user_id = 'user_a'"
                ),
                {"sid": str(user_a_session)},
            ).scalar()
            assert count == 1
    finally:
        app.dependency_overrides.pop(require_query_credential, None)
        engine.dispose()
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `uv run pytest tests/test_session_store.py::test_resolve_session_rejects_cross_user_access_within_same_tenant -v`
Expected: FAIL — the response body contains "A confidential prompt" because `_resolve_session` loaded user_a's messages.

If the test fails for a *different* reason (e.g. the endpoint URL is wrong, or `query/generate` requires a body field this test omits), fix the test. Inspect `app/api/router.py` for the `query/generate` route signature and adjust.

- [ ] **Step 4: Implement the fix**

In `app/api/router.py`, find the SELECT at line 191:

```python
            res = await session.execute(
                select(ChatSession).where(
                    ChatSession.session_id == session_uuid,
                    ChatSession.tenant_id == tenant_id,
                )
            )
```

Replace with:

```python
            res = await session.execute(
                select(ChatSession).where(
                    ChatSession.session_id == session_uuid,
                    ChatSession.tenant_id == tenant_id,
                    ChatSession.user_id == user_id,
                )
            )
```

The `user_id` parameter is already passed in to `_resolve_session()` at line 173 — no signature changes needed.

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_session_store.py::test_resolve_session_rejects_cross_user_access_within_same_tenant -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green. If any existing test fails because it implicitly relied on cross-user session loading, that test was wrong — fix it to use the correct user_id.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/router.py tests/test_session_store.py
uv run mypy app/api/router.py tests/test_session_store.py
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/api/router.py tests/test_session_store.py
git commit -m "$(cat <<'EOF'
fix(api): scope chat session lookup by user_id (review finding #5)

_resolve_session previously matched ChatSession rows by session_id and
tenant_id alone, allowing any user with a valid key for a tenant to
hijack another user's chat history if they knew the session UUID. The
ChatSession table already stores user_id, and the parameter was passed
in to _resolve_session — it just wasn't used in the WHERE clause.

This adds ChatSession.user_id == user_id to the lookup. Cross-user
access now silently falls through to creating a fresh session for the
requesting user, leaving the original owner's history untouched.

Test: a planted confidential prompt for user_a does not appear in
user_b's response when user_b presents user_a's session_id.

EOF
)"
```

## Task 1.2: Commit error mapping (finding #8)

**Why this exists:** Two endpoints — `create_column_value` at `app/api/router.py:996-1003` and `create_credential` at `app/api/router.py:1664-1671` — wrap `session.commit()` in a bare `except Exception` and rewrite every failure to HTTP 409 ("already exists"). Database restart, network blip, role permission issue — all reported as duplicate-key conflicts. Operators lose the real signal and clients retry the wrong way.

The fix is to catch `sqlalchemy.exc.IntegrityError` specifically and let other exceptions propagate through FastAPI's default exception handling.

**Files:**
- Modify: `app/api/router.py:996-1003` (`create_column_value`)
- Modify: `app/api/router.py:1664-1671` (`create_credential`)
- Modify: `tests/test_api.py` (or the closest existing test for these endpoints — find with `grep -rn "create_column_value\|create_credential\|column_id.*values" tests/`)

- [ ] **Step 1: Write the failing test targeting `create_credential`**

**Why target `create_credential` and not `create_column_value`:** `conftest.py` seeds the `tenant_credentials` table (line 115) and an admin credential (lines 129-149), so the `create_credential` path reaches `session.commit()` cleanly. `create_column_value` would first need a `metadata_columns` row to exist, which conftest does not provide — the handler short-circuits with 404 before touching `commit()` and the bug never manifests. Both endpoints have the same bug and receive the same one-line fix in Steps 3-4, so validating either one proves the pattern.

Add to `tests/test_api.py` (or wherever credential-endpoint tests live — find with `grep -rn "create_credential\|/auth/credentials" tests/`):

```python
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

    from fastapi.testclient import TestClient
    from sqlalchemy.exc import OperationalError

    from app.api.auth import ResolvedCredential, require_admin_credential
    from app.main import app
    from tests.conftest import TEST_ADMIN_CREDENTIAL_ID

    admin_cred = ResolvedCredential(
        credential_id=TEST_ADMIN_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="admin_user",
        scope="admin",
    )
    app.dependency_overrides[require_admin_credential] = lambda: admin_cred

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
            with TestClient(app) as client:
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
            f"OperationalError was masked as 409 conflict; response body: {response.text}"
        )
        assert response.status_code >= 500, (
            f"expected 5xx for commit failure, got {response.status_code}"
        )
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
```

**Note on the request payload:** the field names (`tenant_id`, `user_id`, `scope`, `description`) match what `create_credential` expects based on the router code at lines 1655-1663. Before running the test, sanity-check the endpoint's Pydantic request model (search for the `CredentialCreateRequest` class or similar in `app/api/`) and align the payload if the field names differ.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_api.py::test_create_credential_propagates_operational_error_as_5xx -v`
Expected: FAIL — `create_credential` reaches `session.commit()`, the patched commit raises `OperationalError`, the bare `except Exception` rewrites it to HTTP 409, and the assertion `response.status_code != 409` fails.

- [ ] **Step 3: Implement the fix in `create_column_value`**

In `app/api/router.py`, find lines 996-1003:

```python
    session.add(val)
    try:
        await session.commit()
        await session.refresh(val)
    except Exception as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="Value already exists for this column."
        ) from exc
```

Replace with:

```python
    session.add(val)
    try:
        await session.commit()
        await session.refresh(val)
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="Value already exists for this column."
        ) from exc
```

Add the import to the top of `app/api/router.py` if not already present:

```python
from sqlalchemy.exc import IntegrityError
```

(Verify with `grep -n "from sqlalchemy" app/api/router.py`. If `IntegrityError` is already imported elsewhere in the file, skip this.)

- [ ] **Step 4: Implement the fix in `create_credential`**

In `app/api/router.py`, find lines 1664-1671:

```python
    session.add(new_cred)
    try:
        await session.commit()
        await session.refresh(new_cred)
    except Exception as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="A credential with this key hash already exists."
        ) from exc
```

Replace with:

```python
    session.add(new_cred)
    try:
        await session.commit()
        await session.refresh(new_cred)
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status_code=409, detail="A credential with this key hash already exists."
        ) from exc
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_api.py::test_create_credential_propagates_operational_error_as_5xx -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green. If any existing test asserted that a non-integrity commit failure produces 409, that test was codifying the bug — fix the test.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/router.py tests/test_api.py
uv run mypy app/api/router.py tests/test_api.py
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/api/router.py tests/test_api.py
git commit -m "$(cat <<'EOF'
fix(api): catch IntegrityError specifically on commit (review finding #8)

create_column_value and create_credential were catching every exception
from session.commit() and rewriting it to HTTP 409 "already exists".
That hid OperationalError, role permission errors, and transaction
failures behind a duplicate-key response, so operators lost the real
signal and clients retried the wrong way.

Now we catch IntegrityError specifically (the SQLAlchemy class that
duplicate-key violations raise on every backend) and let other
SQLAlchemy exceptions propagate through FastAPI's default 5xx handling.

EOF
)"
```

## Task 1.3: Engine disposal on shutdown (finding #6)

**Why this exists:** `app/main.py:345-357` creates four async engines inline as positional arguments to `async_sessionmaker(...)`:

```python
app.state.registry_runtime_session_factory = async_sessionmaker(
    create_async_engine(secure_registry_runtime_db_url),  # not bound to a name
    expire_on_commit=False,
)
```

The engines are reachable transitively via the session factories on `app.state` but never get `.dispose()`'d. Only the `executor` engine is closed at line 435. Repeated lifespan cycles (e.g. `TestClient` startup/shutdown loops, dev-server reloads, multi-worker rolling restarts) accumulate connection pools until the database starts refusing new connections.

The fix: bind each engine to an explicit local, store it on `app.state`, and dispose them all in the shutdown branch after `yield`.

**Files:**
- Modify: `app/main.py:345-357` (engine creation)
- Modify: `app/main.py:432-436` (shutdown branch)
- Create: `tests/test_main_lifespan.py` (no existing lifespan test file)

- [ ] **Step 1: Write the failing test**

Create `tests/test_main_lifespan.py`:

```python
"""Lifespan management tests for app.main.

Code-review finding #6 (2026-04-07): four async engines were created
inline inside session factories and never disposed on shutdown. These
tests verify that all DB engines on app.state get disposed when the
lifespan exits.
"""
import pytest


@pytest.mark.asyncio
async def test_lifespan_disposes_all_database_engines() -> None:
    """All async engines stored on app.state must be disposed exactly once
    when the lifespan context exits."""
    from unittest.mock import AsyncMock, patch

    from app.main import app, lifespan

    # The four engines we expect to be disposed (in addition to the
    # executor's internal engine, which was already being closed).
    expected_engine_attrs = (
        "registry_runtime_engine",
        "steward_engine",
        "registry_admin_engine",
        "runtime_engine",
    )

    dispose_calls: list[str] = []

    real_dispose = None

    def make_tracking_dispose(name: str):
        async def _track() -> None:
            dispose_calls.append(name)
        return _track

    async with lifespan(app):
        # Inside the context: every expected engine attribute must exist on
        # app.state and be a real async engine. Patch each one's dispose to
        # record the call.
        for attr in expected_engine_attrs:
            assert hasattr(app.state, attr), (
                f"app.state.{attr} missing — engines must be tracked explicitly"
            )
            engine = getattr(app.state, attr)
            engine.dispose = make_tracking_dispose(attr)  # type: ignore[method-assign]

    # After the context exits, every engine should have been disposed exactly once.
    for attr in expected_engine_attrs:
        assert attr in dispose_calls, (
            f"app.state.{attr} was not disposed during shutdown"
        )
    assert len(dispose_calls) == len(expected_engine_attrs)
```

**Note on the type-ignore:** assigning to `engine.dispose` is necessary for the test mock and is local to the test file. The user feedback rule against `# type: ignore` applies to *production* code; tests legitimately need to patch instance methods, and this is a one-line, scoped, defensible use. If the project's mypy config rejects this even in tests, replace the assignment with `monkeypatch.setattr(engine, "dispose", make_tracking_dispose(attr))` using the pytest `monkeypatch` fixture, which won't trigger the assignment-typecheck.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_main_lifespan.py -v`
Expected: FAIL — the assertion `hasattr(app.state, "registry_runtime_engine")` fails because the engines aren't currently bound to attributes; they're hidden inside session factories.

- [ ] **Step 3: Bind engines to explicit `app.state` attributes**

In `app/main.py`, find lines 345-357:

```python
    app.state.registry_runtime_session_factory = async_sessionmaker(
        create_async_engine(secure_registry_runtime_db_url),
        expire_on_commit=False,
    )
    app.state.steward_session_factory = async_sessionmaker(
        create_async_engine(secure_steward_db_url), expire_on_commit=False
    )
    app.state.registry_admin_session_factory = async_sessionmaker(
        create_async_engine(secure_registry_admin_db_url), expire_on_commit=False
    )
    app.state.runtime_session_factory = async_sessionmaker(
        create_async_engine(secure_runtime_db_url), expire_on_commit=False
    )
```

Replace with:

```python
    app.state.registry_runtime_engine = create_async_engine(
        secure_registry_runtime_db_url
    )
    app.state.steward_engine = create_async_engine(secure_steward_db_url)
    app.state.registry_admin_engine = create_async_engine(
        secure_registry_admin_db_url
    )
    app.state.runtime_engine = create_async_engine(secure_runtime_db_url)

    app.state.registry_runtime_session_factory = async_sessionmaker(
        app.state.registry_runtime_engine, expire_on_commit=False
    )
    app.state.steward_session_factory = async_sessionmaker(
        app.state.steward_engine, expire_on_commit=False
    )
    app.state.registry_admin_session_factory = async_sessionmaker(
        app.state.registry_admin_engine, expire_on_commit=False
    )
    app.state.runtime_session_factory = async_sessionmaker(
        app.state.runtime_engine, expire_on_commit=False
    )
```

- [ ] **Step 4: Dispose engines in the shutdown branch**

In the same file, find the shutdown branch at lines 432-436:

```python
    yield

    await _cancel_reload_tasks(reload_tasks)
    await session_store.close()
    await app.state.executor.close()
    logger.info("Aegis Semantic Proxy Shutting down.")
```

Replace with:

```python
    yield

    await _cancel_reload_tasks(reload_tasks)
    await session_store.close()
    await app.state.executor.close()
    await app.state.registry_runtime_engine.dispose()
    await app.state.steward_engine.dispose()
    await app.state.registry_admin_engine.dispose()
    await app.state.runtime_engine.dispose()
    logger.info("Aegis Semantic Proxy Shutting down.")
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_main_lifespan.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green. The session factories now reference the same engine objects as before; the only change is that the engines are *also* reachable as their own attributes and get disposed at shutdown.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/main.py tests/test_main_lifespan.py
uv run mypy app/main.py tests/test_main_lifespan.py
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/main.py tests/test_main_lifespan.py
git commit -m "$(cat <<'EOF'
fix(lifespan): dispose all DB engines on shutdown (review finding #6)

Four async engines were created inline as positional arguments to
async_sessionmaker(...) and were only reachable via the session factory
closures on app.state. The shutdown branch only disposed the execution
engine, so every lifespan cycle leaked four pools — visible under
TestClient startup/shutdown loops, multi-worker restarts, or repeated
dev-server reloads.

Fix:
- Bind each engine to an explicit app.state attribute
- Pass the bound engine to async_sessionmaker (same factory wiring)
- Dispose each engine in the shutdown branch after yield

A new tests/test_main_lifespan.py covers the contract: every expected
engine attribute exists on app.state during the lifespan, and every one
gets disposed exactly once on exit.

EOF
)"
```

---

# Phase 2 — Hot-reload runtime correctness

Two related fixes around the metadata compile/reload pipeline. The order matters: Task 2.1 narrows the surface area before Task 2.2 fixes the retry semantics, so the second fix is easier to test.

## Task 2.1: Gate hot-reload on `active` status (finding #2)

**Why this exists:** `MetadataCompiler.compile_version()` at `app/api/compiler.py:77` accepts both `active` and `pending_review`. The router endpoint `compile_metadata_version` at `app/api/router.py:822` then unconditionally hot-reloads runtime state at lines 868-880, regardless of which status was compiled. The hot-reload calls `RegistryLoader.load_active_schema(...)` (line 870), which finds the most recent *active* artifact for the tenant — not the just-compiled one.

Worked example: tenant has V1 active and V2 pending_review.
1. Admin POSTs `/metadata/compile/V2`.
2. `compile_version(V2)` succeeds → V2 artifact written to DB.
3. Line 870 loads V1's schema (the active one).
4. Line 871 stores V1's schema in `app.state.registries[tenant]`.
5. Line 872 stores V2's hash in `app.state.loaded_artifact_hashes[tenant]`.
6. Lines 886-911 build a RAG index from V2's `artifact_blob`.

Result: schema=V1, RAG=V2, recorded hash=V2. Total mismatch until another reload corrects it.

**Fix:** in the router (not the compiler), gate the entire hot-reload block on `version_obj.status == "active"`. The compiler keeps its existing capability to compile `pending_review` versions for preview. Only active compiles trigger runtime swaps.

**Additionally, address the related preview-compile oddity** (flagged as Open Question #2 by the reviewer): `MetadataCompiler.compile_version()` currently stamps `version.approved_by` and `version.approved_at` unconditionally at `app/api/compiler.py:244-246`. For a preview compile on a `pending_review` version, that records an approval that hasn't happened yet. The clean semantics: `registry_hash` always updates (reflects the latest compile output), but `approved_by`/`approved_at` only update when `version.status == "active"`. It's a two-line change that fits naturally with this task's principle.

**Files:**
- Modify: `app/api/router.py:868-911` (the hot-reload block in `compile_metadata_version`)
- Modify: `app/api/compiler.py:243-246` (approval-field stamping)
- Modify: `tests/test_api.py` (or similar; find existing compile-endpoint tests with `grep -rn "metadata/compile\|compile_metadata_version" tests/`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_api.py`:

```python
def test_compile_pending_review_version_does_not_mutate_runtime_state() -> None:
    """Compiling a pending_review version must not touch app.state.registries
    or app.state.loaded_artifact_hashes.

    Code-review finding #2 (2026-04-07): the router blindly hot-reloaded after
    every successful compile, leaving the worker with mismatched schema/RAG/
    hash state when the compiled version was not yet active.
    """
    import uuid as uuid_mod

    from fastapi.testclient import TestClient
    from sqlalchemy import create_engine, text

    from app.api.auth import ResolvedCredential, require_admin_credential
    from app.main import app
    from tests.conftest import TEST_ADMIN_CREDENTIAL_ID

    admin_cred = ResolvedCredential(
        credential_id=TEST_ADMIN_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="admin_user",
        scope="admin",
    )

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

    app.dependency_overrides[require_admin_credential] = lambda: admin_cred

    try:
        with TestClient(app) as client:
            # Snapshot runtime state BEFORE the compile request
            registries_before = dict(app.state.registries)
            hashes_before = dict(app.state.loaded_artifact_hashes)

            response = client.post(
                f"/api/v1/metadata/compile/{pending_vid}"
            )

            # Snapshot runtime state AFTER the compile request
            registries_after = dict(app.state.registries)
            hashes_after = dict(app.state.loaded_artifact_hashes)

        # The compile may succeed (preview compile) or fail with 4xx if the
        # endpoint is restricted further. The critical assertion is that
        # runtime state is UNCHANGED for the requesting tenant.
        assert registries_after.get("test_tenant") is registries_before.get(
            "test_tenant"
        ), "Compiling pending_review should not swap app.state.registries"
        assert hashes_after.get("test_tenant") == hashes_before.get(
            "test_tenant"
        ), "Compiling pending_review should not advance loaded_artifact_hashes"

        # Additionally: the version row must not have approved_by / approved_at
        # stamped by a preview compile. Approval metadata belongs to the
        # active transition, not to the compile step.
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT approved_by, approved_at FROM metadata_versions"
                    " WHERE version_id = :vid"
                ),
                {"vid": pending_vid.hex},
            ).fetchone()
            assert row is not None
            assert row[0] is None, (
                f"preview compile stamped approved_by={row[0]!r} on a"
                f" pending_review version"
            )
            assert row[1] is None, (
                f"preview compile stamped approved_at={row[1]!r} on a"
                f" pending_review version"
            )

        # If the endpoint chose to reject pending_review compiles outright,
        # that's also acceptable — the safety property is the same.
        if response.status_code == 422:
            assert "active" in response.text.lower() or "status" in response.text.lower()
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_api.py::test_compile_pending_review_version_does_not_mutate_runtime_state -v`
Expected: FAIL — the runtime state assertions fail because the router unconditionally hot-reloads after compile.

- [ ] **Step 3: Implement the fix**

In `app/api/router.py`, find the hot-reload block at lines 868-911. The full block currently looks like:

```python
    # Hot-reload this tenant's schema slot only
    async with request.app.state.registry_runtime_session_factory() as rt_session:
        schema = await RegistryLoader.load_active_schema(rt_session, cred.tenant_id)
    request.app.state.registries[cred.tenant_id] = schema
    request.app.state.loaded_artifact_hashes[cred.tenant_id] = artifact.artifact_hash

    # Notify all other workers — always publish regardless of local state so
    # workers that haven't loaded this artifact yet receive the signal.
    await publish_reload(
        request.app.state.redis_client,
        cred.tenant_id,
        artifact.artifact_hash,
    )

    # Fetch column values for RAG builder
    async with request.app.state.registry_admin_session_factory() as val_session:
        column_values = await _fetch_rag_column_values(version_id, val_session)

    async def _rebuild_index() -> None:
        ...

    if wait_for_index:
        await _rebuild_index()
    else:
        _ = asyncio.create_task(_rebuild_index())
```

Wrap the entire block (from "Hot-reload this tenant's schema slot only" through the `_rebuild_index` dispatch) in a status guard:

```python
    # Hot-reload runtime state only when the compiled version is the active
    # one. Compiling a pending_review version is a preview operation and must
    # NOT swap the registry, advance loaded_artifact_hashes, publish reload
    # signals, or rebuild the RAG index — those steps would leave the worker
    # serving a mismatched schema/RAG/hash combination.
    if version_obj.status == "active":
        # Hot-reload this tenant's schema slot only
        async with request.app.state.registry_runtime_session_factory() as rt_session:
            schema = await RegistryLoader.load_active_schema(
                rt_session, cred.tenant_id
            )
        request.app.state.registries[cred.tenant_id] = schema
        request.app.state.loaded_artifact_hashes[cred.tenant_id] = (
            artifact.artifact_hash
        )

        # Notify all other workers — always publish regardless of local
        # state so workers that haven't loaded this artifact yet receive
        # the signal.
        await publish_reload(
            request.app.state.redis_client,
            cred.tenant_id,
            artifact.artifact_hash,
        )

        # Fetch column values for RAG builder
        async with request.app.state.registry_admin_session_factory() as val_session:
            column_values = await _fetch_rag_column_values(
                version_id, val_session
            )

        async def _rebuild_index() -> None:
            try:
                new_store = await build_from_artifact(
                    artifact_blob=artifact.artifact_blob,
                    version_id=str(version_id),
                    tenant_id=cred.tenant_id,
                    artifact_version=artifact.artifact_hash,
                    column_values=column_values,
                )
                request.app.state.vector_stores[cred.tenant_id] = new_store
                request.app.state.compiler.set_vector_store(
                    new_store, cred.tenant_id
                )
            except RagDivergenceError:
                logger.warning(
                    "RAG divergence detected for version %s — "
                    "index not updated; re-compile after fixing values.",
                    version_id,
                )
            except Exception:
                logger.exception(
                    "RAG index rebuild failed for version %s", version_id
                )

        if wait_for_index:
            await _rebuild_index()
        else:
            _ = asyncio.create_task(_rebuild_index())
```

The compile-and-write-artifact path above this block runs unconditionally — that's correct. Only the runtime mutation block is gated.

- [ ] **Step 4: Gate approval-field stamping on `active` status in the compiler**

In `app/api/compiler.py`, find the block at lines 243-246:

```python
        # 6. Lock the hash trace dynamically to the version object
        version.registry_hash = final_hash
        version.approved_by = actor
        version.approved_at = datetime.now(UTC)
```

Replace with:

```python
        # 6. Lock the hash trace dynamically to the version object.
        # registry_hash always updates (it reflects the latest compile
        # output), but approved_by/approved_at only update when the
        # version is already active — a preview compile on a
        # pending_review version must not record an approval that has
        # not happened yet.
        version.registry_hash = final_hash
        if version.status == "active":
            version.approved_by = actor
            version.approved_at = datetime.now(UTC)
```

This is the compiler-side half of the same principle the router-side gate enforces: preview compiles are inert with respect to lifecycle state. The test from Step 1 asserts both `approved_by` and `approved_at` remain NULL after compiling a `pending_review` version.

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_api.py::test_compile_pending_review_version_does_not_mutate_runtime_state -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -v`
Expected: all green. If any existing test compiled a `pending_review` version and expected runtime state to update, that test was relying on the broken behavior — fix it to compile an `active` version instead.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/router.py app/api/compiler.py tests/test_api.py
uv run mypy app/api/router.py app/api/compiler.py tests/test_api.py
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/api/router.py app/api/compiler.py tests/test_api.py
git commit -m "$(cat <<'EOF'
fix(api): gate compile hot-reload on active status (review finding #2)

compile_metadata_version unconditionally hot-reloaded runtime state
after every successful compile, even when the compiled version was
pending_review. The hot-reload then loaded the *currently active*
schema (not the just-compiled one) into app.state.registries while
advancing loaded_artifact_hashes to the just-compiled version's hash
and rebuilding the RAG index from that compile's artifact_blob. The
worker ended up serving a mismatched schema/RAG/hash combination.

Fix:
- router.compile_metadata_version: wrap the runtime mutation block in
  `if version_obj.status == "active"`. The compiler still supports
  compiling pending_review versions for preview — only active compiles
  trigger runtime swaps, publish reload signals, and rebuild RAG.
- compiler.MetadataCompiler.compile_version: gate approved_by /
  approved_at stamping on the same `version.status == "active"` check.
  A preview compile on a pending_review version must not record an
  approval that has not happened yet. registry_hash still updates
  unconditionally (it reflects the latest compile output).

EOF
)"
```

## Task 2.2: Defer `loaded_artifact_hashes` until RAG succeeds (finding #3)

**Why this exists:** `app/reload.py:182` advances `app.state.loaded_artifact_hashes[tenant_id] = db_hash` *before* the RAG rebuild starts at line 190. If the rebuild raises (lines 203-211), the function logs and returns — but the hash is already advanced. Next poll cycle hits the gate at line 156 (`if app.state.loaded_artifact_hashes.get(tenant_id) == db_hash: return`) and skips the tenant. The worker stays in a degraded state with stale RAG indefinitely until something else triggers a reload.

The same pattern exists in `app/api/router.py:872` (compile path), but Task 2.1 already gated that block on `active` status, so the `_rebuild_index` async task there runs only when we expect a swap. Both paths still need the same fix: only advance the loaded hash *after* RAG succeeds.

**Files:**
- Modify: `app/reload.py:182-211` (the schema-then-RAG sequence)
- Modify: `app/api/router.py:868-911` (the same sequence in the compile path; this is the block from Task 2.1)
- Create: `tests/test_reload.py`

- [ ] **Step 1: Write the failing test**

**Critical note on patch targets:** `_perform_reload` imports `RegistryLoader` and `build_from_artifact` INSIDE the function body (verify: `grep -n "from app.rag\|from app.steward" app/reload.py` — the `from` statements are at lines 135-136, inside `_perform_reload`). These are function-local rebindings, so patching `reload_mod.build_from_artifact` or `reload_mod.RegistryLoader` does nothing — those names aren't in the module's namespace at call time. The correct patches target the *source* modules (`app.rag.builder.build_from_artifact` and `app.steward.loader.RegistryLoader.load_active_schema`), so the next `from ... import` inside `_perform_reload` picks up the patched objects. `_load_active_artifact_and_values`, in contrast, is defined at module level (`app/reload.py:74`) and called unqualified at line 144 — patching `reload_mod._load_active_artifact_and_values` does work for that one.

Create `tests/test_reload.py`:

```python
"""Tests for app.reload._perform_reload retry semantics.

Code-review finding #3 (2026-04-07): both reload paths advanced
loaded_artifact_hashes before the RAG rebuild succeeded, so a failed
rebuild left the worker in a degraded state that subsequent polls
considered "up to date" and refused to retry.
"""
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_perform_reload_does_not_advance_hash_when_rag_rebuild_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If build_from_artifact() raises during _perform_reload, the tenant's
    loaded_artifact_hashes entry must NOT advance to the new hash. The next
    poll cycle must see a mismatch and retry the rebuild."""
    from app import reload as reload_mod

    # Build a fake app with the minimum state _perform_reload touches
    fake_app: Any = MagicMock()
    fake_app.state.loaded_artifact_hashes = {"tenant_a": "old_hash"}
    fake_app.state.registries = {}
    fake_app.state.vector_stores = {}
    fake_app.state.reload_locks = {}

    # Stub the artifact loader to return a fresh artifact for tenant_a.
    # _load_active_artifact_and_values is defined at module level in
    # app/reload.py so patching reload_mod.<name> intercepts the call.
    fake_artifact = MagicMock()
    fake_artifact.artifact_hash = "new_hash"
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.version_id = "00000000-0000-0000-0000-000000000001"

    async def fake_load(_app: Any, _tenant: str) -> tuple[Any, dict]:
        return (fake_artifact, {})

    monkeypatch.setattr(
        reload_mod, "_load_active_artifact_and_values", fake_load
    )

    # Stub the schema loader at the SOURCE module. _perform_reload does
    # `from app.steward.loader import RegistryLoader` inside the function
    # body, so the function-local import resolves from app.steward.loader
    # at call time. Patching reload_mod.RegistryLoader would be a no-op.
    fake_schema = MagicMock()
    monkeypatch.setattr(
        "app.steward.loader.RegistryLoader.load_active_schema",
        AsyncMock(return_value=fake_schema),
    )

    # Stub registry_runtime_session_factory as a no-op async context manager
    class _NoopSession:
        async def __aenter__(self) -> Any:
            return self
        async def __aexit__(self, *_: Any) -> None:
            pass

    fake_app.state.registry_runtime_session_factory = _NoopSession

    # Force build_from_artifact to raise. Same reasoning as above:
    # `from app.rag.builder import ... build_from_artifact` is a function-
    # local import, so we patch app.rag.builder.build_from_artifact
    # directly at its source.
    monkeypatch.setattr(
        "app.rag.builder.build_from_artifact",
        AsyncMock(side_effect=RuntimeError("boom")),
    )

    # Run the reload
    await reload_mod._perform_reload(fake_app, "tenant_a")

    # The schema swap is allowed (it's idempotent) but the hash MUST stay at
    # the old value so the next poll retries.
    assert fake_app.state.loaded_artifact_hashes["tenant_a"] == "old_hash", (
        "loaded_artifact_hashes advanced to new_hash despite RAG failure — "
        "the next poll will skip this tenant and the RAG store stays stale"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_reload.py -v`
Expected: FAIL — the assertion fails because line 182 of `reload.py` advances the hash before line 190 calls `build_from_artifact`.

- [ ] **Step 3: Move the hash advance to after the RAG rebuild in `reload.py`**

In `app/reload.py`, find the block at lines 180-211:

```python
        # Mark schema as live before starting RAG rebuild.
        # Old RAG store remains active until the rebuild succeeds.
        app.state.loaded_artifact_hashes[tenant_id] = db_hash
        logger.info(
            "reload: tenant %s schema updated to artifact %s",
            tenant_id, db_hash[:12],
        )

        # --- RAG rebuild (keeps old store on failure) ---
        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact.artifact_blob,
                version_id=str(artifact.version_id),
                tenant_id=tenant_id,
                artifact_version=db_hash,
                column_values=column_values,
            )
            app.state.vector_stores[tenant_id] = new_store
            app.state.compiler.set_vector_store(new_store, tenant_id)
            logger.info(
                "reload: tenant %s RAG rebuilt for artifact %s",
                tenant_id, db_hash[:12],
            )
        except RagDivergenceError:
            logger.warning(
                "reload: RAG divergence for tenant %s — old index retained", tenant_id
            )
        except Exception:
            logger.exception(
                "reload: RAG rebuild failed for tenant %s — old index retained",
                tenant_id,
            )
```

Replace with:

```python
        # Schema is in place. RAG rebuild is next; loaded_artifact_hashes
        # is intentionally NOT advanced yet — if the RAG rebuild fails, the
        # next poll cycle must see a hash mismatch and retry. Schema reload
        # is idempotent so re-running it on the next poll is cheap.
        logger.info(
            "reload: tenant %s schema swapped to artifact %s",
            tenant_id, db_hash[:12],
        )

        # --- RAG rebuild (keeps old store on failure) ---
        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact.artifact_blob,
                version_id=str(artifact.version_id),
                tenant_id=tenant_id,
                artifact_version=db_hash,
                column_values=column_values,
            )
            app.state.vector_stores[tenant_id] = new_store
            app.state.compiler.set_vector_store(new_store, tenant_id)
        except RagDivergenceError:
            logger.warning(
                "reload: RAG divergence for tenant %s — old index retained,"
                " hash NOT advanced (will retry)",
                tenant_id,
            )
            return
        except Exception:
            logger.exception(
                "reload: RAG rebuild failed for tenant %s — old index"
                " retained, hash NOT advanced (will retry)",
                tenant_id,
            )
            return

        # Both schema and RAG are now live for this artifact. Advance the
        # loaded hash so the next poll skips this tenant until the next
        # genuine version change.
        app.state.loaded_artifact_hashes[tenant_id] = db_hash
        logger.info(
            "reload: tenant %s fully reloaded to artifact %s",
            tenant_id, db_hash[:12],
        )
```

The two key changes:
1. The hash advance moves to *after* the try/except block, gated on success.
2. The two failure handlers add `return` so the success path can't run.

- [ ] **Step 4: Mirror the fix in `app/api/router.py:_rebuild_index`**

In `app/api/router.py`, find the `_rebuild_index` inner function inside `compile_metadata_version` (it's inside the `if version_obj.status == "active":` block from Task 2.1). The current shape is:

```python
        async def _rebuild_index() -> None:
            try:
                new_store = await build_from_artifact(
                    artifact_blob=artifact.artifact_blob,
                    version_id=str(version_id),
                    tenant_id=cred.tenant_id,
                    artifact_version=artifact.artifact_hash,
                    column_values=column_values,
                )
                request.app.state.vector_stores[cred.tenant_id] = new_store
                request.app.state.compiler.set_vector_store(
                    new_store, cred.tenant_id
                )
            except RagDivergenceError:
                logger.warning(
                    "RAG divergence detected for version %s — "
                    "index not updated; re-compile after fixing values.",
                    version_id,
                )
            except Exception:
                logger.exception(
                    "RAG index rebuild failed for version %s", version_id
                )
```

You also need to find the line that advanced `loaded_artifact_hashes` for the compile path. After Task 2.1, it lives at:

```python
        request.app.state.loaded_artifact_hashes[cred.tenant_id] = (
            artifact.artifact_hash
        )
```

Move that line *out of* the synchronous block and *into* the success branch of `_rebuild_index`. The new shape:

```python
        async def _rebuild_index() -> None:
            try:
                new_store = await build_from_artifact(
                    artifact_blob=artifact.artifact_blob,
                    version_id=str(version_id),
                    tenant_id=cred.tenant_id,
                    artifact_version=artifact.artifact_hash,
                    column_values=column_values,
                )
                request.app.state.vector_stores[cred.tenant_id] = new_store
                request.app.state.compiler.set_vector_store(
                    new_store, cred.tenant_id
                )
                # Advance hash only after both schema and RAG are live.
                request.app.state.loaded_artifact_hashes[cred.tenant_id] = (
                    artifact.artifact_hash
                )
            except RagDivergenceError:
                logger.warning(
                    "RAG divergence detected for version %s — "
                    "index not updated; loaded hash NOT advanced (retry on next reload).",
                    version_id,
                )
            except Exception:
                logger.exception(
                    "RAG index rebuild failed for version %s — "
                    "loaded hash NOT advanced (retry on next reload)",
                    version_id,
                )
```

And remove the standalone `request.app.state.loaded_artifact_hashes[cred.tenant_id] = artifact.artifact_hash` line that was set unconditionally before `publish_reload`. After this change, the compile-path execution order is: write artifact → swap schema in registries → publish reload signal → schedule RAG rebuild → (inside rebuild) advance loaded hash on success.

**Note:** the `publish_reload` call still happens unconditionally before the RAG rebuild because other workers need to learn about the new artifact and run their own reload (which now also has the fixed retry semantics from `reload.py`). That's correct.

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_reload.py -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/reload.py app/api/router.py tests/test_reload.py
uv run mypy app/reload.py app/api/router.py tests/test_reload.py
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/reload.py app/api/router.py tests/test_reload.py
git commit -m "$(cat <<'EOF'
fix(reload): defer loaded_artifact_hashes update until RAG succeeds (#3)

Both reload paths advanced loaded_artifact_hashes before the RAG
rebuild started. If build_from_artifact() raised, the failure handler
logged and returned, but the hash was already advanced — so the next
poll cycle (and the cross-worker pub/sub gate) saw a "match" and
skipped the tenant. The worker stayed in a degraded state with stale
RAG indefinitely.

Fix:
- In reload._perform_reload: move the hash advance to AFTER the RAG
  try/except block, and add `return` in both failure handlers so the
  success path cannot run.
- In router.compile_metadata_version._rebuild_index: move the hash
  advance into the success branch of the inner try/except.

Schema reload remains idempotent — re-running it on the next poll is
cheap. The retry semantics are now correct: any RAG failure leaves the
loaded hash at the previous version, and the next reload retries.

Test: monkeypatched build_from_artifact raises RuntimeError;
loaded_artifact_hashes for that tenant must remain at the old value.

EOF
)"
```

## Task 2.3: Serialize compile-path rebuild with pub/sub echoes (reviewer's Open Question #1)

**Why this exists:** The Phase 2 fixes introduce a new race. Originally, `_perform_reload` at `app/reload.py:156` skipped the echo because the compile path had *already* advanced `loaded_artifact_hashes` before publishing the signal. Task 2.2 defers that advance until after RAG succeeds — which correctly fixes the retry semantics but *breaks the echo skip*. The sequence is now:

1. Local request handler: schema swap (synchronous)
2. Local: `publish_reload(...)` → broadcasts signal
3. Local: schedules the compile-path RAG rebuild as an asyncio task
4. **Echo arrives**: `_perform_reload` is scheduled for the same tenant on the same worker
5. Both paths run concurrently — both call `build_from_artifact`, both write to `app.state.vector_stores[tenant]` and `app.state.compiler.set_vector_store(...)`, both try to advance the loaded hash

**Fix:** use the per-tenant reload lock that already exists at `app/reload.py:140-143`. The compile-path rebuild and `_perform_reload` both acquire `app.state.reload_locks[tenant_id]` and re-check `loaded_artifact_hashes` inside the lock so whichever runs second skips naturally.

**Why a refactor is part of the fix:** the previous draft of this task left `_rebuild_index` as an inner function inside `compile_metadata_version`, which could not be called from a test and could only be exercised end-to-end via the FastAPI route. The reviewer flagged that the proposed test ran *two `_perform_reload` calls*, not the actual race the fix is supposed to prevent — and `_perform_reload` already serializes on its own lock, so the test would have passed both before AND after the fix. The honest fix is to extract `_rebuild_index` to a module-level helper that can be invoked directly. The test then races *the new helper* against `_perform_reload`, which is the actual production race.

**Files:**
- Modify: `app/api/router.py` — extract `_rebuild_index` to a module-level `_rebuild_rag_index_for_tenant` function; update `compile_metadata_version` to call it
- Modify: `tests/test_reload.py` — add a regression test that races the helper against `_perform_reload`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_reload.py`:

```python
@pytest.mark.asyncio
async def test_rebuild_helper_and_perform_reload_serialize_via_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The compile-path RAG rebuild and a concurrent pub/sub echo path
    must serialize through app.state.reload_locks[tenant_id]. Whichever
    runs first advances loaded_artifact_hashes; the second path sees the
    hash match inside the lock and skips.

    This races the REAL extracted helper (_rebuild_rag_index_for_tenant)
    against the REAL _perform_reload, so the test will fail before the
    fix lands and pass after.

    Regression: reviewer's Open Question #1 after the Task 2.2 fix
    deferred the hash advance, breaking the original hash-match skip.
    """
    import asyncio
    import uuid
    from typing import Any
    from unittest.mock import AsyncMock, MagicMock

    from app import reload as reload_mod
    from app.api.router import _rebuild_rag_index_for_tenant

    # Counter for how many times build_from_artifact actually runs.
    build_calls = [0]

    async def counting_build(**_kwargs: Any) -> Any:
        build_calls[0] += 1
        # Sleep so the concurrent task has time to queue on the lock —
        # without this, the first task may finish entirely before the
        # second starts and the test wouldn't exercise the race window.
        await asyncio.sleep(0.05)
        return MagicMock()

    monkeypatch.setattr(
        "app.rag.builder.build_from_artifact", counting_build
    )

    # Fake app state shared by both paths.
    fake_app: Any = MagicMock()
    fake_app.state.loaded_artifact_hashes = {"tenant_a": "old_hash"}
    fake_app.state.registries = {}
    fake_app.state.vector_stores = {}
    fake_app.state.reload_locks = {}
    fake_app.state.compiler.set_vector_store = MagicMock()

    fake_artifact = MagicMock()
    fake_artifact.artifact_hash = "new_hash"
    fake_artifact.artifact_blob = {"tables": []}
    fake_artifact.version_id = uuid.UUID(
        "00000000-0000-0000-0000-000000000001"
    )

    # Stub _perform_reload's dependencies so it reaches build_from_artifact.
    async def fake_load(_app: Any, _tenant: str) -> tuple[Any, dict]:
        return (fake_artifact, {})

    monkeypatch.setattr(
        reload_mod, "_load_active_artifact_and_values", fake_load
    )
    monkeypatch.setattr(
        "app.steward.loader.RegistryLoader.load_active_schema",
        AsyncMock(return_value=MagicMock()),
    )

    class _NoopSession:
        async def __aenter__(self) -> Any:
            return self
        async def __aexit__(self, *_: Any) -> None:
            pass

    fake_app.state.registry_runtime_session_factory = _NoopSession

    # Race the compile-path helper against the pub/sub echo path.
    # asyncio.gather schedules both coroutines; the counting_build sleep
    # guarantees both reach the lock before either completes its rebuild.
    await asyncio.gather(
        _rebuild_rag_index_for_tenant(
            app=fake_app,
            tenant_id="tenant_a",
            artifact_blob=fake_artifact.artifact_blob,
            artifact_hash="new_hash",
            artifact_version_id=str(fake_artifact.version_id),
            column_values={},
        ),
        reload_mod._perform_reload(fake_app, "tenant_a"),
    )

    # Exactly ONE build_from_artifact call — whichever path acquired the
    # lock first did the rebuild, and the other saw the hash match inside
    # the lock and skipped.
    assert build_calls[0] == 1, (
        f"expected exactly 1 build_from_artifact call, got {build_calls[0]};"
        " the compile-path helper and pub/sub echo are not serializing"
        " on app.state.reload_locks[tenant_id]"
    )
    assert fake_app.state.loaded_artifact_hashes["tenant_a"] == "new_hash"
```

- [ ] **Step 2: Run the test to verify it fails for the right reason**

Run: `uv run pytest tests/test_reload.py::test_rebuild_helper_and_perform_reload_serialize_via_lock -v`
Expected: **`ImportError` — `_rebuild_rag_index_for_tenant` does not exist yet**. That's the correct failure mode for a TDD red step: the helper is the API the fix introduces, and the test cannot pass until the helper exists *and* takes the lock correctly.

If the test fails with `build_calls[0] == 2` *after* the helper is created in Step 3, the helper is missing the lock or the inside-lock hash check.

- [ ] **Step 3: Extract `_rebuild_index` to a module-level helper**

In `app/api/router.py`, locate the `_rebuild_index` inner function inside `compile_metadata_version` (after Task 2.2's fix, it's inside the `if version_obj.status == "active":` block). The current shape captures four closure variables: `request.app.state`, `cred.tenant_id`, `artifact`, `column_values`, `version_id`.

Extract a new module-level function near the top of the file (just below the existing helpers like `_resolve_session`, `_fetch_rag_column_values`, etc.):

```python
async def _rebuild_rag_index_for_tenant(
    app: Any,
    tenant_id: str,
    artifact_blob: dict[str, Any],
    artifact_hash: str,
    artifact_version_id: str,
    column_values: dict[str, list[str]],
) -> None:
    """Rebuild the RAG vector store for a tenant, serialized via the
    per-tenant reload lock.

    This is the compile-path counterpart to app.reload._perform_reload —
    both paths take the same app.state.reload_locks[tenant_id] and
    re-check loaded_artifact_hashes inside the lock so concurrent paths
    skip naturally instead of double-rebuilding.

    Extracted to a module-level function (rather than living as an
    inner function inside compile_metadata_version) so the lock
    behavior is testable in isolation — see
    tests/test_reload.py::test_rebuild_helper_and_perform_reload_serialize_via_lock.
    """
    import asyncio

    from app.rag.builder import RagDivergenceError, build_from_artifact

    locks = app.state.reload_locks
    if tenant_id not in locks:
        locks[tenant_id] = asyncio.Lock()

    async with locks[tenant_id]:
        # Idempotency: if a concurrent pub/sub echo already rebuilt the
        # index for this artifact, skip. Checked inside the lock so
        # both paths see a consistent loaded_artifact_hashes view.
        if app.state.loaded_artifact_hashes.get(tenant_id) == artifact_hash:
            logger.info(
                "rag_rebuild: tenant %s already at artifact %s — skipping"
                " (concurrent path won the race)",
                tenant_id, artifact_hash[:12],
            )
            return

        try:
            new_store = await build_from_artifact(
                artifact_blob=artifact_blob,
                version_id=artifact_version_id,
                tenant_id=tenant_id,
                artifact_version=artifact_hash,
                column_values=column_values,
            )
            app.state.vector_stores[tenant_id] = new_store
            app.state.compiler.set_vector_store(new_store, tenant_id)
            # Advance the loaded hash only after BOTH schema and RAG
            # are live for this artifact (Task 2.2 contract).
            app.state.loaded_artifact_hashes[tenant_id] = artifact_hash
            logger.info(
                "rag_rebuild: tenant %s reloaded to artifact %s",
                tenant_id, artifact_hash[:12],
            )
        except RagDivergenceError:
            logger.warning(
                "rag_rebuild: divergence for tenant %s — index not updated;"
                " loaded hash NOT advanced (retry on next reload)",
                tenant_id,
            )
        except Exception:
            logger.exception(
                "rag_rebuild: failed for tenant %s — loaded hash NOT"
                " advanced (retry on next reload)",
                tenant_id,
            )
```

The local `import asyncio` inside the function is intentional: `asyncio` is already imported at module level in `router.py`, so the local import is technically redundant — verify with `grep -n "^import asyncio\b" app/api/router.py` and remove the local import if the module-level one is present. The same applies to `RagDivergenceError`/`build_from_artifact` — they were lazy-imported in the original `compile_metadata_version` body to avoid a circular import; preserve that pattern in the helper.

`Any` and `dict[str, Any]` need `from typing import Any` at the top of `router.py` (which is almost certainly already imported — verify with `grep -n "from typing" app/api/router.py`).

- [ ] **Step 4: Replace the inline `_rebuild_index` with a call to the helper**

In `app/api/router.py`, inside `compile_metadata_version`'s `if version_obj.status == "active":` block, replace the inner function and its scheduling with:

Find:
```python
        async def _rebuild_index() -> None:
            try:
                new_store = await build_from_artifact(
                    artifact_blob=artifact.artifact_blob,
                    version_id=str(version_id),
                    tenant_id=cred.tenant_id,
                    artifact_version=artifact.artifact_hash,
                    column_values=column_values,
                )
                request.app.state.vector_stores[cred.tenant_id] = new_store
                request.app.state.compiler.set_vector_store(
                    new_store, cred.tenant_id
                )
                # Advance hash only after both schema and RAG are live.
                request.app.state.loaded_artifact_hashes[cred.tenant_id] = (
                    artifact.artifact_hash
                )
            except RagDivergenceError:
                logger.warning(
                    "RAG divergence detected for version %s — "
                    "index not updated; loaded hash NOT advanced"
                    " (retry on next reload).",
                    version_id,
                )
            except Exception:
                logger.exception(
                    "RAG index rebuild failed for version %s — "
                    "loaded hash NOT advanced (retry on next reload)",
                    version_id,
                )

        if wait_for_index:
            await _rebuild_index()
        else:
            _ = asyncio.create_task(_rebuild_index())
```

Replace with:
```python
        if wait_for_index:
            await _rebuild_rag_index_for_tenant(
                app=request.app,
                tenant_id=cred.tenant_id,
                artifact_blob=artifact.artifact_blob,
                artifact_hash=artifact.artifact_hash,
                artifact_version_id=str(version_id),
                column_values=column_values,
            )
        else:
            _ = asyncio.create_task(
                _rebuild_rag_index_for_tenant(
                    app=request.app,
                    tenant_id=cred.tenant_id,
                    artifact_blob=artifact.artifact_blob,
                    artifact_hash=artifact.artifact_hash,
                    artifact_version_id=str(version_id),
                    column_values=column_values,
                )
            )
```

The lazy `from app.rag.builder import RagDivergenceError, build_from_artifact` that previously lived inside `compile_metadata_version` can now be removed from there — the helper does its own lazy import.

- [ ] **Step 5: Run the test to verify it passes**

Run: `uv run pytest tests/test_reload.py::test_rebuild_helper_and_perform_reload_serialize_via_lock -v`
Expected: PASS. `build_calls[0] == 1` because the lock + inside-lock hash check serialize the two paths.

Then run *both* `tests/test_reload.py` tests together to confirm the Task 2.2 RAG-failure test still passes:

Run: `uv run pytest tests/test_reload.py -v`
Expected: both tests PASS.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -v`
Expected: all green. The router restructure changes the call signature of the rebuild path; if any existing test (e.g. in `test_api.py` or `test_version_lifecycle.py`) was patching `_rebuild_index` by name, it will need to switch to patching `_rebuild_rag_index_for_tenant`. Fix at the call site, not the production code.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/router.py app/reload.py tests/test_reload.py
uv run mypy app/api/router.py app/reload.py tests/test_reload.py
uv run lint-imports
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/api/router.py tests/test_reload.py
git commit -m "$(cat <<'EOF'
fix(reload): serialize compile path with pub/sub echoes (reviewer Q1)

Task 2.2 deferred the loaded_artifact_hashes advance until after the
RAG rebuild succeeded. That fixed the retry semantics but broke the
original echo-skip behavior: the publishing worker's own pub/sub echo
arrives BEFORE the compile-path rebuild has advanced the hash, so
_perform_reload no longer short-circuits on the hash match. Both
paths would then rebuild the RAG store concurrently — wasting
embeddings-API calls and risking inconsistent vector_store state
under single-writer assumptions.

Fix: extract the compile-path rebuild from an inner function in
compile_metadata_version into a module-level helper
_rebuild_rag_index_for_tenant. The helper acquires the same per-tenant
reload lock that _perform_reload already takes (app.state.reload_locks
[tenant_id]) and re-checks loaded_artifact_hashes inside the lock so
the second path can skip naturally.

The extraction is part of the fix, not incidental — it's what makes
the lock behavior testable in isolation. tests/test_reload.py now
races _rebuild_rag_index_for_tenant against _perform_reload directly
via asyncio.gather and asserts build_from_artifact runs exactly once.
The earlier draft of this test ran two _perform_reload calls, which
proved nothing because _perform_reload already serializes on its own
lock — the reviewer correctly flagged that the test could pass both
before and after the fix.

EOF
)"
```

---

# Phase 3 — Session schema staleness (finding #1)

The follow-up detection path at `app/compiler/engine.py:94-119` reads the prior session context and reuses `last_filtered_schema` directly with no version check. `SessionQueryContext` (`app/compiler/models.py:43`) carries only `last_filtered_schema`, `last_successful_sql`, `timestamp` — no record of which registry version produced the schema. When an admin compiles a new registry that removes or reclassifies a sensitive column, any user with an existing `session_id` continues querying against the old aliases, physical mappings, and safety flags for the duration of the session TTL.

The fix is structural: tag every `SessionQueryContext` with the `RegistrySchema.version` it was built against, and refuse to reuse it when that version doesn't match the currently loaded registry. The compare is on the artifact hash (`schema.version` is the artifact hash — see `app/steward/loader.py:164`), so any genuine compile produces a fresh hash and invalidates every cached context.

## Task 3.1: Add `registry_version` to `SessionQueryContext`

**Files:**
- Modify: `app/compiler/models.py:43-46` (`SessionQueryContext`)
- Modify: `app/compiler/engine.py:94-105` (follow-up gate)
- Modify: `app/compiler/engine.py:241-249` (context write)
- Modify: `tests/test_session_store.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_session_store.py`:

```python
@pytest.mark.asyncio
async def test_follow_up_rejects_cached_context_when_registry_version_changes() -> None:
    """A SessionQueryContext built against registry version A must NOT be
    reused as a follow-up when the active registry version is now B.

    Code-review finding #1 (2026-04-07): the follow-up path reused
    last_filtered_schema with no version check, allowing queries against
    aliases that had been removed or reclassified by a newer registry.
    """
    import time
    from unittest.mock import AsyncMock, MagicMock

    from app.compiler.engine import CompilerEngine
    from app.compiler.models import (
        FilteredSchema,
        PromptHints,
        SessionQueryContext,
        UserIntent,
    )
    from app.compiler.session_store import SessionStore
    from app.steward import RegistrySchema

    # Stub the dependencies CompilerEngine needs
    schema_filter = MagicMock()
    schema_filter.is_follow_up = MagicMock(return_value=True)
    schema_filter.filter_schema = MagicMock(
        return_value=FilteredSchema(
            version="v_new",
            tables=[],
            relationships=[],
            omitted_columns={},
        )
    )

    prompt_builder = MagicMock()
    prompt_builder.build_prompt = MagicMock(return_value=MagicMock(
        system_instruction="", user_prompt="", chat_history=[]
    ))

    llm_gateway = MagicMock()
    llm_gateway.generate = AsyncMock(return_value=MagicMock(
        raw_text='{"sql": "SELECT 1"}',
        model_id="mock",
        latency_ms=1.0,
        prompt_tokens=0,
        completion_tokens=0,
    ))

    parser = MagicMock()
    safety_engine = MagicMock()
    translator = MagicMock()

    engine = CompilerEngine(
        schema_filter=schema_filter,
        prompt_builder=prompt_builder,
        llm_gateway=llm_gateway,
        parser=parser,
        safety_engine=safety_engine,
        translator=translator,
    )
    engine.session_store = SessionStore(redis_client=None, ttl=3600)

    # Pre-seed a stale context tied to registry version "v_old"
    stale_filtered = FilteredSchema(
        version="v_old",
        tables=[],
        relationships=[],
        omitted_columns={"old_alias": "removed in v_new"},
    )
    stale_context = SessionQueryContext(
        last_filtered_schema=stale_filtered,
        last_successful_sql="SELECT * FROM old_table",
        timestamp=time.time(),
        registry_version="v_old",
    )
    await engine.session_store.set("session-123", stale_context)

    # The currently loaded registry is "v_new"
    new_schema = RegistrySchema(version="v_new", tables=[], relationships=[])

    intent = UserIntent(natural_language_query="follow-up question")
    hints = PromptHints(column_hints=[])

    # We expect the engine to NOT treat this as a follow-up — even though
    # is_follow_up returns True, the version mismatch should override.
    try:
        await engine.compile(
            intent=intent,
            schema=new_schema,
            hints=hints,
            tenant_id="test_tenant",
            session_id="session-123",
        )
    except Exception:
        # Other failures (mocked translator, etc.) are fine — we only care
        # about whether filter_schema was called (proving fresh-build path)
        # versus whether stale_filtered was reused.
        pass

    # If the version-check fix is in place, filter_schema must have been
    # called — meaning the engine rebuilt the filtered schema instead of
    # reusing the stale one.
    assert schema_filter.filter_schema.called, (
        "Engine reused the stale SessionQueryContext despite registry"
        " version mismatch (v_old context vs v_new active schema)"
    )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_session_store.py::test_follow_up_rejects_cached_context_when_registry_version_changes -v`
Expected: FAIL — `schema_filter.filter_schema` was not called because the engine reused the stale context.

(If the test fails to *construct* `SessionQueryContext` with `registry_version=` because that field doesn't exist yet, that's also a "test fails as expected" — the next step adds the field.)

- [ ] **Step 3: Add `registry_version` to `SessionQueryContext`**

In `app/compiler/models.py`, find the class at line 43:

```python
class SessionQueryContext(BaseModel):
    last_filtered_schema: FilteredSchema
    last_successful_sql: str
    timestamp: float
```

Replace with:

```python
class SessionQueryContext(BaseModel):
    last_filtered_schema: FilteredSchema
    last_successful_sql: str
    timestamp: float
    registry_version: str
```

The field is required (no default) so any code path that constructs `SessionQueryContext` without specifying it will fail at type-check time — the engine update in Step 4 fixes that.

- [ ] **Step 4: Record `registry_version` when writing the context**

In `app/compiler/engine.py`, find the `SessionQueryContext` construction at line 244:

```python
            if session_id:
                await self.session_store.set(
                    session_id,
                    SessionQueryContext(
                        last_filtered_schema=filtered_schema,
                        last_successful_sql=executable.sql,
                        timestamp=time.time(),
                    ),
                )
```

Replace with:

```python
            if session_id:
                await self.session_store.set(
                    session_id,
                    SessionQueryContext(
                        last_filtered_schema=filtered_schema,
                        last_successful_sql=executable.sql,
                        timestamp=time.time(),
                        registry_version=schema.version,
                    ),
                )
```

`schema` is the `RegistrySchema` parameter to `compile()`; its `.version` field holds the artifact hash (set by `RegistryLoader.load_schema_from_artifact` at `app/steward/loader.py:164`).

- [ ] **Step 5: Check `registry_version` in the follow-up gate**

In the same file, find the follow-up detection block at lines 97-105:

```python
            is_follow_up = (
                prior_context is not None
                and hasattr(self.schema_filter, "is_follow_up")
                and self.schema_filter.is_follow_up(
                    intent,
                    prior_context.last_filtered_schema,
                    full_schema=schema,
                )
            )
```

Replace with:

```python
            is_follow_up = (
                prior_context is not None
                and prior_context.registry_version == schema.version
                and hasattr(self.schema_filter, "is_follow_up")
                and self.schema_filter.is_follow_up(
                    intent,
                    prior_context.last_filtered_schema,
                    full_schema=schema,
                )
            )
```

The version check sits *before* the `is_follow_up` predicate so that a stale-version context never even reaches the follow-up logic. A registry compile produces a new artifact hash, which produces a new `schema.version`, which invalidates every cached context built against the old version.

- [ ] **Step 6: Run the test to verify it passes**

Run: `uv run pytest tests/test_session_store.py::test_follow_up_rejects_cached_context_when_registry_version_changes -v`
Expected: PASS.

- [ ] **Step 7: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green. Existing tests that construct `SessionQueryContext` directly will fail type-check or runtime validation if they don't supply `registry_version` — fix them by adding `registry_version="<some_test_version>"` to each construction. There should be no production callers other than the engine itself; check with:

```bash
grep -rn "SessionQueryContext(" app/ tests/
```

- [ ] **Step 8: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/compiler/models.py app/compiler/engine.py tests/test_session_store.py
uv run mypy app/compiler/models.py app/compiler/engine.py tests/test_session_store.py
```
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add app/compiler/models.py app/compiler/engine.py tests/test_session_store.py
git commit -m "$(cat <<'EOF'
fix(engine): invalidate stale session context on registry change (#1)

The follow-up path at engine.py:94-119 reused
SessionQueryContext.last_filtered_schema with no version check. After
an admin compiled a new registry that removed or reclassified a
sensitive column, any user with an active session_id continued
querying against the old aliases, physical mappings, and safety flags
for the duration of the session TTL — a real safety bypass.

Fix:
- Add a required registry_version: str field to SessionQueryContext.
- engine.compile() records schema.version (the artifact hash) when it
  writes the context.
- The follow-up gate checks prior_context.registry_version against
  the current schema.version BEFORE invoking is_follow_up. A version
  mismatch falls through to the fresh-build path.

Any registry compile produces a new artifact hash and invalidates
every cached context built against the old version, by construction.
No proactive cache eviction needed.

EOF
)"
```

---

# Phase 4 — WORM audit chain serialization (finding #4)

`app/api/router.py:690-696` and `app/api/compiler.py:208-227` both read the chain tip with a plain `SELECT MetadataAudit ORDER BY timestamp DESC, audit_id DESC LIMIT 1` and compute the next row hash. No `FOR UPDATE`, no advisory lock, no unique constraint on `previous_hash`. Two concurrent transactions can read the same tip and write different rows pointing at the same `previous_hash`, branching the chain.

The existing schema (`backend_migrations/versions/0001_initial_schema.py:53`) has `row_hash UNIQUE` but `previous_hash` is just nullable text with no constraint. There's a BEFORE UPDATE/DELETE trigger that prevents mutation of existing audit rows but does nothing to prevent concurrent inserts.

The fix has two parts:
1. **Backstop:** add a partial unique index on `previous_hash` for non-genesis rows (`WHERE previous_hash != ''`). This makes a fork *impossible* at the database level — the second concurrent insert raises `IntegrityError`. Added directly to `0001_initial_schema.py` per the "no new migrations" rule.
2. **Retry:** wrap the audit appenders in a small retry loop that catches `IntegrityError`, re-reads the chain tip, and tries again. Without the retry, the loser of a race gets a 500 — with it, both writers succeed serially.

For SQLite (used in tests), a partial unique index is supported in modern SQLite versions but the syntax differs slightly. We use `op.execute("CREATE UNIQUE INDEX ...")` so we can write the index DDL directly and have it work on both backends.

## Task 4.1: Add partial unique index on `previous_hash` to the consolidated baseline

**Per the "no new migrations" rule** (`feedback_no_new_migrations.md`): edit `backend_migrations/versions/0001_initial_schema.py` directly. The DB is dropped and recreated freely; `0001` is *the* init script.

The index is partial — `WHERE previous_hash != ''` — so the genesis row (which has `previous_hash = ''`) is not subject to uniqueness. There is at most one genesis row in practice; if that ever needed to be multiple, the schema would need a different model anyway.

**Files:**
- Modify: `backend_migrations/versions/0001_initial_schema.py:424-429` (add the new index alongside the existing `uq_one_active_version_per_tenant`)

- [ ] **Step 1: Add the partial unique index to `0001`**

In `backend_migrations/versions/0001_initial_schema.py`, find the block that creates `uq_one_active_version_per_tenant` at lines 424-429:

```python
    # Enforce at most one active version per tenant
    op.execute("""
        CREATE UNIQUE INDEX uq_one_active_version_per_tenant
            ON aegis_meta.metadata_versions (tenant_id)
            WHERE (status = 'active')
    """)
```

Immediately after this block (before the WORM trigger block at line 431), add the new index:

```python
    # Enforce at most one active version per tenant
    op.execute("""
        CREATE UNIQUE INDEX uq_one_active_version_per_tenant
            ON aegis_meta.metadata_versions (tenant_id)
            WHERE (status = 'active')
    """)

    # Prevent WORM audit chain forks under concurrent admin writes.
    # Two concurrent transactions could otherwise read the same chain tip
    # and commit different rows pointing at the same previous_hash, branching
    # the audit history. The partial unique index rejects the second writer;
    # the application retries against the new tip (see app/api/compiler.py
    # and app/api/router.py). The index is partial so the genesis row
    # (previous_hash = '') is exempt from uniqueness.
    op.execute("""
        CREATE UNIQUE INDEX uq_audit_previous_hash_nonempty
            ON aegis_meta.metadata_audit (previous_hash)
            WHERE previous_hash != ''
    """)
```

Also update the `downgrade()` function. Find the existing drop at line 449:

```python
def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_one_active_version_per_tenant"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_audit_worm ON aegis_meta.metadata_audit"
    )
```

Add the matching drop for the new index:

```python
def downgrade() -> None:
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_audit_previous_hash_nonempty"
    )
    op.execute(
        "DROP INDEX IF EXISTS aegis_meta.uq_one_active_version_per_tenant"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_audit_worm ON aegis_meta.metadata_audit"
    )
```

- [ ] **Step 2: Drop and rebuild the DB to verify the index is created**

Per `feedback_bird_reset_procedure.md`:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d
```

Wait for the health check, then verify the index exists and is partial:

```bash
uv run python -c "
import asyncio, os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def check():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    async with engine.begin() as conn:
        result = await conn.execute(text(
            \"SELECT indexdef FROM pg_indexes \"
            \"WHERE schemaname='aegis_meta' \"
            \"AND indexname='uq_audit_previous_hash_nonempty'\"
        ))
        rows = result.fetchall()
        assert len(rows) == 1, f'expected 1 row, got {rows}'
        indexdef = rows[0][0]
        assert 'UNIQUE' in indexdef
        assert 'previous_hash' in indexdef
        assert 'WHERE' in indexdef
        print('OK:', indexdef)

asyncio.run(check())
"
```
Expected: prints the index definition with UNIQUE and WHERE clauses.

- [ ] **Step 3: Commit**

```bash
git add backend_migrations/versions/0001_initial_schema.py
git commit -m "$(cat <<'EOF'
fix(schema): partial unique index on metadata_audit.previous_hash (#4)

Code-review finding #4 (2026-04-07) backstop: prevents WORM chain forks
at the database level. Two concurrent admin transactions could
otherwise read the same chain tip and commit different rows pointing at
the same previous_hash, branching the audit history.

The partial unique index uq_audit_previous_hash_nonempty rejects the
second writer with IntegrityError; Task 4.2 adds the application-side
retry loop that re-reads the chain tip and tries again. The index is
partial (WHERE previous_hash != '') so the genesis row is exempt.

Per the "no new migrations" rule, this is added directly to
0001_initial_schema.py rather than as a new migration file.

EOF
)"
```

## Task 4.2: Extract audit-chain classifier and scope retry to real collisions

**Why the shape of this task changed after the first review pass:** the reviewer correctly flagged three problems with the initial approach:
1. The test I wrote drove an *inline* retry helper that had no relationship to production code. The test could stay green while `compiler.py` and `router.py` were still wrong.
2. The retry loop caught *every* `IntegrityError` and retried it. A genuine `compiled_registry_artifacts.version_id` UNIQUE violation, or any FK violation, would be retried four pointless times and then returned as "audit chain contention" — hiding the real signal.
3. The `update_version_status` retry loop re-set `version.status`, `change_reason`, and `existing_active.status` inside the loop, but *not* `version.approved_by` / `version.approved_at` (which are set before the audit block at `router.py:676-677`). On rollback those fields revert to NULL; a successful retry activates a version with missing approval metadata.

All three are addressed by:
- Extracting a small pure module `app/audit/append.py` with two classifier functions that read `exc.orig.constraint_name` (asyncpg path) with a string-parsing fallback for SQLite. Both production call sites and the unit tests consume the same classifiers.
- Scoping the retry to `is_audit_chain_collision(exc)` only. Anything else re-raises immediately.
- Moving *every* field-assignment that was pre-loop in the original code (including `approved_by`/`approved_at`) into the per-attempt setup.
- Adding a focused integration test in `tests/test_audit_chain.py` that patches `session.commit()` to raise an audit-chain `IntegrityError` on the first call and succeed on the second — driving real `MetadataCompiler.compile_version` and asserting the retry happens.

**Files:**
- Create: `app/audit/append.py` — collision classifiers
- Create: `tests/test_audit_append.py` — unit tests for classifiers
- Modify: `app/api/compiler.py:208-249` (`compile_version` audit append + commit)
- Modify: `app/api/router.py:660-778` (`update_version_status` pre-activation setup + audit block + commit)
- Create: `tests/test_audit_chain.py` — integration test driving real production retry path

- [ ] **Step 1: Create `app/audit/append.py` with the classifiers**

Create `app/audit/append.py`:

```python
"""Audit chain append helpers.

Provides error classification for the WORM audit chain's partial unique
indexes, so retry logic in MetadataCompiler.compile_version and the
status-transition handler in router.update_version_status can
distinguish audit-chain contention (retry internally) from genuine
activation races (return 409 to the client) and from unrelated
IntegrityError causes (propagate as 5xx).

The two index names this module knows about are both declared in
backend_migrations/versions/0001_initial_schema.py:
  - uq_audit_previous_hash_nonempty — partial unique on
    metadata_audit.previous_hash, enforces WORM chain linearity
  - uq_one_active_version_per_tenant — partial unique on
    metadata_versions(tenant_id) WHERE status='active', enforces the
    single-active-version invariant

Production runs against asyncpg, where IntegrityError.orig is a
UniqueViolationError that exposes the violated constraint via the
.constraint_name attribute — that's the canonical path.

Tests run against sqlite3, where IntegrityError.orig is a
sqlite3.IntegrityError whose args[0] looks like
``UNIQUE constraint failed: <table>.<column>`` (verified empirically:
sqlite never includes the index name in its message). For the two
indexes this module cares about, the (table, column) pair is unique
across the schema in 0001_initial_schema.py — there is no other
unique constraint on metadata_audit.previous_hash or
metadata_versions.tenant_id — so matching the table.column substring
is unambiguous in practice. If a future schema change adds a second
unique constraint on either of those columns, this fallback will need
to be revisited.
"""
from sqlalchemy.exc import IntegrityError

AUDIT_CHAIN_INDEX_NAME = "uq_audit_previous_hash_nonempty"
ACTIVATION_INDEX_NAME = "uq_one_active_version_per_tenant"

# sqlite3.IntegrityError reports UNIQUE violations as
# "UNIQUE constraint failed: <table>.<column>" (verified via the
# Python sqlite3 module against an in-memory DB; the index name is
# NOT included in the error text). These constants are the
# table.column substrings the SQLite fallback matches against.
_SQLITE_AUDIT_CHAIN_HINT = "metadata_audit.previous_hash"
_SQLITE_ACTIVATION_HINT = "metadata_versions.tenant_id"


def _extract_constraint_name(exc: IntegrityError) -> str | None:
    """Return the canonical violated index name from an IntegrityError.

    Two paths:

    1. asyncpg path (production): IntegrityError.orig is a
       UniqueViolationError exposing .constraint_name directly. We
       return that name verbatim.

    2. sqlite3 path (tests): IntegrityError.orig is a
       sqlite3.IntegrityError whose args[0] is
       ``UNIQUE constraint failed: <table>.<column>``. We map that
       table.column substring to the canonical index name so callers
       can compare against AUDIT_CHAIN_INDEX_NAME / ACTIVATION_INDEX_NAME
       regardless of which driver path was taken.

    Returns None if no known constraint can be identified — callers
    should treat that as "unknown origin, do not retry".
    """
    orig = getattr(exc, "orig", None)
    if orig is None:
        return None

    # asyncpg.UniqueViolationError (and other asyncpg errors) expose
    # constraint_name directly. This is the production path.
    name = getattr(orig, "constraint_name", None)
    if name:
        return str(name)

    # sqlite3 fallback: parse args[0] for the known table.column hints.
    # The fallback returns the canonical index name (not the message
    # substring) so the rest of the module is driver-agnostic.
    args = getattr(orig, "args", ())
    if args and isinstance(args[0], str):
        msg = args[0]
        if _SQLITE_AUDIT_CHAIN_HINT in msg:
            return AUDIT_CHAIN_INDEX_NAME
        if _SQLITE_ACTIVATION_HINT in msg:
            return ACTIVATION_INDEX_NAME
    return None


def is_audit_chain_collision(exc: IntegrityError) -> bool:
    """True if `exc` is specifically a uq_audit_previous_hash_nonempty
    violation. Used by retry loops to decide whether to re-read the
    chain tip and try again."""
    return _extract_constraint_name(exc) == AUDIT_CHAIN_INDEX_NAME


def is_activation_collision(exc: IntegrityError) -> bool:
    """True if `exc` is specifically a uq_one_active_version_per_tenant
    violation. Used by update_version_status to return 409 on genuine
    activation races rather than retrying them."""
    return _extract_constraint_name(exc) == ACTIVATION_INDEX_NAME
```

- [ ] **Step 2: Write unit tests for the classifiers**

Create `tests/test_audit_append.py`:

```python
"""Unit tests for app.audit.append collision classifiers.

Code-review finding #2 / #4 (2026-04-07): the retry loops must scope
themselves to the specific audit-chain constraint, not to every
IntegrityError. These tests pin down the classifier behavior across
both the asyncpg path (production) and the sqlite3 path (tests).
"""
import sqlite3
from unittest.mock import MagicMock

from sqlalchemy.exc import IntegrityError

from app.audit.append import (
    ACTIVATION_INDEX_NAME,
    AUDIT_CHAIN_INDEX_NAME,
    is_activation_collision,
    is_audit_chain_collision,
)


def _fake_asyncpg_error(constraint: str) -> IntegrityError:
    """Build an IntegrityError whose .orig mimics asyncpg.UniqueViolationError."""
    orig = MagicMock()
    orig.constraint_name = constraint
    # Ensure args fallback won't accidentally match — only the structured
    # attribute should matter for this path.
    orig.args = ("unrelated message",)
    return IntegrityError("stmt", None, orig)


def _fake_sqlite_error(message: str) -> IntegrityError:
    """Build an IntegrityError whose .orig is a real sqlite3.IntegrityError."""
    orig = sqlite3.IntegrityError(message)
    return IntegrityError("stmt", None, orig)


# --------------------------------------------------------------------
# is_audit_chain_collision
# --------------------------------------------------------------------

def test_audit_chain_collision_asyncpg_structured_constraint_name() -> None:
    exc = _fake_asyncpg_error(AUDIT_CHAIN_INDEX_NAME)
    assert is_audit_chain_collision(exc) is True


def test_audit_chain_collision_sqlite_message_format() -> None:
    """sqlite3 reports UNIQUE violations as the literal text
    'UNIQUE constraint failed: <table>.<column>' — verified
    empirically against an in-memory sqlite3 database. The index
    name is NOT in the message, so the classifier must map the
    table.column substring to the canonical index name."""
    exc = _fake_sqlite_error(
        "UNIQUE constraint failed: metadata_audit.previous_hash"
    )
    assert is_audit_chain_collision(exc) is True


def test_audit_chain_collision_rejects_activation_constraint() -> None:
    """A different partial unique index must not be misclassified as
    audit-chain contention."""
    exc = _fake_asyncpg_error(ACTIVATION_INDEX_NAME)
    assert is_audit_chain_collision(exc) is False


def test_audit_chain_collision_rejects_unrelated_integrity_error() -> None:
    """A generic FK or NOT NULL violation must not trigger audit-chain
    retry — that would hide real errors behind 503 contention messages."""
    exc = _fake_asyncpg_error("metadata_columns_version_id_fkey")
    assert is_audit_chain_collision(exc) is False


def test_audit_chain_collision_rejects_error_with_no_orig() -> None:
    """Defensive: IntegrityError constructed without .orig must not
    trigger any classification."""
    exc = IntegrityError("stmt", None, Exception("bare exception"))
    assert is_audit_chain_collision(exc) is False


# --------------------------------------------------------------------
# is_activation_collision
# --------------------------------------------------------------------

def test_activation_collision_asyncpg_structured_constraint_name() -> None:
    exc = _fake_asyncpg_error(ACTIVATION_INDEX_NAME)
    assert is_activation_collision(exc) is True


def test_activation_collision_sqlite_message_format() -> None:
    """sqlite3 reports the activation race as
    'UNIQUE constraint failed: metadata_versions.tenant_id'
    (the partial index's WHERE clause is not in the message).
    The classifier must map this to ACTIVATION_INDEX_NAME."""
    exc = _fake_sqlite_error(
        "UNIQUE constraint failed: metadata_versions.tenant_id"
    )
    assert is_activation_collision(exc) is True


def test_audit_chain_collision_rejects_unrelated_sqlite_table_column() -> None:
    """A sqlite3 message naming a different table.column must NOT be
    classified as an audit-chain collision — the fallback is narrow."""
    exc = _fake_sqlite_error(
        "UNIQUE constraint failed: metadata_columns.alias"
    )
    assert is_audit_chain_collision(exc) is False
    assert is_activation_collision(exc) is False


def test_activation_collision_rejects_audit_chain_constraint() -> None:
    exc = _fake_asyncpg_error(AUDIT_CHAIN_INDEX_NAME)
    assert is_activation_collision(exc) is False


def test_activation_collision_rejects_unrelated_integrity_error() -> None:
    exc = _fake_asyncpg_error("some_other_unique_constraint")
    assert is_activation_collision(exc) is False
```

- [ ] **Step 3: Run the unit tests to verify the classifiers work**

Run: `uv run pytest tests/test_audit_append.py -v`
Expected: all 11 tests PASS. These are pure-Python tests with no DB dependency.

Then run a one-shot empirical check that the SQLite message format the test assumes is the format sqlite3 actually emits. This is cheap insurance against the format ever drifting:

```bash
uv run python -c "
import sqlite3
con = sqlite3.connect(':memory:')
con.execute('CREATE TABLE t (a TEXT)')
con.execute(\"CREATE UNIQUE INDEX uq_test ON t(a) WHERE a != ''\")
con.execute(\"INSERT INTO t VALUES ('x')\")
try:
    con.execute(\"INSERT INTO t VALUES ('x')\")
except sqlite3.IntegrityError as e:
    print('args[0]:', repr(e.args[0]))
    assert 't.a' in e.args[0], 't.a substring missing — sqlite3 format changed'
    assert 'uq_test' not in e.args[0], (
        'sqlite3 unexpectedly included the index name — fallback can be tightened'
    )
    print('OK: sqlite3 format matches the fallback assumption')
"
```
Expected: prints `args[0]: 'UNIQUE constraint failed: t.a'` then `OK: ...`. If this fails, the SQLite format has changed and `_extract_constraint_name` needs updating before the unit tests will be reliable.

- [ ] **Step 4: Wrap the audit append in `compile_version` with scoped retry**

In `app/api/compiler.py`, find the block from "4. Sign and Compute Hash Payload" (around line 180) through `await session.commit()` (around line 248). The current shape computes `final_hash`/`signature` once, does `delete + add artifact`, then appends the audit row and commits.

Restructure so that **only the hash and signature computation stays outside the retry loop** (they're deterministic given the payload — recomputing is wasted work), while `delete + add artifact + chain-tip read + audit row + version-metadata update + commit` all live inside the loop. On `IntegrityError`, classify via `is_audit_chain_collision`; retry only audit-chain collisions, propagate everything else.

Add the import at the top of `app/api/compiler.py`:

```python
from sqlalchemy.exc import IntegrityError

from app.audit.append import is_audit_chain_collision
```

(Verify with `grep -n "^from sqlalchemy\|^from app.audit" app/api/compiler.py` to avoid duplicate imports.)

Then replace the block from "4. Sign and Compute Hash Payload" (line ~180 — the comment marking step 4) through `return artifact` (line ~249) with:

```python
        # 4. Sign and Compute Hash Payload (deterministic — runs once)
        canonical_payload = get_canonical_json(payload)
        final_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

        secrets_mgr = get_secrets_manager()
        current_key_id = secrets_mgr.get_current_signing_key_id()
        signing_key = secrets_mgr.get_signing_key(current_key_id)
        signature = compute_artifact_hmac_signature(signing_key, canonical_payload)

        # 5. Retry loop for audit-chain contention.
        #
        # The partial unique index uq_audit_previous_hash_nonempty on
        # metadata_audit.previous_hash (see 0001_initial_schema.py) rejects
        # two concurrent rows pointing at the same tip. On collision we
        # roll back, re-read the tip, and rebuild the artifact + audit row
        # from scratch. We retry ONLY that specific constraint — any other
        # IntegrityError (e.g. an unrelated FK/UNIQUE violation) propagates
        # immediately so the caller sees the real error.
        #
        # CACHED SCALARS (reviewer pass 3): AsyncSession.rollback() expires
        # all loaded ORM attributes — not just on commit, but on any
        # rollback regardless of expire_on_commit. After attempt 1's
        # rollback, a plain read of `version.version_id`, `version.tenant_id`,
        # or `version.status` would trigger an implicit refresh that needs
        # to await a SELECT, but we are in plain attribute-access code with
        # no greenlet context — the result is a `MissingGreenlet` exception
        # before attempt 2 even reaches commit. Verified empirically: even
        # the primary key fails to read after rollback.
        #
        # The fix: read every scalar we need from `version` ONCE, before the
        # loop, and use the locals inside the loop. ASSIGNMENTS to expired
        # attributes are still safe (they just mark the attribute dirty),
        # so version.registry_hash / version.approved_by / version.approved_at
        # can stay as ORM mutations.
        cached_version_id = version.version_id
        cached_tenant_id = version.tenant_id
        cached_version_was_active = version.status == "active"

        last_exc: IntegrityError | None = None
        for attempt in range(5):
            # Delete any prior artifact for this version (idempotent on retry).
            await session.execute(
                delete(CompiledRegistryArtifact).where(
                    CompiledRegistryArtifact.version_id == cached_version_id
                )
            )

            # Build a fresh artifact each attempt — rollback detaches the
            # previous one, and constructing a new object is cheaper than
            # reasoning about SQLAlchemy re-attachment semantics.
            artifact = CompiledRegistryArtifact(
                version_id=cached_version_id,
                tenant_id=cached_tenant_id,
                artifact_blob=payload,
                artifact_hash=final_hash,
                compiler_version="1.0.0",
                signature=signature,
                signature_key_id=current_key_id,
            )
            session.add(artifact)

            # Read the current chain tip and build the audit row.
            last_audit_res = await session.execute(
                select(MetadataAudit).order_by(
                    MetadataAudit.timestamp.desc(),
                    MetadataAudit.audit_id.desc()
                ).limit(1)
            )
            last_row = last_audit_res.scalar_one_or_none()
            previous_hash = last_row.row_hash if last_row else ""

            audit_timestamp_native = datetime.now(UTC)
            audit_payload = {
                "event": "compile_version",
                "version_id": str(cached_version_id),
                "artifact_hash": final_hash,
                "signature_key_id": current_key_id,
                "status": "SUCCESS",
            }
            audit_canonical = get_canonical_json(audit_payload)
            new_row_hash = compute_audit_row_hash(
                previous_hash, audit_canonical,
                audit_timestamp_native.isoformat(),
            )
            session.add(
                MetadataAudit(
                    version_id=cached_version_id,
                    actor=actor,
                    action="deploy",
                    payload=audit_payload,
                    timestamp=audit_timestamp_native,
                    previous_hash=previous_hash,
                    row_hash=new_row_hash,
                    key_id=current_key_id,
                )
            )

            # 6. Lock the hash trace dynamically to the version object.
            # registry_hash always updates; approved_by/approved_at only
            # update when the version is already active (see Task 2.1's
            # preview-compile semantics fix). These are assignments, not
            # reads — safe on expired ORM objects.
            version.registry_hash = final_hash
            if cached_version_was_active:
                version.approved_by = actor
                version.approved_at = datetime.now(UTC)

            try:
                await session.commit()
                return artifact
            except IntegrityError as exc:
                await session.rollback()
                if not is_audit_chain_collision(exc):
                    # Not an audit-chain collision — this is a real integrity
                    # error and the caller should see it unchanged.
                    raise
                last_exc = exc
                # Fall through to the next iteration, which re-reads the
                # chain tip and tries again.
                continue

        # Exhausted all retries — surface the last audit-chain collision
        # as-is so the caller can distinguish it from single-attempt errors.
        assert last_exc is not None
        raise last_exc
```

The critical invariants:
- `is_audit_chain_collision(exc)` gates the retry (finding #2 fix).
- Every piece of state that `rollback()` drops — the artifact object, the audit row, the `version.registry_hash` / `version.approved_*` attribute writes — is re-staged inside the loop on each attempt.
- **Scalar reads from `version` happen ONCE before the loop** and are passed in as locals on each retry. Assignments to expired ORM attributes are safe; reads are not. (Reviewer pass 3 finding #1.)
- The final branch after the loop re-raises the **actual** `IntegrityError` rather than wrapping it in a synthetic `RuntimeError`, so operators see the real driver message when contention exhausts all five attempts.

- [ ] **Step 5: Wrap the audit append in `update_version_status` with scoped retry and metadata restore**

In `app/api/router.py`, the existing code at lines 654-777 has this shape (reading the file directly):
1. Lines 654-672: Compute `previous_status`, fetch `existing_active`
2. Lines 676-677: Set `version.approved_by`, `version.approved_at` (inside `if payload.status == "active"`)
3. Lines 680-682: Set `version.status`, `version.change_reason`
4. Lines 684-778: Audit chain — fetch tip, build optional archive row, build main transition row, commit

The reviewer correctly flagged that the initial retry-loop draft only re-set `version.status`, `change_reason`, and `existing_active.status` inside the loop. On rollback, `approved_by`/`approved_at` (lines 676-677) are also reverted — but they weren't re-set on retry, so a successful retry activated the version with NULL approval metadata.

The restructure: `existing_active` is computed **once** before the loop (it's a snapshot of the current DB state, not something we want to re-fetch per attempt). *All* version-mutation assignments go inside the loop. Classification uses the new helpers; the activation constraint is detected by `is_activation_collision` (structured attribute access, not substring matching).

Add the import at the top of `app/api/router.py`:

```python
from app.audit.append import is_activation_collision, is_audit_chain_collision
```

(`IntegrityError` is already imported from Task 1.2 — verify with `grep -n "IntegrityError" app/api/router.py`.)

Then replace lines 654-778 (from `previous_status = version.status` through the existing `try: await session.commit() except IntegrityError ...` activation-conflict handler) with:

```python
    previous_status = version.status

    # CACHED SCALARS (reviewer pass 3, findings #1 + #3): cache the
    # ORM scalar reads we'll need across retries BEFORE the loop.
    # AsyncSession.rollback() expires loaded attributes; reading
    # version.created_at or existing_active.version_id on attempt 2
    # would crash with MissingGreenlet. Verified empirically against
    # an in-memory aiosqlite session.
    cached_version_created_at_iso = version.created_at.isoformat()

    # ------------------------------------------------------------------
    # Pre-activation snapshot (runs once; doesn't depend on retry state)
    # ------------------------------------------------------------------
    # existing_active is a snapshot of the current DB state. If another
    # transaction modifies it concurrently, the uq_one_active_version_per_tenant
    # index on commit will detect the race and we surface a 409.
    existing_active: MetadataVersion | None = None
    cached_existing_active_id: uuid.UUID | None = None
    if payload.status == "active":
        existing_active_res = await session.execute(
            select(MetadataVersion).where(
                MetadataVersion.tenant_id == cred.tenant_id,
                MetadataVersion.status == "active",
                MetadataVersion.version_id != version_id,
            )
        )
        existing_active = existing_active_res.scalars().first()
        if existing_active is not None:
            # Cache the ID before the loop. existing_active.version_id
            # would expire on rollback and crash on attempt 2 when the
            # archival audit row is built.
            cached_existing_active_id = existing_active.version_id

    secrets_mgr = get_secrets_manager()

    # ------------------------------------------------------------------
    # WORM audit chain — with scoped retry on audit-chain contention
    # ------------------------------------------------------------------
    # Three possible commit outcomes and how we handle each:
    #   1. success                              → return 200
    #   2. uq_audit_previous_hash_nonempty      → retry (rollback + rebuild)
    #   3. uq_one_active_version_per_tenant     → 409 to client, no retry
    #   4. any other IntegrityError             → propagate unchanged (5xx)
    #
    # Every version-mutation below must be inside this loop because
    # session.rollback() reverts all attribute assignments on managed
    # objects. If approved_by/approved_at were set outside the loop, a
    # successful retry would activate the version with NULL approval
    # metadata (reviewer pass 2 finding #3).
    #
    # ORDERING NOTE (reviewer pass 3 finding #2): the chain-tip select
    # MUST run BEFORE any ORM mutations. Default autoflush=True means
    # the SELECT would otherwise flush our pending version.status /
    # existing_active.status changes, which can fire the partial unique
    # index uq_one_active_version_per_tenant during the flush — raising
    # an IntegrityError from `await session.execute(...)` BEFORE the
    # try/except around session.commit(). The handler would then return
    # an unclassified 500 instead of the intended 409 or audit-chain
    # retry. Verified empirically: a pending mutation that would
    # violate a partial unique index causes the next ORM-style select
    # to raise "IntegrityError (raised as a result of Query-invoked
    # autoflush)". Doing the select first dodges the issue entirely.
    for attempt in range(5):
        # 1. READ chain tip BEFORE any ORM mutations. With no pending
        #    changes from this iteration, autoflush is a no-op and the
        #    select can never trigger the activation index check.
        last_audit_res = await session.execute(
            select(MetadataAudit)
            .order_by(
                MetadataAudit.timestamp.desc(),
                MetadataAudit.audit_id.desc(),
            )
            .limit(1)
        )
        last_row = last_audit_res.scalar_one_or_none()
        chain_tip = last_row.row_hash if last_row else ""

        # 2. NOW apply version mutations. ASSIGNMENTS to expired ORM
        #    attributes are safe — they just mark the attribute dirty.
        #    Only READS would trigger MissingGreenlet.
        version.status = payload.status
        if payload.reason:
            version.change_reason = payload.reason
        if payload.status == "active":
            version.approved_by = cred.user_id
            version.approved_at = datetime.now(UTC)
        if existing_active is not None:
            existing_active.status = "archived"

        # 3. If superseding an active version, stage its archival audit
        #    row and thread the chain tip forward. Uses
        #    cached_existing_active_id rather than
        #    existing_active.version_id, which would crash on attempt 2
        #    after a rollback expired the ORM object.
        if cached_existing_active_id is not None:
            archive_ts = datetime.now(UTC)
            archive_payload = {
                "event": "status_transition",
                "version_id": str(cached_existing_active_id),
                "from_status": "active",
                "to_status": "archived",
                "reason": "Superseded by activation of a newer version",
                "status": "SUCCESS",
            }
            archive_canonical = get_canonical_json(archive_payload)
            archive_row_hash = compute_audit_row_hash(
                chain_tip, archive_canonical, archive_ts.isoformat()
            )
            session.add(
                MetadataAudit(
                    version_id=cached_existing_active_id,
                    actor=cred.user_id,
                    action="revoke",
                    payload=archive_payload,
                    timestamp=archive_ts,
                    previous_hash=chain_tip,
                    row_hash=archive_row_hash,
                    key_id=secrets_mgr.get_current_signing_key_id(),
                    credential_id=cred.credential_id,
                )
            )
            chain_tip = archive_row_hash

        # 4. Main transition audit row. version_id is the route parameter
        #    (not version.version_id), so it's a local from the start
        #    and never expires.
        audit_timestamp = datetime.now(UTC)
        audit_action = _TRANSITION_AUDIT_ACTION[
            (previous_status, payload.status)
        ]
        audit_payload_data = {
            "event": "status_transition",
            "version_id": str(version_id),
            "from_status": previous_status,
            "to_status": payload.status,
            "reason": payload.reason,
            "status": "SUCCESS",
        }
        audit_canonical = get_canonical_json(audit_payload_data)
        new_row_hash = compute_audit_row_hash(
            chain_tip, audit_canonical, audit_timestamp.isoformat()
        )
        session.add(
            MetadataAudit(
                version_id=version_id,
                actor=cred.user_id,
                action=audit_action,
                payload=audit_payload_data,
                timestamp=audit_timestamp,
                previous_hash=chain_tip,
                row_hash=new_row_hash,
                key_id=secrets_mgr.get_current_signing_key_id(),
                credential_id=cred.credential_id,
            )
        )

        try:
            await session.commit()
            # 5. Success-path return uses cached/route-level values
            #    rather than reading version.*. Even though the session
            #    has expire_on_commit=False (so version.* would survive
            #    a successful commit), using cached values keeps the
            #    retry-vs-success paths symmetric and prevents the
            #    return path from regressing if the session config
            #    ever changes.
            return ProtocolMetadataVersion(
                version_id=str(version_id),
                status=payload.status,
                created_at=cached_version_created_at_iso,
            )
        except IntegrityError as exc:
            await session.rollback()

            # Activation race: genuine concurrent activation. Return 409
            # immediately — this is NOT a retry-able condition, the client
            # should verify the current active version and retry itself.
            if is_activation_collision(exc):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Activation conflict: another version became"
                        " active concurrently. Retry the request after"
                        " verifying the current active version."
                    ),
                ) from exc

            # Audit-chain race: retry internally.
            if is_audit_chain_collision(exc):
                if attempt == 4:
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Audit chain contention: failed to append"
                            " after retries. Try again."
                        ),
                    ) from exc
                continue

            # Any other IntegrityError is a genuine data problem that
            # the caller should see unchanged — do NOT wrap it in a
            # contention message.
            raise

    # Unreachable in practice — the loop either returns or raises on
    # every path — but keeps mypy happy.
    raise HTTPException(
        status_code=500,
        detail="Audit append loop exited without committing.",
    )
```

The key differences from the original (broken) draft:
- `version.approved_by` and `version.approved_at` are now inside the loop (fix for reviewer pass 2 finding #3).
- Classification uses `is_activation_collision` and `is_audit_chain_collision` from `app.audit.append` — structured attribute access via `exc.orig.constraint_name`, not substring matching (fix for reviewer pass 2 findings #2 and #4).
- Non-audit, non-activation `IntegrityError` propagates unchanged (fix for reviewer pass 2 finding #2). The initial draft would have caught it and retried pointlessly.
- **Cached `cached_existing_active_id` and `cached_version_created_at_iso` BEFORE the loop** — `AsyncSession.rollback()` expires loaded attributes, and a plain read on attempt 2 would raise `MissingGreenlet` (reviewer pass 3 findings #1 + #3, verified empirically).
- **Chain-tip SELECT runs BEFORE any version/existing_active mutations** — with mutations pending, default autoflush would otherwise fire the partial unique index `uq_one_active_version_per_tenant` during the SELECT, raising an IntegrityError from outside our try/except (reviewer pass 3 finding #2, verified empirically with `IntegrityError (raised as a result of Query-invoked autoflush)`).
- Objects don't need explicit `session.add(version)` after rollback — they're still managed objects (loaded via `session.execute(select(...))`); the next attribute assignment re-enters the unit of work automatically. The initial draft's `session.add(version)` lines inside the retry branch were unnecessary and have been removed.

- [ ] **Step 6: Write a focused integration test driving the real retry path**

Create `tests/test_audit_chain.py`:

```python
"""Integration test for the WORM audit-chain retry loops.

Code-review finding #4 + reviewer's follow-up #1 (2026-04-07): the
original test built an inline retry helper and tested the helper, not
production code — the test could stay green while compiler.py and
router.py were still broken.

This replacement drives the REAL retry path by patching session.commit()
to raise an audit-chain IntegrityError on the first call and succeed
on the second call, then invoking MetadataCompiler.compile_version
through a fully-mocked session graph. We assert that:

  1. commit() was called twice (proves the retry happened)
  2. rollback() was called once (proves the retry cleaned up correctly)
  3. The function returned a non-None artifact (proves the second
     attempt succeeded)

This test cannot prove "the retry only catches audit-chain collisions
and not other IntegrityError causes" — that's what
tests/test_audit_append.py::test_audit_chain_collision_* does, and
together they constitute the regression suite for finding #4.
"""
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from app.api.compiler import MetadataCompiler
from app.audit.append import AUDIT_CHAIN_INDEX_NAME


def _make_audit_chain_integrity_error() -> IntegrityError:
    """Build an IntegrityError that is_audit_chain_collision classifies
    as an audit-chain collision."""
    orig = MagicMock()
    orig.constraint_name = AUDIT_CHAIN_INDEX_NAME
    return IntegrityError("INSERT ...", None, orig)


@pytest.mark.asyncio
async def test_compile_version_retries_on_audit_chain_collision() -> None:
    """MetadataCompiler.compile_version must retry when session.commit()
    raises an IntegrityError whose constraint is the audit chain's
    partial unique index. A second commit attempt that succeeds should
    produce a returned artifact."""
    # Build a minimal mock version graph. The real code uses selectinload
    # for the version's tables/columns/edges; a MagicMock with empty
    # collections satisfies it.
    version_id = uuid.uuid4()

    mock_version = MagicMock()
    mock_version.version_id = version_id
    mock_version.tenant_id = "test_tenant"
    mock_version.status = "active"  # triggers approved_by/at assignment
    mock_version.tables = []  # no tables → empty artifact payload
    mock_version.edges = []
    mock_version.registry_hash = None

    # session.execute is called for: (1) version SELECT, (2) chain tip
    # SELECT on each attempt, (3) the delete-artifact statement on each
    # attempt. Discriminate by the rendered table name in the statement
    # — SQLAlchemy emits the actual table name (e.g. metadata_versions),
    # NOT the ORM class name. Matching on "MetadataVersion" in str(stmt)
    # would never fire and the test would silently take the default
    # branch for every call.
    version_result = MagicMock()
    version_result.scalar_one_or_none.return_value = mock_version

    tip_result = MagicMock()
    tip_result.scalar_one_or_none.return_value = None  # genesis

    delete_result = MagicMock()

    async def fake_execute(stmt: Any) -> MagicMock:
        sql = str(stmt).lower()
        # Order matters: the audit chain tip read targets metadata_audit
        # only, while the version select targets metadata_versions only.
        # The artifact delete targets compiled_registry_artifacts.
        if "metadata_audit" in sql:
            return tip_result
        if "metadata_versions" in sql:
            return version_result
        return delete_result

    # session.commit raises on the first call, succeeds on the second.
    commit_calls = [0]

    async def fake_commit() -> None:
        commit_calls[0] += 1
        if commit_calls[0] == 1:
            raise _make_audit_chain_integrity_error()
        return None

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=fake_execute)
    mock_session.commit = AsyncMock(side_effect=fake_commit)
    mock_session.rollback = AsyncMock()
    mock_session.add = MagicMock()

    # Call the real production function.
    artifact = await MetadataCompiler.compile_version(
        session=mock_session,
        version_id=version_id,
        actor="test_actor",
    )

    # Assertions on real production behavior.
    assert commit_calls[0] == 2, (
        f"Expected compile_version to retry and call commit twice;"
        f" got {commit_calls[0]} calls. The retry loop is not wired up"
        f" to the audit-chain classifier."
    )
    assert mock_session.rollback.await_count >= 1, (
        "Expected rollback to be called after the first IntegrityError;"
        " the retry loop is not cleaning up the failed attempt."
    )
    assert artifact is not None


@pytest.mark.asyncio
async def test_compile_version_does_not_retry_on_unrelated_integrity_error() -> None:
    """MetadataCompiler.compile_version must NOT retry when the
    IntegrityError is for a constraint OTHER than the audit chain.
    Unrelated errors must propagate immediately so the caller sees the
    real cause."""
    version_id = uuid.uuid4()

    mock_version = MagicMock()
    mock_version.version_id = version_id
    mock_version.tenant_id = "test_tenant"
    mock_version.status = "active"
    mock_version.tables = []
    mock_version.edges = []

    version_result = MagicMock()
    version_result.scalar_one_or_none.return_value = mock_version
    tip_result = MagicMock()
    tip_result.scalar_one_or_none.return_value = None

    async def fake_execute(stmt: Any) -> MagicMock:
        # Match on the rendered table name (SQLAlchemy emits real table
        # names, not ORM class names — see comment in the previous test).
        sql = str(stmt).lower()
        if "metadata_audit" in sql:
            return tip_result
        if "metadata_versions" in sql:
            return version_result
        return MagicMock()

    # Raise a DIFFERENT IntegrityError — e.g. a fake FK violation.
    fk_orig = MagicMock()
    fk_orig.constraint_name = "metadata_columns_version_id_fkey"
    fk_orig.args = ("unrelated fk violation",)
    fk_exc = IntegrityError("INSERT ...", None, fk_orig)

    commit_calls = [0]

    async def fake_commit() -> None:
        commit_calls[0] += 1
        raise fk_exc  # every call raises

    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(side_effect=fake_execute)
    mock_session.commit = AsyncMock(side_effect=fake_commit)
    mock_session.rollback = AsyncMock()
    mock_session.add = MagicMock()

    # The unrelated IntegrityError must propagate — NOT be retried.
    with pytest.raises(IntegrityError) as exc_info:
        await MetadataCompiler.compile_version(
            session=mock_session,
            version_id=version_id,
            actor="test_actor",
        )
    assert exc_info.value is fk_exc
    assert commit_calls[0] == 1, (
        f"Expected compile_version to NOT retry non-audit IntegrityError;"
        f" got {commit_calls[0]} commit attempts. A genuine FK/UNIQUE"
        f" violation is being masked as audit-chain contention."
    )
```

- [ ] **Step 7: Add real-AsyncSession regression tests for ORM expiration**

The mocked tests above prove the retry loop is wired to `is_audit_chain_collision` and that the classifier discriminates correctly, but they cannot catch *AsyncSession-specific bugs*: rollback expiring loaded ORM attributes, or autoflush firing the partial unique index on a SELECT that runs while mutations are pending. Both bugs were verified empirically in the third reviewer pass with minimal aiosqlite reproductions.

**Critical schema-setup constraint:** `Base.metadata.create_all()` does NOT work against SQLite for this project. `app/api/meta_models.py` declares columns with `JSONB` (a Postgres-only type), and SQLAlchemy's SQLite type compiler raises:

```
CompileError: ... can't render element of type JSONB
```

Verified empirically. The fix is to issue raw-SQL DDL via `text("CREATE TABLE ...")` — same pattern as `tests/conftest.py:179-219`. The TEXT columns we use in the raw DDL accept the ORM's bind parameters because SQLAlchemy's `UUID(as_uuid=True)` and `JSONB` types fall back to dialect-appropriate encoders when the underlying column type is permissive — the `UUID` type bind-processes to a hex string and the `JSONB` type bind-processes to a JSON string, both of which SQLite stores happily as TEXT and round-trips back to `uuid.UUID` / `dict` on read. **Both round-trips verified empirically** before writing this step.

Append a shared DDL helper plus two real-session tests to `tests/test_audit_chain.py`:

```python
async def _setup_metadata_schema_for_sqlite(conn: object) -> None:
    """Create the seven metadata tables that compile_version /
    update_version_status touch, using raw-SQL DDL that SQLite can
    actually compile.

    Why raw SQL: app/api/meta_models.py uses postgresql-specific JSONB
    columns and SQLAlchemy's SQLite type compiler refuses to render
    them, so Base.metadata.create_all() fails immediately. This helper
    issues SQLite-compatible CREATE TABLE statements with TEXT columns
    everywhere; the ORM's UUID and JSONB type processors fall back to
    string/JSON encoding when the storage type is permissive, so the
    round-trip via session.add() / session.execute(select(...)) works
    correctly. Verified empirically before this plan was written.

    The seven tables match the ones the compile_version selectinload
    chain and update_version_status SELECT touch:
      metadata_versions, metadata_tables, metadata_columns,
      metadata_column_values, metadata_relationships,
      compiled_registry_artifacts, metadata_audit
    """
    from sqlalchemy import text

    statements = [
        """CREATE TABLE metadata_versions (
            version_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            registry_hash TEXT,
            status TEXT NOT NULL DEFAULT 'draft',
            created_by TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            approved_by TEXT,
            approved_at TEXT,
            change_reason TEXT
        )""",
        """CREATE TABLE metadata_tables (
            table_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            tenant_id TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            source_database TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE metadata_columns (
            column_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            table_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            data_type TEXT NOT NULL,
            is_nullable INTEGER NOT NULL DEFAULT 1,
            is_primary_key INTEGER NOT NULL DEFAULT 0,
            is_unique INTEGER NOT NULL DEFAULT 0,
            is_sensitive INTEGER NOT NULL DEFAULT 0,
            allowed_in_select INTEGER NOT NULL DEFAULT 0,
            allowed_in_filter INTEGER NOT NULL DEFAULT 0,
            allowed_in_join INTEGER NOT NULL DEFAULT 0,
            safety_classification TEXT,
            sample_values TEXT,
            sample_values_exhaustive INTEGER NOT NULL DEFAULT 0,
            rag_enabled INTEGER NOT NULL DEFAULT 0,
            rag_cardinality_hint TEXT,
            rag_limit INTEGER,
            rag_sample_strategy TEXT,
            rag_order_by_column TEXT,
            rag_order_direction TEXT,
            refresh_on_compile INTEGER NOT NULL DEFAULT 0
        )""",
        """CREATE TABLE metadata_column_values (
            value_id TEXT PRIMARY KEY,
            column_id TEXT NOT NULL,
            version_id TEXT NOT NULL,
            value TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE metadata_relationships (
            relationship_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            source_table_id TEXT NOT NULL,
            source_column_id TEXT NOT NULL,
            target_table_id TEXT NOT NULL,
            target_column_id TEXT NOT NULL,
            relationship_type TEXT NOT NULL DEFAULT 'fk',
            cardinality TEXT NOT NULL DEFAULT '1:n',
            bidirectional INTEGER NOT NULL DEFAULT 1,
            active INTEGER NOT NULL DEFAULT 1
        )""",
        """CREATE TABLE compiled_registry_artifacts (
            artifact_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            artifact_blob TEXT NOT NULL DEFAULT '{}',
            artifact_hash TEXT NOT NULL,
            compiled_at TEXT DEFAULT CURRENT_TIMESTAMP,
            compiler_version TEXT NOT NULL DEFAULT '1.0.0',
            signature TEXT,
            signature_algo TEXT NOT NULL DEFAULT 'hmac-sha256-v1',
            signature_key_id TEXT
        )""",
        """CREATE TABLE metadata_audit (
            audit_id TEXT PRIMARY KEY,
            version_id TEXT,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            timestamp TEXT NOT NULL,
            previous_hash TEXT,
            row_hash TEXT NOT NULL UNIQUE,
            hash_algorithm TEXT NOT NULL DEFAULT 'sha256-v1',
            key_id TEXT,
            credential_id TEXT
        )""",
    ]
    for stmt in statements:
        await conn.execute(text(stmt))


@pytest.mark.asyncio
async def test_compile_version_real_session_retries_after_rollback() -> None:
    """Real AsyncSession integration test: force one audit-chain
    IntegrityError on the first commit attempt and verify the second
    attempt succeeds without crashing on expired ORM attributes.

    Reviewer pass 3 finding #1: AsyncSession.rollback() expires loaded
    ORM attributes regardless of expire_on_commit. A plain read of
    version.version_id / version.tenant_id / version.status on attempt 2
    would otherwise raise MissingGreenlet from the implicit refresh
    (verified empirically against an in-memory aiosqlite session). The
    fix in compile_version caches those scalars before the loop and
    uses locals on every attempt — this test catches any regression
    where a future edit reintroduces the expired-attribute read.
    """
    import sqlite3

    from sqlalchemy import event
    from sqlalchemy.exc import IntegrityError as SAIntegrityError
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker, create_async_engine,
    )

    from app.api.compiler import MetadataCompiler
    from app.api.meta_models import MetadataVersion

    # Isolated in-memory aiosqlite engine — separate from the
    # conftest-shared SQLite so this test does not interfere with
    # other suites that touch the same schema.
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await _setup_metadata_schema_for_sqlite(conn)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            version = MetadataVersion(
                tenant_id="test_tenant",
                status="active",
                created_by="test",
            )
            session.add(version)
            await session.commit()
            version_id = version.version_id

            # Inject one collision via a before_commit hook on the
            # underlying sync session. Events fire from sync code
            # inside SQLAlchemy's commit handling, so the hook can
            # raise a synchronous IntegrityError that propagates as
            # if the DB had rejected the commit.
            commit_attempts = [0]

            @event.listens_for(session.sync_session, "before_commit")
            def force_one_collision(_sess: object) -> None:
                commit_attempts[0] += 1
                if commit_attempts[0] == 1:
                    orig = sqlite3.IntegrityError(
                        "UNIQUE constraint failed: metadata_audit.previous_hash"
                    )
                    raise SAIntegrityError("INSERT", None, orig)

            # Call the REAL production function. If compile_version
            # reads any expired ORM attribute on attempt 2, this raises
            # MissingGreenlet from the implicit refresh.
            artifact = await MetadataCompiler.compile_version(
                session=session,
                version_id=version_id,
                actor="test_actor",
            )

            assert artifact is not None
            assert commit_attempts[0] == 2, (
                f"expected one rollback + one successful commit, got"
                f" {commit_attempts[0]} commit attempts"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_update_version_status_real_session_retries_archival_branch() -> None:
    """Real AsyncSession integration test for update_version_status:
    force one audit-chain IntegrityError, verify the second attempt
    exercises the archival audit branch successfully.

    This test catches three reviewer pass 3 findings simultaneously:

      Finding #1: cached_existing_active_id must be a local — reading
      existing_active.version_id on attempt 2 would crash with
      MissingGreenlet because rollback expired the ORM object.

      Finding #2: the chain-tip select must run BEFORE any version /
      existing_active mutations — otherwise default autoflush would
      fire the partial unique index on the SELECT, raising an
      IntegrityError outside our try/except. (We can't directly
      simulate the autoflush race in a single-process test, but the
      reorder fixes it by construction; this test exercises the path
      with the reorder applied and proves it doesn't crash.)

      Finding #3: cached_version_created_at_iso must be a local — the
      success-path return reads version.created_at, which would also
      crash on attempt 2 after rollback expiration if it weren't
      cached.
    """
    import sqlite3
    import uuid as uuid_mod

    from sqlalchemy import event
    from sqlalchemy.exc import IntegrityError as SAIntegrityError
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker, create_async_engine,
    )

    from app.api.auth import ResolvedCredential
    from app.api.meta_models import MetadataVersion

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await _setup_metadata_schema_for_sqlite(conn)

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            old_version = MetadataVersion(
                tenant_id="test_tenant",
                status="active",
                created_by="test",
            )
            new_version = MetadataVersion(
                tenant_id="test_tenant",
                status="pending_review",
                created_by="test",
            )
            session.add(old_version)
            session.add(new_version)
            await session.commit()
            new_id = new_version.version_id

            commit_attempts = [0]

            @event.listens_for(session.sync_session, "before_commit")
            def force_one_collision(_sess: object) -> None:
                commit_attempts[0] += 1
                if commit_attempts[0] == 1:
                    orig = sqlite3.IntegrityError(
                        "UNIQUE constraint failed: metadata_audit.previous_hash"
                    )
                    raise SAIntegrityError("INSERT", None, orig)

            # Direct invocation of the route handler function. FastAPI's
            # Annotated[X, Depends(...)] is type-hint-only at the function
            # level — we just pass the dependency values directly.
            from app.api.router import update_version_status

            # The Pydantic request model lives in app.api.models — the
            # exact class name was verified via grep before writing this
            # test. If a future rename breaks the import, search with
            # `grep -n "class.*VersionStatus" app/api/models.py`.
            from app.api.models import VersionStatusUpdateRequest

            cred = ResolvedCredential(
                credential_id=str(uuid_mod.uuid4()),
                tenant_id="test_tenant",
                user_id="test_admin",
                scope="admin",
            )
            payload = VersionStatusUpdateRequest(
                status="active",
                reason="real-session activation regression test",
            )

            result = await update_version_status(
                version_id=new_id,
                payload=payload,
                session=session,
                cred=cred,
            )

            assert result is not None
            assert result.status == "active"
            assert commit_attempts[0] == 2, (
                f"expected one rollback + one successful commit, got"
                f" {commit_attempts[0]} commit attempts; the archival"
                f" branch on attempt 2 likely crashed on an expired"
                f" ORM read"
            )
    finally:
        await engine.dispose()
```

**Note on the request-model class name:** the test imports `VersionStatusUpdateRequest` — verified to exist at `app/api/models.py:301` via `grep -n "class.*VersionStatus" app/api/models.py`. The handler signature `update_version_status(version_id, payload, session, cred)` is verified at `app/api/router.py:605` — FastAPI's `Annotated[X, Depends(...)]` is purely a type hint at the function-call level, so direct invocation with named arguments works.

**Note on the conftest interaction:** these tests create their own isolated `:memory:` engines so they do not touch the conftest-shared SQLite (the one at `sqlite:///file:testdb?mode=memory&cache=shared&uri=true`). That isolation is intentional — the conftest fixture only creates a subset of the metadata tables (no `metadata_tables` / `metadata_columns` / `metadata_relationships`), and we want full control of the schema. Each test cleans up via `engine.dispose()` in a `finally` block.

**Note on JSONB columns over TEXT:** the raw-SQL DDL declares `payload TEXT`, `artifact_blob TEXT`, `safety_classification TEXT`, `sample_values TEXT` instead of the production `JSONB`. This works because SQLAlchemy's `JSONB` type bind-processes to a JSON string and result-processes back to a Python dict regardless of the underlying storage type. Verified empirically: `CompiledRegistryArtifact(artifact_blob={"k": "v", "list": [1, 2, 3]})` round-trips through an INSERT and a SELECT against a TEXT column with the dict structure preserved exactly. Same applies to `MetadataAudit.payload`. The same SQLite-permissive trick is what `tests/conftest.py:179-219` uses for `compiled_registry_artifacts` and `metadata_audit`.

**Known test-strength limitation (non-blocking, deferred follow-up):** these two real-session tests inject the synthetic `IntegrityError` via a `before_commit` event listener. That is the cleanest injection point for validating the rollback-expiration + cached-scalars contract (the main Step 7 goal), but it intercepts BEFORE SQLAlchemy's flush actually runs against the DB, so the tests do NOT exercise what happens if a flush succeeds partially and then the next statement raises an IntegrityError mid-commit. The Step 6 mocked retry-path tests cover the wiring of the retry loop itself, and the real-session tests cover expiration safety — together they are sufficient for the current fix. If a future iteration wants stronger coverage of post-flush object re-staging, the next increment is to move one injected failure closer to the actual flush/DB execution (e.g. by registering the event listener on `do_orm_execute` for the specific audit-row INSERT statement, or by pre-inserting a conflicting audit row to trigger a real partial unique index violation at commit time). Tracked here so it isn't lost.

- [ ] **Step 8: Run the integration tests**

Run: `uv run pytest tests/test_audit_chain.py -v`
Expected: all four tests PASS — the two mocked classifier tests (Step 6) and the two real-session expiration tests (Step 7).

If `test_compile_version_real_session_retries_after_rollback` fails with `MissingGreenlet`, the cached scalars in `compile_version` have regressed — re-check that `version.version_id` / `version.tenant_id` / `version.status` reads are NOT inside the loop.

If `test_update_version_status_real_session_retries_archival_branch` fails with `MissingGreenlet`, either `cached_existing_active_id` or `cached_version_created_at_iso` is missing, OR the chain-tip select is still running after the version mutations.

- [ ] **Step 9: Run the full suite for regressions**

Run: `uv run pytest -v`
Expected: all green. The router restructure is significant — pay close attention to existing `test_version_lifecycle.py` results, which exercise the activation path heavily. If an activation-race test that previously returned 409 now retries internally, either the classifier is wrong OR the test's original assertion was relying on the old substring match.

- [ ] **Step 10: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/audit/append.py app/api/compiler.py app/api/router.py tests/test_audit_append.py tests/test_audit_chain.py
uv run mypy app/audit/append.py app/api/compiler.py app/api/router.py tests/test_audit_append.py tests/test_audit_chain.py
uv run lint-imports
```
Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add app/audit/append.py tests/test_audit_append.py app/api/compiler.py app/api/router.py tests/test_audit_chain.py
git commit -m "$(cat <<'EOF'
fix(audit): scoped retry for audit-chain contention (review finding #4)

The WORM audit appenders in MetadataCompiler.compile_version and
router.update_version_status both read the chain tip with a plain
SELECT and committed the next row without any serialization. Two
concurrent admin transactions could read the same tip and commit rows
with identical previous_hash, branching the chain. Task 4.1 added the
partial unique index uq_audit_previous_hash_nonempty as the
database-level backstop; this commit adds the application-side retry.

Key correctness properties (all flagged across three review passes):

1. RETRY IS SCOPED. A new module app/audit/append.py provides two
   classifiers — is_audit_chain_collision and is_activation_collision
   — that read exc.orig.constraint_name (asyncpg) with a sqlite3
   fallback that matches the empirically-verified
   "UNIQUE constraint failed: <table>.<column>" message format. The
   retry loops use these to distinguish: (a) audit-chain contention →
   rollback + retry; (b) uq_one_active_version_per_tenant race → 409
   to client; (c) anything else → re-raise unchanged. An unrelated
   FK/UNIQUE violation is NEVER masked as audit-chain contention.

2. METADATA IS PRESERVED ACROSS RETRIES. update_version_status now
   re-applies EVERY pre-audit version mutation inside the retry loop:
   version.status, change_reason, approved_by, approved_at, and
   existing_active.status. An earlier draft only re-set status /
   change_reason / existing_active.status, so a successful retry
   activated the version with NULL approval metadata.

3. ASYNC SESSION SAFETY. AsyncSession.rollback() expires loaded ORM
   attributes regardless of expire_on_commit; a plain attribute read
   on retry attempts would otherwise raise MissingGreenlet (verified
   empirically against an in-memory aiosqlite session). Both
   compile_version and update_version_status now cache the scalars
   they need from `version` / `existing_active` BEFORE the retry
   loop and use locals on every attempt. ASSIGNMENTS to expired
   attributes are still safe — only READS need caching.

4. AUTOFLUSH SAFETY. update_version_status reads the chain tip
   BEFORE applying any version / existing_active mutations.
   Otherwise, default autoflush would fire the partial unique index
   uq_one_active_version_per_tenant on the SELECT, raising an
   IntegrityError outside the try/except around session.commit() and
   producing an unclassified 500 (verified empirically:
   "IntegrityError raised as a result of Query-invoked autoflush").

5. PRODUCTION CODE IS ACTUALLY TESTED. tests/test_audit_append.py
   unit-tests both classifiers across the asyncpg + sqlite3 paths,
   including the empirically-verified sqlite3 message format.
   tests/test_audit_chain.py drives the real production code with
   four tests: two mocked tests verify classifier wiring (one for
   audit-chain retry, one for non-audit propagation), and two
   real-AsyncSession tests verify ORM expiration safety on retry
   attempts (one for compile_version, one for the
   update_version_status archival branch).

EOF
)"
```

---

# Final checklist

After all four phases land:

- [ ] All tests green: `uv run pytest -v`
- [ ] Lint clean: `uv run ruff check .`
- [ ] Type-check clean: `uv run mypy .`
- [ ] Import boundaries clean: `uv run lint-imports`
- [ ] No `# noqa`, `# type: ignore`, or rule suppressions added in production code (per `feedback_no_rule_relaxation.md`). The single test-side `# type: ignore[method-assign]` in Task 1.3 is acceptable scope (test-only, defensible, scoped to one line) — replace with `monkeypatch.setattr` if the project's mypy config rejects it.
- [ ] Each finding's PR/commit references the original code review (`docs/reviews/20260407_code_review.md` finding #N).
- [ ] No BIRD-related changes accidentally bundled in. This plan is intentionally separate from the BIRD plan; commits should not touch translator/safety/discovery/template files.

---

## Note on the analysis-vs-implementation rule

This plan was written in response to an explicit "write a separate plan for that now" request. Per `feedback_analysis_vs_implementation.md`, the next step is to **wait for an explicit go-ahead** ("implement", "go", "execute", etc.) before touching any source files. Phase 1 is the smallest possible starting point — its three quick wins are the lowest-risk way to validate the plan structure before committing to the larger Phase 2/3/4 work.

If you choose to implement piecemeal: Phase 1 is fully independent, Phase 2 depends on nothing, Phase 3 depends on nothing, and Phase 4 depends only on its own edit to `0001_initial_schema.py` (Task 4.1). They can ship in any order or interleave with the BIRD plan's phases without conflict — file overlaps are limited to `app/api/router.py` (different functions) and `backend_migrations/versions/0001_initial_schema.py` (BIRD Phase 2 adds columns to `metadata_columns`; this plan's Phase 4 adds an index on `metadata_audit` — no collision). If both plans edit `0001` concurrently, merge sequentially and re-run the `docker compose down -v` + rebuild verification in whichever task lands second.
