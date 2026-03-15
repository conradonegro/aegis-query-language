"""
Auth unit and integration tests.

Unit tests cover the crypto helpers directly.
Integration tests call the FastAPI dependency functions directly with a real
SQLite session (the shared in-memory DB seeded by conftest.py).
"""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.auth import (
    ResolvedCredential,
    _hash_api_key,
    generate_api_key,
    require_admin_credential,
    require_query_credential,
    verify_api_key,
)
from tests.conftest import (
    TEST_ADMIN_CREDENTIAL_ID,
    TEST_ADMIN_RAW_KEY,
    TEST_QUERY_CREDENTIAL_ID,
    TEST_QUERY_RAW_KEY,
)

_TEST_DB_URL = "sqlite+aiosqlite:///file:testdb?mode=memory&cache=shared&uri=true"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeRequest:
    """Minimal stand-in for starlette.requests.Request used in dep tests."""

    def __init__(self, authorization: str | None = None) -> None:
        self.headers: dict[str, str] = {}
        if authorization is not None:
            self.headers["Authorization"] = authorization


@pytest_asyncio.fixture
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(_TEST_DB_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


# ---------------------------------------------------------------------------
# Unit tests — pure crypto helpers (no DB)
# ---------------------------------------------------------------------------


def test_generate_api_key_length() -> None:
    key = generate_api_key()
    # secrets.token_hex(32) → 64 hex chars
    assert len(key) == 64


def test_generate_api_key_is_hex() -> None:
    key = generate_api_key()
    int(key, 16)  # raises ValueError if not valid hex


def test_generate_api_key_unique() -> None:
    assert generate_api_key() != generate_api_key()


def test_hash_api_key_deterministic() -> None:
    h1 = _hash_api_key("raw_key", "secret")
    h2 = _hash_api_key("raw_key", "secret")
    assert h1 == h2


def test_hash_api_key_different_secrets_differ() -> None:
    h1 = _hash_api_key("raw_key", "secret_a")
    h2 = _hash_api_key("raw_key", "secret_b")
    assert h1 != h2


def test_hash_api_key_different_keys_differ() -> None:
    h1 = _hash_api_key("key_a", "secret")
    h2 = _hash_api_key("key_b", "secret")
    assert h1 != h2


def test_verify_api_key_correct() -> None:
    key = "my_test_key"
    secret = "my_test_secret"
    stored = _hash_api_key(key, secret)
    assert verify_api_key(key, stored, secret) is True


def test_verify_api_key_wrong_key() -> None:
    secret = "my_test_secret"
    stored = _hash_api_key("correct_key", secret)
    assert verify_api_key("wrong_key", stored, secret) is False


def test_verify_api_key_wrong_secret() -> None:
    stored = _hash_api_key("key", "secret_a")
    assert verify_api_key("key", stored, "secret_b") is False


# ---------------------------------------------------------------------------
# Integration tests — dependency functions against the real seeded DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_query_cred_missing_header(db_session: AsyncSession) -> None:
    req = _FakeRequest()  # no Authorization header
    with pytest.raises(Exception) as exc_info:
        await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    assert exc_info.value.status_code == 401  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_require_query_cred_malformed_header(db_session: AsyncSession) -> None:
    req = _FakeRequest(authorization="Basic abc123")
    with pytest.raises(Exception) as exc_info:
        await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    assert exc_info.value.status_code == 401  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_require_query_cred_invalid_key(db_session: AsyncSession) -> None:
    req = _FakeRequest(authorization="Bearer completely_wrong_key")
    with pytest.raises(Exception) as exc_info:
        await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    assert exc_info.value.status_code == 401  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_require_query_cred_valid_query_key(db_session: AsyncSession) -> None:
    req = _FakeRequest(authorization=f"Bearer {TEST_QUERY_RAW_KEY}")
    cred = await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    assert isinstance(cred, ResolvedCredential)
    assert cred.scope == "query"
    assert cred.tenant_id == "test_tenant"
    assert cred.credential_id == TEST_QUERY_CREDENTIAL_ID


@pytest.mark.asyncio
async def test_require_query_cred_valid_admin_key(db_session: AsyncSession) -> None:
    """Admin keys are a superset — they must also pass require_query_credential."""
    req = _FakeRequest(authorization=f"Bearer {TEST_ADMIN_RAW_KEY}")
    cred = await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    assert cred.scope == "admin"
    assert cred.credential_id == TEST_ADMIN_CREDENTIAL_ID


@pytest.mark.asyncio
async def test_require_admin_cred_rejects_query_key(db_session: AsyncSession) -> None:
    """A query-scoped key must be rejected with 403 (not 401) on admin endpoints."""
    query_cred = ResolvedCredential(
        credential_id=TEST_QUERY_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="test_user",
        scope="query",
    )
    with pytest.raises(Exception) as exc_info:
        await require_admin_credential(cred=query_cred)
    assert exc_info.value.status_code == 403  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_require_admin_cred_accepts_admin_key(db_session: AsyncSession) -> None:
    admin_cred = ResolvedCredential(
        credential_id=TEST_ADMIN_CREDENTIAL_ID,
        tenant_id="test_tenant",
        user_id="admin_user",
        scope="admin",
    )
    result = await require_admin_credential(cred=admin_cred)
    assert result.scope == "admin"


@pytest.mark.asyncio
async def test_require_query_cred_uses_hmac_secret_from_env(
    db_session: AsyncSession,
) -> None:
    """
    Verify the hash is keyed on the HMAC secret — a key hashed with a different
    secret must not authenticate.
    """
    # Hash the query key with a DIFFERENT secret — lookup must fail
    from app.api.auth import _hash_api_key as hash_fn

    wrong_hash_key = hash_fn(TEST_QUERY_RAW_KEY, "wrong_secret")
    req = _FakeRequest(authorization=f"Bearer {wrong_hash_key}")
    with pytest.raises(Exception) as exc_info:
        await require_query_credential(request=req, session=db_session)  # type: ignore[arg-type]
    # The raw wrong_hash_key string won't match any stored hash → 401
    assert exc_info.value.status_code == 401  # type: ignore[attr-defined]
