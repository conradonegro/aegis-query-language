import hashlib
import hmac
import logging
import secrets
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.api.dependencies import get_registry_runtime_db_session
from app.api.meta_models import TenantCredential
from app.vault import VaultMissingSecretError, get_secrets_manager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedCredential:
    credential_id: str
    tenant_id: str
    user_id: str
    scope: str


def generate_api_key() -> str:
    """Generate a cryptographically random 64-character hex API key."""
    return secrets.token_hex(32)


def _hash_api_key(raw: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_api_key(raw: str, stored_hash: str, secret: str) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    expected = _hash_api_key(raw, secret)
    return hmac.compare_digest(expected, stored_hash)


async def require_query_credential(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_registry_runtime_db_session)],
) -> ResolvedCredential:
    """
    FastAPI dependency — accepts any active tenant API key (query or admin scope).
    Raises 401 if the key is missing, malformed, or not found in the database.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    raw_key = auth_header.removeprefix("Bearer ").strip()
    if not raw_key:
        raise HTTPException(
            status_code=401,
            detail="Empty API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        secret = get_secrets_manager().get_credential_hmac_secret()
    except VaultMissingSecretError as exc:
        logger.error("HMAC secret unavailable: %s", exc)
        raise HTTPException(
            status_code=500, detail="Auth service unavailable."
        ) from exc

    key_hash = _hash_api_key(raw_key, secret)

    res = await session.execute(
        select(TenantCredential).where(
            TenantCredential.key_hash == key_hash,
            TenantCredential.is_active.is_(True),
        )
    )
    cred = res.scalar_one_or_none()
    if cred is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid or inactive API key.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return ResolvedCredential(
        credential_id=str(cred.credential_id),
        tenant_id=cred.tenant_id,
        user_id=cred.user_id,
        scope=cred.scope,
    )


async def require_admin_credential(
    cred: Annotated[ResolvedCredential, Depends(require_query_credential)],
) -> ResolvedCredential:
    """
    FastAPI dependency — chains require_query_credential and additionally
    enforces scope='admin'. Raises 403 (not 401) for a valid key with
    insufficient scope so the client knows not to clear the stored key.
    """
    if cred.scope != "admin":
        raise HTTPException(
            status_code=403,
            detail="Admin scope required.",
        )
    return cred
