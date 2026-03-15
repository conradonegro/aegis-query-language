"""
Bootstrap CLI: create the first (or any subsequent) Aegis tenant API key.

This script is the only way to create a credential outside of the API itself,
which is required to bootstrap the very first admin key.

Usage:
    uv run python scripts/create_admin_key.py \\
        --tenant-id acme \\
        --user-id alice@acme.com \\
        --scope admin \\
        --description "Initial bootstrap key"

Required environment variables:
    DB_URL_REGISTRY_ADMIN   — asyncpg connection URL for the registry-admin role
    API_KEY_HMAC_SECRET     — HMAC-SHA256 secret (or SECRETS_PROVIDER=vault)
"""

import argparse
import asyncio
import logging
import os
import sys
import uuid

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create an Aegis tenant API key and print it to stdout once."
    )
    parser.add_argument(
        "--tenant-id",
        required=True,
        help="Tenant identifier this key belongs to.",
    )
    parser.add_argument(
        "--user-id",
        required=True,
        help="User or service account this key is issued to.",
    )
    parser.add_argument(
        "--scope",
        choices=["query", "admin"],
        default="admin",
        help="Key scope: 'query' for read-only endpoints, 'admin' for full access "
             "(default: admin).",
    )
    parser.add_argument(
        "--description",
        default=None,
        help="Optional human-readable label for this key.",
    )
    return parser.parse_args()


async def create_key(
    tenant_id: str,
    user_id: str,
    scope: str,
    description: str | None,
) -> None:
    # -- Resolve DB URL -------------------------------------------------------
    db_url = os.getenv("DB_URL_REGISTRY_ADMIN")
    if not db_url:
        print(
            "ERROR: DB_URL_REGISTRY_ADMIN is not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    # -- Resolve HMAC secret via vault layer ----------------------------------
    # Import here so the script can be run without the full app import chain
    # triggering at module level (which would import meta_models → SQLAlchemy
    # mapper setup that requires a running loop in some configurations).
    from app.api.auth import _hash_api_key, generate_api_key
    from app.vault import VaultMissingSecretError, get_secrets_manager

    try:
        secret = get_secrets_manager().get_credential_hmac_secret()
    except VaultMissingSecretError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # -- Generate and hash the key --------------------------------------------
    raw_key = generate_api_key()
    key_hash = _hash_api_key(raw_key, secret)
    credential_id = str(uuid.uuid4())

    # -- Insert into database -------------------------------------------------
    engine = create_async_engine(db_url, echo=False)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO aegis_meta.tenant_credentials"
                    " (credential_id, tenant_id, user_id, key_hash, scope,"
                    "  description, is_active, created_at)"
                    " VALUES (:cid, :tid, :uid, :khash, :scope,"
                    "         :desc, TRUE, now())"
                ),
                {
                    "cid": credential_id,
                    "tid": tenant_id,
                    "uid": user_id,
                    "khash": key_hash,
                    "scope": scope,
                    "desc": description,
                },
            )
    except Exception as exc:
        print(f"ERROR: Failed to insert credential: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await engine.dispose()

    # -- Print result ---------------------------------------------------------
    separator = "-" * 60
    print(separator, file=sys.stderr)
    print("Aegis API key created successfully.", file=sys.stderr)
    print(f"  credential_id : {credential_id}", file=sys.stderr)
    print(f"  tenant_id     : {tenant_id}", file=sys.stderr)
    print(f"  user_id       : {user_id}", file=sys.stderr)
    print(f"  scope         : {scope}", file=sys.stderr)
    if description:
        print(f"  description   : {description}", file=sys.stderr)
    print(separator, file=sys.stderr)
    print(
        "WARNING: The raw key below is shown ONCE and cannot be recovered.",
        file=sys.stderr,
    )
    print(separator, file=sys.stderr)
    # Raw key goes to stdout so callers can capture it cleanly:
    #   RAW_KEY=$(uv run python scripts/create_admin_key.py ...)
    print(raw_key)


def main() -> None:
    args = _parse_args()
    asyncio.run(
        create_key(
            tenant_id=args.tenant_id,
            user_id=args.user_id,
            scope=args.scope,
            description=args.description,
        )
    )


if __name__ == "__main__":
    main()
