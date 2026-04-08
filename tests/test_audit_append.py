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
