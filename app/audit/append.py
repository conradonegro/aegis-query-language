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
