"""Unit tests for RegistryLoader.load_schema_from_artifact.

These are pure-Python tests: no database. They construct an in-memory
CompiledRegistryArtifact with a valid hash and HMAC signature using the
same canonicalization helpers as the production compiler, then assert
that the loader hydrates the resulting RegistrySchema correctly.
"""
import hashlib
from typing import Any

from app.api.meta_models import CompiledRegistryArtifact
from app.audit.chaining import (
    compute_artifact_hmac_signature,
    get_canonical_json,
)
from app.steward.loader import RegistryLoader
from app.vault import get_secrets_manager


def _signed_artifact_for(blob: dict[str, Any]) -> CompiledRegistryArtifact:
    """Build a CompiledRegistryArtifact whose hash and signature pass
    RegistryLoader's verification, given the same canonicalization and
    HMAC helpers production uses.
    """
    canon = get_canonical_json(blob)
    valid_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    secrets_mgr = get_secrets_manager()
    kid = secrets_mgr.get_current_signing_key_id()
    signing_key = secrets_mgr.get_signing_key(kid)
    signature = compute_artifact_hmac_signature(signing_key, canon)
    return CompiledRegistryArtifact(
        artifact_blob=blob,
        artifact_hash=valid_hash,
        signature=signature,
        signature_key_id=kid,
        tenant_id="default",
        compiler_version="1.0.0",
    )


def _minimal_blob_with_two_columns(
    *,
    first_exhaustive: bool,
    include_second_field: bool,
) -> dict[str, Any]:
    """Build a minimal valid artifact blob with one table and two columns.

    The first column carries sample_values_exhaustive=first_exhaustive.
    The second column either includes the field set to False, or omits it
    entirely (to verify the .get(..., False) back-compat default).
    """
    second_col: dict[str, Any] = {
        "id": "00000000-0000-0000-0000-000000000003",
        "name": "name",
        "alias": "name",
        "description": "",
        "type": "text",
        "is_primary": False,
        "is_nullable": True,
        "allowed_in_select": True,
        "allowed_in_filter": True,
        "allowed_in_join": False,
        "is_sensitive": False,
        "safety_classification": {},
        "rag_enabled": False,
        "rag_cardinality_hint": None,
        "rag_limit": None,
        "rag_values_hash": "",
        "sample_values": [],
    }
    if include_second_field:
        second_col["sample_values_exhaustive"] = False
    return {
        "meta_version": "v1.0.0",
        "compiled_at": "2026-04-07T00:00:00Z",
        "tables": [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "enum_table",
                "alias": "enum_table",
                "description": "",
                "tenant_id": "default",
                "source_database": None,
                "columns": [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "name": "status",
                        "alias": "status",
                        "description": "",
                        "type": "text",
                        "is_primary": False,
                        "is_nullable": False,
                        "allowed_in_select": True,
                        "allowed_in_filter": True,
                        "allowed_in_join": False,
                        "is_sensitive": False,
                        "safety_classification": {},
                        "rag_enabled": False,
                        "rag_cardinality_hint": None,
                        "rag_limit": None,
                        "rag_values_hash": "",
                        "sample_values": ["active", "archived"],
                        "sample_values_exhaustive": first_exhaustive,
                    },
                    second_col,
                ],
                "relationships": [],
            }
        ],
        "rag_manifest": {"default_rag_limit": 100, "rag_enabled_count": 0},
        "roles": {"system": "admin"},
    }


def test_loader_hydrates_sample_values_exhaustive_true() -> None:
    """A column whose blob has sample_values_exhaustive=True must hydrate
    into AbstractColumnDef.sample_values_exhaustive=True."""
    blob = _minimal_blob_with_two_columns(
        first_exhaustive=True, include_second_field=True
    )
    artifact = _signed_artifact_for(blob)
    schema = RegistryLoader.load_schema_from_artifact(artifact)

    cols = schema.tables[0].columns
    assert cols[0].alias == "status"
    assert cols[0].sample_values == ["active", "archived"]
    assert cols[0].sample_values_exhaustive is True
    assert cols[1].alias == "name"
    assert cols[1].sample_values_exhaustive is False


def test_loader_defaults_sample_values_exhaustive_when_field_absent() -> None:
    """A column whose blob omits sample_values_exhaustive entirely must
    hydrate as False (back-compat: old artifacts predate the field)."""
    blob = _minimal_blob_with_two_columns(
        first_exhaustive=False, include_second_field=False
    )
    artifact = _signed_artifact_for(blob)
    schema = RegistryLoader.load_schema_from_artifact(artifact)

    cols = schema.tables[0].columns
    assert cols[1].alias == "name"
    assert cols[1].sample_values_exhaustive is False
