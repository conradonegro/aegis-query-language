import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.meta_models import (
    CompiledRegistryArtifact,
    MetadataAudit,
    MetadataColumn,
    MetadataTable,
    MetadataVersion,
)
from app.audit.append import is_audit_chain_collision
from app.audit.chaining import (
    compute_artifact_hmac_signature,
    compute_audit_row_hash,
    get_canonical_json,
)
from app.vault import get_secrets_manager


class MixedTenantArtifactError(ValueError):
    """Raised when a table's tenant_id conflicts with its owning version's tenant_id."""


def _assert_table_tenant(tbl: MetadataTable, version: MetadataVersion) -> None:
    """Raise MixedTenantArtifactError if the table's tenant_id conflicts."""
    if tbl.tenant_id is not None and tbl.tenant_id != version.tenant_id:
        raise MixedTenantArtifactError(
            f"Table '{tbl.alias}' has tenant_id='{tbl.tenant_id}' "
            f"which conflicts with version tenant_id='{version.tenant_id}'. "
            f"All tables in a version must belong to the same tenant."
        )


def _compute_rag_values_hash(values: list[str]) -> str:
    """SHA256 of sorted JSON-encoded normalized values."""
    sorted_vals = sorted(values)
    canonical = json.dumps(
        sorted_vals, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _build_table_dict(
    tbl: MetadataTable, version: MetadataVersion
) -> dict[str, Any]:
    """Build the per-table payload entry, including all active column rows."""
    tbl_dict: dict[str, Any] = {
        "id": str(tbl.table_id),
        "name": tbl.real_name,
        "alias": tbl.alias,
        "description": tbl.description,
        "tenant_id": version.tenant_id,
        "source_database": tbl.source_database,
        "columns": [],
        "relationships": [],
    }

    for col in tbl.columns:
        active_values = [v.value for v in col.values if v.active]
        tbl_dict["columns"].append({
            "id": str(col.column_id),
            "name": col.real_name,
            "alias": col.alias,
            "description": col.description or "",
            "type": col.data_type,
            "is_primary": col.is_primary_key,
            "is_nullable": col.is_nullable,
            "allowed_in_select": col.allowed_in_select,
            "allowed_in_filter": col.allowed_in_filter,
            "allowed_in_join": col.allowed_in_join,
            "is_sensitive": col.is_sensitive,
            "safety_classification": col.safety_classification or {},
            "rag_enabled": col.rag_enabled,
            "rag_cardinality_hint": col.rag_cardinality_hint,
            "rag_limit": col.rag_limit,
            "rag_values_hash": _compute_rag_values_hash(active_values),
            "sample_values": col.sample_values or [],
            "sample_values_exhaustive": col.sample_values_exhaustive,
        })

    return tbl_dict


def _apply_relationships(
    version: MetadataVersion,
    table_idx_map: dict[uuid.UUID, dict[str, Any]],
) -> None:
    """Resolve active relationships into the relevant table_dict entries."""
    for edge in version.edges:
        if not edge.active:
            continue

        src_tbl = table_idx_map.get(edge.source_table_id)
        tgt_tbl = table_idx_map.get(edge.target_table_id)

        if not src_tbl or not tgt_tbl:
            # Should structurally never happen due to Postgres FKs, but
            # defensive checks
            continue

        src_tbl["relationships"].append({
            "target_table": tgt_tbl["alias"],
            "source_column_id": str(edge.source_column_id),
            "target_column_id": str(edge.target_column_id),
            "type": edge.relationship_type,
            "cardinality": edge.cardinality,
        })

        if edge.bidirectional:
            tgt_tbl["relationships"].append({
                "target_table": src_tbl["alias"],
                "source_column_id": str(edge.target_column_id),
                "target_column_id": str(edge.source_column_id),
                "type": edge.relationship_type,
                # Flips cardinality dynamically! 1:n -> n:1
                "cardinality": edge.cardinality[::-1]
            })


def _build_compile_payload(version: MetadataVersion) -> dict[str, Any]:
    """Build the full compile payload (tables + relationships + RAG manifest)
    from a hydrated MetadataVersion object graph.

    Extracted from MetadataCompiler.compile_version so the public method
    stays under the C901 complexity threshold after the audit-chain retry
    loop was added (review finding #4).
    """
    payload: dict[str, Any] = {
        "meta_version": str(version.version_id),
        "compiled_at": datetime.now(UTC).isoformat(),
        "tables": [],
        "roles": {"system": "admin"},  # Mock roles injection for future
    }

    table_idx_map: dict[uuid.UUID, dict[str, Any]] = {}

    for tbl in version.tables:
        if not tbl.active:
            continue

        _assert_table_tenant(tbl, version)
        tbl_dict = _build_table_dict(tbl, version)
        table_idx_map[tbl.table_id] = tbl_dict
        payload["tables"].append(tbl_dict)

    _apply_relationships(version, table_idx_map)

    payload["rag_manifest"] = {
        "default_rag_limit": 100,
        "rag_enabled_count": sum(
            1
            for tbl in version.tables
            if tbl.active
            for col in tbl.columns
            if col.rag_enabled and not col.is_sensitive
        ),
    }

    return payload


class MetadataCompiler:
    """
    Freezes a human-reviewed MetadataVersion into a highly optimized,
    immutable JSON blob that the Aegis AST engine natively boots from.
    """

    @classmethod
    async def compile_version(
        cls, session: AsyncSession, version_id: uuid.UUID, actor: str
    ) -> CompiledRegistryArtifact:
        # Load the complete object graph for the target version
        stmt = (
            select(MetadataVersion)
            .where(MetadataVersion.version_id == version_id)
            .execution_options(populate_existing=True)
            .options(
                selectinload(MetadataVersion.tables)
                .selectinload(MetadataTable.columns)
                .selectinload(MetadataColumn.values),
                selectinload(MetadataVersion.edges),
            )
        )

        result = await session.execute(stmt)
        version = result.scalar_one_or_none()

        if not version:
            raise ValueError(f"Version {version_id} not found.")

        if version.status not in ("active", "pending_review"):
            raise ValueError(
                f"Cannot compile artifact. Version {version_id} must be"
                f" 'active' or 'pending_review'."
            )

        # 1-3. Build the physical runtime payload (tables, columns,
        # relationships, RAG manifest). Extracted to a module-level helper
        # to keep this method under the C901 complexity threshold after
        # the audit-chain retry loop was added.
        payload = _build_compile_payload(version)

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
        artifact: CompiledRegistryArtifact | None = None
        for _attempt in range(5):
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
