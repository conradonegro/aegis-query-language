import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.meta_models import (
    CompiledRegistryArtifact,
    MetadataAudit,
    MetadataColumn,
    MetadataRelationship,
    MetadataTable,
    MetadataVersion,
)
from app.audit.chaining import (
    compute_artifact_hmac_signature,
    compute_audit_row_hash,
    get_canonical_json,
)
from app.vault import get_secrets_manager


class MetadataCompiler:
    """
    Freezes a human-reviewed MetadataVersion into a highly optimized, immutable JSON blob
    that the Aegis AST engine natively boots from.
    """

    @classmethod
    async def compile_version(cls, session: AsyncSession, version_id: uuid.UUID, actor: str) -> CompiledRegistryArtifact:
        # Load the complete object graph for the target version
        stmt = select(MetadataVersion).where(
            MetadataVersion.version_id == version_id
        ).options(
            selectinload(MetadataVersion.tables).selectinload(MetadataTable.columns),
            selectinload(MetadataVersion.edges)
        )
        
        result = await session.execute(stmt)
        version = result.scalar_one_or_none()
        
        if not version:
            raise ValueError(f"Version {version_id} not found.")
            
        if version.status != "active":
            raise ValueError(f"Cannot compile artifact. Version {version_id} must be 'active'.")

        # 1. Build the physical runtime Dictionary Payload mapping
        payload: dict[str, Any] = {
            "meta_version": str(version.version_id),
            "compiled_at": datetime.now(timezone.utc).isoformat(),
            "tables": [],
            "roles": {"system": "admin"} # Mock roles injection for future
        }

        table_idx_map: dict[uuid.UUID, dict[str, Any]] = {}

        # 2. Iterate Tables and inner Columns
        for tbl in version.tables:
            if not tbl.active:
                continue

            tbl_dict = {
                "id": str(tbl.table_id),
                "name": tbl.real_name,
                "alias": tbl.alias,
                "description": tbl.description,
                "columns": [],
                "relationships": []
            }

            for col in tbl.columns:
                tbl_dict["columns"].append({
                    "id": str(col.column_id),
                    "name": col.real_name,
                    "alias": col.alias,
                    "description": col.description,
                    "type": col.data_type,
                    "is_primary": col.is_primary_key,
                    "is_nullable": col.is_nullable,
                    "allowed_in_select": col.allowed_in_select,
                    "allowed_in_filter": col.allowed_in_filter,
                    "allowed_in_join": col.allowed_in_join,
                    "is_sensitive": col.is_sensitive,
                    "safety_classification": col.safety_classification or {},
                })
            
            table_idx_map[tbl.table_id] = tbl_dict
            payload["tables"].append(tbl_dict)

        # 3. Resolve Relationships across Tables Native Object Graph
        for edge in version.edges:
            if not edge.active:
                continue
                
            src_tbl = table_idx_map.get(edge.source_table_id)
            tgt_tbl = table_idx_map.get(edge.target_table_id)
            
            if not src_tbl or not tgt_tbl:
                # Should structurally never happen due to Postgres FKs, but defensive checks
                continue
                
            src_tbl["relationships"].append({
                "target_table": tgt_tbl["alias"],
                "source_column_id": str(edge.source_column_id),
                "target_column_id": str(edge.target_column_id),
                "type": edge.relationship_type,
                "cardinality": edge.cardinality
            })

            if edge.bidirectional:
                 tgt_tbl["relationships"].append({
                    "target_table": src_tbl["alias"],
                    "source_column_id": str(edge.target_column_id),
                    "target_column_id": str(edge.source_column_id),
                    "type": edge.relationship_type,
                    "cardinality": edge.cardinality[::-1] # Flips cardinality dynamically! 1:n -> n:1
                })
        
        # 4. Sign and Compute Hash Payload
        canonical_payload = get_canonical_json(payload)
        final_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        
        # Vault Signing Lifecycle
        secrets_mgr = get_secrets_manager()
        current_key_id = secrets_mgr.get_current_signing_key_id()
        signing_key = secrets_mgr.get_signing_key(current_key_id)
        
        signature = compute_artifact_hmac_signature(signing_key, canonical_payload)
        
        artifact = CompiledRegistryArtifact(
            version_id=version.version_id,
            artifact_blob=payload,
            artifact_hash=final_hash,
            compiler_version="1.0.0",
            signature=signature,
            signature_key_id=current_key_id
        )
        session.add(artifact)
        
        # 5. Native Deterministic WORM Audit Write
        last_audit_res = await session.execute(
            select(MetadataAudit).order_by(MetadataAudit.timestamp.desc(), MetadataAudit.audit_id.desc()).limit(1)
        )
        last_row = last_audit_res.scalar_one_or_none()
        previous_hash = last_row.row_hash if last_row else ""
        
        audit_timestamp_native = datetime.now(timezone.utc)
        audit_payload = {
            "event": "compile_version",
            "version_id": str(version.version_id),
            "artifact_hash": final_hash,
            "signature_key_id": current_key_id,
            "status": "SUCCESS"
        }
        
        audit_canonical = get_canonical_json(audit_payload)
        new_row_hash = compute_audit_row_hash(previous_hash, audit_canonical, audit_timestamp_native.isoformat())
        
        audit_event = MetadataAudit(
            version_id=version.version_id,
            actor=actor,
            action="deploy",
            payload=audit_payload,
            timestamp=audit_timestamp_native,
            previous_hash=previous_hash,
            row_hash=new_row_hash,
            key_id=current_key_id
        )
        session.add(audit_event)
        
        # 6. Lock the hash trace dynamically to the version object
        version.registry_hash = final_hash
        version.approved_by = actor
        version.approved_at = datetime.now(timezone.utc)
        
        await session.commit()
        return artifact
