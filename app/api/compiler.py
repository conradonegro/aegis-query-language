import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.meta_models import (
    CompiledRegistryArtifact,
    MetadataColumn,
    MetadataRelationship,
    MetadataTable,
    MetadataVersion,
)


class MetadataCompiler:
    """
    Freezes a human-reviewed MetadataVersion into a highly optimized, immutable JSON blob
    that the Aegis AST engine natively boots from.
    """
    @staticmethod
    def _hash_payload(payload: dict[str, Any]) -> str:
        blob = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()

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
            "compiled_at": datetime.utcnow().isoformat(),
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
                    "type": col.data_type,
                    "is_primary": col.is_primary_key,
                    "is_nullable": col.is_nullable,
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
        final_hash = cls._hash_payload(payload)
        
        artifact = CompiledRegistryArtifact(
            version_id=version.version_id,
            artifact_blob=payload,
            artifact_hash=final_hash,
            compiler_version="1.0.0",
            signature="unsigned" # Placeholder for future RSA signing implementation
        )
        
        session.add(artifact)
        
        # Lock the hash trace dynamically to the version object
        version.registry_hash = final_hash
        version.approved_by = actor
        version.approved_at = datetime.utcnow()
        
        await session.commit()
        return artifact
