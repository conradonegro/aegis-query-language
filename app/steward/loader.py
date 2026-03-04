import hashlib
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.meta_models import CompiledRegistryArtifact
from app.steward.models import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)
from app.audit.chaining import get_canonical_json, verify_hmac_signature
from app.vault import get_secrets_manager

logger = logging.getLogger(__name__)

class UnauthorizedRegistryTamperError(Exception):
    """Raised natively when the DB Artifact's physical footprint fails HMAC verification."""
    pass


class RegistryLoader:
    """
    Bridge utility fetching the absolute latest Active compilation hash
    from the PostgreSQL Database and mapping it statically into memory for the LLM Middleware.
    """

    @staticmethod
    async def load_active_schema(session: AsyncSession) -> RegistrySchema | None:
        # Load the most recent artifact natively
        stmt = select(CompiledRegistryArtifact).order_by(
            CompiledRegistryArtifact.compiled_at.desc()
        ).limit(1)
        
        result = await session.execute(stmt)
        artifact = result.scalar_one_or_none()

        if not artifact:
            return None

        # --- Cryptographic Native Tampering Detection (Phase 18) ---
        canonical_payload = get_canonical_json(artifact.artifact_blob)
        computed_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
        
        if computed_hash != artifact.artifact_hash:
            logger.critical(f"Tamper Error: Registry Hash mismatch on Artifact {artifact.artifact_id}!")
            raise UnauthorizedRegistryTamperError("Artifact Hash discrepancy detected.")
            
        secrets_mgr = get_secrets_manager()
        # Fallback to dev strings exclusively if migrating from non-signed mocks
        kid = artifact.signature_key_id or secrets_mgr.get_current_signing_key_id() 
        signing_key = secrets_mgr.get_signing_key(kid)
        
        if not verify_hmac_signature(signing_key, canonical_payload, str(artifact.signature)):
            logger.critical(f"HMAC Verification Failed! Artifact {artifact.artifact_id} - Key {kid}")
            raise UnauthorizedRegistryTamperError(f"HMAC Signature match absolutely failed for Artifact {artifact.artifact_id}. Execution halted.")
            
        logger.info(f"Verified WORM Boot HMAC Signature! Artifact {artifact.artifact_id} - Key {kid}")
        # -------------------------------------------------------------
        
        payload = artifact.artifact_blob
        
        # Hydrate JSON blob into strict internal typed Pydantic structures needed by CompilerEngine
        tables_def = []
        relationships_def = []

        for tbl_dict in payload.get("tables", []):
            columns_def = []
            for col_dict in tbl_dict.get("columns", []):
                
                safety = SafetyClassification(
                    allowed_in_where=True,
                    allowed_in_select=True,
                    allowed_in_group_by=True,
                    aggregation_allowed=col_dict.get("type") in ("numeric", "integer", "real", "double precision"),
                    join_participation_allowed=True
                )
                
                columns_def.append(
                    AbstractColumnDef(
                        alias=col_dict["alias"],
                        description=col_dict.get("description", ""),
                        data_type=col_dict.get("type", "text"),
                        safety=safety,
                        physical_target=col_dict["name"] # Mapping conceptual alias directly to real name
                    )
                )

            tables_def.append(
                AbstractTableDef(
                    alias=tbl_dict["alias"],
                    description=tbl_dict.get("description", ""),
                    physical_target=tbl_dict["name"],
                    columns=columns_def
                )
            )
            
            # Map Relationships internally against string abstract aliases so Engine RAG succeeds
            for rel_dict in tbl_dict.get("relationships", []):
                # Search the col map local alias to match against source UUID
                source_col = next((c for c in tbl_dict.get("columns", []) if c["id"] == rel_dict["source_column_id"]), None)
                target_col = None # Can't immediately map Target Column purely from dictionary flattening without a pass, but for LLM, only table edge connects matter!
                
                # In Steward Schema right now, we just map table-to-table string bindings for edges conceptually
                relationships_def.append(
                    AbstractRelationshipDef(
                        source_table=tbl_dict["alias"],
                        source_column=source_col["alias"] if source_col else "",
                        target_table=rel_dict["target_table"],
                        target_column="" # Omitted temporarily as LLMs generally can construct native target bindings when source keys match physical mappings
                    )
                )

        return RegistrySchema(
            version=str(artifact.artifact_hash),
            tables=tables_def,
            relationships=relationships_def
        )
