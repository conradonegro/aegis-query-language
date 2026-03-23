import hashlib
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.meta_models import CompiledRegistryArtifact, MetadataVersion
from app.audit.chaining import get_canonical_json, verify_hmac_signature
from app.steward.models import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)
from app.vault import get_secrets_manager

logger = logging.getLogger(__name__)

class UnauthorizedRegistryTamperError(Exception):
    """Raised natively when the DB Artifact's physical footprint fails HMAC
    verification."""
    pass


class RegistryLoader:
    """
    Bridge utility fetching the absolute latest Active compilation hash
    from the PostgreSQL Database and mapping it statically into memory for
    the LLM Middleware.
    """

    @staticmethod
    def load_schema_from_artifact(
        artifact: CompiledRegistryArtifact,
    ) -> RegistrySchema:
        """Verify and hydrate a RegistrySchema from an already-fetched artifact.

        Performs the same HMAC + hash checks as load_active_schema but accepts
        an artifact object directly instead of querying the DB. Use this when
        the caller already holds a consistent artifact snapshot (e.g. at boot)
        to avoid a second DB round-trip that could see a newer version.
        """
        # --- Cryptographic Native Tampering Detection (Phase 18) ---
        canonical_payload = get_canonical_json(artifact.artifact_blob)
        computed_hash = hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

        if computed_hash != artifact.artifact_hash:
            logger.critical(
                f"Tamper Error: Registry Hash mismatch on Artifact "
                f"{artifact.artifact_id}!"
            )
            raise UnauthorizedRegistryTamperError("Artifact Hash discrepancy detected.")

        secrets_mgr = get_secrets_manager()
        # Fallback to dev strings exclusively if migrating from non-signed mocks
        kid = artifact.signature_key_id or secrets_mgr.get_current_signing_key_id()
        signing_key = secrets_mgr.get_signing_key(kid)

        if not verify_hmac_signature(
            signing_key, canonical_payload, str(artifact.signature)
        ):
            logger.critical(
                f"HMAC Verification Failed! Artifact {artifact.artifact_id} "
                f"- Key {kid}"
            )
            raise UnauthorizedRegistryTamperError(
                f"HMAC Signature match absolutely failed for Artifact "
                f"{artifact.artifact_id}. Execution halted."
            )

        logger.info(
            f"Verified WORM Boot HMAC Signature! Artifact {artifact.artifact_id} "
            f"- Key {kid}"
        )
        # -------------------------------------------------------------

        payload = artifact.artifact_blob

        # Hydrate JSON blob into strict internal typed Pydantic structures needed
        # by CompilerEngine
        tables_def = []
        relationships_def = []

        # Pre-pass: build a global column-ID → abstract alias map so relationship
        # target_column can be resolved without a second nested scan.
        col_id_to_alias: dict[str, str] = {}
        for tbl_dict in payload.get("tables", []):
            for col_dict in tbl_dict.get("columns", []):
                col_id = col_dict.get("id")
                if col_id:
                    col_id_to_alias[col_id] = col_dict["alias"]

        for tbl_dict in payload.get("tables", []):
            columns_def = []
            for col_dict in tbl_dict.get("columns", []):

                sc_extra = col_dict.get("safety_classification") or {}
                safety = SafetyClassification(
                    allowed_in_select=col_dict.get("allowed_in_select", False),
                    allowed_in_where=col_dict.get("allowed_in_filter", False),
                    allowed_in_group_by=sc_extra.get(
                        "allowed_in_group_by",
                        col_dict.get("allowed_in_select", False),
                    ),
                    aggregation_allowed=sc_extra.get(
                        "aggregation_allowed",
                        # Default: any selectable column may appear in aggregations.
                        # COUNT(col), COUNT(DISTINCT col), and CASE-inside-SUM
                        # patterns are valid for all types. Explicit
                        # safety_classification in the artifact overrides this
                        # for sensitive columns.
                        col_dict.get("allowed_in_select", False),
                    ),
                    join_participation_allowed=col_dict.get("allowed_in_join", False),
                )

                columns_def.append(
                    AbstractColumnDef(
                        alias=col_dict["alias"],
                        description=col_dict.get("description") or "",
                        data_type=col_dict.get("type", "text"),
                        safety=safety,
                        # Mapping conceptual alias directly to real name
                        physical_target=col_dict["name"],
                        sample_values=col_dict.get("sample_values") or [],
                    )
                )

            tables_def.append(
                AbstractTableDef(
                    alias=tbl_dict["alias"],
                    description=tbl_dict.get("description", ""),
                    physical_target=tbl_dict["name"],
                    columns=columns_def,
                    source_database=tbl_dict.get("source_database"),
                )
            )

            # Map relationships using the pre-built column-ID → alias map for
            # both ends.
            for rel_dict in tbl_dict.get("relationships", []):
                source_col = next(
                    (
                        c for c in tbl_dict.get("columns", [])
                        if c["id"] == rel_dict.get("source_column_id")
                    ),
                    None,
                )
                target_col_alias = col_id_to_alias.get(
                    rel_dict.get("target_column_id", ""), ""
                )

                relationships_def.append(
                    AbstractRelationshipDef(
                        source_table=tbl_dict["alias"],
                        source_column=source_col["alias"] if source_col else "",
                        target_table=rel_dict["target_table"],
                        target_column=target_col_alias,
                    )
                )

        return RegistrySchema(
            version=str(artifact.artifact_hash),
            tables=tables_def,
            relationships=relationships_def
        )

    @staticmethod
    async def load_active_schema(
        session: AsyncSession, tenant_id: str
    ) -> RegistrySchema | None:
        """Query the DB for the latest active artifact and hydrate it.

        For callers that already hold a consistent artifact snapshot, prefer
        load_schema_from_artifact() to avoid a redundant DB query.
        """
        # Load the most recent artifact whose parent version is still active
        # and belongs to the given tenant.
        stmt = (
            select(CompiledRegistryArtifact)
            .join(
                MetadataVersion,
                CompiledRegistryArtifact.version_id == MetadataVersion.version_id,
            )
            .where(
                MetadataVersion.status == "active",
                MetadataVersion.tenant_id == tenant_id,
            )
            .order_by(CompiledRegistryArtifact.compiled_at.desc())
            .limit(1)
        )

        result = await session.execute(stmt)
        artifact = result.scalar_one_or_none()

        if not artifact:
            return None

        return RegistryLoader.load_schema_from_artifact(artifact)
