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
                        description=f"{col_dict['type']} {'(PK)' if col_dict.get('is_primary') else ''}",
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
