from app.compiler import UserIntent, ValueMatchResult
from app.steward import (
    AbstractColumnDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)


def test_registry_schema_creation() -> None:
    """Test generating a registry schema with an abstract identifier."""
    schema = RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="The users table",
                physical_target="users",
                columns=[
                    AbstractColumnDef(
                        alias="column1",
                        description="User's First Name",
                        safety=SafetyClassification(
                            allowed_in_where=True,
                            allowed_in_select=True,
                        ),
                        physical_target="first_name"
                    )
                ]
            )
        ],
        relationships=[]
    )
    assert schema.version == "v1.0.0"
    assert len(schema.tables) == 1
    assert schema.tables[0].columns[0].alias == "column1"
    assert schema.tables[0].columns[0].safety.allowed_in_select is True
    assert schema.tables[0].columns[0].safety.aggregation_allowed is False

def test_compiler_domain_models() -> None:
    """Test creating baseline compiler models to ensure Pydantic parsing succeeds."""
    intent = UserIntent(natural_language_query="Show me the revenue")
    assert intent.natural_language_query == "Show me the revenue"

    match = ValueMatchResult(status="success", matches=["column1"])
    assert match.status == "success"
    assert "column1" in match.matches
