from app.compiler.filter import DeterministicSchemaFilter
from app.compiler import UserIntent
from app.steward import (
    AbstractIdentifierDef,
    RegistrySchema,
    SafetyClassification,
)


def test_deterministic_schema_filter_simple_overlap() -> None:
    filter_engine = DeterministicSchemaFilter(cutoff_threshold=1)

    schema = RegistrySchema(
        version="v1.0.0",
        identifiers=[
            AbstractIdentifierDef(
                alias="users",
                description="User accounts and profiles",
                safety=SafetyClassification(allowed_in_select=True),
                physical_target="auth.users"
            ),
            AbstractIdentifierDef(
                alias="orders",
                description="Purchase history",
                safety=SafetyClassification(allowed_in_select=True),
                physical_target="sales.orders"
            )
        ]
    )

    intent = UserIntent(natural_language_query="Show me the profile for user 123")
    filtered = filter_engine.filter(intent, schema)

    # "profile" / "user" -> matches users table
    # but not "orders"
    assert len(filtered.active_identifiers) == 1
    assert filtered.active_identifiers[0].alias == "users"

    assert len(filtered.omitted_identifiers) == 1
    assert "orders" in filtered.omitted_identifiers
