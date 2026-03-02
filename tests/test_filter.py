from app.compiler.filter import DeterministicSchemaFilter
from app.compiler import UserIntent
from app.steward import (
    AbstractTableDef,
    AbstractColumnDef,
    RegistrySchema,
    SafetyClassification,
)


def test_deterministic_schema_filter_simple_overlap() -> None:
    filter_engine = DeterministicSchemaFilter(cutoff_threshold=1)

    schema = RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="User accounts and profiles",
                physical_target="auth.users",
                columns=[
                    AbstractColumnDef(alias="id", description="ID", safety=SafetyClassification(allowed_in_select=True), physical_target="auth.users.id")
                ]
            ),
            AbstractTableDef(
                alias="orders",
                description="Purchase history",
                physical_target="sales.orders",
                columns=[
                    AbstractColumnDef(alias="id", description="ID", safety=SafetyClassification(allowed_in_select=True), physical_target="sales.orders.id")
                ]
            )
        ],
        relationships=[]
    )

    intent = UserIntent(natural_language_query="Show me the profile for user 123")
    filtered = filter_engine.filter_schema(intent, schema)

    # "profile" / "user" -> matches users table
    # but not "orders"
    assert len(filtered.tables) == 1
    assert filtered.tables[0].alias == "users"

    # orders structure is entirely omitted, 
    # and users columns are kept because the parent table matched.
    assert "orders" not in [t.alias for t in filtered.tables]
