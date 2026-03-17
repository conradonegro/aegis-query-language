import pytest

from app.compiler import AbstractQuery
from app.compiler.parser import SQLParser
from app.compiler.safety import SafetyEngine
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.steward import (
    AbstractColumnDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)


def test_deterministic_translation() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()

    # 1. Provide the physical mappings in the schema
    schema = RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="table1",
                description="The Orders Table",
                physical_target="public.orders_v2",
                columns=[
                    AbstractColumnDef(
                        alias="col1",
                        description="Total Sales",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="net_total"
                    ),
                    AbstractColumnDef(
                        alias="col2",
                        description="Status",
                        safety=SafetyClassification(allowed_in_where=True),
                        physical_target="order_status"
                    )
                ]
            )
        ],
        relationships=[]
    )

    # 2. Parse and Validate the Abstract Query (LLM Output)
    ast = parser.parse(
        AbstractQuery(sql="SELECT col1 FROM table1 WHERE col2 = 'Shipped'")
    )
    validated = safety.validate(ast)

    # 3. Translate
    executable = translator.translate(
        validated, schema,
        abstract_query_hash="mock_hash_123"
    )

    # ExecutableSQL should map aliases to physical targets
    assert "public.orders_v2" in executable.sql
    assert "net_total" in executable.sql
    assert "order_status" in executable.sql

    # Abstract names should be completely gone
    assert "table1" not in executable.sql
    assert "col1" not in executable.sql
    assert "col2" not in executable.sql

    # Value should be parameterized
    assert "'Shipped'" not in executable.sql
    assert len(executable.parameters) == 1

    # Ensure copy-on-write preserved original AST
    # The original AST should still format back to the abstract names
    original_sql = validated.tree.sql(dialect="postgres")
    assert "table1" in original_sql
    assert "public.orders_v2" not in original_sql


def _make_schema() -> RegistrySchema:
    """Minimal two-table schema reused by the enforcement tests below."""
    return RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="Users",
                physical_target="public.users",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="ID",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="user_id",
                    ),
                    AbstractColumnDef(
                        alias="salary",
                        description="Salary",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="salary_cents",
                    ),
                ],
            ),
            AbstractTableDef(
                alias="orders",
                description="Orders",
                physical_target="public.orders",
                columns=[
                    AbstractColumnDef(
                        alias="total",
                        description="Total",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="order_total",
                    ),
                ],
            ),
        ],
        relationships=[],
    )


def test_unknown_column_raises_translation_error() -> None:
    """A column absent from the schema must raise TranslationError, not pass
    through to the database where it could resolve against a physical column."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(sql="SELECT secret_col FROM users"))
    validated = safety.validate(ast)

    with pytest.raises(TranslationError, match="does not exist in the schema context"):
        translator.translate(validated, schema, abstract_query_hash="h")


def test_column_from_out_of_scope_table_raises_translation_error() -> None:
    """A column that exists in the schema but whose owning table is not
    referenced in the FROM clause must raise TranslationError, not silently
    resolve without a safety check."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    # 'salary' belongs to 'users', but only 'orders' is in the FROM clause.
    ast = parser.parse(AbstractQuery(sql="SELECT salary FROM orders"))
    validated = safety.validate(ast)

    with pytest.raises(TranslationError, match="owning table"):
        translator.translate(validated, schema, abstract_query_hash="h")
