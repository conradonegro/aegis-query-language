import pytest
import sqlglot

from app.compiler import AbstractQuery
from app.compiler.models import ValidatedAST
from app.compiler.parser import SQLParser
from app.compiler.safety import SafetyEngine, SafetyViolationError
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.steward import (
    AbstractColumnDef,
    AbstractRelationshipDef,
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


def _make_schema_with_relationship() -> RegistrySchema:
    """Two-table schema with a declared users→orders FK, used for JOIN tests."""
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
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            join_participation_allowed=True,
                        ),
                        physical_target="user_id",
                    ),
                ],
            ),
            AbstractTableDef(
                alias="orders",
                description="Orders",
                physical_target="public.orders",
                columns=[
                    AbstractColumnDef(
                        alias="user_id",
                        description="FK to users",
                        safety=SafetyClassification(
                            join_participation_allowed=True,
                        ),
                        physical_target="fk_user_id",
                    ),
                    AbstractColumnDef(
                        alias="total",
                        description="Total",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="order_total",
                    ),
                ],
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="users",
                source_column="id",
                target_table="orders",
                target_column="user_id",
            )
        ],
    )


def test_valid_join_passes() -> None:
    """A JOIN whose ON clause matches a declared relationship is accepted."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_relationship()

    ast = parser.parse(AbstractQuery(
        sql="SELECT users.id FROM users JOIN orders ON users.id = orders.user_id"
    ))
    validated = safety.validate(ast)
    executable = translator.translate(
        validated, schema,
        abstract_query_hash="h",
        relationships=schema.relationships,
    )
    assert executable is not None


def test_join_literal_predicate_blocked() -> None:
    """ON 1=1 contains no Column=Column equality — must be rejected."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_relationship()

    ast = parser.parse(AbstractQuery(
        sql="SELECT users.id FROM users JOIN orders ON 1=1"
    ))
    validated = safety.validate(ast)

    with pytest.raises(TranslationError, match="no column-equality condition"):
        translator.translate(
            validated, schema,
            abstract_query_hash="h",
            relationships=schema.relationships,
        )


def test_join_inequality_predicate_blocked() -> None:
    """ON a.id >= b.user_id has no EQ node — must be rejected."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_relationship()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT users.id FROM users"
            " JOIN orders ON users.id >= orders.user_id"
        )
    ))
    validated = safety.validate(ast)

    with pytest.raises(TranslationError, match="no column-equality condition"):
        translator.translate(
            validated, schema,
            abstract_query_hash="h",
            relationships=schema.relationships,
        )


def test_join_without_on_clause_blocked() -> None:
    """A CROSS JOIN (no ON clause) is rejected by the safety engine before
    reaching the translator; the translator's own check is defence-in-depth."""
    parser = SQLParser()
    safety = SafetyEngine()

    ast = parser.parse(AbstractQuery(
        sql="SELECT users.id FROM users CROSS JOIN orders"
    ))
    with pytest.raises(SafetyViolationError, match="cross JOIN"):
        safety.validate(ast)


# ------------------------------------------------------------------
# LLM-supplied placeholder regression tests
# ------------------------------------------------------------------

def _minimal_schema() -> RegistrySchema:
    """One-table, one-column schema for placeholder regression tests."""
    return RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="Users",
                physical_target="public.users",
                columns=[
                    AbstractColumnDef(
                        alias="name",
                        description="User name",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                        ),
                        physical_target="full_name",
                    )
                ],
            )
        ],
        relationships=[],
    )


def test_llm_named_placeholder_rejected_by_safety() -> None:
    """SQL with :p1 style placeholder from LLM output must be rejected by
    SafetyEngine with the explicit 'Explicitly denied node type' message."""
    parser = SQLParser()
    safety = SafetyEngine()

    # sqlglot parses :p1 as exp.Placeholder
    ast = parser.parse(AbstractQuery(
        sql="SELECT name FROM users WHERE name = :p1"
    ))
    with pytest.raises(SafetyViolationError, match="(?i)denied"):
        safety.validate(ast)


def test_llm_positional_placeholder_rejected_by_safety() -> None:
    """SQL with $1 style placeholder from LLM output must be rejected by
    SafetyEngine with the explicit 'Explicitly denied node type' message."""
    parser = SQLParser()
    safety = SafetyEngine()

    # sqlglot parses $1 as exp.Parameter
    ast = parser.parse(AbstractQuery(
        sql="SELECT name FROM users WHERE name = $1"
    ))
    with pytest.raises(SafetyViolationError, match="(?i)denied"):
        safety.validate(ast)


def test_string_literal_containing_placeholder_text_passes() -> None:
    """A string literal whose value happens to look like ':p1' is not a
    bind parameter and must pass safety validation and translate correctly."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _minimal_schema()

    # The value ':p1' is a plain string, not a bind parameter.
    ast = parser.parse(AbstractQuery(
        sql="SELECT name FROM users WHERE name = ':p1'"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    # The literal ':p1' must be bound as a parameter value, not treated
    # as a placeholder reference.
    assert ":p1" in result.sql
    assert result.parameters.get("p1") == ":p1"


def test_translator_guard_raises_on_placeholder_if_safety_bypassed() -> None:
    """Belt-and-suspenders: translator must raise TranslationError if a
    Parameter/Placeholder node somehow reaches it despite SafetyEngine."""
    translator = DeterministicTranslator()
    schema = _minimal_schema()

    # Manually construct a ValidatedAST that contains a Placeholder node,
    # simulating a future scenario where SafetyEngine is bypassed.
    tree = sqlglot.parse_one("SELECT name FROM users WHERE name = :p1")
    assert tree is not None
    validated = ValidatedAST(tree=tree)

    with pytest.raises(TranslationError, match="Pre-translation bind parameter"):
        translator.translate(validated, schema, abstract_query_hash="h")
