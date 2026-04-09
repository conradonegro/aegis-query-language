import pytest
import sqlglot

from app.compiler import AbstractQuery
from app.compiler.models import ValidatedAST
from app.compiler.parser import SQLParser
from app.compiler.safety import (
    SafetyEngine,
    SafetyViolationError,
    UnsafeExpressionError,
)
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


# ------------------------------------------------------------------
# Fix 1 — Numeric literal parameterization
# ------------------------------------------------------------------

def test_integer_literals_remain_inline() -> None:
    """Integer literals must not be parameterized.

    asyncpg sends Python int for bound integer parameters, but PostgreSQL
    can infer TEXT from context (e.g. THEN 1 inside a CASE expression)
    and raises DataError. Leaving integers inline avoids this entirely.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql="SELECT CASE WHEN salary > 0 THEN 1 ELSE 0 END FROM users"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "1" in result.sql
    assert "0" in result.sql
    assert not any(isinstance(v, int) for v in result.parameters.values())


def test_string_literals_still_parameterized() -> None:
    """String literals must still be bound as parameters for injection safety."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _minimal_schema()

    ast = parser.parse(AbstractQuery(
        sql="SELECT name FROM users WHERE name = 'alice'"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "'alice'" not in result.sql
    assert result.parameters.get("p1") == "alice"


# ------------------------------------------------------------------
# Fix 3 — CTE-aware translator
# ------------------------------------------------------------------

def test_cte_virtual_table_resolves_without_error() -> None:
    """A CTE alias used as a FROM target must not raise TranslationError."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql="WITH top_users AS (SELECT id FROM users) SELECT id FROM top_users"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_cte_column_prefix_resolves_without_error() -> None:
    """CTE-prefixed column reference in outer query must not raise TranslationError."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH top_users AS (SELECT id FROM users) "
            "SELECT top_users.id FROM top_users"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_cte_prefixed_alias_only_output_column_resolves_without_error() -> None:
    """A CTE-prefixed reference to an alias-only output column (no underlying
    schema column with the same name) must not raise TranslationError.

    This is the exact BIRD failure pattern from q=1479/1480: the LLM emits
    `agg.total_consumption` after defining `SUM(consumption) AS total_consumption`
    inside a CTE. The bypass at translator._resolve_column_with_prefix must
    short-circuit on the CTE name BEFORE attempting physical column lookup,
    because `total_consumption` does not exist in any schema table.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    # Self-contained schema: salary must be aggregation-allowed so the inner
    # CTE body's SUM(salary) clears safety. Other tests rely on the shared
    # _make_schema() not permitting aggregation on salary, so we use a local
    # schema instead of perturbing it.
    schema = RegistrySchema(
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
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            aggregation_allowed=True,
                        ),
                        physical_target="salary_cents",
                    ),
                ],
            ),
        ],
        relationships=[],
    )

    # `aggregate_total` is NOT a schema column anywhere — it only exists as the
    # AS-declared output of the CTE body's SUM expression. The outer query
    # references it via the CTE alias prefix.
    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH agg AS (SELECT SUM(salary) AS aggregate_total FROM users) "
            "SELECT agg.aggregate_total FROM agg "
            "ORDER BY agg.aggregate_total DESC"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None
    # The CTE alias and its output column must survive in the final SQL —
    # they have no physical counterpart and must not be rewritten.
    assert "aggregate_total" in result.sql
    assert "agg" in result.sql


def _make_schema_with_aggregation() -> RegistrySchema:
    """Schema where salary allows aggregation and id allows group_by,
    used by the SELECT-alias bypass tests."""
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
                            allowed_in_group_by=True,
                        ),
                        physical_target="user_id",
                    ),
                    AbstractColumnDef(
                        alias="salary",
                        description="Salary",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            aggregation_allowed=True,
                        ),
                        physical_target="salary_cents",
                    ),
                ],
            ),
        ],
        relationships=[],
    )


def test_select_alias_in_order_by_resolves_without_error() -> None:
    """A top-level SELECT alias (e.g. SUM(...) AS total_consumption) referenced
    in ORDER BY must not raise TranslationError.

    This is the actual BIRD failure mode for q=1479/q=1480: the LLM emits
    `SELECT SUM(consumption) AS total_consumption ... ORDER BY total_consumption`
    and the translator rejects `total_consumption` because it's not a schema
    column and not a CTE output alias. The fix is to also collect AS-declared
    aliases from all SELECT nodes, not just CTE bodies.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_aggregation()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT users.id, SUM(users.salary) AS total_salary"
            " FROM users"
            " GROUP BY users.id"
            " ORDER BY total_salary DESC"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None
    assert "total_salary" in result.sql


def test_select_alias_in_having_resolves_without_error() -> None:
    """A SELECT alias referenced in HAVING must also be bypassed."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_aggregation()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT users.id, SUM(users.salary) AS total_salary"
            " FROM users"
            " GROUP BY users.id"
            " HAVING total_salary > 0"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None
    assert "total_salary" in result.sql


# ------------------------------------------------------------------
# BUG-6 — EXTRACT on CAST expression
# ------------------------------------------------------------------

def _make_schema_with_text_date() -> RegistrySchema:
    """Schema with a TEXT column that stores ISO date strings.

    Models the debit_card_specializing.transactions_1k.date scenario where the
    underlying column is TEXT but contains parseable date strings, requiring
    a CAST before temporal extraction.
    """
    return RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="txns",
                description="Transactions with text-encoded dates",
                physical_target="public.transactions",
                columns=[
                    AbstractColumnDef(
                        alias="txn_date",
                        description="ISO date stored as text",
                        data_type="text",
                        safety=SafetyClassification(
                            allowed_in_select=True, allowed_in_where=True
                        ),
                        physical_target="txn_date",
                    ),
                    AbstractColumnDef(
                        alias="amount",
                        description="Numeric amount",
                        data_type="numeric",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="amount",
                    ),
                ],
            )
        ],
        relationships=[],
    )


def test_extract_year_from_cast_text_to_date_passes() -> None:
    """EXTRACT(YEAR FROM CAST(text_col AS DATE)) must be permitted.

    The CAST target type is DATE, so the resulting expression IS temporal,
    regardless of the source column's declared type. The temporal validator
    must inspect the cast target rather than insisting on a bare Column.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_text_date()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT EXTRACT(YEAR FROM CAST(txn_date AS DATE)) AS yr"
            " FROM txns"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None
    assert "EXTRACT" in result.sql.upper()


def test_extract_year_from_cast_text_to_text_rejected() -> None:
    """EXTRACT(YEAR FROM CAST(text_col AS TEXT)) must be rejected.

    The cast target is TEXT, not a temporal type, so the resulting
    expression is not temporal. PostgreSQL would reject this at runtime;
    we reject it at compile time for a better error message.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_text_date()

    ast = parser.parse(AbstractQuery(
        sql="SELECT EXTRACT(YEAR FROM CAST(txn_date AS TEXT)) FROM txns"
    ))
    validated = safety.validate(ast)
    with pytest.raises(
        UnsafeExpressionError, match="does not resolve to a temporal"
    ):
        translator.translate(validated, schema, abstract_query_hash="h")


def test_extract_year_from_cast_text_to_timestamp_passes() -> None:
    """EXTRACT(YEAR FROM CAST(text_col AS TIMESTAMP)) must be permitted."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_text_date()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT EXTRACT(YEAR FROM CAST(txn_date AS TIMESTAMP)) FROM txns"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_extract_on_bare_temporal_column_still_passes() -> None:
    """Regression: EXTRACT on a bare DATE column must still work."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()  # has events.event_date DATE

    ast = parser.parse(AbstractQuery(
        sql="SELECT EXTRACT(YEAR FROM event_date) FROM events"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_extract_on_bare_text_column_still_rejected() -> None:
    """Regression: EXTRACT on a bare TEXT column must still be rejected."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_text_date()

    ast = parser.parse(AbstractQuery(
        sql="SELECT EXTRACT(YEAR FROM txn_date) FROM txns"
    ))
    validated = safety.validate(ast)
    with pytest.raises(
        UnsafeExpressionError, match="does not resolve to a temporal"
    ):
        translator.translate(validated, schema, abstract_query_hash="h")


def test_extract_with_subquery_still_blocked() -> None:
    """Regression: EXTRACT with a nested SELECT must remain blocked."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()

    # sqlglot will parse this as EXTRACT containing a Subquery
    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT EXTRACT(YEAR FROM (SELECT event_date FROM events LIMIT 1))"
            " FROM events"
        )
    ))
    validated = safety.validate(ast)
    with pytest.raises(UnsafeExpressionError, match="subqueries"):
        translator.translate(validated, schema, abstract_query_hash="h")


# ------------------------------------------------------------------
# BUG-1 — Temporal literal parameterization
# ------------------------------------------------------------------

def _make_schema_with_dates() -> RegistrySchema:
    """Schema with a date column and a timestamp column for temporal tests."""
    return RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="events",
                description="Events",
                physical_target="public.events",
                columns=[
                    AbstractColumnDef(
                        alias="event_date",
                        description="Event date",
                        data_type="date",
                        safety=SafetyClassification(
                            allowed_in_select=True, allowed_in_where=True
                        ),
                        physical_target="event_date",
                    ),
                    AbstractColumnDef(
                        alias="created_at",
                        description="Created at",
                        data_type="timestamp",
                        safety=SafetyClassification(
                            allowed_in_select=True, allowed_in_where=True
                        ),
                        physical_target="created_at",
                    ),
                    AbstractColumnDef(
                        alias="label",
                        description="Label",
                        data_type="text",
                        safety=SafetyClassification(
                            allowed_in_select=True, allowed_in_where=True
                        ),
                        physical_target="label",
                    ),
                ],
            )
        ],
        relationships=[],
    )


def test_date_literal_in_equality_left_inline() -> None:
    """A string literal compared to a DATE column must not be parameterized.

    asyncpg infers the bind parameter type from the column being compared.
    For DATE columns it expects a Python datetime.date and calls .toordinal(),
    crashing with AttributeError when given a plain string. Leaving the
    literal inline lets PostgreSQL parse it directly.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()

    ast = parser.parse(AbstractQuery(
        sql="SELECT event_date FROM events WHERE event_date = '2024-01-15'"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "'2024-01-15'" in result.sql
    assert not any(v == "2024-01-15" for v in result.parameters.values())


def test_timestamp_literal_in_comparison_left_inline() -> None:
    """A string literal compared to a TIMESTAMP column must not be parameterized."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()

    ast = parser.parse(AbstractQuery(
        sql="SELECT created_at FROM events WHERE created_at > '2024-01-15 10:00:00'"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "'2024-01-15 10:00:00'" in result.sql
    assert not any(v == "2024-01-15 10:00:00" for v in result.parameters.values())


def test_date_literal_in_between_left_inline() -> None:
    """Both bounds of a BETWEEN against a DATE column must stay inline."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()

    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT event_date FROM events"
            " WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31'"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "'2024-01-01'" in result.sql
    assert "'2024-12-31'" in result.sql
    assert len(result.parameters) == 0


def test_text_literal_still_parameterized_despite_date_column_in_scope() -> None:
    """A string literal compared to a TEXT column must still be parameterized,
    even when a temporal column exists elsewhere in the same query.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_dates()

    ast = parser.parse(AbstractQuery(
        sql="SELECT label FROM events WHERE label = 'conference'"
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")

    assert "'conference'" not in result.sql
    assert result.parameters.get("p1") == "conference"


def test_cte_join_on_condition_resolves_without_error() -> None:
    """JOIN ON referencing a CTE virtual table must not raise TranslationError."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_relationship()

    # CTE wraps users; outer query JOINs orders using the CTE alias.
    # The JOIN ON condition references the CTE alias (user_ids.id) — this
    # side of the condition must be exempt from edge-index validation.
    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH user_ids AS (SELECT id FROM users) "
            "SELECT user_ids.id, orders.total "
            "FROM user_ids JOIN orders ON user_ids.id = orders.user_id"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(
        validated, schema,
        abstract_query_hash="h",
        relationships=schema.relationships,
    )
    assert result is not None


# ------------------------------------------------------------------
# BUG-3 — CTE output column alias bypass
# ------------------------------------------------------------------

def test_cte_output_alias_in_order_by_resolves() -> None:
    """A bare CTE-derived alias referenced in ORDER BY must not raise
    TranslationError.

    The LLM commonly writes aggregation CTEs like:
        WITH agg AS (SELECT SUM(x) AS total FROM t GROUP BY ...)
        SELECT col FROM agg ORDER BY total DESC
    'total' is not a schema column — it is the AS-declared output of the CTE.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH agg AS (SELECT id, salary AS total_salary FROM users)"
            " SELECT id FROM agg ORDER BY total_salary DESC"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_cte_output_alias_in_select_resolves() -> None:
    """A bare CTE-derived alias referenced in the outer SELECT must not raise."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH agg AS (SELECT id, salary AS earnings FROM users)"
            " SELECT id, earnings FROM agg"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_cte_output_alias_in_where_resolves() -> None:
    """A bare CTE-derived alias used in WHERE must not raise."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH agg AS (SELECT id, salary AS total_salary FROM users)"
            " SELECT id FROM agg WHERE total_salary > 0"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(validated, schema, abstract_query_hash="h")
    assert result is not None


def test_real_schema_column_still_raises_when_not_in_scope() -> None:
    """A column that genuinely does not exist in the schema must still raise,
    even when CTE column aliases are present."""
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema()

    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH agg AS (SELECT id FROM users)"
            " SELECT ghost_column FROM agg"
        )
    ))
    validated = safety.validate(ast)
    with pytest.raises(TranslationError, match="does not exist in the schema context"):
        translator.translate(validated, schema, abstract_query_hash="h")
