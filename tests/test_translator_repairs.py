import pytest
import sqlglot
from sqlglot import exp

from app.compiler.models import SQLAst, ValidatedAST
from app.compiler.safety import (
    SafetyEngine,
    SafetyPolicyViolationError,
    SafetyViolationError,
    UnsafeExpressionError,
)
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.steward.models import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)


@pytest.fixture
def translator() -> DeterministicTranslator:
    return DeterministicTranslator()

@pytest.fixture
def mock_schema() -> RegistrySchema:
    return RegistrySchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="users",
                physical_target="phys_users",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="user id", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                            allowed_in_group_by=True,
                            aggregation_allowed=True,
                            join_participation_allowed=True,
                        ),
                    ),
                    AbstractColumnDef(
                        alias="name", description="name", physical_target="name",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                            allowed_in_group_by=True,
                        ),
                    ),
                ]
            ),
            AbstractTableDef(
                alias="orders",
                description="orders",
                physical_target="phys_orders",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="order id", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                            join_participation_allowed=True,
                        ),
                    ),
                    AbstractColumnDef(
                        alias="total_amount",
                        description="total",
                        physical_target="total_amount",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            aggregation_allowed=True,
                        ),
                    ),
                    AbstractColumnDef(
                        alias="name",
                        description="order name",
                        physical_target="name",  # INTENTIONAL COLLISION
                        safety=SafetyClassification(allowed_in_select=True),
                    ),
                    AbstractColumnDef(
                        alias="created_at",
                        description="date",
                        data_type="timestamp",
                        physical_target="created_at",
                        safety=SafetyClassification(allowed_in_select=True),
                    ),
                ]
            ),
        ],
        relationships=[]
    )


def test_orphaned_alias_with_unique_scoped_column_repaired(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # 'total_amount' only exists on 'orders' table.
    # LLM hallucinates `o.` prefix but forgot `orders AS o`.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT o.total_amount FROM orders"))

    executable = translator.translate(ast, mock_schema)

    assert "phys_orders.total_amount" in executable.sql
    assert len(executable.translation_repairs) == 1
    repair = executable.translation_repairs[0]
    assert repair.type == "orphaned_alias"
    assert repair.original == "o.total_amount"
    assert repair.resolved_to == "phys_orders.total_amount"


def test_orphaned_alias_ambiguous_column_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # 'name' exists on both 'users' and 'orders'.
    # LLM hallucinates `x.` prefix but forgot `AS x`.
    # Since both tables are in scope, guess is unsafe.
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT x.name FROM orders JOIN users ON users.id = orders.id"
        )
    )

    with pytest.raises(
        TranslationError, match="Ambiguous orphaned prefix 'x' for column 'name'"
    ):
        translator.translate(ast, mock_schema)


def test_orphaned_alias_column_not_in_scope_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # 'total_amount' only exists on 'orders'.
    # LLM hallucinates `o.total_amount` but queries the 'users' table.
    # Since 'orders' is not in the scope of the FROM, repair is impossible/unsafe.
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT o.total_amount FROM users")
    )

    with pytest.raises(
        TranslationError,
        match="does not belong to any table formally declared in scope",
    ):
        translator.translate(ast, mock_schema)


def test_prefix_refers_to_non_existent_table_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # Prefix 'y' is firmly declared in FROM clause (`FROM unknown_table y`)
    # The physical mapping will fail directly. Isolating to 'id' to ensure
    # missing table, not missing column, triggers failure.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT y.id FROM unknown y"))

    # We never make it to column traversal if we fail base table existence,
    # or it fails to find the column.
    with pytest.raises(
        TranslationError,
        match="Table 'unknown' does not exist in schema context",
    ):
        translator.translate(ast, mock_schema)

def test_explicit_prefix_non_owning_table_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # Prefix exists, but explicitly binds to a table that doesn't own the
    # column. LLM explicitly aliases `users u` and asks for `u.total_amount`.
    # Even though `total_amount` is uniquely in `orders`, the explicit override
    # binding to `users` must fail strictly instead of heuristically grabbing
    # `orders`.
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT u.total_amount FROM users u JOIN orders o ON u.id = o.id"
        )
    )

    with pytest.raises(
        TranslationError,
        match="Column 'users.total_amount' does not exist in schema context",
    ):
        translator.translate(ast, mock_schema)

def test_explicit_dialect_alias_replaces_base_table_prefix(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # SQLite/Postgres strictly fail if you query `users.name` running
    # `FROM users u`. The translator MUST extract the explicit query `u` alias,
    # identify that `users` is the table, and remap the column prefix down to
    # `u`.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT users.name FROM users u"))

    executable = translator.translate(ast, mock_schema)
    assert "u.name" in executable.sql
    assert "users.name" not in executable.sql

def test_where_aggregation_relocated_to_having(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # Standard COUNT in WHERE should be moved to HAVING, leaving other WHEREs
    # intact
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.id"
        " WHERE u.id > 1 AND SUM(o.total_amount) > 100 GROUP BY u.name"
    ))

    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()

    # Assert WHERE has the non-aggregate and HAVING has the aggregate.
    # Numeric literals are left inline (not parameterized).
    assert "WHERE U.ID > 1" in sql
    assert "HAVING SUM(O.TOTAL_AMOUNT) > 100" in sql
    assert "SUM(" not in sql.split("GROUP BY")[0] # Double check it didn't stay in WHERE

    assert len(executable.translation_repairs) == 1
    assert executable.translation_repairs[0].type == "where_aggregation_relocation"

def test_where_aggregation_skipped_for_subqueries(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # A SUM(x) > (SELECT ...) should NOT be moved, because subqueries often
    # bind weirdly or cause scoping issues.
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT u.name FROM users u WHERE SUM(u.id) > (SELECT 5) GROUP BY u.name"
    ))

    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()

    assert "WHERE SUM(U.ID) > (SELECT 5)" in sql
    assert "HAVING" not in sql

def test_where_aggregation_skipped_for_windows(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # Window functions (e.g. SUM() OVER ()) are NOT valid in HAVING. Must NOT
    # be moved.
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT u.name FROM users u"
        " WHERE SUM(u.id) OVER (PARTITION BY u.name) > 5 GROUP BY u.name"
    ))

    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()

    assert "WHERE SUM(U.ID) OVER (PARTITION BY U.NAME) > 5" in sql
    assert "HAVING" not in sql

def test_where_aggregation_skipped_for_or(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # If the WHERE tree contains an OR condition anywhere, the boolean grouping
    # order would be corrupted by extracting the aggregate into HAVING.
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT u.name FROM users u"
        " WHERE SUM(u.id) > 100 OR u.id = 5 GROUP BY u.name"
    ))

    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()

    assert "WHERE SUM(U.ID) > 100 OR U.ID = 5" in sql
    assert "HAVING" not in sql
    assert len(executable.translation_repairs) == 0

def test_where_aggregation_skipped_for_mixed_nodes(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    # If a WHERE leaf condition compares an aggregate against a naked column
    # instead of a scalar, moving it to HAVING would violate PostgreSQL
    # grouping context logic because `u.id` is not grouped.
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.id"
        " WHERE SUM(o.total_amount) > u.id GROUP BY u.name"
    ))

    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()

    assert "WHERE SUM(O.TOTAL_AMOUNT) > U.ID" in sql
    assert "HAVING" not in sql
    assert len(executable.translation_repairs) == 0

def test_extract_valid_temporal_column(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT EXTRACT(MONTH FROM o.created_at) FROM orders o")
    )
    executable = translator.translate(ast, mock_schema)
    sql = executable.sql.upper()
    assert (
        "EXTRACT(MONTH FROM O.CREATED_AT)" in sql
        or "DATE_PART('MONTH', O.CREATED_AT)" in sql
    )

def test_extract_invalid_non_temporal_column(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT EXTRACT(MONTH FROM o.total_amount) FROM orders o"
        )
    )
    with pytest.raises(
        UnsafeExpressionError, match="only permitted on temporal columns"
    ):
        translator.translate(ast, mock_schema)

def test_extract_invalid_literal_source(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT EXTRACT(MONTH FROM '2023-01-01') FROM orders o"
        )
    )
    with pytest.raises(
        UnsafeExpressionError, match="must be natively bound to a column"
    ):
        translator.translate(ast, mock_schema)

def test_extract_nested_subquery_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT EXTRACT(MONTH FROM (SELECT created_at FROM orders LIMIT 1))"
        )
    )
    with pytest.raises(
        UnsafeExpressionError, match="must be natively bound to a column"
    ):
        translator.translate(ast, mock_schema)

def test_interval_allowed_and_translated(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT o.created_at + INTERVAL '1 day' FROM orders o"
        )
    )
    executable = translator.translate(ast, mock_schema)
    assert "INTERVAL" in executable.sql.upper()

def test_interval_nested_subquery_fails(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT o.created_at + INTERVAL '1 day' FROM orders o"
        )
    )
    # Manually inject a dangerous node into the parsed AST since strict parsers
    # might block this natively
    interval_node = list(ast.tree.find_all(exp.Interval))[0]
    interval_node.this.replace(sqlglot.parse_one("(SELECT '1 day')"))

    with pytest.raises(
        UnsafeExpressionError,
        match=(
            "Nested subqueries or window constructs are strictly blocked"
            " inside INTERVAL"
        ),
    ):
        translator.translate(ast, mock_schema)


# ---------------------------------------------------------------------------
# SafetyClassification enforcement tests
# ---------------------------------------------------------------------------

@pytest.fixture
def restricted_schema() -> RegistrySchema:
    """Schema with tightly controlled per-column safety flags."""
    return RegistrySchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="accounts",
                description="accounts",
                physical_target="phys_accounts",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="pk", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                            join_participation_allowed=True,
                        ),
                    ),
                    AbstractColumnDef(
                        alias="secret",
                        description="sensitive field",
                        physical_target="secret",
                        safety=SafetyClassification(),  # all False — fully blocked
                    ),
                    AbstractColumnDef(
                        alias="balance",
                        description="numeric balance",
                        physical_target="balance",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            # aggregation_allowed intentionally False
                        ),
                    ),
                    AbstractColumnDef(
                        alias="category",
                        description="category",
                        physical_target="category",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                            # join_participation_allowed intentionally False
                        ),
                    ),
                ]
            ),
        ],
        relationships=[]
    )


def test_select_blocked_column_raises(
    translator: DeterministicTranslator, restricted_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT accounts.secret FROM accounts")
    )
    with pytest.raises(SafetyPolicyViolationError, match="not permitted in SELECT"):
        translator.translate(ast, restricted_schema)


def test_where_blocked_column_raises(
    translator: DeterministicTranslator, restricted_schema: RegistrySchema
) -> None:
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT accounts.id FROM accounts WHERE accounts.secret = 'x'"
        )
    )
    with pytest.raises(SafetyPolicyViolationError, match="not permitted in WHERE"):
        translator.translate(ast, restricted_schema)


def test_aggregation_blocked_column_raises(
    translator: DeterministicTranslator, restricted_schema: RegistrySchema
) -> None:
    # balance has allowed_in_select=True but aggregation_allowed=False
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT SUM(accounts.balance) FROM accounts")
    )
    with pytest.raises(
        SafetyPolicyViolationError, match="not permitted inside aggregation"
    ):
        translator.translate(ast, restricted_schema)


def test_join_blocked_column_raises(
    translator: DeterministicTranslator, restricted_schema: RegistrySchema
) -> None:
    # category has join_participation_allowed=False — block it when used in
    # JOIN ON
    join_schema = RegistrySchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="accounts", description="accounts",
                physical_target="phys_accounts",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="pk", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True, join_participation_allowed=True,
                        ),
                    ),
                    AbstractColumnDef(
                        alias="category", description="category",
                        physical_target="category",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            # join_participation_allowed intentionally False
                        ),
                    ),
                ]
            ),
            AbstractTableDef(
                alias="tags", description="tags", physical_target="phys_tags",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="pk", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True, join_participation_allowed=True,
                        ),
                    ),
                ]
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="accounts", source_column="category",
                target_table="tags", target_column="id",
            )
        ],
    )
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT a.id FROM accounts a JOIN tags t ON a.category = t.id"
    ))
    with pytest.raises(SafetyPolicyViolationError, match="not permitted in JOIN"):
        translator.translate(
            ast, join_schema, relationships=join_schema.relationships
        )


# ---------------------------------------------------------------------------
# Relationship graph JOIN validation tests
# ---------------------------------------------------------------------------

@pytest.fixture
def join_graph_schema() -> RegistrySchema:
    """Schema with two tables linked by a declared relationship."""
    return RegistrySchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="posts", description="blog posts",
                physical_target="phys_posts",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="pk", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True, join_participation_allowed=True
                        ),
                    ),
                    AbstractColumnDef(
                        alias="author_id", description="fk to users",
                        physical_target="author_id",
                        safety=SafetyClassification(
                            allowed_in_select=True, join_participation_allowed=True
                        ),
                    ),
                    AbstractColumnDef(
                        alias="title", description="title", physical_target="title",
                        safety=SafetyClassification(allowed_in_select=True),
                    ),
                ]
            ),
            AbstractTableDef(
                alias="users", description="users", physical_target="phys_users",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="pk", physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True, join_participation_allowed=True
                        ),
                    ),
                    AbstractColumnDef(
                        alias="name", description="name", physical_target="name",
                        safety=SafetyClassification(allowed_in_select=True),
                    ),
                ]
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="posts", source_column="author_id",
                target_table="users", target_column="id",
            )
        ],
    )


def test_join_on_declared_relationship_passes(
    translator: DeterministicTranslator, join_graph_schema: RegistrySchema
) -> None:
    """A JOIN whose ON columns match a declared relationship edge must succeed."""
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT p.title, u.name FROM posts p JOIN users u ON p.author_id = u.id"
    ))
    executable = translator.translate(
        ast, join_graph_schema, relationships=join_graph_schema.relationships
    )
    assert "phys_posts" in executable.sql
    assert "phys_users" in executable.sql


def test_join_on_undeclared_edge_raises(
    translator: DeterministicTranslator, join_graph_schema: RegistrySchema
) -> None:
    """A JOIN whose ON columns do NOT match any declared edge must be rejected."""
    ast = ValidatedAST(tree=sqlglot.parse_one(
        # Hallucinated: joining on title = name — no such relationship declared
        "SELECT p.title FROM posts p JOIN users u ON p.title = u.name"
    ))
    with pytest.raises(TranslationError, match="Hallucinated JOIN blocked"):
        translator.translate(
            ast, join_graph_schema, relationships=join_graph_schema.relationships
        )


def test_join_without_relationships_arg_skips_graph_validation(
    translator: DeterministicTranslator, join_graph_schema: RegistrySchema
) -> None:
    """When relationships=None (not passed), graph validation is skipped entirely."""
    ast = ValidatedAST(tree=sqlglot.parse_one(
        "SELECT p.title FROM posts p JOIN users u ON p.title = u.name"
    ))
    # No relationships argument — should not raise TranslationError for graph
    # mismatch. Safety flag errors (SafetyPolicyViolationError) are acceptable;
    # graph errors are not.
    try:
        translator.translate(ast, join_graph_schema)
    except (TranslationError, SafetyPolicyViolationError) as e:
        assert "Hallucinated JOIN blocked" not in str(e)


# ---------------------------------------------------------------------------
# Implicit cross-join safety engine tests
# ---------------------------------------------------------------------------

@pytest.fixture
def safety_engine() -> SafetyEngine:
    return SafetyEngine()


def test_implicit_cross_join_blocked(safety_engine: SafetyEngine) -> None:
    """FROM a, b without explicit JOIN ON must be rejected at the safety engine
    level."""
    tree = sqlglot.parse_one("SELECT posts.id FROM posts, users")
    ast = SQLAst(tree=tree)
    with pytest.raises(SafetyViolationError, match="Implicit or cross JOIN detected"):
        safety_engine.validate(ast)


# ---------------------------------------------------------------------------
# Row limit injection
# ---------------------------------------------------------------------------

def test_row_limit_applied_for_plain_select(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """A plain SELECT with no aggregation and no existing LIMIT gets LIMIT injected."""
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT id FROM users", read="postgres"))
    result = translator.translate(ast, mock_schema, row_limit=500)
    assert result.row_limit_applied is True
    assert "LIMIT" in result.sql


def test_row_limit_applied_to_aggregation(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """Aggregate queries now receive a LIMIT cap — the exemption was a bypass."""
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT COUNT(*) FROM users", read="postgres")
    )
    result = translator.translate(ast, mock_schema)
    assert result.row_limit_applied is True
    assert "LIMIT" in result.sql


def test_row_limit_applied_to_group_by(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """GROUP BY queries now receive a LIMIT cap — the exemption was a bypass."""
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT name, COUNT(*) FROM users GROUP BY name", read="postgres"
        )
    )
    result = translator.translate(ast, mock_schema)
    assert result.row_limit_applied is True
    assert "LIMIT" in result.sql


def test_row_limit_not_applied_when_safe_limit_already_exists(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """A LIMIT already within the cap is preserved as-is; no second LIMIT injected."""
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT id FROM users LIMIT 5", read="postgres")
    )
    result = translator.translate(ast, mock_schema)
    assert result.row_limit_applied is False
    # Verify there is exactly one LIMIT in the output (no duplication)
    assert result.sql.upper().count("LIMIT") == 1
    # Numeric literals are left inline (not parameterized)
    assert "LIMIT 5" in result.sql


def test_row_limit_clamped_when_explicit_limit_exceeds_cap(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """An explicit LIMIT exceeding the cap is clamped to row_limit."""
    ast = ValidatedAST(
        tree=sqlglot.parse_one("SELECT id FROM users LIMIT 99999", read="postgres")
    )
    result = translator.translate(ast, mock_schema)
    assert result.row_limit_applied is True
    assert "99999" not in result.sql
    assert "LIMIT" in result.sql


def test_where_only_aggregates_moves_to_having_and_removes_where(
    translator: DeterministicTranslator, mock_schema: RegistrySchema
) -> None:
    """
    When the WHERE clause contains ONLY aggregate predicates (and no
    non-aggregate conditions), the repair must remove the WHERE entirely and
    promote all conditions to HAVING.
    """
    ast = ValidatedAST(
        tree=sqlglot.parse_one(
            "SELECT name, SUM(id) FROM users GROUP BY name WHERE SUM(id) > 0",
            read="postgres",
        )
    )
    result = translator.translate(ast, mock_schema)
    sql_upper = result.sql.upper()
    # WHERE must be completely removed
    assert "WHERE" not in sql_upper
    # The aggregate condition must be in HAVING
    assert "HAVING" in sql_upper
    # Exactly one repair recorded
    assert len(result.translation_repairs) == 1
    assert result.translation_repairs[0].type == "where_aggregation_relocation"


def test_explicit_cross_join_blocked(safety_engine: SafetyEngine) -> None:
    """CROSS JOIN (no ON condition) must also be blocked."""
    tree = sqlglot.parse_one("SELECT posts.id FROM posts CROSS JOIN users")
    ast = SQLAst(tree=tree)
    with pytest.raises(SafetyViolationError, match="Implicit or cross JOIN detected"):
        safety_engine.validate(ast)


def test_explicit_join_with_on_passes_safety(safety_engine: SafetyEngine) -> None:
    """An explicit JOIN with an ON condition must pass the cross-join check."""
    tree = sqlglot.parse_one(
        "SELECT p.id FROM posts p JOIN users u ON p.author_id = u.id"
    )
    ast = SQLAst(tree=tree)
    validated = safety_engine.validate(ast)
    assert validated is not None
