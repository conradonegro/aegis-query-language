import pytest
from sqlglot import exp

from app.compiler import AbstractQuery
from app.compiler.models import SQLAst
from app.compiler.parser import SQLParser
from app.compiler.safety import SafetyEngine, SafetyViolationError


def test_parser_success() -> None:
    parser = SQLParser()
    ast = parser.parse(AbstractQuery(sql="SELECT col1 FROM tab1 WHERE col2 = 'val'"))
    assert ast.tree is not None
    assert ast.tree.key == "select"

def test_safety_engine_allow_simple_select() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(
            sql="SELECT col1, COUNT(col2) FROM tab1 WHERE col3 > 10 GROUP BY col1"
        )
    )
    validated = safety.validate(ast)
    assert validated.tree is not None

@pytest.mark.parametrize("malicious_query", [
    "DROP TABLE tab1",
    "GRANT ALL PRIVILEGES ON tab1 TO user",
    "SELECT * FROM tab1; DROP TABLE tab2",
    # Note: UNION at the root is now permitted (BUG-2 fix). A UNION that
    # references sensitive columns is blocked by column-level SafetyClassification,
    # not by the root-node check.
    "INSERT INTO tab1 (col1) VALUES ('val')",
])
def test_safety_engine_blocks_dangerous_payloads(malicious_query: str) -> None:
    parser = SQLParser()
    safety = SafetyEngine()

    try:
        # Some of these might not even parse as a single expression
        ast = parser.parse(AbstractQuery(sql=malicious_query))
    except Exception:
        # If it fails to parse (e.g. multiple statements), that's also a win
        return

    with pytest.raises(SafetyViolationError):
        safety.validate(ast)


# ─── Parser error paths ───────────────────────────────────────────────────────

def test_parser_empty_sql_produces_null_tree() -> None:
    """
    sqlglot.parse("") returns [None], so len == 1 passes the parser's count
    check and it returns SQLAst(tree=None).  The safety engine is responsible
    for rejecting the null tree.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=""))
    # tree is None — safety engine must reject it
    with pytest.raises(SafetyViolationError, match="(?i)empty"):
        safety.validate(ast)


def test_parser_rejects_multi_statement() -> None:
    parser = SQLParser()
    with pytest.raises(Exception, match=r"."):
        parser.parse(AbstractQuery(sql="SELECT 1; SELECT 2"))


# ─── Safety engine — additional DENY_LIST coverage ───────────────────────────

def test_safety_engine_rejects_null_ast() -> None:
    """An SQLAst with tree=None must raise immediately."""
    safety = SafetyEngine()
    with pytest.raises(SafetyViolationError, match="(?i)empty"):
        safety.validate(SQLAst(tree=None))


def test_safety_engine_rejects_non_select_root() -> None:
    """Any root node that is not SELECT or UNION must be rejected."""
    safety = SafetyEngine()
    # Manually construct an UPDATE node — bypasses the parser deliberately
    update_tree = exp.update("users", {"name": exp.Literal.string("x")})
    with pytest.raises(SafetyViolationError, match="(?i)root node must be"):
        safety.validate(SQLAst(tree=update_tree))


@pytest.mark.parametrize("dml_query", [
    "UPDATE users SET name = 'x' WHERE id = 1",
    "DELETE FROM users WHERE id = 1",
    "ALTER TABLE users ADD COLUMN foo TEXT",
])
def test_safety_engine_blocks_dml_and_ddl(dml_query: str) -> None:
    """UPDATE, DELETE, and ALTER must be blocked (root node check)."""
    parser = SQLParser()
    safety = SafetyEngine()
    try:
        ast = parser.parse(AbstractQuery(sql=dml_query))
    except Exception:
        return  # parse failure is also acceptable
    with pytest.raises(SafetyViolationError):
        safety.validate(ast)


def test_safety_engine_blocks_anonymous_function() -> None:
    """An unknown/custom function call parses as exp.Anonymous and must be denied."""
    parser = SQLParser()
    safety = SafetyEngine()
    try:
        ast = parser.parse(AbstractQuery(sql="SELECT my_secret_func(id) FROM users"))
    except Exception:
        return
    with pytest.raises(SafetyViolationError, match="(?i)denied"):
        safety.validate(ast)


@pytest.mark.parametrize("window_query", [
    "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM users",
    "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM users",
    "SELECT LAG(total) OVER (ORDER BY created_at) FROM orders",
    "SELECT SUM(total) OVER (PARTITION BY user_id) FROM orders",
])
def test_safety_engine_allows_window_functions(window_query: str) -> None:
    """Window functions are read-only analytics; permitted for BIRD-style
    top-N-per-group questions."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=window_query))
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_age_function() -> None:
    """AGE() parses as exp.Anonymous but is an approved read-only function."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(
            sql="SELECT AGE(CAST(a.d AS DATE), CAST(a.e AS DATE)) FROM a"
        )
    )
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_filter_clause() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(sql="SELECT COUNT(*) FILTER (WHERE t.x > 1) FROM t")
    )
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_dpipe_concat() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT t.a || '-' || t.b FROM t"))
    assert safety.validate(ast).tree is not None


@pytest.mark.parametrize("query", [
    "SELECT STRPOS(t.a, 'x') FROM t",
    "SELECT SPLIT_PART(t.a, ',', 1) FROM t",
])
def test_safety_engine_allows_strpos_and_split_part(query: str) -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=query))
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_explicit_cross_join() -> None:
    """Deliberate CROSS JOIN (e.g. against a single-row totals CTE) is
    permitted; cost is bounded by statement_timeout."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(
            sql=(
                "WITH totals AS (SELECT COUNT(*) AS n FROM t) "
                "SELECT t.x, totals.n FROM t CROSS JOIN totals"
            )
        )
    )
    assert safety.validate(ast).tree is not None


def test_safety_engine_still_blocks_comma_join() -> None:
    """Accidental comma-join stays blocked: it is almost always a missing
    ON condition, and produces silently wrong Cartesian products."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT a.x FROM a, b WHERE a.id = b.id"))
    with pytest.raises(SafetyViolationError, match="(?i)implicit"):
        safety.validate(ast)


# ─── Safety engine — ALLOW_LIST spot-checks ──────────────────────────────────

def test_safety_engine_allows_between() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT id FROM t WHERE id BETWEEN 1 AND 10"))
    validated = safety.validate(ast)
    assert validated.tree is not None


def test_safety_engine_allows_ilike() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT id FROM t WHERE name ILIKE '%alice%'"))
    validated = safety.validate(ast)
    assert validated.tree is not None


def test_safety_engine_allows_coalesce() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT COALESCE(name, 'unknown') FROM t"))
    validated = safety.validate(ast)
    assert validated.tree is not None


def test_safety_engine_allows_cast() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql="SELECT CAST(id AS TEXT) FROM t"))
    validated = safety.validate(ast)
    assert validated.tree is not None


def test_safety_engine_allows_join_using() -> None:
    """JOIN ... USING (...) is an explicit condition and must be permitted."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(sql="SELECT a.id FROM a JOIN b USING (id)")
    )
    validated = safety.validate(ast)
    assert validated.tree is not None


@pytest.mark.parametrize("query", [
    "SELECT a / b FROM t",
    "SELECT a + b FROM t",
    "SELECT a - b FROM t",
    "SELECT a * b FROM t",
    "SELECT a % b FROM t",
    "SELECT -a FROM t",
])
def test_safety_engine_allows_arithmetic(query: str) -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=query))
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_case_expression() -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(
        sql="SELECT CASE WHEN a > 1 THEN 'high' ELSE 'low' END FROM t"
    ))
    assert safety.validate(ast).tree is not None


@pytest.mark.parametrize("query", [
    "SELECT UPPER(name) FROM t",
    "SELECT LOWER(name) FROM t",
    "SELECT TRIM(name) FROM t",
    "SELECT CONCAT(a, b) FROM t",
    "SELECT SUBSTRING(name, 1, 3) FROM t",
    "SELECT LENGTH(name) FROM t",
    "SELECT ROUND(val, 2) FROM t",
    "SELECT FLOOR(val) FROM t",
    "SELECT CEIL(val) FROM t",
    "SELECT ABS(val) FROM t",
    "SELECT NULLIF(val, 0) FROM t",
    "SELECT GREATEST(a, b) FROM t",
    "SELECT LEAST(a, b) FROM t",
])
def test_safety_engine_allows_scalar_functions(query: str) -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=query))
    assert safety.validate(ast).tree is not None


# ─── CTEs and subqueries ──────────────────────────────────────────────────────

@pytest.mark.parametrize("query", [
    # Non-recursive CTE
    "WITH cte AS (SELECT id FROM t) SELECT * FROM cte",
    # Recursive CTE — allowed; statement_timeout caps resource use
    (
        "WITH RECURSIVE cte(n) AS "
        "(SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 10) "
        "SELECT n FROM cte"
    ),
    # Inline subquery in FROM
    "SELECT sub.id FROM (SELECT id FROM t) AS sub",
    # Subquery in WHERE with IN
    "SELECT id FROM t WHERE id IN (SELECT id FROM t2)",
    # EXISTS subquery
    "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)",
])
def test_safety_engine_allows_ctes_and_subqueries(query: str) -> None:
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(sql=query))
    assert safety.validate(ast).tree is not None


def test_safety_engine_allows_top_level_union() -> None:
    """UNION of two SELECTs must be accepted — the LLM legitimately uses UNION
    to answer 'biggest AND lowest' style questions in a single result set."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(
        AbstractQuery(sql="SELECT id FROM users UNION ALL SELECT id FROM users")
    )
    result = safety.validate(ast)
    assert result is not None


def test_safety_engine_union_deny_list_enforced_in_branch() -> None:
    """The deny-list must still be checked inside each UNION branch.
    An INSERT hidden inside a UNION should never reach the executor."""
    safety = SafetyEngine()
    import sqlglot
    # Manually craft a Union whose right branch is an Insert to bypass the parser
    left = sqlglot.parse_one("SELECT 1")
    right = sqlglot.parse_one("INSERT INTO t VALUES (1)")
    assert left is not None and right is not None
    union_tree = exp.Union(this=left, expression=right)
    with pytest.raises(SafetyViolationError):
        safety.validate(SQLAst(tree=union_tree))


def test_safety_engine_union_comma_join_blocked_in_branch() -> None:
    """An implicit comma-join inside a UNION branch must still be rejected —
    validation walks into every branch, not just the first."""
    parser = SQLParser()
    safety = SafetyEngine()
    ast = parser.parse(AbstractQuery(
        sql=(
            "SELECT id FROM users"
            " UNION ALL"
            " SELECT id FROM users, orders"
        )
    ))
    with pytest.raises(SafetyViolationError, match="(?i)implicit"):
        safety.validate(ast)
