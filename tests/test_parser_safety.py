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
    "SELECT * FROM tab1 WHERE col1 = 1 UNION SELECT password FROM users",
    "WITH cte AS (SELECT * FROM tab1) SELECT * FROM cte",
    "SELECT (SELECT col1 FROM tab2) FROM tab1",
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
    """Any root node that is not SELECT must be rejected."""
    safety = SafetyEngine()
    # Manually construct an UPDATE node — bypasses the parser deliberately
    update_tree = exp.update("users", {"name": exp.Literal.string("x")})
    with pytest.raises(SafetyViolationError, match="(?i)root node must be SELECT"):
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


def test_safety_engine_blocks_window_function() -> None:
    """Window functions (exp.Window) are not in the ALLOW_LIST and must be rejected."""
    parser = SQLParser()
    safety = SafetyEngine()
    try:
        ast = parser.parse(
            AbstractQuery(sql="SELECT ROW_NUMBER() OVER (ORDER BY id) FROM users")
        )
    except Exception:
        return
    with pytest.raises(SafetyViolationError):
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
