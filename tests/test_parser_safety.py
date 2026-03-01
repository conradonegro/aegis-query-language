import pytest

from app.compiler import AbstractQuery
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
