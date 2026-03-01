
import pytest

from app.compiler import ExecutableQuery
from app.execution.executor import ExecutionEngine
from app.execution import ExecutionContext


@pytest.mark.asyncio
async def test_execution_parameter_binding() -> None:
    """Tests the executor can safely bind parameters using SQLite mocked backend."""
    # Use SQLite for parameter binding correctness, Postgres for semantic / timeouts
    engine = ExecutionEngine(connection_string="sqlite+aiosqlite:///:memory:")

    # We must mock table creation for testing SQL
    async with engine.engine.begin() as conn:
         from sqlalchemy import text
         await conn.execute(text("CREATE TABLE mock_table (id INTEGER, val TEXT)"))
         await conn.execute(text("INSERT INTO mock_table VALUES (1, 'mocked')"))
         await conn.execute(text("INSERT INTO mock_table VALUES (2, 'target')"))

    query = ExecutableQuery(
        sql="SELECT * FROM mock_table WHERE val = :p1",
        parameters={"p1": "target"},
        registry_version="v1",
        safety_engine_version="v1",
        abstract_query_hash="hash"
    )
    context = ExecutionContext(tenant_id="t1")

    result = await engine.execute(query, context=context)

    # Assert return types and mapping shapes
    assert result.columns == ["id", "val"]
    assert len(result.rows) == 1
    assert result.rows[0]["id"] == 2
    assert result.metadata["abstract_query_hash"] == "hash"
    assert result.metadata["row_limit_applied"] is False

    await engine.close()
