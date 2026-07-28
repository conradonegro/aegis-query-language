"""Tests for the AEGIS_ROW_LIMIT environment override.

The translator's injected LIMIT and the executor's Python-side fetch cap
both default to 1000 but must be raisable for benchmark alignment (official
BIRD EX fetches full result sets).
"""

import pytest
import sqlglot

from app.compiler.models import ValidatedAST
from app.compiler.translator import DeterministicTranslator, configured_row_limit
from app.execution.executor import configured_fetch_cap
from app.steward.models import (
    AbstractColumnDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)


@pytest.fixture
def schema() -> RegistrySchema:
    return RegistrySchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="users",
                physical_target="phys_users",
                columns=[
                    AbstractColumnDef(
                        alias="id",
                        description="user id",
                        physical_target="id",
                        safety=SafetyClassification(
                            allowed_in_select=True,
                            allowed_in_where=True,
                        ),
                    ),
                ],
            ),
        ],
        relationships=[],
    )


def test_configured_row_limit_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AEGIS_ROW_LIMIT", raising=False)
    assert configured_row_limit() == 1000


def test_configured_row_limit_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AEGIS_ROW_LIMIT", "500000")
    assert configured_row_limit() == 500000


def test_configured_row_limit_ignores_garbage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AEGIS_ROW_LIMIT", "not-a-number")
    assert configured_row_limit() == 1000


def test_translator_injects_env_row_limit(
    monkeypatch: pytest.MonkeyPatch, schema: RegistrySchema
) -> None:
    monkeypatch.setenv("AEGIS_ROW_LIMIT", "500000")
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT users.id FROM users"))
    executable = DeterministicTranslator().translate(ast, schema)
    assert "LIMIT 500000" in executable.sql


def test_translator_explicit_row_limit_beats_env(
    monkeypatch: pytest.MonkeyPatch, schema: RegistrySchema
) -> None:
    monkeypatch.setenv("AEGIS_ROW_LIMIT", "500000")
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT users.id FROM users"))
    executable = DeterministicTranslator().translate(ast, schema, row_limit=10)
    assert "LIMIT 10" in executable.sql


def test_executor_fetch_cap_follows_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AEGIS_ROW_LIMIT", "500000")
    assert configured_fetch_cap() == 500000


def test_executor_fetch_cap_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AEGIS_ROW_LIMIT", raising=False)
    assert configured_fetch_cap() == 1000
