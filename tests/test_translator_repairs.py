import pytest
from sqlglot import exp
import sqlglot

from app.api.models import TranslationRepair
from app.compiler.models import ValidatedAST
from app.compiler.translator import DeterministicTranslator, TranslationError
from app.steward.models import (
    AbstractTableDef,
    AbstractColumnDef,
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
                    AbstractColumnDef(alias="id", description="user id", safety=SafetyClassification(allowed_in_select=True), physical_target="id"),
                    AbstractColumnDef(alias="name", description="name", safety=SafetyClassification(allowed_in_select=True), physical_target="name")
                ]
            ),
            AbstractTableDef(
                alias="orders",
                description="orders",
                physical_target="phys_orders",
                columns=[
                    AbstractColumnDef(alias="id", description="order id", safety=SafetyClassification(allowed_in_select=True), physical_target="id"),
                    AbstractColumnDef(alias="total_amount", description="total", safety=SafetyClassification(allowed_in_select=True), physical_target="total_amount"),
                    AbstractColumnDef(alias="name", description="order name", safety=SafetyClassification(allowed_in_select=True), physical_target="name") # INTENTIONAL COLLISION
                ]
            )
        ],
        relationships=[]
    )


def test_orphaned_alias_with_unique_scoped_column_repaired(translator: DeterministicTranslator, mock_schema: RegistrySchema):
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


def test_orphaned_alias_ambiguous_column_fails(translator: DeterministicTranslator, mock_schema: RegistrySchema):
    # 'name' exists on both 'users' and 'orders'. 
    # LLM hallucinates `x.` prefix but forgot `AS x`.
    # Since both tables are in scope, guess is unsafe.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT x.name FROM orders JOIN users ON users.id = orders.id"))
    
    with pytest.raises(TranslationError, match="Ambiguous orphaned prefix 'x' for column 'name'"):
        translator.translate(ast, mock_schema)


def test_orphaned_alias_column_not_in_scope_fails(translator: DeterministicTranslator, mock_schema: RegistrySchema):
    # 'total_amount' only exists on 'orders'.
    # LLM hallucinates `o.total_amount` but queries the 'users' table.
    # Since 'orders' is not in the scope of the FROM, repair is impossible/unsafe.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT o.total_amount FROM users"))
    
    with pytest.raises(TranslationError, match="does not belong to any table formally declared in scope"):
        translator.translate(ast, mock_schema)


def test_prefix_refers_to_non_existent_table_fails(translator: DeterministicTranslator, mock_schema: RegistrySchema):
    # Prefix 'y' is firmly declared in FROM clause (`FROM unknown_table y`)
    # The physical mapping will fail directly. Isolating to 'id' to ensure missing table, not missing column, triggers failure.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT y.id FROM unknown y"))
    
    # We never make it to column traversal if we fail base table existence, or it fails to find the column.
    with pytest.raises(TranslationError, match="Table 'unknown' does not exist in schema context"):
        translator.translate(ast, mock_schema)

def test_explicit_prefix_non_owning_table_fails(translator: DeterministicTranslator, mock_schema: RegistrySchema):
    # Prefix exists, but explicitly binds to a table that doesn't own the column.
    # LLM explicitly aliases `users u` and asks for `u.total_amount`. Even though `total_amount` is uniquely in `orders`,
    # the explicit override binding to `users` must fail strictly instead of heuristically grabbing `orders`.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT u.total_amount FROM users u JOIN orders o ON u.id = o.id"))
    
    with pytest.raises(TranslationError, match="Column 'users.total_amount' does not exist in schema context"):
        translator.translate(ast, mock_schema)

def test_explicit_dialect_alias_replaces_base_table_prefix(translator: DeterministicTranslator, mock_schema: RegistrySchema):
    # SQLite/Postgres strictly fail if you query `users.name` running `FROM users u`.
    # The translator MUST extract the explicit query `u` alias, identify that `users` is the table, 
    # and remap the column prefix down to `u`.
    ast = ValidatedAST(tree=sqlglot.parse_one("SELECT users.name FROM users u"))
    
    executable = translator.translate(ast, mock_schema)
    assert "u.name" in executable.sql
    assert "users.name" not in executable.sql
