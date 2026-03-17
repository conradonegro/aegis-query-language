import pytest

from app.compiler import UserIntent
from app.compiler.exceptions import (
    AmbiguousSourceDatabaseError,
    UnknownSourceDatabaseError,
)
from app.compiler.filter import DeterministicSchemaFilter
from app.compiler.models import FilteredSchema
from app.steward import (
    AbstractColumnDef,
    AbstractRelationshipDef,
    AbstractTableDef,
    RegistrySchema,
    SafetyClassification,
)


def test_deterministic_schema_filter_simple_overlap() -> None:
    filter_engine = DeterministicSchemaFilter(cutoff_threshold=1)

    schema = RegistrySchema(
        version="v1.0.0",
        tables=[
            AbstractTableDef(
                alias="users",
                description="User accounts and profiles",
                physical_target="auth.users",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="ID",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="auth.users.id",
                    )
                ]
            ),
            AbstractTableDef(
                alias="orders",
                description="Purchase history",
                physical_target="sales.orders",
                columns=[
                    AbstractColumnDef(
                        alias="id", description="ID",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="sales.orders.id",
                    )
                ]
            )
        ],
        relationships=[]
    )

    intent = UserIntent(natural_language_query="Show me the profile for user 123")
    filtered = filter_engine.filter_schema(intent, schema)

    # "profile" / "user" -> matches users table
    # but not "orders"
    assert len(filtered.tables) == 1
    assert filtered.tables[0].alias == "users"

    # orders structure is entirely omitted,
    # and users columns are kept because the parent table matched.
    assert "orders" not in [t.alias for t in filtered.tables]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _col(alias: str, description: str = "") -> AbstractColumnDef:
    return AbstractColumnDef(
        alias=alias,
        description=description,
        safety=SafetyClassification(allowed_in_select=True),
        physical_target=f"phys_{alias}",
    )


def _table(
    alias: str,
    description: str = "",
    cols: list[AbstractColumnDef] | None = None,
) -> AbstractTableDef:
    return AbstractTableDef(
        alias=alias,
        description=description,
        physical_target=f"phys_{alias}",
        columns=cols or [_col("id")],
    )


def _filtered(tables: list[AbstractTableDef]) -> FilteredSchema:
    return FilteredSchema(
        version="1",
        tables=tables,
        relationships=[],
        omitted_columns={},
    )


# ─── is_follow_up ─────────────────────────────────────────────────────────────

def test_is_follow_up_no_last_schema_returns_false() -> None:
    f = DeterministicSchemaFilter()
    intent = UserIntent(natural_language_query="and active ones")
    assert f.is_follow_up(intent, last_schema=None) is False


def test_is_follow_up_zero_intent_tokens_returns_false() -> None:
    """After stop-word removal, empty token set → not a follow-up."""
    f = DeterministicSchemaFilter()
    # All words are stop words
    intent = UserIntent(natural_language_query="and or the a")
    last = _filtered([_table("users", "User accounts")])
    assert f.is_follow_up(intent, last_schema=last) is False


def test_is_follow_up_short_query_no_structural_match_returns_true() -> None:
    """
    A short query (< 8 non-stop tokens) with no schema structural tokens
    is treated as a follow-up continuation.
    """
    f = DeterministicSchemaFilter()
    intent = UserIntent(natural_language_query="filter active 2023")
    last = _filtered([_table("sales", "Revenue records")])
    assert f.is_follow_up(intent, last_schema=last) is True


def test_is_follow_up_structural_match_in_full_schema_forces_fresh() -> None:
    """
    Even if the token does not match the prior schema, if it matches any
    table in the full registry the query is treated as a fresh one.
    """
    f = DeterministicSchemaFilter()
    # Intent mentions "orders" which is in the full schema but NOT in last_schema
    intent = UserIntent(natural_language_query="what about orders")
    last = _filtered([_table("users", "User accounts")])
    full = RegistrySchema(
        version="1",
        tables=[
            _table("users", "User accounts"),
            _table("orders", "Purchase records"),
        ],
        relationships=[],
    )
    assert f.is_follow_up(intent, last_schema=last, full_schema=full) is False


def test_is_follow_up_long_query_no_structural_match_returns_false() -> None:
    """
    A long query (≥ 8 non-stop tokens) without any structural match is
    considered a fresh query, not a continuation.
    """
    f = DeterministicSchemaFilter()
    intent = UserIntent(
        natural_language_query=(
            "recent monthly revenue breakdown quarter category region"
            " segment channel"
        )
    )
    last = _filtered([_table("x", "something completely unrelated")])
    assert f.is_follow_up(intent, last_schema=last) is False


# ─── filter_schema — relationship augmentation ────────────────────────────────

def test_filter_schema_augments_via_relationship() -> None:
    """
    A table that does not match the intent tokens must still be included
    when it is 1-degree related to a matched table.
    """
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = RegistrySchema(
        version="1",
        tables=[
            _table("users", "User accounts", cols=[_col("id"), _col("email")]),
            _table("orders", "Purchase history", cols=[_col("id"), _col("user_id")]),
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
    # Only "users" token in intent — orders has no direct overlap
    intent = UserIntent(natural_language_query="Show all users")
    filtered = f.filter_schema(intent, schema)

    aliases = {t.alias for t in filtered.tables}
    assert "users" in aliases
    assert "orders" in aliases  # pulled in via relationship


def test_filter_schema_prunes_unrelated_relationships() -> None:
    """
    Relationships whose source or target table was dropped must be pruned
    from the filtered result.
    """
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = RegistrySchema(
        version="1",
        tables=[
            _table("users", "User accounts"),
            _table("products", "Product catalogue"),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="users",
                source_column="id",
                target_table="products",
                target_column="owner_id",
            )
        ],
    )
    intent = UserIntent(natural_language_query="Show user profile")
    filtered = f.filter_schema(intent, schema)

    # products doesn't match "user profile" tokens and has no RAG force
    # BUT it IS augmented via relationship — check that if products gets
    # dropped (zero overlap + no force), the relationship is also pruned.
    # Since augmentation adds products, verify relationship is kept when both
    # present.
    if len(filtered.tables) == 2:
        assert len(filtered.relationships) == 1
    else:
        # If products was dropped, relationship must be absent
        assert len(filtered.relationships) == 0


# ─── source_database helpers ──────────────────────────────────────────────────

def _table_with_db(
    alias: str,
    source_database: str,
    description: str = "",
    cols: list[AbstractColumnDef] | None = None,
) -> AbstractTableDef:
    return AbstractTableDef(
        alias=alias,
        description=description,
        physical_target=f"phys_{alias}",
        source_database=source_database,
        columns=cols or [_col("id")],
    )


def _multi_db_schema() -> RegistrySchema:
    """Schema with tables spread across two logical databases."""
    return RegistrySchema(
        version="1",
        tables=[
            _table_with_db("loan", "financial", "Loan records"),
            _table_with_db("client", "financial", "Bank clients"),
            _table_with_db("circuits", "formula_1", "Formula 1 circuits"),
            _table_with_db("drivers", "formula_1", "Formula 1 drivers"),
        ],
        relationships=[],
    )


# ─── Explicit source_database scoping ────────────────────────────────────────

def test_explicit_source_database_restricts_to_matching_tables() -> None:
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = _multi_db_schema()
    intent = UserIntent(
        natural_language_query="Show loan details for each client",
        source_database="financial",
    )
    filtered = f.filter_schema(intent, schema)
    aliases = {t.alias for t in filtered.tables}
    # loan and client match; circuits/drivers are excluded by DB scope
    assert aliases == {"loan", "client"}
    assert filtered.source_database_used == "financial"


def test_explicit_source_database_unknown_raises() -> None:
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = _multi_db_schema()
    intent = UserIntent(
        natural_language_query="anything", source_database="nonexistent"
    )
    with pytest.raises(UnknownSourceDatabaseError) as exc_info:
        f.filter_schema(intent, schema)
    assert exc_info.value.name == "nonexistent"


def test_explicit_source_database_used_in_filtered_schema() -> None:
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = _multi_db_schema()
    intent = UserIntent(
        natural_language_query="List circuits and drivers",
        source_database="formula_1",
    )
    filtered = f.filter_schema(intent, schema)
    assert filtered.source_database_used == "formula_1"
    aliases = {t.alias for t in filtered.tables}
    assert aliases == {"circuits", "drivers"}


# ─── Auto-detection ───────────────────────────────────────────────────────────

def test_auto_detect_single_winner_restricts_schema() -> None:
    """When one DB clearly matches (2× margin), schema is restricted to it."""
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = _multi_db_schema()
    # "loan" and "client" are financial-DB terms — no formula_1 signal
    intent = UserIntent(
        natural_language_query="What are the loan details for each client"
    )
    filtered = f.filter_schema(intent, schema)
    aliases = {t.alias for t in filtered.tables}
    assert aliases.issubset({"loan", "client"})
    assert filtered.source_database_used == "financial"


def test_auto_detect_ambiguous_raises() -> None:
    """When two DBs match equally, AmbiguousSourceDatabaseError is raised."""
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = RegistrySchema(
        version="1",
        tables=[
            _table_with_db("alpha", "db_a", "alpha records"),
            _table_with_db("beta", "db_b", "beta records"),
        ],
        relationships=[],
    )
    # "alpha beta" — each DB scores 1 (equal tie, no 2× margin)
    intent = UserIntent(natural_language_query="show alpha and beta records")
    with pytest.raises(AmbiguousSourceDatabaseError) as exc_info:
        f.filter_schema(intent, schema)
    assert set(exc_info.value.candidates) == {"db_a", "db_b"}


def test_auto_detect_no_match_uses_full_schema() -> None:
    """When no DB has token signal, source_database_used is None."""
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = _multi_db_schema()
    intent = UserIntent(natural_language_query="xyz completely unrelated qwerty")
    filtered = f.filter_schema(intent, schema)
    assert filtered.source_database_used is None


def test_schema_without_source_database_no_auto_detect() -> None:
    """Tables without source_database are never candidates for auto-detection."""
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = RegistrySchema(
        version="1",
        tables=[
            _table("users", "User accounts"),
            _table("orders", "Purchase history"),
        ],
        relationships=[],
    )
    intent = UserIntent(natural_language_query="Show user orders")
    filtered = f.filter_schema(intent, schema)
    assert filtered.source_database_used is None


def test_detect_source_database_clear_winner_two_match() -> None:
    """DB with score 4 beats DB with score 1 — 4× margin clears the 2× bar."""
    f = DeterministicSchemaFilter(cutoff_threshold=1)
    schema = RegistrySchema(
        version="1",
        tables=[
            _table_with_db(
                "loan", "financial", "Loan account balance interest rate"
            ),
            _table_with_db("circuits", "formula_1", "Circuit"),
        ],
        relationships=[],
    )
    intent = UserIntent(
        natural_language_query="loan balance interest rate account"
    )
    filtered = f.filter_schema(intent, schema)
    assert filtered.source_database_used == "financial"
