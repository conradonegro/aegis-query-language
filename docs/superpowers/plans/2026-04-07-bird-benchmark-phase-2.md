# BIRD Benchmark Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise BIRD-SQL mini-dev benchmark accuracy from the current 40% baseline by addressing the four remaining classes of failures with root-cause fixes that generalize across all databases — no per-question tuning, no FK injection, no description edits.

**Architecture:** Five sequential phases, each independently reviewable, testable, and mergeable. Phase 0 locks in the existing BUG-5 fix with a real regression test. Phase 1 (BUG-6) replaces a brittle node-type allow-list with expression-aware temporal validation in the translator. Phase 2 (PROMPT-1+) introduces *bimodal* value display in the schema context — exhaustive enumerations for low-cardinality columns, prominently labeled non-exhaustive samples for high-cardinality columns — so the LLM stops reading sample lists as closed-world enumerations. Phase 3 (BUG-4) hardens the prompt against implicit comma-joins with positive phrasing, a counter-example, and JOIN templates rendered from the relationship graph. Phase 4 (scope refactor) replaces the global `_collect_table_scope` mechanism with sqlglot's `Scope` object, eliminating the BUG-3/BUG-5 bypass workarounds at their root and fixing the PROMPT-3 ambiguous-naked-column failure by construction.

Each phase ends with a benchmark rerun checkpoint so we can attribute accuracy gains to specific changes.

**Tech Stack:** Python 3.12 (strict mypy, ruff), sqlglot, FastAPI, SQLAlchemy 2.x async, Alembic, Jinja2, asyncpg, pytest, uv

**Out of scope (firm):** Editing column descriptions, adding fake FK relationships, hand-tuning per-question prompts, or any change that would not generalize across all 13 BIRD databases. The benchmark integrity rule is absolute.

---

## File map

| File | Phase | Action |
|---|---|---|
| `tests/test_translator.py` | 0, 1, 4 | Modify — add regression tests |
| `app/compiler/translator.py` | 1, 4 | Modify — temporal validation, scope refactor |
| `app/steward/models.py` | 2 | Modify — add `sample_values_exhaustive` to `AbstractColumnDef` |
| `app/api/meta_models.py` | 2 | Modify — add `sample_values_exhaustive` column to `MetadataColumn` |
| `backend_migrations/versions/0001_initial_schema.py` | 2 | Modify — add `sample_values` and `sample_values_exhaustive` columns directly to the consolidated baseline (no new migration file) |
| `backend_migrations/versions/0002_add_sample_values.py` | 2 | **Delete** — folded into `0001` per the "no new migrations, everything in the init script" rule |
| `scripts/discover_metadata.py` | 2 | Modify — branch on distinct count |
| `app/api/compiler.py` | 2 | Modify — propagate field into compiled artifact |
| `app/steward/loader.py` | 2 | Modify — read field from artifact JSON |
| `app/compiler/templates/system.jinja` | 2, 3 | Modify — bimodal value rendering, JOIN templates, rule 7 |
| `tests/test_loader.py` | 2 | Create — loader hydration unit tests (does not currently exist) |
| `tests/test_prompt_builder.py` | 2, 3 | Modify — template rendering assertions |
| `app/api/router.py` | 2 | Modify — fix clone path (Task 2.7) so it preserves sample_values, sample_values_exhaustive, and curated RAG values |
| `tests/test_metadata_clone.py` | 2 | Create — clone-preservation regression tests |

---

# Phase 0 — Lock in BUG-5 with a real regression test

**Why this exists:** The CTE-prefix bypass at `app/compiler/translator.py:244` was committed in `fe67ecf` but the only regression test (`test_cte_column_prefix_resolves_without_error`) uses `top_users.id` where `id` is a real schema column. If a future refactor removed the bypass, that test would catch it (because `top_users` isn't in `alias_to_physical_table`), but it doesn't pin down the actual BIRD failure mode: a CTE alias prefix paired with an alias-only output column (e.g. `agg.total_consumption` where `total_consumption` was created by `SUM(consumption) AS total_consumption` inside the CTE).

We add one test that exactly mirrors the BIRD failure shape, then run the full suite to confirm everything still passes.

### Task 0.1: Add CTE-only-output-column regression test

**Files:**
- Modify: `tests/test_translator.py` (append after `test_cte_column_prefix_resolves_without_error` at line 459)

- [ ] **Step 1: Write the failing test**

Append after the existing CTE prefix test (around line 460):

```python
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
    schema = _make_schema()  # users(id, salary), orders(total)

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
```

- [ ] **Step 2: Run the new test to verify it passes against current HEAD**

Run: `uv run pytest tests/test_translator.py::test_cte_prefixed_alias_only_output_column_resolves_without_error -v`
Expected: PASS (the bypass at translator.py:244 already handles this case; the test locks it in).

If it FAILS, the bypass is incomplete and Phase 0 turns into a real fix. Do not proceed; investigate.

- [ ] **Step 3: Run the full test suite to confirm no regressions**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 4: Run lint and type checks**

Run in parallel:
```bash
uv run ruff check tests/test_translator.py
uv run mypy tests/test_translator.py
```
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add tests/test_translator.py
git commit -m "$(cat <<'EOF'
test(translator): lock in BUG-5 CTE-prefixed alias-only output column bypass

The existing test_cte_column_prefix_resolves_without_error covers a CTE
prefix with a column that also exists in the schema, so it does not pin
down the BIRD q=1479/1480 failure shape. This adds a regression test that
references an alias-only CTE output column (SUM(...) AS aggregate_total),
the exact pattern that triggered the original failure.

EOF
)"
```

---

## Phase 0 checkpoint

Rerun the BIRD benchmark to establish a clean Phase 0 baseline. The expected outcome is that q=1479 and q=1480 are now correct (they were the BUG-5 failures), giving us roughly 22/50 ≈ 44%. If the actual number differs, the gap is information — record it before proceeding.

```bash
uv run python scripts/run_bird_benchmark.py \
  --questions data/minidev/MINIDEV/mini_dev_postgresql.json \
  --api-key <REDACTED-API-KEY> \
  --api-url http://localhost:8000 \
  --db-url "postgresql+asyncpg://user_aegis_runtime:runtime_pass@127.0.0.1:5433/aegis_data_warehouse" \
  --provider-id "anthropic:claude-haiku-4-5-20251001" \
  --limit 50 --concurrency 2
```

---

# Phase 1 — BUG-6: Expression-aware temporal validation

**Why this exists:** `app/compiler/translator.py:411-417` rejects `EXTRACT(YEAR FROM CAST(text_col AS DATE))` because the validation only accepts a bare `exp.Column` as the EXTRACT argument. The naive fix is to add `exp.Cast` to an allow-list, but that lets `EXTRACT(YEAR FROM CAST(x AS TEXT))` through — meaningless and a runtime error. The right fix is a small `_resolves_to_temporal()` helper that understands expression types: a Cast is temporal iff its target type is temporal; a Column is temporal iff its resolved datatype is temporal. The helper has one extension point so future temporal expression forms (TO_DATE, TO_TIMESTAMP) can be added without growing parallel allow-lists.

The change lives entirely in the translator, where `column_datatypes` is already populated. The original analysis-doc proposal would put it in `safety.py`, but `safety.py` runs *before* translation and has no datatype knowledge — that location is wrong.

### Task 1.1: Add `_resolves_to_temporal` helper with failing test

**Files:**
- Modify: `tests/test_translator.py` (append a new section)
- Modify: `app/compiler/translator.py:406-434` (`_validate_temporal_expressions`)

- [ ] **Step 1: Write the failing test for EXTRACT on CAST(text → date)**

Append a new section in `tests/test_translator.py`, after the existing CTE tests:

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_translator.py::test_extract_year_from_cast_text_to_date_passes -v`
Expected: FAIL with `UnsafeExpressionError: EXTRACT numeric target must be natively bound to a column, found 'Cast'`.

- [ ] **Step 3: Add the `_resolves_to_temporal` helper and rewrite `_validate_temporal_expressions`**

In `app/compiler/translator.py`, replace the existing `_validate_temporal_expressions` method (lines 406-434) with the version below. Note: `_TEMPORAL_TYPES` already exists at line 440-442 — reuse it; do **not** redeclare it.

Replace lines 406-434 (the entire `_validate_temporal_expressions` body, leaving the method signature):

```python
    def _validate_temporal_expressions(
        self, tree: exp.Expression, column_datatypes: dict[int, str]
    ) -> None:
        """Validates EXTRACT and INTERVAL nodes after physical resolution.

        EXTRACT requires a temporal *expression*, which may be either a bare
        column (whose datatype was recorded during the AST walk) or a CAST
        whose target type is temporal. Other forms are rejected at compile
        time so the LLM gets a useful error rather than a runtime crash.
        """
        for extract_node in tree.find_all(exp.Extract):
            source = extract_node.expression
            if any(extract_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside EXTRACT."
                )
            if not self._resolves_to_temporal(source, column_datatypes):
                raise UnsafeExpressionError(
                    f"EXTRACT requires a temporal expression; got"
                    f" '{type(source).__name__}' that does not resolve to a"
                    f" temporal type."
                )
        for interval_node in tree.find_all(exp.Interval):
            if any(interval_node.find_all(exp.Subquery, exp.Select, exp.Window)):
                raise UnsafeExpressionError(
                    "Nested subqueries or window constructs are strictly blocked"
                    " inside INTERVAL."
                )

    def _resolves_to_temporal(
        self, expr: exp.Expression, column_datatypes: dict[int, str]
    ) -> bool:
        """Returns True iff `expr` is guaranteed to evaluate to a temporal value.

        Recognized forms:
          - exp.Column whose resolved datatype contains a temporal type token
          - exp.Cast whose target DataType is one of the temporal types

        Not recognized (returns False — caller raises): arithmetic, anonymous
        function calls, literals, parameters, anything else.
        """
        if isinstance(expr, exp.Column):
            dtype = column_datatypes.get(id(expr), "")
            return any(t in dtype for t in self._TEMPORAL_TYPES)
        if isinstance(expr, exp.Cast):
            target = expr.to
            if not isinstance(target, exp.DataType):
                return False
            target_name = target.this.name.lower() if target.this else ""
            return target_name in self._TEMPORAL_TYPES
        return False
```

- [ ] **Step 4: Run the test to verify it now passes**

Run: `uv run pytest tests/test_translator.py::test_extract_year_from_cast_text_to_date_passes -v`
Expected: PASS.

- [ ] **Step 5: Commit (incremental — full coverage tests come next)**

```bash
git add app/compiler/translator.py tests/test_translator.py
git commit -m "$(cat <<'EOF'
fix(translator): expression-aware temporal validation for EXTRACT (BUG-6)

Replaces the bare-column-only check in _validate_temporal_expressions
with a small _resolves_to_temporal() helper that understands two
expression forms: a Column (temporal iff its resolved datatype is
temporal) and a Cast (temporal iff its target DataType is temporal).

This unblocks EXTRACT(YEAR FROM CAST(text_col AS DATE)) — the natural
pattern for text-encoded date columns — without widening the allow-list
to also accept meaningless forms like CAST(... AS TEXT). Future temporal
expression forms (TO_DATE, TO_TIMESTAMP, DATE_TRUNC) can be added by
extending the helper rather than growing parallel allow-lists.

EOF
)"
```

### Task 1.2: Add the rest of the temporal-validation coverage

**Files:**
- Modify: `tests/test_translator.py` (append after Task 1.1's test)

- [ ] **Step 1: Add tests for all four temporal-validation cases**

Append after `test_extract_year_from_cast_text_to_date_passes`:

```python
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
```

- [ ] **Step 2: Run all five new tests**

Run: `uv run pytest tests/test_translator.py -k "extract" -v`
Expected: 5 PASS (1 from Task 1.1 + 4 from this task), no failures.

- [ ] **Step 3: Run the full test suite for regressions**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 4: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/compiler/translator.py tests/test_translator.py
uv run mypy app/compiler/translator.py tests/test_translator.py
```
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add tests/test_translator.py
git commit -m "$(cat <<'EOF'
test(translator): full coverage for expression-aware EXTRACT validation

Adds four cases around _resolves_to_temporal:
- CAST(text AS TEXT) inside EXTRACT is rejected (the cast target is not temporal)
- CAST(text AS TIMESTAMP) inside EXTRACT is permitted
- Bare DATE column inside EXTRACT still works (regression)
- Bare TEXT column inside EXTRACT still rejected (regression)
- Subquery inside EXTRACT still blocked (regression)

EOF
)"
```

---

## Phase 1 checkpoint

Rerun the BIRD benchmark. Expected impact: q=1340 (the EXTRACT-on-CAST failure) becomes correct. Combined with Phase 0 lock-in, we should be at ~46% (23/50). If only one or two databases benefit, that is information; record it.

---

# Phase 2 — PROMPT-1+: Bimodal value display

**Why this exists:** The LLM treats `Sample values: X, Y, Z` as a closed-world enumeration regardless of how rule 10 is phrased. The structural fix is to *change the data presentation*: for low-cardinality columns (≤8 distinct values), show ALL values labeled "Allowed values (complete list)"; for high-cardinality columns, show top-K samples with a prominent "NOT exhaustive" warning. This eliminates the false signal at its source, without relying on the LLM remembering a meta-rule against its own default reasoning.

A new boolean `sample_values_exhaustive` rides alongside the existing `sample_values` field through the entire pipeline:

```
discover_metadata.py → MetadataColumn (DB) → MetadataCompiler (artifact JSON) → RegistryLoader → AbstractColumnDef → system.jinja
```

Each layer needs one small change. We do them in dependency order (DB → ORM → discovery → compiler → loader → schema model → template), with tests after each layer.

### Task 2.1: Add `sample_values` and `sample_values_exhaustive` to the consolidated baseline

**Per the "no new migrations" rule** (`feedback_no_new_migrations.md`): edit `backend_migrations/versions/0001_initial_schema.py` directly. The DB is dropped and recreated freely, so there is no forward-migration step to preserve. `0001` is *the* init script.

This task folds both `sample_values` (currently living in the vestigial `0002_add_sample_values.py`) and the new `sample_values_exhaustive` column into `0001`, then deletes `0002`.

**Files:**
- Modify: `backend_migrations/versions/0001_initial_schema.py:166-189` (add two columns to the `metadata_columns` create_table)
- Delete: `backend_migrations/versions/0002_add_sample_values.py`

- [ ] **Step 1: Add both columns to the `metadata_columns` create_table in `0001`**

In `backend_migrations/versions/0001_initial_schema.py`, find the `metadata_columns` table definition. The `refresh_on_compile` column is at lines 184-189 and is the last column before the foreign key constraints:

```python
        sa.Column("rag_order_direction", sa.Text(), nullable=True),
        sa.Column(
            "refresh_on_compile",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.ForeignKeyConstraint(
```

Insert two new columns *between* `refresh_on_compile` and the `ForeignKeyConstraint` so the column ordering matches the field addition timeline (`sample_values` first, since `0002` originally added it; `sample_values_exhaustive` second, as the Phase 2 addition):

```python
        sa.Column("rag_order_direction", sa.Text(), nullable=True),
        sa.Column(
            "refresh_on_compile",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "sample_values",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "sample_values_exhaustive",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.ForeignKeyConstraint(
```

No new imports needed — `postgresql.JSONB` is already imported at the top of the file (it's used elsewhere in the same migration).

- [ ] **Step 2: Delete the vestigial `0002_add_sample_values.py`**

```bash
git rm backend_migrations/versions/0002_add_sample_values.py
```

After deletion, `0001` is the only migration file and describes the complete schema. The `backend_migrations/versions/` directory should contain only `0001_initial_schema.py` (and the `__pycache__/` directory, which git ignores).

- [ ] **Step 3: Drop and rebuild the DB to verify the schema is consistent**

Per `feedback_bird_reset_procedure.md`, the correct reset is a full `down -v` + rebuild:

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d
```

Wait for the health check to pass, then verify both columns exist:

```bash
uv run python -c "
import asyncio, os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

async def check():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    async with engine.begin() as conn:
        result = await conn.execute(text(
            \"SELECT column_name FROM information_schema.columns \"
            \"WHERE table_schema='aegis_meta' \"
            \"AND table_name='metadata_columns' \"
            \"AND column_name IN ('sample_values', 'sample_values_exhaustive') \"
            \"ORDER BY column_name\"
        ))
        rows = [r[0] for r in result.fetchall()]
        assert rows == ['sample_values', 'sample_values_exhaustive'], (
            f'expected both columns, got {rows}'
        )
        print('OK: both columns present')

asyncio.run(check())
"
```
Expected: `OK: both columns present`.

- [ ] **Step 4: Commit**

```bash
git add backend_migrations/versions/0001_initial_schema.py
git rm backend_migrations/versions/0002_add_sample_values.py
git commit -m "$(cat <<'EOF'
feat(schema): fold sample_values + add sample_values_exhaustive to baseline

Per the "no new migrations" rule: the DB is dropped and recreated freely,
so there is no forward-migration step to preserve and 0001_initial_schema
is effectively *the* init script. New schema changes go directly into it.

This commit:
- Adds sample_values (JSONB, nullable) directly to metadata_columns in
  0001, folding in what 0002_add_sample_values.py previously did.
- Adds sample_values_exhaustive (Boolean NOT NULL DEFAULT false) to the
  same table. Set true by discover_metadata.py when COUNT(DISTINCT col)
  <= 8; threaded through compiler → loader → prompt template so the
  LLM gets closed-world signal where the closed-world assumption is
  actually correct.
- Deletes 0002_add_sample_values.py which is now redundant.

EOF
)"
```

### Task 2.2: Add `sample_values_exhaustive` to the SQLAlchemy ORM

**Files:**
- Modify: `app/api/meta_models.py:137` (the existing `sample_values` line)

- [ ] **Step 1: Add the new mapped column**

In `app/api/meta_models.py`, find the existing `sample_values` line at 137:

```python
    sample_values: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
```

Replace it with:

```python
    sample_values: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    sample_values_exhaustive: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa.false()
    )
```

If `sa` (sqlalchemy) is not yet imported in this module, add `import sqlalchemy as sa` at the top of the imports block.

- [ ] **Step 2: Run mypy on the model**

Run: `uv run mypy app/api/meta_models.py`
Expected: clean.

- [ ] **Step 3: Run the existing meta_models tests**

Run: `uv run pytest tests/ -k "meta_models or metadata" -v`
Expected: existing tests still pass (the new field defaults to False so existing fixtures keep working).

- [ ] **Step 4: Commit**

```bash
git add app/api/meta_models.py
git commit -m "feat(orm): add sample_values_exhaustive mapping to MetadataColumn"
```

### Task 2.3: Branch discovery on distinct count

**Files:**
- Modify: `scripts/discover_metadata.py:107-138` (the sample-value block and `MetadataColumn` constructor)

- [ ] **Step 1: Replace the sample-value block with a distinct-count branch**

In `scripts/discover_metadata.py`, find the comment block starting at line 107:

```python
        # Sample the 3 most frequently occurring non-null values for this column.
```

Replace lines 107-123 (from the comment through the bare `pass` of the existing try/except) with:

```python
        # Choose between exhaustive enumeration (low cardinality) and
        # frequency-ranked sampling (high cardinality).
        #
        # The LLM treats Sample values lists as closed-world enumerations
        # regardless of how rule 10 is phrased. For columns with <=8 distinct
        # values, we surface ALL values and label them as the complete set,
        # giving the model accurate closed-world signal. For higher-cardinality
        # columns we keep frequency-ranked samples and label them prominently
        # as non-exhaustive at render time.
        sample_vals: list[str] = []
        sample_vals_exhaustive: bool = False
        try:
            distinct_sql = text(
                f'SELECT COUNT(DISTINCT "{col_name}") FROM "{tbl_name}"'
            )
            distinct_count_row = await session.execute(distinct_sql)
            distinct_count = distinct_count_row.scalar()
            if distinct_count is not None and 0 < distinct_count <= 8:
                exhaustive_sql = text(
                    f'SELECT "{col_name}" FROM "{tbl_name}"'
                    f' WHERE "{col_name}" IS NOT NULL'
                    f' GROUP BY "{col_name}"'
                    f' ORDER BY COUNT(*) DESC'
                )
                exhaustive_res = await session.execute(exhaustive_sql)
                sample_vals = [str(row[0]) for row in exhaustive_res.fetchall()]
                sample_vals_exhaustive = True
            else:
                sample_sql = text(
                    f'SELECT "{col_name}" FROM "{tbl_name}"'
                    f' WHERE "{col_name}" IS NOT NULL'
                    f' GROUP BY "{col_name}"'
                    f' ORDER BY COUNT(*) DESC LIMIT 3'
                )
                sample_res = await session.execute(sample_sql)
                sample_vals = [str(row[0]) for row in sample_res.fetchall()]
        except Exception:
            pass  # Non-fatal — skip sample values for this column
```

Then in the `MetadataColumn(...)` constructor at line 126, add the new field:

Find:
```python
            sample_values=sample_vals or None,
        )
```

Replace with:
```python
            sample_values=sample_vals or None,
            sample_values_exhaustive=sample_vals_exhaustive,
        )
```

- [ ] **Step 2: Sanity-run discovery on a clean BIRD database**

Use the project's `down -v` + rebuild flow per `feedback_bird_reset_procedure.md` (do not partial-truncate):

```bash
docker compose -f docker-compose.yml -f docker-compose.bird.yml down -v
docker compose -f docker-compose.yml -f docker-compose.bird.yml up --build -d
```

Wait for the health check, then check that low-cardinality columns are flagged as exhaustive. Run a one-shot SQL probe via the metadata role:

```bash
uv run python -c "
import asyncio, os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
async def check():
    engine = create_async_engine(os.environ['DATABASE_URL'])
    async with engine.begin() as conn:
        rows = await conn.execute(text(
            \"SELECT real_name, sample_values, sample_values_exhaustive \"
            \"FROM aegis_meta.metadata_columns \"
            \"WHERE alias IN ('segment','position','currency') \"
            \"ORDER BY real_name LIMIT 20\"
        ))
        for r in rows:
            print(r)
asyncio.run(check())
"
```
Expected: low-cardinality columns like `segment` (4 values) and `currency` (2 values) have `sample_values_exhaustive=True` and a value list with all distinct values.

- [ ] **Step 3: Lint**

Run: `uv run ruff check scripts/discover_metadata.py`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add scripts/discover_metadata.py
git commit -m "$(cat <<'EOF'
feat(discovery): exhaustive value enumeration for low-cardinality columns

When COUNT(DISTINCT col) <= 8, fetch ALL distinct values and mark
sample_values_exhaustive=True. Otherwise keep the existing top-3
frequency-ranked sample with the flag false. Lets the prompt template
render closed-world "Allowed values (complete list)" labels for enums
like segment/position/currency, eliminating LLM over-refusal driven by
samples being read as enumerations.

EOF
)"
```

### Task 2.4: Propagate the field through `MetadataCompiler`

**Files:**
- Modify: `app/api/compiler.py:113-131` (the column dict construction)

**Note on testing:** This task does NOT add a dedicated unit test for the dict-key change. The full compiler integration is exercised by `tests/test_version_lifecycle.py`, but those tests use raw SQL inserts via the in-memory SQLite tables created in `conftest.py` — and `conftest.py` does not currently create `metadata_tables` or `metadata_columns` rows, only the version/artifact/audit tables. Building a heavyweight mock for one new dict key is poor ROI: the change is one line, the compile_version path is exercised by existing tests (which still pass with the default False value), and Task 2.5 adds the loader-side hydration test that *does* assert the field on the round-trip. The Phase 2 checkpoint benchmark is the integration test for the full pipeline.

- [ ] **Step 1: Add the field to the artifact JSON**

In `app/api/compiler.py`, find the `tbl_dict["columns"].append({...})` block at line 113. The relevant lines are 129-131:

```python
                    "rag_values_hash": _compute_rag_values_hash(active_values),
                    "sample_values": col.sample_values or [],
                })
```

Replace with:

```python
                    "rag_values_hash": _compute_rag_values_hash(active_values),
                    "sample_values": col.sample_values or [],
                    "sample_values_exhaustive": col.sample_values_exhaustive,
                })
```

- [ ] **Step 2: Confirm existing compiler tests still pass**

Run: `uv run pytest tests/test_version_lifecycle.py -v`
Expected: all green. The default value of `sample_values_exhaustive` is `False` so existing rejects-path tests are unaffected.

- [ ] **Step 3: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/compiler.py
uv run mypy app/api/compiler.py
```
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add app/api/compiler.py
git commit -m "$(cat <<'EOF'
feat(compiler): include sample_values_exhaustive in artifact JSON

One-line addition to the column dict construction in MetadataCompiler.
The field is hydrated back by RegistryLoader (Task 2.5) and read by
the Jinja template (Task 2.6). Existing test_version_lifecycle tests
continue to pass; the round-trip is verified by the loader unit test.

EOF
)"
```

### Task 2.5: Hydrate the field in `RegistryLoader` and `AbstractColumnDef`

**Files:**
- Modify: `app/steward/models.py:13-21` (`AbstractColumnDef`)
- Modify: `app/steward/loader.py:118-128` (the `AbstractColumnDef(...)` constructor call)
- Create: `tests/test_loader.py` (does not currently exist; `test_worm_security.py` covers integration security but not pure hydration)

- [ ] **Step 1: Add the field to `AbstractColumnDef`**

In `app/steward/models.py`, find the `AbstractColumnDef` class at line 13:

```python
class AbstractColumnDef(BaseModel):
    alias: str
    description: str
    data_type: str = "text"
    safety: SafetyClassification
    physical_target: str
    rag_enabled: bool = False
    rag_cardinality_hint: Literal["low", "medium", "high"] | None = None
    sample_values: list[str] = []
```

Add the new field at the end:

```python
class AbstractColumnDef(BaseModel):
    alias: str
    description: str
    data_type: str = "text"
    safety: SafetyClassification
    physical_target: str
    rag_enabled: bool = False
    rag_cardinality_hint: Literal["low", "medium", "high"] | None = None
    sample_values: list[str] = []
    sample_values_exhaustive: bool = False
```

- [ ] **Step 2: Hydrate the field in `RegistryLoader`**

In `app/steward/loader.py`, find the `AbstractColumnDef(...)` call at line 118:

```python
                columns_def.append(
                    AbstractColumnDef(
                        alias=col_dict["alias"],
                        description=col_dict.get("description") or "",
                        data_type=col_dict.get("type", "text"),
                        safety=safety,
                        # Mapping conceptual alias directly to real name
                        physical_target=col_dict["name"],
                        sample_values=col_dict.get("sample_values") or [],
                    )
                )
```

Replace with:

```python
                columns_def.append(
                    AbstractColumnDef(
                        alias=col_dict["alias"],
                        description=col_dict.get("description") or "",
                        data_type=col_dict.get("type", "text"),
                        safety=safety,
                        # Mapping conceptual alias directly to real name
                        physical_target=col_dict["name"],
                        sample_values=col_dict.get("sample_values") or [],
                        sample_values_exhaustive=col_dict.get(
                            "sample_values_exhaustive", False
                        ),
                    )
                )
```

The `.get(..., False)` default keeps backward compatibility with old artifacts that predate the field — they hydrate as non-exhaustive, which is the safe default.

- [ ] **Step 3: Create `tests/test_loader.py` with a hydration unit test**

`tests/test_loader.py` does not currently exist (the only loader test today is `tests/test_worm_security.py`, which exercises the security paths against a real Postgres). Create a new file for pure-Python hydration unit tests using the established hash+signature construction pattern from `test_worm_security.py:197-200`.

Create `tests/test_loader.py`:

```python
"""Unit tests for RegistryLoader.load_schema_from_artifact.

These are pure-Python tests: no database. They construct an in-memory
CompiledRegistryArtifact with a valid hash and HMAC signature using the
same canonicalization helpers as the production compiler, then assert
that the loader hydrates the resulting RegistrySchema correctly.
"""
import hashlib

from app.api.meta_models import CompiledRegistryArtifact
from app.audit.chaining import (
    compute_artifact_hmac_signature,
    get_canonical_json,
)
from app.steward.loader import RegistryLoader
from app.vault import get_secrets_manager


def _signed_artifact_for(blob: dict) -> CompiledRegistryArtifact:
    """Build a CompiledRegistryArtifact whose hash and signature pass
    RegistryLoader's verification, given the same canonicalization and
    HMAC helpers production uses.
    """
    canon = get_canonical_json(blob)
    valid_hash = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    secrets_mgr = get_secrets_manager()
    kid = secrets_mgr.get_current_signing_key_id()
    signing_key = secrets_mgr.get_signing_key(kid)
    signature = compute_artifact_hmac_signature(signing_key, canon)
    return CompiledRegistryArtifact(
        artifact_blob=blob,
        artifact_hash=valid_hash,
        signature=signature,
        signature_key_id=kid,
        tenant_id="default",
        compiler_version="1.0.0",
    )


def _minimal_blob_with_two_columns(
    *,
    first_exhaustive: bool,
    include_second_field: bool,
) -> dict:
    """Build a minimal valid artifact blob with one table and two columns.

    The first column carries sample_values_exhaustive=first_exhaustive.
    The second column either includes the field set to False, or omits it
    entirely (to verify the .get(..., False) back-compat default).
    """
    second_col: dict = {
        "id": "00000000-0000-0000-0000-000000000003",
        "name": "name",
        "alias": "name",
        "description": "",
        "type": "text",
        "is_primary": False,
        "is_nullable": True,
        "allowed_in_select": True,
        "allowed_in_filter": True,
        "allowed_in_join": False,
        "is_sensitive": False,
        "safety_classification": {},
        "rag_enabled": False,
        "rag_cardinality_hint": None,
        "rag_limit": None,
        "rag_values_hash": "",
        "sample_values": [],
    }
    if include_second_field:
        second_col["sample_values_exhaustive"] = False
    return {
        "meta_version": "v1.0.0",
        "compiled_at": "2026-04-07T00:00:00Z",
        "tables": [
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": "enum_table",
                "alias": "enum_table",
                "description": "",
                "tenant_id": "default",
                "source_database": None,
                "columns": [
                    {
                        "id": "00000000-0000-0000-0000-000000000002",
                        "name": "status",
                        "alias": "status",
                        "description": "",
                        "type": "text",
                        "is_primary": False,
                        "is_nullable": False,
                        "allowed_in_select": True,
                        "allowed_in_filter": True,
                        "allowed_in_join": False,
                        "is_sensitive": False,
                        "safety_classification": {},
                        "rag_enabled": False,
                        "rag_cardinality_hint": None,
                        "rag_limit": None,
                        "rag_values_hash": "",
                        "sample_values": ["active", "archived"],
                        "sample_values_exhaustive": first_exhaustive,
                    },
                    second_col,
                ],
                "relationships": [],
            }
        ],
        "rag_manifest": {"default_rag_limit": 100, "rag_enabled_count": 0},
        "roles": {"system": "admin"},
    }


def test_loader_hydrates_sample_values_exhaustive_true() -> None:
    """A column whose blob has sample_values_exhaustive=True must hydrate
    into AbstractColumnDef.sample_values_exhaustive=True."""
    blob = _minimal_blob_with_two_columns(
        first_exhaustive=True, include_second_field=True
    )
    artifact = _signed_artifact_for(blob)
    schema = RegistryLoader.load_schema_from_artifact(artifact)

    cols = schema.tables[0].columns
    assert cols[0].alias == "status"
    assert cols[0].sample_values == ["active", "archived"]
    assert cols[0].sample_values_exhaustive is True
    assert cols[1].alias == "name"
    assert cols[1].sample_values_exhaustive is False


def test_loader_defaults_sample_values_exhaustive_when_field_absent() -> None:
    """A column whose blob omits sample_values_exhaustive entirely must
    hydrate as False (back-compat: old artifacts predate the field)."""
    blob = _minimal_blob_with_two_columns(
        first_exhaustive=False, include_second_field=False
    )
    artifact = _signed_artifact_for(blob)
    schema = RegistryLoader.load_schema_from_artifact(artifact)

    cols = schema.tables[0].columns
    assert cols[1].alias == "name"
    assert cols[1].sample_values_exhaustive is False
```

- [ ] **Step 4: Run the loader tests**

Run: `uv run pytest tests/test_loader.py -v`
Expected: 2 PASS.

If the test fails with a signing-key error, the test environment isn't picking up dev signing keys. Look at how `tests/test_worm_security.py` configures `EnvFallbackProvider` (line 11) and replicate the env-var setup in your test (or in `conftest.py` if it should be global).

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 6: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/steward tests/test_loader.py
uv run mypy app/steward tests/test_loader.py
```
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add app/steward/models.py app/steward/loader.py tests/test_loader.py
git commit -m "$(cat <<'EOF'
feat(steward): hydrate sample_values_exhaustive in RegistryLoader

AbstractColumnDef gains a sample_values_exhaustive: bool = False field.
RegistryLoader reads it from the compiled artifact JSON via
.get(..., False) to keep old artifacts that predate the field
hydrating as non-exhaustive (the safe default).

Adds tests/test_loader.py with two pure-Python hydration tests:
- field=True round-trips correctly
- field absent → default False (back-compat)

The signed-artifact helper uses the same canonical/HMAC machinery as
production, so the tests cover the loader's hash+signature path too.

EOF
)"
```

### Task 2.6: Render bimodal value display in the system prompt

**Files:**
- Modify: `app/compiler/templates/system.jinja:29-30` (the existing `Sample values` rendering)
- Modify: `app/compiler/templates/system.jinja:17` (rule 10 reword)
- Modify: `tests/test_prompt_builder.py` (add rendering assertions)

- [ ] **Step 1: Update the Jinja template**

In `app/compiler/templates/system.jinja`, find the existing sample-values rendering at lines 29-30:

```jinja
{% if col.sample_values %}      Sample values: {{ col.sample_values | join(", ") }}
{% endif %}
```

Replace with:

```jinja
{% if col.sample_values %}{% if col.sample_values_exhaustive %}      Allowed values (complete list, case-sensitive): {{ col.sample_values | join(", ") }}
{% else %}      Example values (NOT exhaustive — column may contain other values): {{ col.sample_values | join(", ") }}
{% endif %}{% endif %}
```

Then in the same file, find rule 10 at line 17:

```jinja
10. Only refuse if the data required is fundamentally absent from the schema. If the schema contains the relevant tables and columns, always attempt a query. Do not refuse based on missing value-level knowledge (e.g., what specific values a column contains). Sample values shown in the Schema Context are the most frequent examples only — they are not exhaustive. Never infer that a value or time period is absent from the data because it does not appear in the samples.
```

Replace with:

```jinja
10. Only refuse if the data required is fundamentally absent from the schema. If the schema contains the relevant tables and columns, you MUST attempt a query — even if the queried value does not appear in any "Example values" list. Example values are frequency-ranked illustrations only and are explicitly labeled "NOT exhaustive". Refusing because a value is not shown in an Example values list is a violation of this rule. The "Allowed values (complete list)" label is the only place where the displayed values are exhaustive; everywhere else, assume the column contains values you cannot see.
```

- [ ] **Step 2: Add prompt rendering tests**

In `tests/test_prompt_builder.py`, append:

```python
def test_prompt_renders_exhaustive_label_for_low_cardinality() -> None:
    """A column with sample_values_exhaustive=True must render with the
    'Allowed values (complete list)' label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="members",
                description="Club members",
                physical_target="public.members",
                columns=[
                    AbstractColumnDef(
                        alias="position",
                        description="Member position",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="position",
                        sample_values=["President", "Vice President", "Member"],
                        sample_values_exhaustive=True,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "Allowed values (complete list" in envelope.system_instruction
    assert "Vice President" in envelope.system_instruction
    assert "NOT exhaustive" not in envelope.system_instruction


def test_prompt_renders_non_exhaustive_label_for_high_cardinality() -> None:
    """A column with sample_values_exhaustive=False must render with the
    prominent 'NOT exhaustive' warning label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="customers",
                description="Customers",
                physical_target="public.customers",
                columns=[
                    AbstractColumnDef(
                        alias="city",
                        description="Customer city",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="city",
                        sample_values=["Prague", "Brno", "Ostrava"],
                        sample_values_exhaustive=False,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "NOT exhaustive" in envelope.system_instruction
    assert "Prague" in envelope.system_instruction
    assert "Allowed values (complete list" not in envelope.system_instruction


def test_prompt_omits_value_block_when_no_samples() -> None:
    """A column with sample_values=[] must render NEITHER label."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="t",
                description="t",
                physical_target="public.t",
                columns=[
                    AbstractColumnDef(
                        alias="c",
                        description="c",
                        data_type="text",
                        safety=SafetyClassification(allowed_in_select=True),
                        physical_target="c",
                        sample_values=[],
                        sample_values_exhaustive=False,
                    ),
                ],
            )
        ],
        relationships=[],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "NOT exhaustive" not in envelope.system_instruction
    assert "Allowed values" not in envelope.system_instruction
```

- [ ] **Step 3: Run the prompt builder tests**

Run: `uv run pytest tests/test_prompt_builder.py -v`
Expected: 3 new tests PASS, plus existing tests still pass.

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 5: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/compiler/templates app/steward app/api scripts tests
uv run mypy app/compiler app/steward app/api scripts tests
uv run lint-imports
```
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/compiler/templates/system.jinja tests/test_prompt_builder.py
git commit -m "$(cat <<'EOF'
feat(prompt): bimodal value display — exhaustive vs sample (PROMPT-1+)

The Jinja template now branches on col.sample_values_exhaustive:
- True  -> "Allowed values (complete list, case-sensitive): a, b, c"
- False -> "Example values (NOT exhaustive — column may contain other
            values): x, y, z"

Rule 10 is reworded to make the open-world assumption mandatory for
the "Example values" label and explicit about which label carries the
closed-world signal. This eliminates the LLM over-refusal pattern
behind q=1484 / q=1331 / q=1356 / q=1362 by changing the data
presentation, not by hoping the model remembers a meta-rule.

EOF
)"
```

### Task 2.7: Fix the metadata clone path (code-review finding #7)

**Why this exists:** The 2026-04-07 code review found that the `POST /metadata/versions` clone endpoint at `app/api/router.py:1476-1541` silently drops two pieces of state when cloning a baseline:

1. **`MetadataColumn.values`** (the curated RAG values seeded into the vector store) — the baseline `selectinload` chain at line 1481 only loads `tables.columns`, not `tables.columns.values`. The clone loop never copies them.
2. **`MetadataColumn.sample_values`** — the field was added in commit `fe67ecf` but the clone-loop column constructor at lines 1516-1539 was never updated to copy it.

The result: a steward who clones the active version to make a small alias change ends up with a draft that has empty curated values *and* empty sample values, regressing prompt enrichment for unrelated columns.

This task is bundled into Phase 2 (rather than a separate code-review followup plan) for one specific reason: Phase 2 introduces `sample_values_exhaustive`, the third field in this same family. Fixing the clone bug at the same time means we copy `sample_values`, `sample_values_exhaustive`, AND the curated values together — landing two related field additions and the bug fix as one coherent change.

**Files:**
- Modify: `app/api/router.py:1476-1541` (clone endpoint baseline fetch + clone loop)
- Create: `tests/test_metadata_clone.py` (does not exist; closest existing test is `test_version_lifecycle.py` for status transitions)

- [ ] **Step 1: Add the eager-load for `MetadataColumn.values`**

In `app/api/router.py`, find the baseline fetch at line 1476:

```python
        stmt = (
            select(MetadataVersion)
            .where(MetadataVersion.version_id == baseline_id)
            .options(
                selectinload(MetadataVersion.tables).selectinload(
                    MetadataTable.columns
                ),
                selectinload(MetadataVersion.edges)
            )
        )
```

Replace with:

```python
        stmt = (
            select(MetadataVersion)
            .where(MetadataVersion.version_id == baseline_id)
            .options(
                selectinload(MetadataVersion.tables)
                .selectinload(MetadataTable.columns)
                .selectinload(MetadataColumn.values),
                selectinload(MetadataVersion.edges)
            )
        )
```

The third `selectinload` chain ensures `old_c.values` is materialized in the same query rather than triggering a lazy-load on a closed async session.

You will also need to add `MetadataColumn` to the existing `from app.api.meta_models import ...` line if it isn't already imported in this file. Verify with `grep -n "from app.api.meta_models" app/api/router.py`.

- [ ] **Step 2: Copy `sample_values` and `sample_values_exhaustive` in the column clone**

In the same file, find the `MetadataColumn(...)` constructor inside the clone loop at line 1516. The fields currently copied are:

```python
                new_c = MetadataColumn(
                    column_id=new_c_id,
                    version_id=new_version.version_id,
                    table_id=new_t_id,
                    real_name=old_c.real_name,
                    alias=old_c.alias,
                    description=old_c.description,
                    data_type=old_c.data_type,
                    is_nullable=old_c.is_nullable,
                    is_primary_key=old_c.is_primary_key,
                    is_unique=old_c.is_unique,
                    is_sensitive=old_c.is_sensitive,
                    allowed_in_select=old_c.allowed_in_select,
                    allowed_in_filter=old_c.allowed_in_filter,
                    allowed_in_join=old_c.allowed_in_join,
                    safety_classification=old_c.safety_classification,
                    rag_enabled=old_c.rag_enabled,
                    rag_cardinality_hint=old_c.rag_cardinality_hint,
                    rag_limit=old_c.rag_limit,
                    rag_sample_strategy=old_c.rag_sample_strategy,
                    rag_order_by_column=old_c.rag_order_by_column,
                    rag_order_direction=old_c.rag_order_direction,
                    refresh_on_compile=old_c.refresh_on_compile,
                )
```

Add three lines before the closing `)`:

```python
                    refresh_on_compile=old_c.refresh_on_compile,
                    sample_values=old_c.sample_values,
                    sample_values_exhaustive=old_c.sample_values_exhaustive,
                )
```

- [ ] **Step 3: Copy curated `MetadataColumnValue` rows after the column flush**

In the same file, find the column flush at line 1546 (`await session.flush()`) and the relationships loop that follows. Insert a new values-clone loop **between the flush and the edges loop**, so the new column primary keys exist before the values reference them:

Find:
```python
        # Flush tables and columns to the DB before inserting relationships so
        # the FK constraint (version_id, source/target_column_id) → metadata_columns
        # is satisfied when the relationship rows are written.
        await session.flush()

        for old_e in baseline.edges:
```

Replace with:
```python
        # Flush tables and columns to the DB before inserting relationships so
        # the FK constraint (version_id, source/target_column_id) → metadata_columns
        # is satisfied when the relationship rows are written.
        await session.flush()

        # Clone curated MetadataColumnValue rows. We carry only active rows
        # forward — archived values are intentionally not propagated, mirroring
        # the compiler's _compute_rag_values_hash filter at api/compiler.py:112.
        # Each row is rebound to (new_version_id, new_column_id); value_id is
        # regenerated so the new draft owns its rows independently of the
        # baseline.
        for old_t in baseline.tables:
            for old_c in old_t.columns:
                new_c_id = col_id_map[old_c.column_id]
                for old_v in old_c.values:
                    if not old_v.active:
                        continue
                    session.add(
                        MetadataColumnValue(
                            value_id=uuid.uuid4(),
                            column_id=new_c_id,
                            version_id=new_version.version_id,
                            value=old_v.value,
                            active=True,
                        )
                    )

        for old_e in baseline.edges:
```

Add `MetadataColumnValue` to the existing `from app.api.meta_models import ...` line if it isn't already imported.

- [ ] **Step 4: Create `tests/test_metadata_clone.py` with regression coverage**

Create a new test file. Use the synchronous SQLite pattern from `tests/test_version_lifecycle.py` (raw `text()` inserts and the `_sync_engine()` helper) so you don't need async fixtures or the full ORM round-trip.

Create `tests/test_metadata_clone.py`:

```python
"""Regression tests for the metadata clone endpoint.

The clone endpoint at POST /metadata/versions previously silently dropped
sample_values and curated MetadataColumnValue rows when cloning a baseline.
These tests pin down the fix from code-review finding #7 (2026-04-07):

  - sample_values is copied
  - sample_values_exhaustive is copied
  - active MetadataColumnValue rows are copied (rebound to the new
    version_id and column_id)
  - inactive MetadataColumnValue rows are NOT copied
"""
import json
import uuid
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from app.api.auth import ResolvedCredential, require_admin_credential
from app.main import app
from tests.conftest import TEST_ADMIN_CREDENTIAL_ID

_FAKE_ADMIN_CRED = ResolvedCredential(
    credential_id=TEST_ADMIN_CREDENTIAL_ID,
    tenant_id="test_tenant",
    user_id="admin_user",
    scope="admin",
)

_SQLITE_URL = "sqlite:///file:testdb?mode=memory&cache=shared&uri=true"


def _sync_engine() -> Any:
    return create_engine(
        _SQLITE_URL,
        connect_args={"check_same_thread": False},
    )


def _seed_baseline_with_values_and_samples(conn: Any) -> str:
    """Insert a baseline version with one table, one column, two values
    (one active, one archived), and a non-empty sample_values list.
    Returns the baseline version_id (hex form, no dashes)."""
    vid = uuid.uuid4()
    tid = uuid.uuid4()
    cid = uuid.uuid4()

    conn.execute(
        text(
            "INSERT INTO metadata_versions"
            " (version_id, tenant_id, status, created_by)"
            " VALUES (:vid, 'test_tenant', 'pending_review', 'baseline_seed')"
        ),
        {"vid": vid.hex},
    )
    # NOTE: conftest.py creates the metadata_versions / artifacts / audit
    # tables but does NOT create metadata_tables / metadata_columns / values.
    # Add ad-hoc CREATE IF NOT EXISTS so this test is self-contained without
    # touching conftest.py for unrelated tests.
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_tables (
            table_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            tenant_id TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            source_database TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_columns (
            column_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            table_id TEXT NOT NULL,
            real_name TEXT NOT NULL,
            alias TEXT NOT NULL,
            description TEXT,
            data_type TEXT NOT NULL,
            is_nullable INTEGER NOT NULL DEFAULT 1,
            is_primary_key INTEGER NOT NULL DEFAULT 0,
            is_unique INTEGER NOT NULL DEFAULT 0,
            is_sensitive INTEGER NOT NULL DEFAULT 0,
            allowed_in_select INTEGER NOT NULL DEFAULT 0,
            allowed_in_filter INTEGER NOT NULL DEFAULT 0,
            allowed_in_join INTEGER NOT NULL DEFAULT 0,
            safety_classification TEXT,
            sample_values TEXT,
            sample_values_exhaustive INTEGER NOT NULL DEFAULT 0,
            rag_enabled INTEGER NOT NULL DEFAULT 0,
            rag_cardinality_hint TEXT,
            rag_limit INTEGER,
            rag_sample_strategy TEXT,
            rag_order_by_column TEXT,
            rag_order_direction TEXT,
            refresh_on_compile INTEGER NOT NULL DEFAULT 0
        )
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS metadata_relationships (
            relationship_id TEXT PRIMARY KEY,
            version_id TEXT NOT NULL,
            source_table_id TEXT NOT NULL,
            source_column_id TEXT NOT NULL,
            target_table_id TEXT NOT NULL,
            target_column_id TEXT NOT NULL,
            relationship_type TEXT NOT NULL DEFAULT 'fk',
            cardinality TEXT NOT NULL DEFAULT '1:n',
            bidirectional INTEGER NOT NULL DEFAULT 1,
            active INTEGER NOT NULL DEFAULT 1
        )
    """))
    # metadata_column_values is already created by conftest.py.

    conn.execute(
        text(
            "INSERT INTO metadata_tables"
            " (table_id, version_id, real_name, alias, description, active)"
            " VALUES (:tid, :vid, 'members', 'members',"
            " 'Test members', 1)"
        ),
        {"tid": tid.hex, "vid": vid.hex},
    )
    conn.execute(
        text(
            "INSERT INTO metadata_columns"
            " (column_id, version_id, table_id, real_name, alias,"
            " data_type, is_nullable, is_primary_key, allowed_in_select,"
            " allowed_in_filter, allowed_in_join, sample_values,"
            " sample_values_exhaustive)"
            " VALUES (:cid, :vid, :tid, 'position', 'position',"
            " 'text', 1, 0, 1, 1, 0, :sv, 1)"
        ),
        {
            "cid": cid.hex,
            "vid": vid.hex,
            "tid": tid.hex,
            "sv": json.dumps(["President", "Vice President", "Member"]),
        },
    )
    # Two values: one active (must be cloned), one archived (must NOT).
    conn.execute(
        text(
            "INSERT INTO metadata_column_values"
            " (value_id, column_id, version_id, value, active)"
            " VALUES (:vid_v, :cid, :vid, 'Treasurer', 1)"
        ),
        {"vid_v": uuid.uuid4().hex, "cid": cid.hex, "vid": vid.hex},
    )
    conn.execute(
        text(
            "INSERT INTO metadata_column_values"
            " (value_id, column_id, version_id, value, active)"
            " VALUES (:vid_v, :cid, :vid, 'Retired', 0)"
        ),
        {"vid_v": uuid.uuid4().hex, "cid": cid.hex, "vid": vid.hex},
    )
    return vid.hex


def test_clone_preserves_sample_values_and_active_curated_values() -> None:
    """Cloning a baseline must copy sample_values, sample_values_exhaustive,
    and active MetadataColumnValue rows. Archived values must NOT be copied.
    """
    engine = _sync_engine()
    with engine.begin() as conn:
        baseline_vid_hex = _seed_baseline_with_values_and_samples(conn)

    app.dependency_overrides[require_admin_credential] = lambda: _FAKE_ADMIN_CRED
    try:
        with TestClient(app) as client:
            # Convert hex to dashed UUID form for the JSON payload.
            baseline_uuid = str(uuid.UUID(baseline_vid_hex))
            response = client.post(
                "/api/v1/metadata/versions",
                json={
                    "baseline_version_id": baseline_uuid,
                    "change_reason": "regression test clone",
                },
            )
        assert response.status_code in (200, 201), response.text
        new_version_id = uuid.UUID(response.json()["version_id"]).hex

        with engine.connect() as conn:
            # Sample values and exhaustive flag carried forward
            row = conn.execute(
                text(
                    "SELECT sample_values, sample_values_exhaustive"
                    " FROM metadata_columns"
                    " WHERE version_id = :vid"
                ),
                {"vid": new_version_id},
            ).fetchone()
            assert row is not None
            assert json.loads(row[0]) == [
                "President", "Vice President", "Member"
            ]
            assert int(row[1]) == 1

            # Curated values: only the active one carried forward
            value_rows = conn.execute(
                text(
                    "SELECT value, active FROM metadata_column_values"
                    " WHERE version_id = :vid ORDER BY value"
                ),
                {"vid": new_version_id},
            ).fetchall()
            assert len(value_rows) == 1
            assert value_rows[0][0] == "Treasurer"
            assert int(value_rows[0][1]) == 1
    finally:
        app.dependency_overrides.pop(require_admin_credential, None)
        engine.dispose()
```

**Important caveat:** the exact JSON path of the clone endpoint may differ from `/api/v1/metadata/versions` — verify with `grep -n '@api_router.post.*versions' app/api/router.py` and adjust the URL and the request body schema to match the actual endpoint signature. The test as written assumes the existing endpoint accepts `{"baseline_version_id": ..., "change_reason": ...}`; check the request model around `router.py:1460` to confirm field names. If the request model is something like `MetadataVersionCreateRequest`, copy its field names exactly.

- [ ] **Step 5: Run the new test**

Run: `uv run pytest tests/test_metadata_clone.py -v`
Expected: PASS.

If the test fails because the endpoint URL or payload schema is wrong, fix the test (not the endpoint). If it fails because the clone code change is wrong, fix the code.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -v`
Expected: all green. The clone change is additive (new field copies and a new values loop) so existing clone tests should be unaffected.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/api/router.py tests/test_metadata_clone.py
uv run mypy app/api/router.py tests/test_metadata_clone.py
uv run lint-imports
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/api/router.py tests/test_metadata_clone.py
git commit -m "$(cat <<'EOF'
fix(steward): clone preserves sample_values and curated RAG values

Code-review finding #7 (2026-04-07): the metadata clone endpoint at
POST /metadata/versions silently dropped two pieces of state from the
baseline:

1. MetadataColumn.values (curated RAG values) — the baseline selectinload
   chain only loaded tables.columns, not tables.columns.values, and the
   clone loop never copied them.

2. MetadataColumn.sample_values — added in commit fe67ecf but the clone
   loop's column constructor was never updated. Cloning to make a small
   alias change silently lost prompt enrichment for unrelated columns.

Fix:
- Extend the baseline selectinload chain to materialize MetadataColumn.values
- Copy sample_values and sample_values_exhaustive in the column clone
- New active-only loop after the column flush copies MetadataColumnValue
  rows, rebound to the new version_id and column_id

Bundled into Phase 2 because Phase 2 introduces sample_values_exhaustive,
the third field in this same family — bundling the bug fix with the new
field plumbing means we land the related changes coherently.

EOF
)"
```

---

## Phase 2 checkpoint

Re-run discovery (`docker compose down -v` + rebuild) so the new column flag is populated, then rerun the BIRD benchmark. Expected impact: q=1484, q=1331, q=1356, q=1362 become correct (4 questions). Combined cumulative: ~54% (27/50). Watch for any regressions in databases that have many low-cardinality columns where exhaustive enumeration may surface ambiguous values.

---

# Phase 3 — BUG-4: Prompt-only fixes for implicit JOIN

**Why this exists:** The LLM still emits `FROM a, b WHERE a.x = b.x` for some questions despite rule 7. The fix is to attack the model's default reasoning at multiple levels: (a) reword the rule positively as a *syntactic* constraint, (b) include a concrete WRONG/CORRECT counter-example pair, and (c) render the relationships section as ready-to-paste JOIN templates so the model is biased toward the form we want. No translator rewriter — the compiler stays a validator.

If after this phase BUG-4 still occurs, *then* a translator-level desugaring pass becomes justified, and it gets its own plan with full safety re-validation.

### Task 3.1: Strengthen rule 7 and the relationships rendering

**Files:**
- Modify: `app/compiler/templates/system.jinja:14` (rule 7)
- Modify: `app/compiler/templates/system.jinja:35-37` (relationships block)
- Modify: `tests/test_prompt_builder.py` (assert new template content)

- [ ] **Step 1: Reword rule 7 with a counter-example**

In `app/compiler/templates/system.jinja`, find rule 7 at line 14:

```jinja
7. CTEs (WITH) and subqueries are permitted. Prefer standard JOINs when they are simpler. Never use comma-separated table lists in FROM clauses (e.g. never `FROM a, b`); always use explicit `JOIN ... ON` syntax.
```

Replace with:

```jinja
7. Every multi-table query MUST use explicit `JOIN ... ON` syntax. Comma-separated tables in `FROM` are syntactically invalid in this dialect and will be rejected by the safety engine. CTEs (WITH) and subqueries are permitted. WRONG: `FROM yearmonth, customers WHERE yearmonth.customerid = customers.customerid`. CORRECT: `FROM yearmonth JOIN customers ON yearmonth.customerid = customers.customerid`. The CORRECT form is the only acceptable shape.
```

- [ ] **Step 2: Render relationships as JOIN templates**

In the same file, find the relationships block at lines 33-38:

```jinja
{% if schema.relationships -%}
Relationships:
{% for rel in schema.relationships -%}
- {{ rel.source_table }}.{{ rel.source_column }} -> {{ rel.target_table }}.{{ rel.target_column }}
{% endfor %}
{%- endif %}
```

Replace with:

```jinja
{% if schema.relationships -%}
Relationships (use these JOIN templates verbatim — they are the only legal way to join these tables):
{% for rel in schema.relationships -%}
- JOIN {{ rel.target_table }} ON {{ rel.source_table }}.{{ rel.source_column }} = {{ rel.target_table }}.{{ rel.target_column }}
{% endfor %}
{%- endif %}
```

- [ ] **Step 3: Add assertions in `test_prompt_builder.py`**

Append a new test:

```python
def test_prompt_relationships_render_as_join_templates() -> None:
    """Relationships must render as ready-to-paste JOIN templates so the LLM
    is biased toward explicit JOIN syntax over comma-separated FROM clauses.
    """
    from app.steward import AbstractRelationshipDef

    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0",
        tables=[
            AbstractTableDef(
                alias="yearmonth",
                description="Monthly billing periods",
                physical_target="public.yearmonth",
                columns=[
                    AbstractColumnDef(
                        alias="customerid",
                        description="FK",
                        data_type="integer",
                        safety=SafetyClassification(
                            join_participation_allowed=True
                        ),
                        physical_target="customerid",
                    ),
                ],
            ),
            AbstractTableDef(
                alias="customers",
                description="Customers",
                physical_target="public.customers",
                columns=[
                    AbstractColumnDef(
                        alias="customerid",
                        description="PK",
                        data_type="integer",
                        safety=SafetyClassification(
                            join_participation_allowed=True
                        ),
                        physical_target="customerid",
                    ),
                ],
            ),
        ],
        relationships=[
            AbstractRelationshipDef(
                source_table="yearmonth",
                source_column="customerid",
                target_table="customers",
                target_column="customerid",
            )
        ],
        omitted_columns={},
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert (
        "JOIN customers ON yearmonth.customerid = customers.customerid"
        in envelope.system_instruction
    )
    # The arrow form must NOT appear — it was the misleading old format
    assert "yearmonth.customerid -> customers.customerid" not in envelope.system_instruction


def test_prompt_rule_7_contains_counter_example() -> None:
    """Rule 7 must contain the concrete WRONG/CORRECT counter-example pair."""
    builder = PromptBuilder()
    intent = UserIntent(natural_language_query="test")
    schema = FilteredSchema(
        version="1.0", tables=[], relationships=[], omitted_columns={}
    )
    hints = PromptHints(column_hints=[])
    envelope = builder.build_prompt(intent, schema, hints, chat_history=[])

    assert "WRONG:" in envelope.system_instruction
    assert "CORRECT:" in envelope.system_instruction
    assert "FROM yearmonth, customers" in envelope.system_instruction
    assert "FROM yearmonth JOIN customers" in envelope.system_instruction
```

- [ ] **Step 4: Run the prompt builder tests**

Run: `uv run pytest tests/test_prompt_builder.py -v`
Expected: all PASS.

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -v`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add app/compiler/templates/system.jinja tests/test_prompt_builder.py
git commit -m "$(cat <<'EOF'
feat(prompt): JOIN templates and rule 7 counter-example (BUG-4 prompt-only)

Two prompt-only changes designed to push the LLM away from
comma-separated FROM clauses on the three persistent failure questions
(q=1476, q=1486, q=1493) without resorting to a translator-level
rewriter:

1. Rule 7 reworded as a syntactic constraint with an explicit
   WRONG/CORRECT counter-example pair using a real BIRD relationship.

2. The relationships block now renders each declared edge as a
   ready-to-paste JOIN template ("JOIN customers ON yearmonth.customerid
   = customers.customerid") instead of the misleading arrow form. This
   biases the LLM toward emitting the join in the syntactic shape we
   want — positive signal beats negative instruction.

If these prompt changes do not eliminate BUG-4, the next iteration can
add a translator desugaring pass that runs BEFORE the safety engine and
join-graph validator (so safety guarantees are preserved). That work
gets its own plan.

EOF
)"
```

---

## Phase 3 checkpoint

Rerun the BIRD benchmark. Expected impact: most or all of q=1476, q=1486, q=1493 become correct (3 questions). Combined cumulative: ~60% (30/50).

If any of those three still fail with the implicit JOIN safety violation, the prompt-only approach has hit its ceiling and a translator desugaring pass is the next step. Document which question(s) failed and the LLM output, then write a separate plan for the desugaring pass before proceeding.

---

# Phase 4 — Scope-aware column resolution refactor

**Why this exists:** The current `_collect_table_scope` method walks `tree.find_all(exp.Table)` and adds *every* referenced table to a single `tables_in_scope` set, regardless of whether the reference is in an outer query, a CTE body, or a subquery. This is the root cause of two bug classes:

1. **BUG-3/BUG-5 bypass workarounds** at `translator.py:244` and `translator.py:363`. We added explicit "is this name a CTE?" guards in `_resolve_column_with_prefix` and `_resolve_column_without_prefix` because the global scope set was lying to us — telling us a CTE name was a table.
2. **PROMPT-3 ambiguous-naked-column failure (q=1526)**. The outer query references a bare `customerid` that is unambiguous in its actual scope (a single CTE that exposes one column with that name), but the global scope set includes the inner-CTE tables (`transactions_1k` and `yearmonth`), so the resolver sees two candidates and raises.

The proposed fix in the analysis doc — "if all candidates resolve to the same physical target, pick one" — is a *correctness bug* in disguise. Two columns with the same physical name (`customers.id` vs `orders.id`) are not interchangeable. The right fix is to make scope local: a column reference is resolved against the sources visible at *its own AST location*, not against a global set.

`sqlglot.optimizer.scope.build_scope()` returns a tree of `Scope` objects, each of which knows its `selected_sources` (tables and CTEs visible at that level) and `cte_sources` (CTEs in scope). We use it to build a `node_id → Scope` map and a `Scope → SourceContext` map, then resolve every column against its local context.

**Risk:** This is the largest change in the plan and touches the core of the translator. Existing tests should catch most regressions, but there is residual risk in the join-graph validator and the orphaned-prefix repair path. Expect to iterate.

If after Task 4.1 (discovery) the work looks larger than estimated, **stop and split this phase into its own plan** — it is independently mergeable from Phases 0-3 and can ship later.

### Task 4.1: Discovery — read sqlglot's scope API

**Files:** none (research task)

- [ ] **Step 1: Read the relevant sqlglot source**

Open the installed sqlglot package and read these files:

```bash
uv run python -c "import sqlglot.optimizer.scope; print(sqlglot.optimizer.scope.__file__)"
```

Read the `Scope` class and the `build_scope`, `traverse_scope`, and `walk_in_scope` functions. Pay attention to:

- How `selected_sources` is keyed (alias vs name) and what its values are (Table, CTE, Subquery, etc.)
- How `cte_sources` differs from `selected_sources`
- Whether there is a built-in mapping from a `Column` node to the `Scope` it belongs to (look for `find_all`, `column_alias_or_name`, `references`)
- How the API handles SET operations (UNION) at the root

- [ ] **Step 2: Read the sqlglot tests for scope**

```bash
uv run python -c "import sqlglot.optimizer; import os; print(os.path.dirname(sqlglot.optimizer.__file__))"
```

Look for `test_scope.py` in the sqlglot test directory or upstream repo. Note the patterns used to walk scopes and resolve columns.

- [ ] **Step 3: Write a tiny exploratory script**

Create a temporary file `/tmp/scope_explore.py` (do NOT commit this):

```python
import sqlglot
from sqlglot.optimizer.scope import build_scope, traverse_scope

sql = """
WITH agg AS (
    SELECT customerid, SUM(amount) AS total
    FROM transactions_1k
    JOIN yearmonth ON transactions_1k.customerid = yearmonth.customerid
    GROUP BY customerid
)
SELECT customerid, total FROM agg ORDER BY total DESC
"""

tree = sqlglot.parse_one(sql, dialect="postgres")
root = build_scope(tree)
print("Root scope sources:", root.selected_sources.keys() if root else None)
print("Root scope CTEs:", root.cte_sources.keys() if root else None)
for s in traverse_scope(tree):
    print(f"\nScope: {type(s.expression).__name__}")
    print(f"  selected_sources: {list(s.selected_sources.keys())}")
    print(f"  cte_sources: {list(s.cte_sources.keys())}")
    for col in s.expression.find_all(sqlglot.exp.Column):
        # Only top-level — does not descend into nested scopes
        if any(col is c for c in s.expression.find_all(sqlglot.exp.Column)):
            print(f"  column: {col.sql()}")
```

Run: `uv run python /tmp/scope_explore.py`
Expected: prints the scope tree with `selected_sources` showing exactly what's locally visible at each level. Confirm that the outer scope's `selected_sources` is `{"agg": <CTE>}` and does NOT include `transactions_1k` or `yearmonth`.

- [ ] **Step 4: Document findings inline**

In your scratch notes (not committed), record:
- The exact API signatures you'll use
- How to map an `exp.Column` instance back to its containing scope (helper or manual walk)
- Whether `traverse_scope` handles UNION queries correctly
- Any sqlglot bugs or edge cases you hit

Carry these notes into Task 4.2.

- [ ] **Step 5: Delete the exploratory script**

```bash
rm /tmp/scope_explore.py
```

(Nothing to commit for this task.)

### Task 4.2: Introduce a `_ScopeIndex` helper that wraps sqlglot

**Files:**
- Modify: `app/compiler/translator.py` — add a new internal helper class

This task introduces the new abstraction *without* wiring it in yet. The existing translator continues to work; we add the helper alongside and add unit tests for it. This isolation lets us validate the helper before committing to the larger rewiring.

- [ ] **Step 1: Add the `_ScopeIndex` dataclass**

In `app/compiler/translator.py`, after the existing `_TableScope` dataclass (around line 33), add:

```python
@dataclass
class _ScopeContext:
    """Sources visible at a specific scope level.

    `physical_sources` maps an alias (or table name when no alias) to the
    abstract table name. CTE references go into `cte_sources` and are NOT
    physically resolved — they are virtual tables whose columns were already
    validated when the CTE body was processed.

    `cte_output_columns` lists the AS-declared output names of any CTEs in
    this scope's selected_sources, so a bare reference to a CTE-only column
    in the outer query can be recognized.
    """
    physical_sources: dict[str, str]
    cte_sources: set[str]
    cte_output_columns: set[str]


@dataclass
class _ScopeIndex:
    """Per-node lookup of the scope a Column belongs to.

    Built once per translate() call from sqlglot's Scope tree. Each Column
    node in the AST maps to exactly one _ScopeContext describing what is
    visible at its location.
    """
    column_to_context: dict[int, _ScopeContext]
```

- [ ] **Step 2: Add the builder method**

After `_collect_table_scope` (around line 144), add:

```python
    def _build_scope_index(self, tree: exp.Expression) -> _ScopeIndex:
        """Builds a per-Column _ScopeContext lookup using sqlglot's Scope tree.

        Each Column's containing scope determines what tables, CTEs, and
        CTE-output columns are visible to it. This replaces the global
        `_collect_table_scope` set, which conflates outer-query, CTE-body,
        and subquery sources into one big bag.
        """
        from sqlglot.optimizer.scope import build_scope, traverse_scope

        index: dict[int, _ScopeContext] = {}
        root = build_scope(tree)
        if root is None:
            return _ScopeIndex(column_to_context=index)

        for scope in traverse_scope(tree):
            ctx = self._scope_to_context(scope)
            # Walk only the columns that belong to THIS scope, not its
            # descendants. sqlglot's Scope.columns yields exactly that.
            for col in scope.columns:
                index[id(col)] = ctx
        return _ScopeIndex(column_to_context=index)

    def _scope_to_context(self, scope: Any) -> _ScopeContext:
        """Translates a sqlglot Scope into a translator-friendly _ScopeContext."""
        physical_sources: dict[str, str] = {}
        cte_sources: set[str] = set()
        cte_output_columns: set[str] = set()

        for alias, source in scope.selected_sources.items():
            # source is a (node, parent_scope_or_None) tuple in some sqlglot
            # versions; in others it's the node directly. Handle both.
            node = source[0] if isinstance(source, tuple) else source
            alias_lower = alias.lower()
            if alias_lower in scope.cte_sources:
                cte_sources.add(alias_lower)
                cte_scope = scope.cte_sources[alias_lower]
                # CTE bodies are sqlglot Scope objects whose `expression` is
                # the inner Select. Pull AS-declared output columns.
                cte_body = cte_scope[0] if isinstance(cte_scope, tuple) else cte_scope
                inner_select = (
                    cte_body.expression
                    if hasattr(cte_body, "expression")
                    else cte_body
                )
                if isinstance(inner_select, exp.Select):
                    for proj in inner_select.expressions:
                        if isinstance(proj, exp.Alias):
                            cte_output_columns.add(proj.alias.lower())
            else:
                # Physical table reference. Source node is exp.Table.
                if isinstance(node, exp.Table):
                    physical_sources[alias_lower] = node.name.lower()

        return _ScopeContext(
            physical_sources=physical_sources,
            cte_sources=cte_sources,
            cte_output_columns=cte_output_columns,
        )
```

**Important:** the exact shape of `selected_sources` and `cte_sources` (tuple-wrapped or not) depends on the installed sqlglot version. The discovery script in Task 4.1 will tell you which form you're dealing with — adapt the helper to match. If the sqlglot version yields a tuple, keep the `isinstance(source, tuple)` guards; if it yields the node directly, remove them. Do not leave both code paths in production code — pick one and add a comment naming the sqlglot version.

You will need to import `Any` if it isn't already imported, and add `Any` to the type signature of `_scope_to_context` to avoid pulling sqlglot internals into the public type surface. Confirm with: `uv run python -c "from sqlglot.optimizer.scope import Scope; print(Scope)"`

- [ ] **Step 3: Add unit tests for `_ScopeIndex`**

In `tests/test_translator.py`, append a new section:

```python
# ------------------------------------------------------------------
# Phase 4 — Scope-aware column resolution
# ------------------------------------------------------------------

def test_scope_index_isolates_cte_body_from_outer_query() -> None:
    """A bare column in the outer query must see only the outer scope's
    selected sources, not the tables referenced inside a CTE body."""
    import sqlglot
    translator = DeterministicTranslator()
    tree = sqlglot.parse_one(
        "WITH agg AS (SELECT customerid, SUM(amount) AS total"
        " FROM transactions_1k JOIN yearmonth"
        " ON transactions_1k.customerid = yearmonth.customerid"
        " GROUP BY customerid)"
        " SELECT customerid, total FROM agg ORDER BY total DESC",
        dialect="postgres",
    )
    index = translator._build_scope_index(tree)

    # Find the outer-query `total` column reference (in ORDER BY)
    outer_total = None
    for col in tree.find_all(sqlglot.exp.Column):
        if col.name == "total":
            # The outer ORDER BY total is at the root, not inside the CTE
            parent = col.parent
            while parent is not None and not isinstance(parent, sqlglot.exp.CTE):
                if isinstance(parent, sqlglot.exp.Select) and parent is tree:
                    outer_total = col
                    break
                parent = parent.parent
            if outer_total is not None:
                break

    assert outer_total is not None, "outer total reference not found"
    ctx = index.column_to_context.get(id(outer_total))
    assert ctx is not None, "outer total has no scope context"
    # Outer scope sees the CTE 'agg' but NOT transactions_1k or yearmonth
    assert "agg" in ctx.cte_sources
    assert "transactions_1k" not in ctx.physical_sources
    assert "yearmonth" not in ctx.physical_sources
    # The CTE's AS-declared output column 'total' is visible
    assert "total" in ctx.cte_output_columns


def test_scope_index_inner_cte_sees_its_own_tables() -> None:
    """A column inside a CTE body must see the tables referenced in that
    CTE body's FROM clause."""
    import sqlglot
    translator = DeterministicTranslator()
    tree = sqlglot.parse_one(
        "WITH agg AS (SELECT customerid FROM transactions_1k)"
        " SELECT customerid FROM agg",
        dialect="postgres",
    )
    index = translator._build_scope_index(tree)

    inner_customerid = None
    for col in tree.find_all(sqlglot.exp.Column):
        # The inner reference is inside the CTE body
        parent = col.parent
        while parent is not None:
            if isinstance(parent, sqlglot.exp.CTE):
                inner_customerid = col
                break
            parent = parent.parent
        if inner_customerid is not None:
            break

    assert inner_customerid is not None
    ctx = index.column_to_context.get(id(inner_customerid))
    assert ctx is not None
    assert "transactions_1k" in ctx.physical_sources
```

- [ ] **Step 4: Run the new tests**

Run: `uv run pytest tests/test_translator.py -k "scope_index" -v`
Expected: 2 PASS. If they fail, the `_scope_to_context` helper has wrong assumptions about sqlglot's Scope shape — adjust based on the discovery findings from Task 4.1.

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest -v`
Expected: all green. The new helper is not yet wired in, so existing translation tests are unaffected.

- [ ] **Step 6: Commit**

```bash
git add app/compiler/translator.py tests/test_translator.py
git commit -m "$(cat <<'EOF'
feat(translator): introduce _ScopeIndex helper backed by sqlglot Scope

Adds a per-Column scope context lookup (_ScopeIndex) built from
sqlglot's Scope tree. Each Column node maps to a _ScopeContext that
describes the physical sources, CTE sources, and CTE-declared output
columns visible at its specific AST location — not a global bag of
every table referenced anywhere in the query.

The helper is unwired in this commit; the next commit replaces the
global _collect_table_scope mechanism with it. Two unit tests lock in
the key invariant: the outer query's scope does NOT include tables
referenced inside a CTE body.

EOF
)"
```

### Task 4.3: Wire `_ScopeIndex` into column resolution

**Files:**
- Modify: `app/compiler/translator.py` — `translate()`, `_walk_tree_nodes`, `_resolve_column_with_prefix`, `_resolve_column_without_prefix`

This is the largest single change in the plan. Read the existing methods carefully before editing.

- [ ] **Step 1: Thread `_ScopeIndex` through `translate()` and `_walk_tree_nodes`**

In `app/compiler/translator.py`, find the `translate()` method (lines 41-89). After the existing `cte_aliases` / `cte_col_aliases` collection at lines 54-55, add the scope index build:

```python
        cte_aliases = self._collect_cte_aliases(tree)
        cte_col_aliases = self._collect_cte_column_aliases(tree)
        scope_index = self._build_scope_index(tree)
```

Then update the call to `_walk_tree_nodes` (line 64):

```python
        literals, column_datatypes = self._walk_tree_nodes(
            tree, maps, scope, repairs, cte_aliases, cte_col_aliases, scope_index
        )
```

In `_walk_tree_nodes` (lines 150-191), add the new parameter and pass it through to the column resolution methods:

Find:
```python
    def _walk_tree_nodes(
        self,
        tree: exp.Expression,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        cte_aliases: set[str],
        cte_col_aliases: set[str],
    ) -> tuple[list[exp.Literal], dict[int, str]]:
```

Replace with:
```python
    def _walk_tree_nodes(
        self,
        tree: exp.Expression,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        cte_aliases: set[str],
        cte_col_aliases: set[str],
        scope_index: _ScopeIndex,
    ) -> tuple[list[exp.Literal], dict[int, str]]:
```

In the same method, find the column branches at lines 178-187 and update both calls to pass `scope_index`:

```python
                if t_prefix:
                    self._resolve_column_with_prefix(
                        node_inst, c_name, t_prefix,
                        maps, scope, repairs, column_datatypes, cte_aliases,
                        scope_index,
                    )
                else:
                    self._resolve_column_without_prefix(
                        node_inst, c_name, maps, scope, column_datatypes,
                        cte_col_aliases, scope_index,
                    )
```

- [ ] **Step 2: Update `_resolve_column_with_prefix` to use the local scope**

Find the existing method at lines 231-284. Replace its body with one that consults `scope_index` first:

```python
    def _resolve_column_with_prefix(
        self,
        node_inst: exp.Column,
        c_name: str,
        t_prefix: str,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        repairs: list[TranslationRepair],
        column_datatypes: dict[int, str],
        cte_aliases: set[str],
        scope_index: _ScopeIndex,
    ) -> None:
        local_ctx = scope_index.column_to_context.get(id(node_inst))
        if local_ctx is not None and t_prefix in local_ctx.cte_sources:
            # CTE virtual table — column was validated inside the CTE body;
            # leave identifier as-is, no physical resolution or safety checks.
            return

        # Local scope says the prefix is a physical source — use the local
        # source's actual abstract name (handles aliases like `c` → customers).
        if local_ctx is not None and t_prefix in local_ctx.physical_sources:
            resolved_table = local_ctx.physical_sources[t_prefix]
        elif t_prefix in scope.dynamic_table_aliases or t_prefix in scope.tables_in_scope:
            # Fallback path: the global scope still knows about it. This handles
            # the case where sqlglot's Scope analysis missed something we did
            # not anticipate. The fallback also covers cte_aliases for back-compat.
            resolved_table = scope.dynamic_table_aliases.get(t_prefix, t_prefix)
            if resolved_table in cte_aliases:
                return
        else:
            self._resolve_orphaned_prefix(
                node_inst, c_name, t_prefix, maps, scope, repairs, column_datatypes
            )
            return

        if resolved_table not in maps.alias_to_physical_table:
            raise TranslationError(
                f"Table '{resolved_table}' does not exist in schema context."
            )
        full_alias = f"{resolved_table}.{c_name}"
        if full_alias not in maps.alias_to_physical_col:
            raise TranslationError(
                f"Column '{full_alias}' does not exist in schema context."
            )
        node_inst.set(
            "this",
            exp.Identifier(this=maps.alias_to_physical_col[full_alias]),
        )
        column_datatypes[id(node_inst)] = maps.alias_to_datatype.get(
            full_alias, ""
        )
        assigned_aliases = scope.table_runtime_prefixes.get(
            resolved_table, set()
        )
        runtime_prefix = self._resolve_runtime_prefix(
            t_prefix,
            resolved_table,
            assigned_aliases,
            maps.alias_to_physical_table[resolved_table],
            c_name,
        )
        node_inst.set("table", exp.Identifier(this=runtime_prefix))
        self._check_column_safety(
            c_name,
            resolved_table,
            maps.alias_to_safety[full_alias],
            node_inst,
        )
```

- [ ] **Step 3: Update `_resolve_column_without_prefix` to scope-narrow ambiguity**

Find the method at lines 354-400. Replace it with:

```python
    def _resolve_column_without_prefix(
        self,
        node_inst: exp.Column,
        c_name: str,
        maps: _SchemaLookupMaps,
        scope: _TableScope,
        column_datatypes: dict[int, str],
        cte_col_aliases: set[str],
        scope_index: _ScopeIndex,
    ) -> None:
        local_ctx = scope_index.column_to_context.get(id(node_inst))

        # CTE-declared output columns are virtual; bypass before any other check.
        if local_ctx is not None and c_name in local_ctx.cte_output_columns:
            return
        if c_name in cte_col_aliases:
            # Fallback for nodes the scope index didn't see — keep BUG-3 working.
            return

        # Use the LOCAL scope's physical sources to narrow ambiguity.
        if local_ctx is not None and local_ctx.physical_sources:
            local_tables = set(local_ctx.physical_sources.values())
            owning_tables = maps.column_ownership.get(c_name, set())
            scoped_owning_tables = owning_tables.intersection(local_tables)
        else:
            # Global fallback for queries with no recognizable scope structure.
            owning_tables = maps.column_ownership.get(c_name, set())
            scoped_owning_tables = owning_tables.intersection(scope.tables_in_scope)

        if len(scoped_owning_tables) > 1:
            raise TranslationError(
                f"Ambiguous naked column '{c_name}'. Belongs to multiple scoped"
                f" tables: {list(scoped_owning_tables)}. Explicit aliasing required."
            )
        if len(scoped_owning_tables) == 1:
            unique_owning_table = scoped_owning_tables.pop()
            full_alias = f"{unique_owning_table}.{c_name}"
            node_inst.set(
                "this",
                exp.Identifier(this=maps.alias_to_physical_col[full_alias]),
            )
            column_datatypes[id(node_inst)] = maps.alias_to_datatype.get(
                full_alias, ""
            )
            self._check_column_safety(
                c_name,
                unique_owning_table,
                maps.alias_to_safety[full_alias],
                node_inst,
            )
        elif c_name in maps.alias_to_physical_col:
            raise TranslationError(
                f"Column '{c_name}' exists in the schema but its owning table"
                f" is not referenced in this query."
                f" Explicit table qualification required."
            )
        else:
            raise TranslationError(
                f"Column '{c_name}' does not exist in the schema context."
            )
```

- [ ] **Step 4: Run the entire test suite**

Run: `uv run pytest -v`
Expected: all green. The fallback paths are intentionally kept so existing tests do not regress while the local-scope path takes over the BIRD failure cases.

If any existing test fails, **stop and investigate** — the new logic is wrong, not the test. The fallback paths exist precisely to keep existing behavior; a failing test means a real regression.

- [ ] **Step 5: Add the q=1526 regression test**

In `tests/test_translator.py`, append:

```python
def test_bare_column_in_outer_query_unambiguous_via_local_scope() -> None:
    """A bare column in the outer query that is unambiguous in its LOCAL scope
    (a single CTE that exposes one column with that name) must resolve cleanly,
    even when the underlying physical tables referenced inside the CTE body
    contain multiple columns with the same name.

    This is the q=1526 PROMPT-3 fallback failure: customerid exists in both
    transactions_1k and yearmonth (joined inside a CTE), but the outer query
    only sees the CTE's single customerid output. Global-scope resolution
    saw both physical tables and raised "Ambiguous naked column"; local-scope
    resolution sees only the CTE.
    """
    parser = SQLParser()
    safety = SafetyEngine()
    translator = DeterministicTranslator()
    schema = _make_schema_with_relationship()  # users(id), orders(user_id, total)

    # Build a CTE that joins users and orders, exposing one bare column,
    # then reference that column in the outer query without a prefix.
    ast = parser.parse(AbstractQuery(
        sql=(
            "WITH joined AS ("
            " SELECT users.id, orders.total"
            " FROM users JOIN orders ON users.id = orders.user_id"
            ") "
            "SELECT total FROM joined ORDER BY total DESC"
        )
    ))
    validated = safety.validate(ast)
    result = translator.translate(
        validated, schema, abstract_query_hash="h",
        relationships=schema.relationships,
    )
    assert result is not None
```

- [ ] **Step 6: Run the new test**

Run: `uv run pytest tests/test_translator.py::test_bare_column_in_outer_query_unambiguous_via_local_scope -v`
Expected: PASS.

- [ ] **Step 7: Lint and type-check**

Run in parallel:
```bash
uv run ruff check app/compiler/translator.py tests/test_translator.py
uv run mypy app/compiler/translator.py tests/test_translator.py
uv run lint-imports
```
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add app/compiler/translator.py tests/test_translator.py
git commit -m "$(cat <<'EOF'
fix(translator): scope-aware column resolution via sqlglot Scope (PROMPT-3)

Replaces the global _collect_table_scope set as the primary signal for
column resolution with a per-Column _ScopeIndex built from sqlglot's
Scope tree. Each column now resolves against its actual local scope:
the outer query sees the CTEs and tables visible at the outer level,
not every table referenced anywhere in the query.

This fixes the q=1526 PROMPT-3 failure: a bare 'customerid' in the
outer query that is unambiguous in its local scope (a single CTE
exposing one customerid) but was previously ambiguous against the
global set (which also included transactions_1k and yearmonth from
inside the CTE body).

The existing global-scope path is preserved as a fallback so any
sqlglot scope edge case we did not anticipate falls back to current
behavior. The BUG-3/BUG-5 cte_aliases / cte_col_aliases bypasses are
also kept as fallbacks; they can be removed in a follow-up once we
have benchmark evidence that the local-scope path covers every case.

EOF
)"
```

### Task 4.4: Remove the BUG-3/BUG-5 fallbacks (deferred clean-up)

**This task is intentionally deferred** to a later iteration. The fallbacks at `cte_aliases`/`cte_col_aliases` are belt-and-suspenders during the migration. After Phase 4 ships and we have benchmark evidence that the local-scope path covers every case (no failures pinned to the fallback firing), open a separate small PR that:

1. Removes the `cte_aliases` parameter from `_resolve_column_with_prefix` and the fallback `if resolved_table in cte_aliases: return` branch.
2. Removes the `cte_col_aliases` parameter from `_resolve_column_without_prefix` and the fallback `if c_name in cte_col_aliases: return` branch.
3. Removes the now-unused `_collect_cte_column_aliases` static method (the scope index now provides `cte_output_columns`).
4. Updates tests accordingly.

**Do not bundle this clean-up with Phase 4.** Splitting it lets the benchmark data prove the local-scope path is sufficient before deleting the safety net.

---

## Phase 4 checkpoint

Rerun the BIRD benchmark. Expected impact: q=1526 (PROMPT-3) becomes correct. Combined cumulative: ~62% (31/50). The bigger win is *latent*: a class of future scope-related bugs is now structurally prevented, and the codebase is meaningfully simpler to reason about.

If any *new* failure surfaces (regression in a question that was previously correct), pin it to either:
- A wrong scope assumption in `_scope_to_context` → fix the helper
- The local-scope path being correct but stricter than the global-scope path → that question was relying on accidentally permissive behavior; investigate whether the gold SQL is actually unambiguous

---

# Final checklist

After all five phases land:

- [ ] All tests green: `uv run pytest -v`
- [ ] Lint clean: `uv run ruff check .`
- [ ] Type-check clean: `uv run mypy .`
- [ ] Import boundaries clean: `uv run lint-imports`
- [ ] BIRD benchmark rerun confirms cumulative gain
- [ ] Per-phase results documented in `docs/benchmark/` matching the existing convention
- [ ] Memory updated: `project_status.md` reflects new accuracy and what's next
- [ ] No `# noqa`, `# type: ignore`, or rule suppressions added (per `feedback_no_rule_relaxation.md`)
- [ ] No column descriptions edited, no fake FK relationships added (per benchmark integrity rule)

---

## Note on the analysis-vs-implementation rule

This plan was written in response to an explicit "make a plan" request. Per `feedback_analysis_vs_implementation.md`, the next step is to **wait for an explicit go-ahead** ("implement", "go", "execute", etc.) before touching any source files. Phase 0 is the smallest possible starting point — running it alone is a low-risk way to validate the plan before committing to the larger phases.
